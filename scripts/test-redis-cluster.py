#!/usr/bin/env python3
"""Run real Redis Cluster primary-crash tests in one owned six-process Docker fixture."""

import argparse
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LABEL = "hostloom.redis.cluster-owner"


def run(arguments: list[str], *, timeout: int = 30) -> str:
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"{arguments[0]} {arguments[1]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def inspect_owned(container: str, owner: str, ports: list[int]) -> dict[str, Any]:
    data = json.loads(run(["docker", "inspect", container]))[0]
    expected = {f"{port}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(port)}] for port in ports}
    if (
        data["Id"] != container
        or data["Name"] != "/hostloom-redis-cluster-" + owner
        or data["Config"]["Labels"].get(LABEL) != owner
        or data["HostConfig"]["Memory"] != 512 * 1024 * 1024
        or data["HostConfig"]["NanoCpus"] != 2_000_000_000
        or data["HostConfig"]["PortBindings"] != expected
    ):
        raise RuntimeError("Redis Cluster ownership or containment mismatch.")
    return data


def wait_ready(container: str, ports: list[int]) -> None:
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        healthy = True
        for port in ports:
            output = run(
                ["docker", "exec", container, "redis-cli", "-p", str(port), "CLUSTER", "INFO"]
            )
            healthy &= "cluster_state:ok" in output and "cluster_slots_ok:16384" in output
            replication = run(
                ["docker", "exec", container, "redis-cli", "-p", str(port), "INFO", "replication"]
            )
            if "role:slave" in replication:
                healthy &= "master_link_status:up" in replication
        if healthy:
            return
        time.sleep(0.1)
    raise TimeoutError("Six-node Redis Cluster did not become healthy.")


def run_tests(environment: dict[str, str], method: str) -> None:
    arguments = [
        "dotnet",
        "test",
        "--project",
        "tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj",
        "-c",
        "Release",
        "--no-build",
        "--no-restore",
        "--",
        "--filter-class",
        "*RedisClusterFailoverTests",
        "--filter-method",
        method,
    ]
    with subprocess.Popen(arguments, cwd=ROOT, env=environment, start_new_session=True) as process:
        try:
            if process.wait(timeout=600):
                raise RuntimeError("Redis Cluster tests failed; see the assertions above.")
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-build", action="store_true", help="Use already built Release tests.")
    parser.add_argument("--filter-method", default="*", help="Optional xUnit method filter.")
    args = parser.parse_args()
    if os.name != "posix":
        raise RuntimeError("This fixture runner requires Linux or macOS.")
    if not args.no_build:
        subprocess.run(
            ["dotnet", "build", "HostLoom.slnx", "-c", "Release"], cwd=ROOT, check=True, timeout=300
        )
    run(["docker", "info", "--format", "{{.ServerVersion}}"])
    # Reuse the repository's Redis version. Pull only when it is not already available.
    if subprocess.run(
        ["docker", "image", "inspect", "redis:7.4"], capture_output=True, check=False
    ).returncode:
        run(["docker", "pull", "redis:7.4"], timeout=180)
    owner = uuid.uuid4().hex
    container = ""
    ports: list[int] = []
    with tempfile.TemporaryDirectory(prefix="hostloom-redis-cluster-") as temporary:
        directory = Path(temporary).resolve()
        runtime = directory / "runtime"
        runtime.mkdir(mode=0o755)
        reservations: list[socket.socket] = []
        try:
            for _ in range(6):
                reservation = socket.socket()
                reservations.append(reservation)
                reservation.bind(("127.0.0.1", 0))
                ports.append(reservation.getsockname()[1])
        finally:
            for reservation in reservations:
                reservation.close()
        for index, port in enumerate(ports):
            config = runtime / f"node-{port}.conf"
            # All six processes share a network namespace: the advertised loopback addresses
            # reach the same node both inside Docker and through identical host port bindings.
            # Cluster bus ports are internal only and never exposed on the host.
            config.write_text(
                f"bind 0.0.0.0\nprotected-mode no\nport {port}\ndaemonize yes\n"
                f"pidfile /data/node-{port}/redis.pid\ndir /data/node-{port}\n"
                f"logfile /data/node-{port}/redis.log\ncluster-enabled yes\n"
                f"cluster-config-file nodes.conf\ncluster-node-timeout 1000\n"
                f"cluster-announce-ip 127.0.0.1\ncluster-announce-port {port}\n"
                f"cluster-port {20000 + index}\ncluster-announce-bus-port {20000 + index}\n"
                'save ""\nappendonly no\nrepl-diskless-sync-delay 0\n'
                "replica-announce-ip 127.0.0.1\n"
                f"replica-announce-port {port}\n"
            )
            config.chmod(0o644)
        try:
            command = [
                "docker",
                "run",
                "-d",
                "--pull=never",
                "--name",
                "hostloom-redis-cluster-" + owner,
                "--label",
                LABEL + "=" + owner,
                "--memory",
                "512m",
                "--cpus",
                "2",
                "--mount",
                f"type=bind,src={runtime},dst=/fixture,readonly",
                "--tmpfs",
                "/data:rw,size=67108864",
            ]
            for port in ports:
                command.extend(["--publish", f"127.0.0.1:{port}:{port}"])
            command.extend(["redis:7.4", "sleep", "infinity"])
            container = run(command)
            inspect_owned(container, owner, ports)
            for port in ports:
                run(["docker", "exec", container, "mkdir", "-p", f"/data/node-{port}"])
                run(["docker", "exec", container, "redis-server", f"/fixture/node-{port}.conf"])
            print(
                run(
                    [
                        "docker",
                        "exec",
                        container,
                        "redis-cli",
                        "--cluster",
                        "create",
                        *[f"127.0.0.1:{port}" for port in ports],
                        "--cluster-replicas",
                        "1",
                        "--cluster-yes",
                    ],
                    timeout=60,
                ),
                flush=True,
            )
            wait_ready(container, ports)
            fixture = directory / "fixture.json"
            fixture.write_text(json.dumps({"container": container, "owner": owner, "ports": ports}))
            fixture.chmod(0o600)
            print(
                "Testing Redis 7.4: three primaries, three replicas, process crashes, loopback only.",
                flush=True,
            )
            run_tests(
                dict(os.environ, HOSTLOOM_REDIS_CLUSTER_FIXTURE=str(fixture)), args.filter_method
            )
            wait_ready(container, ports)
        finally:
            if container:
                inspect_owned(container, owner, ports)
                run(["docker", "rm", "-f", container])
                print(
                    "Removed the owned Redis Cluster container and ephemeral node data.", flush=True
                )


if __name__ == "__main__":
    main()
