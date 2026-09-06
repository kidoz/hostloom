#!/usr/bin/env python3
"""Run live TLS/ACL and restart tests against one owned, disposable Valkey container."""

import argparse
import hashlib
import json
import os
import secrets
import signal
import socket
import ssl
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LABEL = "hostloom.valkey.deployment-owner"


def run(arguments: list[str], *, timeout: int = 30) -> str:
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        # Commands never contain passwords, but server output and certificate tool output need
        # not become diagnostics. The test process reports its own assertion failures separately.
        raise RuntimeError(f"{arguments[0]} {arguments[1]} failed (exit {result.returncode}).")
    return result.stdout.strip()


def certificates(directory: Path) -> None:
    runtime = directory / "runtime"
    runtime.mkdir(mode=0o755)
    run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-days",
            "2",
            "-subj",
            "/CN=HostLoom disposable test CA",
            "-keyout",
            str(directory / "ca.key"),
            "-out",
            str(runtime / "ca.crt"),
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]
    )
    run(
        [
            "openssl",
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(runtime / "server.key"),
            "-out",
            str(directory / "server.csr"),
        ]
    )
    extensions = directory / "extensions.cnf"
    extensions.write_text(
        "subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"
    )
    run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(directory / "server.csr"),
            "-CA",
            str(runtime / "ca.crt"),
            "-CAkey",
            str(directory / "ca.key"),
            "-CAcreateserial",
            "-out",
            str(runtime / "server.crt"),
            "-days",
            "2",
            "-sha256",
            "-extfile",
            str(extensions),
        ]
    )
    # The parent temporary directory is private to the invoking user. Only runtime/ is mounted,
    # read-only; the unprivileged container process must be able to read its server key.
    for path in runtime.iterdir():
        path.chmod(0o644)


def inspect_owned(container: str, owner: str) -> dict[str, Any]:
    data = json.loads(run(["docker", "inspect", container]))[0]
    if data["Id"] != container or data["Config"]["Labels"].get(LABEL) != owner:
        raise RuntimeError("Container ownership mismatch; refusing lifecycle operations.")
    if data["Name"] != "/hostloom-valkey-deployment-" + owner:
        raise RuntimeError("Unexpected container name.")
    if data["HostConfig"]["Memory"] != 128 * 1024 * 1024:
        raise RuntimeError("Unexpected container memory limit.")
    if data["HostConfig"]["NanoCpus"] != 1_000_000_000:
        raise RuntimeError("Unexpected container CPU limit.")
    return data


def wait_ready(port: int, ca: Path, password: str) -> None:
    context = ssl.create_default_context(cafile=str(ca))
    arguments = [b"AUTH", b"catalog", password.encode()]
    authentication = b"*3\r\n" + b"".join(
        b"$" + str(len(argument)).encode() + b"\r\n" + argument + b"\r\n" for argument in arguments
    )
    deadline = time.monotonic() + 20
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as tcp:
                with context.wrap_socket(tcp, server_hostname="localhost") as secure:
                    secure.sendall(authentication + b"*1\r\n$4\r\nPING\r\n")
                    with secure.makefile("rb") as replies:
                        if (
                            replies.readline(128) != b"+OK\r\n"
                            or replies.readline(128) != b"+PONG\r\n"
                        ):
                            raise RuntimeError("TLS/ACL readiness failed.")
                    return
        except OSError:
            if time.monotonic() >= deadline:
                raise TimeoutError("TLS/ACL readiness timed out.") from None
            time.sleep(0.05)


def run_tests(environment: dict[str, str]) -> None:
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
        "*ValkeyDeploymentTests",
    ]
    with subprocess.Popen(arguments, cwd=ROOT, env=environment, start_new_session=True) as process:
        try:
            if process.wait(timeout=240):
                raise RuntimeError("Valkey deployment tests failed; see the test output above.")
        finally:
            # End the entire test process group before removing its fixture, including on Ctrl-C.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image",
        choices=["valkey/valkey:9.1", "valkey/valkey:8.1", "valkey/valkey:7.2"],
        default="valkey/valkey:9.1",
    )
    parser.add_argument(
        "--no-build", action="store_true", help="Use already built Release test binaries."
    )
    args = parser.parse_args()
    if os.name != "posix":
        raise RuntimeError("This Docker fixture runner requires Linux or macOS.")
    if not args.no_build:
        subprocess.run(
            ["dotnet", "build", "HostLoom.slnx", "-c", "Release"], cwd=ROOT, check=True, timeout=300
        )
    run(["docker", "info", "--format", "{{.ServerVersion}}"])
    owner = uuid.uuid4().hex
    container = ""
    with tempfile.TemporaryDirectory(prefix="hostloom-valkey-deployment-") as temporary:
        directory = Path(temporary).resolve()
        certificates(directory)
        password = secrets.token_hex(24)
        digest = hashlib.sha256(password.encode()).hexdigest()
        runtime = directory / "runtime"
        # Explicit commands plus key/channel patterns. No administrative commands, no default user.
        commands = "+hello +ping +select +get +set +exists +pttl +pexpire +sadd +smembers +unlink +del +eval +evalsha +publish +subscribe +unsubscribe"
        (runtime / "users.acl").write_text(
            f"user default off\nuser catalog on #{digest} ~deployment-* &deployment-* -@all {commands}\n"
        )
        (runtime / "valkey.conf").write_text(
            "bind 0.0.0.0\nport 0\ntls-port 6379\ntls-auth-clients no\n"
            "tls-cert-file /fixture/server.crt\ntls-key-file /fixture/server.key\n"
            "tls-ca-cert-file /fixture/ca.crt\naclfile /fixture/users.acl\n"
            'save ""\nappendonly no\n'
        )
        for path in runtime.iterdir():
            path.chmod(0o644)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        try:
            container = run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--pull=never",
                    "--name",
                    "hostloom-valkey-deployment-" + owner,
                    "--label",
                    LABEL + "=" + owner,
                    "--memory",
                    "128m",
                    "--cpus",
                    "1",
                    "--publish",
                    f"127.0.0.1:{port}:6379",
                    "--mount",
                    f"type=bind,src={runtime},dst=/fixture,readonly",
                    "--tmpfs",
                    "/data:rw,size=16777216",
                    args.image,
                    "valkey-server",
                    "/fixture/valkey.conf",
                ]
            )
            deadline = time.monotonic() + 20
            while True:
                state = inspect_owned(container, owner)
                if not state["State"]["Running"]:
                    raise RuntimeError("Disposable Valkey exited before test startup.")
                bindings = state["NetworkSettings"]["Ports"].get("6379/tcp")
                if bindings:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Valkey port allocation timed out.")
                time.sleep(0.05)
            if (
                len(bindings) != 1
                or bindings[0]["HostIp"] != "127.0.0.1"
                or int(bindings[0]["HostPort"]) != port
            ):
                raise RuntimeError("Valkey port must be bound only to loopback.")
            wait_ready(int(bindings[0]["HostPort"]), runtime / "ca.crt", password)
            fixture = directory / "fixture.json"
            fixture.write_text(
                json.dumps(
                    {
                        "container": container,
                        "owner": owner,
                        "port": int(bindings[0]["HostPort"]),
                        "ca": str(runtime / "ca.crt"),
                        "password": password,
                    }
                )
            )
            fixture.chmod(0o600)
            environment = dict(os.environ, HOSTLOOM_VALKEY_DEPLOYMENT_FIXTURE=str(fixture))
            print(
                f"Testing {args.image}: TLS-only, restricted ACL, 128 MiB, one CPU, loopback port.",
                flush=True,
            )
            run_tests(environment)
        finally:
            if container:
                inspect_owned(container, owner)
                run(["docker", "rm", "-f", container])
                print(
                    "Removed the owned Valkey container and temporary TLS/ACL material.", flush=True
                )


if __name__ == "__main__":
    main()
