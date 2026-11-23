"""Run a three-member partition trial against disposable, unpublished Docker networks."""

import argparse
import json
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from typing import Any

PROJECT = Path(__file__).resolve().parent
IMAGE = "mcr.microsoft.com/dotnet/sdk:10.0.403"
STORE = "redis:7.4"


def command(*args: str, timeout: int = 30, check: bool = True) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{args}: {result.stderr or result.stdout}")
    return result.stdout.strip()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def until(probe: Callable[[], str], accept: Callable[[str], bool], seconds: int) -> str:
    deadline = time.monotonic() + seconds
    last = ""
    while time.monotonic() < deadline:
        last = probe()
        if accept(last):
            return last
        time.sleep(0.3)
    raise TimeoutError(f"condition unmet after {seconds}s; last observation {last!r}")


class Trial:
    def __init__(self, runtime: str, binary: Path, logs: Path) -> None:
        self.runtime = runtime
        self.binary = binary
        self.logs = logs
        self.prefix = f"hl-actor-{uuid.uuid4().hex[:12]}"
        self.nodes = ["n0", "n1", "n2"]
        self.ips: dict[str, str] = {}

    def name(self, suffix: str) -> str:
        return f"{self.prefix}-{suffix}"

    def redis(self, *args: str) -> str:
        return command("docker", "exec", self.name("store"), "redis-cli", "--raw", *args)

    def start(self, cleanup: ExitStack) -> None:
        for network in ("peer", "control"):
            name = self.name(network)
            command("docker", "network", "create", "--internal", name)
            cleanup.callback(command, "docker", "network", "rm", name)
        control = self.name("control")
        store = self.name("store")
        command(
            "docker",
            "run",
            "-d",
            "--name",
            store,
            "--network",
            control,
            "--network-alias",
            "store",
            STORE,
            "redis-server",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
        )
        cleanup.callback(command, "docker", "rm", "-fv", store)
        until(
            lambda: command("docker", "exec", store, "redis-cli", "PING", check=False),
            lambda value: value == "PONG",
            20,
        )

        for node in self.nodes:
            name = self.name(node)
            command(
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "--network",
                control,
                "-e",
                f"NODE={node}",
                "-v",
                f"{self.binary}:/app:ro",
                IMAGE,
                "sleep",
                "infinity",
            )
            cleanup.callback(command, "docker", "rm", "-fv", name)
            command("docker", "network", "connect", self.name("peer"), name)
            networks = json.loads(command("docker", "inspect", name))[0]["NetworkSettings"][
                "Networks"
            ]
            self.ips[node] = networks[self.name("peer")]["IPAddress"]
        for node in self.nodes:
            command(
                "docker",
                "exec",
                "-d",
                self.name(node),
                "sh",
                "-c",
                'exec dotnet /app/ActorClusterTrial.dll "$@" >/tmp/trial.log 2>&1',
                "trial",
                self.runtime,
                self.ips[node],
                self.ips["n0"],
            )

    def route(self, node: str, operation: str, timeout: int = 15) -> str:
        correlation = uuid.uuid4().hex
        self.redis("RPUSH", f"in:{node}", f"{correlation}|{operation}")
        return until(lambda: self.redis("GET", f"out:{correlation}"), bool, timeout)

    def reachable(self, sender: str, recipient: str) -> bool:
        port = "2552" if self.runtime == "akka" else "11111"
        test = subprocess.run(
            [
                "docker",
                "exec",
                self.name(sender),
                "timeout",
                "2",
                "bash",
                "-c",
                'exec 3<>/dev/tcp/"$1"/"$2"',
                "probe",
                self.ips[recipient],
                port,
            ],
            capture_output=True,
            timeout=8,
        )
        require(test.returncode in (0, 1, 124), f"peer probe failed to execute: {test.stderr!r}")
        return test.returncode == 0

    def replay(self, sender: str, token: str, operation: str) -> str:
        return command(
            "docker",
            "exec",
            self.name(sender),
            "dotnet",
            "/app/ActorClusterTrial.dll",
            "replay",
            token,
            operation,
        )

    def pair(self, nodes: list[str], operations: list[str]) -> list[str]:
        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(self.route, nodes, operations))

    def run(self) -> dict[str, Any]:
        for node in self.nodes:
            until(
                lambda node=node: self.route(node, "members", timeout=60),
                lambda value: value == "3",
                90,
            )
        before = self.pair(self.nodes[:2], ["read", "read"])
        require(before[0] == before[1] and before[0].endswith("|"), f"initial routes: {before}")
        old = before[0].split("|")[0]
        isolated = old.split(":")[0]
        majority = [n for n in self.nodes if n != isolated]
        require(self.redis("GET", "writer") == old, "initial grant from two ingress votes")
        acks = self.pair(self.nodes[:2], ["first", "second"])
        require(acks == [f"{old}|committed"] * 2, f"initial acknowledgements: {acks}")
        history = self.redis("LRANGE", "journal", "0", "-1").splitlines()
        require(sorted(history) == sorted([f"first@{old}", f"second@{old}"]), "initial journal")
        require(
            self.reachable(isolated, majority[0]) and self.reachable(majority[0], isolated),
            "peer path unavailable before partition",
        )

        started = time.monotonic()
        command("docker", "network", "disconnect", self.name("peer"), self.name(isolated))
        for node in majority:
            require(
                not self.reachable(isolated, node) and not self.reachable(node, isolated),
                "partitioned peer port still reachable",
            )
        read = until(
            lambda: self.route(majority[0], "read"),
            lambda v: v.split(":")[0] in majority and "|" in v,
            150,
        )
        recovered_at = round(time.monotonic() - started, 2)
        new, restored = read.split("|")
        require(restored.split(",") == history, f"replacement read: {read}")
        require(self.route(majority[0], "before-grant") == f"{new}|fenced", "ungranted writer")
        require(self.redis("GET", "writer") == old, "one voter transferred authority")
        last_old = self.replay(isolated, old, "at-handoff")
        require(last_old == "committed", f"previous writer before transfer: {last_old}")
        handoff_history = [*history, f"at-handoff@{old}"]
        witnessed = self.route(majority[1], "read")
        require(
            witnessed == f"{new}|{','.join(handoff_history)}",
            "survivors disagree or lost late commit",
        )
        require(self.redis("GET", "writer") == new, "two witnesses failed to transfer authority")
        require(self.redis("SISMEMBER", "retired", old) == "1", "former activation not retired")

        # The former runtime can self-terminate. Replay its retained credential from its
        # still-isolated container through the identical commit function and store path.
        with ThreadPoolExecutor(max_workers=2) as pool:
            stale = pool.submit(self.replay, isolated, old, "late")
            after = self.pair(majority, ["third", "fourth"])
            require(stale.result() == "fenced", "stale write accepted")
        require(after == [f"{new}|committed"] * 2, f"post-handoff acknowledgements: {after}")
        require(self.route(majority[0], "first") == f"{new}|duplicate", "ambiguous retry reapplied")
        final = self.redis("LRANGE", "journal", "0", "-1").splitlines()
        require(
            final[:3] == handoff_history
            and sorted(final[3:]) == sorted([f"third@{new}", f"fourth@{new}"]),
            f"unexpected final journal: {final}",
        )
        for node in majority:
            require(
                self.route(node, "read") == f"{new}|{','.join(final)}", "final recovery mismatch"
            )
        return dict(
            runtime=self.runtime,
            initial_routes=before,
            old_owner=old,
            new_owner=new,
            survivors=majority,
            initial_members=3,
            acknowledged_before=acks,
            replacement_restored=restored.split(","),
            seconds_until_replacement_read=recovered_at,
            replacement_before_grant="fenced",
            former_writer_before_transfer=last_old,
            transfer_witnesses=majority,
            stale_commit="fenced",
            retry="duplicate",
            acknowledged_after=after,
            final_journal=final,
        )

    def save_logs(self) -> None:
        for node in self.nodes:
            log = command("docker", "exec", self.name(node), "cat", "/tmp/trial.log", check=False)
            (self.logs / f"{self.runtime}-{node}.log").write_text(log)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="new directory for results and logs"
    )
    parser.add_argument("--runtime", choices=["akka", "orleans", "both"], default="both")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="hl-actor-bin-") as scratch:
        binary = Path(scratch).resolve()
        command(
            "dotnet",
            "publish",
            str(PROJECT / "ActorClusterTrial.csproj"),
            "-c",
            "Release",
            "-o",
            str(binary),
            timeout=180,
        )
        for runtime in ["orleans", "akka"] if args.runtime == "both" else [args.runtime]:
            trial = Trial(runtime, binary, args.output)
            with ExitStack() as cleanup:
                try:
                    trial.start(cleanup)
                    report = trial.run()
                    print(json.dumps(report), flush=True)
                    (args.output / f"{runtime}.json").write_text(
                        json.dumps(report, indent=2) + "\n"
                    )
                finally:
                    trial.save_logs()


if __name__ == "__main__":
    main()
