#!/usr/bin/env python3
"""Qualify the local Linux ARM64/overlayfs grid prototype in one owned container."""

import argparse
import hashlib
import json
import os
import platform
import signal
import subprocess
import sys
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any, NoReturn

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "mcr.microsoft.com/dotnet/sdk@sha256:212fe4cb74627d5adf9817a4088906ae3edff1bd853bb27565d4b46731efe61e"
LABEL = "hostloom.grid.environment.owner"
REQUIRED = {
    "DataGridProcessTests",
    "DataGridPeerTlsTests",
    "DataGridClientTlsTests",
    "DataGridCredentialTests",
    "DataGridCredentialTlsTests",
    "DataGridFileStorageTests",
    "DataGridWalFrontierTests",
    "DataGridProtocolTests",
}


def run(arguments: list[str], *, timeout: int = 30) -> str:
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"{arguments[0]} {arguments[1]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def validate_matrix(xml: str, exit_code: int) -> dict[str, Any]:
    root = ET.fromstring(xml)
    assemblies = root.findall("assembly")
    tests = root.findall(".//test")
    if exit_code or not assemblies or not tests:
        raise RuntimeError("Qualification requires successful nonempty executed test evidence.")
    if root.findall(".//error") or any(int(assembly.get("errors", "0")) for assembly in assemblies):
        raise RuntimeError("Test infrastructure reported errors.")
    counts = Counter(test.get("result") for test in tests)
    expected = {
        "total": len(tests),
        "passed": counts["Pass"],
        "failed": counts["Fail"],
        "skipped": counts["Skip"],
    }
    if counts["Pass"] == 0 or counts["Fail"] or set(counts) - {"Pass", "Skip"}:
        raise RuntimeError("Failed, missing or unexecuted qualification cases.")
    for key, value in expected.items():
        if sum(int(assembly.get(key, "-1")) for assembly in assemblies) != value:
            raise RuntimeError(f"Inconsistent xUnit {key} accounting.")
    classes: dict[str, dict[str, int]] = {}
    skips = []
    for test in tests:
        name = test.get("type", "").removeprefix("HostLoom.Tests.")
        if not name.startswith("DataGrid") or not test.get("method"):
            raise RuntimeError("Unexpected or unidentified test outside the selected grid suite.")
        counts_for_class = classes.setdefault(name, {"passed": 0, "skipped": 0})
        if test.get("result") == "Pass":
            counts_for_class["passed"] += 1
            continue
        reason = (test.findtext("reason/message") or test.findtext("reason", "")).strip()
        method = test.get("method", "")
        allowed = (
            name in {"DataGridDiskFullTests", "DataGridDiskFullTlsTests"}
            and reason == "Requires explicit owned bounded tmpfs disk-full fixture."
        ) or (
            name == "DataGridSessionPressureTlsTests"
            and method == "GlobalSessionPressureSurvivesCompactionRestartAndBoundedExpiry"
            and reason.startswith("Full TLS session-capacity qualification takes")
        )
        allowed |= (
            name == "DataGridFeasibilityTests"
            and method == "OwnedThreeProcessFeasibilityCase"
            and reason == "Requires explicit owned feasibility runner."
        )
        if not allowed:
            raise RuntimeError(
                f"Required or unexpected skipped qualification case: {name}.{method}"
            )
        counts_for_class["skipped"] += 1
        skips.append({"class": name, "method": method, "reason": reason})
    missing = REQUIRED - classes.keys()
    if missing or any(classes[name]["skipped"] or not classes[name]["passed"] for name in REQUIRED):
        raise RuntimeError(
            f"Missing or skipped required process/TLS/storage matrix: {sorted(missing)}"
        )
    return {**expected, "classes": classes, "explicit_exclusions": skips}


def inspect_owned(container: str, owner: str, cache: Path) -> None:
    data = json.loads(run(["docker", "inspect", container]))[0]
    mounts = {mount["Destination"]: mount for mount in data["Mounts"]}
    if (
        data["Id"] != container
        or data["Name"] != "/hostloom-grid-environment-" + owner
        or data["Config"]["Labels"].get(LABEL) != owner
        or data["Config"]["Image"] != IMAGE
        or data["HostConfig"]["NetworkMode"] != "none"
        or data["HostConfig"]["Privileged"]
        or data["HostConfig"]["CapDrop"] != ["ALL"]
        or "no-new-privileges" not in data["HostConfig"]["SecurityOpt"]
        or data["HostConfig"]["Memory"] != 2 * 1024**3
        or data["HostConfig"]["MemorySwap"] != 2 * 1024**3
        or data["HostConfig"]["NanoCpus"] != 2_000_000_000
        or data["HostConfig"]["PortBindings"]
        or set(mounts) != {"/cache"}
        or mounts["/cache"]["RW"]
        or Path(mounts["/cache"]["Source"]).resolve() != cache.resolve()
    ):
        raise RuntimeError("Container ownership, profile or containment mismatch.")


COMMAND = r"""
set -euo pipefail
umask 077
[[ -f /.dockerenv && "$(uname -m)" == aarch64 ]]
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 ]]
[[ "$(dotnet --version)" == 10.0.403 ]]
[[ "$(stat -f -c %T /tmp)" == overlayfs ]]
[[ "$(df -B1 --output=avail /tmp | tail -1)" -gt 2147483648 ]]
[[ "$(cat /sys/fs/cgroup/memory.max)" == 2147483648 ]]
[[ "$(cat /sys/fs/cgroup/cpu.max)" == '200000 100000' ]]
{
    printf 'profile=local-linux-arm64-overlayfs-v1\n'
    cat /etc/os-release
    uname -srvm
    dotnet --info
    openssl version
    date -u +utc=%Y-%m-%dT%H:%M:%SZ
    df -T /tmp /work
    stat -f -c 'temp_filesystem=%T' /tmp
    getconf PAGESIZE
    cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/cpu.max
} > /work/environment.txt
cd /work
printf '%s  source.tar\n' "$HOSTLOOM_GRID_SOURCE_SHA256" | sha256sum -c -
tar --no-same-owner -xf source.tar
rm source.tar
# These opt-ins are set by this owned runner, never inherited from the invoking shell.
[[ "$HOSTLOOM_DATA_GRID_PROCESSES" == 1 ]]
[[ -z "${HOSTLOOM_DATA_GRID_DISK_FULL:-}" && -z "${HOSTLOOM_DATA_GRID_SESSION_PRESSURE:-}" ]]
dotnet restore tests/HostLoom.Tests/HostLoom.Tests.csproj --packages /work/.packages --source /cache -m:1
dotnet build tests/HostLoom.Tests/HostLoom.Tests.csproj -c Release --no-restore -m:1
dotnet tests/HostLoom.Tests/bin/Release/net10.0/HostLoom.Tests.dll -class '*DataGrid*' -showLiveOutput -xml /work/tests.xml
"""


def execute_owned(
    report_dir: Path,
    *,
    command: str = COMMAND,
    environment: dict[str, str] | None = None,
    validate: Callable[[str, int], dict[str, Any]] = validate_matrix,
    extra_artifacts: list[str] | None = None,
) -> dict[str, Any]:
    owner = uuid.uuid4().hex
    report: dict[str, Any] = {
        "schema_version": 1,
        "profile": "local-linux-arm64-overlayfs-v1",
        "scope": "synthetic local-process qualification; one shared host/VM/container",
        "image": IMAGE,
        "owner": owner,
        "status": "failed",
        "cleanup": False,
        "controls": {
            "cpus": 2,
            "memory_bytes": 2 * 1024**3,
            "swap_bytes": 0,
            "network": "none",
            "published_ports": [],
            "cap_drop": ["ALL"],
            "no_new_privileges": True,
            "command_timeout_seconds": 900,
        },
        "host": {"os": platform.system(), "kernel": platform.release(), "arch": platform.machine()},
    }
    container = ""
    started = time.monotonic()
    try:
        engine = run(
            [
                "docker",
                "info",
                "--format",
                "server={{.ServerVersion}} os={{.OSType}} arch={{.Architecture}} "
                "driver={{.Driver}} kernel={{.KernelVersion}}",
            ]
        )
        report["engine"] = engine
        if "os=linux arch=aarch64 driver=overlayfs " not in engine:
            raise RuntimeError(
                "This profile requires a native Linux ARM64 Docker engine/overlayfs."
            )
        cache_text = run(["dotnet", "nuget", "locals", "global-packages", "--list"])
        cache = Path(cache_text.removeprefix("global-packages: ")).resolve()
        if not cache.is_dir():
            raise RuntimeError("Restore the solution first; a local NuGet cache is required.")
        archive = report_dir / "source.tar"
        arguments = [
            "tar",
            "--no-xattrs",
            "--exclude=bin",
            "--exclude=obj",
            "--exclude=TestResults",
            "--exclude=__pycache__",
            "-cf",
            str(archive),
            "-C",
            str(ROOT),
            "src",
            "tests",
            "benchmarks/HostLoom.Benchmarks/CatalogRefreshWorkers.cs",
            "examples/HostLoom.Examples.Actors/RegionalCatalog.cs",
            "examples/HostLoom.Examples.Actors/RegionalCatalogOwner.cs",
            "examples/HostLoom.Examples.CompositionDiagnostics/ApplicationCompositionLedger.cs",
            "global.json",
            "Directory.Build.props",
            "Directory.Build.targets",
            "Directory.Packages.props",
            ".editorconfig",
            "HostLoom.slnx",
        ]
        subprocess.run(
            arguments, check=True, timeout=60, env={**os.environ, "COPYFILE_DISABLE": "1"}
        )
        with archive.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        report["source_archive_sha256"] = digest
        report["runner_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        container = run(
            [
                "docker",
                "create",
                "--platform",
                "linux/arm64",
                "--name",
                "hostloom-grid-environment-" + owner,
                "--label",
                LABEL + "=" + owner,
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--network",
                "none",
                "--memory",
                "2g",
                "--memory-swap",
                "2g",
                "--cpus",
                "2",
                "--mount",
                f"type=bind,source={cache},target=/cache,readonly",
                "--env",
                "HOSTLOOM_DATA_GRID_PROCESSES=1",
                "--env",
                "DOTNET_CLI_TELEMETRY_OPTOUT=1",
                "--env",
                "DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1",
                IMAGE,
                "sleep",
                "infinity",
            ]
        )
        report["container"] = container
        inspect_owned(container, owner, cache)
        print(f"Owned container: {container}; reports: {report_dir}", flush=True)
        run(["docker", "start", container])
        run(["docker", "exec", container, "mkdir", "/work"])
        run(["docker", "cp", str(archive), container + ":/work/source.tar"], timeout=60)
        with (report_dir / "run.log").open("w") as output:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "--env",
                    "HOSTLOOM_GRID_SOURCE_SHA256=" + digest,
                    *[
                        argument
                        for key, value in (environment or {}).items()
                        for argument in ("--env", key + "=" + value)
                    ],
                    container,
                    "timeout",
                    "--kill-after=15s",
                    "900s",
                    "bash",
                    "-c",
                    command,
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=940,
                check=False,
            )
        report["test_exit_code"] = result.returncode
        artifact_errors = []
        for name in ["environment.txt", "tests.xml", *(extra_artifacts or [])]:
            try:
                run(["docker", "cp", container + ":/work/" + name, str(report_dir / name)])
            except RuntimeError as error:
                artifact_errors.append(str(error))
        report["artifact_errors"] = artifact_errors
        if result.returncode or artifact_errors:
            raise RuntimeError(
                f"Build/test process exited {result.returncode}; inspect run.log and any test XML."
            )
        report["tests"] = validate((report_dir / "tests.xml").read_text(), result.returncode)
        report["status"] = "passed"
        return report
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError, ET.ParseError) as error:
        report["error"] = str(error)
        raise
    except KeyboardInterrupt:
        report["status"] = "failed"
        report["error"] = "Qualification interrupted."
        raise
    finally:
        try:
            if container:
                # Recheck the exact immutable ID and ownership label before deleting any resource.
                actual = json.loads(run(["docker", "inspect", container]))[0]
                if actual["Id"] != container or actual["Config"]["Labels"].get(LABEL) != owner:
                    raise RuntimeError("Refusing cleanup of a container with changed ownership.")
                run(["docker", "rm", "--force", container])
            report["cleanup"] = True
        except (OSError, RuntimeError, subprocess.SubprocessError, ValueError) as error:
            report["status"] = "failed"
            report["cleanup_error"] = str(error)
            raise
        finally:
            report["elapsed_seconds"] = round(time.monotonic() - started, 3)
            (report_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")


def qualify(report_dir: Path) -> dict[str, Any]:
    return execute_owned(report_dir)


def terminate(_signal: int, _frame: FrameType | None) -> NoReturn:
    raise KeyboardInterrupt("Termination requested.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-dir", type=Path, help="New private report directory; must not exist."
    )
    args = parser.parse_args()
    if args.report_dir:
        directory = args.report_dir.resolve()
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    else:
        directory = Path(tempfile.mkdtemp(prefix="hostloom-grid-environment-"))
    signal.signal(signal.SIGTERM, terminate)
    try:
        report = qualify(directory)
    except KeyboardInterrupt:
        print(f"Qualification interrupted; reports: {directory}", file=sys.stderr)
        return 130
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError, ET.ParseError) as error:
        print(f"Qualification failed: {error}; reports: {directory}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {"status": report["status"], "tests": report["tests"], "reports": str(directory)},
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
