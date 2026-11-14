#!/usr/bin/env python3
"""Plan P0 grid feasibility cases; execute one explicit case in the selected owned environment."""

import argparse
import hashlib
import importlib.util
import json
import math
import signal
import subprocess
import sys
import tempfile
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any, Protocol, cast

SPEC = importlib.util.spec_from_file_location(
    "grid_environment", Path(__file__).with_name("test-data-grid-environment.py")
)
if SPEC is None or SPEC.loader is None:
    raise ImportError("Cannot load owned environment fixture")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Environment(Protocol):
    COMMAND: str

    def execute_owned(
        self,
        report_dir: Path,
        *,
        command: str,
        environment: dict[str, str],
        validate: Callable[[str, int], dict[str, Any]],
        extra_artifacts: list[str],
    ) -> dict[str, Any]: ...
    def terminate(self, signal: int, frame: FrameType | None) -> None: ...


ENV = cast(Environment, MODULE)
SEEDS = [17, 29, 43]


def memory_profile(bounded: bool) -> dict[str, str | int | None]:
    return {
        "name": "shared-2g-bounded-gc-v1" if bounded else "runtime_defaults",
        "driver_heap_limit_bytes": 192 * 1024**2 if bounded else None,
        "member_heap_limit_bytes": 448 * 1024**2 if bounded else None,
        "remaining_container_bytes": 512 * 1024**2 if bounded else None,
    }


def scenario(batch: int, concurrency: int, seed: int, *, smoke: bool) -> dict[str, int]:
    if batch not in [1, 32, 128] or concurrency not in [1, 16, 64] or seed not in SEEDS:
        raise ValueError("Unsupported matrix coordinates")
    return {
        "key_count": 256 if smoke else 100000,
        "batch_size": batch,
        "concurrency": concurrency,
        "seed": seed,
        "warmup_seconds": 1 if smoke else 60,
        "sample_seconds": 5 if smoke else 300,
    }


def nearest_rank(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(percentile * len(values)) - 1)]


def validate_measurement(
    directory: Path, expected: dict[str, int], xml: str, code: int, *, bounded_memory: bool = False
) -> dict[str, Any]:
    root = ET.fromstring(xml)
    tests = root.findall(".//test")
    assemblies = root.findall("assembly")
    if (
        code
        or not assemblies
        or any(
            sum(int(assembly.get(name, "-1")) for assembly in assemblies) != count
            for name, count in [
                ("total", 1),
                ("passed", 1),
                ("failed", 0),
                ("skipped", 0),
                ("errors", 0),
            ]
        )
        or len(tests) != 1
        or tests[0].get("type") != "HostLoom.Tests.DataGridFeasibilityTests"
        or tests[0].get("result") != "Pass"
        or root.findall(".//error")
    ):
        raise RuntimeError(
            "Feasibility requires one executed driver with no infrastructure errors/skips."
        )
    result = json.loads((directory / "feasibility/case.json").read_text())
    if result.get("process_memory_profile") != memory_profile(bounded_memory):
        raise RuntimeError(
            "Process memory profile mismatch; default and bounded runs are separate evidence."
        )
    if bounded_memory:
        if result["runtime"]["total_available_memory_bytes"] != 192 * 1024**2:
            raise RuntimeError("Driver GC budget was not applied.")
        samples = (directory / "feasibility/resources.jsonl").read_text().splitlines()
        members = [
            m
            for line in samples
            for m in json.loads(line)["members"]
            if m["member"] >= 0 and "managed" in m
        ]
        if {m["member"] for m in members} != {0, 1, 2} or any(
            m["managed"]["total_available_memory_bytes"] != 448 * 1024**2 for m in members
        ):
            raise RuntimeError("Member GC budgets were absent or inconsistent.")
    if result["case"] != expected or result["is_p0"] != (expected["key_count"] == 100000):
        raise RuntimeError("Case parameters or P0/smoke label mismatch.")
    if result["status"] not in {"completed", "infeasible", "incomplete_budget"}:
        raise RuntimeError("Missing or failed feasibility execution.")
    identities: dict[tuple[int, int], str] = {}
    for phase in ["warmup", "sample"]:
        path = directory / f"feasibility/{phase}-calls.jsonl"
        if not path.exists():
            if result["status"] == "completed":
                raise RuntimeError("Completed case lost its raw invocation observations.")
            continue
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        summary = result[phase]
        if (
            (result["status"] == "completed" and phase == "sample" and not rows)
            or len(rows) > 100000
            or len(rows) != summary["invocations"]
        ):
            raise RuntimeError("Raw invocation count is empty, truncated or inconsistent.")
        for row in rows:
            if (
                row["duration_us"] < 0
                or row["attempts"] not in range(4)
                or row["items"] != expected["batch_size"]
            ):
                raise RuntimeError("Invalid per-invocation units/attempt/batch accounting.")
            if row["operation"] == "write":
                identity = row["worker"], row["request_number"]
                if identity in identities and identities[identity] != row["digest"]:
                    raise RuntimeError(
                        "An immutable mutation identity changed across retries/phases."
                    )
                identities[identity] = row["digest"]
        in_window = [row for row in rows if row["within_window"] and row["outcome"] != "cancelled"]
        success = [
            row for row in in_window if row["outcome"] == "completed" and not row["item_failures"]
        ]
        logical = [
            row["logical_duration_us"]
            for row in in_window
            if row["logical_completed_here"] and row["originated_here"]
        ]
        for name, values in [
            ("successful_call_us", [r["duration_us"] for r in success]),
            ("all_completed_call_us", [r["duration_us"] for r in in_window]),
            ("logical_write_us", logical),
        ]:
            if summary[name]["count"] != len(values):
                raise RuntimeError("Percentile denominator mismatch.")
            for key, quantile in [("p50", 0.50), ("p95", 0.95), ("p99", 0.99)]:
                if summary[name][key] != nearest_rank(values, quantile):
                    raise RuntimeError("Percentile is not nearest rank of individual observations.")
        seconds = expected["warmup_seconds" if phase == "warmup" else "sample_seconds"]
        throughput = len(success) / seconds if seconds else 0
        counts = {
            "unknown": sum(r["outcome"] == "OutcomeUnknown" for r in rows),
            "refused": sum(
                r["outcome"] not in {"completed", "cancelled"} and not r["may_have_been_admitted"]
                for r in rows
            ),
            "errors": sum(r["outcome"] != "completed" or bool(r["item_failures"]) for r in rows),
        }
        for count, rate in [
            ("unknown", "unknown_rate"),
            ("refused", "refusal_rate"),
            ("errors", "error_rate"),
        ]:
            if summary[count] != counts[count] or not math.isclose(
                summary[rate], counts[count] / len(rows) if rows else 0
            ):
                raise RuntimeError("Failure count/rate denominator mismatch.")
        if not 0 <= summary["sdk_timeouts"] <= len(rows) or not math.isclose(
            summary["sdk_timeout_rate"], summary["sdk_timeouts"] / len(rows) if rows else 0
        ):
            raise RuntimeError("SDK deadline counter/rate denominator mismatch.")
        if (
            not math.isclose(summary["request_throughput"], throughput)
            or summary["successful_within_window"] != len(success)
            or summary["late_or_censored"] != len(rows) - len(in_window)
        ):
            raise RuntimeError("Success, throughput or censored-window accounting mismatch.")
    if result["status"] == "completed" and (
        not result.get("recovery_verified") or result["unresolved_mutations"]
    ):
        raise RuntimeError("Completed execution lacks resolved identity/recovery evidence.")
    return {
        "driver_passed": True,
        "case_status": result["status"],
        "is_p0": result["is_p0"],
        "acceptance": "no_numerical_targets",
        "result": result,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run one owned case; otherwise print the 27-case P0 plan.",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="256 keys, one-second warmup/five-second sample; never a P0 measurement.",
    )
    parser.add_argument("--batch-size", type=int, choices=[1, 32, 128], default=1)
    parser.add_argument("--concurrency", type=int, choices=[1, 16, 64], default=1)
    parser.add_argument("--seed", type=int, choices=SEEDS, default=17)
    parser.add_argument("--report-dir", type=Path)
    parser.add_argument(
        "--bounded-memory",
        action="store_true",
        help="Use explicit 192-MiB driver/448-MiB member GC caps within the same two-GiB container.",
    )
    args = parser.parse_args()
    if not args.execute:
        print(
            json.dumps(
                {
                    "execution": False,
                    "acceptance": "targets_unset",
                    "process_memory_profile": memory_profile(args.bounded_memory),
                    "cases": [
                        scenario(batch, concurrency, seed, smoke=False)
                        for batch in [1, 32, 128]
                        for concurrency in [1, 16, 64]
                        for seed in SEEDS
                    ],
                },
                indent=2,
            )
        )
        return 0
    expected = scenario(args.batch_size, args.concurrency, args.seed, smoke=args.smoke)
    if args.report_dir:
        directory = args.report_dir.resolve()
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    else:
        directory = Path(tempfile.mkdtemp(prefix="hostloom-grid-feasibility-"))
    (directory / "plan.json").write_text(
        json.dumps(
            {
                "case": expected,
                "is_p0_spec": not args.smoke,
                "entrypoint_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "matrix_seeds": SEEDS,
                "acceptance": "targets_unset",
                "process_memory_profile": memory_profile(args.bounded_memory),
            },
            indent=2,
        )
        + "\n"
    )
    # Same copied source/build, containment and ownership checks as the qualified environment.
    command = (
        ENV.COMMAND[: ENV.COMMAND.index("dotnet tests/HostLoom.Tests/bin/")]
        + r"""
dotnet build-server shutdown
mkdir /work/feasibility
printf %s "$HOSTLOOM_GRID_FEASIBILITY_OWNER" > /work/FEASIBILITY_OWNER
__DRIVER_GC__dotnet tests/HostLoom.Tests/bin/Release/net10.0/HostLoom.Tests.dll -class '*DataGridFeasibilityTests' -showLiveOutput -xml /work/tests.xml
"""
    ).replace("__DRIVER_GC__", "DOTNET_GCHeapHardLimit=C000000 " if args.bounded_memory else "")
    signal.signal(signal.SIGTERM, ENV.terminate)
    try:
        report = ENV.execute_owned(
            directory,
            command=command,
            environment={
                "HOSTLOOM_GRID_FEASIBILITY": "1",
                "HOSTLOOM_GRID_FEASIBILITY_BOUNDED_GC": "1" if args.bounded_memory else "0",
                "HOSTLOOM_GRID_FEASIBILITY_OWNER": uuid.uuid4().hex,
                "HOSTLOOM_GRID_FEASIBILITY_CASE": json.dumps(expected),
            },
            extra_artifacts=["feasibility"],
            validate=lambda xml, code: validate_measurement(
                directory, expected, xml, code, bounded_memory=args.bounded_memory
            ),
        )
    except KeyboardInterrupt:
        return 130
    except (OSError, RuntimeError, ValueError, ET.ParseError, subprocess.SubprocessError) as error:
        print(f"Feasibility failed: {error}; artifacts: {directory}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "execution_status": report["status"],
                "case_status": report["tests"]["case_status"],
                "is_p0": report["tests"]["is_p0"],
                "acceptance": "no_numerical_targets",
                "reports": str(directory),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
