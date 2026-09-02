#!/usr/bin/env python3
"""Create or enforce the HostLoom cache/lock BenchmarkDotNet baseline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPORT_NAMES = (
    "HostLoom.Benchmarks.CachingBenchmarks-report-full-compressed.json",
    "HostLoom.Benchmarks.LockingBenchmarks-report-full-compressed.json",
)
ENVIRONMENT_FIELDS = (
    "BenchmarkDotNetVersion",
    "ProcessorName",
    "RuntimeVersion",
    "Architecture",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail when cache/lock mean time or allocation regresses past the baseline."
    )
    parser.add_argument(
        "--results",
        type=Path,
        default=Path("BenchmarkDotNet.Artifacts/results"),
        help="Directory containing BenchmarkDotNet JSON reports.",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("benchmarks/baselines/cache-lock.json"),
        help="Committed baseline file.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Replace the baseline with the current reports instead of checking it.",
    )
    return parser.parse_args()


def load_reports(results: Path) -> tuple[dict[str, str], str, dict[str, dict[str, float]]]:
    reports: list[dict[str, Any]] = []
    for name in REPORT_NAMES:
        path = results / name
        if not path.is_file():
            raise ValueError(f"missing BenchmarkDotNet report: {path}")
        with path.open(encoding="utf-8") as stream:
            reports.append(json.load(stream))

    environments = [
        {field: str(report["HostEnvironmentInfo"][field]) for field in ENVIRONMENT_FIELDS}
        for report in reports
    ]
    if environments[0] != environments[1]:
        raise ValueError("cache and lock reports were produced by different environments")

    benchmarks: dict[str, dict[str, float]] = {}
    jobs: set[str] = set()
    for report in reports:
        for benchmark in report["Benchmarks"]:
            statistics = benchmark.get("Statistics")
            memory = benchmark.get("Memory")
            if statistics is None or memory is None:
                raise ValueError(f"benchmark has no measurements: {benchmark['FullName']}")
            jobs.add(benchmark["DisplayInfo"].split(": ", 1)[1])
            benchmarks[benchmark["FullName"]] = {
                "meanNanoseconds": float(statistics["Mean"]),
                "allocatedBytes": float(memory["BytesAllocatedPerOperation"]),
            }

    if len(jobs) != 1:
        raise ValueError("cache and lock reports used different BenchmarkDotNet jobs")
    return environments[0], jobs.pop(), benchmarks


def update_baseline(
    path: Path,
    environment: dict[str, str],
    job: str,
    benchmarks: dict[str, dict[str, float]],
) -> None:
    document = {
        "schemaVersion": 1,
        "regressionThreshold": 0.10,
        "environment": environment,
        "job": job,
        "benchmarks": dict(sorted(benchmarks.items())),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {path} with {len(benchmarks)} benchmarks.")


def check_baseline(
    path: Path,
    environment: dict[str, str],
    job: str,
    benchmarks: dict[str, dict[str, float]],
) -> int:
    if not path.is_file():
        raise ValueError(f"missing committed baseline: {path}")
    with path.open(encoding="utf-8") as stream:
        baseline = json.load(stream)

    if baseline.get("schemaVersion") != 1:
        raise ValueError("unsupported baseline schemaVersion")
    if baseline.get("environment") != environment:
        raise ValueError(
            "benchmark environment differs from the committed baseline; "
            "run on the baseline machine/runtime or deliberately update the baseline"
        )
    if baseline.get("job") != job:
        raise ValueError("BenchmarkDotNet job differs from the committed baseline")

    expected = baseline.get("benchmarks", {})
    missing = sorted(set(expected) - set(benchmarks))
    unexpected = sorted(set(benchmarks) - set(expected))
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        raise ValueError("benchmark set changed; update deliberately (" + "; ".join(details) + ")")

    threshold = float(baseline.get("regressionThreshold", 0.10))
    failures: list[str] = []
    for name, expected_metrics in expected.items():
        current_metrics = benchmarks[name]
        for metric, unit in (("meanNanoseconds", "ns"), ("allocatedBytes", "B")):
            old = float(expected_metrics[metric])
            new = float(current_metrics[metric])
            if old == 0:
                regressed = new > 0
                change = "0 -> " + f"{new:.2f}"
            else:
                ratio = new / old
                regressed = ratio > 1 + threshold
                change = f"{old:.2f} -> {new:.2f} ({(ratio - 1) * 100:+.1f}%)"
            if regressed:
                failures.append(f"{name} {metric}: {change} {unit}")

    if failures:
        print(f"Performance regressions above {threshold * 100:.0f}%:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(
        f"Cache/lock baseline passed: {len(benchmarks)} benchmarks, "
        f"maximum allowed regression {threshold * 100:.0f}%."
    )
    return 0


def main() -> int:
    args = parse_args()
    try:
        environment, job, benchmarks = load_reports(args.results)
        if args.update:
            update_baseline(args.baseline, environment, job, benchmarks)
            return 0
        return check_baseline(args.baseline, environment, job, benchmarks)
    except (
        IndexError,
        KeyError,
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exception:
        print(f"Benchmark baseline error: {exception}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
