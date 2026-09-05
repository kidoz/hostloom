#!/usr/bin/env python3
"""Enforce reviewed, machine-specific composition phase and allocation budgets."""

import argparse
import json
import math
from pathlib import Path
from typing import Any


def check(result: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    if result.get("schemaVersion") != 1 or baseline.get("schemaVersion") != 1:
        raise ValueError("Unsupported measurement schema")
    for key in ("environment", "method"):
        if result.get(key) != baseline.get(key):
            raise ValueError(f"Incomparable {key}; rerun on the documented reference environment")
    if set(result["cases"]) != set(baseline["budgets"]):
        raise ValueError("Missing or additional measurement cases")
    if not result.get("trackedReasons") or not set(result["trackedReasons"]) <= {
        "Cached",
        "Unchanged",
    }:
        raise ValueError("Unrelated edits did not reuse source output")
    failures = []
    for name, ceilings in baseline["budgets"].items():
        case = result["cases"][name]
        for metric, ceiling in ceilings.items():
            field, statistic = metric.split("/")
            value = case[field][statistic]
            if not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"Invalid numeric result for {name}: {metric}")
            if value > ceiling:
                failures.append(f"{name} {metric}: {value:.3f} > {ceiling:.3f}")
    if set(result.get("consumerBuilds", {})) != {"46", "160", "1000"}:
        raise ValueError("Missing paired consumer builds")
    for size, ceiling in baseline["consumerBuildBudgets"].items():
        value = result["consumerBuilds"][size]["addedNanoseconds"]["median"]
        if not isinstance(value, (float, int)) or not math.isfinite(value):
            raise ValueError("Invalid paired build result")
        if value > ceiling:
            failures.append(f"consumer/{size} median added ns: {value:.3f} > {ceiling:.3f}")
    target = result["method"]["generatorTargetNanoseconds"]
    added_p95 = result["consumerBuilds"]["1000"]["addedNanoseconds"]["p95"]
    if not isinstance(added_p95, (float, int)) or not math.isfinite(added_p95):
        raise ValueError("Invalid paired build p95")
    if added_p95 > target:
        failures.append("1,000-candidate paired build p95 exceeds the 200 ms target")
    if result["cases"]["generator/1000/fresh-driver"]["nanoseconds"]["p95"] > target:
        failures.append("1,000-candidate fresh-driver p95 exceeds the 200 ms target")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument(
        "--baseline", type=Path, default=Path("benchmarks/baselines/composition.json")
    )
    args = parser.parse_args()
    failures = check(json.loads(args.results.read_text()), json.loads(args.baseline.read_text()))
    if failures:
        raise SystemExit("Composition regression budget exceeded:\n" + "\n".join(failures))
    print("Composition performance budgets passed.")


if __name__ == "__main__":
    main()
