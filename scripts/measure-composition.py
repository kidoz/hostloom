#!/usr/bin/env python3
"""Measure composition phases in fresh processes; write raw samples and compact distributions.

Run on an idle reference machine. The benchmark executable must already be built in Release.
This script never updates the reviewed baseline. Use check-composition-baseline.py to enforce it.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

RUNTIME_CASES = (
    "plan",
    "apply",
    "probe",
    "total",
    "handwritten",
    "scrutor",
    "ledger-record",
    "ledger-report",
    "total-ledger",
)


def distribution(samples: Sequence[float]) -> dict[str, float]:
    values = sorted(samples)
    return {
        "median": statistics.median(values),
        "p95": values[math.ceil(len(values) * 0.95) - 1],
        "min": values[0],
        "max": values[-1],
        "samples": len(values),
    }


def summarize(samples: Sequence[Mapping[str, float]]) -> dict[str, dict[str, float]]:
    return {
        "nanoseconds": distribution([x["Nanoseconds"] for x in samples]),
        "allocatedBytes": distribution([x["AllocatedBytes"] for x in samples]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    args.output.mkdir(parents=True, exist_ok=True)
    executable = (
        repo
        / "benchmarks/HostLoom.Composition.Benchmarks/bin/Release/net10.0/HostLoom.Composition.Benchmarks.dll"
    )
    env = dict(os.environ, DOTNET_TieredCompilation="0", DOTNET_ReadyToRun="1")

    def run(arguments: Sequence[str]) -> dict[str, Any]:
        value = subprocess.run(
            ["dotnet", str(executable), *arguments],
            cwd=repo,
            env=env,
            text=True,
            capture_output=True,
            timeout=180,
        )
        if value.returncode:
            raise RuntimeError(value.stdout + value.stderr)
        return json.loads(value.stdout)

    verification = run(["verify"])
    (args.output / "verification.json").write_text(json.dumps(verification, indent=2) + "\n")
    cases: dict[str, dict[str, dict[str, float]]] = {}
    environment: dict[str, Any] = {}
    for name in RUNTIME_CASES:
        print("runtime " + name, flush=True)
        runs = [run([name]) for _ in range(5)]
        (args.output / (name + ".raw.json")).write_text(json.dumps(runs, indent=2) + "\n")
        environment = runs[0]["environment"]
        cases["runtime/" + name + "/first-call"] = summarize([x["cold"] for x in runs])
        cases["runtime/" + name + "/steady"] = summarize(
            [sample for x in runs for sample in x["samples"]]
        )
    reasons = set()
    for size in (46, 160, 1000):
        print("generator " + str(size), flush=True)
        runs = [run(["generator", str(size)]) for _ in range(5)]
        (args.output / ("generator-" + str(size) + ".raw.json")).write_text(
            json.dumps(runs, indent=2) + "\n"
        )
        cases[f"generator/{size}/process-first"] = summarize([x["first"] for x in runs])
        for phase in ("fresh-driver", "unchanged", "unrelated-edit", "rule-edit"):
            cases[f"generator/{size}/{phase}"] = summarize(
                [sample for x in runs for sample in x["cases"][phase]]
            )
        for result in runs:
            reasons.update(result["trackedReasons"])
    build_pairs = {}
    for size in (46, 160, 1000):
        print("paired consumer builds " + str(size), flush=True)
        directory = args.output.resolve() / ("consumer-" + str(size))
        subprocess.run(
            ["dotnet", str(executable), "export", str(size), str(directory)],
            cwd=repo,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        (directory / "global.json").write_text((repo / "global.json").read_text())
        for mode in ("handwritten", "generated"):
            restored = subprocess.run(
                ["dotnet", "restore", "Consumer.csproj"],
                cwd=directory / mode,
                env=env,
                capture_output=True,
                text=True,
                timeout=180,
            )
            (directory / (mode + "-restore.log")).write_text(restored.stdout + restored.stderr)
            if restored.returncode:
                raise RuntimeError("Consumer restore failed: " + str(directory))
        pairs = []
        for launch in range(5):
            pair = {}
            for mode in (
                ("handwritten", "generated") if launch % 2 == 0 else ("generated", "handwritten")
            ):
                started = time.perf_counter_ns()
                built = subprocess.run(
                    [
                        "dotnet",
                        "build",
                        "Consumer.csproj",
                        "-c",
                        "Release",
                        "--no-restore",
                        "-t:Rebuild",
                        "-p:UseSharedCompilation=false",
                    ],
                    cwd=directory / mode,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=180,
                )
                pair[mode] = time.perf_counter_ns() - started
                (directory / (mode + "-" + str(launch) + ".log")).write_text(
                    built.stdout + built.stderr
                )
                if built.returncode:
                    raise RuntimeError("Consumer build failed: " + str(directory))
            pair["added"] = pair["generated"] - pair["handwritten"]
            pairs.append(pair)
        build_pairs[str(size)] = {
            "pairs": pairs,
            "addedNanoseconds": distribution([pair["added"] for pair in pairs]),
            "generatedNanoseconds": distribution([pair["generated"] for pair in pairs]),
            "handwrittenNanoseconds": distribution([pair["handwritten"] for pair in pairs]),
        }
    cpu = platform.processor()
    if platform.system() == "Darwin":
        cpu = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
    elif Path("/proc/cpuinfo").exists():
        cpu = next(
            (
                line.split(":", 1)[1].strip()
                for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")
            ),
            cpu,
        )
    environment.update(
        cpu=cpu,
        sdk=subprocess.check_output(["dotnet", "--version"], cwd=repo, text=True).strip(),
        tieredCompilation=False,
        readyToRun=True,
        scrutor="7.0.0",
    )
    digest = hashlib.sha256()
    for directory in (
        "src/HostLoom.Composition",
        "src/HostLoom.Composition.Generators",
        "benchmarks/HostLoom.Composition.Benchmarks",
    ):
        for path in sorted((repo / directory).rglob("*.cs")):
            if {"bin", "obj"}.intersection(path.parts):
                continue
            digest.update(path.relative_to(repo).as_posix().encode())
            digest.update(path.read_bytes())
    result = {
        "schemaVersion": 1,
        "environment": environment,
        "method": {
            "launches": 5,
            "samplesPerLaunch": 15,
            "runtimeIterations": 32,
            "probeIterations": 100000,
            "warmupCalls": 64,
            "generatorTargetNanoseconds": 200000000,
        },
        "sourceSha256": digest.hexdigest(),
        "consumerBuilds": build_pairs,
        "trackedReasons": sorted(reasons),
        "cases": cases,
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(args.output / "summary.json", flush=True)


if __name__ == "__main__":
    main()
