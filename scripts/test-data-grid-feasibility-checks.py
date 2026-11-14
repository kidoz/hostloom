#!/usr/bin/env python3
"""Check feasibility planning and measurement evidence without Docker or load."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Protocol, cast

SPEC = importlib.util.spec_from_file_location(
    "grid_feasibility", Path(__file__).with_name("run-data-grid-feasibility.py")
)
if SPEC is None or SPEC.loader is None:
    raise ImportError("Cannot load feasibility runner")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Runner(Protocol):
    SEEDS: list[int]

    def memory_profile(self, bounded: bool) -> dict[str, str | int | None]: ...

    def scenario(
        self, batch: int, concurrency: int, seed: int, *, smoke: bool
    ) -> dict[str, int]: ...
    def nearest_rank(self, values: list[float], percentile: float) -> float | None: ...
    def validate_measurement(
        self,
        directory: Path,
        expected: dict[str, int],
        xml: str,
        code: int,
        *,
        bounded_memory: bool = False,
    ) -> dict[str, Any]: ...


RUNNER = cast(Runner, MODULE)
XML = '<assemblies><assembly total="1" passed="1" failed="0" skipped="0" errors="0"><collection><test type="HostLoom.Tests.DataGridFeasibilityTests" result="Pass" /></collection></assembly></assemblies>'


def files(directory: Path) -> dict[str, int]:
    spec = RUNNER.scenario(1, 1, 17, smoke=True)
    location = directory / "feasibility"
    location.mkdir()
    row = {
        "worker": 0,
        "request_number": 1,
        "operation": "write",
        "digest": "abc",
        "duration_us": 10,
        "attempts": 1,
        "items": 1,
        "within_window": True,
        "outcome": "completed",
        "item_failures": 0,
        "logical_duration_us": 12,
        "logical_completed_here": True,
        "originated_here": True,
        "may_have_been_admitted": True,
    }
    quantiles = {"count": 1, "p50": 10, "p95": 10, "p99": 10}
    summary = {
        "invocations": 1,
        "successful_within_window": 1,
        "late_or_censored": 0,
        "request_throughput": 1,
        "unknown": 0,
        "unknown_rate": 0,
        "refused": 0,
        "refusal_rate": 0,
        "errors": 0,
        "error_rate": 0,
        "sdk_timeouts": 0,
        "sdk_timeout_rate": 0,
        "successful_call_us": quantiles,
        "all_completed_call_us": quantiles,
        "logical_write_us": {"count": 1, "p50": 12, "p95": 12, "p99": 12},
    }
    report = {
        "case": spec,
        "is_p0": False,
        "status": "completed",
        "process_memory_profile": RUNNER.memory_profile(False),
        "recovery_verified": True,
        "unresolved_mutations": 0,
        "warmup": summary,
        "sample": {**summary, "request_throughput": 0.2},
    }
    (location / "case.json").write_text(json.dumps(report))
    for phase in ["warmup", "sample"]:
        (location / f"{phase}-calls.jsonl").write_text(json.dumps(row) + "\n")
    return spec


class FeasibilityEvidenceTests(unittest.TestCase):
    def test_bounded_profile_applies_each_process_budget_without_increasing_container(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            spec = files(directory)
            path = directory / "feasibility/case.json"
            report = json.loads(path.read_text())
            profile = RUNNER.memory_profile(True)
            self.assertEqual(
                2 * 1024**3,
                cast(int, profile["driver_heap_limit_bytes"])
                + 3 * cast(int, profile["member_heap_limit_bytes"])
                + cast(int, profile["remaining_container_bytes"]),
            )
            report["process_memory_profile"] = profile
            report["runtime"] = {"total_available_memory_bytes": 192 * 1024**2}
            path.write_text(json.dumps(report))
            resources = directory / "feasibility/resources.jsonl"
            rows: dict[str, Any] = {
                "members": [
                    {"member": i, "managed": {"total_available_memory_bytes": 448 * 1024**2}}
                    for i in range(3)
                ]
            }
            resources.write_text(json.dumps(rows) + "\n")
            self.assertEqual(
                "completed",
                RUNNER.validate_measurement(directory, spec, XML, 0, bounded_memory=True)[
                    "case_status"
                ],
            )
            with self.assertRaises(RuntimeError):
                RUNNER.validate_measurement(directory, spec, XML, 0)
            report["runtime"] = {"total_available_memory_bytes": 1536 * 1024**2}
            path.write_text(json.dumps(report))
            with self.assertRaises(RuntimeError):
                RUNNER.validate_measurement(directory, spec, XML, 0, bounded_memory=True)
            report["runtime"] = {"total_available_memory_bytes": 192 * 1024**2}
            path.write_text(json.dumps(report))
            rows["members"][1]["managed"] = {"total_available_memory_bytes": 1536 * 1024**2}
            resources.write_text(json.dumps(rows) + "\n")
            with self.assertRaises(RuntimeError):
                RUNNER.validate_measurement(directory, spec, XML, 0, bounded_memory=True)
            rows["members"][1]["managed"] = {"total_available_memory_bytes": 448 * 1024**2}
            rows["members"].pop()
            resources.write_text(json.dumps(rows) + "\n")
            with self.assertRaises(RuntimeError):
                RUNNER.validate_measurement(directory, spec, XML, 0, bounded_memory=True)

    def test_full_plan_has_27_distinct_coordinates_and_three_seeds(self) -> None:
        plan = [
            RUNNER.scenario(b, c, s, smoke=False)
            for b in [1, 32, 128]
            for c in [1, 16, 64]
            for s in RUNNER.SEEDS
        ]
        self.assertEqual(27, len({json.dumps(case, sort_keys=True) for case in plan}))
        self.assertTrue(
            all(
                case["key_count"] == 100000
                and case["warmup_seconds"] == 60
                and case["sample_seconds"] == 300
                for case in plan
            )
        )
        self.assertEqual(256, RUNNER.scenario(1, 1, 17, smoke=True)["key_count"])
        with self.assertRaises(ValueError):
            RUNNER.scenario(129, 1, 17, smoke=False)

    def test_individual_nearest_rank_and_empty_sample(self) -> None:
        self.assertIsNone(RUNNER.nearest_rank([], 0.99))
        self.assertEqual(3, RUNNER.nearest_rank([5, 4, 3, 2, 1], 0.5))
        self.assertEqual(5, RUNNER.nearest_rank([5, 4, 3, 2, 1], 0.95))

    def test_raw_accounted_smoke_is_execution_not_numerical_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            spec = files(directory)
            result = RUNNER.validate_measurement(directory, spec, XML, 0)
            self.assertEqual("completed", result["case_status"])
            self.assertEqual("no_numerical_targets", result["acceptance"])
            self.assertFalse(result["is_p0"])

    def test_empty_or_skipped_driver_or_failed_process_never_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            spec = files(directory)
            for xml, code in [
                ("<assemblies/>", 0),
                (XML.replace('result="Pass"', 'result="Skip"'), 0),
                (XML, 1),
                (XML.replace('total="1"', 'total="2"'), 0),
                (XML.replace('errors="0"', 'errors="1"'), 0),
            ]:
                with self.subTest(xml=xml, code=code), self.assertRaises(RuntimeError):
                    RUNNER.validate_measurement(directory, spec, xml, code)

    def test_infeasible_population_does_not_invent_a_sample_or_workload_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            files(directory)
            expected = RUNNER.scenario(128, 1, 43, smoke=False)
            path = directory / "feasibility/case.json"
            report = json.loads(path.read_text())
            report.update(case=expected, is_p0=True, status="infeasible")
            for phase in ["warmup", "sample"]:
                del report[phase]
                (directory / f"feasibility/{phase}-calls.jsonl").unlink()
            path.write_text(json.dumps(report))
            result = RUNNER.validate_measurement(directory, expected, XML, 0)
            self.assertTrue(result["driver_passed"])
            self.assertEqual("infeasible", result["case_status"])
            self.assertNotIn("sample", result["result"])

    def test_summary_percentile_or_throughput_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            spec = files(directory)
            path = directory / "feasibility/case.json"
            original = json.loads(path.read_text())
            for key, value in [
                ("request_throughput", 999),
                ("late_or_censored", 99),
                ("invocations", 0),
                ("unknown_rate", 1),
                ("refused", 1),
                ("error_rate", 1),
                ("sdk_timeouts", 2),
            ]:
                damaged = json.loads(json.dumps(original))
                damaged["sample"][key] = value
                path.write_text(json.dumps(damaged))
                with self.subTest(key=key), self.assertRaises(RuntimeError):
                    RUNNER.validate_measurement(directory, spec, XML, 0)
            original["sample"]["successful_call_us"]["p95"] = 999
            path.write_text(json.dumps(original))
            with self.assertRaises(RuntimeError):
                RUNNER.validate_measurement(directory, spec, XML, 0)

    def test_retry_digest_change_across_windows_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            spec = files(directory)
            path = directory / "feasibility/sample-calls.jsonl"
            row = json.loads(path.read_text())
            row["digest"] = "different"
            path.write_text(json.dumps(row) + "\n")
            with self.assertRaises(RuntimeError):
                RUNNER.validate_measurement(directory, spec, XML, 0)

    def test_unresolved_recovery_or_smoke_promoted_to_p0_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            spec = files(directory)
            path = directory / "feasibility/case.json"
            original = json.loads(path.read_text())
            for key, value in [
                ("recovery_verified", False),
                ("is_p0", True),
                ("unresolved_mutations", 1),
            ]:
                damaged = {**original, key: value}
                path.write_text(json.dumps(damaged))
                with self.subTest(key=key), self.assertRaises(RuntimeError):
                    RUNNER.validate_measurement(directory, spec, XML, 0)


if __name__ == "__main__":
    unittest.main()
