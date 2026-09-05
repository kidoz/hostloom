#!/usr/bin/env python3
"""Deterministic negative checks for the performance gate; does not run benchmarks."""

import copy
import importlib.util
import json
import unittest
from pathlib import Path
from typing import Any, Protocol, cast

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "gate", ROOT / "scripts/check-composition-baseline.py"
)
if SPEC is None or SPEC.loader is None:
    raise ImportError("Cannot load the composition budget checker")
GATE_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE_MODULE)


class BudgetChecker(Protocol):
    def check(self, result: dict[str, Any], baseline: dict[str, Any]) -> list[str]: ...


GATE = cast(BudgetChecker, GATE_MODULE)


class BudgetGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.baseline = json.loads((ROOT / "benchmarks/baselines/composition.json").read_text())
        self.result = copy.deepcopy(self.baseline)

    def test_reference_passes(self) -> None:
        self.assertEqual([], GATE.check(self.result, self.baseline))

    def test_regressions_in_time_and_allocation_are_detected(self) -> None:
        for metric in ("nanoseconds", "allocatedBytes"):
            with self.subTest(metric=metric):
                changed = copy.deepcopy(self.result)
                name = "runtime/total/steady"
                changed["cases"][name][metric]["median"] = (
                    self.baseline["budgets"][name][metric + "/median"] + 1
                )
                self.assertTrue(GATE.check(changed, self.baseline))

    def test_environment_and_missing_cases_are_rejected(self) -> None:
        self.result["environment"]["cpu"] = "Different CPU"
        with self.assertRaises(ValueError):
            GATE.check(self.result, self.baseline)
        self.result = copy.deepcopy(self.baseline)
        del self.result["cases"]["runtime/probe/steady"]
        with self.assertRaises(ValueError):
            GATE.check(self.result, self.baseline)

    def test_invalid_incremental_and_nonfinite_data_are_rejected(self) -> None:
        self.result["trackedReasons"] = ["Modified"]
        with self.assertRaises(ValueError):
            GATE.check(self.result, self.baseline)
        self.result = copy.deepcopy(self.baseline)
        self.result["cases"]["runtime/probe/steady"]["nanoseconds"]["median"] = float("nan")
        with self.assertRaises(ValueError):
            GATE.check(self.result, self.baseline)

    def test_consumer_build_budget_is_enforced(self) -> None:
        self.result["consumerBuilds"]["1000"]["addedNanoseconds"]["median"] = (
            self.baseline["consumerBuildBudgets"]["1000"] + 1
        )
        self.assertTrue(GATE.check(self.result, self.baseline))

    def test_generator_target_includes_consumer_tail(self) -> None:
        self.result["consumerBuilds"]["1000"]["addedNanoseconds"]["p95"] = 200000001
        self.assertTrue(GATE.check(self.result, self.baseline))


if __name__ == "__main__":
    unittest.main()
