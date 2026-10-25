#!/usr/bin/env python3
"""Negative checks for chaos evidence validation; never starts Docker or injects faults."""

import importlib.util
import unittest
from pathlib import Path
from typing import Protocol, cast

SPEC = importlib.util.spec_from_file_location(
    "chaos_runner", Path(__file__).with_name("test-redis-cluster.py")
)
if SPEC is None or SPEC.loader is None:
    raise ImportError("Cannot load Redis fixture runner")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Runner(Protocol):
    def normalize_method_filter(self, method: str) -> str: ...
    def validate_test_result(self, exit_code: int, output: str) -> None: ...


RUNNER = cast(Runner, MODULE)


class ChaosRunnerTests(unittest.TestCase):
    def test_bare_methods_match_the_qualified_xunit_name(self) -> None:
        self.assertEqual(
            "*SingleEndpointOnly_*", RUNNER.normalize_method_filter("SingleEndpointOnly_*")
        )
        self.assertEqual("*Method", RUNNER.normalize_method_filter("Method"))
        for pattern in ["*", "*Method", "Namespace.Type.Method"]:
            self.assertEqual(pattern, RUNNER.normalize_method_filter(pattern))

    def test_real_nonempty_pass_is_accepted(self) -> None:
        RUNNER.validate_test_result(0, "  total: 5\n  failed: 0\n  succeeded: 5\n  skipped: 0\n")

    def test_zero_tests_never_pass_even_with_a_zero_exit_code(self) -> None:
        for code in [0, 8]:
            with self.subTest(code=code), self.assertRaises(RuntimeError):
                RUNNER.validate_test_result(code, "  total: 0\n  skipped: 0\n")

    def test_failure_and_missing_or_skipped_evidence_are_rejected(self) -> None:
        for code, output in [
            (2, "  total: 1\n  skipped: 0\n"),
            (0, "  total: 5\n  skipped: 1\n"),
            (0, "  total: 5\n"),
            (0, ""),
            (0, "  total: 1\n  failed: 1\n  succeeded: 0\n  skipped: 0\n"),
            (0, "  total: 5\n  failed: 0\n  succeeded: 4\n  skipped: 0\n"),
        ]:
            with self.subTest(code=code, output=output), self.assertRaises(RuntimeError):
                RUNNER.validate_test_result(code, output)


if __name__ == "__main__":
    unittest.main()
