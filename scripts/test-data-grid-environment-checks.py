#!/usr/bin/env python3
"""Negative qualification-evidence/ownership checks; never starts Docker or injects faults."""

import importlib.util
import json
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Protocol, cast
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "grid_environment", Path(__file__).with_name("test-data-grid-environment.py")
)
if SPEC is None or SPEC.loader is None:
    raise ImportError("Cannot load grid environment runner")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Runner(Protocol):
    IMAGE: str
    LABEL: str
    REQUIRED: set[str]

    def validate_matrix(self, xml: str, exit_code: int) -> dict[str, Any]: ...
    def inspect_owned(self, container: str, owner: str, cache: Path) -> None: ...


RUNNER = cast(Runner, MODULE)


def evidence(results: list[tuple[str, str, str, str]], *, total_override: int | None = None) -> str:
    root = ET.Element("assemblies")
    assembly = ET.SubElement(
        root,
        "assembly",
        total=str(len(results) if total_override is None else total_override),
        passed=str(sum(result == "Pass" for _, _, result, _ in results)),
        failed=str(sum(result == "Fail" for _, _, result, _ in results)),
        skipped=str(sum(result == "Skip" for _, _, result, _ in results)),
        errors="0",
    )
    collection = ET.SubElement(assembly, "collection")
    for name, method, result, reason in results:
        test = ET.SubElement(
            collection, "test", type="HostLoom.Tests." + name, method=method, result=result
        )
        if reason:
            ET.SubElement(test, "reason").text = reason
    return ET.tostring(root, encoding="unicode")


def passing() -> list[tuple[str, str, str, str]]:
    return [(name, "ExecutedCase", "Pass", "") for name in sorted(RUNNER.REQUIRED)]


class GridEnvironmentEvidenceTests(unittest.TestCase):
    def test_nonempty_complete_required_matrix_is_accepted(self) -> None:
        summary = RUNNER.validate_matrix(evidence(passing()), 0)
        self.assertEqual(len(RUNNER.REQUIRED), summary["passed"])
        self.assertEqual(0, summary["skipped"])

    def test_only_named_fault_and_long_capacity_exclusions_are_accepted(self) -> None:
        rows = passing() + [
            (
                "DataGridDiskFullTests",
                "ActualWalEnospcRecoversPublishedPrefixAndRetriesThroughFreshGeneration",
                "Skip",
                "Requires explicit owned bounded tmpfs disk-full fixture.",
            ),
            (
                "DataGridFeasibilityTests",
                "OwnedThreeProcessFeasibilityCase",
                "Skip",
                "Requires explicit owned feasibility runner.",
            ),
            (
                "DataGridSessionPressureTlsTests",
                "GlobalSessionPressureSurvivesCompactionRestartAndBoundedExpiry",
                "Skip",
                "Full TLS session-capacity qualification takes about eleven minutes.",
            ),
        ]
        summary = RUNNER.validate_matrix(evidence(rows), 0)
        self.assertEqual(3, summary["skipped"])
        self.assertEqual(3, len(summary["explicit_exclusions"]))

    def test_nested_legacy_skip_reason_is_supported_without_allowing_other_skips(self) -> None:
        xml = ET.fromstring(
            evidence(
                passing()
                + [
                    (
                        "DataGridDiskFullTests",
                        "Case",
                        "Skip",
                        "Requires explicit owned bounded tmpfs disk-full fixture.",
                    )
                ]
            )
        )
        reason = xml.find(".//reason")
        assert reason is not None
        text = reason.text
        reason.text = None
        ET.SubElement(reason, "message").text = text
        self.assertEqual(
            1, RUNNER.validate_matrix(ET.tostring(xml, encoding="unicode"), 0)["skipped"]
        )

    def test_zero_exit_does_not_allow_empty_incomplete_or_inconsistent_evidence(self) -> None:
        for xml in [evidence([]), evidence(passing()[1:]), evidence(passing(), total_override=999)]:
            with self.subTest(xml=xml), self.assertRaises(RuntimeError):
                RUNNER.validate_matrix(xml, 0)

    def test_failed_exit_or_failed_or_unexecuted_case_cannot_pass(self) -> None:
        for code, result in [(2, "Pass"), (0, "Fail"), (0, "NotRun")]:
            rows = passing()
            name, method, _, reason = rows[0]
            rows[0] = name, method, result, reason
            with self.subTest(code=code, result=result), self.assertRaises(RuntimeError):
                RUNNER.validate_matrix(evidence(rows), code)

    def test_required_process_and_tls_skips_are_never_qualification(self) -> None:
        for name in ["DataGridProcessTests", "DataGridCredentialTlsTests", "DataGridPeerTlsTests"]:
            rows = [row for row in passing() if row[0] != name]
            rows.append((name, "SkippedCase", "Skip", "No service or wrong operating system"))
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                RUNNER.validate_matrix(evidence(rows), 0)

    def test_wrong_skip_reason_or_unrelated_test_is_rejected(self) -> None:
        for name, result, reason in [
            ("DataGridDiskFullTests", "Skip", "Unrelated missing dependency"),
            ("OtherSubsystemTests", "Pass", ""),
        ]:
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                RUNNER.validate_matrix(evidence(passing() + [(name, "Case", result, reason)]), 0)

    def test_infrastructure_errors_fail_even_with_passing_test_counters(self) -> None:
        xml = ET.fromstring(evidence(passing()))
        ET.SubElement(ET.SubElement(xml[0], "errors"), "error")
        with self.assertRaises(RuntimeError):
            RUNNER.validate_matrix(ET.tostring(xml, encoding="unicode"), 0)


class GridEnvironmentOwnershipTests(unittest.TestCase):
    def test_profile_or_owner_substitution_is_rejected(self) -> None:
        cache = Path("/tmp/hostloom-grid-cache-check").resolve()
        original: dict[str, Any] = {
            "Id": "owned-id",
            "Name": "/hostloom-grid-environment-owned",
            "Config": {"Labels": {RUNNER.LABEL: "owned"}, "Image": RUNNER.IMAGE},
            "HostConfig": {
                "NetworkMode": "none",
                "Privileged": False,
                "CapDrop": ["ALL"],
                "SecurityOpt": ["no-new-privileges"],
                "Memory": 2 * 1024**3,
                "MemorySwap": 2 * 1024**3,
                "NanoCpus": 2_000_000_000,
                "PortBindings": {},
            },
            "Mounts": [{"Destination": "/cache", "Source": str(cache), "RW": False}],
        }
        with patch.object(MODULE, "run", return_value=json.dumps([original])):
            RUNNER.inspect_owned("owned-id", "owned", cache)
        for section, key, value in [
            ("Config", "Labels", {RUNNER.LABEL: "foreign"}),
            ("Config", "Image", "unselected-image"),
            ("HostConfig", "NetworkMode", "host"),
            ("HostConfig", "Privileged", True),
            ("HostConfig", "CapDrop", []),
            ("HostConfig", "SecurityOpt", []),
            ("HostConfig", "Memory", 0),
            ("HostConfig", "MemorySwap", -1),
            ("HostConfig", "PortBindings", {"7400/tcp": []}),
        ]:
            changed = json.loads(json.dumps(original))
            changed[section][key] = value
            with (
                self.subTest(section=section, key=key),
                patch.object(MODULE, "run", return_value=json.dumps([changed])),
                self.assertRaises(RuntimeError),
            ):
                RUNNER.inspect_owned("owned-id", "owned", cache)
        changed = json.loads(json.dumps(original))
        changed["Mounts"][0]["RW"] = True
        with (
            patch.object(MODULE, "run", return_value=json.dumps([changed])),
            self.assertRaises(RuntimeError),
        ):
            RUNNER.inspect_owned("owned-id", "owned", cache)


if __name__ == "__main__":
    unittest.main()
