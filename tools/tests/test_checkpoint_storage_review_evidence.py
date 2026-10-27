"""End-to-end error-path retention of independently parsed component events; generated evidence only."""
from contextlib import ExitStack
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def fixture_module(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("review_evidence_" + name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


events = fixture_module("test_checkpoint_storage_component_events")
launch = fixture_module("test_checkpoint_storage_check_launch")
check = launch.check


class AttemptEventRetentionTest(unittest.TestCase):
    def test_deleted_report_events_reject_attempt_but_preserve_independent_payload(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, unused = events.fixture(directory)
            result["evidence"]["componentErrors"] = events.reported([])
            with patch.object(check, "audit_attempt_contract") as contract:
                with self.assertRaisesRegex(ValueError, "incomplete or differs") as caught:
                    check.audit_attempt(result, 0, {"mode": "correct", "execution": "healthy"}, {}, directory)
                contract.assert_not_called()
            proof = caught.exception.component_event_audit
            self.assertEqual([events.EVENT], proof["events"])
            self.assertEqual(1, proof["eventCount"])
            self.assertEqual(0, proof["reportedEventCount"])
            self.assertEqual(3, len(proof["captures"]))
            self.assertFalse(proof["confirmed"])
            self.assertFalse(proof["consistent"])
            self.assertEqual("retained-final-prefix", proof["coverage"])
            self.assertFalse(proof["originalCaptureBoundaryKnown"])

    def test_later_state_contract_error_keeps_successful_independent_event_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, unused = events.fixture(directory)
            # Run the real state-contract gate after raw component parsing; no fake exception.
            result.update(scenario="foreign-scenario", diagnostics=[])
            manifest = {"workload": {"keyCount": 8, "totalRecords": 6000}}
            with self.assertRaisesRegex(ValueError, "Unexpected scenario") as caught:
                check.audit_attempt(result, 0, {"mode": "correct", "execution": "healthy"}, manifest, directory)
            proof = caught.exception.component_event_audit
            self.assertTrue(proof["confirmed"])
            self.assertTrue(proof["consistent"])
            self.assertEqual([events.EVENT], proof["events"])
            self.assertEqual(1, proof["eventCount"])
            self.assertEqual(3, len(proof["captures"]))

    def test_rest_count_error_after_component_parse_keeps_successful_event_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, unused = events.fixture(directory)
            result["evidence"]["flinkRest"]["errorCount"] = 1
            with self.assertRaisesRegex(ValueError, "REST error count") as caught:
                check.audit_attempt(result, 0, {"mode": "correct", "execution": "healthy"}, {}, directory)
            proof = caught.exception.component_event_audit
            self.assertTrue(proof["confirmed"])
            self.assertEqual([events.EVENT], proof["events"])
            self.assertEqual(1, proof["eventCount"])

    def test_changed_raw_log_bytes_cannot_be_accepted_by_a_successful_downstream_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, unused = events.fixture(directory)
            path = directory / "logs/flink-stability-output-taskmanager-1-2.log"
            path.write_bytes(b"startup complete\n")
            with patch.object(check, "audit_attempt_contract", return_value={"confirmed": True}) as contract:
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    check.audit_attempt(result, 0, {"mode": "correct", "execution": "healthy"}, {}, directory)
                contract.assert_not_called()


class MatrixEventRetentionTest(unittest.TestCase):
    def generate(self, root, changes):
        manifest = launch.FixedMatrixAllocationTest().generate(root)
        for number, relative in enumerate(path for cell in manifest["cells"] for path in cell["attempts"]):
            directory = root / relative
            raw = json.loads((directory / "stdout.json").read_bytes())
            event_result, unused = events.fixture(directory)
            raw["evidence"].update(event_result["evidence"])
            for ordinal, component in enumerate(raw["evidence"]["flinkRuntime"]["components"]):
                component["runtimeId"] = f"{number * 3 + ordinal + 1:064x}"
            if changes.get(number) == "deleted":
                raw["evidence"]["componentErrors"] = events.reported([])
            elif changes.get(number) == "rest":
                raw["evidence"]["flinkRest"]["errorCount"] = 1
            launch.put(directory / "stdout.json", raw)
        return manifest

    def run_matrix(self, root, manifest, changes):
        original_contract = check.audit_attempt_contract

        def contract(raw, code, cell, frozen, directory, retained, subject):
            allocation = [path for row in manifest["cells"] for path in row["attempts"]]
            number = allocation.index(str(directory.relative_to(root)))
            if changes.get(number) == "contract":
                raise ValueError("Generated later causal proof failure")
            if changes.get(number) == "rest":
                return original_contract(raw, code, cell, frozen, directory, retained, subject)
            return {"confirmed": True, **subject}

        with ExitStack() as stack:
            stack.enter_context(patch.object(check, "validate_manifest", return_value=manifest))
            stack.enter_context(patch.object(check, "validate_catalog_receipts", return_value=[
                {"cell": cell["name"], "startedAt": f"2026-01-01T00:00:{number * 2:02d}+00:00",
                 "finishedAt": f"2026-01-01T00:00:{number * 2 + 1:02d}+00:00", "elapsedSeconds": 1.0}
                for number, cell in enumerate(manifest["cells"])]))
            stack.enter_context(patch.object(check, "execution_receipts", return_value={}))
            stack.enter_context(patch.object(check, "audit_attempt_contract", side_effect=contract))
            return check.check_matrix(root)

    def test_error_rows_keep_independent_events_and_aggregate_all_observed_attempts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            changes = {0: "deleted", 1: "contract"}
            manifest = self.generate(root, changes)
            report = self.run_matrix(root, manifest, changes)
            self.assertFalse(report["qualified"])
            self.assertEqual(12, len(report["attempts"]))
            self.assertEqual(2, len(report["errors"]))
            self.assertEqual(10, sum(report["confirmedCounts"].values()))
            self.assertEqual(10, report["cleanupConfirmedCount"])
            self.assertEqual(11, report["componentEventCount"], "Original reported totals remain visibly different")
            self.assertEqual(12, report["retainedComponentEventCount"])
            self.assertEqual(12, report["componentEventAuditAttemptCount"])
            self.assertEqual(11, report["componentEventAuditConfirmedCount"])
            omitted, other_failure = report["attempts"][:2]
            self.assertFalse(omitted["confirmed"])
            self.assertFalse(other_failure["confirmed"])
            self.assertIn("incomplete or differs", omitted["error"])
            self.assertEqual("Generated later causal proof failure", other_failure["error"])
            for row in report["attempts"]:
                self.assertEqual([events.EVENT], row["componentEventAudit"]["events"])
                self.assertEqual(1, row["componentEventAudit"]["eventCount"])
                self.assertEqual(3, len(row["componentEventAudit"]["captures"]))
            self.assertFalse(omitted["componentEventAudit"]["confirmed"])
            self.assertTrue(other_failure["componentEventAudit"]["confirmed"])
            self.assertEqual([], omitted["componentErrors"]["events"])
            self.assertEqual([events.EVENT], other_failure["componentErrors"]["events"])

    def test_rest_gate_error_does_not_reduce_retained_event_audit_totals(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            changes = {0: "rest"}
            manifest = self.generate(root, changes)
            report = self.run_matrix(root, manifest, changes)
            self.assertFalse(report["qualified"])
            self.assertEqual(1, len(report["errors"]))
            self.assertEqual(11, sum(report["confirmedCounts"].values()))
            self.assertEqual(12, report["retainedComponentEventCount"])
            self.assertEqual(12, report["componentEventAuditAttemptCount"])
            self.assertEqual(12, report["componentEventAuditConfirmedCount"])
            self.assertEqual([events.EVENT], report["attempts"][0]["componentEventAudit"]["events"])
            self.assertIn("REST error count", report["attempts"][0]["error"])


if __name__ == "__main__":
    unittest.main()
