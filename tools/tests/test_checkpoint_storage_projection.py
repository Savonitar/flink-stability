"""Compact CLI projections are checked against complete retained native journals."""
from collections import Counter
from copy import deepcopy
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("storage_projection_fixture", Path(__file__).with_name("test_checkpoint_storage_calibration_check.py"))
fixture_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture_module)
audit = fixture_module.audit


def compact(result):
    evidence = result["evidence"]
    snapshots = [evidence["checkpointStorage"]]
    snapshots += [sample["storage"] for fault in evidence.get("checkpointStorageFaults", []) for sample in fault["samples"]]
    for snapshot in snapshots:
        paths = {file["path"].removeprefix("/flink/checkpoints/") for file in snapshot["materializedFiles"]}
        for instance in snapshot["instances"]:
            operations = instance["operations"]
            counts = Counter((row["operation"], row["state"], row["errno"]) for row in operations)
            instance.update(operationsRepresentation="bound-subset-v1", operationCount=len(operations),
                            operationCounts=[dict(operation=op, state=state, errno=errno, count=count)
                                             for (op, state, errno), count in sorted(counts.items())])
            instance["operations"] = [row for row in operations if row["operation"].isupper() and row["operation"] != "STATUS"
                                      or row["errno"] == 5 and row["operation"] in ("create", "write")
                                      or row["path"] in paths and row["operation"] in ("read", "write", "create")]


class JournalProjectionTest(unittest.TestCase):
    def test_complete_journals_prove_compact_projection_without_mutating_input(self):
        for mode, execution in (("correct", "healthy"), ("correct", "restart"), ("correct", "eio"), ("mutant", "eio")):
            with self.subTest(mode=mode, execution=execution), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp).resolve()
                result, unused, cell, manifest = fixture_module.fixture(directory, mode, execution)
                compact(result)
                original = deepcopy(result)
                for repeat in range(2):
                    report = audit.audit_storage(result, cell, manifest, audit.RetainedEvidence(directory))
                    self.assertTrue(report["complete"])
                    self.assertEqual(original, result)

    def test_deleted_bound_receipt_counts_or_unknown_representation_fail(self):
        changes = {
            "omitted receipt": lambda item: item["operations"].pop(),
            "changed total": lambda item: item.update(operationCount=30001),
            "changed counts": lambda item: item["operationCounts"][0].update(count=999),
            "boolean count": lambda item: item["operationCounts"][0].update(count=True),
            "unknown representation": lambda item: item.update(operationsRepresentation="unchecked"),
        }
        for name, change in changes.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp).resolve()
                result, unused, cell, manifest = fixture_module.fixture(directory)
                compact(result)
                change(result["evidence"]["checkpointStorage"]["instances"][0])
                with self.assertRaises(ValueError):
                    audit.audit_storage(result, cell, manifest, audit.RetainedEvidence(directory))

    def test_changed_inline_errno_cannot_hide_raw_eio(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp).resolve()
            result, unused, cell, manifest = fixture_module.fixture(directory)
            compact(result)
            item = next(item for item in result["evidence"]["checkpointStorage"]["instances"]
                        if any(row["errno"] == 5 for row in item["operations"]))
            next(row for row in item["operations"] if row["errno"] == 5)["errno"] = 0
            with self.assertRaisesRegex(ValueError, "subset differs"):
                audit.audit_storage(result, cell, manifest, audit.RetainedEvidence(directory))


if __name__ == "__main__":
    unittest.main()
