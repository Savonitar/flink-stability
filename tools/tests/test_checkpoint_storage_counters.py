"""Generated complete journal captures for aggregate IO and STATUS semantics."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("storage_counter_audit", Path(__file__).absolute().parents[2] / "calibration/checkpoint-storage/audit.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def fixture(directory, change=None):
    identity = dict(instanceId="mount", daemonPid=42, runtimeId="container", process="taskmanager-1#1",
                    logicalName="taskmanager-1", role="TASK_MANAGER")
    storage = dict(attemptId="attempt", storageId="storage", namespace="namespace", materializedFiles=[])
    zero = {op: {"count": 0, "bytes": 0} for op in ("read", "getattr", "readdir", "access", "flush", "release")}
    summary = deepcopy(zero)
    summary["read"] = dict(count=1, bytes=16)
    summary["getattr"]["count"] = 1000
    rows = []
    for sequence, operation in enumerate(("START", "READY", "read", "SUMMARY"), 1):
        row = dict(version=1, sequence=sequence, timestampMs=sequence, monotonicNs=sequence,
                   attemptId="attempt", storageId="storage", namespace="namespace", instanceId="mount", daemonPid=42,
                   state="STARTING" if sequence == 1 else "HEALTHY", journalComplete=True,
                   faultId=None, revision=0, leaseMs=0, leaseDeadlineMonotonicNs=0, allowedOperations=[],
                   operation=operation, path="namespace/job/chk-1/state" if operation == "read" else ".",
                   faultOperation=None, result=16 if operation == "read" else 0, errno=0,
                   requestedBytes=16 if operation == "read" else 0, returnedBytes=16 if operation == "read" else 0,
                   datasync=None, reason=audit.READY if operation == "READY" else "ok")
        if operation != "read":
            row["counters"] = deepcopy(summary if operation == "SUMMARY" else zero)
        rows.append(row)
    status = {key: rows[-1][key] for key in ("state", "faultId", "revision", "sequence", "leaseMs", "leaseDeadlineMonotonicNs", "journalComplete")}
    status.update(stopping=False, counters=deepcopy(summary))
    if change:
        change(rows, status)
    data = b"".join((json.dumps(row, sort_keys=True) + "\n").encode() for row in rows)
    (directory / "journal.jsonl").write_bytes(data)
    sha = audit.sha(data)
    (directory / "retained-evidence.json").write_text(json.dumps(dict(version=1, files=[dict(
        sourcePath="/generated/journal.jsonl", localPath="journal.jsonl", sha256=sha, kind="journal")])))
    projected = []
    for row in rows:
        value = {key: row[key] for key in audit.PROJECTED}
        value.update(instance=identity, timestampMillis=row["timestampMs"], monotonicNanos=row["monotonicNs"],
                     counters=row.get("counters", {}), pathTruncated=False)
        projected.append(value)
    instance = dict(journalPath="/generated/journal.jsonl", journalSha256=sha, journalBytes=len(data),
                    lastSequence=len(rows), instance=identity, operations=projected, status=status)
    return instance, storage


class AggregateCounterTest(unittest.TestCase):
    def test_status_does_not_need_its_own_journal_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp).resolve()
            instance, storage = fixture(directory)
            data, rows = audit.RetainedEvidence(directory).journal(instance, storage)
            self.assertEqual(4, len(rows))
            self.assertEqual("SUMMARY", rows[-1]["operation"])
            self.assertEqual(1000, instance["status"]["counters"]["getattr"]["count"])

    def test_rehashed_inconsistent_counters_are_rejected(self):
        changes = {
            "missing summary counters": lambda rows, status: rows[-1].pop("counters"),
            "nonzero startup": lambda rows, status: rows[0]["counters"]["read"].update(count=1),
            "metadata bytes": lambda rows, status: rows[-1]["counters"]["getattr"].update(bytes=8),
            "bytes without reads": lambda rows, status: rows[-1]["counters"]["read"].update(count=0),
            "boolean count": lambda rows, status: rows[-1]["counters"]["read"].update(count=True),
            "status behind": lambda rows, status: status["counters"]["getattr"].update(count=999),
            "missing status": lambda rows, status: status.pop("counters"),
        }
        for name, change in changes.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp).resolve()
                instance, storage = fixture(directory, change)
                with self.assertRaises(ValueError):
                    audit.RetainedEvidence(directory).journal(instance, storage)


if __name__ == "__main__":
    unittest.main()
