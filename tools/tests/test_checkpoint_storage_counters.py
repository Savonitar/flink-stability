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


def fixture(directory, change=None, schema=6):
    identity = dict(instanceId="mount", daemonPid=42, runtimeId="container", process="taskmanager-1#1",
                    logicalName="taskmanager-1", role="TASK_MANAGER")
    storage = dict(attemptId="attempt", storageId="storage", namespace="namespace", materializedFiles=[])
    zero = {op: {"count": 0, "bytes": 0} for op in ("read", "getattr", "readdir", "access", "flush", "release")}
    if schema == 8:
        zero.update(fsync=dict(count=0, bytes=0), fdatasync=dict(count=0, bytes=0))
    summary = deepcopy(zero)
    summary["read"] = dict(count=1, bytes=16)
    summary["getattr"]["count"] = 1000
    if schema == 8:
        summary["fsync"]["count"] = 3
        summary["fdatasync"]["count"] = 2
    rows = []
    operations = ("START", "READY", "read", "SUMMARY") if schema else ("START", "READY", "read")
    for sequence, operation in enumerate(operations, 1):
        row = dict(version=1, sequence=sequence, timestampMs=sequence, monotonicNs=sequence,
                   attemptId="attempt", storageId="storage", namespace="namespace", instanceId="mount", daemonPid=42,
                   state="STARTING" if sequence == 1 else "HEALTHY", journalComplete=True,
                   faultId=None, revision=0, leaseMs=0, leaseDeadlineMonotonicNs=0, allowedOperations=[],
                   operation=operation, path="namespace/job/chk-1/state" if operation == "read" else ".",
                   faultOperation=None, result=16 if operation == "read" else 0, errno=0,
                   requestedBytes=16 if operation == "read" else 0, returnedBytes=16 if operation == "read" else 0,
                   datasync=None, reason=audit.READY if operation == "READY" else "ok")
        if schema and operation != "read":
            row["counters"] = deepcopy(summary if operation == "SUMMARY" else zero)
        rows.append(row)
    status = {key: rows[-1][key] for key in ("state", "faultId", "revision", "sequence", "leaseMs", "leaseDeadlineMonotonicNs", "journalComplete")}
    status.update(stopping=False)
    if schema:
        status["counters"] = deepcopy(summary)
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
    def test_historical_absence_legacy_six_and_current_eight_are_distinct_valid_formats(self):
        for schema in (0, 6, 8):
            with self.subTest(schema=schema), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp).resolve()
                instance, storage = fixture(directory, schema=schema)
                data, rows = audit.RetainedEvidence(directory).journal(instance, storage)
                self.assertEqual(schema, len(instance["status"].get("counters", {})))
                if schema == 8:
                    self.assertEqual({"count": 3, "bytes": 0}, instance["status"]["counters"]["fsync"])
                    self.assertEqual({"count": 2, "bytes": 0}, instance["status"]["counters"]["fdatasync"])
                    self.assertFalse(any(row["operation"] in {"fsync", "fdatasync"} for row in rows))

    def test_partial_unknown_or_nonzero_sync_bytes_are_rejected(self):
        changes = {
            "only fsync": lambda rows, status: rows[-1]["counters"].pop("fdatasync"),
            "unknown key": lambda rows, status: rows[-1]["counters"].update(fsyncdir=dict(count=1, bytes=0)),
            "fsync bytes": lambda rows, status: rows[-1]["counters"]["fsync"].update(bytes=1),
            "fdatasync bytes": lambda rows, status: rows[-1]["counters"]["fdatasync"].update(bytes=1),
            "fsync count boolean": lambda rows, status: rows[-1]["counters"]["fsync"].update(count=True),
            "fdatasync count negative": lambda rows, status: rows[-1]["counters"]["fdatasync"].update(count=-1),
            "fsync status behind": lambda rows, status: status["counters"]["fsync"].update(count=2),
            "fdatasync status behind": lambda rows, status: status["counters"]["fdatasync"].update(count=1),
        }
        for name, change in changes.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp).resolve()
                instance, storage = fixture(directory, change, schema=8)
                with self.assertRaises(ValueError):
                    audit.RetainedEvidence(directory).journal(instance, storage)

    def test_schema_cannot_change_between_start_lifecycle_io_or_status(self):
        def counters(schema):
            if schema == 0:
                return None
            keys = audit.LEGACY_IO_OPERATIONS if schema == 6 else audit.IO_OPERATIONS
            return {key: dict(count=0, bytes=0) for key in keys}

        for original in (0, 6, 8):
            for replacement in (0, 6, 8):
                if original == replacement:
                    continue
                for location in ("ready", "io", "status"):
                    if location == "io" and replacement == 0:
                        continue  # Individual I/O receipts deliberately omit aggregate counters.
                    with self.subTest(original=original, replacement=replacement, location=location), tempfile.TemporaryDirectory() as tmp:
                        def change(rows, status):
                            target = status if location == "status" else rows[1 if location == "ready" else 2]
                            value = counters(replacement)
                            if value is None:
                                target.pop("counters", None)
                            else:
                                target["counters"] = value
                        directory = Path(tmp).resolve()
                        instance, storage = fixture(directory, change, schema=original)
                        with self.assertRaises(ValueError):
                            audit.RetainedEvidence(directory).journal(instance, storage)

    def test_fsync_and_fdatasync_counters_cannot_regress_in_later_summary(self):
        for operation in ("fsync", "fdatasync"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as tmp:
                def change(rows, status):
                    later = deepcopy(rows[-1])
                    for key in ("sequence", "timestampMs", "monotonicNs"):
                        later[key] += 1
                    later["counters"][operation]["count"] -= 1
                    rows.append(later)
                    status.update(sequence=later["sequence"], counters=deepcopy(later["counters"]))
                directory = Path(tmp).resolve()
                instance, storage = fixture(directory, change, schema=8)
                with self.assertRaisesRegex(ValueError, "counters moved backwards"):
                    audit.RetainedEvidence(directory).journal(instance, storage)

    def test_control_reply_and_status_preserve_exact_journal_counter_schema(self):
        for schema in (0, 6, 8):
            with self.subTest(schema=schema), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp).resolve()
                instance, storage = fixture(directory, schema=schema)
                rows = [json.loads(line) for line in (directory / "journal.jsonl").read_text().splitlines()]
                native = deepcopy(rows[-1])
                native.update(operation="ARM", state="ARMED", faultId="fault", revision=1,
                              sequence=len(rows) + 1, monotonicNs=100, leaseMs=120000,
                              leaseDeadlineMonotonicNs=120000000100, allowedOperations=["create", "write"],
                              path=".", result=0, errno=0, reason="accepted")
                rows.append(native)
                status = {key: native[key] for key in ("state", "faultId", "revision", "sequence", "leaseMs",
                                                       "leaseDeadlineMonotonicNs", "journalComplete")}
                status["stopping"] = False
                if schema:
                    status["counters"] = deepcopy(native["counters"])
                reply = dict(status, version=1, ok=True, error=None, allowedOperations=["create", "write"],
                             **{key: storage[key] for key in ("attemptId", "storageId", "namespace")},
                             **{key: instance["instance"][key] for key in ("instanceId", "daemonPid")})
                ack = dict(confirmed=True, exitCode=0, stderr="", startedNanos=1, completedNanos=2,
                           command="ARM", status=status, stdout=json.dumps(reply), instance=instance["instance"])
                transition = dict(startedNanos=1, completedNanos=2, faultId="fault", revision=1)
                self.assertEqual(native, audit.verify_ack(ack, transition, storage, rows))
                for replacement in (0, 6, 8):
                    if replacement == schema:
                        continue
                    for location in ("reply", "status"):
                        with self.subTest(schema=schema, replacement=replacement, location=location):
                            altered_ack, altered_reply = deepcopy(ack), deepcopy(reply)
                            target = altered_reply if location == "reply" else altered_ack["status"]
                            if replacement == 0:
                                target.pop("counters", None)
                            else:
                                keys = audit.LEGACY_IO_OPERATIONS if replacement == 6 else audit.IO_OPERATIONS
                                target["counters"] = {key: dict(count=0, bytes=0) for key in keys}
                            altered_ack["stdout"] = json.dumps(altered_reply)
                            with self.assertRaises(ValueError):
                                audit.verify_ack(altered_ack, transition, storage, rows)

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
