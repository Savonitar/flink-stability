"""Bounded generated native receipts; no Docker, Flink, network or retained runs."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).absolute().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


check = load("checkpoint_storage_check_tests", ROOT / "calibration/checkpoint-storage/check.py")
base = load("checkpoint_storage_state_fixture", ROOT / "tools/tests/test_state_recovery_calibration.py")
audit = check.audit


def fixture(directory, mode="correct", execution="eio"):
    result, code, unused, manifest = base.fixture(mode, "uninterrupted" if execution == "healthy" else "restart")
    result["scenario"] = check.SCENARIO
    evidence = result["evidence"]
    cell = {"mode": mode, "execution": execution}
    for marker in evidence["stateRecovery"]["trace"]["markers"]:
        marker["operatorUid"] = audit.UID
    evidence["componentErrors"] = {"eventCount": 0, "events": [], "coverage": "captured-prefix", "diagnostics": []}
    helper = {"imageId": manifest["runtimeIdentity"]["flinkImageId"], "manifestSha256": "f" * 64,
              "files": {"checkpoint-fuse": "1" * 64, "probe.jar": "2" * 64}}
    manifest["helperBundle"] = helper
    config = {"execution.checkpointing.data-inline-threshold": "0 bytes",
              "execution.checkpointing.max-concurrent-checkpoints": "1",
              "execution.checkpointing.tolerable-failed-checkpoints": "10"}
    evidence["flinkRuntime"]["config"] = config
    for component in evidence["flinkRuntime"]["components"]:
        component.update(config=config, effectiveConfiguration={"observedValues": config,
                          "sourceSha256": "3" * 64, "observedSha256": "3" * 64})
    namespace, job, fault_id = "job_" + "a" * 32, "job-123", "fault-fixture"
    storage_base = {"attemptId": "attempt-fixture", "storageId": "storage-fixture", "namespace": namespace,
                    "checkpointUri": "file:///flink/checkpoints/" + namespace, "helperManifestSha256": helper["manifestSha256"],
                    "materializedFiles": [], "complete": True, "quiescent": False, "diagnostics": []}
    identities = {}
    for name, rid, role in (("jobmanager-1#1", "jm-a", "JOB_MANAGER"), ("taskmanager-1#1", "tm-a", "TASK_MANAGER"),
                            ("taskmanager-1#2", "tm-b", "TASK_MANAGER")):
        identities[name] = {"logicalName": name.split("#")[0], "process": name, "runtimeId": rid,
                            "instanceId": "instance-" + rid, "role": role, "daemonPid": 20}
    metadata_path, state_path = namespace + "/" + job + "/chk-42/_metadata", namespace + "/" + job + "/chk-42/state-file"
    rows = {name: [] for name in identities}

    def append(name, operation, when, path=".", size=0, state="HEALTHY", revision=0, errno=0, reason="ok"):
        identity = identities[name]
        row = {"version": 1, "sequence": len(rows[name]) + 1, "timestampMs": when, "monotonicNs": when * 1000000,
               "daemonPid": identity["daemonPid"], "attemptId": storage_base["attemptId"], "storageId": storage_base["storageId"],
               "instanceId": identity["instanceId"], "namespace": namespace, "faultId": fault_id if revision else None,
               "revision": revision, "state": state, "journalComplete": True, "leaseMs": 120000 if revision else 0,
               "leaseDeadlineMonotonicNs": 120030000000 if revision else 0, "allowedOperations": ["create", "write"] if revision else [],
               "operation": operation, "path": path, "faultOperation": operation if errno else None,
               "result": -1 if errno else size, "errno": errno, "requestedBytes": size, "returnedBytes": 0 if errno else size,
               "datasync": None, "reason": reason}
        rows[name].append(row)
        return row

    for name in identities:
        append(name, "START", 1, state="STARTING")
        append(name, "READY", 2, reason=audit.READY)
    append("jobmanager-1#1", "read", 21, metadata_path, 64)
    append("jobmanager-1#1", "read", 22, state_path, 16)
    append("taskmanager-1#1", "write", 19, state_path, 16)
    append("taskmanager-1#2", "read", 125, state_path, 16)
    commands = {name: [] for name in identities}
    transitions = []
    if execution == "eio":
        for name, when in (("jobmanager-1#1", 30), ("taskmanager-1#1", 31)):
            append(name, "ARM", when, state="ARMED", revision=1, reason="accepted")
        append("taskmanager-1#1", "create", 35, namespace + "/" + job + "/chk-43/new-file", state="ARMED", revision=1, errno=5)
        append("jobmanager-1#1", "HEAL", 118, revision=2, reason="accepted")
        for revision, command, names, started in ((1, "ARM", ["jobmanager-1#1", "taskmanager-1#1"], 1000),
                                                  (2, "HEAL", ["jobmanager-1#1"], 3000)):
            acknowledgements = []
            for ordinal, name in enumerate(names):
                native = next(row for row in rows[name] if row["operation"] == command)
                status = {key: native[key] for key in ("state", "faultId", "revision", "sequence", "leaseMs", "leaseDeadlineMonotonicNs", "journalComplete")}
                status["stopping"] = False
                reply = dict(status, version=1, ok=True, error=None, attemptId=storage_base["attemptId"],
                             storageId=storage_base["storageId"], instanceId=identities[name]["instanceId"],
                             namespace=namespace, daemonPid=20, allowedOperations=["create", "write"])
                ack = {"instance": identities[name], "command": command, "startedNanos": started + ordinal * 20,
                       "completedNanos": started + ordinal * 20 + 10, "exitCode": 0, "status": status,
                       "stdout": json.dumps(reply), "stderr": "", "confirmed": True}
                acknowledgements.append(ack)
                commands[name].append(ack)
            transitions.append({"faultId": fault_id, "revision": revision, "startedNanos": started,
                                "completedNanos": started + 100, "acknowledgements": acknowledgements,
                                "deadInstances": [] if revision == 1 else [identities["taskmanager-1#1"]], "confirmed": True, "diagnostics": []})
    copies = {}

    def instance(name, count=None, dead=False):
        selected = deepcopy(rows[name][:count])
        data = b"".join((json.dumps(row, sort_keys=True) + "\n").encode() for row in selected)
        hashed = audit.sha(data)
        local = "native-journals/" + identities[name]["instanceId"] + "-" + hashed + ".jsonl"
        source = "/generated/" + local
        copies[source] = (local, data)
        projected = []
        for row in selected:
            p = {key: row[key] for key in audit.PROJECTED}
            p.update(instance=identities[name], timestampMillis=row["timestampMs"], monotonicNanos=row["monotonicNs"])
            projected.append(p)
        status = {key: selected[-1][key] for key in ("state", "faultId", "revision", "sequence", "leaseMs", "leaseDeadlineMonotonicNs", "journalComplete")}
        status["stopping"] = False
        return {"instance": identities[name], "verification": {"complete": True, "diagnostics": [], "uid": [9999] * 4,
                "gid": [9999] * 4, "capabilities": "0000000000000000", "jvmPid": 42, "rawReadDenied": True, "rawWriteDenied": True,
                "mountInfo": "1 2 0:1 / /flink/checkpoints rw - fuse /dev/fuse rw", "imageId": helper["imageId"],
                "helperSha256": helper["files"]["checkpoint-fuse"], "probeSha256": helper["files"]["probe.jar"]},
                "status": status, "processDead": dead, "journalComplete": True, "journalPath": source,
                "journalSha256": hashed, "journalBytes": len(data), "lastSequence": len(selected), "operations": projected,
                "commands": deepcopy(commands[name]), "diagnostics": []}

    final_names = ["jobmanager-1#1", "taskmanager-1#1"] + ([] if execution == "healthy" else ["taskmanager-1#2"])
    final = dict(storage_base, instances=[instance(name, dead=True) for name in final_names], quiescent=True)
    evidence["checkpointStorage"] = final
    evidence["processFence"]["observations"] = [{"runtimeId": identities[name]["runtimeId"], "logicalName": identities[name]["logicalName"],
        "moment": "after-declared-kill" if name == "taskmanager-1#1" and execution != "healthy" else "after-fence-kill",
        "missing": False, "diagnostic": None, "state": {"runtimeId": identities[name]["runtimeId"], "running": False,
        "paused": False, "oomKilled": False, "exitCode": 137}} for name in final_names]
    evidence["flinkJob"].update(completedCheckpoints=2, latestRestoredCheckpoint=42 if execution != "healthy" else None)
    evidence["kafka"]["containers"] = [{"containerId": "kafka-a"}]
    if execution != "healthy":
        proof = evidence["stateRecovery"]["recoveries"][0]
        proof["readiness"]["completedSnapshots"]["42"]["acknowledgedAtMillis"] = 20
    if execution == "eio":
        def cp(number, status, trigger, failure=None):
            return {"id": number, "status": status, "savepoint": False, "checkpointType": "CHECKPOINT",
                    "triggerTimestamp": trigger, "latestAckTimestamp": 20 if number == 42 else 132,
                    "failureTimestamp": failure}
        saved, failed, late, subsequent = cp(42, "COMPLETED", 5), cp(43, "FAILED", 32, 36), cp(44, "FAILED", 116, 117), cp(45, "COMPLETED", 130)

        def history(values, restored=False):
            counts = {"total": len(values), "inProgress": 0, "completed": sum(row["status"] == "COMPLETED" for row in values),
                      "failed": sum(row["status"] == "FAILED" for row in values), "restored": int(restored)}
            return {"jobId": job, "counts": counts, "retained": values, "latestCompleted": subsequent if restored else saved,
                    "latestFailed": next((row for row in values if row["status"] == "FAILED"), None), "latestSavepoint": None,
                    "latestRestored": {"id": 42, "restoreTimestamp": 120} if restored else None}
        meta = {"path": "/flink/checkpoints/" + metadata_path, "size": 64, "regularFile": True, "sha256": "4" * 64, "observedNanos": 900}
        state_file = dict(meta, path="/flink/checkpoints/" + state_path, size=16, sha256="5" * 64)
        materialized = [meta, state_file]
        final["materializedFiles"] = materialized
        counts = ((4, 3), (5, 4), (5, 5), (5, 5), (6, 5), (6, 5))
        histories = (history([saved]), history([saved]), history([failed, saved]), history([late, failed, saved]),
                     history([late, failed, saved]), history([subsequent, late, failed, saved], True))
        samples = []
        for index, (stage, pair, h) in enumerate(zip(audit.STAGES, counts, histories), 1):
            si = [instance("jobmanager-1#1", pair[0]), instance("taskmanager-1#1", pair[1], index >= 4)]
            if stage == "RECOVERED":
                si.append(instance("taskmanager-1#2"))
            samples.append({"stage": stage, "sequence": index, "elapsedNanos": index * 1000000,
                            "storage": dict(storage_base, instances=si, materializedFiles=materialized), "checkpoints": h})
            observation = {"jobManagerTimeMillis": [25, 31, 37, 117, 119, 133][index - 1], "state": "RUNNING",
                           "completedCheckpoints": h["counts"]["completed"], "restoredCheckpoints": h["counts"]["restored"],
                           "latestRestore": {"checkpointId": 42} if index == 6 else None, "failures": [],
                           "subtasks": [{"vertexName": vertex, "index": 0, "attempt": 1 if index == 6 else 0,
                             "status": "CANCELED" if index in (4, 5) else "RUNNING",
                             "taskManagerId": "resource-b" if index == 6 else "resource-a"} for vertex in ("source", "keyed")]}
            samples[-1]["job"] = observation
        markers = evidence["stateRecovery"]["trace"]["markers"]
        failed_marker = deepcopy(markers[1]); failed_marker.update(checkpointId=43, lineNumber=4)
        markers.append(failed_marker)
        after = proof["postRestoreProgress"]
        subsequent_marker = deepcopy(after); subsequent_marker.update(event="SNAPSHOT", checkpointId=45, lineNumber=after["lineNumber"] + 1)
        completion = deepcopy(subsequent_marker); completion.update(event="COMPLETED", lineNumber=after["lineNumber"] + 2, progress=None)
        markers.extend([subsequent_marker, completion])
        kill = dict(evidence["taskManagerKills"][0], identityFailure=None)
        kill.update(stateReadiness=proof["readiness"], jobBeforeKill={"observation": samples[2]["job"], "failure": None})
        restart = dict(evidence["taskManagerRestarts"][0], previousIdentityFailure=None, replacementIdentityFailure=None)
        fault = {"jobId": job, "triggerId": fault_id, "timeoutMillis": 90000, "checkpointId": 42, "failedCheckpointId": 43,
                 "samples": samples, "transitions": transitions, "kill": kill, "restart": restart, "readiness": proof["readiness"],
                 "configuration": {"jobId": job, "mode": "exactly_once", "intervalMillis": 10000, "timeoutMillis": 600000,
                    "minPauseMillis": 0, "maxConcurrent": 1, "tolerableFailedCheckpoints": 10, "stateBackend": "HashMapStateBackend",
                    "checkpointStorage": "FileSystemCheckpointStorage", "unalignedCheckpoints": False, "stateChangelogEnabled": False,
                    "externalized": False, "deleteOnCancellation": True},
                 "failedCheckpoint": failed, "triggerResult": {"state": "COMPLETED", "checkpointId": None,
                    "failure": json.dumps({"stack-trace": "Input/output error /chk-43/file"})},
                 "materialization": {"instance": identities["jobmanager-1#1"], "jobId": job, "checkpointId": 42, "operatorUid": audit.UID,
                    "metadata": meta, "files": [state_file], "complete": True, "diagnostics": []},
                 "failureTrace": {"complete": True, "diagnostics": [], "markers": [failed_marker]},
                 "recoveryTrace": {"complete": True, "diagnostics": [], "markers": markers}, "recovery": proof,
                 "errors": [], "confirmed": True, "finalCaptureConfirmed": True}
        evidence["checkpointStorageFaults"] = [fault]
    receipts = []
    for source, (local, data) in copies.items():
        path = directory / local; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
        receipts.append({"sourcePath": source, "localPath": local, "sha256": audit.sha(data), "kind": "native-journal"})
    (directory / "retained-evidence.json").write_text(json.dumps({"version": 1, "files": receipts}))
    return result, code, cell, manifest


def rewrite_journals(directory, result, transform, only_stage=None):
    """Rehash coherent adversarial raw/typed copies so tests reach semantic checks, not just hashes."""
    index = json.loads((directory / "retained-evidence.json").read_text())
    original = {row["sourcePath"]: row for row in index["files"]}
    snapshots = [("FINAL", result["evidence"]["checkpointStorage"])]
    snapshots += [(sample["stage"], sample["storage"]) for sample in result["evidence"]["checkpointStorageFaults"][0]["samples"]]
    changed = {}
    for stage, snapshot in snapshots:
        if only_stage is not None and stage != only_stage:
            continue
        for instance in snapshot["instances"]:
            source = instance["journalPath"]
            if source not in changed:
                old = original[source]
                rows = [json.loads(line) for line in (directory / old["localPath"]).read_bytes().splitlines()]
                for row in rows:
                    transform(instance["instance"], row)
                data = b"".join((json.dumps(row, sort_keys=True) + "\n").encode() for row in rows)
                local = "rewritten/" + audit.sha(data) + ".jsonl"
                path = directory / local; path.parent.mkdir(exist_ok=True); path.write_bytes(data)
                receipt = dict(old, sourcePath=source + ".rewritten", localPath=local, sha256=audit.sha(data))
                index["files"].append(receipt)
                changed[source] = (receipt, rows, data)
            receipt, rows, data = changed[source]
            instance.update(journalPath=receipt["sourcePath"], journalSha256=receipt["sha256"], journalBytes=len(data))
            projected = []
            for row in rows:
                value = {key: row[key] for key in audit.PROJECTED}
                value.update(instance=instance["instance"], timestampMillis=row["timestampMs"], monotonicNanos=row["monotonicNs"])
                projected.append(value)
            instance["operations"] = projected
    (directory / "retained-evidence.json").write_text(json.dumps(index))


class NativeStorageAuditTest(unittest.TestCase):
    def run_case(self, change=None, mode="correct", execution="eio"):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp).resolve()
            args = fixture(directory, mode, execution)
            if change:
                change(args[0])
            return audit.audit_storage(args[0], args[2], args[3], audit.RetainedEvidence(directory))

    def test_all_native_cells_pass_with_generated_receipts(self):
        for mode, execution in (("correct", "healthy"), ("correct", "restart"), ("correct", "eio"), ("mutant", "eio")):
            with self.subTest(mode=mode, execution=execution):
                report = self.run_case(mode=mode, execution=execution)
                self.assertTrue(report["complete"])
                if execution == "eio":
                    self.assertEqual(report["nativeEioOperations"], 1)
                    self.assertEqual(report["stages"], list(audit.STAGES))

    def test_idless_startup_failure_is_retained_through_the_full_atomic_sequence(self):
        def change(result):
            fault = result["evidence"]["checkpointStorageFaults"][0]
            fault["baselineIdlessTriggerFailures"] = 1
            for sample in fault["samples"]:
                counts = sample["checkpoints"]["counts"]
                counts["total"] += 1
                counts["failed"] += 1
        report = self.run_case(change)
        self.assertTrue(report["complete"])
        self.assertEqual(1, report["baselineIdlessTriggerFailures"])

    def test_real_baseline_failures_and_unaccounted_idless_deltas_are_rejected(self):
        def idless(fault):
            fault["baselineIdlessTriggerFailures"] = 1
            for sample in fault["samples"]:
                sample["checkpoints"]["counts"]["total"] += 1
                sample["checkpoints"]["counts"]["failed"] += 1

        def actual_failure(fault, retained):
            idless(fault)
            old = deepcopy(fault["failedCheckpoint"])
            old.update(id=41, triggerTimestamp=1, failureTimestamp=2)
            for sample in fault["samples"][:2]:
                sample["checkpoints"]["latestFailed"] = old
                if retained:
                    sample["checkpoints"]["retained"].append(old)

        changes = {
            "retained failure": lambda fault: actual_failure(fault, True),
            "latest outside cache": lambda fault: actual_failure(fault, False),
            "forged baseline count": lambda fault: fault.update(baselineIdlessTriggerFailures=1),
            "missing nonzero baseline": lambda fault: (idless(fault), fault.pop("baselineIdlessTriggerFailures")),
            "idless change during ARM": lambda fault: fault["samples"][1]["checkpoints"]["counts"].update(total=2, failed=1),
            "extra idless failure with injected failure": lambda fault: fault["samples"][2]["checkpoints"]["counts"].update(total=3, failed=2),
        }
        for name, change in changes.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.run_case(lambda result: change(result["evidence"]["checkpointStorageFaults"][0]))

    def test_causal_negative_receipts_cannot_qualify(self):
        changes = {
            "missing stage": lambda f: f["samples"].pop(2),
            "missing arm": lambda f: f["transitions"][0]["acknowledgements"].pop(),
            "wrong ack daemon": lambda f: f["transitions"][0]["acknowledgements"][0]["instance"].update(daemonPid=99),
            "changed failure ID": lambda f: f.update(failedCheckpointId=44),
            "missing failed snapshot": lambda f: f["failureTrace"].update(markers=[]),
            "missing recovery trace": lambda f: f.pop("recoveryTrace"),
            "no subsequent checkpoint": lambda f: f["recoveryTrace"].update(markers=[m for m in f["recoveryTrace"]["markers"] if m["checkpointId"] != 45]),
            "wrong materialized operator": lambda f: f["materialization"].update(operatorUid="source-operator"),
            "metadata as state": lambda f: f["materialization"].update(files=[f["materialization"]["metadata"]]),
            "inline state": lambda f: f["materialization"].update(files=[]),
            "outside checkpoint": lambda f: f["materialization"]["files"][0].update(path="/flink/checkpoints/other/chk-42/state"),
            "state bytes missing": lambda f: f["materialization"]["files"][0].update(size=17),
            "config nonexternal": lambda f: f["configuration"].update(checkpointStorage="JobManagerCheckpointStorage"),
            "wrong interval": lambda f: f["configuration"].update(intervalMillis=500),
            "wrong old process": lambda f: f["kill"]["identity"].update(runtimeId="unrelated"),
            "deadline": lambda f: f["samples"][-1].update(elapsedNanos=90000000001),
            "generic IO failure": lambda f: f["triggerResult"].update(failure=json.dumps({"stack-trace": "IOException /chk-43/file"})),
            "premature replacement": lambda f: f["samples"][3]["job"]["subtasks"][0].update(status="RUNNING"),
            "different original attempt": lambda f: f["samples"][2]["job"]["subtasks"][0].update(attempt=1),
            "forged stage revision": lambda f: f["samples"][1]["storage"]["instances"][0]["status"].update(revision=0),
        }
        for name, change in changes.items():
            with self.subTest(name=name), self.assertRaises((ValueError, KeyError, StopIteration)):
                self.run_case(lambda result: change(result["evidence"]["checkpointStorageFaults"][0]))

    def test_raw_journal_missing_hash_tampering_and_symlink_rejected(self):
        for variant in ("missing", "hash", "symlink", "duplicate-key"):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve(); args = fixture(root)
                index = json.loads((root / "retained-evidence.json").read_text()); row = index["files"][0]; path = root / row["localPath"]
                if variant == "missing": path.unlink()
                elif variant == "hash": path.write_bytes(path.read_bytes() + b"\n")
                elif variant == "symlink":
                    original = root / "elsewhere"; original.write_bytes(path.read_bytes()); path.unlink(); path.symlink_to(original)
                else:
                    data = path.read_bytes().replace(b'"version": 1', b'"version": 1, "version": 1', 1)
                    path.write_bytes(data); row["sha256"] = audit.sha(data); (root / "retained-evidence.json").write_text(json.dumps(index))
                with self.assertRaises((ValueError, OSError)):
                    audit.audit_storage(args[0], args[2], args[3], audit.RetainedEvidence(root))

    def test_missing_physical_fence_and_raw_isolation_rejected(self):
        for change in (lambda e: e["processFence"].update(observations=[]),
                       lambda e: e["checkpointStorage"]["instances"][0]["verification"].update(rawReadDenied=False),
                       lambda e: e["checkpointStorage"]["instances"][0]["verification"].update(capabilities="0000000000200000"),
                       lambda e: e["checkpointStorage"]["instances"][0]["verification"].update(jvmPid=0),
                       lambda e: e["checkpointStorage"]["instances"][0].update(processDead=False)):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.run_case(lambda result: change(result["evidence"]))

    def test_old_journal_prefix_cannot_be_rewritten(self):
        def change(result):
            f = result["evidence"]["checkpointStorageFaults"][0]
            f["samples"][0]["storage"]["instances"][0]["operations"][0]["reason"] = "forged"
        with self.assertRaisesRegex(ValueError, "differs from raw"):
            self.run_case(change)

    def test_coherent_rehashed_native_forgeries_are_rejected(self):
        def no_ready(identity, row):
            if row["operation"] == "READY": row.update(operation="getattr", reason="replaced")
        def before_arm(identity, row):
            if row["errno"] == 5: row["timestampMs"] = 29
        def unrelated_path(identity, row):
            if row["errno"] == 5: row["path"] = "other-job/chk-43/file"
        def expired(identity, row):
            if row["errno"] == 5: row["leaseDeadlineMonotonicNs"] = row["monotonicNs"] - 1
        def hidden_new_arm(identity, row):
            if identity["runtimeId"] == "tm-b" and row["operation"] == "read":
                row.update(operation="ARM", revision=1, faultId="fault-fixture", state="ARMED",
                           leaseMs=120000, leaseDeadlineMonotonicNs=200000000000)
        for change in (no_ready, before_arm, unrelated_path, expired, hidden_new_arm):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp).resolve(); args = fixture(directory)
                rewrite_journals(directory, args[0], change)
                with self.assertRaises(ValueError):
                    audit.audit_storage(args[0], args[2], args[3], audit.RetainedEvidence(directory))

    def test_rehashed_old_snapshot_still_must_prefix_final_journal(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp).resolve(); args = fixture(directory)
            def change(identity, row):
                if identity["role"] == "JOB_MANAGER" and row["operation"] == "read": row["reason"] = "rewritten-old-snapshot"
            rewrite_journals(directory, args[0], change, only_stage="BEFORE_ARM")
            with self.assertRaisesRegex(ValueError, "prefix"):
                audit.audit_storage(args[0], args[2], args[3], audit.RetainedEvidence(directory))

    def test_cleanup_rejects_missing_process_different_daemon_or_nonempty_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp).resolve(); result, unused, ignored, manifest = fixture(directory)
            endpoint = "unix:///generated/docker.sock"
            def receipt(arguments):
                return {"argv": ["docker", "--host", endpoint, *arguments], "exitCode": 0, "stdout": "", "stderr": ""}
            ids = ["jm-a", "tm-a", "tm-b", "kafka-a"]
            cleanup = {"version": 1, "complete": True, "containers": [{"runtimeId": value, "absent": True,
                       "receipt": receipt(["container", "ls", "-a", "--no-trunc", "--filter", "id=" + value, "--format", "{{.ID}}"])} for value in ids],
                       "storage": {"storageId": "storage-fixture", "absent": True,
                       "receipt": receipt(["volume", "ls", "--filter", "label=org.savonitar.flink-stability.storage=storage-fixture", "--format", "{{.Name}}"])} }
            (directory / "cleanup.json").write_text(json.dumps(cleanup))
            self.assertTrue(audit.cleanup_receipt(directory, result, endpoint)["complete"])
            for change in (lambda c: c["containers"].pop(),
                           lambda c: c["containers"][0]["receipt"]["argv"].__setitem__(2, "unix:///different.sock"),
                           lambda c: c["containers"][0]["receipt"].update(stdout="jm-a\n"),
                           lambda c: c["storage"]["receipt"].update(exitCode=1)):
                altered = deepcopy(cleanup); change(altered); (directory / "cleanup.json").write_text(json.dumps(altered))
                with self.subTest(change=change), self.assertRaises(ValueError):
                    audit.cleanup_receipt(directory, result, endpoint)

    def test_component_events_and_rest_errors_survive_unqualified_attempt(self):
        # The matrix row is filled before proof evaluation; an unrelated failure must not erase diagnostics.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            result, code, cell, manifest = fixture(root)
            result["evidence"]["componentErrors"] = {"eventCount": 2, "events": ["first", "second"]}
            result["evidence"]["flinkRest"] = {"errorCount": 1, "errors": ["rest-failure"]}
            manifest.update(cells=[dict(cell, name="correct-eio", attempts=["attempts/correct-eio/run-1"])], frameworkFreeze={"sha256": "f" * 64}, references={})
            attempt = root / "attempts/correct-eio/run-1"; attempt.mkdir(parents=True)
            (root / "manifest.json").write_text("{}")
            (attempt / "stdout.json").write_text(json.dumps(result)); (attempt / "stderr.log").write_text("stderr")
            (attempt / "exit-code.txt").write_text(str(code))
            with patch.object(check, "validate_manifest", return_value=manifest):
                report = check.check_matrix(root)
            self.assertFalse(report["qualified"])
            self.assertEqual(report["componentEventCount"], 2)
            self.assertEqual(report["restErrorCount"], 1)
            self.assertEqual(report["attempts"][0]["componentErrors"]["events"], ["first", "second"])


if __name__ == "__main__":
    unittest.main()
