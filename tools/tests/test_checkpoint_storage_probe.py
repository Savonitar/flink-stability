"""Independent synthetic FUSE/Java evidence fixtures; these tests never contact Docker."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


REPOSITORY = Path(__file__).absolute().parents[2]
SPEC = importlib.util.spec_from_file_location("isolated_checkpoint_storage_probe",
                                            REPOSITORY / "testcontainers" / "checkpoint_storage_probe.py")
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)

BASE = b"before-fault"
AFTER = b"-after-heal"
ATTEMPT = "fuse_fixture"
STORAGE = "storage_fixture"
NAMESPACE = "job_fixture"
IMAGE = "sha256:" + "1" * 64
FILES = {"checkpoint-fuse": "2" * 64, "probe.jar": "3" * 64}


def encoded(value):
    return base64.b64encode(value).decode("ascii")


def sha(value):
    return hashlib.sha256(value).hexdigest()


class Evidence:
    """Construct real protocol shapes independently of the driver's qualification helpers."""
    def __init__(self):
        self.clock = 10_000_000_000
        self.journals = {name: [] for name in "abcd"}
        self.revision = {name: 0 for name in "abcd"}
        self.state = {name: "HEALTHY" for name in "abcd"}
        self.fault = {name: None for name in "abcd"}
        self.requests = {name: 0 for name in "abcd"}
        self.counters = {name: {operation: {"count": 0, "bytes": 0}
                               for operation in ("read", "getattr", "readdir", "access", "flush", "release", "fsync", "fdatasync")}
                         for name in "abcd"}
        self.report = {"version": 1, "kind": "checkpoint-fuse-probe", "status": "observed",
                       "attemptId": ATTEMPT, "storageId": STORAGE, "namespace": NAMESPACE,
                       "imageId": IMAGE, "platform": "linux/arm64",
                       "image": {"Id": IMAGE, "Os": "linux", "Architecture": "arm64",
                                 "RepoDigests": ["flink@" + IMAGE]},
                       "engine": {"Version": "29.0.0", "Os": "linux", "Arch": "arm64"},
                       "bundle": {"files": FILES.copy(), "platform": "linux/arm64"},
                       "observations": [], "instances": {}, "cleanup": []}
        for index, name in enumerate("abcd", 4):
            cid = str(index) * 64
            inspection = {"Id": cid, "Image": IMAGE,
                          "Config": {"User": "0:0", "Labels": {probe.LABEL: ATTEMPT}},
                          "HostConfig": {"Privileged": False, "NetworkMode": "none", "CapAdd": ["SYS_ADMIN"],
                                         "CapDrop": ["ALL"], "Binds": None, "PidMode": "", "IpcMode": "private",
                                         "Devices": [{"PathOnHost": "/dev/fuse", "PathInContainer": "/dev/fuse",
                                                      "CgroupPermissions": "rwm"}]},
                          "Mounts": [{"Type": "volume", "Name": "fsi-" + ATTEMPT,
                                      "Source": "/var/lib/docker/volumes/fsi-" + ATTEMPT + "/_data",
                                      "Destination": "/backing", "RW": True}]}
            self.report["instances"][name] = {
                "containerId": cid, "inspection": inspection, "daemonPid": 100 + index,
                "copiedHashes": "".join(value + "  " + key + "\n" for key, value in FILES.items()),
                "mountInfo": "41 29 0:33 / /view rw,nosuid,nodev - fuse.checkpoint-fuse checkpoint-fuse rw,user_id=0,group_id=0,allow_other\n",
                "kernel": "Linux 6.12.0 aarch64 GNU/Linux", "uidMap": "0 0 4294967295\n",
                "journalCaptureExit": 0, "daemonStoppedBeforeCapture": True}
            self.report["cleanup"].append({"instance": name, "containerId": cid, "removed": True})
            self.event(name, "START", reason="mounted")
            self.event(name, "READY", reason="direct_io=1,writeback_cache=0,entry_timeout=0,attr_timeout=0,negative_timeout=0,kernel_cache=0,auto_cache=0")
            self.control(name, "ready", "STATUS")
        self.report["cleanup"].append({"volume": "fsi-" + ATTEMPT, "removed": True})
        self.build_observations()

    def tick(self):
        self.clock += 10_000_000
        return self.clock

    def event(self, name, operation, path=".", result=0, error=0, requested=0, returned=0,
              datasync=None, fault_operation=None, reason="forwarded"):
        if operation == "fsync" and result >= 0 and error == 0:
            self.counters[name]["fdatasync" if datasync else "fsync"]["count"] += 1
            return None
        row = {"version": 1, "sequence": len(self.journals[name]) + 1,
               "timestampMs": 1_790_000_000_000 + self.clock // 1_000_000,
               "monotonicNs": self.tick(), "attemptId": ATTEMPT, "storageId": STORAGE,
               "instanceId": name, "namespace": NAMESPACE, "faultId": self.fault[name],
               "daemonPid": self.report["instances"][name]["daemonPid"],
               "revision": self.revision[name], "state": self.state[name], "journalComplete": True,
               "operation": operation, "path": path, "faultOperation": fault_operation,
               "result": result, "errno": error, "requestedBytes": requested,
               "returnedBytes": returned, "datasync": datasync, "reason": reason}
        if operation[0].isupper():
            row["counters"] = copy.deepcopy(self.counters[name])
        self.journals[name].append(row)
        return row

    def observation(self, name, suffix, kind, request, response, started=None):
        self.report["observations"].append({"label": name + "." + suffix, "instance": name, "kind": kind,
                                            "request": request, "response": response,
                                            "startedMonotonicNs": started or self.tick(),
                                            "finishedMonotonicNs": self.tick()})

    def control(self, name, suffix, operation, *args, ok=True, error=None, request_revision=None,
                request_instance=None):
        started = self.tick()
        revision = self.revision[name] if operation == "STATUS" else self.revision[name] + 1
        if request_revision is not None:
            revision = request_revision
        if ok and operation != "STATUS":
            self.revision[name] = revision
            if operation == "ARM":
                self.state[name], self.fault[name] = "ARMED", args[0]
            elif operation == "HEAL":
                self.state[name] = "HEALTHY"
            elif operation == "ABORT":
                self.state[name] = "ABORTED"
        event = (self.journals[name][-1] if operation == "STATUS" and ok else
                 self.event(name, operation if ok else "REJECT", error=0 if ok else 22,
                            result=0 if ok else -1, reason="accepted" if ok else error))
        response = {"version": 1, "ok": ok, "error": error, "state": self.state[name],
                    "attemptId": ATTEMPT, "storageId": STORAGE, "instanceId": name, "namespace": NAMESPACE,
                    "faultId": self.fault[name], "revision": self.revision[name], "sequence": event["sequence"],
                    "journalComplete": True, "daemonPid": self.report["instances"][name]["daemonPid"],
                    "leaseRemainingMs": 30000 if self.state[name] == "ARMED" else 0,
                    "stopping": operation == "STOP" and ok}
        response["counters"] = copy.deepcopy(self.counters[name])
        request = " ".join(map(str, (operation, ATTEMPT, STORAGE, request_instance or name, revision, *args)))
        self.observation(name, suffix, "control", request, response, started)

    def java(self, name, suffix, operation, *args, status="ok", phase="complete", exception=None, **details):
        started = self.tick()
        self.requests[name] += 1
        number = self.requests[name]
        response = {"protocol": "checkpoint-storage-io-v1", "sequence": number, "requestId": number,
                    "operation": operation, "status": status, "phase": phase,
                    "timestamp": "2026-10-27T00:00:00Z", **details}
        if operation == "hello":
            response.update(pid=400 + ord(name), javaVersion="17.0.13", javaVendor="Amazon.com Inc.",
                            osName="Linux", osArch="aarch64", osVersion="6.12.0", timeoutSeconds=2 if name == "d" else 120,
                            watchdogExitCode=124, maximumPayloadBytes=4096, linuxIdentityAvailable=True,
                            uid=[9999] * 4, gid=[9999] * 4, capEff="0000000000000000")
        if exception:
            response.update(exceptionClass=exception, exceptionMessage="fixture filesystem failure",
                            exceptionMessageTruncated=False)
        if status == "ok":
            aggregate = {"read": "read", "stat": "getattr", "list": "readdir", "close": "release"}.get(operation)
            if aggregate:
                self.counters[name][aggregate]["count"] += 1
                if aggregate == "read":
                    self.counters[name][aggregate]["bytes"] += details["readBytes"]
        request = "\t".join(map(str, (number, operation, *args)))
        self.observation(name, suffix, "java", request, response, started)
        return response

    def read(self, name, suffix, path, value):
        self.java(name, suffix, "read", path, path=path, readBytes=len(value), sha256=sha(value), base64=encoded(value))

    def io(self, name, suffix, operation, *args, exception="java.io.IOException", phase="open", **details):
        self.java(name, suffix, operation, *args, status="io-exception", phase=phase, exception=exception, **details)

    def build_observations(self):
        for name in "ab":
            path = "held-" + name
            self.java(name, "hello", "hello")
            self.java(name, "create", "create", path, encoded(BASE), path=path, requestedBytes=len(BASE), writtenBytes=len(BASE))
            self.java(name, "open", "open", "held", path, handle="held", path=path, position=len(BASE))
            for suffix, metadata in (("fsync", True), ("fdatasync", False)):
                self.java(name, suffix, "force", "held", str(metadata).lower(), handle="held", path=path, metadata=metadata)
                self.event(name, "fsync", NAMESPACE + "/" + path, datasync=not metadata)
            self.io(name, "raw-read", "raw-read", path, path=path, rawAccessProbe=True,
                    exception="java.nio.file.AccessDeniedException")
            self.io(name, "raw-write", "raw-write", "escape-" + name, encoded(BASE), path="escape-" + name,
                    rawAccessProbe=True, requestedBytes=len(BASE), exception="java.nio.file.AccessDeniedException")
        for name, other in (("a", "b"), ("b", "a")):
            self.read(name, "peer-read", "held-" + other, BASE)
        for name in "ab":
            self.control(name, "arm", "ARM", "write_eio", 30000, "create,write")
        for name in "ab":
            self.io(name, "fault-create", "create", "failed-" + name, encoded(BASE), path="failed-" + name,
                    requestedBytes=len(BASE))
            self.event(name, "create", NAMESPACE + "/failed-" + name, result=-1, error=5,
                       fault_operation="create", reason="armed-write-eio")
            self.io(name, "fault-write", "write", "held", encoded(AFTER), phase="write", handle="held",
                    path="held-" + name, requestedBytes=len(AFTER), positionBefore=len(BASE))
            self.event(name, "write", NAMESPACE + "/held-" + name, result=-1, error=5,
                       requested=len(AFTER), fault_operation="write", reason="armed-write-eio")
            self.read(name, "fault-read", "held-" + name, BASE)
            self.java(name, "evidence-write", "evidence-write", "still-writable", encoded(BASE),
                      path="still-writable", requestedBytes=len(BASE), writtenBytes=len(BASE), evidenceAccessProbe=True)
        for name in "ab":
            self.control(name, "heal", "HEAL", "write_eio")
        for name in "ab":
            self.java(name, "healed-write", "write", "held", encoded(AFTER), path="held-" + name, handle="held",
                      positionBefore=len(BASE), positionAfter=len(BASE + AFTER), requestedBytes=len(AFTER), writtenBytes=len(AFTER))
            self.java(name, "healed-fsync", "force", "held", "true", handle="held", path="held-" + name, metadata=True)
            self.event(name, "fsync", NAMESPACE + "/held-" + name, datasync=False)
            self.java(name, "healed-create", "create", "new-" + name, encoded(AFTER), path="new-" + name,
                      requestedBytes=len(AFTER), writtenBytes=len(AFTER))
            self.java(name, "close", "close", "held", handle="held", path="held-" + name)
            self.java(name, "rename", "rename", "new-" + name, "renamed-" + name, path="new-" + name, target="renamed-" + name)
            self.control(name, "stale", "ARM", "stale", 1000, "create,write", ok=False,
                         error="stale-revision", request_revision=1)
            self.control(name, "foreign", "ARM", "foreign", 1000, "create,write", ok=False,
                         error="foreign-identity", request_instance="wrong_instance")
            self.java(name, "traversal", "read", "../outside", status="rejected", phase="read",
                      path="../outside", exception="java.lang.IllegalArgumentException")
        for name, other in (("a", "b"), ("b", "a")):
            self.read(name, "healed-peer-read", "held-" + other, BASE + AFTER)
            self.java(name, "peer-stat", "stat", "renamed-" + other, path="renamed-" + other,
                      size=len(AFTER), regularFile=True, directory=False)
            self.java(name, "peer-list", "list", "@", path="@", entries=["held-a", "held-b", "renamed-" + other])
            self.java(name, "peer-delete", "delete", "renamed-" + other, path="renamed-" + other)
            self.io(name, "missing-created", "read", "failed-" + name, path="failed-" + name,
                    exception="java.nio.file.NoSuchFileException")
        main_request = self.requests["a"]
        self.requests["a"] = 0
        self.java("a", "scope-hello", "hello")["pid"] = 800
        self.io("a", "outside", "read", "other_namespace/unsafe", path="other_namespace/unsafe",
                exception="java.nio.file.AccessDeniedException")
        self.requests["a"] = main_request
        for name in "ab":
            self.java(name, "symlink", "read", "escape-link/unknown", path="escape-link/unknown",
                      status="rejected", phase="read", exception="java.lang.IllegalArgumentException")
        self.event("a", "SUMMARY")
        self.control("a", "before-death", "STATUS")
        self.report["instances"]["a"]["daemonBeforeKill"] = "104 (checkpoint-fuse) S 1 104 104 0 " + "0 " * 17 + "123456 0\n"
        self.report["instances"]["a"]["mountInfoAfterKill"] = self.report["instances"]["a"]["mountInfo"]
        self.io("a", "dead-read", "read", "held-a", path="held-a")
        self.java("a", "still-alive", "hello")
        self.control("b", "expiry-arm", "ARM", "expiry", 500, "create,write")
        self.clock += 700_000_000
        self.state["b"] = "EXPIRED"
        self.event("b", "EXPIRE", error=110, result=-1, reason="lease-expired")
        self.control("b", "expired", "STATUS")
        self.io("b", "expired-create", "create", "expired", encoded(BASE), path="expired", requestedBytes=len(BASE))
        self.control("b", "expired-heal", "HEAL", "expiry", ok=False, error="terminal-state")
        self.control("b", "expired-stop", "STOP")
        for name, kind, code in (("c", "cancel", 143), ("d", "deadline", 124)):
            self.java(name, "hello", "hello")
            self.control(name, "arm", "ARM", kind, 10000, "create,write")
            self.observation(name, "exit", "process", kind, {"exitCode": code})
            self.control(name, "abort", "ABORT", kind)
            self.control(name, "heal-rejected", "HEAL", kind, ok=False, error="terminal-state")
            self.control(name, "stop", "STOP")

    def row(self, label):
        return next(row for row in self.report["observations"] if row["label"] == label)

    def write(self, output):
        for name, events in self.journals.items():
            data = "".join(json.dumps(row) + "\n" for row in events).encode("utf-8")
            (output / (name + "-journal.jsonl")).write_bytes(data)
            self.report["instances"][name]["journalSha256"] = sha(data)


class QualificationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name).resolve()
        self.evidence = Evidence()

    def qualify(self):
        self.evidence.write(self.output)
        return probe.qualify(self.evidence.report, self.output)

    def reject(self):
        with self.assertRaises((ValueError, KeyError, TypeError, OSError)):
            self.qualify()

    def test_complete_four_instance_evidence_qualifies(self):
        self.assertEqual(7, len(self.qualify()))

    def test_status_observes_previous_sequence_without_spending_a_journal_record(self):
        self.assertFalse(any(event["operation"] == "STATUS" for events in self.evidence.journals.values() for event in events))
        response = self.evidence.row("a.before-death")["response"]
        self.assertEqual("SUMMARY", self.evidence.journals["a"][response["sequence"] - 1]["operation"])
        self.assertEqual(3, response["counters"]["read"]["count"])
        self.assertEqual(len(BASE) * 3 + len(AFTER), response["counters"]["read"]["bytes"])
        self.assertEqual(7, len(self.qualify()))

    def test_status_counters_may_advance_without_an_additional_journal_row(self):
        # Counters need not be identical to the last record: successful metadata is aggregated.
        value = self.evidence.row("a.before-death")["response"]
        value["counters"]["getattr"]["count"] += 1
        self.assertEqual(7, len(self.qualify()))

    def test_missing_malformed_overflowed_and_noninteger_counters_reject(self):
        mutations = [lambda row: row.pop("counters"),
                     lambda row: row["counters"].pop("access"),
                     lambda row: row["counters"].update(unknown={"count": 0, "bytes": 0}),
                     lambda row: row["counters"]["read"].update(count=True),
                     lambda row: row["counters"]["read"].update(count=1 << 63),
                     lambda row: row["counters"]["getattr"].update(bytes=1),
                     lambda row: row["counters"]["read"].update(count=0, bytes=1)]
        for target in ("control", "journal"):
            for number, mutate in enumerate(mutations):
                with self.subTest(target=target, mutation=number):
                    self.evidence = Evidence()
                    value = (self.evidence.row("a.before-death")["response"] if target == "control"
                             else next(row for row in self.evidence.journals["a"] if row["operation"] == "SUMMARY"))
                    mutate(value)
                    self.reject()

    def test_regressing_summary_and_control_counters_reject(self):
        for target in ("journal", "control"):
            with self.subTest(target=target):
                self.evidence = Evidence()
                value = (next(row for row in self.evidence.journals["a"] if row["operation"] == "SUMMARY")
                         if target == "journal" else self.evidence.row("a.heal")["response"])
                value["counters"]["read"] = {"count": 0, "bytes": 0}
                self.reject()

    def test_status_counters_cannot_exceed_the_next_durable_snapshot(self):
        self.evidence.row("b.ready")["response"]["counters"]["read"] = {"count": 99, "bytes": 99}
        self.reject()

    def test_summary_requires_actual_activity_and_startup_counts_are_zero(self):
        previous = next(row for row in reversed(self.evidence.journals["a"])
                        if row["operation"] == "REJECT")
        summary = next(row for row in self.evidence.journals["a"] if row["operation"] == "SUMMARY")
        summary["counters"] = copy.deepcopy(previous["counters"])
        self.reject()
        self.evidence = Evidence()
        self.evidence.journals["a"][0]["counters"]["access"]["count"] = 1
        self.reject()

    def test_status_requires_previous_revision_state_and_fault_identity(self):
        for key, value in (("state", "ARMED"), ("revision", 3), ("faultId", "other")):
            with self.subTest(key=key):
                self.evidence = Evidence()
                self.evidence.row("a.before-death")["response"][key] = value
                self.reject()

    def test_top_level_abort_expiry_failure_and_predeclared_pass_cannot_qualify(self):
        for status in ("aborted", "expired", "failed", "unsupported", "pass"):
            with self.subTest(status=status):
                self.evidence.report["status"] = status
                self.reject()

    def test_missing_matching_eio_receipt_rejects(self):
        event = next(event for event in self.evidence.journals["a"] if event["operation"] == "write")
        event["errno"] = 0
        self.reject()

    def test_held_write_error_in_force_or_close_cannot_replace_write_error(self):
        for phase in ("open", "position", "force", "close"):
            with self.subTest(phase=phase):
                self.evidence.row("a.fault-write")["response"]["phase"] = phase
                self.reject()

    def test_runtime_exception_disguised_as_io_exception_rejects(self):
        self.evidence.row("a.fault-write")["response"]["exceptionClass"] = "java.lang.RuntimeException"
        self.reject()

    def test_actual_create_collision_does_not_prove_injected_eio(self):
        self.evidence.row("a.fault-create")["response"]["exceptionClass"] = "java.nio.file.FileAlreadyExistsException"
        self.reject()

    def test_foreign_control_identity_rejects(self):
        self.evidence.row("a.arm")["response"]["storageId"] = "another_storage"
        self.reject()

    def test_stale_arm_revision_rejects(self):
        self.evidence.row("a.arm")["response"]["revision"] = 0
        self.reject()

    def test_control_ack_must_match_its_own_journal_receipt(self):
        self.evidence.row("a.heal")["response"]["sequence"] = self.evidence.row("a.arm")["response"]["sequence"]
        self.reject()

    def test_control_sequence_zero_cannot_alias_the_last_status_receipt(self):
        self.evidence.row("a.before-death")["response"]["sequence"] = 0
        self.reject()

    def test_control_daemon_pid_must_match_the_captured_incarnation(self):
        self.evidence.row("a.arm")["response"]["daemonPid"] = 999
        self.reject()

    def test_fault_receipt_from_the_wrong_revision_rejects(self):
        next(event for event in self.evidence.journals["a"] if event["operation"] == "write")["revision"] = 2
        self.reject()

    def test_fault_receipt_cannot_claim_success_or_returned_bytes(self):
        event = next(event for event in self.evidence.journals["a"] if event["operation"] == "write")
        event.update(result=1, returnedBytes=1)
        self.reject()

    def test_wrong_jvm_uid_or_effective_capability_rejects(self):
        for key, value in (("uid", [0] * 4), ("gid", [0] * 4), ("capEff", "0000000000200000")):
            with self.subTest(key=key):
                self.evidence = Evidence()
                self.evidence.row("a.hello")["response"][key] = value
                self.reject()

    def test_direct_backing_success_rejects(self):
        for label in ("a.raw-read", "b.raw-write"):
            with self.subTest(label=label):
                self.evidence = Evidence()
                self.evidence.row(label)["response"]["status"] = "ok"
                self.reject()

    def test_missing_raw_path_is_not_proof_of_access_denial(self):
        self.evidence.row("a.raw-read")["response"]["exceptionClass"] = "java.nio.file.NoSuchFileException"
        self.reject()

    def test_lost_cross_mount_observation_rejects(self):
        self.evidence.report["observations"].remove(self.evidence.row("b.peer-read"))
        self.reject()

    def test_healed_cross_mount_bytes_must_include_the_suffix(self):
        self.evidence.row("b.healed-peer-read")["response"]["sha256"] = sha(BASE)
        self.reject()

    def test_read_from_the_local_path_cannot_fake_peer_visibility(self):
        row = self.evidence.row("a.peer-read")
        row["request"] = row["request"].replace("held-b", "held-a")
        row["response"]["path"] = "held-a"
        self.reject()

    def test_missing_cleanup_and_duplicate_resource_cleanup_reject(self):
        self.evidence.report["cleanup"].pop()
        self.reject()
        self.evidence = Evidence()
        self.evidence.report["cleanup"][-1] = copy.deepcopy(self.evidence.report["cleanup"][0])
        self.reject()

    def test_cleanup_error_is_preserved(self):
        self.evidence.report["cleanup"][0]["error"] = "container removal failed"
        self.reject()

    def test_lost_receipt_and_incomplete_journal_reject(self):
        for field, value in (("sequence", 9), ("journalComplete", False), ("attemptId", "foreign")):
            with self.subTest(field=field):
                self.evidence = Evidence()
                self.evidence.journals["a"][0][field] = value
                self.reject()

    def test_partial_last_journal_line_is_not_accepted(self):
        self.evidence.write(self.output)
        journal = self.output / "a-journal.jsonl"
        journal.write_bytes(journal.read_bytes().rstrip(b"\n"))
        self.evidence.report["instances"]["a"]["journalSha256"] = sha(journal.read_bytes())
        with self.assertRaises(ValueError):
            probe.qualify(self.evidence.report, self.output)

    def test_duplicate_json_fields_in_journal_reject(self):
        self.evidence.write(self.output)
        journal = self.output / "a-journal.jsonl"
        journal.write_bytes(journal.read_bytes().replace(b'"sequence": 1,', b'"sequence": 1, "sequence": 1,', 1))
        self.evidence.report["instances"]["a"]["journalSha256"] = sha(journal.read_bytes())
        with self.assertRaises(ValueError):
            probe.qualify(self.evidence.report, self.output)

    def test_both_force_modes_need_positive_native_sync_counters(self):
        for operation, count in (("fsync", 1), ("fdatasync", 0)):
            with self.subTest(operation=operation):
                self.evidence = Evidence()
                for event in self.evidence.journals["a"]:
                    if "counters" in event:
                        event["counters"][operation]["count"] = min(event["counters"][operation]["count"], count)
                for row in self.evidence.report["observations"]:
                    if row["instance"] == "a" and row["kind"] == "control":
                        counters = row["response"]["counters"]
                        counters[operation]["count"] = min(counters[operation]["count"], count)
                self.reject()

    def test_successful_nonfamily_callbacks_do_not_have_individual_receipts(self):
        for operation in ("fsync", "open", "opendir", "mkdir", "rename", "rename-target", "unlink", "rmdir"):
            with self.subTest(operation=operation):
                self.evidence = Evidence()
                event = copy.deepcopy(self.evidence.journals["a"][-1])
                event.update(operation=operation, sequence=len(self.evidence.journals["a"]) + 1,
                             result=0, errno=0, path=NAMESPACE + "/ordinary")
                event.pop("counters")
                self.evidence.journals["a"].append(event)
                self.reject()

    def test_new_probe_requires_sync_counters_even_if_all_java_force_calls_succeed(self):
        for event in self.evidence.journals["a"]:
            if "counters" in event:
                event["counters"].pop("fsync"); event["counters"].pop("fdatasync")
        for row in self.evidence.report["observations"]:
            if row["instance"] == "a" and row["kind"] == "control":
                row["response"]["counters"].pop("fsync"); row["response"]["counters"].pop("fdatasync")
        self.reject()

    def test_missing_or_unsafe_effective_cache_configuration_rejects(self):
        ready = next(event for event in self.evidence.journals["a"] if event["operation"] == "READY")
        ready["reason"] = ready["reason"].replace("writeback_cache=0", "writeback_cache=1")
        self.reject()

    def test_outside_namespace_requires_real_access_denial_and_unprivileged_scope_process(self):
        for label, key, value in (("a.outside", "exceptionClass", "java.nio.file.NoSuchFileException"),
                                  ("a.outside", "status", "rejected"),
                                  ("a.scope-hello", "uid", [0] * 4)):
            with self.subTest(label=label, key=key):
                self.evidence = Evidence()
                self.evidence.row(label)["response"][key] = value
                self.reject()

    def test_physical_image_privilege_device_and_host_mount_mismatches_reject(self):
        mutations = [lambda item: item.update(Image="sha256:" + "9" * 64),
                     lambda item: item["HostConfig"].update(Privileged=True),
                     lambda item: item["HostConfig"].update(CapAdd=["SYS_ADMIN", "NET_ADMIN"]),
                     lambda item: item["HostConfig"].update(PidMode="host"),
                     lambda item: item["HostConfig"].update(IpcMode="host"),
                     lambda item: item["HostConfig"].update(Binds=["/tmp:/host"]),
                     lambda item: item["HostConfig"]["Devices"][0].update(PathOnHost="/dev/sda"),
                     lambda item: item["Mounts"][0].update(Type="bind", Source="/tmp"),
                     lambda item: item["Mounts"][0].update(Name="foreign-volume")]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                self.evidence = Evidence()
                mutate(self.evidence.report["instances"]["a"]["inspection"])
                self.reject()

    def test_docker_inspection_cap_sys_admin_spelling_qualifies(self):
        for item in self.evidence.report["instances"].values():
            item["inspection"]["HostConfig"]["CapAdd"] = ["CAP_SYS_ADMIN"]
        self.assertEqual(7, len(self.qualify()))

    def test_capability_alias_does_not_allow_extras_duplicates_or_mixed_aliases(self):
        for capabilities in (["CAP_SYS_ADMIN", "CAP_NET_ADMIN"], ["CAP_SYS_ADMIN", "CAP_KILL"],
                             ["CAP_SYS_ADMIN", "CAP_SYS_ADMIN"], ["SYS_ADMIN", "SYS_ADMIN"],
                             ["SYS_ADMIN", "CAP_SYS_ADMIN"], ["CAP_ALL"], []):
            with self.subTest(capabilities=capabilities):
                self.evidence = Evidence()
                self.evidence.report["instances"]["a"]["inspection"]["HostConfig"]["CapAdd"] = capabilities
                self.reject()

    def test_cancel_and_watchdog_must_end_with_terminal_abort_receipts(self):
        for label in ("c.abort", "d.abort"):
            with self.subTest(label=label):
                self.evidence = Evidence()
                self.evidence.row(label)["response"]["state"] = "HEALTHY"
                self.reject()

    def test_dead_daemon_must_fail_io_and_leave_java_alive(self):
        self.evidence.row("a.dead-read")["response"]["status"] = "ok"
        self.reject()
        self.evidence = Evidence()
        self.evidence.report["observations"].remove(self.evidence.row("a.still-alive"))
        self.reject()

    def test_scope_guard_rejects_parent_traversal_absolute_escape_and_symlinks(self):
        scope = self.output / "scope"
        scope.mkdir()
        outside = self.output / "outside"
        outside.mkdir()
        (scope / "link").symlink_to(outside, target_is_directory=True)
        for path in (scope / ".." / "outside", outside, scope / "link" / "file"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                probe.guarded(path, scope)
        self.assertEqual(scope / "new", probe.guarded(scope / "new", scope))


class SupervisorLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name).resolve()

    def supervisor(self, output, failure=None):
        output.mkdir()
        evidence = Evidence()
        evidence.write(output)
        owner = probe.Probe.__new__(probe.Probe)
        owner.output, owner.attempt, owner.volume = output, ATTEMPT, "fsi-" + ATTEMPT
        owner.args = SimpleNamespace(image_id=IMAGE, platform="linux/arm64")
        owner.report = evidence.report
        owner.containers = {"a": "4" * 64}
        owner.faults = {"a": "write_eio"}
        owner.control = Mock()
        owner.cleanup = Mock()
        owner.run_cases = Mock(side_effect=failure)

        def command(*args, **kwargs):
            if args[0] == "version":
                result = {"Version": "29.0.0", "Os": "linux", "Arch": "arm64"}
            elif args[:2] == ("image", "inspect"):
                result = [evidence.report["image"]]
            elif args[:2] == ("volume", "inspect"):
                result = [{"Name": owner.volume, "Labels": {probe.LABEL: ATTEMPT}}]
            elif args[:2] == ("volume", "create"):
                return 0, owner.volume.encode(), b""
            else:
                raise AssertionError("Unexpected mocked Docker request: " + repr(args))
            return 0, json.dumps(result).encode(), b""

        owner.docker = Mock(run=Mock(side_effect=command), cleanup_deadline=None)
        return owner

    def test_interruption_and_deadline_preserve_aborted_status_even_with_complete_successful_io_evidence(self):
        for index, failure in enumerate((InterruptedError("cancelled"), KeyboardInterrupt(), TimeoutError("deadline"))):
            with self.subTest(failure=type(failure).__name__):
                owner = self.supervisor(self.output / str(index), failure)
                with patch.object(probe, "qualify", wraps=probe.qualify) as qualify:
                    result = owner.execute()
                self.assertEqual("aborted", result["status"])
                self.assertIn("did not complete normally", result["qualificationError"])
                qualify.assert_called_once()
                owner.cleanup.assert_called_once()
                owner.control.assert_called_once_with("a", "a.supervisor-abort", "ABORT", "write_eio",
                                                       check=False, cleanup=True)
                self.assertIsNotNone(owner.docker.cleanup_deadline)
                self.assertEqual("aborted", json.loads((owner.output / "result.json").read_text())["status"])
                self.assertEqual("aborted", json.loads((owner.output / "observations.json").read_text())["status"])

    def test_failure_and_unsupported_remain_nonpass_and_always_cleanup(self):
        for index, (failure, expected) in enumerate(((RuntimeError("fixture failure"), "failed"),
                                                   (probe.Unsupported("no FUSE"), "unsupported"))):
            with self.subTest(expected=expected):
                owner = self.supervisor(self.output / str(index), failure)
                result = owner.execute()
                self.assertEqual(expected, result["status"])
                owner.cleanup.assert_called_once()
                self.assertIn("error", result)
                self.assertIn("qualificationError", result)

    def test_cleanup_error_prevents_observed_run_from_becoming_pass(self):
        owner = self.supervisor(self.output / "cleanup-failure")
        owner.cleanup.side_effect = lambda: owner.report["cleanup"].append({"error": "retained resource"})
        result = owner.execute()
        self.assertEqual("failed", result["status"])
        self.assertEqual("Cleanup failed", result["qualificationError"])
        self.assertEqual("observed", json.loads((owner.output / "observations.json").read_text())["status"])

    def test_malformed_qualification_shape_still_writes_failure_report(self):
        owner = self.supervisor(self.output / "malformed-shape")
        owner.run_cases.side_effect = lambda: owner.report.update(instances=None)
        result = owner.execute()
        self.assertEqual("failed", result["status"])
        self.assertIn("qualificationError", result)
        self.assertEqual("failed", json.loads((owner.output / "result.json").read_text())["status"])
        owner.cleanup.assert_called_once()

    def test_cleanup_refuses_unrelated_container_and_volume_before_any_mutation(self):
        owner = probe.Probe.__new__(probe.Probe)
        owner.output, owner.attempt, owner.volume = self.output, ATTEMPT, "fsi-" + ATTEMPT
        owner.names, owner.instances, owner.sessions = {"a": "owned-looking-name"}, {}, []
        owner.report = {"cleanup": []}

        def inspect(*args, **kwargs):
            if args[0] == "inspect":
                result = [{"Id": "9" * 64, "Config": {"Labels": {probe.LABEL: "foreign_attempt"}}}]
            elif args[:2] == ("volume", "inspect"):
                result = [{"Name": owner.volume, "Labels": {probe.LABEL: "foreign_attempt"}}]
            else:
                raise AssertionError("Cleanup attempted an unexpected operation: " + repr(args))
            return 0, json.dumps(result).encode(), b""

        owner.docker = Mock(run=Mock(side_effect=inspect), cleanup_deadline=None)
        owner.cleanup()
        self.assertEqual(2, owner.docker.run.call_count)
        self.assertEqual(2, len(owner.report["cleanup"]))
        self.assertTrue(all("Refusing unrelated" in row["error"] for row in owner.report["cleanup"]))
        self.assertFalse(any(row.get("removed") for row in owner.report["cleanup"]))
        self.assertIsNotNone(owner.docker.cleanup_deadline)

    def test_operation_and_cleanup_budgets_stop_before_starting_another_process(self):
        docker = probe.Docker(self.output, "unix:///tmp/fixture-docker.sock", ATTEMPT)
        docker.deadline = 0
        with patch.object(probe.subprocess, "Popen") as popen:
            with self.assertRaises(TimeoutError):
                docker.run("inspect", "fixture")
            docker.cleanup_deadline = 0.1
            with self.assertRaises(TimeoutError):
                docker.run("rm", "fixture", cleanup=True)
            popen.assert_not_called()
        self.assertEqual([], docker.commands)

    def test_cleanup_gets_a_separate_bounded_budget_after_the_operation_deadline(self):
        docker = probe.Docker(self.output, "unix:///tmp/fixture-docker.sock", ATTEMPT)
        docker.deadline = 0
        docker.cleanup_deadline = probe.time.monotonic() + 10
        process = Mock(returncode=0)
        process.poll.return_value = 0
        with patch.object(probe.subprocess, "Popen", return_value=process) as popen:
            self.assertEqual((0, b"", b""), docker.run("rm", "fixture", cleanup=True))
            popen.assert_called_once()
        self.assertEqual(1, len(docker.commands))
        self.assertEqual(0, docker.commands[0]["exitCode"])

    def test_hung_transport_is_killed_and_retains_interrupted_command_receipt(self):
        docker = probe.Docker(self.output, "unix:///tmp/fixture-docker.sock", ATTEMPT)
        docker.deadline = 1000
        process = Mock(pid=12345, returncode=-9)
        process.poll.return_value = None
        process.wait.return_value = -9
        with patch.object(probe.subprocess, "Popen", return_value=process), \
                patch.object(probe.time, "monotonic", side_effect=[100, 100, 200]), \
                patch.object(probe.os, "killpg") as kill:
            with self.assertRaises(TimeoutError):
                docker.run("inspect", "fixture", timeout=1)
            kill.assert_called_once_with(12345, probe.signal.SIGKILL)
            process.wait.assert_called_once_with(timeout=3)
        self.assertTrue(docker.commands[0]["interrupted"])
        self.assertEqual(-9, docker.commands[0]["exitCode"])
        retained = json.loads((self.output / "commands" / "0001.json").read_text())
        self.assertTrue(retained["interrupted"])

    def test_remote_docker_endpoint_is_rejected_without_constructing_a_client(self):
        with self.assertRaises(ValueError):
            probe.Docker(self.output, "tcp://example.invalid:2375", ATTEMPT)
        self.assertFalse((self.output / "docker-client").exists())

    def test_explicit_docker_binary_preserves_isolated_client_configuration(self):
        binary = self.output / "selected-docker"
        binary.write_text("generated executable fixture\n")
        binary.chmod(0o700)
        docker = probe.Docker(self.output, "unix:///tmp/fixture-docker.sock", ATTEMPT, docker=binary)
        self.assertEqual(str(binary), docker.prefix[0])
        self.assertEqual(["--config", str(self.output / "docker-client"), "--host",
                          "unix:///tmp/fixture-docker.sock"], docker.prefix[1:])
        self.assertEqual("{}\n", (self.output / "docker-client/config.json").read_text())
        self.assertNotIn("DOCKER_CONTEXT", docker.environment)

    def test_explicit_docker_path_rejects_relative_personal_or_nonexecutable_inputs(self):
        for value in ("relative/docker", "/Users/example/Documents/not-read"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                probe.docker_executable(value)
        binary = self.output / "not-executable"
        binary.write_text("generated fixture")
        binary.chmod(0o600)
        with self.assertRaises(ValueError):
            probe.docker_executable(binary)
        self.assertEqual("docker", probe.docker_executable(None))

    def test_docker_cli_option_reaches_the_probe_without_context_discovery(self):
        binary = self.output / "selected-docker"
        with patch.object(probe, "Probe") as owner, patch("builtins.print"):
            owner.return_value.execute.return_value = {"status": "pass"}
            result = probe.main(["--bundle", str(self.output / "bundle"), "--output", str(self.output / "run"),
                                 "--image-id", IMAGE, "--platform", "linux/arm64",
                                 "--docker-host", "unix:///tmp/fixture-docker.sock", "--docker", str(binary)])
        self.assertEqual(0, result)
        self.assertEqual(binary, owner.call_args.args[0].docker)


if __name__ == "__main__":
    unittest.main()
