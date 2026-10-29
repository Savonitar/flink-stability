"""Independent synthetic receipts for the state-recovery qualification contract; no containers."""
import argparse
import importlib.util
from io import BytesIO
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile

RECIPE = Path(__file__).absolute().parents[2] / "calibration" / "state-recovery"
spec = importlib.util.spec_from_file_location("state_recovery_check", RECIPE / "check.py")
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)

CONNECTOR = "a" * 64
CORRECT = "b" * 64
MUTANT = "c" * 64
SOURCE = "/opt/flink/lib/000-" + CONNECTOR + ".jar"


def marker(event, number, restored=False, mode="correct", progress=None, key=None):
    return {"process": {"logicalName": "taskmanager-1", "runtimeId": "tm-b" if restored else "tm-a",
                        "process": "taskmanager-1#2" if restored else "taskmanager-1#1"},
            "event": event, "jobId": "job-123", "operatorUid": "keyed-prefix-v1", "executionAttempt": 1 if restored else 0,
            "subtask": 0, "checkpointId": 42 if restored or event in ("SNAPSHOT", "COMPLETED") else -1,
            "restoredCheckpointId": 42 if restored else -1, "lineNumber": number,
            "progress": progress, "keyProgress": key,
            "buildMode": ("reset-state-on-restore-v1" if mode == "mutant" else "correct") if event == "START" else None}


def fixture(mode="correct", execution="uninterrupted", nonce="00000001"):
    restart = execution == "restart"
    bad_state = mode == "mutant" and restart
    progress = {"keyCount": 8, "observedKeys": 8, "minimumObservedCount": 2,
                "processedThisAttempt": 16, "maximumInputId": 15}
    initial = marker("START", 1, mode=mode)
    saved = marker("SNAPSHOT", 2, mode=mode, progress=progress)
    completed = marker("COMPLETED", 3, mode=mode)
    markers = [initial, saved, completed]
    target = {"logicalName": "taskmanager-1", "runtimeId": "tm-a", "resourceId": "resource-a"}
    replacement = {"logicalName": "taskmanager-1", "runtimeId": "tm-b", "resourceId": "resource-b"}
    recoveries, kills, restarts = [], [], []
    if restart:
        start = marker("START", 1, True, mode)
        markers.append(start)
        restored_keys, resets = [], []
        line = 2
        for key in range(8):
            value = {"keyCount": 8, "key": key, "inputId": 16 + key, "previousCount": 2,
                     "previousSum": 10 + 2 * key, "newCount": 1 if bad_state else 3,
                     "newSum": 17 + key if bad_state else 27 + 3 * key}
            restored_key = marker("RESTORED_KEY", line, True, mode, key=value)
            restored_keys.append(restored_key)
            markers.append(restored_key)
            line += 1
            if bad_state:
                reset = marker("MUTANT_RESET", line, True, mode, key=value)
                markers.append(reset)
                resets.append(reset)
                line += 1
        after = marker("PROGRESS", line, True, mode, progress={"keyCount": 8, "observedKeys": 8,
                       "minimumObservedCount": 1 if bad_state else 3, "processedThisAttempt": 8, "maximumInputId": 23})
        markers.append(after)
        readiness = {"ready": True, "reason": "ready", "jobId": "job-123", "keyCount": 8, "totalRecords": 6000,
                     "target": target, "executionAttempt": 0, "jobObservation": {"state": "RUNNING", "jobManagerTimeMillis": 105, "completedCheckpoints": 1},
                     "completedSnapshots": {"42": {"checkpointId": 42, "acknowledgedAtMillis": 100,
                                                   "snapshot": saved, "completion": completed}},
                     "committedPrefix": {"snapshotComplete": True, "validStatePrefix": True, "observedRecords": 16,
                                         "maximumId": 15, "perKeyCounts": {str(key): 2 for key in range(8)},
                                         "beginningOffsets": {"0": 0}, "endOffsets": {"0": 18}}}
        recoveries = [{"confirmed": True, "reason": "confirmed", "readiness": readiness, "restoredCheckpointId": 42,
                       "restoredStart": start, "postRestoreProgress": after, "restoredKeys": restored_keys, "mutantResets": resets}]
        kills = [{"confirmed": True, "jobStateBeforeKill": "RUNNING", "restoredCheckpoint": 42,
                  "identity": target, "jobManagerTimeBeforeKill": 110, "jobManagerTimeAfterKill": 115, "restoredAtMillis": 120}]
        restarts = [{"previousIdentity": target, "replacementIdentity": replacement}]
    status = "fail" if bad_state else "pass"
    reason = "validator.kafka.state-prefix.state-mismatch" if bad_state else "validator.kafka.state-prefix.match"
    samples = []
    for event_id in range(16, 116):
        key = event_id % 8
        count = event_id // 8 + 1
        expected_sum = count * (key + 1) + 8 * count * (count - 1) // 2
        samples.append({"partition": 0, "offset": event_id + 4, "id": event_id,
                        "expectedKey": key, "actualKey": key, "expectedCount": count,
                        "actualCount": count - 2, "expectedSum": expected_sum,
                        "actualSum": expected_sum - (10 + 2 * key)})
    component_ids = ["jm-a", "tm-a"] + (["tm-b"] if restart else [])
    components = [{"logicalName": "jobmanager-1" if value.startswith("jm") else "taskmanager-1", "runtimeId": value,
                   "imageReference": "flink:2.2.0", "imageId": "sha256:" + "d" * 64, "connectorArtifactsRef": 0}
                  for value in component_ids]
    result = {"scenario": "state-recovery", "status": status, "reason": "scenario.expected-pass-mismatch" if bad_state else reason,
              "attempt": {"nonce": nonce, "status": status, "reason": reason},
              "expectation": {"outcome": "pass", "matched": not bad_state}, "diagnostics": [],
              "evidence": {
                  "terminalValidation": {"type": "kafka.state-prefix", "mode": "exactly-once", "completed": True,
                      "snapshotComplete": True, "expected": 6000, "observed": 6000, "distinctExpected": 6000,
                      "missing": 0, "duplicates": 0, "unexpected": 0, "malformed": 0, "status": status, "reason": reason,
                      "statePrefix": {"keyCount": 8, "mismatches": 5984 if bad_state else 0,
                                      "mismatchSamples": samples if bad_state else []}},
                  "input": {"status": "complete", "complete": True, "reconciliationComplete": True,
                            "records": 6000, "observed": 6000, "partitions": 1},
                  "phases": {"completed": True, "failed": 0},
                  "writeFence": {"completed": True, "finalState": "FINISHED"},
                  "flinkJob": {"state": "FINISHED", "restoredCheckpoints": 1 if restart else 0},
                  "processFence": {"status": "complete", "completed": True, "observationOverflow": False,
                                   "health": {"outcome": "healthy"},
                                   "components": [{"runtimeId": value, "outcome": "sigkilled"} for value in component_ids]},
                  "flinkRuntime": {"status": "confirmed", "components": components,
                                   "connectorArtifactSets": [[{"containerPath": SOURCE, "sha256": CONNECTOR}]]},
                  "connectorPrimaries": [{"alias": "kafka", "observedSha256": CONNECTOR}],
                  "kafka": {"imageReference": "apache/kafka:4.0.0", "imageId": "sha256:" + "e" * 64},
                  "subjectClasses": {"status": "confirmed", "expectedSource": SOURCE,
                                     "processes": [{"process": "taskmanager-1#" + str(number),
                                                    "sources": {entry: [SOURCE] for entry in check.ENTRY_CLASSES}}
                                                   for number in range(1, 3 if restart else 2)]},
                  "sinkTransactions": {"status": "listed", "unresolved": []},
                  "stateRecovery": {"jobId": "job-123", "workloadSha256": MUTANT if mode == "mutant" else CORRECT,
                                    "allowStateResetMutant": mode == "mutant",
                                    "buildMode": "reset-state-on-restore-v1" if mode == "mutant" else "correct",
                                    "keyCount": 8, "totalRecords": 6000, "physicalFenceConfirmed": True,
                                    "confirmed": True, "reason": "confirmed",
                                    "trace": {"markers": markers, "diagnostics": [], "complete": True}, "recoveries": recoveries},
                  "taskManagerKills": kills, "taskManagerRestarts": restarts,
                  "componentErrors": {"eventCount": 1 if restart else 0, "events": ["declared-kill"] if restart else []},
                  "flinkRest": {"errorCount": 0, "errors": []}}}
    manifest = {"workload": {"keyCount": 8, "totalRecords": 6000},
                "artifacts": {"correct": {"sha256": CORRECT}, "mutant": {"sha256": MUTANT}},
                "runtimeIdentity": check.runtime_identity(result)}
    cell = {"name": mode + "-" + execution, "mode": mode, "execution": execution}
    return result, 1 if bad_state else 0, cell, manifest


def write_marker_logs(directory, markers):
    outputs = {}
    for item in markers:
        fields = {"version": 1, "jobId": item["jobId"], "operatorUid": item["operatorUid"],
                  "attempt": item["executionAttempt"], "subtask": item["subtask"],
                  "checkpoint": item["checkpointId"], "restoredCheckpoint": item["restoredCheckpointId"]}
        fields.update(item["progress"] or item["keyProgress"] or {})
        if item["buildMode"]:
            fields["buildMode"] = item["buildMode"]
        process = item["process"]["process"].replace("#", "-")
        outputs.setdefault(process, []).append("STATE_PREFIX_" + item["event"] + " "
                                               + " ".join(f"{key}={value}" for key, value in fields.items()))
    (directory / "logs").mkdir()
    for process, lines in outputs.items():
        (directory / "logs" / ("flink-stability-output-" + process + ".log")).write_text("\n".join(lines) + "\n")


def matrix_fixture(batch):
    def put(name, data):
        if not isinstance(data, bytes):
            data = (json.dumps(data, indent=2) + "\n").encode()
        path = batch / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return {"path": name, "sha256": check.digest(data)}

    artifacts = {}
    for mode in ("correct", "mutant"):
        buffer = BytesIO()
        build_mode = "correct" if mode == "correct" else "reset-state-on-restore-v1"
        with ZipFile(buffer, "w") as jar:
            jar.writestr("Example.class", b"same-workload-code")
            jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\r\nBuild: " + build_mode + "\r\n")
            jar.writestr("META-INF/flink-stability/keyed-prefix-build.properties", "mode=" + build_mode + "\n")
        artifacts[mode] = put("artifacts/" + mode + ".jar", buffer.getvalue())
    build_source = put("evidence/build-sources/example.java", b"generated source fixture\n")
    build = {"version": 1, "mutation": "reset-state-on-restore-v1", "releaseBytecodeTarget": 11,
             "mutationScope": "Keyed aggregate only; once per key on first event after genuine restore; initial start, source and sink unchanged",
             "artifacts": artifacts, "changedEntries": ["META-INF/MANIFEST.MF", "META-INF/flink-stability/keyed-prefix-build.properties"],
             "recipeInputSha256": {"example.java": build_source["sha256"]}}
    reference = fixture()[0]
    manifest = {"formatVersion": 1, "catalogContractVersion": 2, "kind": "state-recovery-calibration", "attemptsPerCell": 3,
                "workload": {"keyCount": 8, "totalRecords": 6000}, "artifacts": artifacts,
                "buildEvidence": put("evidence/build-evidence.json", build),
                "referenceResult": put("evidence/reference-result.json", reference),
                "runtimeIdentity": check.runtime_identity(reference), "recipeFiles": {build_source["path"]: build_source["sha256"]},
                "cells": []}
    for name in ("baseline.yaml", "restart-phase.yaml", "state-recovery.expected.yaml", "build.py", "catalogs.py", "check.py", "run.py", "README.md"):
        receipt = put("evidence/recipe/" + name, b"independent recipe fixture\n")
        manifest["recipeFiles"][receipt["path"]] = receipt["sha256"]
    ordinal = 0
    for mode in ("correct", "mutant"):
        for execution in ("uninterrupted", "restart"):
            name = mode + "-" + execution
            catalog = "catalogs/" + name
            scenario = put(catalog + "/state-recovery.yaml", b"format: v1\nkind: scenario\n")
            expected = put(catalog + "/state-recovery.expected.yaml",
                           b"format: v1\nkind: expected-result\n\nmeta:\n  name: state-recovery.expected\n  scenario: state-recovery\n\ndefault:\n  outcome: pass\n")
            attempts = []
            for number in range(1, 4):
                ordinal += 1
                directory = "attempts/" + name + "/run-" + str(number)
                attempts.append(directory)
                result, code, unused, unused_manifest = fixture(mode, execution, f"{ordinal:08x}")
                result["evidence"]["stateRecovery"]["workloadSha256"] = artifacts[mode]["sha256"]
                put(directory + "/stdout.json", result)
                put(directory + "/stderr.log", b"retained stderr\n")
                put(directory + "/exit-code.txt", (str(code) + "\n").encode())
                write_marker_logs(batch / directory, result["evidence"]["stateRecovery"]["trace"]["markers"])
            manifest["cells"].append({"name": name, "mode": mode, "execution": execution, "scenario": "state-recovery",
                                      "catalog": catalog, "scenarioSha256": scenario["sha256"], "expectedSha256": expected["sha256"],
                                      "requiredOutcome": "pass", "attempts": attempts})
    put("manifest.json", manifest)
    return manifest


class StateRecoveryCheckerTest(unittest.TestCase):
    def test_workload_opt_in_and_observed_mode_are_explicit(self):
        for mode in ("correct", "mutant"):
            args = fixture(mode, "uninterrupted")
            row = check.audit_attempt(*args)
            self.assertEqual(mode == "mutant", row["allowStateResetMutant"])
            for field in ("buildMode", "allowStateResetMutant"):
                missing = fixture(mode, "uninterrupted")
                del missing[0]["evidence"]["stateRecovery"][field]
                with self.assertRaisesRegex(ValueError, "opt-in"):
                    check.audit_attempt(*missing)
            args[0]["evidence"]["stateRecovery"]["allowStateResetMutant"] = mode != "mutant"
            with self.assertRaisesRegex(ValueError, "opt-in"):
                check.audit_attempt(*args)

    def test_runner_pins_both_docker_clients_without_shell_evaluation(self):
        spec = importlib.util.spec_from_file_location("state_recovery_runner", RECIPE / "run.py")
        runner = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"check": check}):
            spec.loader.exec_module(runner)
        arguments = (Path("/generated/harness"), Path("/generated/batch"),
                     {"catalog": "catalogs/correct-restart", "scenario": "state-recovery"},
                     Path("/generated/attempt"), "mvn", Path("/generated/repository"), Path("/generated/settings.xml"))
        explicit = runner.command(*arguments, docker_api_version="1.44")
        self.assertEqual(explicit[:3], ["/usr/bin/env", "DOCKER_API_VERSION=1.44", "mvn"])
        self.assertEqual(explicit.count("-Dapi.version=1.44"), 1)
        default = runner.command(*arguments)
        self.assertEqual(default[0], "mvn")
        self.assertFalse(any("DOCKER_API_VERSION" in item or item.startswith("-Dapi.version=") for item in default))
        for invalid in ("", "1", "1.44.0", "1.44 --flag", "1.44\n", "${VERSION}"):
            with self.subTest(version=invalid), self.assertRaises(argparse.ArgumentTypeError):
                runner.command(*arguments, docker_api_version=invalid)

    def test_all_four_cells_preserve_raw_outcomes_and_component_errors(self):
        for mode in ("correct", "mutant"):
            for execution in ("uninterrupted", "restart"):
                args = fixture(mode, execution)
                result = check.audit_attempt(*args)
                self.assertTrue(result["confirmed"])
                self.assertEqual(result["rawStatus"], "fail" if mode == "mutant" and execution == "restart" else "pass")
                self.assertEqual(result["componentErrors"], args[0]["evidence"]["componentErrors"])

    def test_missing_evidence_cannot_qualify(self):
        for field in ("stateRecovery", "terminalValidation", "processFence", "subjectClasses", "flinkRuntime", "input"):
            args = fixture("correct", "restart")
            del args[0]["evidence"][field]
            with self.subTest(field=field), self.assertRaises((ValueError, KeyError)):
                check.audit_attempt(*args)

    def test_mutant_failure_cannot_be_reclassified_or_hide_id_defects(self):
        for change in (lambda r: r["expectation"].update(outcome="fail", matched=True),
                       lambda r: r["attempt"].update(reason="verification.kafka.incomplete-after-timeout"),
                       lambda r: r["evidence"]["terminalValidation"].update(snapshotComplete=False),
                       lambda r: r["evidence"]["terminalValidation"].update(missing=1),
                       lambda r: r["evidence"]["terminalValidation"].update(duplicates=1),
                       lambda r: r["evidence"]["terminalValidation"].update(mode="at-least-once"),
                       lambda r: r["evidence"]["terminalValidation"]["statePrefix"].update(mismatches=0),
                       lambda r: r["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"][0].update(expectedSum=99)):
            args = fixture("mutant", "restart")
            change(args[0])
            with self.assertRaises(ValueError):
                check.audit_attempt(*args)

    def test_restore_must_match_saved_checkpoint_attempt_and_every_key(self):
        for change in (lambda p: p.update(confirmed=False),
                       lambda p: p.update(restoredCheckpointId=43),
                       lambda p: p["readiness"].update(ready=False),
                       lambda p: p["readiness"]["committedPrefix"].update(snapshotComplete=False),
                       lambda p: p["readiness"]["committedPrefix"]["perKeyCounts"].update({"0": 0}),
                       lambda p: p["restoredStart"].update(executionAttempt=0),
                       lambda p: p["restoredKeys"].pop(),
                       lambda p: p["restoredKeys"][0]["keyProgress"].update(previousSum=99),
                       lambda p: p["restoredKeys"][0]["keyProgress"].update(inputId=0),
                       lambda p: p["postRestoreProgress"]["progress"].update(observedKeys=7)):
            args = fixture("correct", "restart")
            change(args[0]["evidence"]["stateRecovery"]["recoveries"][0])
            with self.assertRaises((ValueError, KeyError)):
                check.audit_attempt(*args)

    def test_mutant_must_activate_once_per_key_after_genuine_restore(self):
        args = fixture("mutant", "restart")
        args[0]["evidence"]["stateRecovery"]["recoveries"][0]["mutantResets"].pop()
        with self.assertRaises(ValueError):
            check.audit_attempt(*args)
        args = fixture("mutant", "uninterrupted")
        args[0]["evidence"]["stateRecovery"]["trace"]["markers"].append(marker("MUTANT_RESET", 4, key={"key": 0}))
        with self.assertRaises(ValueError):
            check.audit_attempt(*args)

    def test_artifact_fence_natural_finish_and_per_incarnation_subject_are_required(self):
        for change in (lambda e: e["stateRecovery"].update(workloadSha256="d" * 64),
                       lambda e: e["writeFence"].update(finalState="CANCELED"),
                       lambda e: e["processFence"].update(completed=False),
                       lambda e: e["processFence"]["components"].pop(),
                       lambda e: e["subjectClasses"]["processes"].pop(),
                       lambda e: e["flinkRuntime"]["components"][0].update(imageId="other")):
            args = fixture("correct", "restart")
            change(args[0]["evidence"])
            with self.assertRaises(ValueError):
                check.audit_attempt(*args)

    def test_declared_killed_incarnation_requires_exact_stopped_observation(self):
        def actual_fence_shape(args):
            evidence = args[0]["evidence"]
            fence = evidence["processFence"]
            fence["components"] = [item for item in fence["components"] if item["runtimeId"] != "tm-a"]
            fence["observations"] = [{"logicalName": "taskmanager-1", "runtimeId": "tm-a",
                "moment": "after-declared-kill", "missing": False, "diagnostic": None,
                "state": {"runtimeId": "tm-a", "running": False, "paused": False, "oomKilled": False}}]
            return fence
        args = fixture("correct", "restart")
        actual_fence_shape(args)
        self.assertTrue(check.audit_attempt(*args)["confirmed"])
        for change in (lambda f: f["observations"].clear(),
                       lambda f: f["observations"][0].update(runtimeId="other"),
                       lambda f: f["observations"][0].update(missing=True),
                       lambda f: f["observations"][0].update(diagnostic="inspection failed"),
                       lambda f: f["observations"][0]["state"].update(running=True),
                       lambda f: f["observations"][0]["state"].update(oomKilled=True)):
            args = fixture("correct", "restart")
            change(actual_fence_shape(args))
            with self.assertRaises(ValueError):
                check.audit_attempt(*args)

    def test_mutant_raw_failure_with_cleanup_failure_does_not_qualify(self):
        for code in ("infrastructure.attempt-cleanup-failed", "infrastructure.prepared-artifact-cleanup-failed"):
            args = fixture("mutant", "restart")
            args[0]["diagnostics"] = [code + ": IOException: generated cleanup failure"]
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, "cleanup failed"):
                check.audit_attempt(*args)
            self.assertEqual(args[0]["attempt"]["reason"], "validator.kafka.state-prefix.state-mismatch")
        with tempfile.TemporaryDirectory() as name:
            batch = Path(name).resolve()
            manifest = matrix_fixture(batch)
            path = batch / manifest["cells"][3]["attempts"][0] / "stdout.json"
            raw = json.loads(path.read_text())
            raw["diagnostics"] = ["infrastructure.attempt-cleanup-failed: IOException: generated failure"]
            path.write_text(json.dumps(raw))
            report = check.check_matrix(batch)
            self.assertFalse(report["qualified"])
            row = next(row for row in report["attempts"] if row["cell"] == "mutant-restart" and row["number"] == 1)
            self.assertEqual(row["rawReason"], "validator.kafka.state-prefix.state-mismatch")
            self.assertEqual(row["diagnostics"], raw["diagnostics"])
            self.assertFalse(row["confirmed"])

    def test_raw_marker_lines_are_bound_to_typed_provenance(self):
        markers = fixture("mutant", "restart")[0]["evidence"]["stateRecovery"]["trace"]["markers"]
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name).resolve()
            write_marker_logs(directory, markers)
            self.assertEqual(len(check.verify_marker_logs(directory, markers)), 2)
            markers[-2]["keyProgress"]["newSum"] = 99
            with self.assertRaises(ValueError):
                check.verify_marker_logs(directory, markers)

    def test_rejects_duplicate_json_and_symlink_or_traversal_evidence(self):
        with self.assertRaises(ValueError):
            json.loads('{"a":1,"a":2}', object_pairs_hook=check.unique_object)
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name).resolve()
            data = directory / "file.json"
            data.write_text("{}")
            (directory / "link.json").symlink_to(data)
            with self.assertRaises(ValueError):
                check.read(directory / "link.json", directory)
            with self.assertRaises(ValueError):
                check.read(directory / ".." / "file.json", directory)

    def test_matrix_requires_twelve_distinct_confirmed_attempts(self):
        with tempfile.TemporaryDirectory() as name:
            batch = Path(name).resolve()
            manifest = matrix_fixture(batch)
            report = check.check_matrix(batch)
            self.assertTrue(report["qualified"], report["errors"])
            self.assertEqual(report["confirmedCounts"], {cell: 3 for cell in check.CELLS})
            self.assertEqual(len(report["attempts"]), 12)
            lost = batch / manifest["cells"][3]["attempts"][2] / "stdout.json"
            lost.unlink()
            report = check.check_matrix(batch)
            self.assertFalse(report["qualified"])
            self.assertEqual(report["confirmedCounts"]["mutant-restart"], 2)
            self.assertEqual(len(report["attempts"]), 12)

    def test_window_miss_is_retained_with_raw_mutant_failure_not_retried_or_passed(self):
        with tempfile.TemporaryDirectory() as name:
            batch = Path(name).resolve()
            manifest = matrix_fixture(batch)
            path = batch / manifest["cells"][3]["attempts"][0] / "stdout.json"
            raw = json.loads(path.read_text())
            raw["evidence"]["stateRecovery"]["recoveries"][0]["readiness"]["ready"] = False
            path.write_text(json.dumps(raw))
            report = check.check_matrix(batch)
            self.assertFalse(report["qualified"])
            row = next(row for row in report["attempts"] if row["cell"] == "mutant-restart" and row["number"] == 1)
            self.assertFalse(row["confirmed"])
            self.assertEqual(row["rawReason"], "validator.kafka.state-prefix.state-mismatch")
            self.assertGreater(row["terminalValidation"]["statePrefix"]["mismatches"], 0)

    def test_tampered_artifact_recipe_and_expected_outcome_fail_closed(self):
        for name in ("artifacts/mutant.jar", "evidence/recipe/build.py", "catalogs/mutant-restart/state-recovery.expected.yaml"):
            with self.subTest(path=name), tempfile.TemporaryDirectory() as directory:
                batch = Path(directory).resolve()
                matrix_fixture(batch)
                with (batch / name).open("ab") as stream:
                    stream.write(b"tamper")
                with self.assertRaises(ValueError):
                    check.validate_manifest(batch)

    def test_duplicate_attempt_nonce_does_not_count_twice(self):
        with tempfile.TemporaryDirectory() as name:
            batch = Path(name).resolve()
            manifest = matrix_fixture(batch)
            path = batch / manifest["cells"][0]["attempts"][1] / "stdout.json"
            result = json.loads(path.read_text())
            result["attempt"]["nonce"] = "00000001"
            path.write_text(json.dumps(result))
            report = check.check_matrix(batch)
            self.assertFalse(report["qualified"])
            self.assertEqual(report["confirmedCounts"]["correct-uninterrupted"], 2)

    def test_catalog_freeze_preserves_pass_and_rejects_overwriting(self):
        spec = importlib.util.spec_from_file_location("state_recovery_catalogs", RECIPE / "catalogs.py")
        catalogs = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"check": check}):
            spec.loader.exec_module(catalogs)
        with tempfile.TemporaryDirectory() as name:
            root = Path(name).resolve()
            fixture_dir = root / "jobs" / "input-fixture"
            fixture_dir.mkdir(parents=True)
            manifest = matrix_fixture(fixture_dir)
            build_path = fixture_dir / "evidence" / "build-evidence.json"
            build = json.loads(build_path.read_text())
            for mode in ("correct", "mutant"):
                build["artifacts"][mode]["path"] = str(fixture_dir / manifest["artifacts"][mode]["path"])
            build_path.write_text(json.dumps(build))
            (root / "example.java").write_bytes(b"generated source fixture\n")
            pilot = fixture()[0]
            pilot["evidence"]["stateRecovery"]["workloadSha256"] = manifest["artifacts"]["correct"]["sha256"]
            pilot_path = root / "pilot.json"
            pilot_path.write_text(json.dumps(pilot))
            recipe = root / "recipe"
            recipe.mkdir()
            for filename in ("baseline.yaml", "restart-phase.yaml", "state-recovery.expected.yaml", "build.py", "catalogs.py", "check.py", "run.py", "README.md"):
                (recipe / filename).write_bytes((RECIPE / filename).read_bytes())
            output = root / "jobs" / "qualification"
            frozen = catalogs.freeze(build_path, pilot_path, output, root, recipe)
            self.assertEqual(len(frozen["cells"]), 4)
            self.assertEqual(check.validate_manifest(output), frozen)
            for cell in frozen["cells"]:
                scenario = (output / cell["catalog"] / "state-recovery.yaml").read_text()
                self.assertNotIn("@WORKLOAD_JAR@", scenario)
                self.assertEqual("      - kill:" in scenario, cell["execution"] == "restart")
                self.assertEqual("allow_state_reset_mutant: true" in scenario, cell["mode"] == "mutant")
                self.assertIn("./jobs/qualification/artifacts/" + cell["mode"] + ".jar", scenario)
                self.assertIn('image_id: "' + manifest["runtimeIdentity"]["flinkImageId"] + '"', scenario)
                self.assertIn('image_id: "' + manifest["runtimeIdentity"]["kafkaImageId"] + '"', scenario)
                self.assertEqual(scenario.count("image_id:"), 2)
                self.assertEqual((output / cell["catalog"] / "state-recovery.expected.yaml").read_bytes(), check.EXPECTED_BYTES)
            with self.assertRaises(ValueError):
                catalogs.freeze(build_path, pilot_path, output, root, recipe)
            artifact = Path(build["artifacts"]["mutant"]["path"])
            artifact.write_bytes(artifact.read_bytes() + b"modified")
            second = root / "jobs" / "another-qualification"
            with self.assertRaises(ValueError):
                catalogs.freeze(build_path, pilot_path, second, root, recipe)
            self.assertFalse(second.exists())


if __name__ == "__main__":
    unittest.main()
