#!/usr/bin/env python3
"""Audit the frozen twelve-attempt state recovery matrix; never run or rewrite it."""
import argparse
import hashlib
import importlib.util
from io import BytesIO
import json
from pathlib import Path
import re
from zipfile import ZipFile
from zipfile import BadZipFile

SCENARIO = "state-recovery"
MODES = ("correct", "mutant")
EXECUTIONS = ("uninterrupted", "restart")
CELLS = tuple(f"{mode}-{execution}" for mode in MODES for execution in EXECUTIONS)
MUTATION = "reset-state-on-restore-v1"
MATCH = "validator.kafka.state-prefix.match"
MISMATCH = "validator.kafka.state-prefix.state-mismatch"
ENTRY_CLASSES = ("org.apache.flink.connector.kafka.source.KafkaSource",
                 "org.apache.flink.connector.kafka.sink.KafkaSink")
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_LONG = (1 << 63) - 1
STATE_SAMPLE_FIELDS = {"partition", "offset", "id", "expectedKey", "actualKey", "expectedCount",
                       "actualCount", "expectedSum", "actualSum"}
KEY_PROGRESS_FIELDS = {"keyCount", "key", "inputId", "previousCount", "previousSum", "newCount", "newSum"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def catalog_contract():
    # Lazy loading keeps the vendored state oracle usable by other calibration
    # checkers that do not audit the state-recovery catalog format.
    path = Path(__file__).with_name("catalog_contract.py")
    spec = importlib.util.spec_from_file_location("state_recovery_catalog_contract", path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def bounded_integer(value, minimum=0, maximum=MAX_LONG):
    return integer(value, minimum) and value <= maximum


def digest(data):
    return hashlib.sha256(data).hexdigest()


def sha(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "Invalid SHA-256")
    return value


def guarded(path, scope):
    path, scope = Path(path).absolute(), Path(scope).absolute()
    require(".." not in path.parts and path.is_relative_to(scope), "Evidence path escapes its scope")
    cursor = Path(path.anchor)
    for part in path.parts[1:]:
        cursor /= part
        require(not cursor.is_symlink(), "Symlink evidence is rejected")
    return path


def read(path, scope, expected=None):
    path = guarded(path, scope)
    require(path.is_file() and path.stat().st_size <= MAX_FILE_BYTES, "Missing or oversized evidence: " + str(path))
    data = path.read_bytes()
    if expected is not None:
        require(digest(data) == sha(expected), "Evidence hash mismatch: " + str(path))
    return data


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "Duplicate JSON key: " + key)
        value[key] = item
    return value


def load(path, scope, expected=None):
    return json.loads(read(path, scope, expected), object_pairs_hook=unique_object)


def file_receipt(batch, receipt):
    require(isinstance(receipt, dict) and set(receipt) == {"path", "sha256"}, "Invalid file receipt")
    require(isinstance(receipt["path"], str) and not Path(receipt["path"]).is_absolute(), "Expected relative evidence path")
    return read(batch / receipt["path"], batch, receipt["sha256"])


def runtime_identity(result):
    evidence = result["evidence"]
    runtime = evidence["flinkRuntime"]
    require(runtime["status"] == "confirmed", "Flink runtime identity is unconfirmed")
    components = runtime["components"]
    require(components, "Runtime components are missing")
    images = {(item["imageReference"], item["imageId"]) for item in components}
    require(len(images) == 1 and re.fullmatch(r"sha256:[0-9a-f]{64}", next(iter(images))[1]),
            "Mixed or absent Flink image identities")
    closures = []
    for component in components:
        artifacts = runtime["connectorArtifactSets"][component["connectorArtifactsRef"]]
        closure = sorted((entry["containerPath"], sha(entry["sha256"])) for entry in artifacts)
        require(closure and len(set(closure)) == len(closure), "Missing or duplicated connector closure")
        closures.append(closure)
    require(all(value == closures[0] for value in closures), "Connector closures differ across incarnations")
    primaries = evidence["connectorPrimaries"]
    require(len(primaries) == 1 and primaries[0]["alias"] == "kafka", "Expected one Kafka subject primary")
    primary_hash = sha(primaries[0]["observedSha256"])
    require(primary_hash in [value for unused, value in closures[0]], "Primary is absent from installed closure")
    kafka = evidence["kafka"]
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", kafka["imageId"]), "Kafka image identity is unavailable")
    image, image_id = next(iter(images))
    return {"connectorSha256": primary_hash,
            "connectorArtifacts": [{"containerPath": path, "sha256": value} for path, value in closures[0]],
            "flinkImageReference": image, "flinkImageId": image_id,
            "kafkaImageReference": kafka["imageReference"], "kafkaImageId": kafka["imageId"]}


def marker_identity(marker):
    return (marker["jobId"], marker["operatorUid"], marker["executionAttempt"], marker["subtask"],
            marker["process"]["logicalName"], marker["process"]["runtimeId"])


def progress(marker, keys, total):
    value = marker["progress"]
    require(value["keyCount"] == keys and value["observedKeys"] == keys
            and integer(value["minimumObservedCount"], 1)
            and integer(value["processedThisAttempt"], keys)
            and integer(value["maximumInputId"], keys - 1)
            and value["maximumInputId"] < total, "Progress does not cover every key")
    return value


def check_recovery(proof, kill, restart, keys, total, mode, markers):
    require(proof["confirmed"] is True, "Recovery is not confirmed: " + str(proof.get("reason")))
    ready = proof["readiness"]
    require(ready["ready"] is True and ready["keyCount"] == keys and ready["totalRecords"] == total,
            "Pre-kill readiness is unconfirmed")
    observation = ready["jobObservation"]
    require(observation["state"] == "RUNNING" and integer(observation["completedCheckpoints"], 1),
            "Job was not RUNNING with a completed checkpoint at readiness")
    prefix = ready["committedPrefix"]
    require(prefix["snapshotComplete"] is True and prefix["validStatePrefix"] is True,
            "Committed prefix was not fully verified")
    count, maximum = prefix["observedRecords"], prefix["maximumId"]
    require(integer(count, keys) and count <= total - keys and maximum == count - 1,
            "Committed prefix is not bounded with subsequent input for every key")
    require(prefix["perKeyCounts"] == {str(key): (maximum - key) // keys + 1 for key in range(keys)},
            "Committed prefix does not independently cover each key")
    require(prefix["beginningOffsets"] == {"0": 0} and set(prefix["endOffsets"]) == {"0"}
            and integer(prefix["endOffsets"]["0"], count), "Invalid fixed committed-prefix bounds")
    require(kill["confirmed"] is True and kill["jobStateBeforeKill"] == "RUNNING", "Kill effect is unconfirmed")
    checkpoint = proof["restoredCheckpointId"]
    require(integer(checkpoint, 1) and kill["restoredCheckpoint"] == checkpoint,
            "Restored checkpoint differs from confirmed kill evidence")
    snapshot = ready["completedSnapshots"][str(checkpoint)]
    require(snapshot["checkpointId"] == checkpoint, "Checkpoint receipt identity mismatch")
    saved, completed = snapshot["snapshot"], snapshot["completion"]
    require(saved in markers and completed in markers, "Checkpoint proof is not in retained trace")
    require(saved["event"] == "SNAPSHOT" and completed["event"] == "COMPLETED"
            and saved["checkpointId"] == completed["checkpointId"] == checkpoint
            and marker_identity(saved) == marker_identity(completed)
            and saved["lineNumber"] < completed["lineNumber"], "Checkpoint progress and completion are not bound")
    require(saved["jobId"] == ready["jobId"] and saved["executionAttempt"] == ready["executionAttempt"],
            "Checkpoint belongs to a different job attempt")
    before, after, restored_at = kill["jobManagerTimeBeforeKill"], kill["jobManagerTimeAfterKill"], kill["restoredAtMillis"]
    require(integer(snapshot["acknowledgedAtMillis"], 1)
            and snapshot["acknowledgedAtMillis"] <= before <= after < restored_at,
            "Checkpoint/kill/restore clocks do not establish ordering")
    saved_progress = progress(saved, keys, total)
    require(saved_progress["maximumInputId"] < total - keys, "Checkpoint leaves no following event for every key")
    target = ready["target"]
    require(target == kill["identity"] == restart["previousIdentity"], "Readiness and kill target identities differ")
    require(saved["process"]["runtimeId"] == target["runtimeId"], "Snapshot came from another physical process")
    replacement = restart["replacementIdentity"]
    require(replacement["logicalName"] == target["logicalName"]
            and replacement["runtimeId"] != target["runtimeId"]
            and replacement["resourceId"] != target["resourceId"], "Replacement identity is not distinct")
    start, after_progress = proof["restoredStart"], proof["postRestoreProgress"]
    require(start in markers and after_progress in markers, "Restore proof is not in retained trace")
    require(start["event"] == "START" and after_progress["event"] == "PROGRESS"
            and start["restoredCheckpointId"] == after_progress["restoredCheckpointId"] == checkpoint
            and start["executionAttempt"] > ready["executionAttempt"]
            and start["process"]["runtimeId"] == replacement["runtimeId"]
            and marker_identity(start) == marker_identity(after_progress)
            and start["jobId"] == ready["jobId"] and start["operatorUid"] == saved["operatorUid"],
            "Restored execution is not bound to the checkpoint and replacement")
    progress(after_progress, keys, total)
    expected_mode = MUTATION if mode == "mutant" else "correct"
    require(start["buildMode"] == expected_mode, "Restored workload build mode mismatch")
    rows = proof["restoredKeys"]
    require(isinstance(rows, list) and all(isinstance(row, dict) and isinstance(row.get("keyProgress"), dict)
            and set(row["keyProgress"]) == KEY_PROGRESS_FIELDS
            and all(bounded_integer(value) for value in row["keyProgress"].values()) for row in rows),
            "Malformed numeric restored-key receipt")
    require(len(rows) == keys and {row["keyProgress"]["key"] for row in rows} == set(range(keys)),
            "Missing or duplicate per-key restore receipts")
    for row in rows:
        require(row in markers and row["event"] == "RESTORED_KEY" and marker_identity(row) == marker_identity(start)
                and row["restoredCheckpointId"] == checkpoint
                and start["lineNumber"] < row["lineNumber"] < after_progress["lineNumber"],
                "Restored key belongs to another attempt or follows the progress receipt")
        value = row["keyProgress"]
        event_id, key = value["inputId"], value["key"]
        require(value["keyCount"] == keys and integer(event_id) and event_id % keys == key
                and saved_progress["maximumInputId"] < event_id < total, "No post-checkpoint event for restored key")
        prior_count = (saved_progress["maximumInputId"] - key) // keys + 1
        prior_sum = prior_count * (key + 1) + keys * prior_count * (prior_count - 1) // 2
        require(value["previousCount"] == prior_count and value["previousSum"] == prior_sum,
                "Restored aggregate does not match the saved prefix")
        expected_count = 1 if mode == "mutant" else prior_count + 1
        expected_sum = event_id + 1 if mode == "mutant" else prior_sum + event_id + 1
        require(value["newCount"] == expected_count and value["newSum"] == expected_sum,
                "First post-restore transition does not match the selected artifact")
    require(after_progress["progress"]["processedThisAttempt"] == keys
            and after_progress["progress"]["maximumInputId"] == max(row["keyProgress"]["inputId"] for row in rows)
            and after_progress["progress"]["minimumObservedCount"] == min(row["keyProgress"]["newCount"] for row in rows),
            "Post-restore summary does not match the per-key transitions")
    resets = proof["mutantResets"]
    require(isinstance(resets, list) and len(resets) == (keys if mode == "mutant" else 0)
            and all(isinstance(row, dict) and isinstance(row.get("keyProgress"), dict)
                    and set(row["keyProgress"]) == KEY_PROGRESS_FIELDS
                    and all(bounded_integer(value) for value in row["keyProgress"].values()) for row in resets),
            "Mutant activation count or numeric fields mismatch")
    if mode == "mutant":
        require({row["keyProgress"]["key"] for row in resets} == set(range(keys)), "Mutant did not activate once per key")
        by_key = {row["keyProgress"]["key"]: row for row in rows}
        for reset in resets:
            require(reset in markers and reset["event"] == "MUTANT_RESET"
                    and marker_identity(reset) == marker_identity(start) and reset["restoredCheckpointId"] == checkpoint
                    and isinstance(reset["keyProgress"], dict) and set(reset["keyProgress"]) == KEY_PROGRESS_FIELDS
                    and all(bounded_integer(value) for value in reset["keyProgress"].values())
                    and reset["keyProgress"] == by_key[reset["keyProgress"]["key"]]["keyProgress"]
                    and by_key[reset["keyProgress"]["key"]]["lineNumber"] < reset["lineNumber"] < after_progress["lineNumber"],
                    "Mutant activation provenance does not match the restored transition")


def check_mutant_state(state, proof, keys, total):
    """Bind the fixed once-per-key reset mutant's entire corrupt suffix to its restored transitions."""
    rows = {row["keyProgress"]["key"]: row["keyProgress"] for row in proof["restoredKeys"]}
    saved = proof["readiness"]["completedSnapshots"][str(proof["restoredCheckpointId"])]["snapshot"]
    first_id = saved["progress"]["maximumInputId"] + 1
    corrupted = 0
    for key, transition in rows.items():
        # Single ordered input partition: the first restored record for each key must immediately
        # follow that key's saved prefix. Skipping records cannot describe this pinned mutant.
        require(transition["inputId"] == key + keys * transition["previousCount"],
                "Mutant reset does not begin immediately after the saved key prefix")
        corrupted += (total - 1 - transition["inputId"]) // keys + 1
    require(state["mismatches"] == corrupted == total - first_id,
            "State mismatch total differs from the restored mutant's corrupted suffix")
    previous_offset = -1
    for ordinal, sample in enumerate(state["mismatchSamples"]):
        event_id, key = sample["id"], sample["expectedKey"]
        transition = rows[key]
        require(event_id == first_id + ordinal and sample["offset"] > previous_offset,
                "State samples are not the first distinct ordered records of the corrupted suffix")
        previous_offset = sample["offset"]
        count = (event_id - transition["inputId"]) // keys + 1
        aggregate = count * (transition["inputId"] + 1) + keys * count * (count - 1) // 2
        require((sample["actualKey"], sample["actualCount"], sample["actualSum"]) == (key, count, aggregate)
                and sample["expectedCount"] - sample["actualCount"] == transition["previousCount"]
                and sample["expectedSum"] - sample["actualSum"] == transition["previousSum"],
                "State sample differs from the proven reset mutant's per-key suffix arithmetic")


def verify_marker_logs(directory, markers):
    """Bind typed receipts back to the retained physical process output, including exact line numbers."""
    logs, receipts = {}, {}
    for marker in markers:
        process = marker["process"]["process"]
        require(isinstance(process, str) and re.fullmatch(r"taskmanager-[0-9]+#[0-9]+", process), "Invalid marker process")
        name = "flink-stability-output-" + process.replace("#", "-") + ".log"
        if name not in logs:
            data = read(directory / "logs" / name, directory)
            # Match BufferedReader.readLine: Unicode separators and control bytes are not line endings.
            logs[name] = re.split(r"\r\n|\r|\n", data.decode("utf-8"))
            if logs[name] and logs[name][-1] == "":
                logs[name].pop()
            receipts[name] = digest(data)
        line_number = marker["lineNumber"]
        require(integer(line_number, 1) and line_number <= len(logs[name]), "Marker line is missing from raw output")
        line = logs[name][line_number - 1]
        match = re.search(r"STATE_PREFIX_([A-Z_]+)\s+(.+)$", line)
        require(match is not None and match.group(1) == marker["event"], "Typed marker event differs from raw output")
        pairs = [token.split("=", 1) for token in match.group(2).split()]
        require(all(len(pair) == 2 for pair in pairs), "Malformed raw marker fields")
        actual = unique_object(pairs)
        expected = {"version": "1", "jobId": marker["jobId"], "operatorUid": marker["operatorUid"],
                    "attempt": str(marker["executionAttempt"]), "subtask": str(marker["subtask"]),
                    "checkpoint": str(marker["checkpointId"]), "restoredCheckpoint": str(marker["restoredCheckpointId"])}
        for field in ("progress", "keyProgress"):
            if marker.get(field) is not None:
                expected.update({key: str(value) for key, value in marker[field].items()})
        if marker.get("buildMode") is not None:
            expected["buildMode"] = marker["buildMode"]
        require(actual == expected, "Typed marker fields differ from retained raw output")
    return receipts


def verify_physical_fence(evidence):
    fence = evidence["processFence"]
    require(fence["completed"] is True and fence["status"] == "complete" and fence["observationOverflow"] is False
            and fence["health"]["outcome"] == "healthy", "Physical write fence or lifecycle evidence is incomplete")
    components = evidence["flinkRuntime"]["components"]
    ids = {item["runtimeId"] for item in components}
    fenced = fence["components"]
    fenced_ids = {item["runtimeId"] for item in fenced}
    require(len(ids) == len(components) and len(fenced_ids) == len(fenced)
            and fenced_ids <= ids and all(item["outcome"] in ("sigkilled", "already-stopped") for item in fenced),
            "Invalid final physical fence identities")
    killed_ids = set()
    for kill in evidence["taskManagerKills"]:
        identity = kill.get("identity")
        if kill.get("confirmed") is not True or not isinstance(identity, dict):
            continue
        stopped = [observation for observation in fence.get("observations", [])
                   if observation.get("moment") == "after-declared-kill"
                   and observation.get("runtimeId") == identity["runtimeId"]
                   and observation.get("logicalName") == identity["logicalName"]
                   and observation.get("missing") is False and observation.get("diagnostic") is None
                   and isinstance(observation.get("state"), dict)
                   and observation["state"].get("runtimeId") == identity["runtimeId"]
                   and observation["state"].get("running") is False
                   and observation["state"].get("paused") is False
                   and observation["state"].get("oomKilled") is False]
        if len(stopped) == 1:
            killed_ids.add(identity["runtimeId"])
    require(fenced_ids | killed_ids == ids, "Not every physical incarnation has confirmed stopped evidence")


def audit_attempt(result, exit_code, cell, manifest):
    mode, execution = cell["mode"], cell["execution"]
    mutant_failure = mode == "mutant" and execution == "restart"
    keys, total = manifest["workload"]["keyCount"], manifest["workload"]["totalRecords"]
    require(bounded_integer(keys, 1, 1024) and bounded_integer(total, 1), "Invalid workload dimensions")
    evidence = result["evidence"]
    diagnostics = result["diagnostics"]
    require(isinstance(diagnostics, list) and all(isinstance(item, str) for item in diagnostics),
            "Attempt diagnostics are missing or malformed")
    require(not any(item.startswith(("infrastructure.attempt-cleanup-failed",
                                    "infrastructure.prepared-artifact-cleanup-failed")) for item in diagnostics),
            "Infrastructure cleanup failed; raw data failure cannot qualify the attempt")
    require(result["scenario"] == SCENARIO, "Unexpected scenario")
    require(result["expectation"]["outcome"] == "pass" and "oracle" not in result["expectation"], "PASS expectation was changed")
    oracle = evidence["terminalValidation"]
    require(oracle["type"] == "kafka.state-prefix" and oracle["mode"] == "exactly-once"
            and oracle["completed"] is True and oracle["snapshotComplete"] is True, "Terminal state snapshot is incomplete")
    require(all(bounded_integer(oracle[name]) and oracle[name] == total
                for name in ("expected", "observed", "distinctExpected"))
            and all(type(oracle[name]) is int and oracle[name] == 0 for name in ("missing", "duplicates", "unexpected", "malformed")),
            "ID integrity is not exact")
    state = oracle["statePrefix"]
    require(bounded_integer(state["keyCount"], 1, 1024) and state["keyCount"] == keys
            and bounded_integer(state["mismatches"], 0, total), "State totals are missing or out of bounds")
    samples = state["mismatchSamples"]
    require(isinstance(samples, list) and len(samples) == min(100, state["mismatches"]),
            "Incomplete or invalid bounded state samples")
    if mutant_failure:
        require(state["mismatches"] > 0 and samples and oracle["reason"] == MISMATCH
                and oracle["status"] == "fail" and result["attempt"]["status"] == result["status"] == "fail"
                and result["attempt"]["reason"] == MISMATCH and exit_code == 1
                and result["expectation"]["matched"] is False, "Mutant recovery did not expose the required raw state failure")
        for sample in samples:
            require(isinstance(sample, dict) and set(sample) == STATE_SAMPLE_FIELDS
                    and all(bounded_integer(value) for value in sample.values()) and sample["partition"] == 0,
                    "Malformed numeric mismatch sample")
            event_id = sample["id"]
            require(integer(event_id) and event_id < total, "Mismatch sample has unexpected ID")
            key, count = event_id % keys, event_id // keys + 1
            expected_sum = count * (key + 1) + keys * count * (count - 1) // 2
            require((sample["expectedKey"], sample["expectedCount"], sample["expectedSum"]) == (key, count, expected_sum)
                    and (sample["actualKey"], sample["actualCount"], sample["actualSum"]) != (key, count, expected_sum),
                    "Mismatch sample does not demonstrate independent arithmetic disagreement")
    else:
        require(state["mismatches"] == 0 and samples == [] and oracle["status"] == "pass" and oracle["reason"] == MATCH
                and result["attempt"]["status"] == result["status"] == "pass" and exit_code == 0
                and result["expectation"]["matched"] is True, "Correct/control arm did not pass")
    source = evidence["input"]
    require(source["complete"] is True and source["reconciliationComplete"] is True
            and source["status"] == "complete" and source["records"] == source["observed"] == total
            and source["partitions"] == 1, "Input reconciliation is incomplete")
    require(evidence["phases"]["completed"] is True and evidence["phases"]["failed"] == 0, "A phase failed")
    require(evidence["writeFence"]["completed"] is True and evidence["writeFence"]["finalState"] == "FINISHED"
            and evidence["flinkJob"]["state"] == "FINISHED", "Natural bounded completion is missing")
    verify_physical_fence(evidence)
    require(runtime_identity(result) == manifest["runtimeIdentity"], "Runtime or connector artifacts differ from the frozen pilot")
    origins = evidence["subjectClasses"]
    require(origins["status"] == "confirmed" and manifest["runtimeIdentity"]["connectorSha256"] in origins["expectedSource"],
            "Subject class origin is unconfirmed")
    processes = origins["processes"]
    task_managers = [item for item in processes if item["process"].startswith("taskmanager-")]
    require(len(task_managers) == (2 if execution == "restart" else 1), "Subject proof misses a TaskManager incarnation")
    for process in task_managers:
        require(all(process["sources"].get(entry) == [origins["expectedSource"]] for entry in ENTRY_CLASSES),
                "A TaskManager loaded a different or unknown connector")
    require(evidence["sinkTransactions"]["status"] == "listed" and evidence["sinkTransactions"]["unresolved"] == [],
            "Sink transactions are unresolved")
    report = evidence["stateRecovery"]
    require(report.get("allowStateResetMutant") is (mode == "mutant")
            and report.get("buildMode") == (MUTATION if mode == "mutant" else "correct"),
            "Explicit workload build mode or mutant opt-in is missing or differs")
    require(report["workloadSha256"] == manifest["artifacts"][mode]["sha256"] and report["keyCount"] == keys
            and report["totalRecords"] == total and report["physicalFenceConfirmed"] is True and report["confirmed"] is True,
            "Workload identity or state recovery proof is unconfirmed")
    trace = report["trace"]
    require(trace["complete"] is True and trace["diagnostics"] == [], "State trace is incomplete")
    markers = trace["markers"]
    require(markers and all(item["jobId"] == report["jobId"] for item in markers), "Mixed or missing job trace")
    starts = [item for item in markers if item["event"] == "START"]
    require(len(starts) == (2 if execution == "restart" else 1)
            and all(item["buildMode"] == (MUTATION if mode == "mutant" else "correct") for item in starts),
            "Start provenance does not match the selected workload")
    initial = [item for item in starts if item["restoredCheckpointId"] < 0]
    require(len(initial) == 1, "Missing or ambiguous initial start")
    resets = [item for item in markers if item["event"] == "MUTANT_RESET"]
    require(len(resets) == (keys if mutant_failure else 0), "Mutant activated outside the intended restored arm")
    kills, restarts = evidence["taskManagerKills"], evidence["taskManagerRestarts"]
    if execution == "restart":
        require(len(kills) == len(restarts) == len(report["recoveries"]) == 1, "Expected one recovery operation")
        check_recovery(report["recoveries"][0], kills[0], restarts[0], keys, total, mode, markers)
        if mutant_failure:
            check_mutant_state(state, report["recoveries"][0], keys, total)
    else:
        require(kills == restarts == report["recoveries"] == [] and evidence["flinkJob"]["restoredCheckpoints"] == 0,
                "Uninterrupted control contains a recovery")
    return {"confirmed": True, "rawStatus": result["attempt"]["status"], "rawReason": result["attempt"]["reason"],
            "buildMode": report["buildMode"], "allowStateResetMutant": report["allowStateResetMutant"],
            "terminalValidation": oracle, "componentErrors": evidence["componentErrors"],
            "flinkRest": evidence["flinkRest"], "stateRecovery": report}


def validate_manifest(batch):
    manifest = load(batch / "manifest.json", batch)
    require(manifest["formatVersion"] == 1 and manifest.get("catalogContractVersion") == 2
            and manifest["kind"] == "state-recovery-calibration"
            and manifest["attemptsPerCell"] == 3, "Unsupported calibration contract")
    require(manifest["workload"] == {"keyCount": 8, "totalRecords": 6000}, "Frozen workload differs from qualification recipe")
    build = json.loads(file_receipt(batch, manifest["buildEvidence"]), object_pairs_hook=unique_object)
    require(build["version"] == 1 and build["mutation"] == MUTATION and build["releaseBytecodeTarget"] == 11
            and build["mutationScope"] == "Keyed aggregate only; once per key on first event after genuine restore; initial start, source and sink unchanged",
            "Wrong mutant recipe")
    require(set(manifest["artifacts"]) == set(MODES), "Wrong artifact arms")
    jar_entries = {}
    for mode in MODES:
        require(manifest["artifacts"][mode]["path"] == "artifacts/" + mode + ".jar", "Wrong fixed workload artifact path")
        data = file_receipt(batch, manifest["artifacts"][mode])
        with ZipFile(BytesIO(data)) as jar:
            names = jar.namelist()
            require(len(names) == len(set(names)) and len(names) <= 1024
                    and sum(info.file_size for info in jar.infolist()) <= MAX_FILE_BYTES,
                    "Duplicate or oversized workload JAR entries")
            jar_entries[mode] = {name: jar.read(name) for name in names if not name.endswith("/")}
        expected_mode = MUTATION if mode == "mutant" else "correct"
        require(jar_entries[mode].get("META-INF/flink-stability/keyed-prefix-build.properties")
                == ("mode=" + expected_mode + "\n").encode("ascii"), "Wrong packaged workload identity")
        require(manifest["artifacts"][mode]["sha256"] == build["artifacts"][mode]["sha256"], "Workload differs from build receipt")
    require(set(jar_entries["correct"]) == set(jar_entries["mutant"]), "Workload JAR entry sets differ")
    differences = sorted(name for name in jar_entries["correct"]
                         if jar_entries["correct"][name] != jar_entries["mutant"][name])
    require(differences == build["changedEntries"] == sorted(["META-INF/MANIFEST.MF",
            "META-INF/flink-stability/keyed-prefix-build.properties"]), "Mutation changed unexpected workload entries")
    require(manifest["artifacts"]["correct"]["sha256"] != manifest["artifacts"]["mutant"]["sha256"], "Mutant is identical to control")
    required_recipe = {"evidence/recipe/" + name for name in ("baseline.yaml", "restart-phase.yaml",
                       "state-recovery.expected.yaml", "build.py", "catalogs.py", "catalog_contract.py", "check.py", "run.py", "README.md")}
    require(required_recipe <= manifest["recipeFiles"].keys(), "Frozen recipe is missing")
    for path, value in manifest["recipeFiles"].items():
        read(batch / path, batch, value)
    for path, value in build["recipeInputSha256"].items():
        require(manifest["recipeFiles"].get("evidence/build-sources/" + path) == value,
                "Build source is missing or differs from frozen recipe")
    reference = json.loads(file_receipt(batch, manifest["referenceResult"]), object_pairs_hook=unique_object)
    require(runtime_identity(reference) == manifest["runtimeIdentity"], "Frozen runtime differs from reference pilot")
    require(isinstance(reference["attempt"]["nonce"], str) and re.fullmatch(r"[0-9a-f]{8}", reference["attempt"]["nonce"]),
            "Reference pilot nonce is missing or malformed")
    contract = catalog_contract()
    template = read(batch / "evidence/recipe/baseline.yaml", batch).decode("utf-8")
    restart_phase = read(batch / "evidence/recipe/restart-phase.yaml", batch).decode("utf-8")
    cells = manifest["cells"]
    require([cell["name"] for cell in cells] == list(CELLS), "Missing, reordered or duplicate matrix cells")
    for cell in cells:
        require(cell["name"] == cell["mode"] + "-" + cell["execution"] and cell["requiredOutcome"] == "pass"
                and cell["scenario"] == SCENARIO and cell["catalog"] == "catalogs/" + cell["name"], "Invalid matrix cell")
        require(cell["attempts"] == [f"attempts/{cell['name']}/run-{number}" for number in range(1, 4)], "Expected three fixed attempts")
        scenario = read(batch / cell["catalog"] / (SCENARIO + ".yaml"), batch, cell["scenarioSha256"])
        require(scenario == contract.render(template, restart_phase, contract.artifact_reference(batch, cell["mode"]),
                                            cell["execution"], manifest["runtimeIdentity"], mode=cell["mode"]),
                "Frozen scenario differs from the independently rendered qualification contract")
        expected = read(batch / cell["catalog"] / (SCENARIO + ".expected.yaml"), batch, cell["expectedSha256"])
        require(expected == EXPECTED_BYTES, "Expected-result catalog is not unchanged PASS")
    return manifest


EXPECTED_BYTES = b"format: v1\nkind: expected-result\n\nmeta:\n  name: state-recovery.expected\n  scenario: state-recovery\n\ndefault:\n  outcome: pass\n"


def check_matrix(batch):
    batch = Path(batch).absolute()
    manifest = validate_manifest(batch)
    report = {"qualified": False, "manifestSha256": digest(read(batch / "manifest.json", batch)),
              "confirmedCounts": {name: 0 for name in CELLS}, "attempts": [], "errors": []}
    reference = load(batch / manifest["referenceResult"]["path"], batch, manifest["referenceResult"]["sha256"])
    nonces = {reference["attempt"]["nonce"]}
    try:
        started = load(batch / "execution-started.json", batch)
        require(type(started.get("attempts")) is int and started["attempts"] == 12
                and type(started.get("automaticRetries")) is int and started["automaticRetries"] == 0,
                "Execution receipt does not allocate exactly twelve attempts without retries")
        allocated = {relative for cell in manifest["cells"] for relative in cell["attempts"]}
        actual = set()
        for cell_path in guarded(batch / "attempts", batch).iterdir():
            guarded(cell_path, batch)
            require(cell_path.is_dir() and cell_path.name in CELLS, "Unexpected attempt cell or file")
            for attempt_path in cell_path.iterdir():
                guarded(attempt_path, batch)
                require(attempt_path.is_dir(), "Unexpected file in attempt allocation")
                actual.add(attempt_path.relative_to(batch).as_posix())
        require(actual == allocated, "Missing or extra attempt directories")
    except (ValueError, KeyError, TypeError, OSError) as failure:
        report["errors"].append({"scope": "execution-allocation", "error": str(failure)})
    for cell in manifest["cells"]:
        for number, relative in enumerate(cell["attempts"], 1):
            row = {"cell": cell["name"], "number": number, "directory": relative, "confirmed": False}
            try:
                directory = guarded(batch / relative, batch)
                stdout = read(directory / "stdout.json", batch)
                raw = json.loads(stdout, object_pairs_hook=unique_object)
                row.update({"stdoutSha256": digest(stdout), "rawStatus": raw.get("attempt", {}).get("status"),
                            "rawReason": raw.get("attempt", {}).get("reason"), "status": raw.get("status"),
                            "reason": raw.get("reason"), "terminalValidation": raw.get("evidence", {}).get("terminalValidation"),
                            "componentErrors": raw.get("evidence", {}).get("componentErrors"), "diagnostics": raw.get("diagnostics")})
                row["stderrSha256"] = digest(read(directory / "stderr.log", batch))
                code = int(read(directory / "exit-code.txt", batch))
                row["exitCode"] = code
                nonce = raw["attempt"]["nonce"]
                require(isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{8}", nonce) and nonce not in nonces,
                        "Missing or reused attempt nonce")
                nonces.add(nonce)
                row.update(audit_attempt(raw, code, cell, manifest))
                row["logSha256"] = verify_marker_logs(directory, raw["evidence"]["stateRecovery"]["trace"]["markers"])
                report["confirmedCounts"][cell["name"]] += 1
            except (ValueError, KeyError, TypeError, IndexError, OSError) as failure:
                row["confirmed"] = False
                row["error"] = str(failure)
                report["errors"].append({"cell": cell["name"], "number": number, "error": str(failure)})
            report["attempts"].append(row)
    report["qualified"] = not report["errors"] and all(count == 3 for count in report["confirmedCounts"].values())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch", type=Path)
    args = parser.parse_args()
    try:
        report = check_matrix(args.batch)
    except Exception as failure:
        report = {"qualified": False, "errors": [str(failure)]}
    print(json.dumps(report, indent=2))
    return 0 if report["qualified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
