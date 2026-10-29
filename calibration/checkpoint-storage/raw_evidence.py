"""Compare typed proof with retained source bytes, not reserialized DTO copies.

Scope: decoded Flink HTTP bodies, exact inspector stdout and Docker inspect Id/State
source-token projections. Hashes prove retained consistency, not daemon authenticity.
Harness monotonic times and JobManager epoch times remain distinct observed clocks.
Kafka terminal counts remain the framework oracle's report; this is not Kafka replay.
"""
import json
import re
from pathlib import PurePosixPath

MAX_BODY = 2 * 1024 * 1024


def require(ok, message):
    if not ok:
        raise ValueError("Raw evidence: " + message)


def semantic(value):
    """Remove provenance only; every remaining typed field participates in equality."""
    if isinstance(value, dict):
        return {key: semantic(item) for key, item in value.items() if key != "rawEvidence"}
    if isinstance(value, list):
        return [semantic(item) for item in value]
    return value


def equal(left, right, label):
    # Python bool/int equality must not conceal a typed coercion.
    require(json.dumps(semantic(left), sort_keys=True, separators=(",", ":")) ==
            json.dumps(semantic(right), sort_keys=True, separators=(",", ":")), label + " differs from source bytes")


def checkpoint(raw):
    mapping = {"id": "id", "status": "status", "savepoint": "is_savepoint", "checkpointType": "checkpoint_type",
               "triggerTimestamp": "trigger_timestamp", "latestAckTimestamp": "latest_ack_timestamp",
               "checkpointedSize": "checkpointed_size", "stateSize": "state_size", "numSubtasks": "num_subtasks",
               "numAcknowledgedSubtasks": "num_acknowledged_subtasks"}
    value = {typed: raw[source] for typed, source in mapping.items()}
    value.update(failureTimestamp=raw.get("failure_timestamp"), failureMessage=raw.get("failure_message"),
                 externalPath=raw.get("external_path"), discarded=raw.get("discarded"))
    return value


def history(raw, job):
    latest = raw["latest"]
    restored = latest.get("restored")
    return {"jobId": job, "counts": {"total": raw["counts"]["total"], "inProgress": raw["counts"]["in_progress"],
            **{name: raw["counts"][name] for name in ("completed", "failed", "restored")}},
            "retained": [checkpoint(row) for row in raw["history"]],
            **{target: checkpoint(latest[name]) if latest.get(name) is not None else None
               for name, target in (("completed", "latestCompleted"), ("failed", "latestFailed"), ("savepoint", "latestSavepoint"))},
            "latestRestored": None if restored is None else {"id": restored["id"], "restoreTimestamp": restored["restore_timestamp"],
                                                            "savepoint": restored["is_savepoint"], "externalPath": restored.get("external_path")}}


def configuration(raw, job):
    mapping = {"mode": "mode", "intervalMillis": "interval", "timeoutMillis": "timeout", "minPauseMillis": "min_pause",
               "maxConcurrent": "max_concurrent", "tolerableFailedCheckpoints": "tolerable_failed_checkpoints",
               "stateBackend": "state_backend", "checkpointStorage": "checkpoint_storage",
               "unalignedCheckpoints": "unaligned_checkpoints", "stateChangelogEnabled": "state_changelog_enabled"}
    return {"jobId": job, **{key: raw[field] for key, field in mapping.items()},
            "externalized": raw["externalization"]["enabled"], "deleteOnCancellation": raw["externalization"]["delete_on_cancellation"]}


class Inspector:
    def __init__(self, retained, checkpoint_root=None):
        self.retained = retained
        self.checkpoint_root = checkpoint_root
        self.references = {}
        self.groups = {}
        self.typed = {"jobObservations": 0, "checkpointHistories": 0, "checkpointDetails": 0,
                      "checkpointConfigurations": 0, "processStates": 0, "materializations": 0, "scalarCalls": 0}

    def source(self, ref, kind):
        require(isinstance(ref, dict) and set(ref) == {"path", "sha256", "bytes", "kind", "sequence", "startedNanos",
                "completedNanos", "group", "method", "endpoint", "status"}, "missing or malformed capture reference")
        require(ref["kind"] == kind and type(ref["bytes"]) is int and 0 < ref["bytes"] <= MAX_BODY
                and type(ref["sequence"]) is int and 0 < ref["sequence"] <= 4096
                and type(ref["startedNanos"]) is int and type(ref["completedNanos"]) is int
                and ref["completedNanos"] >= ref["startedNanos"]
                and type(ref["status"]) is int and 0 <= ref["status"] <= 599
                and isinstance(ref["group"], str) and ref["group"]
                and isinstance(ref["endpoint"], str) and ref["endpoint"].startswith("/"), "invalid capture identity/bounds")
        previous = self.references.get(ref["path"])
        require(previous is None or previous == ref, "one source path has conflicting references")
        self.references[ref["path"]] = ref
        if self.checkpoint_root is not None:
            folder = "raw-flink-rest" if kind == "flink-rest-decoded-body" else "raw-storage"
            expected = PurePosixPath(self.checkpoint_root) / folder / (str(ref["sequence"]).zfill(6) + ".json")
            require(ref["path"] == str(expected), "capture path/sequence is outside its attempt store")
        return self.retained.raw(ref, parse_json=kind != "flink-rest-decoded-body" or ref["status"] < 400)

    def operation(self, references, name):
        require(isinstance(references, list) and 0 < len(references) <= 1024, "missing/bounded REST call group")
        group = references[0].get("group", "")
        require(re.fullmatch(re.escape(name) + r"-[1-9][0-9]*", group), "REST call group has wrong purpose")
        previous = self.groups.get(group)
        require(previous is None or previous == references, "REST group was rebound to other responses")
        self.groups[group] = references
        successful = []
        for index, reference in enumerate(references):
            require(reference.get("group") == group, "REST call mixes response groups")
            if index:
                require(reference["sequence"] == references[index - 1]["sequence"] + 1
                        and reference["startedNanos"] >= references[index - 1]["completedNanos"], "REST response order/gap differs")
            source = self.source(reference, "flink-rest-decoded-body")
            if 200 <= reference["status"] < 300:
                successful.append((reference, source))
            else:
                require(reference["method"] == "GET" and reference["status"] >= 500
                        and index + 1 < len(references)
                        and reference["endpoint"] == references[index + 1]["endpoint"], "Unexpected REST retry/mutation replay")
        require(successful, "REST group has no successful response")
        return successful

    def one(self, references, name, endpoint, method="GET"):
        responses = self.operation(references, name)
        require(len(responses) == 1 and responses[0][0]["method"] == method
                and responses[0][0]["endpoint"] == endpoint, "REST request identity differs")
        return responses[0][1]

    def job(self, typed, job):
        responses = self.operation(typed.get("rawEvidence"), "job-observation")
        prefix = "/jobs/" + job
        require(len(responses) >= 3, "job observation lacks source calls")
        details, checkpoints, exceptions = [entry[1] for entry in responses[:3]]
        endpoints = [prefix, prefix + "/checkpoints", prefix + "/exceptions?maxExceptions=20"]
        endpoints += [prefix + "/vertices/" + vertex["id"] for vertex in details.get("vertices", [])]
        require([ref["endpoint"] for ref, unused in responses] == endpoints
                and all(ref["method"] == "GET" for ref, unused in responses), "job observation endpoint/order differs")
        subtasks = []
        for vertex, (unused, data) in zip(details.get("vertices", []), responses[3:]):
            for row in data.get("subtasks", []):
                subtasks.append({"vertexName": vertex.get("name", ""), "index": row["subtask"], "attempt": row["attempt"],
                                 "status": row["status"], "taskManagerId": assigned(row.get("taskmanager-id", ""))})
        failures = []
        for row in exceptions.get("exceptionHistory", {}).get("entries", []):
            lines = row.get("stacktrace", "").splitlines()
            root = lines[0] if lines else ""
            for line in lines:
                if line.startswith("Caused by: "):
                    root = line[len("Caused by: "):]
            failures.append({"timestampMillis": row["timestamp"], "exceptionName": row.get("exceptionName", ""),
                             "rootCause": root, "taskManagerId": assigned(row.get("taskManagerId", ""))})
        restored = checkpoints.get("latest", {}).get("restored")
        expected = {"jobManagerTimeMillis": details["now"], "state": details["state"],
                    "completedCheckpoints": checkpoints["counts"]["completed"], "restoredCheckpoints": checkpoints["counts"]["restored"],
                    "latestRestore": {"checkpointId": restored["id"], "restoredAtMillis": restored["restore_timestamp"]}
                    if isinstance(restored, dict) else None, "failures": failures, "subtasks": subtasks}
        equal(typed, expected, "job observation")
        self.typed["jobObservations"] += 1

    def process(self, typed):
        ref = typed.get("rawEvidence")
        raw = self.source(ref, "docker-inspect-id-state-source-projection")
        require(ref["method"] == "GET" and ref["status"] == 200 and re.fullmatch(
                r"/v[0-9]+\.[0-9]+/containers/" + re.escape(typed["runtimeId"]) + r"/json", ref["endpoint"])
                and set(raw) == {"Id", "State"}, "Docker inspect projection identity differs")
        state = raw["State"]
        expected = {"runtimeId": raw["Id"], "running": state["Running"], "paused": state["Paused"], "oomKilled": state["OOMKilled"]}
        # The legacy renderer omits absent Optional fields. Compare their semantic null value.
        normalized = dict(typed, exitCode=typed.get("exitCode"), finishedAt=typed.get("finishedAt"))
        expected.update(exitCode=None if state["Running"] else state["ExitCode"],
                        finishedAt=None if state["Running"] else state["FinishedAt"] or None)
        equal(normalized, expected, "Docker process state")
        self.typed["processStates"] += 1

    def materialization(self, typed):
        ref = typed.get("rawEvidence")
        raw = self.source(ref, "checkpoint-inspector-stdout")
        require(ref["method"] == "EXEC" and ref["status"] == 0 and ref["endpoint"] == "/containers/" +
                typed["instance"]["runtimeId"] + "/checkpoint-inspector/" + typed["jobId"] + "/" + str(typed["checkpointId"]),
                "inspector process/job/checkpoint differs")
        equal({key: typed[key] for key in ("jobId", "checkpointId", "operatorUid")},
              {key: raw[key] for key in ("jobId", "checkpointId", "operatorUid")}, "inspector identity")
        require(raw["version"] == 1 and typed["complete"] is True and typed["diagnostics"] == [], "incomplete inspector result")
        for target, source in ((typed["metadata"], raw["metadata"]), *zip(typed["files"], raw["files"])):
            equal({key: target[key] for key in ("path", "size", "sha256")}, source, "inspector file")
            require(target["regularFile"] is True and type(target["observedNanos"]) is int,
                    "inspector file type/observation missing")
        require(len(typed["files"]) == len(raw["files"]), "inspector file inventory differs")
        self.typed["materializations"] += 1

    def readiness(self, typed, job):
        self.job(typed["jobObservation"], job)
        refs = typed.get("checkpointEvidence")
        overview = self.one(refs, "checkpoint-overview", "/jobs/" + job + "/checkpoints")
        require(refs[-1]["completedNanos"] <= typed["jobObservation"]["rawEvidence"][0]["startedNanos"],
                "readiness checkpoint response was observed after its job response")
        candidates = [overview.get("latest", {}).get("completed")] + overview.get("history", [])
        completed = {}
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("status") == "COMPLETED":
                completed[candidate["id"]] = max(completed.get(candidate["id"], -1), candidate["latest_ack_timestamp"])
        proven = typed["completedSnapshots"]
        require(proven and all(type(item["checkpointId"]) is int and str(item["checkpointId"]) == str(key)
                and completed.get(item["checkpointId"]) == item["acknowledgedAtMillis"] for key, item in proven.items()),
                "readiness completed checkpoint/acknowledgement differs from source")
        latest = overview["latest"]["completed"]
        require(str(latest["id"]) in {str(key) for key in proven}
                and max(completed) == latest["id"] and overview.get("counts", {}).get("in_progress", 0) == 0
                and all(row.get("status") != "IN_PROGRESS" for row in overview.get("history", [])),
                "readiness source has a different latest or in-flight checkpoint")


def assigned(value):
    return None if value is None or not str(value).strip() or value == "(unassigned)" else value


def audit(result, retained):
    root = result["attempt"]["checkpointRoot"]
    require(isinstance(root, str) and PurePosixPath(root).is_absolute()
            and str(PurePosixPath(root)) == root and ".." not in PurePosixPath(root).parts,
            "checkpoint source root is not an absolute normalized label")
    inspector = Inspector(retained, root)
    stores = retained.raw_inventory(root)
    evidence = result["evidence"]
    job = evidence["stateRecovery"]["jobId"]
    final = evidence["flinkJob"]
    inspector.job(final.get("observation", {}), job)
    observed = final["observation"]
    equal({key: final[key] for key in ("state", "completedCheckpoints", "restoredCheckpoints")},
          {key: observed[key] for key in ("state", "completedCheckpoints", "restoredCheckpoints")}, "final job summary")
    require(final["failures"] == len(observed["failures"]), "final failure count differs")
    if observed["latestRestore"] is not None:
        require(final["latestRestoredCheckpoint"] == observed["latestRestore"]["checkpointId"], "final restore ID differs")
    states = [row["state"] for row in evidence["processFence"]["observations"] if row.get("state") is not None]
    require(states, "physical fence has no raw process observations")
    for state in states:
        inspector.process(state)
    final_fence = [row["state"]["rawEvidence"] for row in evidence["processFence"]["observations"]
                   if row["moment"] == "after-fence-kill" and row.get("state") is not None]
    require(final_fence and max(ref["completedNanos"] for ref in observed["rawEvidence"])
            <= min(ref["startedNanos"] for ref in final_fence), "final job observation was captured after physical fencing")
    # Every readiness occurrence remains independently tied to its own original observation.
    def readiness_walk(node):
        if isinstance(node, dict):
            if "jobObservation" in node and "completedSnapshots" in node:
                inspector.readiness(node, job)
                stop = [row["state"]["rawEvidence"] for row in evidence["processFence"]["observations"]
                        if row["moment"] == "after-declared-kill" and row.get("state") is not None
                        and row["runtimeId"] == node["target"]["runtimeId"]]
                require(stop and max(ref["completedNanos"] for ref in node["jobObservation"]["rawEvidence"])
                        <= min(ref["startedNanos"] for ref in stop), "readiness was captured after the target was killed")
            for key, child in node.items():
                if key not in {"rawEvidence", "operations", "markers"}:
                    readiness_walk(child)
        elif isinstance(node, list):
            for child in node:
                readiness_walk(child)
    readiness_walk(evidence)
    for fault in evidence.get("checkpointStorageFaults", []):
        prefix = "/jobs/" + job + "/checkpoints"
        config = fault["configuration"]
        equal(config, configuration(inspector.one(config.get("rawEvidence"), "checkpoint-configuration", prefix + "/config"), job), "checkpoint config")
        inspector.typed["checkpointConfigurations"] += 1
        for sample in fault["samples"]:
            typed = sample["checkpoints"]
            equal(typed, history(inspector.one(typed.get("rawEvidence"), "checkpoint-history", prefix), job), "checkpoint history")
            inspector.typed["checkpointHistories"] += 1
            inspector.job(sample["job"], job)
        failed = fault["failedCheckpoint"]
        equal(failed, checkpoint(inspector.one(failed.get("rawEvidence"), "checkpoint-details", prefix + "/details/" + str(failed["id"]))), "checkpoint details")
        inspector.typed["checkpointDetails"] += 1
        ack = inspector.one(fault.get("triggerSubmitEvidence"), "checkpoint-trigger-submit", prefix, "POST")
        require(ack["request-id"] == fault["triggerId"], "trigger acknowledgement differs")
        status = fault["triggerResult"]
        raw = inspector.one(status.get("rawEvidence"), "checkpoint-trigger-status", prefix + "/" + fault["triggerId"])
        expected = {"state": raw["status"]["id"], "checkpointId": raw.get("operation", {}).get("checkpointId"), "failure": None}
        if raw.get("operation", {}).get("failureCause") is not None:
            expected["failure"] = json.dumps(raw["operation"]["failureCause"], ensure_ascii=False, separators=(",", ":"))
        equal(status, expected, "checkpoint trigger status")
        for name, key in (("beforeKillEvidence", "jobManagerTimeBeforeKill"), ("afterKillEvidence", "jobManagerTimeAfterKill")):
            clock = inspector.one(fault.get(name), "job-manager-clock", "/jobs/" + job)
            require(type(clock["now"]) is int and clock["now"] == fault["kill"][key], "kill clock differs from exact job response")
        inspector.typed["scalarCalls"] += 3
        inspector.materialization(fault["materialization"])
        stage_refs = [sample["job"]["rawEvidence"] + sample["checkpoints"]["rawEvidence"] for sample in fault["samples"]]
        groups = [sample[field]["rawEvidence"][0]["group"] for sample in fault["samples"] for field in ("job", "checkpoints")]
        require(len(groups) == len(set(groups)), "causal stages reuse a REST observation group")
        intervals = [(min(ref["startedNanos"] for ref in refs), max(ref["completedNanos"] for ref in refs)) for refs in stage_refs]
        require(all(left[1] <= right[0] for left, right in zip(intervals, intervals[1:])), "causal source observations were reordered")
        arms = fault["transitions"][0]["acknowledgements"]
        heals = fault["transitions"][1]["acknowledgements"]
        submit = fault["triggerSubmitEvidence"]
        trigger = status["rawEvidence"]
        before_clock, after_clock = fault["beforeKillEvidence"], fault["afterKillEvidence"]
        require(max(ref["completedNanos"] for ref in fault["readiness"]["jobObservation"]["rawEvidence"]) <= intervals[0][0]
                and max(ref["completedNanos"] for ref in failed["rawEvidence"]) <= before_clock[0]["startedNanos"]
                and intervals[-1][1] <= min(ref["startedNanos"] for ref in observed["rawEvidence"])
                and max(ref["completedNanos"] for ref in config["rawEvidence"]) <= min(row["startedNanos"] for row in arms)
                and fault["materialization"]["rawEvidence"]["completedNanos"] <= min(row["startedNanos"] for row in arms)
                and intervals[0][1] <= min(row["startedNanos"] for row in arms)
                and max(row["completedNanos"] for row in arms) <= intervals[1][0]
                and intervals[1][1] <= submit[0]["startedNanos"]
                and submit[-1]["completedNanos"] <= trigger[0]["startedNanos"]
                and trigger[-1]["completedNanos"] <= intervals[2][0]
                and intervals[2][1] <= before_clock[0]["startedNanos"]
                and before_clock[-1]["completedNanos"] <= after_clock[0]["startedNanos"]
                and after_clock[-1]["completedNanos"] <= intervals[3][0]
                and intervals[3][1] <= min(row["startedNanos"] for row in heals)
                and max(row["completedNanos"] for row in heals) <= intervals[4][0],
                "raw REST observations cross the ARM/trigger/kill/HEAL boundary")
        old = fault["kill"]["identity"]["runtimeId"]
        killed = [row["state"]["rawEvidence"] for row in evidence["processFence"]["observations"]
                  if row["runtimeId"] == old and row["moment"] == "after-declared-kill" and row.get("state") is not None]
        require(len(killed) == 1 and before_clock[-1]["completedNanos"] <= killed[0]["startedNanos"]
                and killed[0]["completedNanos"] <= after_clock[0]["startedNanos"], "kill clocks do not enclose the source process-stop observation")
    require(len(inspector.references) <= 8192, "capture reference inventory exceeds bound")
    return {"confirmed": True, "coverage": "retained-source-responses-v1", "captureCount": len(inspector.references),
            "groupCount": len(inspector.groups), "typedComparisons": inspector.typed,
            "captures": list(inspector.references.values()),
            "retainedStores": stores,
            "clockScope": "harness monotonic and JobManager epoch observations; no cross-clock synchronization claim",
            "terminalKafkaScope": "framework terminal-oracle report; no independent raw Kafka replay",
            "derivedFields": ["FileEvidence.observedNanos is the harness observation time",
                              "FileEvidence.regularFile and Materialization.complete assert the pinned inspector contract",
                              "processFence.observedAt is harness observation metadata, not a Docker response field"],
            "authenticityScope": "hash-bound retained bytes; responding daemon authenticity is not attested"}
