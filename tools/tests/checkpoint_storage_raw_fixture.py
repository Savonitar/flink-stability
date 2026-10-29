"""Generated source-format REST/Docker/inspector receipts for integrated offline tests.

This helper translates fixture values into source wire shapes. Independent tests
also handwrite source documents; no production comparison function is called here.
"""
import hashlib
import json
from pathlib import Path


def attach_raw_fixture(result, directory):
    directory = Path(directory)
    index = json.loads((directory / "retained-evidence.json").read_bytes())
    root = "/generated/checkpoints/attempt-fixture"
    result["attempt"]["checkpointRoot"] = root
    evidence = result["evidence"]
    job_id = evidence["stateRecovery"]["jobId"]
    prefix = "/jobs/" + job_id
    sequence = 0
    group_number = 0

    def group(name):
        nonlocal group_number
        group_number += 1
        return name + "-" + str(group_number)

    def source(value, name, endpoint, start, *, method="GET", status=200,
               kind="flink-rest-decoded-body", directory_name="raw-flink-rest"):
        nonlocal sequence
        sequence += 1
        data = (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        relative = directory_name + f"/{sequence:06d}.json"
        destination = directory / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()
        path = root + "/" + relative
        index["files"].append(dict(sourcePath=path, localPath=relative, sha256=digest, kind="raw-response"))
        return dict(path=path, sha256=digest, bytes=len(data), kind=kind, sequence=sequence,
                    startedNanos=start, completedNanos=start + 5, group=name, method=method, endpoint=endpoint, status=status)

    def checkpoint(value):
        if value is None:
            return None
        for key, default in dict(checkpointedSize=16, stateSize=16, numSubtasks=1,
                numAcknowledgedSubtasks=1, failureTimestamp=None, failureMessage=None,
                externalPath=None, discarded=None).items():
            value.setdefault(key, default)
        mapping = dict(id="id", status="status", savepoint="is_savepoint", checkpointType="checkpoint_type",
            triggerTimestamp="trigger_timestamp", latestAckTimestamp="latest_ack_timestamp",
            checkpointedSize="checkpointed_size", stateSize="state_size", numSubtasks="num_subtasks",
            numAcknowledgedSubtasks="num_acknowledged_subtasks", failureTimestamp="failure_timestamp",
            failureMessage="failure_message", externalPath="external_path", discarded="discarded")
        return {wire: value[typed] for typed, wire in mapping.items()}

    def history(value):
        counts = value["counts"]
        restored = value["latestRestored"]
        if restored is not None:
            restored.setdefault("savepoint", False)
            restored.setdefault("externalPath", None)
        return dict(counts={"in_progress": counts["inProgress"], **{key: counts[key] for key in
                         ("total", "completed", "failed", "restored")}},
                    history=[checkpoint(row) for row in value["retained"]], latest=dict(
                        completed=checkpoint(value["latestCompleted"]), failed=checkpoint(value["latestFailed"]),
                        savepoint=checkpoint(value["latestSavepoint"]), restored=None if restored is None else dict(
                            id=restored["id"], restore_timestamp=restored["restoreTimestamp"],
                            is_savepoint=restored["savepoint"], external_path=restored["externalPath"])))

    def job(value, start):
        if "rawEvidence" in value:
            return
        value.setdefault("jobManagerTimeMillis", 200)
        value.setdefault("state", "RUNNING")
        value.setdefault("completedCheckpoints", 1)
        value.setdefault("restoredCheckpoints", 0)
        value.setdefault("latestRestore", None)
        value.setdefault("failures", [])
        value.setdefault("subtasks", [])
        if value["latestRestore"] is not None:
            value["latestRestore"].setdefault("restoredAtMillis", 120)
        grouped = {}
        for subtask in value["subtasks"]:
            grouped.setdefault(subtask["vertexName"], []).append(subtask)
        vertices = [{"id": "vertex-" + str(number), "name": name} for number, name in enumerate(grouped)]
        details = dict(now=value["jobManagerTimeMillis"], state=value["state"], vertices=vertices)
        restored = value["latestRestore"]
        overview = dict(counts=dict(completed=value["completedCheckpoints"], restored=value["restoredCheckpoints"]),
                        latest=dict(restored=None if restored is None else dict(id=restored["checkpointId"],
                                                                         restore_timestamp=restored["restoredAtMillis"])))
        errors = dict(exceptionHistory=dict(entries=[dict(timestamp=row["timestampMillis"],
                exceptionName=row["exceptionName"], stacktrace=row["rootCause"], taskManagerId=row["taskManagerId"])
                for row in value["failures"]]))
        name = group("job-observation")
        bodies = [(prefix, details), (prefix + "/checkpoints", overview), (prefix + "/exceptions?maxExceptions=20", errors)]
        for vertex in vertices:
            bodies.append((prefix + "/vertices/" + vertex["id"], dict(subtasks=[{
                "subtask": row["index"], "attempt": row["attempt"], "status": row["status"],
                "taskmanager-id": row["taskManagerId"]} for row in grouped[vertex["name"]]])))
        value["rawEvidence"] = [source(body, name, endpoint, start + number * 10)
                                for number, (endpoint, body) in enumerate(bodies)]

    seen = set()

    def readiness(value):
        if id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, dict):
            if "jobObservation" in value and "completedSnapshots" in value:
                completed = [dict(id=row["checkpointId"], status="COMPLETED", latest_ack_timestamp=row["acknowledgedAtMillis"])
                             for row in value["completedSnapshots"].values()]
                value["checkpointEvidence"] = [source(dict(counts=dict(in_progress=0), history=completed,
                    latest=dict(completed=max(completed, key=lambda row: row["id"]) if completed else None)),
                    group("checkpoint-overview"), prefix + "/checkpoints", 50)]
                job(value["jobObservation"], 100)
            for key, child in list(value.items()):
                if key not in ("rawEvidence", "markers", "operations"):
                    readiness(child)
        elif isinstance(value, list):
            for child in value:
                readiness(child)

    readiness(evidence)
    for fault in evidence.get("checkpointStorageFaults", []):
        config = fault["configuration"]
        mapping = dict(mode="mode", intervalMillis="interval", timeoutMillis="timeout", minPauseMillis="min_pause",
            maxConcurrent="max_concurrent", tolerableFailedCheckpoints="tolerable_failed_checkpoints", stateBackend="state_backend",
            checkpointStorage="checkpoint_storage", unalignedCheckpoints="unaligned_checkpoints", stateChangelogEnabled="state_changelog_enabled")
        raw = {wire: config[typed] for typed, wire in mapping.items()}
        raw["externalization"] = dict(enabled=config["externalized"], delete_on_cancellation=config["deleteOnCancellation"])
        cp_prefix = prefix + "/checkpoints"
        config["rawEvidence"] = [source(raw, group("checkpoint-configuration"), cp_prefix + "/config", 300)]
        materialization = fault["materialization"]
        fields = ("path", "size", "sha256")
        raw = dict(version=1, jobId=materialization["jobId"], checkpointId=materialization["checkpointId"],
            operatorUid=materialization["operatorUid"], metadata={key: materialization["metadata"][key] for key in fields},
            files=[{key: file[key] for key in fields} for file in materialization["files"]])
        materialization["rawEvidence"] = source(raw, group("checkpoint-inspector"), "/containers/" +
            materialization["instance"]["runtimeId"] + "/checkpoint-inspector/" + job_id + "/" + str(fault["checkpointId"]),
            400, method="EXEC", status=0, kind="checkpoint-inspector-stdout", directory_name="raw-storage")
        for sample, start in zip(fault["samples"], (500, 1200, 1600, 2200, 3200, 4000)):
            typed = sample["checkpoints"]
            job(sample["job"], start)
            typed["rawEvidence"] = [source(history(typed), group("checkpoint-history"), cp_prefix, start + 60)]
        failed = fault["failedCheckpoint"]
        failed["rawEvidence"] = [source(checkpoint(failed), group("checkpoint-details"),
                                         cp_prefix + "/details/" + str(failed["id"]), 1700)]
        fault["triggerSubmitEvidence"] = [source({"request-id": fault["triggerId"]}, group("checkpoint-trigger-submit"),
                                                  cp_prefix, 1400, method="POST")]
        trigger = fault["triggerResult"]
        failure = json.loads(trigger["failure"]) if trigger["failure"] is not None else None
        trigger["failure"] = json.dumps(failure, ensure_ascii=False, separators=(",", ":")) if failure is not None else None
        raw = dict(status=dict(id=trigger["state"]), operation=dict(checkpointId=trigger["checkpointId"], failureCause=failure))
        trigger["rawEvidence"] = [source(raw, group("checkpoint-trigger-status"), cp_prefix + "/" + fault["triggerId"], 1500)]
        for target, key, start in (("beforeKillEvidence", "jobManagerTimeBeforeKill", 1800),
                                   ("afterKillEvidence", "jobManagerTimeAfterKill", 1900)):
            fault[target] = [source(dict(now=fault["kill"][key]), group("job-manager-clock"), prefix, start)]
    final = evidence["flinkJob"]
    observation = dict(state=final["state"], completedCheckpoints=final["completedCheckpoints"],
                       restoredCheckpoints=final["restoredCheckpoints"], latestRestore=None)
    if final["restoredCheckpoints"]:
        observation["latestRestore"] = dict(checkpointId=final["latestRestoredCheckpoint"], restoredAtMillis=120)
    job(observation, 5000)
    final.update(observation=observation, failures=len(observation["failures"]))
    for number, observation in enumerate(evidence["processFence"]["observations"]):
        state = observation.get("state")
        if state is None:
            continue
        state.setdefault("finishedAt", None)
        raw = dict(Id=state["runtimeId"], State=dict(Running=state["running"], Paused=state["paused"],
            OOMKilled=state["oomKilled"], ExitCode=state["exitCode"], FinishedAt=state["finishedAt"] or ""))
        start = 1850 if observation["moment"] == "after-declared-kill" else 6000 + number * 10
        state["rawEvidence"] = source(raw, group("docker-inspect"), "/v1.44/containers/" + state["runtimeId"] + "/json", start,
            kind="docker-inspect-id-state-source-projection", directory_name="raw-storage")
    (directory / "retained-evidence.json").write_text(json.dumps(index, sort_keys=True) + "\n")
    return result
