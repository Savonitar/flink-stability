"""Independent, bounded inspection of retained checkpoint-storage evidence.

Original source paths are lookup keys only. This module never opens paths from
the result outside the copied attempt evidence, starts processes, or repairs a run.
"""
import copy
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import re

JOURNAL_BYTES = 16 * 1024 * 1024
JOURNAL_RECORDS = 25000
JOURNAL_TOTAL = 128 * 1024 * 1024
FILE_BYTES = 64 * 1024 * 1024
UID = "flink-stability-keyed-prefix-v1"
READY = "direct_io=1,writeback_cache=0,entry_timeout=0,attr_timeout=0,negative_timeout=0,kernel_cache=0,auto_cache=0"
STAGES = ("BEFORE_ARM", "ARMED", "FAILED_CHECKPOINT", "STOPPED", "HEALED", "RECOVERED")
PROJECTED = ("sequence", "revision", "state", "faultId", "operation", "path", "faultOperation",
             "result", "errno", "requestedBytes", "returnedBytes", "datasync", "reason")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def sha(data):
    return hashlib.sha256(data).hexdigest()


def hash_value(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "Invalid evidence SHA-256")
    return value


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON key: " + key)
        result[key] = value
    return result


def parse(data):
    return json.loads(data, object_pairs_hook=unique,
                      parse_constant=lambda unused: (_ for _ in ()).throw(ValueError("Non-finite JSON number")))


def path_under(root, relative):
    root = Path(root).absolute()
    require(isinstance(relative, str) and relative and not Path(relative).is_absolute()
            and ".." not in Path(relative).parts, "Evidence path escapes its scope")
    path = root / relative
    require(not {"Documents", "Downloads", "Desktop", "Pictures", "Photos", "Videos", ".ssh", ".aws", ".gnupg", ".git"}.intersection(path.parts),
            "Prohibited evidence path")
    cursor = Path(path.anchor)
    for part in path.parts[1:]:
        cursor /= part
        require(not cursor.is_symlink(), "Symlink evidence is rejected")
    return path


def read(root, relative, expected=None, maximum=FILE_BYTES):
    path = path_under(root, relative)
    require(path.is_file() and path.stat().st_size <= maximum, "Missing or oversized evidence: " + relative)
    data = path.read_bytes()
    require(len(data) <= maximum, "Evidence grew beyond its limit")
    if expected is not None:
        require(sha(data) == hash_value(expected), "Evidence hash mismatch: " + relative)
    return data


def projected_operation(row, identity, extended=False):
    expected = {key: row[key] for key in PROJECTED}
    expected.update(instance=identity, timestampMillis=row["timestampMs"], monotonicNanos=row["monotonicNs"])
    if extended:
        expected.update(counters=row.get("counters", {}), pathTruncated=row.get("pathTruncated", False))
    return expected


def relative_journal_path(path):
    return path.removeprefix("/flink/checkpoints/").removeprefix("/")


def bound_operation(row, storage):
    paths = {relative_journal_path(file["path"]) for file in storage["materializedFiles"]}
    return (row["operation"] in {"START", "READY", "ARM", "HEAL", "ABORT", "STOP", "EXPIRE", "FATAL", "REJECT"}
            or row["errno"] == 5 and row["operation"] in {"create", "write"}
            or relative_journal_path(row["path"]) in paths and row["operation"] in {"create", "write", "read"})


def verify_operation_projection(instance, storage, rows):
    extended = any("counters" in row for row in rows) or any("counters" in row or "pathTruncated" in row for row in instance["operations"])
    projected = [projected_operation(row, instance["instance"], extended) for row in rows]
    representation = instance.get("operationsRepresentation")
    if representation is None:
        require(json.dumps(instance["operations"], sort_keys=True) == json.dumps(projected, sort_keys=True),
                "Typed native operation differs from raw journal")
        return
    require(representation == "bound-subset-v1", "Unknown native journal representation")
    require(type(instance.get("operationCount")) is int and instance["operationCount"] == len(rows),
            "Native operation count differs from raw journal")
    counts = Counter((row["operation"], row["state"], row["errno"]) for row in rows)
    expected_counts = [{"operation": operation, "state": state, "errno": errno, "count": count}
                       for (operation, state, errno), count in sorted(counts.items())]
    require(json.dumps(instance.get("operationCounts"), sort_keys=True) == json.dumps(expected_counts, sort_keys=True),
            "Native operation totals differ from raw journal")
    expected = [value for row, value in zip(rows, projected) if bound_operation(row, storage)]
    require(json.dumps(instance["operations"], sort_keys=True) == json.dumps(expected, sort_keys=True),
            "Bound native operation subset differs from raw journal")


LEGACY_IO_OPERATIONS = {"read", "getattr", "readdir", "access", "flush", "release"}
IO_OPERATIONS = LEGACY_IO_OPERATIONS | {"fsync", "fdatasync"}


def io_counters(value):
    require(isinstance(value, dict) and set(value) in (LEGACY_IO_OPERATIONS, IO_OPERATIONS),
            "Incomplete native IO counters")
    for operation, counter in value.items():
        require(isinstance(counter, dict) and set(counter) == {"count", "bytes"}
                and all(integer(counter[key]) and counter[key] <= 2**63 - 1 for key in counter)
                and (counter["count"] > 0 or counter["bytes"] == 0)
                and (operation == "read" or counter["bytes"] == 0), "Invalid native IO counter")
    return value


def counters_advance(previous, current):
    return (not previous or set(previous) == set(current)) and all(current[operation][key] >= previous[operation][key]
               for operation in previous for key in ("count", "bytes"))


class RetainedEvidence:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.index = parse(read(directory, "retained-evidence.json"))
        require(self.index.get("version") == 1 and isinstance(self.index.get("files"), list)
                and len(self.index["files"]) <= 8704, "Invalid retained evidence index")
        self.files, self.local, self.journals = {}, {}, {}
        self.journal_bytes = 0
        total = 0
        for row in self.index["files"]:
            require(set(row) == {"sourcePath", "localPath", "sha256", "kind"}, "Malformed retained file receipt")
            require(isinstance(row["sourcePath"], str) and row["sourcePath"]
                    and isinstance(row["kind"], str) and row["kind"], "Invalid retained source identity")
            require(row["sourcePath"] not in self.files and row["localPath"] not in self.local,
                    "Duplicate retained source or destination")
            data = read(directory, row["localPath"], row["sha256"])
            total += len(data)
            require(total <= 512 * 1024 * 1024, "Retained evidence exceeds aggregate bound")
            self.files[row["sourcePath"]] = row
            self.local[row["localPath"]] = row

    def raw(self, reference, parse_json=True):
        """Resolve a source label only through the locally retained, hashed file inventory."""
        receipt = self.files.get(reference["path"])
        require(receipt is not None and receipt["kind"] == "raw-response"
                and receipt["sha256"] == reference["sha256"], "Raw response has no matching retained copy")
        data = read(self.directory, receipt["localPath"], reference["sha256"], 2 * 1024 * 1024)
        require(len(data) == reference["bytes"], "Raw response byte length differs")
        return parse(data.decode("utf-8", errors="strict")) if parse_json else data

    def raw_inventory(self, checkpoint_root):
        stores = {name: {"files": 0, "bytes": 0} for name in ("raw-flink-rest", "raw-storage")}
        for source, receipt in self.files.items():
            if receipt["kind"] != "raw-response":
                continue
            path = PurePosixPath(source)
            name = path.parent.name
            require(name in stores and str(path.parent.parent) == checkpoint_root
                    and re.fullmatch(r"[0-9]{6}\.json", path.name) and 1 <= int(path.stem) <= 4096,
                    "Raw response inventory is outside its source store")
            data = read(self.directory, receipt["localPath"], receipt["sha256"], 2 * 1024 * 1024)
            require(data, "Raw response inventory contains an empty body")
            stores[name]["files"] += 1; stores[name]["bytes"] += len(data)
            require(stores[name]["files"] <= 4096 and stores[name]["bytes"] <= 64 * 1024 * 1024,
                    "Raw response store exceeds its capture bound")
        return stores

    def journal(self, instance, storage):
        source = instance["journalPath"]
        require(source in self.files, "Native journal has no retained copy")
        receipt = self.files[source]
        require(receipt["kind"] in ("journal", "native-journal", "native-journals"), "Journal receipt has wrong kind")
        require(receipt["sha256"] == instance["journalSha256"], "Journal receipt hash differs from result")
        if source not in self.journals:
            data = read(self.directory, receipt["localPath"], receipt["sha256"], JOURNAL_BYTES)
            require(data and data.endswith(b"\n"), "Native journal is empty or truncated")
            rows = [parse(line) for line in data.splitlines()]
            require(len(rows) <= JOURNAL_RECORDS, "Native journal record limit exceeded")
            self.journal_bytes += len(data)
            require(self.journal_bytes <= JOURNAL_TOTAL, "Aggregate native journal limit exceeded")
            self.journals[source] = (data, rows)
        data, rows = self.journals[source]
        require(integer(instance["journalBytes"], 1) and len(data) == instance["journalBytes"]
                and integer(instance["lastSequence"], 1) and len(rows) == instance["lastSequence"], "Journal length or sequence differs from result")
        identity = instance["instance"]
        require(len(rows) >= 2 and rows[0]["operation"] == "START" and rows[0]["state"] == "STARTING"
                and rows[1]["operation"] == "READY" and rows[1]["state"] == "HEALTHY" and rows[1]["reason"] == READY
                and sum(row["operation"] == "READY" for row in rows) == 1, "Native mount readiness/cache-policy receipt is missing")
        previous = 0
        previous_counters = {}
        counter_schema = None
        counter_samples = []
        verify_operation_projection(instance, storage, rows)
        for sequence, row in enumerate(rows, 1):
            require(row["version"] == 1 and row["sequence"] == sequence and row["journalComplete"] is True
                    and row["state"] in ("STARTING", "HEALTHY", "ARMED")
                    and integer(row["monotonicNs"], previous), "Invalid, expired, aborted or incomplete native journal")
            previous = row["monotonicNs"]
            counters = io_counters(row["counters"]) if "counters" in row else {}
            if counter_schema is None:
                counter_schema = frozenset(counters)
            if counters or row["operation"][0].isupper():
                require(frozenset(counters) == counter_schema, "Native IO counter schema changed within instance")
            if row["operation"] in {"START", "READY"} and counters:
                require(all(value["count"] == 0 for value in counters.values()), "Startup IO counters are not zero")
            if previous_counters and row["operation"][0].isupper():
                require(counters, "Lifecycle record lost IO counters")
            if row["operation"] == "SUMMARY":
                require(counters and counters != previous_counters, "Idle or incomplete native SUMMARY")
            if counters:
                require(counters_advance(previous_counters, counters), "Native IO counters moved backwards")
                previous_counters = counters
                counter_samples.append((sequence, counters))
            truncated = row.get("pathTruncated", False)
            require(type(truncated) is bool and (not truncated or row["errno"] == 36
                    and row["result"] == -1 and row["reason"] == "receipt-path-truncated"), "Invalid truncated-path receipt")
            require(all(row[key] == storage[key] for key in ("attemptId", "storageId", "namespace"))
                    and all(row[key] == identity[key] for key in ("instanceId", "daemonPid")), "Native journal identity mismatch")
            require(all(integer(row[key]) for key in ("revision", "timestampMs", "requestedBytes", "returnedBytes", "errno"))
                    and type(row["result"]) is int, "Malformed native operation numbers")
            if row["state"] == "ARMED":
                require(row["leaseMs"] == 120000 and row["monotonicNs"] < row["leaseDeadlineMonotonicNs"], "Armed journal operation outlived its lease")
        status = instance["status"]
        require(integer(status["sequence"], 1) and status["sequence"] <= len(rows), "Stage status has no native sequence")
        current = rows[status["sequence"] - 1]
        if counter_samples:
            observed = io_counters(status.get("counters"))
            require(frozenset(observed) == counter_schema, "STATUS IO counter schema differs from journal")
            for sequence, counters in counter_samples:
                require(counters_advance(counters, observed) if sequence <= status["sequence"]
                        else counters_advance(observed, counters), "STATUS counters conflict with journal capture")
        else:
            require(status.get("counters") is None or status["counters"] == {},
                    "Legacy journal cannot acquire STATUS IO counters")
        require(all(status[key] == current[key] for key in ("state", "faultId", "revision", "leaseMs", "leaseDeadlineMonotonicNs", "journalComplete"))
                and status["stopping"] is False, "Stage status differs from its native receipt")
        return data, rows


def stopped_ids(evidence):
    observations = evidence["processFence"]["observations"]
    result = set()
    for row in observations:
        if row["moment"] not in ("after-declared-kill", "after-fence-kill"):
            continue
        state = row.get("state")
        if (row.get("missing") is False and row.get("diagnostic") is None and isinstance(state, dict)
                and state.get("runtimeId") == row["runtimeId"] and state.get("running") is False
                and state.get("paused") is False and state.get("oomKilled") is False and integer(state.get("exitCode"))):
            result.add(row["runtimeId"])
    return result


def checkpoint_history(value, job):
    require(value["jobId"] == job, "Checkpoint history belongs to another job")
    counts = value["counts"]
    require(set(counts) == {"total", "inProgress", "completed", "failed", "restored"}
            and all(integer(number) for number in counts.values())
            and counts["total"] == counts["completed"] + counts["failed"] + counts["inProgress"],
            "Checkpoint counts are inconsistent")
    retained = value["retained"]
    require(isinstance(retained, list) and len(retained) <= 1024, "Invalid checkpoint history bound")
    rows = {row["id"]: row for row in retained}
    require(len(rows) == len(retained) and all(integer(key, 1) for key in rows), "Duplicate or invalid checkpoint IDs")
    for row in retained:
        require(row["status"] in ("COMPLETED", "FAILED", "IN_PROGRESS") and row["savepoint"] is False
                and row["checkpointType"] == "CHECKPOINT" and integer(row["triggerTimestamp"], 1),
                "Unexpected checkpoint history entry")
    return counts, rows


def verify_ack(ack, transition, final, rows):
    require(ack["confirmed"] is True and ack["exitCode"] == 0 and ack["stderr"] == ""
            and transition["startedNanos"] <= ack["startedNanos"] <= ack["completedNanos"] <= transition["completedNanos"],
            "Control acknowledgement failed or has inconsistent clocks")
    reply = parse(ack["stdout"])
    status = ack["status"]
    require(integer(status["sequence"], 1) and status["sequence"] <= len(rows), "Control journal receipt is missing")
    row = rows[status["sequence"] - 1]
    require(reply["version"] == 1 and reply["ok"] is True and reply["error"] is None
            and reply["stopping"] is False and row["operation"] == ack["command"]
            and row["result"] == row["errno"] == 0 and row["reason"] == "accepted", "Control reply is not an accepted native operation")
    require(all(reply[key] == final[key] for key in ("attemptId", "storageId", "namespace"))
            and all(reply[key] == ack["instance"][key] for key in ("instanceId", "daemonPid")), "Control reply identity mismatch")
    for key in ("state", "faultId", "revision", "sequence", "leaseMs", "leaseDeadlineMonotonicNs", "journalComplete"):
        require(reply[key] == status[key] == row[key], "Control reply differs from native receipt")
    if "counters" in row:
        require(io_counters(reply.get("counters")) == io_counters(status.get("counters")) == row["counters"],
                "Control counters differ from native receipt")
    else:
        require((reply.get("counters") is None or reply["counters"] == {})
                and (status.get("counters") is None or status["counters"] == {}),
                "Legacy control cannot acquire IO counters")
    require(row["faultId"] == transition["faultId"] and row["revision"] == transition["revision"]
            and row["leaseMs"] == 120000 and row["monotonicNs"] < row["leaseDeadlineMonotonicNs"]
            and reply["allowedOperations"] == ["create", "write"], "Fault rule or lease differs from frozen contract")
    return row


def materialization(proof, before, final, job, checkpoint, arm_started, original, replacement):
    require(proof["complete"] is True and proof["diagnostics"] == [] and proof["jobId"] == job
            and proof["checkpointId"] == checkpoint and proof["operatorUid"] == UID,
            "Exact keyed checkpoint materialization is missing")
    managers = [item for item in before["instances"] if item["instance"] == proof["instance"]]
    require(len(managers) == 1 and proof["instance"]["role"] == "JOB_MANAGER", "Materialization belongs to another instance")
    files = proof["files"]
    require(isinstance(files, list) and 1 <= len(files) <= 128, "Missing or excessive external keyed state files")
    expected_prefix = "/flink/checkpoints/" + final["namespace"] + "/" + job + "/chk-" + str(checkpoint) + "/"
    require(proof["metadata"]["path"] == expected_prefix + "_metadata", "Metadata path differs from baseline checkpoint")
    paths = set()
    for file in [proof["metadata"], *files]:
        path = file["path"]
        require(isinstance(path, str) and path.startswith(expected_prefix)
                and ".." not in PurePosixPath(path).parts and str(PurePosixPath(path)) == path
                and path not in paths and file["regularFile"] is True and integer(file["size"], 1)
                and integer(file["observedNanos"], 1) and file["observedNanos"] < arm_started,
                "Unsafe, duplicate or late materialization file evidence")
        paths.add(path)
        hash_value(file["sha256"])
        require(file in before["materializedFiles"] and file in final["materializedFiles"], "Materialization receipts were not preserved")
        relative = path.removeprefix("/flink/checkpoints/")
        reads = sum(row["returnedBytes"] for row in managers[0]["operations"]
                    if row["path"] == relative and row["operation"] == "read" and row["errno"] == 0)
        require(reads >= file["size"], "Inspector did not read the materialized file through FUSE")
        if file in files:
            require(not path.endswith("/_metadata"), "Metadata was substituted for keyed state")
            writes = sum(row["returnedBytes"] for row in original["operations"]
                         if row["path"] == relative and row["operation"] == "write" and row["errno"] == 0)
            restored_reads = sum(row["returnedBytes"] for row in replacement["operations"]
                                 if row["path"] == relative and row["operation"] == "read" and row["errno"] == 0)
            require(writes == file["size"] and restored_reads >= file["size"], "Original write or replacement read of keyed state is missing")
    require(proof["metadata"]["size"] <= JOURNAL_BYTES and sum(row["size"] for row in files) <= FILE_BYTES,
            "Checkpoint materialization exceeds inspector limits")
    return {"metadata": proof["metadata"], "files": files,
            "proofKind": "pinned-inspector-receipt-and-native-io", "rawCheckpointBytesRetained": False}


def audit_storage(result, cell, manifest, retained):
    # Expand only a private copy after checking every reference and inline subset.
    result = copy.deepcopy(result)
    evidence = result["evidence"]
    final = evidence["checkpointStorage"]
    require(final["complete"] is True and final["quiescent"] is True and final["diagnostics"] == [], "Final storage capture is incomplete")
    require(final["helperManifestSha256"] == manifest["helperBundle"]["manifestSha256"]
            and re.fullmatch(r"job_[0-9a-f]{32}", final["namespace"])
            and final["checkpointUri"] == "file:///flink/checkpoints/" + final["namespace"], "Storage namespace or helper pin mismatch")
    instances = final["instances"]
    expected_count = 2 if cell["execution"] == "healthy" else 3
    require(len(instances) == expected_count, "Wrong number of storage process incarnations")
    identities = {item["instance"]["instanceId"]: item["instance"] for item in instances}
    require(len(identities) == len(instances), "Duplicate storage instance identity")
    runtime = evidence["flinkRuntime"]
    require({item["runtimeId"] for item in identities.values()} == {item["runtimeId"] for item in runtime["components"]}, "Storage and Flink process identities differ")
    require({item["runtimeId"] for item in identities.values()} <= stopped_ids(evidence), "Storage process lacks physical stopped evidence")
    expected_config = {"execution.checkpointing.data-inline-threshold": "0 bytes",
                       "execution.checkpointing.max-concurrent-checkpoints": "1",
                       "execution.checkpointing.tolerable-failed-checkpoints": "10"}
    require(runtime["config"] == expected_config, "Checkpoint materialization configuration differs")
    for component in runtime["components"]:
        config = component["effectiveConfiguration"]
        require(component["config"] == config["observedValues"] == expected_config
                and config["sourceSha256"] == hash_value(config["observedSha256"]), "Effective Flink configuration is unproven")
    faults = evidence.get("checkpointStorageFaults", [])
    require(len(faults) == (1 if cell["execution"] == "eio" else 0), "Unexpected or missing storage fault")
    snapshots = [sample["storage"] for sample in faults[0]["samples"]] + [final] if faults else [final]
    final_data, final_rows = {}, {}
    for item in instances:
        require(item["processDead"] is True, "Final storage daemon is not stopped")
        final_data[item["instance"]["instanceId"]], final_rows[item["instance"]["instanceId"]] = retained.journal(item, final)
    references = 0
    hydrate = []
    for snapshot in snapshots:
        require(snapshot["complete"] is True and snapshot["diagnostics"] == [], "A storage stage is incomplete")
        require(all(snapshot[key] == final[key] for key in ("attemptId", "storageId", "namespace", "checkpointUri", "helperManifestSha256")), "Stage storage identity changed")
        seen = set()
        for item in snapshot["instances"]:
            identity = item["instance"]
            iid = identity["instanceId"]
            require(iid not in seen and identities.get(iid) == identity and integer(identity["daemonPid"], 1), "Stage has duplicate or changed daemon identity")
            seen.add(iid)
            require(item["journalComplete"] is True and item["diagnostics"] == [], "Stage native journal is incomplete")
            data, rows = retained.journal(item, snapshot)
            hydrate.append((item, rows))
            require(final_data[iid].startswith(data), "Earlier journal is not an immutable prefix of final capture")
            references += 1
            verification = item["verification"]
            helper = manifest["helperBundle"]
            require(verification["complete"] is True and verification["diagnostics"] == []
                    and verification["uid"] == verification["gid"] == [9999] * 4
                    and verification["capabilities"] == "0000000000000000"
                    and integer(verification["jvmPid"], 1)
                    and verification["rawReadDenied"] is True and verification["rawWriteDenied"] is True
                    and " /flink/checkpoints " in verification["mountInfo"] and " - fuse " in verification["mountInfo"]
                    and verification["imageId"] == helper["imageId"]
                    and verification["helperSha256"] == helper["files"]["checkpoint-fuse"]
                    and verification["probeSha256"] == helper["files"]["probe.jar"], "FUSE mount, isolation or helper identity is unproven")
    for item, rows in hydrate:
        item["operations"] = [projected_operation(row, item["instance"]) for row in rows]
    summary = {"complete": True, "journalReferences": references, "uniqueJournals": len(retained.journals),
               "journalBytes": retained.journal_bytes, "finalJournals": [
                   {"instance": item["instance"], "sha256": item["journalSha256"], "bytes": item["journalBytes"], "records": item["lastSequence"]}
                   for item in instances]}
    if not faults:
        require(all(row["state"] in ("STARTING", "HEALTHY") and row["errno"] != 5
                    and row["faultId"] is None and row["revision"] == 0
                    for rows in final_rows.values() for row in rows), "Healthy storage control contains a fault")
        return summary
    fault = faults[0]
    samples = fault["samples"]
    require([sample["stage"] for sample in samples] == list(STAGES)
            and [sample["sequence"] for sample in samples] == list(range(1, 7)), "Missing, reordered or duplicate causal stages")
    require(all(integer(sample["elapsedNanos"], 1) for sample in samples)
            and all(a["elapsedNanos"] < b["elapsedNanos"] for a, b in zip(samples, samples[1:]))
            and fault["timeoutMillis"] == 90000 and samples[-1]["elapsedNanos"] <= 90000000000,
            "Causal stages exceeded their shared deadline")
    require(fault["confirmed"] is True and fault["finalCaptureConfirmed"] is True and fault["errors"] == [], "Storage fault remained unconfirmed")
    before, armed, failed, stopped, healed, recovered = samples
    configuration = {key: value for key, value in fault["configuration"].items() if key != "rawEvidence"}
    require(configuration == {"jobId": fault["jobId"], "mode": "exactly_once", "intervalMillis": 10000,
                              "timeoutMillis": 600000, "minPauseMillis": 0, "maxConcurrent": 1,
                              "tolerableFailedCheckpoints": 10, "stateBackend": "HashMapStateBackend",
                              "checkpointStorage": "FileSystemCheckpointStorage", "unalignedCheckpoints": False,
                              "stateChangelogEnabled": False, "externalized": False, "deleteOnCancellation": True},
            "Observed checkpoint configuration differs from the fixed experiment")
    job, baseline, failed_id = fault["jobId"], fault["checkpointId"], fault["failedCheckpointId"]
    require(job == evidence["stateRecovery"]["jobId"] and integer(baseline, 1) and integer(failed_id, baseline + 1), "Invalid checkpoint correlation IDs")
    bc, br = checkpoint_history(before["checkpoints"], job)
    ac, ar = checkpoint_history(armed["checkpoints"], job)
    fc, fr = checkpoint_history(failed["checkpoints"], job)
    sc, sr = checkpoint_history(stopped["checkpoints"], job)
    checkpoint_history(healed["checkpoints"], job)
    rc, rr = checkpoint_history(recovered["checkpoints"], job)
    require(baseline in br and br[baseline]["status"] == "COMPLETED" and before["checkpoints"]["latestCompleted"] == br[baseline]
            and max(br) == baseline and bc["inProgress"] == bc["restored"] == 0, "Completed baseline was not exclusive before arming")
    require({key: value for key, value in armed["checkpoints"].items() if key != "rawEvidence"}
            == {key: value for key, value in before["checkpoints"].items() if key != "rawEvidence"},
            "Checkpoint boundary changed during ARM")
    require(set(fr) - set(br) == {failed_id} and all(fr[key] == value for key, value in br.items())
            and {key: value for key, value in fr[failed_id].items() if key != "rawEvidence"}
            == {key: value for key, value in fault["failedCheckpoint"].items() if key != "rawEvidence"}
            and fr[failed_id]["status"] == "FAILED"
            and failed["checkpoints"]["latestCompleted"] == br[baseline]
            and fc == dict(bc, total=bc["total"] + 1, failed=bc["failed"] + 1), "Failed trigger does not identify one unique new failed checkpoint")
    require(failed["checkpoints"]["latestFailed"] == fr[failed_id], "Latest failure differs from correlated checkpoint")
    kill, restart = fault["kill"], fault["restart"]
    require(len(evidence["taskManagerKills"]) == len(evidence["taskManagerRestarts"]) == 1, "Expected exactly one physical kill/replacement")
    generic_kill = evidence["taskManagerKills"][0]
    old, new = kill["identity"], restart["replacementIdentity"]
    require(kill["identityFailure"] is None and restart["previousIdentityFailure"] is None and restart["replacementIdentityFailure"] is None
            and old == restart["previousIdentity"] == generic_kill["identity"]
            and all(kill[key] == generic_kill[key] for key in ("jobManagerTimeBeforeKill", "jobManagerTimeAfterKill"))
            and new == evidence["taskManagerRestarts"][0]["replacementIdentity"]
            and old["runtimeId"] != new["runtimeId"] and old["resourceId"] != new["resourceId"], "Physical kill/replacement binding differs")
    old_item = next(item for item in instances if item["instance"]["runtimeId"] == old["runtimeId"])
    new_item = next(item for item in instances if item["instance"]["runtimeId"] == new["runtimeId"])
    manager = next(item for item in instances if item["instance"]["role"] == "JOB_MANAGER")
    previous_clock = 0
    for sample in samples:
        observation = sample["job"]
        require(integer(observation["jobManagerTimeMillis"], previous_clock), "Stage JobManager clock moved backwards")
        previous_clock = observation["jobManagerTimeMillis"]
        if sample["stage"] in ("BEFORE_ARM", "ARMED", "FAILED_CHECKPOINT"):
            require(observation["state"] == "RUNNING" and observation["restoredCheckpoints"] == 0
                    and observation["completedCheckpoints"] == bc["completed"] and observation["failures"] == []
                    and len(observation["subtasks"]) == 2
                    and len({(row["vertexName"], row["index"]) for row in observation["subtasks"]}) == 2
                    and all(row["status"] == "RUNNING" and row["index"] == 0
                            and row["attempt"] == fault["readiness"]["executionAttempt"]
                            and row["taskManagerId"] == old["resourceId"] for row in observation["subtasks"]),
                    "Fault stage is not the original RUNNING execution")
        elif sample["stage"] in ("STOPPED", "HEALED"):
            require(not any(row["status"] in ("DEPLOYING", "INITIALIZING", "RUNNING") for row in observation["subtasks"]),
                    "Tasks remained active before replacement")
        else:
            require(observation["state"] == "RUNNING" and observation["restoredCheckpoints"] == 1
                    and observation["latestRestore"]["checkpointId"] == baseline
                    and len(observation["subtasks"]) == 2 and all(row["status"] == "RUNNING"
                        and row["taskManagerId"] == new["resourceId"] and row["attempt"] > fault["readiness"]["executionAttempt"]
                        for row in observation["subtasks"]), "Recovery stage is not the replacement RUNNING execution")
    require(kill["stateReadiness"] == fault["readiness"] and kill["jobBeforeKill"]["failure"] is None
            and kill["jobBeforeKill"]["observation"] == failed["job"], "Kill was not bound to the final failed-checkpoint observation")
    original_set = {old_item["instance"]["instanceId"], manager["instance"]["instanceId"]}
    for sample in samples:
        require({item["instance"]["instanceId"] for item in sample["storage"]["instances"]}
                == (set(identities) if sample["stage"] == "RECOVERED" else original_set), "Stage omitted or added a storage incarnation")
        for item in sample["storage"]["instances"]:
            state = item["status"]["state"]
            wanted = "HEALTHY" if sample["stage"] == "BEFORE_ARM" or item["instance"] == new_item["instance"] else "ARMED"
            if sample["stage"] in ("HEALED", "RECOVERED") and item["instance"] == manager["instance"]:
                wanted = "HEALTHY"
            require(state == wanted, "Storage stage has the wrong fault state")
    require([item["instance"] for item in stopped["storage"]["instances"] if item["processDead"]] == [old_item["instance"]], "STOPPED lacks the exact dead TaskManager")
    transitions = fault["transitions"]
    require(len(transitions) == 2 and [item["revision"] for item in transitions] == [1, 2], "Unexpected control transitions")
    arm, heal = transitions
    require(all(item["confirmed"] is True and item["diagnostics"] == [] and item["faultId"] == fault["triggerId"] for item in transitions)
            and arm["completedNanos"] < heal["startedNanos"] and arm["deadInstances"] == []
            and heal["deadInstances"] == [old_item["instance"]], "Control transition identity or dead-instance coverage is invalid")
    require(len(arm["acknowledgements"]) == 2 and {item["instance"]["instanceId"] for item in arm["acknowledgements"]}
            == {manager["instance"]["instanceId"], old_item["instance"]["instanceId"]}
            and all(item["command"] == "ARM" for item in arm["acknowledgements"])
            and len(heal["acknowledgements"]) == 1 and heal["acknowledgements"][0]["command"] == "HEAL"
            and heal["acknowledgements"][0]["instance"] == manager["instance"], "ARM/HEAL acknowledgement set differs")
    acknowledgements = []
    for transition in transitions:
        for ack in transition["acknowledgements"]:
            require(ack["instance"] == identities[ack["instance"]["instanceId"]], "Control used another daemon")
            row = verify_ack(ack, transition, final, final_rows[ack["instance"]["instanceId"]])
            require(ack in next(item for item in instances if item["instance"] == ack["instance"])["commands"], "Transition control receipt is missing from final capture")
            acknowledgements.append(row)
    controls = ("ARM", "HEAL", "ABORT", "EXPIRED", "STOP", "FATAL")
    for item, wanted in ((old_item, ["ARM"]), (manager, ["ARM", "HEAL"]), (new_item, [])):
        rows = final_rows[item["instance"]["instanceId"]]
        require([row["operation"] for row in rows if row["operation"] in controls] == wanted, "Unexpected hidden native fault transition")
        revision = 0
        for row in rows:
            if row["operation"] == "ARM": revision = 1
            elif row["operation"] == "HEAL": revision = 2
            require(row["revision"] == revision and row["faultId"] == (fault["triggerId"] if revision else None)
                    and (row["state"] == "ARMED" if revision == 1 else row["state"] in ("STARTING", "HEALTHY")),
                    "Native fault state does not follow the accepted controls")
    require(max(row["timestampMs"] for row in acknowledgements if row["operation"] == "ARM") <= fr[failed_id]["triggerTimestamp"]
            <= fr[failed_id]["failureTimestamp"] <= kill["jobManagerTimeBeforeKill"] <= kill["jobManagerTimeAfterKill"], "ARM/failure/kill clocks are out of order")
    require(br[baseline]["latestAckTimestamp"] <= min(row["timestampMs"] for row in acknowledgements if row["operation"] == "ARM")
            and next(row["timestampMs"] for row in acknowledgements if row["operation"] == "HEAL") >= kill["jobManagerTimeAfterKill"],
            "ARM precedes baseline completion or HEAL precedes physical kill")
    previous_counts, previous_rows = fc, fr
    for stage in (stopped, healed):
        counts, rows = checkpoint_history(stage["checkpoints"], job)
        require(all(rows.get(key) == value for key, value in previous_rows.items())
                and counts["completed"] == fc["completed"] and counts["inProgress"] == 0
                and counts["total"] - previous_counts["total"] == counts["failed"] - previous_counts["failed"] == len(set(rows) - set(previous_rows))
                and previous_counts["restored"] <= counts["restored"] <= fc["restored"] + 1
                and stage["checkpoints"]["latestCompleted"] == br[baseline], "Post-kill boundary advanced or lost a failure")
        for key in set(rows) - set(previous_rows):
            require(key > failed_id and rows[key]["status"] == "FAILED"
                    and kill["jobManagerTimeBeforeKill"] <= rows[key]["triggerTimestamp"] <= rows[key]["failureTimestamp"],
                    "Unaccounted pre-kill checkpoint competitor")
        if counts["restored"] != fc["restored"]:
            restored = stage["checkpoints"]["latestRestored"]
            require(restored["id"] == baseline and restored["restoreTimestamp"] >= kill["jobManagerTimeBeforeKill"], "Early restoration used another checkpoint")
        previous_counts, previous_rows = counts, rows
    require(rc["restored"] == bc["restored"] + 1 and recovered["checkpoints"]["latestRestored"]["id"] == baseline
            and evidence["flinkJob"]["restoredCheckpoints"] == 1 and evidence["flinkJob"]["latestRestoredCheckpoint"] == baseline,
            "Recovery did not restore the exact baseline once")
    trigger = fault["triggerResult"]
    require(trigger["state"] == "COMPLETED" and trigger["checkpointId"] is None and isinstance(trigger["failure"], str)
            and len(trigger["failure"]) <= 2 * 1024 * 1024, "Failed trigger response is incomplete")
    stack = parse(trigger["failure"])["stack-trace"]
    require(isinstance(stack, str) and "Input/output error" in stack and "/chk-" + str(failed_id) + "/" in stack,
            "Trigger failure lacks a checkpoint-bound EIO cause")
    prefix = final["namespace"] + "/" + job + "/chk-" + str(failed_id) + "/"
    eio = [row for row in final_rows[old_item["instance"]["instanceId"]] if row["errno"] == 5]
    old_arm = next(row for ack, row in zip([*arm["acknowledgements"], *heal["acknowledgements"]], acknowledgements)
                   if ack["command"] == "ARM" and ack["instance"] == old_item["instance"])
    require(eio and all(row["state"] == "ARMED" and row["faultId"] == fault["triggerId"] and row["revision"] == 1
                       and row["path"].startswith(prefix) and row["operation"] in ("create", "write")
                       and row["faultOperation"] == row["operation"] and row["result"] == -1
                       and row["sequence"] > old_arm["sequence"]
                       and old_arm["timestampMs"] <= row["timestampMs"] <= kill["jobManagerTimeBeforeKill"] for row in eio), "Native EIO does not bind to the failed checkpoint on the original TaskManager")
    require(all(row["errno"] != 5 for row in final_rows[new_item["instance"]["instanceId"]]), "Replacement inherited EIO")
    main_markers = evidence["stateRecovery"]["trace"]["markers"]
    trace = fault["failureTrace"]
    require(trace["complete"] is True and trace["diagnostics"] == [] and all(row in main_markers for row in trace["markers"]), "Failure markers are not retained in the physical process trace")
    require(any(row["event"] == "SNAPSHOT" and row["checkpointId"] == failed_id and row["jobId"] == job
                and row["process"]["runtimeId"] == old["runtimeId"]
                and row["executionAttempt"] == fault["readiness"]["executionAttempt"] for row in trace["markers"]), "Failed checkpoint has no original-execution workload snapshot")
    require(fault["recovery"] == evidence["stateRecovery"]["recoveries"][0], "Atomic and terminal recovery proofs differ")
    recovery_trace = fault["recoveryTrace"]
    require(recovery_trace["complete"] is True and recovery_trace["diagnostics"] == []
            and all(row in main_markers for row in recovery_trace["markers"]), "Recovery checkpoint trace is incomplete")
    progress = fault["recovery"]["postRestoreProgress"]
    completion = [row for row in recovery_trace["markers"] if row["event"] == "COMPLETED"
                  and row["checkpointId"] > failed_id and row["process"] == progress["process"]
                  and row["executionAttempt"] == progress["executionAttempt"] and row["restoredCheckpointId"] == baseline
                  and row["jobId"] == job and row["lineNumber"] > progress["lineNumber"]]
    require(any(row["checkpointId"] in rr and rr[row["checkpointId"]]["status"] == "COMPLETED"
                and any(saved["event"] == "SNAPSHOT" and saved["checkpointId"] == row["checkpointId"]
                        and saved["process"] == row["process"] and saved["executionAttempt"] == row["executionAttempt"]
                        and saved["restoredCheckpointId"] == baseline and saved["jobId"] == job
                        and progress["lineNumber"] < saved["lineNumber"] < row["lineNumber"]
                        for saved in recovery_trace["markers"]) for row in completion),
            "No subsequent completed checkpoint after all-key recovery progress")
    original_before = next(item for item in before["storage"]["instances"] if item["instance"] == old_item["instance"])
    materialized = materialization(fault["materialization"], before["storage"], final, job, baseline, arm["startedNanos"], original_before, new_item)
    summary.update(stages=list(STAGES), baselineCheckpoint=baseline, failedCheckpoint=failed_id,
                   nativeEioOperations=len(eio), materialization=materialized)
    return summary


def cleanup_receipt(directory, result, endpoint=None):
    value = parse(read(directory, "cleanup.json"))
    evidence = result["evidence"]
    ids = {row["runtimeId"] for row in evidence["flinkRuntime"]["components"]}
    ids.update(row["containerId"] for row in evidence["kafka"]["containers"])
    require(value["version"] == 1 and value["complete"] is True and len(value["containers"]) == len(ids)
            and {row["runtimeId"] for row in value["containers"]} == ids, "Cleanup misses an exact observed container")
    queries = [(row, ["container", "ls", "-a", "--no-trunc", "--filter", "id=" + row["runtimeId"], "--format", "{{.ID}}"])
               for row in value["containers"]]
    storage = value["storage"]
    require(storage["storageId"] == evidence["checkpointStorage"]["storageId"], "Cleanup queried another storage volume")
    queries.append((storage, ["volume", "ls", "--filter", "label=org.savonitar.flink-stability.storage=" + storage["storageId"], "--format", "{{.Name}}"] ))
    for row, suffix in queries:
        receipt = row["receipt"]
        argv = receipt["argv"]
        require(row["absent"] is True and receipt["exitCode"] == 0 and receipt["stdout"].strip() == receipt["stderr"].strip() == ""
                and isinstance(argv, list) and argv[-len(suffix):] == suffix and "--host" in argv,
                "Cleanup absence is not supported by an exact successful query")
        host = argv[argv.index("--host") + 1]
        require(isinstance(host, str) and host.startswith("unix:///") and ".." not in Path(host[7:]).parts,
                "Cleanup did not use a local Unix Docker endpoint")
        if endpoint is not None:
            require(host == endpoint, "Cleanup queried a different Docker daemon")
    return value
