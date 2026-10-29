"""Complete generated attempt receipts exercise the real qualification gates together."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


def load(name):
    spec = importlib.util.spec_from_file_location("contract_happy_" + name, Path(__file__).with_name(name + ".py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


native = load("test_checkpoint_storage_calibration_check")
raw_fixture = load("checkpoint_storage_raw_fixture")
check = native.check


def put(path, value):
    data = value if isinstance(value, bytes) else (json.dumps(value, sort_keys=True) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return check.digest(data)


def complete_attempt(directory, mode="correct", execution="eio"):
    result, code, cell, manifest = native.fixture(directory, mode, execution)
    evidence = result["evidence"]
    names = {"jm-a": "jobmanager-1#1", "tm-a": "taskmanager-1#1", "tm-b": "taskmanager-1#2"}
    for component in evidence["flinkRuntime"]["components"]:
        component["classLoadProcess"] = names[component["runtimeId"]]
    evidence["subjectClasses"]["processes"].append({"process": "jobmanager-1#1", "sources": {}})
    native.base.write_marker_logs(directory, evidence["stateRecovery"]["trace"]["markers"])
    retained = json.loads((directory / "retained-evidence.json").read_bytes())
    for component in evidence["flinkRuntime"]["components"]:
        process = component["classLoadProcess"].replace("#", "-")
        class_path = "logs/flink-stability-class-load-" + process + ".log"
        classes = "".join("[class,load] " + entry + " source: file:" + evidence["subjectClasses"]["expectedSource"] + "\n"
                          for entry in check.state.ENTRY_CLASSES).encode()
        put(directory / class_path, classes)
        output_path = "logs/flink-stability-output-" + process + ".log"
        if not (directory / output_path).exists():
            put(directory / output_path, b"startup complete\n")
        for relative in (class_path, output_path):
            retained["files"].append(dict(sourcePath="/generated/" + relative, localPath=relative,
                sha256=check.digest((directory / relative).read_bytes()), kind="log"))
    put(directory / "retained-evidence.json", retained)
    total = manifest["workload"]["totalRecords"]
    sequence = b"".join((str(value) + "\n").encode() for value in range(total))
    put(directory / "input-manifest.json", dict(version=1, kind="reconstructed-declared-integer-sequence",
        firstId=0, endExclusive=total, records=total, inputReceiptComplete=True, path="input-manifest.txt",
        sha256=put(directory / "input-manifest.txt", sequence)))
    put(directory / "input-evidence.json", dict(kind="derived-cli-result-projection",
        source="stdout.json#/evidence/input", input=evidence["input"]))
    runtime = evidence["flinkRuntime"]
    put(directory / "configuration-evidence.json", dict(kind="derived-cli-result-projection",
        source="stdout.json#/evidence/flinkRuntime", config=runtime["config"], components=[
        dict(runtimeId=value["runtimeId"], effectiveConfiguration=value["effectiveConfiguration"])
        for value in runtime["components"]]))
    endpoint = "unix:///generated/docker.sock"
    put(directory / "command.json", dict(environment=dict(DOCKER_HOST=endpoint)))

    def receipt(args):
        return dict(argv=["docker", "--host", endpoint, *args], exitCode=0, stdout="", stderr="")

    ids = [value["runtimeId"] for value in runtime["components"]]
    ids += [value["containerId"] for value in evidence["kafka"]["containers"]]
    storage_id = evidence["checkpointStorage"]["storageId"]
    put(directory / "cleanup.json", dict(version=1, complete=True, containers=[dict(runtimeId=value, absent=True,
        receipt=receipt(["container", "ls", "-a", "--no-trunc", "--filter", "id=" + value, "--format", "{{.ID}}"]))
        for value in ids], storage=dict(storageId=storage_id, absent=True, receipt=receipt([
            "volume", "ls", "--filter", "label=org.savonitar.flink-stability.storage=" + storage_id,
            "--format", "{{.Name}}"]))))
    for fault in evidence.get("checkpointStorageFaults", []):
        fault["declaredControls"] = dict(smallFileThreshold=0, maxConcurrent=1, tolerableFailures=10)
    raw_fixture.attach_raw_fixture(result, directory)
    return result, code, cell, manifest


class CompleteAttemptContractTest(unittest.TestCase):
    def test_real_contract_accepts_all_four_cells_without_mocking_a_gate(self):
        for mode, execution in (("correct", "healthy"), ("correct", "restart"), ("correct", "eio"), ("mutant", "eio")):
            with self.subTest(mode=mode, execution=execution), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                result, code, cell, manifest = complete_attempt(directory, mode, execution)
                retained = check.audit.RetainedEvidence(directory)
                subject = check.raw_subject_and_events(directory, result, retained)
                row = check.audit_attempt_contract(result, code, cell, manifest, directory, retained, subject)
                self.assertTrue(row["confirmed"])
                self.assertTrue(row["checkpointStorage"]["complete"])
                self.assertTrue(row["cleanup"]["complete"])
                self.assertTrue(row["componentEventAudit"]["confirmed"])
                self.assertTrue(row["rawEvidenceAudit"]["confirmed"])
                self.assertGreater(row["rawEvidenceAudit"]["captureCount"], 0)
                self.assertFalse(row["derivedCliProjections"]["independentProof"])
                self.assertTrue(all(not value["legacyRepresentation"] for value in row["derivedCliProjections"]["files"]))
                self.assertEqual(6 if execution == "eio" else 0,
                                 row["rawEvidenceAudit"]["typedComparisons"]["checkpointHistories"])
                self.assertEqual("fail" if mode == "mutant" else "pass", row["rawStatus"])
                self.assertEqual(6000, row["inputManifest"]["records"])
                self.assertEqual(row, check.audit_attempt(result, code, cell, manifest, directory))

    def test_exact_source_only_legacy_copies_remain_derived_and_cannot_replace_raw_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, code, cell, manifest = complete_attempt(directory, execution="healthy")
            for name in ("input-evidence.json", "configuration-evidence.json"):
                value = json.loads((directory / name).read_bytes()); value.pop("kind")
                put(directory / name, value)
            row = check.audit_attempt(result, code, cell, manifest, directory)
            projections = row["derivedCliProjections"]
            self.assertFalse(projections["independentProof"])
            self.assertTrue(all(value["legacyRepresentation"] for value in projections["files"]))
            self.assertEqual(["stdout.json#/evidence/input", "stdout.json#/evidence/flinkRuntime"],
                             [value["source"] for value in projections["files"]])
            for projection in projections["files"]:
                self.assertEqual(check.digest((directory / projection["path"]).read_bytes()), projection["sha256"])
            result["evidence"]["flinkJob"]["observation"].pop("rawEvidence")
            with self.assertRaisesRegex(ValueError, "missing/bounded REST"):
                check.audit_attempt(result, code, cell, manifest, directory)

    def test_projection_source_or_kind_cannot_claim_independent_evidence(self):
        for name in ("input-evidence.json", "configuration-evidence.json"):
            for field, value in (("kind", "raw-response"), ("kind", None), ("source", "raw/container.json"), ("source", None)):
                with self.subTest(name=name, field=field, value=value), tempfile.TemporaryDirectory() as temporary:
                    directory = Path(temporary).resolve()
                    result, code, cell, manifest = complete_attempt(directory, execution="healthy")
                    projection = json.loads((directory / name).read_bytes())
                    if value is None:
                        if field == "source": projection.pop(field)
                        else: projection[field] = None
                    else: projection[field] = value
                    put(directory / name, projection)
                    with self.assertRaisesRegex(ValueError, "Derived CLI projection"):
                        check.audit_attempt(result, code, cell, manifest, directory)

    def test_rehashed_raw_job_contradiction_is_rejected_by_the_real_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, code, cell, manifest = complete_attempt(directory, execution="healthy")
            reference = result["evidence"]["flinkJob"]["observation"]["rawEvidence"][0]
            index = json.loads((directory / "retained-evidence.json").read_bytes())
            receipt = next(row for row in index["files"] if row["sourcePath"] == reference["path"])
            path = directory / receipt["localPath"]
            body = json.loads(path.read_bytes())
            body["state"] = "RUNNING"
            digest = put(path, body)
            reference.update(sha256=digest, bytes=path.stat().st_size)
            receipt["sha256"] = digest
            put(directory / "retained-evidence.json", index)
            with self.assertRaisesRegex(ValueError, "job observation differs from source bytes"):
                check.audit_attempt(result, code, cell, manifest, directory)


if __name__ == "__main__":
    unittest.main()
