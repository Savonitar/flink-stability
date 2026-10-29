"""Source-format fixtures and adversarial mutations; no Docker or retained runs."""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); return module


raw = load("raw_evidence_test_subject", ROOT / "calibration/checkpoint-storage/raw_evidence.py")
audit = load("raw_evidence_test_retention", ROOT / "calibration/checkpoint-storage/audit.py")
native = load("raw_evidence_native_fixture", ROOT / "tools/tests/test_checkpoint_storage_calibration_check.py")
generated = load("raw_evidence_source_fixture", ROOT / "tools/tests/checkpoint_storage_raw_fixture.py")


class Retained:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.files = []
        self.root = "/generated/attempt-fixture"

    def put(self, body, endpoint, group="job-observation-1", kind="flink-rest-decoded-body", status=200):
        sequence = len(self.files) + 1
        folder = "raw-flink-rest" if kind == "flink-rest-decoded-body" else "raw-storage"
        relative = folder + f"/{sequence:06d}.json"
        path = self.directory / relative; path.parent.mkdir(exist_ok=True)
        path.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
        self.files.append(dict(sourcePath=self.root + "/" + relative, localPath=relative, sha256=digest, kind="raw-response"))
        (self.directory / "retained-evidence.json").write_text(json.dumps(dict(version=1, files=self.files)))
        return dict(path=self.root + "/" + relative, sha256=digest, bytes=len(body), kind=kind, sequence=sequence,
                    startedNanos=sequence * 10, completedNanos=sequence * 10 + 5, group=group, method="GET", endpoint=endpoint, status=status)

    def inspector(self):
        return raw.Inspector(audit.RetainedEvidence(self.directory), self.root)


class RawEvidenceTest(unittest.TestCase):
    def test_handwritten_job_sources_preserve_attempt_failures_and_exact_call_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            retained = Retained(Path(temporary).resolve())
            refs = [retained.put(body, endpoint) for endpoint, body in (
                ("/jobs/job", b'{ "now":42,"state":"RUNNING","vertices":[{"id":"v","name":"aggregate"}] }\n'),
                ("/jobs/job/checkpoints", b'{"counts":{"completed":3,"restored":1},"latest":{"restored":{"id":2,"restore_timestamp":30}}}'),
                ("/jobs/job/exceptions?maxExceptions=20", b'{"exceptionHistory":{"entries":[{"timestamp":31,"exceptionName":"RuntimeException","stacktrace":"outer\\nCaused by: IOException: EIO","taskManagerId":"tm"}]}}'),
                ("/jobs/job/vertices/v", b'{"subtasks":[{"subtask":0,"attempt":1,"status":"RUNNING","taskmanager-id":"tm"}]}'))]
            typed = dict(jobManagerTimeMillis=42, state="RUNNING", completedCheckpoints=3, restoredCheckpoints=1,
                         latestRestore=dict(checkpointId=2, restoredAtMillis=30),
                         failures=[dict(timestampMillis=31, exceptionName="RuntimeException", rootCause="IOException: EIO", taskManagerId="tm")],
                         subtasks=[dict(vertexName="aggregate", index=0, attempt=1, status="RUNNING", taskManagerId="tm")], rawEvidence=refs)
            retained.inspector().job(typed, "job")
            for change in (lambda value: value.update(completedCheckpoints=4),
                           lambda value: value["subtasks"][0].update(attempt=0),
                           lambda value: value.update(failures=[]),
                           lambda value: value["rawEvidence"].reverse(),
                           lambda value: value["rawEvidence"][1].update(group="job-observation-2")):
                modified = deepcopy(typed); change(modified)
                with self.assertRaises((ValueError, KeyError)):
                    retained.inspector().job(modified, "job")

    def test_non_json_get_retry_is_preserved_but_post_replay_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            retained = Retained(Path(temporary).resolve())
            retry = retained.put(b"temporary proxy failure\n", "/jobs/job", "job-manager-clock-1", status=503)
            success = retained.put(b'{"now":42}', "/jobs/job", "job-manager-clock-1")
            self.assertEqual(retained.inspector().one([retry, success], "job-manager-clock", "/jobs/job"), {"now": 42})
            retry["method"] = "POST"
            with self.assertRaisesRegex(ValueError, "retry"):
                retained.inspector().one([retry, success], "job-manager-clock", "/jobs/job")

    def test_docker_source_projection_cannot_be_replaced_by_typed_state_or_include_env(self):
        with tempfile.TemporaryDirectory() as temporary:
            retained = Retained(Path(temporary).resolve())
            ref = retained.put(b'{"Id":"owned","State":{ "Running":false,"Paused":false,"OOMKilled":false,"ExitCode":137,"FinishedAt":"done"}}',
                               "/v1.44/containers/owned/json", "docker-process-1", "docker-inspect-id-state-source-projection")
            typed = dict(runtimeId="owned", running=False, paused=False, oomKilled=False, exitCode=137, finishedAt="done", rawEvidence=ref)
            retained.inspector().process(typed)
            for key, value in (("running", True), ("exitCode", 0), ("runtimeId", "other")):
                changed = deepcopy(typed); changed[key] = value
                with self.assertRaises(ValueError):
                    retained.inspector().process(changed)
            source = retained.directory / retained.files[0]["localPath"]
            source.write_bytes(source.read_bytes().replace(b'"Id":', b'"Config":{"Env":[]},"Id":'))
            data = source.read_bytes(); digest = hashlib.sha256(data).hexdigest()
            retained.files[0]["sha256"] = digest; ref.update(sha256=digest, bytes=len(data))
            (retained.directory / "retained-evidence.json").write_text(json.dumps(dict(version=1, files=retained.files)))
            with self.assertRaisesRegex(ValueError, "projection"):
                retained.inspector().process(typed)

    def test_complete_generated_eio_proof_and_adversarial_missing_or_forged_counterparts(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, unused, cell, manifest = native.fixture(directory)
            generated.attach_raw_fixture(result, directory)
            proof = raw.audit(result, audit.RetainedEvidence(directory))
            self.assertTrue(proof["confirmed"])
            self.assertEqual(proof["typedComparisons"]["checkpointHistories"], 6)
            variants = {
                "missing source": lambda f: f["samples"][0]["checkpoints"].pop("rawEvidence"),
                "typed count": lambda f: f["samples"][2]["checkpoints"]["counts"].update(failed=99),
                "checkpoint ID": lambda f: f["failedCheckpoint"].update(id=999),
                "state handle hash": lambda f: f["materialization"]["files"][0].update(sha256="0" * 64),
                "clock": lambda f: f["kill"].update(jobManagerTimeAfterKill=99999),
                "stage reused": lambda f: f["samples"][1].update(checkpoints=deepcopy(f["samples"][0]["checkpoints"])),
                "group swapped": lambda f: f["samples"][2]["checkpoints"].update(rawEvidence=f["samples"][1]["checkpoints"]["rawEvidence"]),
                "hash": lambda f: f["triggerSubmitEvidence"][0].update(sha256="0" * 64),
                "bytes": lambda f: f["beforeKillEvidence"][0].update(bytes=999),
                "unknown boundary": lambda f: f["afterKillEvidence"][0].update(startedNanos=1000),
                "late failed details": lambda f: f["failedCheckpoint"]["rawEvidence"][0].update(startedNanos=9000, completedNanos=9005),
                "missing readiness checkpoint response": lambda f: f["readiness"].pop("checkpointEvidence"),
                "forged readiness acknowledgement": lambda f: next(iter(f["readiness"]["completedSnapshots"].values())).update(acknowledgedAtMillis=999),
            }
            for label, change in variants.items():
                with self.subTest(label=label):
                    changed = deepcopy(result); change(changed["evidence"]["checkpointStorageFaults"][0])
                    with self.assertRaises((ValueError, KeyError, TypeError)):
                        raw.audit(changed, audit.RetainedEvidence(directory))
            late = deepcopy(result)
            for reference in late["evidence"]["flinkJob"]["observation"]["rawEvidence"]:
                reference["startedNanos"] += 10000; reference["completedNanos"] += 10000
            with self.assertRaisesRegex(ValueError, "after physical fencing"):
                raw.audit(late, audit.RetainedEvidence(directory))
            late = deepcopy(result)
            def delay_readiness(value):
                if isinstance(value, dict):
                    if "jobObservation" in value and "completedSnapshots" in value:
                        for reference in value["jobObservation"]["rawEvidence"]:
                            reference["startedNanos"] += 10000; reference["completedNanos"] += 10000
                    for key, child in value.items():
                        if key not in {"rawEvidence", "operations", "markers"}:
                            delay_readiness(child)
                elif isinstance(value, list):
                    for child in value:
                        delay_readiness(child)
            delay_readiness(late["evidence"])
            with self.assertRaises(ValueError):
                raw.audit(late, audit.RetainedEvidence(directory))

    def test_missing_raw_evidence_does_not_upgrade_historical_derived_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            result, unused, cell, manifest = native.fixture(directory, execution="healthy")
            with self.assertRaises((ValueError, KeyError)):
                raw.audit(result, audit.RetainedEvidence(directory))


if __name__ == "__main__":
    unittest.main()
