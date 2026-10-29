"""Generated freeze/supervisor receipts; these tests never start Docker or Maven."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

RECIPE = Path(__file__).absolute().parents[2] / "calibration/checkpoint-storage"


def import_recipe(name):
    spec = importlib.util.spec_from_file_location("checkpoint_storage_test_" + name, RECIPE / (name + ".py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


catalogs = import_recipe("catalogs")
runner = import_recipe("run")
IMAGE = "sha256:" + "d" * 64
IDENTITY = {"flinkImageId": IMAGE, "flinkImageReference": "flink:2.2.0",
            "kafkaImageId": "sha256:" + "e" * 64, "kafkaImageReference": "apache/kafka:4.0.0"}


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = value if isinstance(value, bytes) else catalogs.encoded(value)
    path.write_bytes(data)
    return catalogs.digest(data)


class FrozenFixture:
    def __init__(self, root):
        self.root = root
        self.recipe = root / "calibration/checkpoint-storage"
        self.parent = root / "jobs/pr4"
        self.output = root / "jobs/qualification"
        self.helper_source = root / "jobs/helper-build"
        self.references = {}
        for name in ("catalogs.py", "run.py", "check.py", "audit.py", "raw_evidence.py", "component_events.py", "inventory.py", "report.py", "framework.py", "prepare.py", "ResolveClosure.java", "README.md"):
            put(self.recipe / name, ("# generated fixture " + name + "\n").encode())
        put(root / "calibration/state-recovery/check.py", b"# independent reference fixture\n")
        artifacts, snapshots = {}, {}
        for mode in ("correct", "mutant"):
            name = "artifacts/" + mode + ".jar"
            artifacts[mode] = {"path": name, "sha256": put(self.parent / name, ("workload-" + mode).encode())}
            source = root / "jobs/build/sources" / mode
            source_hash = put(source / "job/Main.java", b"class Main {}\n")
            snapshots[mode] = {"path": str(source), "sha256": {"job/Main.java": source_hash}}
        helper_files = {name: put(self.parent / "artifacts/helper/bundle" / name, name.encode())
                        for name in ("checkpoint-fuse", "checkpoint-fuse-selftest", "probe.jar")}
        source_hash = put(self.helper_source / "source/core.c", b"/* generated native fixture */\n")
        helper_build = {"status": "built", "imageId": IMAGE, "platform": "linux/arm64", "files": helper_files,
                        "sourceHashes": {"native/core.c": source_hash}}
        helper_hash = put(self.parent / "artifacts/helper/build-evidence.json", helper_build)
        build = {"sourceSnapshots": snapshots, "artifacts": artifacts}
        build_hash = put(self.parent / "evidence/build.json", build)
        self.manifest = {"kind": "checkpoint-storage-integration-pilots", "artifacts": artifacts,
                         "helperBuild": {"path": "artifacts/helper", "source": str(self.helper_source),
                             "manifestSha256": helper_hash, "imageId": IMAGE, "platform": "linux/arm64", "files": helper_files},
                         "workloadBuild": {"path": "evidence/build.json", "sha256": build_hash}, "runtimeIdentity": IDENTITY}
        put(self.parent / "manifest.json", self.manifest)
        for name, mode, execution in catalogs.CELLS:
            outcome = "fail" if mode == "mutant" else "pass"
            result = {"status": outcome, "attempt": {"status": outcome}, "expectation": {"outcome": "pass"},
                      "identity": IDENTITY, "evidence": {
                          "stateRecovery": {"workloadSha256": artifacts[mode]["sha256"], "confirmed": True,
                                            "allowStateResetMutant": mode == "mutant",
                                            "buildMode": "reset-state-on-restore-v1" if mode == "mutant" else "correct"},
                          "checkpointStorage": {"helperManifestSha256": helper_hash},
                          "taskManagerRestarts": [] if execution == "healthy" else [{}],
                          "checkpointStorageFaults": [{}] if execution == "eio" else [],
                          "terminalValidation": {"reason": "validator.kafka.state-prefix." + ("state-mismatch" if mode == "mutant" else "match")}}}
            path = root / "jobs/pilots" / (name + ".json")
            put(path, result)
            self.references[name] = path
        self.framework = SimpleNamespace(freeze_framework=self.freeze_framework)

    def freeze_framework(self, destination, **unused):
        destination.mkdir()
        put(destination / "manifest.json", {"formatVersion": 1, "test": "frozen framework", "files": {}})

    def freeze(self, platform_evidence=None):
        def modules(name, unused):
            return SimpleNamespace(runtime_identity=lambda result: result["identity"]) if name == "state_reference" else self.framework
        with patch.object(catalogs, "module", side_effect=modules):
            return catalogs.freeze(self.parent / "manifest.json", self.references, self.output, {}, root=self.root,
                                   recipe=self.recipe, platform_evidence=platform_evidence)


class CheckpointStorageCatalogTest(unittest.TestCase):
    def test_freezes_exact_four_by_three_matrix_with_pass_expectations_and_copied_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = FrozenFixture(Path(temporary).resolve())
            manifest = fixture.freeze()
            self.assertEqual([name for name, _, _ in catalogs.CELLS], [value["name"] for value in manifest["cells"]])
            self.assertEqual(12, sum(len(value["attempts"]) for value in manifest["cells"]))
            self.assertEqual(manifest, catalogs.validate_frozen(fixture.output))
            self.assertEqual(900, manifest["outerTimeoutSeconds"])
            for cell in manifest["cells"]:
                scenario = (fixture.output / cell["catalog"] / (catalogs.SCENARIO + ".yaml")).read_text()
                self.assertIn("total: 30000", scenario)
                self.assertIn("completion_timeout: 5m", scenario)
                self.assertIn("tolerable_failures: 10", scenario)
                self.assertIn('bundle: "./artifacts/helper"', scenario)
                self.assertEqual(cell["execution"] == "eio", "checkpoint_storage_fault:" in scenario)
                self.assertEqual(cell["execution"] == "restart", "      - kill:" in scenario)
                self.assertEqual("pass", cell["requiredOutcome"])
                self.assertEqual("allow_state_reset_mutant: true" in scenario, cell["mode"] == "mutant")
            self.assertEqual("fail", manifest["cells"][-1]["expectedRawOutcome"])
            self.assertTrue((fixture.output / "evidence/build-sources/mutant/job/Main.java").is_file())
            self.assertTrue((fixture.output / "evidence/helper-sources/native/core.c").is_file())
            self.assertTrue((fixture.output / "evidence/recipe/state_recovery_check.py").is_file())
            event_parser = "evidence/recipe/component_events.py"
            self.assertEqual((fixture.recipe / "component_events.py").read_bytes(), (fixture.output / event_parser).read_bytes())
            self.assertEqual(manifest["frozenFiles"][event_parser], manifest["recipeFiles"][event_parser])
            self.assertEqual(manifest["frozenFiles"]["evidence/recipe/raw_evidence.py"], manifest["recipeFiles"]["evidence/recipe/raw_evidence.py"])
            self.assertEqual({name: "jobs/pilots/" + name + ".json" for name, _, _ in catalogs.CELLS}, manifest["referenceSources"])
            self.assertNotIn("platformEvidence", manifest)
            (fixture.parent / "artifacts/correct.jar").write_bytes(b"changed after freeze")
            catalogs.validate_frozen(fixture.output)
            with self.assertRaisesRegex(ValueError, "exists"):
                fixture.freeze()

    def test_optional_platform_receipt_is_copied_pinned_and_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = FrozenFixture(Path(temporary).resolve())
            platform = fixture.root / "jobs/preparation/platform.json"
            value = {"docker": {"serverVersion": "29.1.3", "architecture": "aarch64"}, "hostSystem": "Darwin", "pythonVersion": "3.13.1"}
            expected = put(platform, value)
            manifest = fixture.freeze(platform)
            self.assertEqual({"path": "evidence/platform.json", "sha256": expected}, manifest["platformEvidence"])
            self.assertEqual(value, json.loads((fixture.output / "evidence/platform.json").read_text()))
            catalogs.validate_frozen(fixture.output)
            (fixture.output / "evidence/platform.json").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "Hash mismatch"):
                catalogs.validate_frozen(fixture.output)

    def test_freeze_requires_separate_restart_pilot_and_matching_workload(self):
        for change in ("missing", "restart", "artifact", "state", "expected"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                fixture = FrozenFixture(Path(temporary).resolve())
                if change == "missing":
                    fixture.references.pop("correct-restart")
                else:
                    path = fixture.references["correct-restart"]
                    value = json.loads(path.read_text())
                    if change == "restart": value["evidence"]["taskManagerRestarts"] = []
                    if change == "artifact": value["evidence"]["stateRecovery"]["workloadSha256"] = "0" * 64
                    if change == "state": value["evidence"]["stateRecovery"]["confirmed"] = False
                    if change == "expected": value["expectation"]["outcome"] = "fail"
                    put(path, value)
                with self.assertRaises(ValueError):
                    fixture.freeze()
                self.assertFalse(fixture.output.exists())

    def test_hash_change_symlink_and_wrong_helper_platform_reject_before_freeze(self):
        for change in ("hash", "symlink", "platform"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                fixture = FrozenFixture(Path(temporary).resolve())
                if change == "hash":
                    (fixture.helper_source / "source/core.c").write_bytes(b"changed")
                elif change == "symlink":
                    path = fixture.parent / "artifacts/correct.jar"
                    path.unlink()
                    path.symlink_to(fixture.parent / "artifacts/mutant.jar")
                else:
                    fixture.manifest["helperBuild"]["platform"] = "linux/amd64"
                    put(fixture.parent / "manifest.json", fixture.manifest)
                with self.assertRaises(ValueError): fixture.freeze()

    def test_runner_validation_rejects_changed_recipe_catalog_artifact_or_allocations(self):
        for change in ("recipe", "component-parser", "raw-parser", "catalog", "artifact", "attempt", "expectation", "recipe-omitted", "component-parser-omitted", "raw-parser-omitted"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                fixture = FrozenFixture(Path(temporary).resolve()); manifest = fixture.freeze()
                paths = {"recipe": "evidence/recipe/framework.py", "component-parser": "evidence/recipe/component_events.py",
                         "raw-parser": "evidence/recipe/raw_evidence.py",
                         "catalog": "catalogs/correct-eio/" + catalogs.SCENARIO + ".yaml",
                         "artifact": "artifacts/correct.jar"}
                if change in paths:
                    (fixture.output / paths[change]).write_bytes(b"changed")
                else:
                    if change == "attempt": manifest["cells"][0]["attempts"].append("attempts/correct-healthy/run-4")
                    if change == "expectation": manifest["cells"][-1]["requiredOutcome"] = "fail"
                    if change == "recipe-omitted": manifest["recipeFiles"].pop("evidence/recipe/framework.py")
                    if change == "component-parser-omitted": manifest["recipeFiles"].pop("evidence/recipe/component_events.py")
                    if change == "raw-parser-omitted": manifest["recipeFiles"].pop("evidence/recipe/raw_evidence.py")
                    put(fixture.output / "manifest.json", manifest)
                with self.assertRaises(ValueError): catalogs.validate_frozen(fixture.output)

    def test_guards_path_escape_and_duplicate_json_without_following_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            for value in (root / ".." / "escape", root / "Documents/private", root / "Photos/private"):
                with self.assertRaises(ValueError): catalogs.guarded(value, root)
            (root / "link").symlink_to(root / "target")
            with self.assertRaises(ValueError): catalogs.guarded(root / "link/file", root)
            with self.assertRaises(ValueError): catalogs.decode(b'{"version":1,"version":2}')

    def test_frozen_inventory_rejects_unlisted_files_and_bytecode_everywhere(self):
        additions = ("evidence/recipe/x.pyc", "evidence/extra.json", "catalogs/correct-eio/extra.yaml",
                     "framework/extra.jar", "unlisted-report.json", "evidence/recipe/__pycache__/x.pyc",
                     "attempts/correct-eio/run-1/__pycache__/x.pyc", "validation/correct-eio/__pycache__/x.pyc")
        for extra in additions:
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as temporary:
                fixture = FrozenFixture(Path(temporary).resolve())
                fixture.freeze()
                put(fixture.output / extra, b"unexpected generated fixture\n")
                with self.assertRaisesRegex(ValueError, "frozen batch"):
                    catalogs.validate_frozen(fixture.output)

    def test_frozen_inventory_allows_only_named_runner_receipts_and_mutable_trees(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = FrozenFixture(Path(temporary).resolve())
            fixture.freeze()
            for name in ("execution-started.json", "execution-finished.json", "validation-started.json",
                         "validation-finished.json", "attempts/correct-eio/run-1/stdout.json",
                         "validation/correct-eio/stdout.json"):
                put(fixture.output / name, {})
            catalogs.validate_frozen(fixture.output)
            (fixture.output / "evidence/unlisted-empty-directory").mkdir()
            with self.assertRaisesRegex(ValueError, "Unexpected directory"):
                catalogs.validate_frozen(fixture.output)

    def test_frozen_inventory_rejects_link_in_mutable_output_without_following_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = FrozenFixture(Path(temporary).resolve())
            fixture.freeze()
            (fixture.output / "attempts").mkdir()
            (fixture.output / "attempts/linked").symlink_to(fixture.root / "not-created")
            with self.assertRaisesRegex(ValueError, "Symbolic link"):
                catalogs.validate_frozen(fixture.output)


def result_fixture(directory):
    checkpoint = directory / "checkpoints/attempt-1-generated"
    log = checkpoint / "flink-stability-output-taskmanager-1-1.log"
    put(log, b"STATE_PREFIX_START generated fixture\n")
    journal = checkpoint / "checkpoint-storage-journals/generated.jsonl"
    journal_hash = put(journal, b'{"sequence":1}\n')
    result = {"attempt": {"checkpointRoot": str(checkpoint)}, "evidence": {
        "input": {"complete": True, "records": 30000, "observed": 30000},
        "flinkRuntime": {"config": {"execution.checkpointing.data-inline-threshold": "0 bytes"},
                         "components": [{"runtimeId": "a" * 64, "effectiveConfiguration": {"path": "/opt/flink/conf/config.yaml"}}]},
        "kafka": {"containers": [{"containerId": "b" * 64}]},
        "checkpointStorage": {"storageId": "storage-" + "12345678-1234-1234-1234-123456789abc",
                              "instances": [{"journalPath": str(journal), "journalSha256": journal_hash}]}}}
    put(directory / "stdout.json", result)
    return result


class CheckpointStorageRunnerTest(unittest.TestCase):
    def test_minimal_environment_never_inherits_ambient_java_docker_or_home_settings(self):
        directory = Path("/generated/attempt")
        with patch.dict(os.environ, {"JAVA_TOOL_OPTIONS": "-Dunexpected=true", "DOCKER_CONTEXT": "remote", "HOME": "/ambient"}):
            environment = runner.minimal_environment(directory, "unix:///generated/docker.sock", "1.44")
        self.assertNotIn("JAVA_TOOL_OPTIONS", environment)
        self.assertNotIn("DOCKER_CONTEXT", environment)
        self.assertEqual(str(directory / "home"), environment["HOME"])
        self.assertEqual(str(directory / "home/.docker"), environment["DOCKER_CONFIG"])
        self.assertEqual("unix:///generated/docker.sock", environment["DOCKER_HOST"])
        for invalid in ("tcp://example:2375", "unix://remote/socket", "unix:///tmp/socket?remote=1", "unix:///tmp/../private.sock"):
            with self.assertRaises(ValueError): runner.local_endpoint(invalid)

    def test_process_failure_is_retained_and_not_retried(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            process = Mock(pid=1234); process.wait.return_value = 1
            popen = Mock(return_value=process)
            observed = runner.execute(["/java", "Main"], directory, {"HOME": str(directory)}, 900, popen=popen)
            self.assertEqual("exited", observed["termination"])
            self.assertEqual(1, observed["exitCode"])
            self.assertTrue(observed["completed"])
            popen.assert_called_once()
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            self.assertEqual(subprocess.DEVNULL, popen.call_args.kwargs["stdin"])
            self.assertEqual("1\n", (directory / "exit-code.txt").read_text())

    def test_outer_timeout_terminates_exact_process_group_and_keeps_noncompletion(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(runner.os, "killpg") as kill:
            directory = Path(temporary).resolve()
            process = Mock(pid=1234)
            process.wait.side_effect = [subprocess.TimeoutExpired("java", 900), subprocess.TimeoutExpired("java", 15), -9]
            observed = runner.execute(["/java"], directory, {}, 900, popen=Mock(return_value=process))
            self.assertEqual("timeout", observed["termination"])
            self.assertFalse(observed["completed"])
            self.assertEqual(-9, observed["exitCode"])
            self.assertEqual([(1234, signal.SIGTERM), (1234, signal.SIGKILL)], [call.args for call in kill.call_args_list])
            self.assertTrue((directory / "stdout.json").is_file())
            self.assertTrue((directory / "stderr.log").is_file())

    def test_spawn_failure_and_unconfirmed_kill_are_retained(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            observed = runner.execute(["/missing/java"], directory, {}, 900, popen=Mock(side_effect=OSError("missing executable")))
            self.assertEqual("spawn-error", observed["termination"])
            self.assertFalse(observed["started"])
            self.assertIn("missing executable", (directory / "stderr.log").read_text())
        with tempfile.TemporaryDirectory() as temporary, patch.object(runner.os, "killpg"):
            process = Mock(pid=1234); process.wait.side_effect = subprocess.TimeoutExpired("java", 5)
            observed = runner.execute(["/java"], Path(temporary).resolve(), {}, 900, popen=Mock(return_value=process))
            self.assertEqual("termination-unconfirmed", observed["termination"])
            self.assertIsNone(observed["exitCode"])

    def test_retains_exact_logs_journals_configuration_and_full_declared_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(); directory = root / "jobs/batch/attempts/correct-eio/run-1"
            result_fixture(directory)
            runner.retain_evidence(root, directory)
            retained = json.loads((directory / "retained-evidence.json").read_text())
            self.assertEqual({"flink-log", "native-journal"}, {value["kind"] for value in retained["files"]})
            for value in retained["files"]:
                self.assertEqual(value["sha256"], catalogs.digest((directory / value["localPath"]).read_bytes()))
            manifest = (directory / "input-manifest.txt").read_text().splitlines()
            self.assertEqual([str(value) for value in range(30000)], manifest)
            self.assertEqual("reconstructed-declared-integer-sequence", json.loads((directory / "input-manifest.json").read_text())["kind"])
            self.assertTrue((directory / "configuration-evidence.json").is_file())
            self.assertTrue((directory / "logs/flink-stability-output-taskmanager-1-1.log").is_file())

    def test_retention_rejects_foreign_path_symlink_and_hash_disagreement(self):
        for change in ("foreign", "symlink", "hash"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(); directory = root / "jobs/batch/attempts/correct-eio/run-1"
                result = result_fixture(directory)
                row = result["evidence"]["checkpointStorage"]["instances"][0]
                if change == "foreign": row["journalPath"] = str(root / "outside.jsonl")
                elif change == "hash": row["journalSha256"] = "0" * 64
                else:
                    journal = Path(row["journalPath"]); journal.unlink(); journal.symlink_to(root / "outside.jsonl")
                put(directory / "stdout.json", result)
                with self.assertRaises(ValueError): runner.retain_evidence(root, directory)

    def test_cleanup_checks_exact_ids_and_label_without_mutations(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve(); result = result_fixture(directory)
            invoke = Mock(return_value=SimpleNamespace(returncode=0, stdout="", stderr=""))
            value = runner.cleanup_evidence(result, directory, "/docker", "unix:///tmp/docker.sock", {}, invoke=invoke)
            self.assertTrue(value["complete"])
            self.assertEqual({"a" * 64, "b" * 64}, {row["runtimeId"] for row in value["containers"]})
            commands = [call.args[0] for call in invoke.call_args_list]
            self.assertEqual(3, len(commands))
            self.assertIn("id=" + "a" * 64, commands[0])
            self.assertIn("label=org.savonitar.flink-stability.storage=" + value["storage"]["storageId"], commands[-1])
            self.assertTrue(all("rm" not in command and "kill" not in command for command in commands))

    def test_present_resource_failed_docker_or_nonempty_stderr_never_confirms_cleanup(self):
        for observed in (SimpleNamespace(returncode=0, stdout="container-still-present\n", stderr=""),
                         SimpleNamespace(returncode=1, stdout="", stderr="unavailable"),
                         SimpleNamespace(returncode=0, stdout="", stderr="warning")):
            with self.subTest(observed=observed), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve(); result = result_fixture(directory)
                value = runner.cleanup_evidence(result, directory, "/docker", "unix:///tmp/docker.sock", {},
                                                invoke=Mock(return_value=observed))
                self.assertFalse(value["complete"])

    def test_serial_matrix_keeps_ordinary_failures_but_stops_on_unconfirmed_cleanup_or_timeout(self):
        for stop in (None, "cleanup", "timeout"):
            with self.subTest(stop=stop), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(); fixture = FrozenFixture(root); manifest = fixture.freeze()
                docker = root / "docker"; docker.write_text("generated test executable"); docker.chmod(0o755)
                framework = SimpleNamespace(validate_framework=Mock(), command=Mock(side_effect=lambda unused, args, *a, **kw: ["/java", "Main", *args]))
                calls = []
                def execute(argv, directory, environment, timeout):
                    if "validate" in argv:
                        put(directory / "stdout.json", b"valid generated plan\n")
                        put(directory / "stderr.log", b"")
                        put(directory / "exit-code.txt", b"0\n")
                        return {"termination": "exited", "exitCode": 0, "started": True, "completed": True, "timeoutSeconds": timeout}
                    calls.append(directory)
                    result_fixture(directory)
                    put(directory / "stderr.log", b"ordinary failed attempt retained\n")
                    put(directory / "exit-code.txt", b"1\n")
                    return {"termination": "timeout" if stop == "timeout" else "exited", "exitCode": 1,
                            "started": True, "completed": stop != "timeout", "timeoutSeconds": timeout}
                def cleanup(unused, directory, *args):
                    value = {"complete": stop != "cleanup"}; runner.write_json(directory / "cleanup.json", value); return value
                with patch.object(runner.catalogs, "module", return_value=framework), patch.object(runner, "execute", side_effect=execute), \
                        patch.object(runner, "cleanup_evidence", side_effect=cleanup):
                    completed = runner.run(fixture.output, "unix:///tmp/docker.sock", docker, root=root)
                    self.assertEqual(stop is None, completed)
                    self.assertEqual(12 if stop is None else 1, len(calls))
                    self.assertTrue(json.loads((fixture.output / "validation-finished.json").read_text())["complete"])
                    with self.assertRaisesRegex(ValueError, "already started"):
                        runner.run(fixture.output, "unix:///tmp/docker.sock", docker, root=root)
                self.assertEqual(calls, sorted(calls, key=lambda path: [str(fixture.output / item)
                    for cell in manifest["cells"] for item in cell["attempts"]].index(str(path))))
                for directory in calls:
                    self.assertTrue((directory / "execution.json").is_file())
                    self.assertTrue((directory / "command.json").is_file())
                    self.assertEqual({"auths": {}}, json.loads((directory / "home/.docker/config.json").read_text()))

    def test_failed_preflight_keeps_receipts_and_never_allocates_an_attempt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(); fixture = FrozenFixture(root); fixture.freeze()
            docker = root / "docker"; docker.write_text("generated executable"); docker.chmod(0o755)
            framework = SimpleNamespace(validate_framework=Mock(), command=Mock(return_value=["/java", "Main", "validate"]))
            def execute(unused, directory, environment, timeout):
                put(directory / "stdout.json", b""); put(directory / "stderr.log", b"invalid frozen plan\n")
                put(directory / "exit-code.txt", b"1\n")
                return {"termination": "exited", "exitCode": 1, "started": True, "completed": True, "timeoutSeconds": timeout}
            with patch.object(runner.catalogs, "module", return_value=framework), patch.object(runner, "execute", side_effect=execute) as launch:
                self.assertFalse(runner.run(fixture.output, "unix:///tmp/docker.sock", docker, root=root))
                self.assertEqual(1, launch.call_count)
                self.assertFalse((fixture.output / "execution-started.json").exists())
                self.assertFalse((fixture.output / "attempts").exists())
                self.assertFalse(json.loads((fixture.output / "validation-finished.json").read_text())["complete"])
                with self.assertRaisesRegex(ValueError, "Validation already started"):
                    runner.run(fixture.output, "unix:///tmp/docker.sock", docker, root=root)


if __name__ == "__main__":
    unittest.main()
