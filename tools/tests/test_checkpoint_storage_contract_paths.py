"""Exercise qualification entry points with real generated files, without mocking the gates."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest


def load(name):
    spec = importlib.util.spec_from_file_location("contract_paths_" + name, Path(__file__).with_name(name + ".py"))
    value = importlib.util.module_from_spec(spec); spec.loader.exec_module(value); return value


freeze_fixture = load("test_checkpoint_storage_calibration_run")
framework_fixture = load("test_checkpoint_storage_framework")
native_fixture = load("test_checkpoint_storage_calibration_check")
build_fixture = load("test_state_recovery_build_contract")
launch_fixture = load("test_checkpoint_storage_check_launch")
check = native_fixture.check


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = value if isinstance(value, bytes) else (json.dumps(value, sort_keys=True) + "\n").encode()
    path.write_bytes(data)
    return check.digest(data)


class CompleteFrozenMatrix:
    def __init__(self, root, framework):
        fixture = freeze_fixture.FrozenFixture(root)
        for name in fixture.recipe.iterdir():
            name.write_bytes((freeze_fixture.RECIPE / name.name).read_bytes())
        (root / "calibration/state-recovery/check.py").write_bytes(check.STATE_SOURCE.read_bytes())
        arms = build_fixture.PackagedArmByteIdentityTest().pair(root)
        for mode, path in zip(("correct", "mutant"), arms):
            receipt = fixture.manifest["artifacts"][mode]
            receipt["sha256"] = put(fixture.parent / receipt["path"], path.read_bytes())
        build_path = fixture.parent / fixture.manifest["workloadBuild"]["path"]
        build = json.loads(build_path.read_bytes())
        build.update(version=1, mutation=check.state.MUTATION, releaseBytecodeTarget=11,
                     changedEntries=sorted(["META-INF/MANIFEST.MF", build_fixture.BUILD.IDENTITY]),
                     artifacts=fixture.manifest["artifacts"])
        fixture.manifest["workloadBuild"]["sha256"] = put(build_path, build)
        for number, (name, mode, execution) in enumerate(check.CELLS, 1):
            result = native_fixture.base.fixture(mode, "uninterrupted" if execution == "healthy" else "restart", f"{number:08x}")[0]
            result["identity"] = check.state.runtime_identity(result)
            result["evidence"]["stateRecovery"]["workloadSha256"] = fixture.manifest["artifacts"][mode]["sha256"]
            result["evidence"]["checkpointStorage"] = {"helperManifestSha256": "temporary"}
            result["evidence"]["checkpointStorageFaults"] = [{}] if execution == "eio" else []
            fixture.manifest["runtimeIdentity"] = result["identity"]
            put(fixture.references[name], result)
        helper_path = fixture.parent / "artifacts/helper/build-evidence.json"
        helper = json.loads(helper_path.read_bytes())
        helper["imageId"] = fixture.manifest["runtimeIdentity"]["flinkImageId"]
        fixture.manifest["helperBuild"].update(imageId=helper["imageId"], manifestSha256=put(helper_path, helper))
        for path in fixture.references.values():
            result = json.loads(path.read_bytes())
            result["evidence"]["checkpointStorage"]["helperManifestSha256"] = fixture.manifest["helperBuild"]["manifestSha256"]
            put(path, result)
        put(fixture.parent / "manifest.json", fixture.manifest)
        framework.destination = fixture.output / "framework"
        fixture.framework = SimpleNamespace(freeze_framework=lambda destination, **unused: framework.freeze())
        self.manifest = fixture.freeze()
        self.batch = fixture.output


class ContractEntryPointsTest(unittest.TestCase):
    def setUp(self):
        self.framework = framework_fixture.FrameworkFreezeTest()
        self.framework.setUp()
        self.addCleanup(self.framework.tearDown)

    def test_validate_manifest_accepts_complete_real_fixture_and_rejects_tampering(self):
        case = CompleteFrozenMatrix(self.framework.root, self.framework)
        self.assertEqual(case.manifest, check.validate_manifest(case.batch))
        helper = case.batch / "artifacts/helper/bundle/checkpoint-fuse"
        helper.write_bytes(b"changed generated helper")
        with self.assertRaisesRegex(ValueError, "[Hh]ash"):
            check.validate_manifest(case.batch)

    def execution(self):
        case = CompleteFrozenMatrix(self.framework.root, self.framework)
        cell = case.manifest["cells"][0]
        directory = case.batch / cell["attempts"][0]; directory.mkdir(parents=True)
        settings_sha = put(directory / "settings.xml", launch_fixture.SETTINGS)
        docker_sha = put(directory / "home/.docker/config.json", launch_fixture.DOCKER)
        environment = launch_fixture.environment(directory)
        arguments = ["run", "--catalog-root", str(case.batch / cell["catalog"]), "--scenario", check.SCENARIO,
                     "--artifact-root", str(case.batch), "--offline", "--kafka-log-output", str(directory / "kafka-logs")]
        command = dict(argv=check.framework.command(case.batch / "framework", arguments, "1.44", work_directory=directory),
                       cwd=str(directory), environment=environment, settingsSha256=settings_sha, dockerConfigSha256=docker_sha,
                       frameworkManifestSha256=case.manifest["frameworkFreeze"]["sha256"])
        execution = dict(formatVersion=1, started=True, completed=True, termination="exited", exitCode=0,
                         retentionComplete=True, retentionErrors=[], timeoutSeconds=case.manifest["outerTimeoutSeconds"],
                         frameworkManifestSha256=case.manifest["frameworkFreeze"]["sha256"],
                         commandSha256=put(directory / "command.json", command), elapsedSeconds=1.0,
                         startedAt="2026-01-01T00:00:10+00:00", finishedAt="2026-01-01T00:00:11+00:00")
        put(directory / "execution.json", execution)
        return case, directory, cell, execution

    def test_execution_receipts_use_real_frozen_command_and_empty_configuration(self):
        case, directory, cell, execution = self.execution()
        self.assertEqual(execution, check.execution_receipts(case.batch, directory, cell, case.manifest, 0))
        for field, value in (("retentionComplete", False), ("retentionErrors", ["missing capture"]),
                             ("commandSha256", "0" * 64), ("termination", "timeout"), ("elapsedSeconds", 0)):
            with self.subTest(field=field):
                put(directory / "execution.json", dict(execution, **{field: value}))
                with self.assertRaises(ValueError):
                    check.execution_receipts(case.batch, directory, cell, case.manifest, 0)
        put(directory / "execution.json", execution)
        put(directory / "home/.docker/config.json", b'{"auths":{},"extra":true}\n')
        with self.assertRaises(ValueError):
            check.execution_receipts(case.batch, directory, cell, case.manifest, 0)


if __name__ == "__main__":
    unittest.main()
