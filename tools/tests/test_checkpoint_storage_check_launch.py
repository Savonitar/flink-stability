"""Independent qualification launch gates; generated receipts, no Java/Maven/Docker."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


HERE = Path(__file__).absolute().parents[2] / "calibration/checkpoint-storage"


def load(name):
    spec = importlib.util.spec_from_file_location("checkpoint_launch_test_" + name, HERE / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check, catalogs = load("check"), load("catalogs")
SETTINGS = b'<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"/>\n'
DOCKER = b'{"auths":{}}\n'
ENDPOINT = "unix:///tmp/generated-docker.sock"


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True).encode()
    path.write_bytes(data)
    return check.digest(data)


def environment(directory):
    return {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(directory / "home"),
            "TMPDIR": str(directory / "tmp"), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC",
            "DOCKER_CONFIG": str(directory / "home/.docker"), "DOCKER_HOST": ENDPOINT, "DOCKER_API_VERSION": "1.44"}


def frozen_command(root, arguments, api_version, *, work_directory):
    return ["/generated-java", "-Dapi.version=" + api_version, "-cp", str(root / "frozen.jar"),
            "generated.Main", *arguments]


def fixture(root):
    manifest = {"frameworkFreeze": {"path": "framework/manifest.json", "sha256": "f" * 64},
                "cells": [{"name": name, "mode": mode, "execution": execution, "catalog": "catalogs/" + name}
                          for name, mode, execution in check.CELLS]}
    manifest_hash = put(root / "manifest.json", manifest)
    put(root / "execution-started.json", {"dockerEndpoint": ENDPOINT, "dockerApiVersion": "1.44"})
    put(root / "validation-started.json", {"formatVersion": 1, "manifestSha256": manifest_hash,
                                          "frameworkManifestSha256": "f" * 64})
    put(root / "validation-finished.json", {"formatVersion": 1, "complete": True,
                                           "validated": [cell["name"] for cell in manifest["cells"]]})
    for number, cell in enumerate(manifest["cells"]):
        directory = root / "validation" / cell["name"]
        settings_hash = put(directory / "settings.xml", SETTINGS)
        docker_hash = put(directory / "home/.docker/config.json", DOCKER)
        arguments = ["validate", "--catalog-root", str(root / cell["catalog"]), "--scenario", check.SCENARIO,
                     "--artifact-root", str(root), "--offline", "--show-plan"]
        command = {"argv": frozen_command(root / "framework", arguments, "1.44", work_directory=directory),
                   "cwd": str(directory), "environment": environment(directory),
                   "frameworkManifestSha256": "f" * 64, "settingsSha256": settings_hash, "dockerConfigSha256": docker_hash}
        command_hash = put(directory / "command.json", command)
        put(directory / "execution.json", {"formatVersion": 1, "started": True, "completed": True,
            "termination": "exited", "exitCode": 0, "timeoutSeconds": 120, "elapsedSeconds": 1.0,
            "startedAt": f"2026-01-01T00:00:{number * 2:02d}+00:00",
            "finishedAt": f"2026-01-01T00:00:{number * 2 + 1:02d}+00:00",
            "frameworkManifestSha256": "f" * 64, "commandSha256": command_hash})
        put(directory / "exit-code.txt", b"0\n")
        put(directory / "stdout.json", b'{"generated": "resolved plan fixture"}\n')
        put(directory / "stderr.log", b"")
    return manifest


class CatalogValidationReceiptTest(unittest.TestCase):
    def verify(self, root, manifest):
        with patch.object(check.framework, "command", side_effect=frozen_command):
            return check.validate_catalog_receipts(root, manifest)

    def test_all_four_real_validation_receipts_are_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(); manifest = fixture(root)
            receipts = self.verify(root, manifest)
            self.assertEqual([cell["name"] for cell in manifest["cells"]], [row["cell"] for row in receipts])
            self.assertTrue(all(row["exitCode"] == 0 and len(row["stdoutSha256"]) == 64 for row in receipts))

    def test_missing_failed_timed_out_reordered_or_repeated_validation_is_rejected(self):
        for change in ("missing-file", "failed", "timeout", "not-started", "wrong-code-file", "wrong-framework",
                       "command-hash", "deadline", "reordered", "extra", "wrong-manifest", "empty-plan", "missing-stderr"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(); manifest = fixture(root)
                directory = root / "validation/correct-healthy"
                execution = json.loads((directory / "execution.json").read_bytes())
                if change == "missing-file": (directory / "execution.json").unlink()
                elif change == "wrong-code-file": put(directory / "exit-code.txt", b"1\n")
                elif change == "extra": (root / "validation/retry").mkdir()
                elif change == "empty-plan": put(directory / "stdout.json", b"\n")
                elif change == "missing-stderr": (directory / "stderr.log").unlink()
                elif change == "reordered":
                    put(root / "validation-finished.json", {"formatVersion": 1, "complete": True,
                        "validated": [cell["name"] for cell in reversed(manifest["cells"])]})
                elif change == "wrong-manifest":
                    put(root / "validation-started.json", {"formatVersion": 1, "manifestSha256": "0" * 64,
                                                          "frameworkManifestSha256": "f" * 64})
                else:
                    field, value = {"failed": ("exitCode", 1), "timeout": ("termination", "timeout"),
                        "not-started": ("started", False), "wrong-framework": ("frameworkManifestSha256", "0" * 64),
                        "command-hash": ("commandSha256", "0" * 64), "deadline": ("elapsedSeconds", 121)}[change]
                    execution[field] = value; put(directory / "execution.json", execution)
                with self.assertRaises(ValueError): self.verify(root, manifest)

    def test_rehashed_command_cannot_change_catalog_framework_or_offline_mode(self):
        for change in ("catalog", "classpath", "offline", "show-plan", "endpoint"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(); manifest = fixture(root)
                directory = root / "validation/correct-healthy"
                command = json.loads((directory / "command.json").read_bytes())
                if change == "catalog": command["argv"][command["argv"].index("--catalog-root") + 1] += "-different"
                elif change == "classpath": command["argv"][command["argv"].index("-cp") + 1] = "/different.jar"
                elif change == "endpoint": command["environment"]["DOCKER_HOST"] = "unix:///tmp/other.sock"
                else: command["argv"].remove("--" + change)
                execution = json.loads((directory / "execution.json").read_bytes())
                execution["commandSha256"] = put(directory / "command.json", command)
                put(directory / "execution.json", execution)
                with self.assertRaises(ValueError): self.verify(root, manifest)


class LaunchConfigurationTest(unittest.TestCase):
    def test_environment_injection_and_remote_or_malformed_endpoint_are_rejected(self):
        for name, value in (("JAVA_TOOL_OPTIONS", "-javaagent:/generated-agent.jar"),
                            ("JDK_JAVA_OPTIONS", "-cp /generated.jar"), ("MAVEN_OPTS", "-Dextra=true"),
                            ("PATH", "/extra/bin"), ("DOCKER_HOST", "tcp://localhost:2375"),
                            ("DOCKER_HOST", "unix://remote/tmp/docker.sock"), ("DOCKER_HOST", "unix:///tmp/../docker.sock"),
                            ("DOCKER_HOST", "unix:///tmp/docker.sock?extra=1"),
                            ("DOCKER_HOST", "unix:///tmp/docker.sock#fragment"), ("DOCKER_API_VERSION", "1.44 -Dextra=true")):
            with self.subTest(name=name, value=value), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                put(directory / "settings.xml", SETTINGS); put(directory / "home/.docker/config.json", DOCKER)
                command = {"environment": environment(directory)}
                check.launch_configuration(directory, command)
                command["environment"][name] = value
                with self.assertRaises(ValueError): check.launch_configuration(directory, command)

    def test_rehashed_nonempty_maven_or_docker_settings_are_rejected(self):
        for filename, content in (("settings.xml", b"<settings><mirrors/></settings>\n"),
                                   ("home/.docker/config.json", b'{"auths":{},"credsStore":"generated"}\n')):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                put(directory / "settings.xml", SETTINGS); put(directory / "home/.docker/config.json", DOCKER)
                command = {"environment": environment(directory)}
                put(directory / filename, content)
                command["settingsSha256"] = check.digest((directory / "settings.xml").read_bytes())
                command["dockerConfigSha256"] = check.digest((directory / "home/.docker/config.json").read_bytes())
                with self.assertRaises(ValueError): check.launch_configuration(directory, command)


class IndependentScenarioContractTest(unittest.TestCase):
    def test_all_fixed_cells_and_rehashed_semantic_changes(self):
        manifest = {"runtimeIdentity": {"flinkImageReference": "flink:2.2.0", "flinkImageId": "sha256:" + "a" * 64,
                       "kafkaImageReference": "apache/kafka:4.0.0", "kafkaImageId": "sha256:" + "b" * 64},
                    "helperBundle": {"manifestSha256": "c" * 64, "imageId": "sha256:" + "a" * 64, "platform": "linux/arm64"},
                    "artifacts": {mode: {"path": "artifacts/" + mode + ".jar"} for mode in ("correct", "mutant")}}
        changes = ((b"interval: 10s", b"interval: 1s"), (b"total: 30000", b"total: 30001"),
                   (b"key_count: 8", b"key_count: 9"), (b"tolerable_failures: 10", b"tolerable_failures: 11"),
                   (b"state_ttl: { enabled: false }", b"state_ttl: { enabled: true }"),
                   (b"small_file_threshold: 0", b"small_file_threshold: 20000"),
                   (b"max_concurrent: 1", b"max_concurrent: 2"), (b"completion_timeout: 5m", b"completion_timeout: 6m"),
                   (b"mode: EXACTLY_ONCE", b"mode: AT_LEAST_ONCE"),
                   (b"timeout: 90s, lease: 120s", b"timeout: 89s, lease: 120s"))
        for name, mode, execution in check.CELLS:
            cell = {"name": name, "mode": mode, "execution": execution}
            data = catalogs.render("./" + manifest["artifacts"][mode]["path"], "./artifacts/helper",
                                   manifest["helperBundle"], manifest["runtimeIdentity"], execution, mode=mode)
            check.scenario_contract(data, cell, manifest)
            for before, after in changes:
                if before not in data: continue
                with self.subTest(cell=name, change=before):
                    changed = data.replace(before, after)
                    rehashed = dict(cell, scenarioSha256=check.digest(changed))
                    with self.assertRaises(ValueError): check.scenario_contract(changed, rehashed, copy.deepcopy(manifest))


class FixedMatrixAllocationTest(unittest.TestCase):
    def generate(self, root, change=None):
        cells = [{"name": name, "mode": mode, "execution": execution, "catalog": "catalogs/" + name,
                  "attempts": [f"attempts/{name}/run-{number}" for number in range(1, 4)]}
                 for name, mode, execution in check.CELLS]
        manifest = {"cells": cells, "frameworkFreeze": {"sha256": "f" * 64}, "references": {},
                    "outerTimeoutSeconds": 900}
        for number, cell in enumerate(cells):
            name = "references/" + cell["name"] + ".json"
            manifest["references"][cell["name"]] = {"path": name, "sha256": put(root / name,
                {"attempt": {"nonce": f"{number + 100:08x}"}})}
        manifest_hash = put(root / "manifest.json", manifest)
        allocated = [relative for cell in cells for relative in cell["attempts"]]
        put(root / "execution-started.json", {"attempts": 12, "automaticRetries": 0,
            "manifestSha256": manifest_hash, "frameworkManifestSha256": "f" * 64,
            "dockerEndpoint": ENDPOINT, "dockerApiVersion": "1.44"})
        put(root / "execution-finished.json", {"automaticRetries": 0, "complete": change != "incomplete",
            "stoppedAt": None, "attempted": list(reversed(allocated)) if change == "reordered" else allocated})
        for number, relative in enumerate(allocated):
            if change == "missing" and number == 11: continue
            directory = root / relative
            raw = {"status": "pass", "reason": "generated", "diagnostics": [],
                   "attempt": {"nonce": f"{1 if change == 'nonce' and number == 11 else number + 1:08x}",
                               "status": "pass", "reason": "generated"},
                   "evidence": {"componentErrors": {"eventCount": 0}, "flinkRest": {"errorCount": 0},
                       "stateRecovery": {"jobId": f"{1 if change == 'job' and number == 11 else number + 1:032x}"},
                       "checkpointStorage": {"storageId": "generated-" + str(number)},
                       "flinkRuntime": {"components": [{"runtimeId": f"{1 if change == 'runtime' and number == 11 else number + 1:064x}"}]}}}
            if change == "audit-failure" and relative == "attempts/correct-eio/run-2":
                raw["evidence"]["rawEvidence"] = {"captures": 4096, "bytes": 654321,
                    "maxCaptures": 4096, "maxBytes": 67108864, "complete": False}
            put(directory / "stdout.json", raw); put(directory / "stderr.log", b"")
            put(directory / "exit-code.txt", b"0\n")
            put(directory / "command.json", {"environment": {"DOCKER_HOST": ENDPOINT, "DOCKER_API_VERSION": "1.44"}})
            put(directory / "execution.json", {"startedAt": f"2026-01-01T00:01:{number * 2:02d}+00:00",
                "finishedAt": f"2026-01-01T00:01:{number * 2 + 1:02d}+00:00", "elapsedSeconds": 1.0})
        if change == "extra": (root / "attempts/correct-healthy/run-4").mkdir()
        return manifest

    def test_exact_twelve_attempts_qualify_and_any_allocation_gap_prevents_qualification(self):
        for change in (None, "missing", "extra", "incomplete", "reordered", "audit-failure", "nonce", "job", "runtime"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(); manifest = self.generate(root, change)
                def outcome(result, code, cell, frozen, directory):
                    if change == "audit-failure" and cell["name"] == "correct-eio" and directory.name == "run-2":
                        raise ValueError("Generated missing causal proof")
                    return {"confirmed": True}
                with patch.object(check, "validate_manifest", return_value=manifest), \
                        patch.object(check, "validate_catalog_receipts", return_value=[
                            {"cell": cell["name"], "startedAt": f"2026-01-01T00:00:{i * 2:02d}+00:00",
                             "finishedAt": f"2026-01-01T00:00:{i * 2 + 1:02d}+00:00", "elapsedSeconds": 1.0}
                            for i, cell in enumerate(manifest["cells"])]), \
                        patch.object(check, "execution_receipts", return_value={}), \
                        patch.object(check, "audit_attempt", side_effect=outcome):
                    report = check.check_matrix(root)
                self.assertEqual(12, len(report["attempts"]))
                self.assertEqual(change is None, report["qualified"])
                if change is None:
                    self.assertEqual({cell["name"]: 3 for cell in manifest["cells"]}, report["confirmedCounts"])
                    self.assertEqual(12, report["cleanupConfirmedCount"])
                    self.assertEqual([], report["errors"])
                else:
                    self.assertTrue(report["errors"])
                if change == "audit-failure":
                    failed = next(row for row in report["attempts"]
                                  if row["cell"] == "correct-eio" and row["number"] == 2)
                    self.assertFalse(failed["confirmed"])
                    self.assertEqual({"captures": 4096, "bytes": 654321, "maxCaptures": 4096,
                                      "maxBytes": 67108864, "complete": False}, failed["rawEvidence"])
                    self.assertNotIn("rawEvidenceAudit", failed)


if __name__ == "__main__":
    unittest.main()
