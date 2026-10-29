"""Real generated cache, matrix and log fixtures; external process execution is faked."""
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).absolute().parents[2]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


build = module("state_tooling_build", ROOT / "calibration/state-recovery/build.py")
fixtures = module("state_tooling_fixtures", Path(__file__).with_name("test_state_recovery_calibration.py"))
with patch.dict(sys.modules, {"check": fixtures.check}):
    runner = module("state_tooling_run", ROOT / "calibration/state-recovery/run.py")


class CacheAndPathTest(unittest.TestCase):
    def test_cache_allowlist_excludes_settings_and_authentication_names(self):
        for name in ("artifact.jar", "artifact.pom.sha256", "maven-metadata-central.xml", "_remote.repositories"):
            self.assertTrue(build.cache_entry_allowed(name), name)
        for name in ("settings.xml", "api-key.pom", "credentials.jar", "id_rsa", "token.txt", "artifact.jar.lastUpdated"):
            self.assertFalse(build.cache_entry_allowed(name), name)

    def test_seed_cache_isolated_copy_skips_nonartifacts_and_hidden_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source = root / "repository"; source.mkdir()
            for name, content in {"g/a/1/a.jar": b"generated artifact", "g/a/1/a.pom": b"<project/>",
                                  "g/a/1/settings.xml": b"generated ignored config", "g/a/1/_remote.repositories": b"old-mirror-id", ".hidden/ignored.jar": b"ignored",
                                  "credentials/ignored.jar": b"generated ignored file"}.items():
                path = source / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(content)
            with patch.object(build.sys, "platform", "linux"):
                receipt = build.seed_cache(source, root / "clone")
            self.assertEqual(3, receipt["files"])
            self.assertEqual(1, receipt["generatedRepositoryMarkers"])
            copied = root / "clone/g/a/1/a.jar"
            marker = (root / "clone/g/a/1/_remote.repositories").read_text()
            self.assertIn("a.jar>central=", marker)
            self.assertNotIn("old-mirror-id", marker)
            copied.write_bytes(b"changed clone")
            self.assertEqual(b"generated artifact", (source / "g/a/1/a.jar").read_bytes())
            self.assertEqual({"g/a/1/a.jar", "g/a/1/a.pom", "g/a/1/_remote.repositories"},
                             {p.relative_to(root / "clone").as_posix() for p in (root / "clone").rglob("*") if p.is_file()})
            with self.assertRaises(FileExistsError):
                build.seed_cache(source, root / "clone")

    def test_cache_and_local_paths_reject_traversal_wrong_root_and_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            cache = root / "repository"; cache.mkdir()
            self.assertEqual(cache, build.artifact_cache_path(cache))
            self.assertEqual(cache / "missing.jar", build.local_path(cache / "missing.jar", root))
            for path in (root, root / "missing/repository", cache / "../repository"):
                with self.assertRaises(ValueError): build.artifact_cache_path(path)
            for path in (root / "../escape", root.parent / "outside"):
                with self.assertRaises(ValueError): build.local_path(path, root)
            (root / "linked").symlink_to(cache, target_is_directory=True)
            with self.assertRaises(ValueError): build.local_path(root / "linked/a.jar", root)
            (cache / "linked.jar").symlink_to(root / "nonexistent")
            with self.assertRaises(ValueError): build.seed_cache(cache, root / "rejected-clone")


class StateRunnerTest(unittest.TestCase):
    def prepare(self, root):
        batch = root / "jobs/matrix"
        manifest = fixtures.matrix_fixture(batch)
        # Only this generated fixture is cleared to exercise a first execution.
        shutil.rmtree(batch / "attempts")
        (batch / "execution-started.json").unlink()
        repository = root / "repository"; repository.mkdir()
        return batch, repository, manifest

    def test_real_manifest_runs_each_allocation_once_and_retains_raw_logs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            batch, repository, manifest = self.prepare(root)
            calls = []

            def execute(argv, *, cwd, stdout, stderr, check, **kwargs):
                calls.append((argv, cwd))
                checkpoint = root / "checkpoints" / str(len(calls)); checkpoint.mkdir(parents=True)
                (checkpoint / "flink-stability-output-taskmanager-1.log").write_bytes(b"generated raw output\r\n")
                (checkpoint / "flink-stability-class-load-taskmanager-1.log").write_bytes(b"generated class origin\n")
                (checkpoint / "unrelated.txt").write_bytes(b"not a selected log")
                json.dump({"attempt": {"checkpointRoot": str(checkpoint)}}, stdout)
                stderr.write("generated stderr\n")
                return SimpleNamespace(returncode=1 if len(calls) % 3 == 0 else 0)

            with patch.object(runner.subprocess, "run", side_effect=execute), redirect_stdout(io.StringIO()):
                runner.run(batch, repository, root=root, maven="generated-maven", docker_api_version="1.44")
                with self.assertRaisesRegex(ValueError, "already started"):
                    runner.run(batch, repository, root=root, maven="generated-maven")
            self.assertEqual(12, len(calls))
            self.assertEqual(12, json.loads((batch / "execution-started.json").read_text())["attempts"])
            allocated = [batch / name for cell in manifest["cells"] for name in cell["attempts"]]
            self.assertEqual(allocated, [directory for argv, directory in calls])
            for number, directory in enumerate(allocated, 1):
                self.assertEqual(str(1 if number % 3 == 0 else 0), (directory / "exit-code.txt").read_text().strip())
                self.assertEqual(b"generated raw output\r\n", (directory / "logs/flink-stability-output-taskmanager-1.log").read_bytes())
                self.assertEqual(2, len(list((directory / "logs").iterdir())))
                self.assertTrue((directory / "command.json").is_file())

    def test_failed_launch_and_retention_remain_visible_without_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            batch, repository, manifest = self.prepare(root)
            with patch.object(runner.subprocess, "run", side_effect=OSError("generated missing launcher")) as execute, redirect_stdout(io.StringIO()):
                runner.run(batch, repository, root=root, maven="generated-maven")
            self.assertEqual(12, execute.call_count)
            for cell in manifest["cells"]:
                for name in cell["attempts"]:
                    directory = batch / name
                    self.assertEqual("not-started\n", (directory / "exit-code.txt").read_text())
                    self.assertIn("generated missing launcher", (directory / "stderr.log").read_text())
                    self.assertTrue((directory / "log-retention-error.txt").is_file())


if __name__ == "__main__":
    unittest.main()
