"""Generated-cache and launcher tests; no Maven or ambient configuration is read."""
import builtins
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).absolute().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "state_recovery_cache_hygiene_build", ROOT / "calibration/state-recovery/build.py")
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


class CacheHygieneTest(unittest.TestCase):
    def test_ignored_repository_metadata_is_never_opened(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source = root / "repository"
            artifacts = source / "g/a/1"
            artifacts.mkdir(parents=True)
            for name, data in {"a.jar": b"generated artifact", "a.pom": b"<project/>",
                               "a.jar.sha256": b"generated checksum"}.items():
                (artifacts / name).write_bytes(data)
            ignored = {artifacts / name for name in (
                "_remote.repositories", "resolver-status.properties", "settings.xml")}
            for path in ignored:
                path.write_bytes(b"generated metadata that must not be read")
            original_builtin_open = builtins.open
            original_path_open = Path.open

            def checked_builtin_open(path, *args, **kwargs):
                if isinstance(path, (str, os.PathLike)) and Path(path) in ignored:
                    self.fail("Ignored source metadata was opened: " + Path(path).name)
                return original_builtin_open(path, *args, **kwargs)

            def checked_path_open(path, *args, **kwargs):
                if path in ignored:
                    self.fail("Ignored source metadata was opened: " + path.name)
                return original_path_open(path, *args, **kwargs)

            destination = root / "isolated/repository"
            with patch.object(build.sys, "platform", "linux"), \
                    patch("builtins.open", side_effect=checked_builtin_open), \
                    patch.object(Path, "open", checked_path_open):
                receipt = build.seed_cache(source, destination)
            self.assertEqual(3, receipt["copied"])
            self.assertEqual(1, receipt["generatedRepositoryMarkers"])
            self.assertEqual(4, receipt["files"])
            marker = destination / "g/a/1/_remote.repositories"
            self.assertEqual(
                "# Generated for this isolated offline cache; source receipts are unchanged.\n"
                "a.jar>=\na.jar>central=\na.pom>=\na.pom>central=\n", marker.read_text())
            self.assertFalse((destination / "g/a/1/resolver-status.properties").exists())
            self.assertFalse((destination / "g/a/1/settings.xml").exists())
            for path in ignored:
                self.assertEqual(b"generated metadata that must not be read", path.read_bytes())

    def test_build_launch_uses_resolved_maven_and_isolated_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            script = root / "calibration/state-recovery/build.py"
            for relative in ("pom.xml", "flink-job-generator/pom.xml",
                             "flink-job-generator/WorkloadBuildGuard.java",
                             "calibration/state-recovery/build.py"):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("generated source\n")
            repository = root / "repository"
            repository.mkdir()
            (repository / "generated.jar").write_bytes(b"generated artifact")
            output = root / "jobs/fresh-build"
            java = root / "jdk"
            maven = root / "maven/bin/mvn"
            calls = []

            def execute(command, **kwargs):
                calls.append((list(command), dict(kwargs["env"]), kwargs))
                if len(calls) == 1:
                    return SimpleNamespace(stdout="javac 21.0.7\n")
                raise subprocess.CalledProcessError(17, command)

            arguments = [str(script), "--java-home", str(java), "--maven-repo", str(repository),
                         "--output", str(output)]
            with patch.object(build, "ROOT", root), patch.object(build, "__file__", str(script)), \
                    patch.object(build.shutil, "which", return_value=str(maven)) as which, \
                    patch.object(build.subprocess, "run", side_effect=execute), \
                    patch.object(build.sys, "argv", arguments), \
                    patch.object(build.sys, "platform", "linux"), \
                    patch.dict(os.environ, {"MAVEN_OPTS": "generated-unwanted-option",
                                            "MAVEN_ARGS": "generated-unwanted-arguments",
                                            "JAVA_TOOL_OPTIONS": "generated-unwanted-jvm-option"}):
                with self.assertRaises(subprocess.CalledProcessError):
                    build.main()
            which.assert_called_once_with("mvn")
            self.assertEqual(2, len(calls))
            command, environment, options = calls[1]
            self.assertEqual(str(maven), command[0])
            self.assertIn("-o", command)
            for option in ("-s", "-gs"):
                self.assertEqual(str(output / "settings.xml"), command[command.index(option) + 1])
            self.assertIn("-Dmaven.repo.local=" + str(output / "cache/repository"), command)
            self.assertEqual(str(output / "sources/correct/flink-job-generator/pom.xml"),
                             command[command.index("-f") + 1])
            self.assertEqual(str(output / "home"), environment["HOME"])
            self.assertEqual(str(output / "tmp"), environment["TMPDIR"])
            self.assertEqual("1", environment["MAVEN_SKIP_RC"])
            self.assertEqual("-Duser.home=" + str(output / "home")
                             + " -Djava.io.tmpdir=" + str(output / "tmp"), environment["MAVEN_OPTS"])
            self.assertNotIn("MAVEN_ARGS", environment)
            self.assertNotIn("JAVA_TOOL_OPTIONS", environment)
            self.assertEqual(root, options["cwd"])
            self.assertTrue(options["check"])
            self.assertEqual('<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"/>\n',
                             (output / "settings.xml").read_text())
            self.assertEqual({"version", "sourceSnapshots", "recipeInputSha256", "cache"},
                             set(json.loads((output / "source-manifest.json").read_text())))
            self.assertTrue((output / "correct-build.log").is_file())
            self.assertFalse((output / "build-evidence.json").exists())


if __name__ == "__main__":
    unittest.main()
