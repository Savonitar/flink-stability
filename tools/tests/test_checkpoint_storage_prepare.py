import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from zipfile import ZipFile

ROOT = Path(__file__).absolute().parents[2]
SPEC = importlib.util.spec_from_file_location("checkpoint_storage_prepare_test", ROOT / "calibration/checkpoint-storage/prepare.py")
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)


class PreparationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root / "jobs").mkdir()
        self.repository = self.root / "repository"
        self.jar = self.repository / "org/example/fixture/1/fixture-1.jar"
        self.jar.parent.mkdir(parents=True)
        with ZipFile(self.jar, "w") as archive:
            archive.writestr("fixture.txt", "fixture")
        self.java = self.root / "jdk"
        (self.java / "bin").mkdir(parents=True)
        for executable in (self.java / "bin/java", self.java / "bin/javac", self.root / "mvn"):
            executable.write_text("fixture")
            executable.chmod(0o700)
        self.commands = []

    def invoke(self, argv, **kwargs):
        self.commands.append((argv, kwargs))
        if argv[-1] == "-version":
            kwargs["stderr"].write(b'openjdk version "21.0.7"\n')
        elif any(arg.startswith("-Dmdep.outputFile=") for arg in argv):
            output = next(arg.split("=", 1)[1] for arg in argv if arg.startswith("-Dmdep.outputFile="))
            Path(output).write_text(str(self.jar))
        elif "org.savonitar.flink.stability.core.artifact.ResolveClosure" in argv:
            Path(argv[-1]).write_text(str(self.jar) + "\n")
        return subprocess.CompletedProcess(argv, 0)

    def prepare(self, invoke=None):
        return PREPARE.prepare(self.root / "jobs/preparation", self.repository, self.java,
                               self.root / "mvn", root=self.root, invoke=invoke or self.invoke)

    def test_exact_offline_commands_empty_settings_and_selected_closure(self):
        result = self.prepare()
        self.assertEqual(result["artifact_files"], [str(self.jar)])
        self.assertEqual(len(self.commands), 4)
        maven = self.commands[1][0]
        self.assertIn("-o", maven)
        self.assertEqual(maven[maven.index("-s") + 1], maven[maven.index("-gs") + 1])
        for _, call in self.commands:
            self.assertEqual(call["timeout"], 120)
            self.assertNotIn("JAVA_TOOL_OPTIONS", call["env"])
            self.assertEqual(call["env"]["HOME"], str(self.root / "jobs/preparation/home"))
            self.assertIn("-Duser.home=" + str(self.root / "jobs/preparation/home"), call["env"]["MAVEN_OPTS"])
        self.assertTrue((self.root / "jobs/preparation/preparation.json").is_file())
        with self.assertRaisesRegex(ValueError, "fresh"):
            self.prepare()

    def test_failed_discovery_retains_receipt_and_does_not_continue(self):
        def fail(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 7)
        with self.assertRaisesRegex(ValueError, "java-version"):
            self.prepare(fail)
        directory = self.root / "jobs/preparation"
        self.assertTrue((directory / "java-version.execution.json").is_file())
        self.assertFalse((directory / "framework-inputs.json").exists())

    def test_classpath_outside_selected_cache_rejected_before_compilation(self):
        def escape(argv, **kwargs):
            result = self.invoke(argv, **kwargs)
            for arg in argv:
                if arg.startswith("-Dmdep.outputFile="):
                    Path(arg.split("=", 1)[1]).write_text(str(self.root / "elsewhere.jar"))
            return result
        with self.assertRaisesRegex(ValueError, "escapes"):
            self.prepare(escape)
        self.assertEqual(len(self.commands), 2)

    def test_timeout_is_retained_and_never_retried(self):
        calls = []
        def timeout(argv, **kwargs):
            calls.append(argv)
            raise subprocess.TimeoutExpired(argv, 120)
        with self.assertRaisesRegex(ValueError, "Preparation failed"):
            self.prepare(timeout)
        self.assertEqual(len(calls), 1)

    def test_manifest_classpath_rejected_before_helper_execution(self):
        with ZipFile(self.jar, "a") as archive:
            archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\nClass-Path: hidden.jar\n")
        with self.assertRaisesRegex(ValueError, "Class-Path"):
            self.prepare()
        self.assertEqual(len(self.commands), 2)


if __name__ == "__main__":
    unittest.main()
