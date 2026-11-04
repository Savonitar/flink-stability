"""Mutant build isolation and packaged-byte contracts, using generated local fixtures only."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
from zipfile import ZipFile

ROOT = Path(__file__).absolute().parents[2]
SPEC = importlib.util.spec_from_file_location("state_recovery_build_contract", ROOT / "calibration/state-recovery/build.py")
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


class MutantBuildGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        home = os.environ.get("JAVA_HOME")
        cls.java = str(Path(home) / "bin/java") if home else shutil.which("java")
        if not cls.java or subprocess.run([cls.java, "-version"], capture_output=True).returncode:
            raise unittest.SkipTest("A local JDK is required for the workload source guard tests")

    def guard(self, target, mode):
        return subprocess.run([self.java, "-Dworkload.build.mode=" + mode,
                               str(ROOT / "flink-job-generator/WorkloadBuildGuard.java"), str(target),
                               "2.2.0", "org.apache.flink", "5.0.0-2.2", "11", "4.0.0", mode],
                              env={"PATH": os.defpath}, capture_output=True, text=True, timeout=30)

    def test_mode_property_alone_cannot_create_mutant_in_normal_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary).resolve() / "target"
            result = self.guard(target, BUILD.MODES["mutant"])
            self.assertNotEqual(0, result.returncode)
            self.assertIn("workload.build.mode alone is forbidden", result.stderr)
            self.assertFalse(target.exists(), "Rejected configuration must not create build output or markers")

    def test_only_the_final_directory_component_grants_mutant_isolation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            for relative in ("state-reset-on-restore/target", "state-reset-on-restore-copy", "mutant"):
                target = root / relative
                with self.subTest(relative=relative):
                    self.assertNotEqual(0, self.guard(target, BUILD.MODES["mutant"]).returncode)
                    self.assertFalse(target.exists())
            isolated = root / "target/state-reset-on-restore"
            result = self.guard(isolated, BUILD.MODES["mutant"])
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual("reset-state-on-restore-v1\n", (isolated / "workload-build.mode").read_text())
            self.assertEqual(0, self.guard(isolated, BUILD.MODES["mutant"]).returncode)

    def test_correct_build_stays_in_normal_target_and_pom_passes_effective_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary).resolve() / "target"
            result = self.guard(target, "correct")
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual("correct\n", (target / "workload-build.mode").read_text())
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        pom = ET.parse(ROOT / "flink-job-generator/pom.xml")
        arguments = pom.findall(".//m:execution[m:id='require-clean-workload-version-change']/m:configuration/m:arguments/m:argument", ns)
        self.assertEqual("${project.build.directory}", arguments[1].text)
        self.assertEqual("${workload.build.mode}", arguments[-1].text)


class PackagedArmByteIdentityTest(unittest.TestCase):
    def pair(self, root):
        paths = []
        for arm, mode in BUILD.MODES.items():
            path = root / (arm + ".jar")
            manifest = ("Manifest-Version: 1.0\r\nFlink-Stability-Workload-Protocol: v1\r\n"
                        "Flink-Stability-Workload-Capabilities: keyed-prefix-v1\r\n"
                        "Flink-Stability-Workload-Build-Mode: " + mode + "\r\n\r\n").encode()
            with ZipFile(path, "w") as jar:
                jar.writestr("META-INF/MANIFEST.MF", manifest)
                jar.writestr(BUILD.IDENTITY, "mode=" + mode + "\n")
                jar.writestr("example/Generated.class", b"\xca\xfe\xba\xbe\0\0\0\x37generated-equal-bytecode")
            paths.append(path)
        return paths

    def test_pair_inspector_opens_both_jars_and_only_allows_the_two_identity_entries(self):
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.pair(Path(temporary))
            self.assertEqual(sorted(["META-INF/MANIFEST.MF", BUILD.IDENTITY]), BUILD.inspect_pair(*paths))

    def test_changed_class_bytes_or_missing_arm_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.pair(Path(temporary))
            entries = BUILD.jar_contents(paths[1])
            entries["example/Generated.class"] += b"changed"
            with ZipFile(paths[1], "w") as jar:
                for name, data in entries.items():
                    jar.writestr(name, data)
            with self.assertRaisesRegex(ValueError, "Unexpected arm differences"):
                BUILD.inspect_pair(*paths)
            paths[1].unlink()
            with self.assertRaises(FileNotFoundError):
                BUILD.inspect_pair(*paths)


if __name__ == "__main__":
    unittest.main()
