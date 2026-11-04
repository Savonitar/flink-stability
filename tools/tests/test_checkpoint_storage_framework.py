import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("checkpoint_storage_framework", ROOT / "calibration/checkpoint-storage/framework.py")
framework = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(framework)


class FrameworkFreezeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp" if Path("/private/tmp").is_dir() else None)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "source"
        self.repository = self.root / "repository"
        self.source.mkdir()
        self.repository.mkdir()
        (self.source / "pom.xml").write_text("<project/>\n")
        self.java = self.root / "java"
        self.java.write_bytes(b"test Java launcher bytes, never executed")
        self.classpath = self.root / "classpath.txt"
        self.module_installed = []
        for module in framework.MODULES:
            name = f"{module}-{framework.VERSION}.jar"
            base = self.source / module
            (base / "target/classes/example").mkdir(parents=True)
            (base / "src/main/java/example").mkdir(parents=True)
            (base / "pom.xml").write_text("<project/>\n")
            (base / "src/main/java/example/Main.java").write_text("class Main {}\n")
            payload = ("class bytes " + module).encode()
            (base / "target/classes/example/Main.class").write_bytes(payload)
            target = base / "target" / name
            self.write_jar(target, {"example/Main.class": payload})
            installed = self.repository / "org/savonitar/flink/stability" / module / framework.VERSION / name
            installed.parent.mkdir(parents=True)
            shutil.copyfile(target, installed)
            self.module_installed.append(installed)
        self.dependency = self.repository / "example/library/1.0/library-1.0.jar"
        self.dependency.parent.mkdir(parents=True)
        self.write_jar(self.dependency, {"example/Dependency.class": b"dependency"})
        self.pom = self.dependency.with_suffix(".pom")
        self.pom.write_text("<project/>\n")
        self.classpath.write_text(os.pathsep.join(map(str, [*self.module_installed, self.dependency])) + "\n")
        self.destination = self.root / "framework"

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def write_jar(path, entries, manifest="Manifest-Version: 1.0\n\n"):
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("META-INF/MANIFEST.MF", manifest)
            for name, payload in entries.items():
                archive.writestr(name, payload)

    def freeze(self, **options):
        values = dict(repository=self.repository, classpath_file=self.classpath,
                      artifact_files=[self.dependency, self.pom], java=self.java, source_root=self.source)
        values.update(options)
        return framework.freeze_framework(self.destination, **values)

    def tamper(self, path, data):
        path.chmod(0o644)
        path.write_bytes(data)

    def test_complete_frozen_subject_is_independent_of_mutable_build_and_cache(self):
        manifest = self.freeze()
        self.assertEqual(list(framework.MODULES), [item["module"] for item in manifest["moduleJars"]])
        self.assertEqual(6, len(manifest["targetClassesSha256"]))
        self.assertEqual(self.classpath.read_bytes(), (self.destination / "inputs/runtime-classpath.txt").read_bytes())
        for path in self.module_installed:
            path.write_bytes(b"subsequent unrelated build")
        self.dependency.write_bytes(b"changed shared cache")
        (self.source / "core/src/main/java/example/Main.java").write_text("changed after freeze")
        self.assertEqual(manifest, framework.validate_framework(self.destination))
        attempt = self.root / "attempt"
        attempt.mkdir()
        argv = framework.command(self.destination, ["run", "--offline", "--scenario", "generated"], "1.44", work_directory=attempt)
        self.assertEqual(str(self.java), argv[0])
        self.assertIn("-Dmaven.repo.local=" + str(self.destination / "repository"), argv)
        self.assertIn("-Duser.home=" + str(attempt / "home"), argv)
        self.assertIn("-Dmaven.multiModuleProjectDirectory=" + str(attempt), argv)
        self.assertIn("-Dapi.version=1.44", argv)
        cp = argv[argv.index("-cp") + 1].split(os.pathsep)
        self.assertEqual([str(self.destination / path) for path in manifest["runtimeClasspath"]], cp)
        self.assertTrue(all(Path(path).is_relative_to(self.destination) for path in cp))
        self.assertNotIn("target/classes", os.pathsep.join(cp))
        self.assertEqual(framework.SETTINGS, (self.destination / "settings.xml").read_bytes())
        self.assertFalse(any("settings.xml" in path for path in manifest["artifactClosureSha256"]))

    def test_installed_byte_mismatch_and_stale_target_classes_rejected(self):
        self.module_installed[0].write_bytes(b"different installed jar")
        with self.assertRaisesRegex(ValueError, "Installed framework differs"):
            self.freeze()
        shutil.rmtree(self.destination)
        shutil.copyfile(self.source / "runtime-api/target/runtime-api-0.1.0-SNAPSHOT.jar", self.module_installed[0])
        (self.source / "core/target/classes/example/Main.class").write_bytes(b"unpackaged class")
        with self.assertRaisesRegex(ValueError, "target/classes"):
            self.freeze()

    def test_modified_frozen_source_jar_or_closure_rejected(self):
        self.freeze()
        paths = [self.destination / "sources/core/src/main/java/example/Main.java",
                 self.destination / "jars/core-0.1.0-SNAPSHOT.jar",
                 self.destination / "repository/example/library/1.0/library-1.0.pom"]
        for path in paths:
            original = path.read_bytes()
            self.tamper(path, b"changed")
            with self.assertRaisesRegex(ValueError, "file changed"):
                framework.validate_framework(self.destination)
            self.tamper(path, original)
        (self.destination / "unlisted.txt").write_text("unlisted")
        with self.assertRaisesRegex(ValueError, "inventory changed"):
            framework.validate_framework(self.destination)

    def test_jar_and_class_map_tampering_is_not_hidden_by_new_file_hash(self):
        manifest = self.freeze()
        name = "jars/core-0.1.0-SNAPSHOT.jar"
        path = self.destination / name
        path.chmod(0o644)
        self.write_jar(path, {"example/Main.class": b"different"})
        digest = framework.sha(path.read_bytes())
        manifest["files"][name] = {"sha256": digest, "bytes": path.stat().st_size}
        record = next(value for value in manifest["moduleJars"] if value["module"] == "core")
        record.update(sha256=digest, targetSha256=digest, installedSha256=digest)
        self.tamper(self.destination / "manifest.json", json.dumps(manifest).encode())
        with self.assertRaisesRegex(ValueError, "class/resource"):
            framework.validate_framework(self.destination)

    def test_symlink_source_cache_and_frozen_entries_rejected(self):
        source = self.source / "core/src/main/java/example/Main.java"
        source.unlink()
        source.symlink_to(self.java)
        with self.assertRaisesRegex(ValueError, "Symbolic-link"):
            self.freeze()
        shutil.rmtree(self.destination)
        source.unlink()
        source.write_text("class Main {}")
        self.dependency.unlink()
        self.dependency.symlink_to(self.java)
        with self.assertRaisesRegex(ValueError, "Symbolic-link"):
            self.freeze()
        self.dependency.unlink()
        self.write_jar(self.dependency, {"example/Dependency.class": b"dependency"})
        self.freeze()
        frozen = self.destination / "sources/pom.xml"
        frozen.unlink()
        frozen.symlink_to(self.source / "pom.xml")
        with self.assertRaisesRegex(ValueError, "Symbolic-link"):
            framework.validate_framework(self.destination)

    def test_traversal_unlisted_classpath_and_nonartifact_cache_inputs_rejected(self):
        for name in ("../escape", "/absolute", "a/../b", "a\\b", "a::b"):
            with self.assertRaises(ValueError):
                framework.relative(name)
        with self.assertRaises(ValueError):
            self.freeze(artifact_files=[self.repository / "settings.xml"])
        self.classpath.write_text(str(self.source / "core/target/classes"))
        with self.assertRaises(ValueError):
            self.freeze()

    def test_manifest_classpath_jar_index_and_duplicate_entries_rejected(self):
        for entries, manifest in [({"example/Dependency.class": b"dep"}, "Manifest-Version: 1.0\nClass-Path: other.jar\n\n"),
                                  ({"META-INF/INDEX.LIST": b"JarIndex-Version: 1.0"}, "Manifest-Version: 1.0\n\n")]:
            self.write_jar(self.dependency, entries, manifest)
            with self.assertRaisesRegex(ValueError, "(?i)class-?path expansion"):
                self.freeze()
            shutil.rmtree(self.destination)
        self.write_jar(self.dependency, {"same": b"one"})
        with zipfile.ZipFile(self.dependency, "a") as archive:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                archive.writestr("same", b"two")
        with self.assertRaisesRegex(ValueError, "Duplicate JAR"):
            self.freeze()

    def test_self_only_jar_index_is_preserved_but_foreign_reference_rejected(self):
        index = b"JarIndex-Version: 1.0\n\nlibrary-1.0.jar\nexample\n\n"
        self.write_jar(self.dependency, {"example/Dependency.class": b"dep", "META-INF/INDEX.LIST": index})
        self.freeze()
        self.assertEqual(self.dependency.read_bytes(), (self.destination / "repository/example/library/1.0/library-1.0.jar").read_bytes())
        shutil.rmtree(self.destination)
        self.write_jar(self.dependency, {"META-INF/INDEX.LIST": index + b"other.jar\npackage\n"})
        with self.assertRaisesRegex(ValueError, "classpath expansion"):
            self.freeze()

    def test_command_requires_offline_and_detects_java_binary_change(self):
        self.freeze()
        with self.assertRaisesRegex(ValueError, "offline"):
            framework.command(self.destination, ["run"], work_directory=self.root / "attempt")
        self.java.write_bytes(b"different launcher")
        with self.assertRaisesRegex(ValueError, "Java launcher changed"):
            framework.validate_framework(self.destination)


if __name__ == "__main__":
    unittest.main()
