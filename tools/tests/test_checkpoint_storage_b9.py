"""Exercise real build orchestration and render determinism using generated local inputs only."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


builder = load("checkpoint_storage_b9_builder", REPO / "testcontainers/checkpoint_storage_build.py")


class ActualBuildOrderingTest(unittest.TestCase):
    def run_build(self, root, tampered=False):
        native = root / builder.NATIVE
        native.mkdir(parents=True)
        for name in builder.NATIVE_SOURCES:
            (native / name).write_text("/* generated source fixture */\n")
        for relative in (builder.JAVA, builder.INSPECTOR):
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("// generated Java source fixture\n")
        source = root / "testcontainers/checkpoint_storage_build.py"
        source.write_bytes(Path(builder.__file__).read_bytes())
        java_home = root / "jdk"
        (java_home / "bin").mkdir(parents=True)
        (java_home / "lib").mkdir()
        (java_home / "release").write_text('JAVA_VERSION="21.0.7"\nIMPLEMENTOR="Generated JDK fixture"\n')
        (java_home / "lib/modules").write_bytes(b"generated module image fixture")
        for name in ("java", "javac", "jar"):
            (java_home / "bin" / name).write_text("generated executable fixture\n")
        docker = root / "docker"
        docker.write_text("generated executable fixture\n")
        packages = {}
        lock = dict(schemaVersion=1, imageId=builder.IMAGE_ID, imageDigest=builder.IMAGE_DIGEST,
                    platform=builder.PLATFORM, sources=builder.SOURCES, packages=[], roots={})
        for name in ("gcc", "libc6-dev", "libfuse3-dev", "pkg-config"):
            data = ("generated package " + name).encode()
            packages[name + ".deb"] = data
            path = root / (name + ".deb")
            path.write_bytes(data)
            filename = "pool/main/f/fixture/" + name + ".deb"
            lock["packages"].append(dict(package=name, version="1", architecture="arm64", size=len(data),
                sha256=builder.sha256(path), filename=filename,
                url="https://ports.ubuntu.com/ubuntu-ports/" + filename))
            lock["roots"][name] = "1"
        (native / "dependencies.json").write_text(json.dumps(lock))
        image = dict(Id=builder.IMAGE_ID, Os="linux", Architecture="arm64", RepoDigests=[builder.IMAGE_DIGEST])
        output = root / "jobs/build"
        instances = []

        class Commands(builder.Commands):
            def __init__(self, *args):
                super().__init__(*args)
                instances.append(self)
                self.name = None

            def run(self, label, command, **unused):
                self.receipts.append(dict(label=label, argv=command, exitCode=0, elapsedSeconds=0))
                if label == "javac-version":
                    return "javac 21.0.7\n"
                if label == "jar-version":
                    return "jar 21.0.7\n"
                if label in ("java-version", "javac-runtime-version", "jar-runtime-version"):
                    return "openjdk 21.0.7\nGenerated JDK fixture (build 21.0.7+1)\n"
                if label == "image-store-backend":
                    return json.dumps(dict(Driver="overlay2", DriverStatus=[]))
                if label == "create-builder":
                    self.name = command[command.index("--name") + 1]
                elif label in ("verify-created-image", "cleanup-inspect"):
                    return json.dumps([dict(Image=builder.IMAGE_ID,
                        Config=dict(Image=builder.IMAGE_ID, Labels={builder.LABEL: self.name}))])
                elif label == "package-sha256":
                    lines = ["\t".join(str(package[key]) for key in
                             ("package", "version", "architecture", "size", "sha256")) for package in lock["packages"]]
                    if tampered:
                        columns = lines[0].split("\t")
                        columns[-1] = "0" * 64
                        lines[0] = "\t".join(columns)
                    return "\n".join(lines) + "\n"
                elif label == "copy-public-packages":
                    destination = Path(command[-1]); destination.mkdir()
                    for name, data in packages.items():
                        (destination / name).write_bytes(data)
                elif label.startswith("verify-network-"):
                    return json.dumps([dict(NetworkSettings=dict(Networks={}))])
                elif label == "compile-native":
                    self.receipts[-1]["exitCode"] = 17
                    raise RuntimeError("intentional fixture stop at native compilation")
                elif label not in ("start-builder", "prepare-package-area", "apt-source", "apt-update",
                                   "signed-index-hashes", "apt-download", "disconnect-network", "apt-install-locked",
                                   "copy-frozen-source", "cleanup-remove"):
                    raise AssertionError("Unexpected subprocess label: " + label)
                return ""

        args = SimpleNamespace(output=output, java_home=java_home, docker=docker,
                               docker_host="unix:///generated/docker.sock", timeout_seconds=60)
        expected = "Downloaded package differs" if tampered else "intentional fixture stop"
        with patch.object(builder, "REPO", root), patch.object(builder, "__file__", str(source)), \
                patch.object(builder, "Commands", Commands), patch.object(builder, "ensure_image", return_value=image):
            with self.assertRaisesRegex((ValueError, RuntimeError), expected):
                builder.build(args)
        self.assertEqual(1, len(instances))
        evidence = json.loads((output / "build-evidence.json").read_bytes())
        self.assertEqual("failed", evidence["status"])
        self.assertEqual([dict(container=instances[0].name, status="removed")], evidence["cleanup"])
        self.assertEqual(instances[0].receipts, evidence["commands"])
        self.assertEqual("cleanup-remove", evidence["commands"][-1]["label"])
        self.assertEqual("Generated JDK fixture", evidence["javaToolchain"]["vendor"])
        self.assertEqual(builder.sha256(java_home / "lib/modules"), evidence["javaToolchain"]["modulesSha256"])
        return evidence

    def test_actual_build_verifies_downloads_then_disconnects_before_install_and_source_transfer(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence = self.run_build(Path(temporary).resolve())
        labels = [row["label"] for row in evidence["commands"]]
        required = ["apt-download", "package-sha256", "copy-public-packages", "disconnect-network",
                    "verify-network-before-install", "apt-install-locked", "verify-network-before-source",
                    "copy-frozen-source", "compile-native", "cleanup-inspect", "cleanup-remove"]
        self.assertEqual(required, [label for label in labels if label in required])
        self.assertTrue(evidence["networkDisconnectedBeforePackageInstall"])
        self.assertTrue(evidence["networkDisconnectedBeforeSourceCopy"])
        self.assertEqual(4, evidence["downloadedPackageCount"])
        install = next(row["argv"] for row in evidence["commands"] if row["label"] == "apt-install-locked")
        self.assertIn("--no-download", install)
        self.assertTrue(all(name + "=1" in install for name in ("gcc", "libc6-dev", "libfuse3-dev", "pkg-config")))

    def test_actual_build_aborts_tampered_package_receipt_before_install_or_source_transfer(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence = self.run_build(Path(temporary).resolve(), tampered=True)
        labels = [row["label"] for row in evidence["commands"]]
        self.assertIn("package-sha256", labels)
        self.assertTrue(set(labels).isdisjoint({"copy-public-packages", "disconnect-network", "apt-install-locked",
                                              "copy-frozen-source", "compile-native"}))
        self.assertFalse(evidence["networkDisconnectedBeforePackageInstall"])
        self.assertFalse(evidence["networkDisconnectedBeforeSourceCopy"])


class CrossProcessReportDeterminismTest(unittest.TestCase):
    def test_json_and_real_markdown_renderer_are_identical_across_hash_seeds(self):
        program = r'''
import importlib.util, json, pathlib, sys
source=pathlib.Path(sys.argv[1])
spec=importlib.util.spec_from_file_location("generated_report_fixture", source)
fixture=importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)
value, manifest=fixture.fixture()
def unordered(item):
    if isinstance(item, dict): return {key: unordered(item[key]) for key in set(item)}
    if isinstance(item, list): return [unordered(value) for value in item]
    return item
value, manifest=unordered(value), unordered(manifest)
value["errors"]=[{"scope":"generated | fixture", "details":{"z":2,"a":1}}]
print(json.dumps({"json":json.dumps(value, sort_keys=True, ensure_ascii=False),
                  "markdown":fixture.report.render(value, manifest)}, sort_keys=True, ensure_ascii=False))
'''
        outputs = []
        for seed in ("1", "19", "137"):
            result = subprocess.run([sys.executable, "-B", "-c", program,
                str(Path(__file__).with_name("test_checkpoint_storage_calibration_report.py"))],
                env={"PATH": "/usr/bin:/bin", "PYTHONHASHSEED": seed, "PYTHONDONTWRITEBYTECODE": "1"},
                capture_output=True, timeout=30, check=True)
            self.assertEqual(b"", result.stderr)
            outputs.append(result.stdout)
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(outputs[0], outputs[2])
        rendered = json.loads(outputs[0])
        self.assertEqual(12, len(json.loads(rendered["json"])["attempts"]))
        self.assertEqual(3, rendered["markdown"].count("| FAIL | validator.kafka.state-prefix.state-mismatch | confirmed |"))
        self.assertIn("Raw component events: 84; REST errors: 6", rendered["markdown"])


if __name__ == "__main__":
    unittest.main()
