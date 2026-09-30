"""Exercise recovery calibration catalogs using synthetic local build artifacts."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
RECIPE = Path("calibration/recovery-mutant")
RECIPE_INPUTS = ("build.py", "pom.xml", "RecoveryCheck.java", "recovery-offset.patch", "README.md")
SOURCE_JAR = "flink-connector-kafka-5.0.0-2.2-sources.jar"
RELEASED_ARTIFACT = "      artifact: maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2\n"
RECOVERY_PHASE = (
    "  - name: taskmanager-recovery\n"
    "    steps:\n"
    "      - kill:\n"
    "          target: { kind: named, role: taskmanager, name: taskmanager-1 }\n"
    "      - wait: { duration: 1s }\n"
    "      - restart: { component: taskmanager }\n"
)
MODES = ("release", "mutant")
FAULTS = ("kill", "no-kill")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class RecoveryCatalogTest(unittest.TestCase):
    def setUp(self):
        # Outside-harness fixtures still stay inside the repository boundary.
        temporary = tempfile.TemporaryDirectory(prefix=".recovery-catalog-test-", dir=ROOT)
        self.addCleanup(temporary.cleanup)
        self.workspace = Path(temporary.name)
        self.harness = self.workspace / "harness"
        self.recipe = self.harness / RECIPE
        self.recipe.mkdir(parents=True)
        helper = self.harness / "tools/subject_catalog.py"
        helper.parent.mkdir()
        shutil.copyfile(ROOT / "tools/subject_catalog.py", helper)
        shutil.copyfile(ROOT / RECIPE / "catalogs.py", self.recipe / "catalogs.py")
        for name in RECIPE_INPUTS:
            (self.recipe / name).write_bytes(("Synthetic recipe input: " + name + "\n").encode())
        self.build = self.recipe / "target"
        runtime = self.build / "runtime"
        runtime.mkdir(parents=True)
        self.dependencies = {}
        for name in ("client.jar", "support.jar"):
            dependency = runtime / name
            dependency.write_bytes(("Synthetic runtime dependency: " + name).encode())
            self.dependencies[name] = digest(dependency)
        source = self.build / SOURCE_JAR
        source.write_bytes(b"Synthetic connector source archive")
        artifacts = {}
        for mode in MODES:
            artifact = self.build / (mode + ".jar")
            artifact.write_bytes(("Synthetic connector: " + mode).encode())
            artifacts[mode] = {
                "artifact": str(artifact),
                "sha256": digest(artifact),
                "changedEntries": [] if mode == "release" else [
                    "org/apache/flink/connector/kafka/source/reader/KafkaSourceReader.class"
                ],
            }
        self.evidence = {
            "artifacts": artifacts,
            "runtimeDependencySha256": self.dependencies,
            "recipeInputSha256": {name: digest(self.recipe / name) for name in RECIPE_INPUTS},
            "sourceSha256": digest(source),
            "releaseSha256": artifacts["release"]["sha256"],
            "matchesTestedArtifactHashes": False,
            "runtimeValidation": "pending",
        }
        self.write_evidence()
        scenarios = self.harness / "scenarios"
        scenarios.mkdir()
        for suffix in (".yaml", ".expected.yaml"):
            name = "bounded-eos" + suffix
            shutil.copyfile(ROOT / "scenarios" / name, scenarios / name)
        self.canonical = (scenarios / "bounded-eos.yaml").read_text()
        self.expected = (scenarios / "bounded-eos.expected.yaml").read_bytes()
        self.assertEqual(1, self.canonical.count(RECOVERY_PHASE))
        self.no_kill = self.canonical.replace(RECOVERY_PHASE, "", 1)
        self.output = self.harness / "catalogs"

    def write_evidence(self):
        (self.build / "build-evidence.json").write_text(json.dumps(self.evidence))

    def generate(self, *, harness=None, artifact_root=None):
        return subprocess.run([
            sys.executable, "-B", str(self.recipe / "catalogs.py"),
            "--harness-root", str(harness or self.harness),
            "--artifact-root", str(artifact_root or self.harness),
            "--output", str(self.output),
        ], cwd=self.harness, capture_output=True, text=True, check=False)

    def generate_successfully(self):
        result = self.generate()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        return json.loads((self.output / "manifest.json").read_text())

    def assert_rejected_before_output(self, **arguments):
        result = self.generate(**arguments)
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertFalse(self.output.exists(), result.stdout + result.stderr)

    def test_four_cells_preserve_exact_contract_and_only_remove_recovery_phase(self):
        manifest = self.generate_successfully()
        cell_names = {mode + "-" + fault for mode in MODES for fault in FAULTS}
        self.assertEqual(cell_names, {row["cell"] for row in manifest["cells"]})
        self.assertEqual(4, len(manifest["cells"]))
        for mode in MODES:
            snippet = (
                f"artifact: ./{RECIPE}/target/{mode}.jar\n"
                "runtime_dependencies:\n"
                f"  - ./{RECIPE}/target/runtime/client.jar\n"
                f"  - ./{RECIPE}/target/runtime/support.jar\n"
            )
            replacement = "".join("      " + line + "\n" for line in snippet.splitlines())
            for fault in FAULTS:
                with self.subTest(mode=mode, fault=fault):
                    cell = self.output / (mode + "-" + fault)
                    template = self.canonical if fault == "kill" else self.no_kill
                    self.assertEqual(
                        template.replace(RELEASED_ARTIFACT, replacement, 1).encode(),
                        (cell / "bounded-eos.yaml").read_bytes())
                    self.assertEqual(self.expected, (cell / "bounded-eos.expected.yaml").read_bytes())
                    self.assertEqual(snippet, (cell / "subject-snippet.txt").read_text())
        self.assertEqual(self.canonical.encode(), (self.harness / "scenarios/bounded-eos.yaml").read_bytes())
        self.assertEqual(self.expected, (self.harness / "scenarios/bounded-eos.expected.yaml").read_bytes())

    def test_archives_and_all_manifest_hashes_match_exact_inputs_and_outputs(self):
        manifest = self.generate_successfully()
        archive = self.output / "evidence"
        source_hashes = manifest["sourceHashes"]
        required_sources = {
            "scenarios/bounded-eos.canonical.yaml",
            "scenarios/bounded-eos.expected.yaml",
            "scenarios/bounded-eos.no-kill.yaml",
            "tools/subject_catalog.py",
            *(str(RECIPE / name) for name in (*RECIPE_INPUTS, "catalogs.py")),
        }
        self.assertTrue(required_sources.issubset(source_hashes), source_hashes)
        self.assertEqual(self.canonical.encode(), (archive / "scenarios/bounded-eos.canonical.yaml").read_bytes())
        self.assertEqual(self.expected, (archive / "scenarios/bounded-eos.expected.yaml").read_bytes())
        self.assertEqual(self.no_kill.encode(), (archive / "scenarios/bounded-eos.no-kill.yaml").read_bytes())
        for name in ("tools/subject_catalog.py", *(str(RECIPE / item) for item in (*RECIPE_INPUTS, "catalogs.py"))):
            with self.subTest(source=name):
                self.assertEqual((self.harness / name).read_bytes(), (archive / name).read_bytes())
        for name, expected_hash in source_hashes.items():
            with self.subTest(source_hash=name):
                self.assertEqual(expected_hash, digest(archive / name))
        self.assertEqual((self.build / "build-evidence.json").read_bytes(), (archive / "build-evidence.json").read_bytes())
        self.assertEqual(manifest["buildEvidenceSha256"], digest(archive / "build-evidence.json"))
        for row in manifest["cells"]:
            with self.subTest(cell=row["cell"]):
                cell = self.output / row["cell"]
                self.assertEqual(row, json.loads((cell / "catalog-manifest.json").read_text()))
                self.assertEqual("bounded-eos", row["scenario"])
                self.assertEqual(str(self.harness), row["artifactRoot"])
                self.assertIn(row["mode"], MODES)
                self.assertIn(row["fault"], FAULTS)
                self.assertEqual(row["mode"] + "-" + row["fault"], row["cell"])
                self.assertEqual("pass", row["requiredOutcome"])
                expected_calibration = "missing-one-after-confirmed-restore" if row["cell"] == "mutant-kill" else "pass"
                self.assertEqual(expected_calibration, row["calibrationExpectation"])
                self.assertEqual(source_hashes, row["sourceHashes"])
                self.assertEqual(self.dependencies, row["runtimeDependencySha256"])
                self.assertEqual(self.evidence["artifacts"][row["mode"]]["sha256"], row["primarySha256"])
                self.assertEqual(digest(cell / "bounded-eos.yaml"), row["scenarioSha256"])
                self.assertEqual(digest(cell / "bounded-eos.expected.yaml"), row["expectedSha256"])
                self.assertEqual(digest(cell / "subject-snippet.txt"), row["subjectSnippetSha256"])
                self.assertEqual(source_hashes["scenarios/bounded-eos.canonical.yaml"], row["canonicalScenarioSha256"])
                template_name = "canonical" if row["fault"] == "kill" else "no-kill"
                self.assertEqual(source_hashes[f"scenarios/bounded-eos.{template_name}.yaml"], row["templateSha256"])

    def test_tampered_build_inputs_fail_before_creating_output(self):
        paths = [
            *(self.build / (mode + ".jar") for mode in MODES),
            *(self.build / "runtime" / name for name in self.dependencies),
            self.build / SOURCE_JAR,
            *(self.recipe / name for name in RECIPE_INPUTS),
        ]
        for path in paths:
            with self.subTest(path=path.relative_to(self.harness)):
                original = path.read_bytes()
                try:
                    path.write_bytes(original + b"\nTampered after build")
                    self.assert_rejected_before_output()
                finally:
                    path.write_bytes(original)

    def test_release_metadata_must_match_the_verified_release_artifact(self):
        self.evidence["releaseSha256"] = "0" * 64
        self.write_evidence()
        self.assert_rejected_before_output()

    def test_artifact_modes_must_be_exactly_release_and_mutant(self):
        original = dict(self.evidence["artifacts"])
        for artifacts in (
            {"release": original["release"]},
            {**original, "extra": original["release"]},
        ):
            with self.subTest(modes=sorted(artifacts)):
                self.evidence["artifacts"] = artifacts
                self.write_evidence()
                self.assert_rejected_before_output()

    def test_artifact_root_must_equal_the_copied_harness_root(self):
        self.assert_rejected_before_output(artifact_root=self.workspace)

    def test_harness_root_must_match_the_executed_recipe(self):
        other = self.workspace / "other-harness"
        shutil.copytree(self.harness / "scenarios", other / "scenarios")
        self.assert_rejected_before_output(harness=other, artifact_root=other)

    def test_primary_artifact_outside_harness_is_rejected_even_with_matching_hash(self):
        outside = self.workspace / "outside.jar"
        outside.write_bytes(b"Synthetic artifact outside harness")
        self.evidence["artifacts"]["mutant"].update(artifact=str(outside), sha256=digest(outside))
        self.write_evidence()
        self.assert_rejected_before_output()

    def test_output_outside_harness_is_rejected_without_creating_it(self):
        self.output = self.workspace / "outside-catalogs"
        self.assert_rejected_before_output()

    def test_existing_output_is_never_overwritten(self):
        self.output.mkdir()
        sentinel = self.output / "retained-evidence.txt"
        sentinel.write_bytes(b"Original retained evidence")
        result = self.generate()
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(b"Original retained evidence", sentinel.read_bytes())
        self.assertEqual([sentinel], list(self.output.iterdir()))

    def test_canonical_scenario_and_expected_contract_are_pinned(self):
        scenario = self.harness / "scenarios/bounded-eos.yaml"
        expected = self.harness / "scenarios/bounded-eos.expected.yaml"
        changes = (
            (scenario, self.canonical.replace("total: 3000", "total: 3001")),
            (scenario, self.canonical.replace("interval: 1s", "interval: 2s")),
            (scenario, self.canonical.replace("duration: 1s", "duration: 2s")),
            (expected, self.expected.decode().replace("outcome: pass", "outcome: fail")),
            (expected, self.expected.decode() + "\n# Altered contract bytes\n"),
        )
        for path, changed in changes:
            with self.subTest(file=path.name, changed=changed):
                original = path.read_bytes()
                try:
                    self.assertNotEqual(original, changed.encode())
                    path.write_text(changed)
                    self.assert_rejected_before_output()
                finally:
                    path.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
