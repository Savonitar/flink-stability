"""Exercise calibration catalog generation with synthetic local artifacts."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
RECIPE = Path("calibration/connector-mutants")
RELEASED_ARTIFACT = "      artifact: maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2\n"
ANCHOR = "        transaction_id_naming_strategy: INCREMENTING\n"
MODES = ("control", "assume", "rewrite")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class CatalogEvidenceTest(unittest.TestCase):
    def setUp(self):
        # Keep every fixture inside the repository, including temporary artifacts.
        temporary = tempfile.TemporaryDirectory(prefix=".catalog-test-", dir=ROOT)
        self.addCleanup(temporary.cleanup)
        self.harness = Path(temporary.name)
        recipe = self.harness / RECIPE
        recipe.mkdir(parents=True)
        helper = self.harness / "tools/subject_catalog.py"
        helper.parent.mkdir()
        shutil.copyfile(ROOT / "tools/subject_catalog.py", helper)
        shutil.copyfile(ROOT / RECIPE / "catalogs.py", recipe / "catalogs.py")
        (recipe / "build.py").write_text("# Synthetic build evidence fixture.\n")
        build = recipe / "target"
        runtime = build / "runtime"
        runtime.mkdir(parents=True)
        dependency = runtime / "client.jar"
        dependency.write_bytes(b"synthetic runtime dependency")
        artifacts = {}
        for mode in MODES:
            artifact = build / (mode + ".jar")
            artifact.write_bytes(("synthetic " + mode).encode())
            artifacts[mode] = {"artifact": str(artifact), "sha256": digest(artifact)}
        (build / "build-evidence.json").write_text(json.dumps({
            "recipeInputSha256": {"build.py": digest(recipe / "build.py")},
            "runtimeDependencySha256": {dependency.name: digest(dependency)},
            "artifacts": artifacts,
        }))
        scenarios = self.harness / "scenarios"
        scenarios.mkdir()
        for side in ("request", "response"):
            name = "commit-" + side + "-lost"
            for suffix in (".yaml", ".expected.yaml"):
                shutil.copyfile(ROOT / "scenarios" / (name + suffix), scenarios / (name + suffix))
        self.output = self.harness / "catalogs"
        result = subprocess.run([
            sys.executable, "-B", str(recipe / "catalogs.py"),
            "--harness-root", str(self.harness), "--artifact-root", str(self.harness),
            "--output", str(self.output),
        ], cwd=self.harness, capture_output=True, text=True, check=False)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_archive_retains_exact_imported_helper_and_hash_in_every_cell(self):
        helper_name = "tools/subject_catalog.py"
        archived = self.output / "evidence" / helper_name
        self.assertEqual((self.harness / helper_name).read_bytes(), archived.read_bytes())
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(6, len(manifest["cells"]))
        for row in manifest["cells"]:
            with self.subTest(cell=row["cell"]):
                self.assertEqual(digest(archived), row["sourceHashes"][helper_name])
                cell_manifest = json.loads(
                    (self.output / row["cell"] / "catalog-manifest.json").read_text())
                self.assertEqual(row["sourceHashes"], cell_manifest["sourceHashes"])
                for name, expected_hash in row["sourceHashes"].items():
                    self.assertEqual(expected_hash, digest(self.output / "evidence" / name))

    def test_templates_expected_contracts_and_generated_catalogs_are_preserved(self):
        archive = self.output / "evidence"
        for side in ("request", "response"):
            name = "commit-" + side + "-lost"
            canonical = (self.harness / "scenarios" / (name + ".yaml")).read_text()
            expected = (self.harness / "scenarios" / (name + ".expected.yaml")).read_bytes()
            shared = canonical.replace(ANCHOR, ANCHOR + "        transaction_timeout: 60s\n")
            self.assertEqual(canonical.encode(), (archive / (name + ".canonical.yaml")).read_bytes())
            self.assertEqual(shared.encode(), (archive / (name + ".shared-controls.yaml")).read_bytes())
            for mode in MODES:
                with self.subTest(side=side, mode=mode):
                    cell = self.output / (mode + "-" + side)
                    replacement = (
                        f"      artifact: ./{RECIPE}/target/{mode}.jar\n"
                        "      runtime_dependencies:\n"
                        f"        - ./{RECIPE}/target/runtime/client.jar\n"
                    )
                    self.assertEqual(
                        shared.replace(RELEASED_ARTIFACT, replacement).encode(),
                        (cell / (name + ".yaml")).read_bytes())
                    self.assertEqual(expected, (cell / (name + ".expected.yaml")).read_bytes())


if __name__ == "__main__":
    unittest.main()
