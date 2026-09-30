#!/usr/bin/env python3
"""Create four recovery-calibration catalogs without running or weakening their oracle."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

RECIPE = Path(__file__).absolute().parent
HARNESS = RECIPE.parents[1]
sys.dont_write_bytecode = True

NAME = "bounded-eos"
CONNECTOR = "flink-connector-kafka-5.0.0-2.2"
CANONICAL_SHA256 = "4bc398c08a5e1e74799594317c439935506ba95271224e5eccef3ab8af3c8a00"
EXPECTED_SHA256 = "d089980ce46c6d553065ba48d2420c0c7b8158269a40683c1a7258eb4057a0a3"
REQUIRED_RECIPE = {"build.py", "pom.xml", "RecoveryCheck.java", "recovery-offset.patch", "README.md"}
RECOVERY_PHASE = """  - name: taskmanager-recovery
    steps:
      - kill:
          target: { kind: named, role: taskmanager, name: taskmanager-1 }
      - wait: { duration: 1s }
      - restart: { component: taskmanager }
"""


def guarded(path, harness):
    """Reject escaping and symlink paths before reading their contents."""
    path = Path(path).absolute()
    if ".." in path.parts or not path.is_relative_to(harness):
        raise SystemExit("Path must be inside --harness-root: " + str(path))
    cursor = harness
    for part in path.relative_to(harness).parts:
        cursor /= part
        if cursor.is_symlink():
            raise SystemExit("Symlink paths are not accepted: " + str(cursor))
    return path


def digest(data):
    return hashlib.sha256(data).hexdigest()


guarded(HARNESS / "tools" / "subject_catalog.py", HARNESS)
sys.path.insert(0, str(HARNESS / "tools"))
import subject_catalog  # noqa: E402
from subject_catalog import (artifact_reference, check_released_subject,  # noqa: E402
                             replace_subject, sha256, subject_snippet)


def checked(path, expected, harness):
    path = guarded(path, harness)
    if not isinstance(expected, str) or len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        raise SystemExit("Invalid SHA-256 declaration for " + str(path))
    if not path.is_file() or sha256(path) != expected:
        raise SystemExit("Artifact or recipe checksum changed: " + str(path))
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-root", type=Path, default=HARNESS)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--output", type=Path, required=True,
                        help="New repository-local directory; existing evidence is never overwritten")
    args = parser.parse_args()
    harness = args.harness_root.absolute()
    artifact_root = (args.artifact_root or harness).absolute()
    if harness != HARNESS or artifact_root != harness:
        raise SystemExit("--artifact-root and --harness-root must equal this recipe's harness root.")
    output = guarded(args.output, harness)
    if output.exists():
        raise SystemExit("Output already exists; refusing to overwrite evidence: " + str(output))
    build = guarded(RECIPE / "target", harness)
    build_bytes = guarded(build / "build-evidence.json", harness).read_bytes()
    evidence = json.loads(build_bytes)
    if set(evidence["artifacts"]) != {"release", "mutant"}:
        raise SystemExit("Build evidence must contain exactly release and mutant artifacts.")
    recipe_inputs = evidence["recipeInputSha256"]
    if not REQUIRED_RECIPE <= set(recipe_inputs):
        raise SystemExit("Build evidence is missing required recipe hashes.")
    for name, expected in recipe_inputs.items():
        if Path(name).name != name:
            raise SystemExit("Recipe hash names must be plain filenames.")
        checked(RECIPE / name, expected, harness)
    checked(build / (CONNECTOR + "-sources.jar"), evidence["sourceSha256"], harness)

    scenario = guarded(harness / "scenarios" / (NAME + ".yaml"), harness)
    expected = guarded(harness / "scenarios" / (NAME + ".expected.yaml"), harness)
    scenario_bytes, expected_bytes = scenario.read_bytes(), expected.read_bytes()
    if digest(scenario_bytes) != CANONICAL_SHA256 or digest(expected_bytes) != EXPECTED_SHA256:
        raise SystemExit("Canonical scenario or expected PASS contract changed; review calibration before regenerating.")
    canonical = scenario_bytes.decode("utf-8")
    check_released_subject(canonical)
    if canonical.count(RECOVERY_PHASE) != 1:
        raise SystemExit("Canonical recovery phase changed; refusing an ambiguous no-kill control.")
    no_kill = canonical.replace(RECOVERY_PHASE, "", 1)
    templates = {"kill": canonical, "no-kill": no_kill}

    dependencies = []
    dependency_hashes = evidence["runtimeDependencySha256"]
    if not dependency_hashes:
        raise SystemExit("Released connector runtime dependency closure is missing.")
    for name, expected_hash in sorted(dependency_hashes.items()):
        if Path(name).name != name or not name.endswith(".jar"):
            raise SystemExit("Runtime dependency hash names must be JAR filenames.")
        path = checked(build / "runtime" / name, expected_hash, harness)
        dependencies.append(artifact_reference(path, artifact_root))
    subjects = {}
    for mode in ("release", "mutant"):
        info = evidence["artifacts"][mode]
        declared = Path(info["artifact"])
        if not declared.is_absolute() or declared.suffix != ".jar":
            raise SystemExit("Primary artifact must be an absolute JAR path.")
        path = checked(declared, info["sha256"], harness)
        if mode == "release" and info["sha256"] != evidence["releaseSha256"]:
            raise SystemExit("Release primary does not match the pinned release SHA-256.")
        if path.name in dependency_hashes:
            raise SystemExit("Primary connector cannot also be a runtime dependency.")
        subjects[mode] = subject_snippet(artifact_reference(path, artifact_root), dependencies)

    # Snapshot every textual input before creating output; archived bytes are the bytes hashed.
    archive = {
        "build-evidence.json": build_bytes,
        "scenarios/" + NAME + ".canonical.yaml": scenario_bytes,
        "scenarios/" + NAME + ".expected.yaml": expected_bytes,
        "scenarios/" + NAME + ".no-kill.yaml": no_kill.encode("utf-8"),
    }
    for path in sorted(RECIPE.iterdir()):
        if path.suffix in {".py", ".java", ".patch", ".md", ".xml"}:
            path = guarded(path, harness)
            if path.is_file():
                archive["calibration/recovery-mutant/" + path.name] = path.read_bytes()
    helper = guarded(Path(subject_catalog.__file__), harness)
    archive["tools/subject_catalog.py"] = helper.read_bytes()
    for name, expected_hash in recipe_inputs.items():
        if digest(archive["calibration/recovery-mutant/" + name]) != expected_hash:
            raise SystemExit("Recipe changed while catalog inputs were being validated: " + name)
    source_hashes = {name: digest(data) for name, data in sorted(archive.items())}
    cells = []
    for mode, snippet in subjects.items():
        for fault, template in templates.items():
            rendered = replace_subject(template, snippet)
            row = {
                "cell": mode + "-" + fault, "scenario": NAME, "mode": mode, "fault": fault,
                "artifactRoot": str(artifact_root), "scenarioSha256": digest(rendered.encode("utf-8")),
                "canonicalScenarioSha256": digest(scenario_bytes), "expectedSha256": digest(expected_bytes),
                "templateSha256": digest(template.encode("utf-8")),
                "subjectSnippetSha256": digest(snippet.encode("utf-8")),
                "primarySha256": evidence["artifacts"][mode]["sha256"],
                "runtimeDependencySha256": dependency_hashes, "sourceHashes": source_hashes,
                "requiredOutcome": "pass",
                "calibrationExpectation": "missing-one-after-confirmed-restore" if mode == "mutant" and fault == "kill" else "pass",
            }
            cells.append((row, rendered, snippet))

    output.mkdir(parents=True, exist_ok=False)
    for name, data in archive.items():
        destination = output / "evidence" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
    for row, rendered, snippet in cells:
        cell = output / row["cell"]
        cell.mkdir()
        (cell / (NAME + ".yaml")).write_text(rendered, encoding="utf-8")
        (cell / (NAME + ".expected.yaml")).write_bytes(expected_bytes)
        (cell / "subject-snippet.txt").write_text(snippet, encoding="utf-8")
        (cell / "catalog-manifest.json").write_text(json.dumps(row, indent=2) + "\n", encoding="utf-8")
    manifest = {"cells": [row for row, unused, unused_snippet in cells],
                "buildEvidenceSha256": digest(build_bytes), "sourceHashes": source_hashes,
                "runtimeValidation": "pending; catalog generation does not execute or classify a run"}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print("Created four recovery catalogs:", output)


if __name__ == "__main__":
    main()
