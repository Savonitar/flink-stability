#!/usr/bin/env python3
"""Create six isolated canonical catalogs; never run a scenario or change its PASS expectation."""
import argparse
import json
from pathlib import Path
import shutil
import sys

RECIPE = Path(__file__).resolve().parent
sys.path.insert(0, str(RECIPE.parents[1] / "tools"))
from subject_catalog import (artifact_reference, check_released_subject,  # noqa: E402
                             replace_subject, sha256 as sha, subject_snippet)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-root", type=Path, default=Path.cwd(), help="FU-7-capable checkout")
    parser.add_argument("--artifact-root", type=Path, help="Root containing harness and generated artifacts")
    parser.add_argument("--output", type=Path, required=True, help="New directory; existing evidence is never overwritten")
    args = parser.parse_args()
    harness = args.harness_root.resolve()
    artifact_root = (args.artifact_root or harness).resolve()
    if artifact_root != harness:
        raise SystemExit("--artifact-root must equal --harness-root so the canonical workload JAR resolves correctly.")
    output = args.output.resolve()
    build = RECIPE / "target"
    evidence = json.loads((build / "build-evidence.json").read_text())
    for name, digest in evidence["recipeInputSha256"].items():
        if sha(RECIPE / name) != digest:
            raise SystemExit("Recipe input changed since build; rebuild before creating catalogs: " + name)
    sources = {}
    for side in ("request", "response"):
        name = "commit-" + side + "-lost"
        scenario = harness / "scenarios" / (name + ".yaml")
        expected = harness / "scenarios" / (name + ".expected.yaml")
        if not scenario.is_file() or not expected.is_file():
            raise SystemExit("Canonical EndTxn scenarios absent: use a FU-7-capable harness checkout.")
        text = scenario.read_text()
        anchor = "        transaction_id_naming_strategy: INCREMENTING\n"
        if text.count(anchor) != 1 or "transaction_timeout:" in text:
            raise SystemExit("Canonical sink shape changed; review the shared timeout before regenerating.")
        check_released_subject(text)
        if "default:\n  outcome: pass" not in expected.read_text():
            raise SystemExit("Canonical expected PASS contract changed.")
        shared = text.replace(anchor, anchor + "        transaction_timeout: 60s\n")
        sources[side] = (name, scenario, expected, shared)

    def reference(path):
        return artifact_reference(path, artifact_root)

    dependencies = []
    for name, digest in sorted(evidence["runtimeDependencySha256"].items()):
        path = build / "runtime" / name
        if sha(path) != digest: raise SystemExit("Runtime dependency changed: " + name)
        dependencies.append(reference(path))
    subjects = {}
    for mode, info in evidence["artifacts"].items():
        path = Path(info["artifact"])
        if sha(path) != info["sha256"]: raise SystemExit("Primary artifact changed: " + mode)
        subjects[mode] = subject_snippet(reference(path), dependencies)
    # All input validation happens before creating an output tree.
    output.mkdir(parents=True, exist_ok=False)
    archive = output / "evidence"
    archive.mkdir()
    shutil.copyfile(build / "build-evidence.json", archive / "build-evidence.json")
    recipe_hashes = {}
    for path in sorted(RECIPE.iterdir()):
        if path.is_file() and path.suffix in {".py", ".java", ".patch", ".md", ".xml"}:
            shutil.copyfile(path, archive / path.name)
            recipe_hashes[path.name] = sha(path)
    rows = []
    for side, (name, scenario, expected, shared) in sources.items():
        shutil.copyfile(scenario, archive / (name + ".canonical.yaml"))
        (archive / (name + ".shared-controls.yaml")).write_text(shared)
        for mode, snippet in subjects.items():
            cell = output / (mode + "-" + side)
            cell.mkdir()
            target = cell / scenario.name
            target.write_text(replace_subject(shared, snippet))
            shutil.copyfile(expected, cell / expected.name)
            (cell / "subject-snippet.txt").write_text(snippet)
            row = {"cell": cell.name, "scenario": name, "artifactRoot": str(artifact_root),
                   "scenarioSha256": sha(target), "canonicalScenarioSha256": sha(scenario),
                   "expectedSha256": sha(expected), "subjectSnippetSha256": sha(cell / "subject-snippet.txt"),
                   "sharedTemplateSha256": sha(archive / (name + ".shared-controls.yaml")),
                   "sharedTransactionTimeout": "60s", "primarySha256": evidence["artifacts"][mode]["sha256"],
                   "sourceHashes": recipe_hashes}
            (cell / "catalog-manifest.json").write_text(json.dumps(row, indent=2) + "\n")
            rows.append(row)
    (output / "manifest.json").write_text(json.dumps({"cells": rows, "buildEvidenceSha256": sha(archive / "build-evidence.json")}, indent=2) + "\n")
    print("Created six catalogs:", output)


if __name__ == "__main__":
    main()
