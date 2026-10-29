#!/usr/bin/env python3
"""Freeze separate workload artifacts, catalogs and recipe for twelve local qualification attempts."""
import argparse
import json
from pathlib import Path
import re

from check import (EXECUTIONS, EXPECTED_BYTES, MODES, SCENARIO, audit_attempt, digest, guarded,
                   read, require, runtime_identity)

RECIPE = Path(__file__).absolute().parent
ROOT = RECIPE.parents[1]


def render(template, restart_phase, workload_reference, execution, runtime_identity=None, *, mode="correct"):
    require(template.count('"@WORKLOAD_JAR@"') == 1, "Workload placeholder changed")
    require(execution in EXECUTIONS, "Unknown execution arm")
    require(mode in MODES, "Unknown workload arm")
    rendered = template.replace('"@WORKLOAD_JAR@"', json.dumps(workload_reference))
    if mode == "mutant":
        require(rendered.count("\nmeta:\n") == 1, "Metadata boundary changed")
        rendered = rendered.replace("\nmeta:\n", "\nmeta:\n  allow_state_reset_mutant: true\n")
    if runtime_identity is not None:
        for component, indentation in (("kafka", "        "), ("flink", "    ")):
            reference, image_id = runtime_identity[component + "ImageReference"], runtime_identity[component + "ImageId"]
            image_line = indentation + "image: " + reference + "\n"
            require(rendered.count(image_line) == 1 and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id),
                    "Reference image identity differs from the calibration template")
            rendered = rendered.replace(image_line, image_line + indentation + "image_id: " + json.dumps(image_id) + "\n")
    if execution == "restart":
        require(rendered.count("\nterminal_validations:\n") == 1, "Terminal boundary changed")
        rendered = rendered.replace("\nterminal_validations:\n", "\n" + restart_phase + "\nterminal_validations:\n")
    return rendered.encode("utf-8")


def freeze(build_path, reference_path, output, root=ROOT, recipe=RECIPE):
    root, output = Path(root).absolute(), Path(output).absolute()
    guarded(output, root / "jobs")
    require(not output.exists(), "Output already exists; retained evidence will not be overwritten")
    build_bytes = read(build_path, root)
    build = json.loads(build_bytes)
    require(set(build["artifacts"]) == set(MODES), "Build requires exactly correct and mutant artifacts")
    reference_bytes = read(reference_path, root)
    reference = json.loads(reference_bytes)
    require(reference["status"] == reference["attempt"]["status"] == "pass", "Reference pilot did not pass")
    require(reference["evidence"]["terminalValidation"]["reason"] == "validator.kafka.state-prefix.match",
            "Reference pilot did not use the state oracle")
    identity = runtime_identity(reference)
    files = {"evidence/build-evidence.json": build_bytes, "evidence/reference-result.json": reference_bytes}
    template = read(recipe / "baseline.yaml", root).decode("utf-8")
    restart_phase = read(recipe / "restart-phase.yaml", root).decode("utf-8")
    expected = read(recipe / "state-recovery.expected.yaml", root)
    require(expected == EXPECTED_BYTES, "Recipe must retain PASS expectation")
    recipe_paths = ("baseline.yaml", "restart-phase.yaml", "state-recovery.expected.yaml",
                    "build.py", "catalogs.py", "check.py", "run.py", "README.md")
    for name in recipe_paths:
        files["evidence/recipe/" + name] = read(recipe / name, root)
    require(build["recipeInputSha256"], "Build inputs were not recorded")
    for name, value in build["recipeInputSha256"].items():
        require(not Path(name).is_absolute() and ".." not in Path(name).parts, "Invalid source recipe path")
        files["evidence/build-sources/" + name] = read(root / name, root, value)
    artifacts = {}
    for mode in MODES:
        source = build["artifacts"][mode]
        data = read(source["path"], root, source["sha256"])
        name = f"artifacts/{mode}.jar"
        files[name] = data
        artifacts[mode] = {"path": name, "sha256": digest(data)}
    audit_attempt(reference, 0, {"mode": "correct", "execution": "uninterrupted"},
                  {"workload": {"keyCount": 8, "totalRecords": 6000}, "artifacts": artifacts, "runtimeIdentity": identity})
    cells = []
    for mode in MODES:
        for execution in EXECUTIONS:
            name = mode + "-" + execution
            catalog = "catalogs/" + name
            artifact_reference = "./" + str((output / artifacts[mode]["path"]).relative_to(root))
            scenario = render(template, restart_phase, artifact_reference, execution, identity, mode=mode)
            files[f"{catalog}/{SCENARIO}.yaml"] = scenario
            files[f"{catalog}/{SCENARIO}.expected.yaml"] = expected
            cells.append({"name": name, "mode": mode, "execution": execution, "scenario": SCENARIO,
                          "catalog": catalog, "scenarioSha256": digest(scenario), "expectedSha256": digest(expected),
                          "requiredOutcome": "pass", "attempts": [f"attempts/{name}/run-{number}" for number in range(1, 4)]})
    manifest = {"formatVersion": 1, "catalogContractVersion": 2, "kind": "state-recovery-calibration", "attemptsPerCell": 3,
                "workload": {"keyCount": 8, "totalRecords": 6000}, "artifacts": artifacts,
                "buildEvidence": {"path": "evidence/build-evidence.json", "sha256": digest(build_bytes)},
                "referenceResult": {"path": "evidence/reference-result.json", "sha256": digest(reference_bytes)},
                "recipeFiles": {name: digest(data) for name, data in files.items() if name.startswith("evidence/recipe/")
                                or name.startswith("evidence/build-sources/")},
                "runtimeIdentity": identity, "cells": cells}
    files["manifest.json"] = (json.dumps(manifest, indent=2) + "\n").encode("utf-8")
    output.mkdir(parents=True, exist_ok=False)
    for name, data in files.items():
        destination = output / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-evidence", type=Path, required=True)
    parser.add_argument("--reference-result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        freeze(args.build_evidence, args.reference_result, args.output)
    except (ValueError, KeyError, TypeError, OSError) as failure:
        raise SystemExit(str(failure))
    print("Frozen twelve-attempt matrix:", args.output)


if __name__ == "__main__":
    main()
