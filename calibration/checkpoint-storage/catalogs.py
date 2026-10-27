#!/usr/bin/env python3
"""Freeze four checkpoint-storage controls and exactly three attempts per control."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).absolute().parents[2]
RECIPE = Path(__file__).absolute().parent
SCENARIO = "checkpoint-storage-calibration"
CELLS = (("correct-healthy", "correct", "healthy"), ("correct-restart", "correct", "restart"),
         ("correct-eio", "correct", "eio"), ("mutant-eio", "mutant", "eio"))
WORKLOAD = {"keyCount": 8, "totalRecords": 30000, "processingDelayMillis": 5,
            "checkpointIntervalMillis": 10000, "completionTimeoutMillis": 300000,
            "tolerableFailures": 10, "parallelism": 1, "smallFileThreshold": 0, "maxConcurrent": 1}
EXPECTED = ("format: v1\nkind: expected-result\nmeta:\n  name: " + SCENARIO + ".expected\n"
            "  scenario: " + SCENARIO + "\ndefault:\n  outcome: pass\n").encode()
FORBIDDEN = {"Documents", "Photos", "Videos", "Downloads", "Desktop", "Pictures"}
MAX_FILE = 64 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def guarded(path, scope):
    path, scope = Path(path).absolute(), Path(scope).absolute()
    require(".." not in path.parts and path.is_relative_to(scope)
            and not FORBIDDEN.intersection(path.parts), "Path escapes authorized scope")
    cursor = Path(path.anchor)
    for part in path.parts[1:]:
        cursor /= part
        require(not cursor.is_symlink(), "Symlinks are forbidden: " + str(path))
    return path


def read(path, scope, expected=None):
    path = guarded(path, scope)
    require(path.is_file() and path.stat().st_size <= MAX_FILE, "Missing or oversized file: " + str(path))
    data = path.read_bytes()
    if expected is not None:
        require(isinstance(expected, str) and re.fullmatch(r"[0-9a-f]{64}", expected)
                and digest(data) == expected, "Hash mismatch: " + str(path))
    return data


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON key: " + key)
        result[key] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=unique)


def load(path, scope, expected=None):
    return decode(read(path, scope, expected))


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def module(name, path):
    spec = importlib.util.spec_from_file_location("checkpoint_storage_" + name, path)
    result = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = result
    spec.loader.exec_module(result)
    return result


def relative(value):
    require(isinstance(value, str) and value and not Path(value).is_absolute()
            and ".." not in Path(value).parts, "Expected a safe relative path")
    return value


def render(workload_path, helper_path, helper, identity, execution):
    require(execution in {"healthy", "restart", "eio"}, "Unknown calibration execution")
    quote = json.dumps
    for key in ("flinkImageId", "kafkaImageId"):
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", identity[key]), "Unpinned runtime image")
    steps = """      - await:
          condition: { type: job-state, job: state-job, state: RUNNING }
          timeout: 2m
          on_timeout: inconclusive
"""
    if execution == "eio":
        steps += """      - checkpoint_storage_fault: { job: state-job, timeout: 90s, lease: 120s }
"""
    else:
        steps += """      - await:
          condition: { type: checkpoint-completed, job: state-job, count: 1 }
          timeout: 2m
          on_timeout: fail
"""
        if execution == "restart":
            steps += """      - kill:
          target: { kind: named, role: taskmanager, name: taskmanager-1 }
      - restart: { component: taskmanager }
"""
    return f"""format: v1
kind: scenario
meta:
  name: {SCENARIO}
  description: Fixed checkpoint-storage qualification control.
completion_timeout: 5m
runs: 1
health_retry_limit: 0
setup:
  kafka:
    clusters:
      main:
        image: {quote(identity['kafkaImageReference'])}
        image_id: {quote(identity['kafkaImageId'])}
        brokers: 1
        topics:
          - name: input
            partitions: 1
            replication_factor: 1
            input_source: {{ mode: generated, format: integer-sequence, total: 30000 }}
          - {{ name: output, partitions: 1, replication_factor: 1 }}
  flink:
    image: {quote(identity['flinkImageReference'])}
    image_id: {quote(identity['flinkImageId'])}
    jobmanagers: 1
    taskmanagers: 1
subject:
  connectors:
    kafka:
      artifact: maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2
workload:
  jobs:
    - alias: state-job
      jar: {quote(workload_path)}
      connectors: [kafka]
      parallelism: 1
      behavior: {{ type: keyed-prefix-v1, key_count: 8 }}
      state_backend: hashmap
      state_ttl: {{ enabled: false }}
      source: {{ cluster: main, topic: input }}
      sink:
        cluster: main
        topic: output
        delivery_guarantee: EXACTLY_ONCE
        transactional_id_prefix: keyed-prefix
        transaction_id_naming_strategy: INCREMENTING
      checkpointing:
        interval: 10s
        mode: EXACTLY_ONCE
        small_file_threshold: 0
        max_concurrent: 1
        tolerable_failures: 10
        storage:
          type: filesystem
          fuse:
            bundle: {quote(helper_path)}
            manifest_sha256: {quote(helper['manifestSha256'])}
            image_id: {quote(helper['imageId'])}
            platform: {quote(helper['platform'])}
      program_args: [--processingDelayMs, "5"]
phases:
  - name: qualification
    steps:
{steps}terminal_validations:
  - type: kafka.state-prefix
    job: state-job
    cluster: main
    topic: output
    expected: input-manifest
""".encode()


def freeze(parent_manifest, references, output, framework_inputs, *, root=ROOT, recipe=RECIPE, timeout_seconds=900,
           platform_evidence=None):
    root, recipe = Path(root).absolute(), Path(recipe).absolute()
    output = guarded(output, root / "jobs")
    require(not output.exists(), "Output already exists; retained evidence is immutable")
    require(set(references) == {name for name, _, _ in CELLS}, "Exactly four separate pilot references are required")
    require(type(timeout_seconds) is int and 600 <= timeout_seconds <= 1200, "Outer deadline must be 600..1200 seconds")
    parent_manifest = guarded(parent_manifest, root)
    parent = load(parent_manifest, root)
    require(parent["kind"] == "checkpoint-storage-integration-pilots", "Expected frozen PR4 pilot artifacts")
    files = {"evidence/parent-manifest.json": read(parent_manifest, root)}
    identity = parent["runtimeIdentity"]
    artifacts = {}
    for mode in ("correct", "mutant"):
        source = parent["artifacts"][mode]
        name = f"artifacts/{mode}.jar"
        files[name] = read(parent_manifest.parent / relative(source["path"]), root, source["sha256"])
        artifacts[mode] = {"path": name, "sha256": digest(files[name])}
    require(artifacts["correct"]["sha256"] != artifacts["mutant"]["sha256"], "Mutant artifact equals correct artifact")
    helper = dict(parent["helperBuild"])
    helper_root = parent_manifest.parent / relative(helper["path"])
    helper_bytes = read(helper_root / "build-evidence.json", root, helper["manifestSha256"])
    helper_build = decode(helper_bytes)
    require(helper_build["status"] == "built" and helper_build["files"] == helper["files"]
            and helper_build["imageId"] == helper["imageId"] == identity["flinkImageId"]
            and helper_build["platform"] == helper["platform"] == "linux/arm64", "Helper identity differs")
    require(set(helper["files"]) == {"checkpoint-fuse", "checkpoint-fuse-selftest", "probe.jar"}, "Unexpected helper bundle files")
    files["artifacts/helper/build-evidence.json"] = helper_bytes
    for name, value in helper["files"].items():
        files["artifacts/helper/bundle/" + name] = read(helper_root / "bundle" / name, root, value)
    helper = {"path": "artifacts/helper", "manifestSha256": helper["manifestSha256"],
              "imageId": helper["imageId"], "platform": helper["platform"], "files": helper["files"]}
    build_receipt = parent["workloadBuild"]
    build_bytes = read(parent_manifest.parent / relative(build_receipt["path"]), root, build_receipt["sha256"])
    files["evidence/workload-build-evidence.json"] = build_bytes
    build = decode(build_bytes)
    for mode in ("correct", "mutant"):
        source = build["sourceSnapshots"][mode]
        require(source["sha256"], "No frozen workload sources")
        for name, value in source["sha256"].items():
            files[f"evidence/build-sources/{mode}/{relative(name)}"] = read(Path(source["path"]) / name, root, value)
        require(build["artifacts"][mode]["sha256"] == artifacts[mode]["sha256"], "Workload build identity differs")
    helper_source = Path(parent["helperBuild"]["source"])
    if not helper_source.is_absolute():
        helper_source = root / helper_source
    for name, value in helper_build["sourceHashes"].items():
        files["evidence/helper-sources/" + relative(name)] = read(helper_source / "source" / Path(name).name, root, value)
    reference_receipts, reference_sources = {}, {}
    state_check = module("state_reference", root / "calibration/state-recovery/check.py")
    for name, mode, execution in CELLS:
        data = read(references[name], root)
        result = decode(data)
        outcome = "fail" if mode == "mutant" else "pass"
        require(result["status"] == result["attempt"]["status"] == outcome
                and result["expectation"]["outcome"] == "pass", "Pilot outcome differs: " + name)
        require(state_check.runtime_identity(result) == identity, "Pilot runtime identity differs: " + name)
        proof = result["evidence"]
        require(proof["stateRecovery"]["workloadSha256"] == artifacts[mode]["sha256"]
                and proof["stateRecovery"]["confirmed"] is True
                and proof["checkpointStorage"]["helperManifestSha256"] == helper["manifestSha256"], "Pilot artifact/state proof differs")
        require(len(proof.get("taskManagerRestarts", [])) == (0 if execution == "healthy" else 1), "Pilot restart control differs")
        require(len(proof.get("checkpointStorageFaults", [])) == (1 if execution == "eio" else 0), "Pilot fault control differs")
        require(proof["terminalValidation"]["reason"] == "validator.kafka.state-prefix." + ("state-mismatch" if mode == "mutant" else "match"), "Pilot terminal reason differs")
        path = "evidence/references/" + name + ".json"
        files[path] = data
        reference_receipts[name] = {"path": path, "sha256": digest(data)}
        reference_sources[name] = str(guarded(references[name], root).relative_to(root))
    platform_receipt = None
    if platform_evidence is not None:
        platform_bytes = read(platform_evidence, root)
        require(isinstance(decode(platform_bytes), dict), "Platform evidence must be a JSON object")
        files["evidence/platform.json"] = platform_bytes
        platform_receipt = {"path": "evidence/platform.json", "sha256": digest(platform_bytes)}
    for name in ("catalogs.py", "run.py", "check.py", "audit.py", "component_events.py", "report.py", "framework.py", "prepare.py", "ResolveClosure.java", "README.md"):
        files["evidence/recipe/" + name] = read(recipe / name, root)
    files["evidence/recipe/state_recovery_check.py"] = read(root / "calibration/state-recovery/check.py", root)
    cells = []
    for name, mode, execution in CELLS:
        catalog = "catalogs/" + name
        scenario = render("./" + artifacts[mode]["path"], "./artifacts/helper", helper, identity, execution)
        files[f"{catalog}/{SCENARIO}.yaml"] = scenario
        files[f"{catalog}/{SCENARIO}.expected.yaml"] = EXPECTED
        cells.append({"name": name, "mode": mode, "execution": execution, "scenario": SCENARIO, "catalog": catalog,
                      "scenarioSha256": digest(scenario), "expectedSha256": digest(EXPECTED), "requiredOutcome": "pass",
                      "expectedRawOutcome": "fail" if mode == "mutant" else "pass",
                      "expectedRawReason": "validator.kafka.state-prefix." + ("state-mismatch" if mode == "mutant" else "match"),
                      "attempts": [f"attempts/{name}/run-{number}" for number in range(1, 4)]})
    output.mkdir(parents=True, exist_ok=False)
    for name, data in files.items():
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(data)
        if name.endswith(("/checkpoint-fuse", "/checkpoint-fuse-selftest")):
            path.chmod(0o755)
    framework = module("framework", recipe / "framework.py")
    framework.freeze_framework(output / "framework", source_root=root, **framework_inputs)
    framework_bytes = read(output / "framework/manifest.json", output)
    manifest = {"formatVersion": 1, "kind": "checkpoint-storage-calibration", "attemptsPerCell": 3,
                "automaticRetries": 0, "outerTimeoutSeconds": timeout_seconds, "workload": WORKLOAD,
                "artifacts": artifacts, "helperBundle": helper, "runtimeIdentity": identity,
                "buildEvidence": {"path": "evidence/workload-build-evidence.json", "sha256": digest(build_bytes)},
                "references": reference_receipts, "referenceSources": reference_sources,
                "frameworkFreeze": {"path": "framework/manifest.json", "sha256": digest(framework_bytes)},
                "recipeFiles": {name: digest(data) for name, data in files.items() if name.startswith("evidence/recipe/")
                                or name.startswith("evidence/build-sources/") or name.startswith("evidence/helper-sources/")},
                "frozenFiles": {name: digest(data) for name, data in files.items()}, "cells": cells}
    if platform_receipt is not None:
        manifest["platformEvidence"] = platform_receipt
    with (output / "manifest.json").open("xb") as handle:
        handle.write(encoded(manifest))
    return manifest


def validate_frozen(batch):
    batch = guarded(batch, batch)
    manifest = load(batch / "manifest.json", batch)
    require(manifest["formatVersion"] == 1 and manifest["kind"] == "checkpoint-storage-calibration"
            and manifest["attemptsPerCell"] == 3 and manifest["automaticRetries"] == 0
            and manifest["workload"] == WORKLOAD, "Unexpected fixed matrix contract")
    require(type(manifest["outerTimeoutSeconds"]) is int and 600 <= manifest["outerTimeoutSeconds"] <= 1200, "Invalid deadline")
    require([(cell["name"], cell["mode"], cell["execution"]) for cell in manifest["cells"]] == list(CELLS), "Matrix cells differ")
    for cell in manifest["cells"]:
        name, mode = cell["name"], cell["mode"]
        require(cell["attempts"] == [f"attempts/{name}/run-{i}" for i in range(1, 4)]
                and cell["scenario"] == SCENARIO and cell["catalog"] == "catalogs/" + name
                and cell["requiredOutcome"] == "pass"
                and cell["expectedRawOutcome"] == ("fail" if mode == "mutant" else "pass")
                and cell["expectedRawReason"] == "validator.kafka.state-prefix." + ("state-mismatch" if mode == "mutant" else "match"),
                "Attempt allocation differs")
        scenario = read(batch / cell["catalog"] / (SCENARIO + ".yaml"), batch, cell["scenarioSha256"])
        require(scenario == render("./" + manifest["artifacts"][mode]["path"], "./artifacts/helper",
                                   manifest["helperBundle"], manifest["runtimeIdentity"], cell["execution"]), "Frozen scenario differs")
        require(read(batch / cell["catalog"] / (SCENARIO + ".expected.yaml"), batch, cell["expectedSha256"]) == EXPECTED,
                "Catalog expectation must remain PASS")
    for name, value in manifest["frozenFiles"].items():
        read(batch / relative(name), batch, value)
    required_recipe = {"evidence/recipe/" + name for name in
                       ("catalogs.py", "run.py", "check.py", "audit.py", "component_events.py", "report.py", "framework.py", "prepare.py", "ResolveClosure.java", "README.md", "state_recovery_check.py")}
    require(required_recipe.issubset(manifest["recipeFiles"]), "Frozen recipe is incomplete")
    for name, value in manifest["recipeFiles"].items():
        require(manifest["frozenFiles"].get(name) == value, "Recipe pin differs")
        read(batch / relative(name), batch, value)
    for receipt in [*manifest["artifacts"].values(), *manifest["references"].values(), manifest["frameworkFreeze"], manifest["buildEvidence"]]:
        read(batch / relative(receipt["path"]), batch, receipt["sha256"])
    if "referenceSources" in manifest:
        require(set(manifest["referenceSources"]) == {name for name, _, _ in CELLS}, "Reference source mapping differs")
        for name in manifest["referenceSources"].values():
            relative(name)
    if "platformEvidence" in manifest:
        receipt = manifest["platformEvidence"]
        require(set(receipt) == {"path", "sha256"} and receipt["path"] == "evidence/platform.json", "Invalid platform receipt")
        require(isinstance(load(batch / receipt["path"], batch, receipt["sha256"]), dict), "Invalid platform evidence")
    helper = manifest["helperBundle"]
    read(batch / relative(helper["path"]) / "build-evidence.json", batch, helper["manifestSha256"])
    for name, value in helper["files"].items():
        read(batch / helper["path"] / "bundle" / relative(name), batch, value)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-manifest", type=Path, required=True)
    parser.add_argument("--references", type=Path, required=True, help="JSON map of the four cell names to separate pilot result paths")
    parser.add_argument("--framework-inputs", type=Path, required=True, help="JSON arguments for framework.freeze_framework")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--platform-evidence", type=Path, help="Explicit local JSON receipt for Docker engine/kernel and host/Python versions")
    args = parser.parse_args()
    inputs = load(args.framework_inputs, ROOT)
    for key in ("repository", "classpath_file", "java"):
        inputs[key] = Path(inputs[key])
    inputs["artifact_files"] = [Path(value) for value in inputs["artifact_files"]]
    freeze(args.parent_manifest, load(args.references, ROOT), args.output, inputs, timeout_seconds=args.timeout_seconds,
           platform_evidence=args.platform_evidence)
    print("Frozen twelve attempts:", args.output)


if __name__ == "__main__":
    main()
