#!/usr/bin/env python3
"""Independently audit exactly twelve frozen checkpoint-storage attempts; never execute or repair them."""
import argparse
from datetime import datetime
import importlib.util
from io import BytesIO
import json
import math
from pathlib import Path
import re
from zipfile import ZipFile, BadZipFile
from urllib.parse import urlsplit


def module(name, path):
    spec = importlib.util.spec_from_file_location("checkpoint_storage_qualification_" + name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


HERE = Path(__file__).absolute().parent
audit = module("native", HERE / "audit.py")
framework = module("framework", HERE / "framework.py")
inventory = module("inventory", HERE / "inventory.py")
STATE_SOURCE = HERE / "state_recovery_check.py"
if not STATE_SOURCE.exists():
    STATE_SOURCE = HERE.parent / "state-recovery/check.py"
state = module("state", STATE_SOURCE)
SCENARIO = "checkpoint-storage-calibration"
state.SCENARIO = SCENARIO
CELLS = (("correct-healthy", "correct", "healthy"), ("correct-restart", "correct", "restart"),
         ("correct-eio", "correct", "eio"), ("mutant-eio", "mutant", "eio"))
WORKLOAD = {"keyCount": 8, "totalRecords": 30000, "processingDelayMillis": 5,
            "checkpointIntervalMillis": 10000, "completionTimeoutMillis": 300000,
            "tolerableFailures": 10, "parallelism": 1, "smallFileThreshold": 0, "maxConcurrent": 1}
EXPECTED = ("format: v1\nkind: expected-result\nmeta:\n  name: " + SCENARIO + ".expected\n"
            "  scenario: " + SCENARIO + "\ndefault:\n  outcome: pass\n").encode()
require, read, parse, digest = audit.require, audit.read, audit.parse, audit.sha


def receipt(batch, value):
    require(isinstance(value, dict) and set(value) == {"path", "sha256"}, "Malformed frozen file receipt")
    return read(batch, value["path"], value["sha256"])


def scenario_contract(data, cell, manifest):
    """An independent closed list of qualification fields; no YAML aliases or extra overrides."""
    common = '''format: v1
kind: scenario
meta:
  name: checkpoint-storage-calibration
  description: Fixed checkpoint-storage qualification control.
completion_timeout: 5m
runs: 1
health_retry_limit: 0
setup:
  kafka:
    clusters:
      main:
        image: @KAFKA@
        image_id: @KAFKA_ID@
        brokers: 1
        topics:
          - name: input
            partitions: 1
            replication_factor: 1
            input_source: { mode: generated, format: integer-sequence, total: 30000 }
          - { name: output, partitions: 1, replication_factor: 1 }
  flink:
    image: @FLINK@
    image_id: @FLINK_ID@
    jobmanagers: 1
    taskmanagers: 1
subject:
  connectors:
    kafka:
      artifact: maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2
workload:
  jobs:
    - alias: state-job
      jar: @JAR@
      connectors: [kafka]
      parallelism: 1
      behavior: { type: keyed-prefix-v1, key_count: 8 }
      state_backend: hashmap
      state_ttl: { enabled: false }
      source: { cluster: main, topic: input }
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
            bundle: "./artifacts/helper"
            manifest_sha256: @HELPER@
            image_id: @HELPER_IMAGE@
            platform: "linux/arm64"
      program_args: [--processingDelayMs, "5"]
phases:
  - name: qualification
    steps:
      - await:
          condition: { type: job-state, job: state-job, state: RUNNING }
          timeout: 2m
          on_timeout: inconclusive
'''
    identity = manifest["runtimeIdentity"]
    substitutions = {"@KAFKA@": identity["kafkaImageReference"], "@KAFKA_ID@": identity["kafkaImageId"],
                     "@FLINK@": identity["flinkImageReference"], "@FLINK_ID@": identity["flinkImageId"],
                     "@JAR@": "./" + manifest["artifacts"][cell["mode"]]["path"],
                     "@HELPER@": manifest["helperBundle"]["manifestSha256"], "@HELPER_IMAGE@": manifest["helperBundle"]["imageId"]}
    for token, value in substitutions.items():
        common = common.replace(token, json.dumps(value))
    if cell["mode"] == "mutant":
        common = common.replace("\nmeta:\n", "\nmeta:\n  allow_state_reset_mutant: true\n")
    if cell["execution"] == "eio":
        common += "      - checkpoint_storage_fault: { job: state-job, timeout: 90s, lease: 120s }\n"
    else:
        common += "      - await:\n          condition: { type: checkpoint-completed, job: state-job, count: 1 }\n          timeout: 2m\n          on_timeout: fail\n"
        if cell["execution"] == "restart":
            common += "      - kill:\n          target: { kind: named, role: taskmanager, name: taskmanager-1 }\n      - restart: { component: taskmanager }\n"
    common += "terminal_validations:\n  - type: kafka.state-prefix\n    job: state-job\n    cluster: main\n    topic: output\n    expected: input-manifest\n"
    require(data == common.encode(), "Scenario differs from the independent fixed qualification contract")


def verifier_identity():
    paths = {"check.py": Path(__file__), "audit.py": HERE / "audit.py",
             "framework.py": HERE / "framework.py", "inventory.py": HERE / "inventory.py",
             "raw_evidence.py": HERE / "raw_evidence.py", "state_recovery_check.py": STATE_SOURCE}
    component_parser = HERE / "component_events.py"
    if component_parser.exists():
        paths["component_events.py"] = component_parser
    return {name: digest(read(path.parent, path.name)) for name, path in paths.items()}


def verify_recipe_identity(recipes, *, re_audit=False):
    """Re-audit is explicit; original frozen files and artifacts are always checked."""
    current = verifier_identity()
    if not re_audit:
        for name, value in current.items():
            require(value == recipes.get("evidence/recipe/" + name),
                    "Checker differs from frozen recipe; run the retained checker or explicitly use --re-audit: " + name)
    return current


def validate_manifest(batch, *, re_audit=False):
    manifest = parse(read(batch, "manifest.json"))
    require(manifest["formatVersion"] == 1 and manifest.get("catalogContractVersion") == 2
            and manifest["kind"] == "checkpoint-storage-calibration"
            and manifest["attemptsPerCell"] == 3 and manifest["automaticRetries"] == 0
            and manifest["workload"] == WORKLOAD, "Wrong fixed matrix contract")
    require(type(manifest["outerTimeoutSeconds"]) is int and 600 <= manifest["outerTimeoutSeconds"] <= 1200,
            "Invalid frozen outer deadline")
    require([(row["name"], row["mode"], row["execution"]) for row in manifest["cells"]] == list(CELLS),
            "Missing, duplicated or reordered matrix cells")
    for cell in manifest["cells"]:
        name, mode = cell["name"], cell["mode"]
        require(cell["scenario"] == SCENARIO and cell["catalog"] == "catalogs/" + name
                and cell["attempts"] == [f"attempts/{name}/run-{number}" for number in range(1, 4)]
                and cell["requiredOutcome"] == "pass" and cell["expectedRawOutcome"] == ("fail" if mode == "mutant" else "pass")
                and cell["expectedRawReason"] == (state.MISMATCH if mode == "mutant" else state.MATCH), "Wrong fixed attempt allocation or expectation")
        scenario_contract(read(batch, cell["catalog"] + "/" + SCENARIO + ".yaml", cell["scenarioSha256"]), cell, manifest)
        require(read(batch, cell["catalog"] + "/" + SCENARIO + ".expected.yaml", cell["expectedSha256"]) == EXPECTED,
                "Catalog expectation was changed from PASS")
    frozen = manifest["frozenFiles"]
    require(isinstance(frozen, dict) and 1 <= len(frozen) <= 2048, "Invalid frozen inventory")
    for path, value in frozen.items():
        read(batch, path, value)
    if "platformEvidence" in manifest:
        require(isinstance(parse(receipt(batch, manifest["platformEvidence"])), dict), "Platform receipt is not an object")
    if "referenceSources" in manifest:
        require(set(manifest["referenceSources"]) == {row[0] for row in CELLS}, "Reference source inventory differs")
        for source in manifest["referenceSources"].values():
            require(isinstance(source, str) and not Path(source).is_absolute() and ".." not in Path(source).parts,
                    "Invalid reference source label")
    recipes = manifest["recipeFiles"]
    required = {"evidence/recipe/" + name for name in
                ("check.py", "audit.py", "raw_evidence.py", "state_recovery_check.py", "framework.py", "inventory.py", "catalogs.py", "run.py", "README.md",
                 "prepare.py", "ResolveClosure.java", "report.py")}
    if not re_audit:
        required.add("evidence/recipe/component_events.py")
    require(required <= recipes.keys() and all(frozen.get(path) == value for path, value in recipes.items()), "Frozen checker recipe is incomplete")
    # An updated verifier may inspect historical evidence only in explicit re-audit mode.
    # This never substitutes current runtime binaries for the recorded framework.
    verify_recipe_identity(recipes, re_audit=re_audit)
    build = parse(receipt(batch, manifest["buildEvidence"]))
    require(build["version"] == 1 and build["mutation"] == state.MUTATION and build["releaseBytecodeTarget"] == 11,
            "Wrong workload mutation build contract")
    require(set(manifest["artifacts"]) == {"correct", "mutant"}, "Missing workload arm")
    jars = {}
    for mode, value in manifest["artifacts"].items():
        require(value["sha256"] == build["artifacts"][mode]["sha256"], "Workload differs from build receipt")
        with ZipFile(BytesIO(receipt(batch, value))) as jar:
            names = jar.namelist()
            require(len(names) == len(set(names)) and len(names) <= 1024
                    and sum(row.file_size for row in jar.infolist()) <= audit.FILE_BYTES, "Duplicate or oversized workload JAR")
            jars[mode] = {name: jar.read(name) for name in names if not name.endswith("/")}
        expected_mode = "correct" if mode == "correct" else state.MUTATION
        require(jars[mode].get("META-INF/flink-stability/keyed-prefix-build.properties") == ("mode=" + expected_mode + "\n").encode(),
                "Wrong immutable workload mode")
        for name, value in build["sourceSnapshots"][mode]["sha256"].items():
            require(frozen.get("evidence/build-sources/" + mode + "/" + name) == value, "Frozen workload source is missing")
    require(set(jars["correct"]) == set(jars["mutant"]), "Workload JAR entries differ")
    differences = sorted(name for name in jars["correct"] if jars["correct"][name] != jars["mutant"][name])
    require(differences == build["changedEntries"] == sorted(["META-INF/MANIFEST.MF", "META-INF/flink-stability/keyed-prefix-build.properties"]),
            "Mutation changed source/sink or class bytes")
    helper = manifest["helperBundle"]
    hb = parse(read(batch, helper["path"] + "/build-evidence.json", helper["manifestSha256"]))
    require(helper["path"] == "artifacts/helper" and helper["platform"] == hb["platform"] == "linux/arm64"
            and helper["imageId"] == hb["imageId"] == manifest["runtimeIdentity"]["flinkImageId"]
            and hb["status"] == "built" and hb["files"] == helper["files"]
            and set(helper["files"]) == {"checkpoint-fuse", "checkpoint-fuse-selftest", "probe.jar"}, "Helper bundle identity differs")
    for name, value in helper["files"].items():
        read(batch, helper["path"] + "/bundle/" + name, value)
    for name, value in hb["sourceHashes"].items():
        require(frozen.get("evidence/helper-sources/" + name) == value, "Frozen helper source is missing")
    require(manifest["frameworkFreeze"]["path"] == "framework/manifest.json", "Wrong framework manifest path")
    framework_manifest = parse(receipt(batch, manifest["frameworkFreeze"]))
    inventory.validate(batch, frozen, framework_manifest["files"])
    framework.validate_framework(Path(batch) / "framework")
    require(set(manifest["references"]) == {row[0] for row in CELLS}, "Missing separate pilot references")
    reference_nonces = set()
    for name, mode, unused in CELLS:
        reference = parse(receipt(batch, manifest["references"][name]))
        require(state.runtime_identity(reference) == manifest["runtimeIdentity"]
                and reference["evidence"]["stateRecovery"]["workloadSha256"] == manifest["artifacts"][mode]["sha256"]
                and reference["evidence"]["stateRecovery"].get("allowStateResetMutant") is (mode == "mutant")
                and reference["evidence"]["stateRecovery"].get("buildMode") == (state.MUTATION if mode == "mutant" else "correct")
                and reference["status"] == ("fail" if mode == "mutant" else "pass"), "Reference runtime or raw outcome differs")
        reference_nonces.add(reference["attempt"]["nonce"])
    require(len(reference_nonces) == 4, "Reference pilots are not separate attempts")
    return manifest


def execution_interval(execution, maximum, label):
    require(type(execution.get("startedAt")) is str and type(execution.get("finishedAt")) is str,
            label + ": missing execution timestamps")
    try:
        start = datetime.fromisoformat(execution["startedAt"])
        finish = datetime.fromisoformat(execution["finishedAt"])
    except ValueError as failure:
        raise ValueError(label + ": invalid execution timestamp") from failure
    elapsed = execution.get("elapsedSeconds")
    require(start.tzinfo is not None and finish.tzinfo is not None and finish > start
            and type(elapsed) in (int, float) and math.isfinite(elapsed) and 0 < elapsed <= maximum,
            label + ": execution interval or deadline is invalid")
    # The runner samples wall and monotonic clocks consecutively at each boundary. Allow
    # one second for that sampling skew; larger clock jumps cannot establish chronology.
    require(abs((finish - start).total_seconds() - elapsed) <= 1,
            label + ": wall-clock interval differs from monotonic elapsed time")
    return start, finish


def validate_execution_order(batch, manifest, validations):
    """Check recorded wall-clock intervals; ordered path names alone are insufficient."""
    require([value["cell"] for value in validations] == [cell["name"] for cell in manifest["cells"]],
            "Live execution has no complete preflight chronology")
    previous_finish = None
    for validation in validations:
        start, finish = execution_interval(validation, 120, "Validation " + validation["cell"])
        require(previous_finish is None or start >= previous_finish, "Preflight validations overlap or are reordered")
        previous_finish = finish
    intervals = []
    for cell in manifest["cells"]:
        for relative in cell["attempts"]:
            execution_bytes = read(batch, relative + "/execution.json")
            execution = parse(execution_bytes)
            start, finish = execution_interval(execution, manifest["outerTimeoutSeconds"], relative)
            require(previous_finish is not None and start >= previous_finish,
                    relative + ": live execution overlaps or precedes preflight/previous attempt")
            intervals.append({"directory": relative, "startedAt": execution["startedAt"],
                              "finishedAt": execution["finishedAt"], "executionSha256": digest(execution_bytes)})
            previous_finish = finish
    return {"confirmed": True, "basis": "recorded-offset-aware-wall-clock-intervals",
            "preflightFinishedAt": validations[-1]["finishedAt"], "attempts": intervals}


def execution_receipts(batch, directory, cell, manifest, code):
    command_bytes = read(directory, "command.json")
    command, execution = parse(command_bytes), parse(read(directory, "execution.json"))
    require(execution["formatVersion"] == 1 and execution["started"] is True and execution["completed"] is True
            and execution["termination"] == "exited" and execution["exitCode"] == code
            and execution["retentionComplete"] is True and execution["retentionErrors"] == []
            and execution["timeoutSeconds"] == manifest["outerTimeoutSeconds"]
            and execution["frameworkManifestSha256"] == command["frameworkManifestSha256"] == manifest["frameworkFreeze"]["sha256"]
            and execution["commandSha256"] == digest(command_bytes), "Execution was interrupted, unretained or used another framework")
    execution_interval(execution, manifest["outerTimeoutSeconds"], str(directory))
    environment = command["environment"]
    require(command["cwd"] == str(directory) and environment["HOME"] == str(directory / "home")
            and environment["DOCKER_CONFIG"] == str(directory / "home/.docker")
            and environment["TMPDIR"] == str(directory / "tmp") and environment["DOCKER_HOST"].startswith("unix:///"),
            "Launch did not use isolated local runtime directories")
    launch_configuration(directory, command)
    arguments = ["run", "--catalog-root", str(batch / cell["catalog"]), "--scenario", SCENARIO,
                 "--artifact-root", str(batch), "--offline", "--kafka-log-output", str(directory / "kafka-logs")]
    expected = framework.command(batch / "framework", arguments, environment["DOCKER_API_VERSION"], work_directory=directory)
    require(command["argv"] == expected, "Actual launch command differs from frozen direct-Java command")
    require(command["settingsSha256"] == digest(read(directory, "settings.xml"))
            and command["dockerConfigSha256"] == digest(read(directory, "home/.docker/config.json")), "Isolated launch configuration receipt changed")
    return execution


def launch_configuration(directory, command):
    environment = command["environment"]
    endpoint = environment["DOCKER_HOST"]
    address = urlsplit(endpoint)
    require(address.scheme == "unix" and not address.netloc and address.path.startswith("/")
            and not address.query and not address.fragment and ".." not in Path(address.path).parts,
            "Launch Docker endpoint is not a local Unix socket")
    require(re.fullmatch(r"[0-9]+\.[0-9]+", environment["DOCKER_API_VERSION"]), "Invalid Docker API version")
    require(environment == {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(directory / "home"),
                           "TMPDIR": str(directory / "tmp"), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC",
                           "DOCKER_CONFIG": str(directory / "home/.docker"), "DOCKER_HOST": endpoint,
                           "DOCKER_API_VERSION": environment["DOCKER_API_VERSION"]}, "Launch environment contains overrides or differs from isolated recipe")
    require(read(directory, "settings.xml") == b'<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"/>\n'
            and read(directory, "home/.docker/config.json") == b'{"auths":{}}\n', "Launch settings/config are not exactly empty")


def validate_catalog_receipts(batch, manifest):
    """Require all four real offline validations before accepting matrix evidence."""
    started = parse(read(batch, "validation-started.json"))
    finished = parse(read(batch, "validation-finished.json"))
    launch = parse(read(batch, "execution-started.json"))
    names = [cell["name"] for cell in manifest["cells"]]
    require(started["formatVersion"] == finished["formatVersion"] == 1
            and started["manifestSha256"] == digest(read(batch, "manifest.json"))
            and started["frameworkManifestSha256"] == manifest["frameworkFreeze"]["sha256"]
            and finished["complete"] is True and finished["validated"] == names,
            "Four frozen catalogs were not successfully validated")
    directory = audit.path_under(batch, "validation")
    require(directory.is_dir(), "Missing catalog validation namespace")
    actual = set()
    for child in directory.iterdir():
        require(not child.is_symlink() and child.is_dir(), "Unexpected catalog validation namespace entry")
        actual.add(child.name)
    require(actual == set(names), "Missing or extra catalog validation attempts")
    receipts, previous_finish = [], None
    for cell in manifest["cells"]:
        directory = audit.path_under(batch, "validation/" + cell["name"])
        command_bytes = read(directory, "command.json")
        command, execution = parse(command_bytes), parse(read(directory, "execution.json"))
        require(execution["formatVersion"] == 1 and execution["started"] is True
                and execution["completed"] is True and execution["termination"] == "exited"
                and type(execution["exitCode"]) is int and execution["exitCode"] == 0
                and read(directory, "exit-code.txt").strip() == b"0" and execution["timeoutSeconds"] == 120
                and execution["frameworkManifestSha256"] == command["frameworkManifestSha256"] == manifest["frameworkFreeze"]["sha256"]
                and execution["commandSha256"] == digest(command_bytes),
                "Catalog validation failed, timed out or used another framework")
        start, finish = execution_interval(execution, 120, "Validation " + cell["name"])
        require(previous_finish is None or start >= previous_finish, "Catalog validation serial order differs")
        previous_finish = finish
        launch_configuration(directory, command)
        environment = command["environment"]
        require(command["cwd"] == str(directory)
                and environment["DOCKER_HOST"] == launch["dockerEndpoint"]
                and environment["DOCKER_API_VERSION"] == launch["dockerApiVersion"],
                "Catalog validation launch differs from matrix endpoint or directory")
        arguments = ["validate", "--catalog-root", str(batch / cell["catalog"]), "--scenario", SCENARIO,
                     "--artifact-root", str(batch), "--offline", "--show-plan"]
        require(command["argv"] == framework.command(batch / "framework", arguments,
                    environment["DOCKER_API_VERSION"], work_directory=directory),
                "Catalog validation command differs from frozen direct-Java command")
        require(command["settingsSha256"] == digest(read(directory, "settings.xml"))
                and command["dockerConfigSha256"] == digest(read(directory, "home/.docker/config.json")),
                "Catalog validation configuration receipt changed")
        output, errors = read(directory, "stdout.json"), read(directory, "stderr.log")
        require(output.decode("utf-8").strip(), "Catalog validation did not retain its resolved plan")
        receipts.append({"cell": cell["name"], "stdoutSha256": digest(output), "stderrSha256": digest(errors),
                         "commandSha256": digest(command_bytes), "exitCode": 0,
                         "startedAt": execution["startedAt"], "finishedAt": execution["finishedAt"],
                         "elapsedSeconds": execution["elapsedSeconds"],
                         "executionSha256": digest(read(directory, "execution.json"))})
    return receipts


def raw_subject_and_events(directory, result, retained):
    evidence = result["evidence"]
    event_parser = module("component_events", HERE / "component_events.py")
    origins, hashes = evidence["subjectClasses"], {}
    physical = evidence["flinkRuntime"]["components"]
    require(isinstance(physical, list) and 0 < len(physical) <= event_parser.MAX_FILES,
            "Missing or oversized physical component inventory")
    process_names = [value["classLoadProcess"] for value in physical]
    require(all(isinstance(name, str) and event_parser.PROCESS.fullmatch(name) for name in process_names)
            and len(set(process_names)) == len(process_names), "Invalid or duplicate physical component process")
    require(all(value["classLoadProcess"].split("#")[0] == value["logicalName"] for value in physical),
            "Physical component process differs from logical identity")
    origin_names = [value["process"] for value in origins["processes"]]
    require(len(origin_names) == len(set(origin_names)) and set(origin_names) == set(process_names),
            "Subject and component process inventories differ")
    expected_output_paths = {"logs/flink-stability-output-" + name.replace("#", "-") + ".log"
                             for name in process_names}
    actual_output_paths = {name for name in retained.local if name.startswith("logs/flink-stability-output-")}
    require(actual_output_paths == expected_output_paths,
            "Retained component output inventory is incomplete or contains an unknown process")
    logs, output_bytes = [], 0
    for name in process_names:
        path = "logs/flink-stability-class-load-" + name.replace("#", "-") + ".log"
        require(path in retained.local, "Physical class-loading log was not retained")
        data = read(directory, path, retained.local[path]["sha256"])
        hashes[path] = digest(data)
        for entry in state.ENTRY_CLASSES:
            matches = re.findall(r"\[class,load\]\s+" + re.escape(entry) + r" source: ([^\r\n]+)", data.decode("utf-8"))
            if name.startswith("taskmanager-"):
                require(matches and set(matches) == {"file:" + origins["expectedSource"]}, "Raw TaskManager class origin differs from subject receipt")
        output_path = "logs/flink-stability-output-" + name.replace("#", "-") + ".log"
        output = read(directory, output_path, retained.local[output_path]["sha256"], event_parser.MAX_FILE_BYTES)
        output_bytes += len(output)
        require(output_bytes <= event_parser.MAX_TOTAL_BYTES, "Component aggregate capture byte budget exceeded")
        logs.append((name, output))
    component_audit = event_parser.audit_retained(evidence["componentErrors"], logs)
    return {"classLogSha256": hashes, "componentEventAudit": component_audit}


def audit_attempt(result, exit_code, cell, manifest, directory):
    retained = audit.RetainedEvidence(directory)
    subject = raw_subject_and_events(directory, result, retained)
    try:
        return audit_attempt_contract(result, exit_code, cell, manifest, directory, retained, subject)
    except (ValueError, KeyError, TypeError, IndexError, OSError, StopIteration, BadZipFile) as failure:
        # Independent log observations survive failures in the other evidence gates.
        failure.component_event_audit = subject["componentEventAudit"]
        raise


def audit_attempt_contract(result, exit_code, cell, manifest, directory, retained, subject):
    rest = result["evidence"]["flinkRest"]
    require(audit.integer(rest["errorCount"]) and rest["errorCount"] == len(rest["errors"]), "REST error count is inconsistent")
    generic_cell = dict(cell, execution="uninterrupted" if cell["execution"] == "healthy" else "restart")
    row = state.audit_attempt(result, exit_code, generic_cell, manifest)
    row["logSha256"] = state.verify_marker_logs(directory, result["evidence"]["stateRecovery"]["trace"]["markers"])
    row.update(subject)
    row["rawEvidenceAudit"] = module("raw_evidence", HERE / "raw_evidence.py").audit(result, retained)
    row["checkpointStorage"] = audit.audit_storage(result, cell, manifest, retained)
    endpoint = parse(read(directory, "command.json"))["environment"]["DOCKER_HOST"]
    row["cleanup"] = audit.cleanup_receipt(directory, result, endpoint)
    declared = parse(read(directory, "input-manifest.json"))
    total = manifest["workload"]["totalRecords"]
    require(declared["version"] == 1 and declared["kind"] == "reconstructed-declared-integer-sequence"
            and declared["firstId"] == 0 and declared["endExclusive"] == declared["records"] == total
            and declared["inputReceiptComplete"] is True and declared["path"] == "input-manifest.txt",
            "Declared input reconstruction differs from frozen input contract")
    require(read(directory, declared["path"], declared["sha256"]) == b"".join((str(i) + "\n").encode() for i in range(total)),
            "Declared input sequence differs")
    input_receipt = parse(read(directory, "input-evidence.json"))
    require(input_receipt["input"] == result["evidence"]["input"], "Retained input receipt differs")
    configuration = parse(read(directory, "configuration-evidence.json"))
    runtime = result["evidence"]["flinkRuntime"]
    require(configuration["config"] == runtime["config"] and configuration["components"] == [
        {"runtimeId": value["runtimeId"], "effectiveConfiguration": value["effectiveConfiguration"]}
        for value in runtime["components"]], "Retained configuration receipt differs")
    row["inputManifest"] = declared
    return row


def check_matrix(batch, *, re_audit=False):
    batch = Path(batch).absolute()
    manifest = validate_manifest(batch, re_audit=True) if re_audit else validate_manifest(batch)
    report = {"qualified": False, "manifestSha256": digest(read(batch, "manifest.json")),
              "frameworkManifestSha256": manifest["frameworkFreeze"]["sha256"],
              "confirmedCounts": {name: 0 for name, unused, ignored in CELLS}, "attempts": [], "errors": [],
              "componentEventCount": 0, "restErrorCount": 0, "cleanupConfirmedCount": 0,
              "retainedComponentEventCount": 0, "componentEventAuditConfirmedCount": 0,
              "componentEventAuditAttemptCount": 0}
    report["verificationMode"] = "independent-re-audit" if re_audit else "frozen-replay"
    report["verifierSha256"] = verifier_identity()
    if re_audit:
        report["reAudit"] = {"scope": "Recorded runtime artifacts only; this does not qualify changed Java runtime code",
                             "originalRecipeSha256": manifest["recipeFiles"],
                             "changedVerifierFiles": [name for name, value in report["verifierSha256"].items()
                                if manifest["recipeFiles"].get("evidence/recipe/" + name) != value]}
    allocated = [path for cell in manifest["cells"] for path in cell["attempts"]]
    try:
        report["catalogValidations"] = validate_catalog_receipts(batch, manifest)
        report["executionOrder"] = validate_execution_order(batch, manifest, report["catalogValidations"])
        start = parse(read(batch, "execution-started.json"))
        finish = parse(read(batch, "execution-finished.json"))
        require(start["attempts"] == 12 and start["automaticRetries"] == finish["automaticRetries"] == 0
                and start["manifestSha256"] == report["manifestSha256"]
                and start["frameworkManifestSha256"] == manifest["frameworkFreeze"]["sha256"]
                and finish["complete"] is True and finish["stoppedAt"] is None and finish["attempted"] == allocated,
                "Matrix was incomplete, retried or reordered")
        actual = set()
        for cell_dir in audit.path_under(batch, "attempts").iterdir():
            require(not cell_dir.is_symlink() and cell_dir.is_dir(), "Unexpected attempt namespace entry")
            for attempt_dir in cell_dir.iterdir():
                require(not attempt_dir.is_symlink() and attempt_dir.is_dir(), "Unexpected attempt namespace entry")
                actual.add(str(attempt_dir.relative_to(batch)))
        require(actual == set(allocated), "Extra or missing attempt directories; fixed matrix cannot be cherry-picked")
    except (ValueError, KeyError, TypeError, OSError) as failure:
        report["errors"].append({"matrix": str(failure)})
    nonces = {parse(receipt(batch, value))["attempt"]["nonce"] for value in manifest["references"].values()}
    jobs, storage_ids, runtime_ids = set(), set(), set()
    for cell in manifest["cells"]:
        for number, relative in enumerate(cell["attempts"], 1):
            row = {"cell": cell["name"], "number": number, "directory": relative, "confirmed": False}
            try:
                directory = audit.path_under(batch, relative)
                raw_bytes = read(directory, "stdout.json")
                raw = parse(raw_bytes)
                evidence = raw.get("evidence", {})
                row.update(stdoutSha256=digest(raw_bytes), rawStatus=raw.get("attempt", {}).get("status"),
                           rawReason=raw.get("attempt", {}).get("reason"), status=raw.get("status"), reason=raw.get("reason"),
                           terminalValidation=evidence.get("terminalValidation"), componentErrors=evidence.get("componentErrors"),
                           flinkRest=evidence.get("flinkRest"), diagnostics=raw.get("diagnostics"))
                for source, target, field in (("componentErrors", "componentEventCount", "eventCount"), ("flinkRest", "restErrorCount", "errorCount")):
                    value = evidence.get(source, {}).get(field)
                    if audit.integer(value):
                        report[target] += value
                row["stderrSha256"] = digest(read(directory, "stderr.log"))
                code = int(read(directory, "exit-code.txt"))
                row["exitCode"] = code
                row["execution"] = execution_receipts(batch, directory, cell, manifest, code)
                command = parse(read(directory, "command.json"))
                start = parse(read(batch, "execution-started.json"))
                require(command["environment"]["DOCKER_HOST"] == start["dockerEndpoint"]
                        and command["environment"]["DOCKER_API_VERSION"] == start["dockerApiVersion"], "Attempt Docker endpoint differs from matrix launch")
                nonce, job = raw["attempt"]["nonce"], evidence["stateRecovery"]["jobId"]
                storage_id = evidence["checkpointStorage"]["storageId"]
                ids = {value["runtimeId"] for value in evidence["flinkRuntime"]["components"]}
                require(isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{8}", nonce) and nonce not in nonces
                        and job not in jobs and storage_id not in storage_ids and not ids.intersection(runtime_ids),
                        "Attempt, job, storage or process identity was reused")
                nonces.add(nonce); jobs.add(job); storage_ids.add(storage_id); runtime_ids.update(ids)
                row.update(audit_attempt(raw, code, cell, manifest, directory))
                report["confirmedCounts"][cell["name"]] += 1
                report["cleanupConfirmedCount"] += 1
            except (ValueError, KeyError, TypeError, IndexError, OSError, StopIteration, BadZipFile) as failure:
                row["confirmed"] = False
                row["error"] = str(failure)
                if hasattr(failure, "component_event_audit"):
                    row["componentEventAudit"] = failure.component_event_audit
                report["errors"].append({"cell": cell["name"], "number": number, "error": str(failure)})
            component_audit = row.get("componentEventAudit")
            if isinstance(component_audit, dict):
                report["componentEventAuditAttemptCount"] += 1
                if audit.integer(component_audit.get("eventCount")):
                    report["retainedComponentEventCount"] += component_audit["eventCount"]
                if component_audit.get("confirmed") is True:
                    report["componentEventAuditConfirmedCount"] += 1
            report["attempts"].append(row)
    report["qualified"] = not report["errors"] and all(value == 3 for value in report["confirmedCounts"].values())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch", type=Path)
    parser.add_argument("--re-audit", action="store_true",
                        help="Explicitly apply this updated verifier to original frozen evidence; never qualifies new runtime binaries")
    args = parser.parse_args()
    try:
        report = check_matrix(args.batch, re_audit=args.re_audit)
    except Exception as failure:
        report = {"qualified": False, "errors": [str(failure)]}
    print(json.dumps(report, indent=2))
    return 0 if report["qualified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
