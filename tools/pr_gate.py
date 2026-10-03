#!/usr/bin/env python3
"""Run canonical scenarios against two explicit Kafka connector builds or the release.

The candidate catalog swaps only the subject connector: the given JAR plus every JAR in
its runtime directory. The baseline defaults to the unchanged catalog in scenarios/;
paired --baseline-* options select a local baseline through the same checks. Run from
the repository root after `mvn install`; docs/PR-TESTING.md describes the procedure.
"""
import argparse
from collections import Counter
import json
import re
import shlex
import shutil
import subprocess
from pathlib import Path

from chaos_profiles import PROFILES
from gate_evidence import fault_requirements, fault_status, coverage_table, data_difference

from subject_catalog import artifact_reference, replace_subject, sha256, subject_snippet, with_producer_max_block

DEFAULT_SCENARIOS = ["bounded-eos", "commit-request-lost", "commit-response-lost"]
RELEASED_SHA256 = "6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da"
STAGED_JAR = re.compile(r"flink-stability-connector-\d+-([0-9a-f]{64})\.jar$")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connector-jar", type=Path, required=True, help="Locally built connector JAR")
    parser.add_argument("--runtime-dir", type=Path, required=True,
                        help="Directory holding exactly the connector's runtime dependency JARs")
    parser.add_argument("--baseline-connector-jar", type=Path,
                        help="Optional local baseline connector; requires --baseline-runtime-dir")
    parser.add_argument("--baseline-runtime-dir", type=Path,
                        help="Explicit baseline runtime dependency directory")
    parser.add_argument("--output", type=Path, required=True, help="New directory for catalogs and results")
    parser.add_argument("--profile", choices=tuple(PROFILES), help="Reviewed chaos coverage; requires an explicit parent baseline")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan only; do not create files or invoke Maven/Docker")
    parser.add_argument("--flink-image", choices=("docker.io/library/flink:2.2.0",), help="Use the fully qualified equivalent image in copied catalogs")
    parser.add_argument("--producer-max-block-ms", type=int, help="Calibration only: identical positive max.block.ms for both copied workloads")
    parser.add_argument("--scenario", action="append", help="Canonical scenario name; repeatable")
    parser.add_argument("--runs", type=int, default=1, help="Runs per scenario and side")
    args = parser.parse_args()
    if args.producer_max_block_ms is not None and not 0 < args.producer_max_block_ms <= 2147483647:
        parser.error("--producer-max-block-ms must be a positive 32-bit integer")
    if args.runs < 1:
        parser.error("--runs must be a positive integer")
    if (args.baseline_connector_jar is None) != (args.baseline_runtime_dir is None):
        parser.error("--baseline-connector-jar and --baseline-runtime-dir must be supplied together")
    if args.profile and args.baseline_connector_jar is None:
        parser.error("Chaos profiles require an explicit PR parent via both --baseline-* options")
    if args.profile and args.scenario:
        parser.error("Choose --profile or repeated --scenario, not both")
    root = Path.cwd().resolve()
    names = list(dict.fromkeys(PROFILES[args.profile] if args.profile else args.scenario or DEFAULT_SCENARIOS))
    scenarios = {name: canonical(root, name) for name in names}
    plan = {"profile": args.profile, "scenarios": names, "runsPerSide": args.runs,
            "totalRuns": len(names) * args.runs * 2,
            "estimatedMinutes": [len(names) * args.runs * 3, len(names) * args.runs * 6],
            "baseline": str(args.baseline_connector_jar) if args.baseline_connector_jar else "released 5.0.0-2.2",
            "candidate": str(args.connector_jar), "flinkImageOverride": args.flink_image,
            "producerMaxBlockMs": args.producer_max_block_ms}
    print("Plan: " + json.dumps(plan, sort_keys=True), flush=True)
    print("Estimate assumes warm images/artifacts: 1.5–3 minutes per independent run; not a deadline or measured guarantee.", flush=True)
    if args.dry_run:
        return 0
    candidate, candidate_snippet = local_subject(root, args.connector_jar, args.runtime_dir)
    baseline = {"connector": "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2",
                "connectorSha256": RELEASED_SHA256, "dependencyMode": "auto",
                "runtimeDependencySha256": None}
    baseline_snippet = None
    if args.baseline_connector_jar is not None:
        baseline, baseline_snippet = local_subject(
            root, args.baseline_connector_jar, args.baseline_runtime_dir)
    # Validate every scenario before creating any output.
    requirements = {name: fault_requirements(path.read_text()) for name, path in scenarios.items()}
    replacements = {"candidate": {name: replace_subject(path.read_text(), candidate_snippet)
                                   for name, path in scenarios.items()}}
    if baseline_snippet is not None:
        replacements["baseline"] = {name: replace_subject(path.read_text(), baseline_snippet)
                                    for name, path in scenarios.items()}
    if args.producer_max_block_ms is not None:
        if "baseline" not in replacements:
            replacements["baseline"] = {name: path.read_text() for name, path in scenarios.items()}
        for documents in replacements.values():
            for name, document in documents.items():
                documents[name] = with_producer_max_block(document, args.producer_max_block_ms)
    if args.flink_image:
        if "baseline" not in replacements:
            replacements["baseline"] = {name: path.read_text() for name, path in scenarios.items()}
        for documents in replacements.values():
            for name, document in documents.items():
                anchor = "    image: flink:2.2.0"
                if document.count(anchor) != 1:
                    raise SystemExit("Expected one canonical Flink 2.2 image in " + name)
                documents[name] = document.replace(anchor, "    image: " + args.flink_image)
    output = args.output.resolve()
    if not output.is_relative_to(root):
        raise SystemExit("Output directory must stay inside the artifact root")
    output.mkdir(parents=True, exist_ok=False)
    catalogs = {"baseline": root / "scenarios"}
    for side, documents in replacements.items():
        catalog = output / (side + "-catalog")
        catalog.mkdir()
        catalogs[side] = catalog
        for name, path in scenarios.items():
            (catalog / path.name).write_text(documents[name])
            shutil.copyfile(path.with_name(name + ".expected.yaml"), catalog / (name + ".expected.yaml"))
    manifest = {"baseline": baseline, "candidate": candidate, "scenarios": names, "runs": args.runs, "plan": plan, "faultRequirements": requirements}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    rows = []
    for name in names:
        for side in ("baseline", "candidate"):
            for run in range(1, args.runs + 1):
                result, exit_code = run_scenario(
                    root, catalogs[side], name, output / side / name / f"run-{run}")
                rows.append(summarize(name, side, run, result,
                                      manifest[side]["connectorSha256"], exit_code, requirements[name]))
    (output / "summary.json").write_text(json.dumps({"manifest": manifest, "rows": rows,
            "candidateOnlyDataFailure": data_difference(rows, args.runs)}, indent=2) + "\n")
    summary = render(manifest, rows)
    (output / "summary.md").write_text(summary)
    print(summary)
    return gate_exit_code(rows)


def local_subject(root, connector, runtime):
    """Bind either side through exactly the same path, digest and closure checks."""
    primary = artifact_reference(connector, root)
    runtime = runtime.resolve()
    if not runtime.is_dir() or not runtime.is_relative_to(root):
        raise SystemExit("Runtime directory must be inside the artifact root: " + str(runtime))
    dependencies = sorted(runtime.glob("*.jar"))
    if not dependencies:
        raise SystemExit("No runtime dependency JARs in " + str(runtime))
    references = [artifact_reference(jar, root) for jar in dependencies]
    digest = sha256(root / primary)
    snippet = subject_snippet(primary, references)
    first, rest = snippet.split("\n", 1)
    snippet = first + "\nsha256: " + digest + "\n" + rest
    return {"connector": primary, "connectorSha256": digest, "dependencyMode": "explicit",
            "runtimeDependencySha256": {reference: sha256(root / reference)
                                         for reference in references}}, snippet


def canonical(root, name):
    matches = list((root / "scenarios").rglob(name + ".yaml"))
    if len(matches) != 1:
        raise SystemExit(f"Expected one canonical scenario file for {name}, found {len(matches)}")
    return matches[0]


def run_scenario(root, catalog_root, name, directory):
    directory.mkdir(parents=True)
    arguments = shlex.join(["run", "--catalog-root", str(catalog_root), "--scenario", name,
                            "--artifact-root", ".", "--offline", "--kafka-log-output", str(directory / "kafka-logs")])
    with (directory / "stdout.json").open("w") as stdout, (directory / "stderr.log").open("w") as stderr:
        try:
            code = subprocess.run(
                ["mvn", "-q", "-o", "exec:java", "-pl", "cli", "-Dexec.args=" + arguments],
                cwd=root, stdout=stdout, stderr=stderr, check=False).returncode
        except OSError as failure:
            stderr.write(str(failure) + "\n")
            code = None
    (directory / "exit-code.txt").write_text(f"{code if code is not None else 'not-started'}\n")
    try:
        result = json.loads((directory / "stdout.json").read_text())
    except ValueError:
        return {"status": "error", "reason": f"no JSON result (exit {code})"}, code
    if not isinstance(result, dict) or result.get("status") not in ("pass", "fail", "inconclusive"):
        return {"status": "error", "reason": f"invalid scenario result (exit {code})"}, code
    return result, code


def summarize(name, side, run, result, expected_subject, exit_code, required_effects=None):
    evidence = result.get("evidence") or {}
    origins = evidence.get("subjectClasses") or {}
    observed = [source for process in origins.get("processes", [])
                for sources in process.get("sources", {}).values() for source in sources]
    staged = [STAGED_JAR.search(source) if isinstance(source, str) else None for source in observed]
    hashes = {match.group(1) for match in staged if match}
    subject_ok = (origins.get("status") == "confirmed" and bool(staged)
                  and all(match and match.group(1) == expected_subject for match in staged))
    terminal = evidence.get("terminalValidation") or {}
    counts = {key: terminal.get(key) for key in ("missing", "duplicates")}
    return {"scenario": name, "side": side, "run": run, "verdict": result.get("status"),
            "reason": result.get("reason"), **counts, "subjects": sorted(hashes),
            "subjectStatus": origins.get("status", "unavailable"),
            "subjectOk": subject_ok, "exitCode": exit_code,
            "componentErrors": evidence.get("componentErrors"),
            "faultStatus": fault_status(evidence, required_effects), "faultRequired": required_effects is not None and bool(required_effects)}


def gate_exit_code(rows):
    """A report is successful only when every run passed with confirmed provenance."""
    return 0 if rows and all(row["verdict"] == "pass" and row["subjectOk"]
                             and row["exitCode"] == 0
                             and (not row.get("faultRequired") or row.get("faultStatus") == "confirmed") for row in rows) else 1


def render(manifest, rows):
    lines = ["# Connector pull-request gate", ""]
    for side in ("baseline", "candidate"):
        subject = manifest[side]
        dependencies = subject["runtimeDependencySha256"]
        closure = ("automatic released Maven closure" if dependencies is None
                   else f"{len(dependencies)} explicit runtime dependency JARs")
        lines += [f"{side.capitalize()}: `{subject['connector']}` "
                  f"(SHA-256 `{subject['connectorSha256']}`), {closure}.", ""]
    difference = data_difference(rows, manifest.get("runs"))
    lines += coverage_table(rows)
    lines += ["", "Candidate-only losses or duplicates: " + difference["status"].upper()
              + (" (" + ", ".join(difference["findings"]) + ")" if difference["findings"] else "") + ".",
              "Unresolved comparisons: " + (", ".join(difference["unresolved"]) or "none") + ".", ""]
    lines += [
             "Gate result: " + ("PASS" if gate_exit_code(rows) == 0 else "NOT PASSED") + ".", "",
             "| Scenario | Side | Run | Verdict | Reason | Missing | Duplicates | Subject JAR | Exit |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for row in rows:
        subject = ", ".join(value[:12] for value in row["subjects"]) or "unknown"
        subject += " ok" if row["subjectOk"] else " NOT VERIFIED (" + row["subjectStatus"] + ")"
        lines.append(f"| {row['scenario']} | {row['side']} | {row['run']} | {row['verdict']} | "
                     f"{row['reason']} | {row['missing']} | {row['duplicates']} | {subject} | "
                     f"{row['exitCode']} |")
    lines += ["", "Component log observations (diagnostic only; counts cover retained log prefixes):",
              "", "| Side | KafkaCommitter ERROR | Other classified observations | Unclassified observations | KafkaCommitter ERROR kinds | Partial / unavailable runs |",
              "| --- | --- | --- | --- | --- | --- |"]
    for side in ("baseline", "candidate"):
        kinds, committer, other, unclassified, partial = Counter(), 0, 0, 0, 0
        for row in rows:
            if row["side"] != side:
                continue
            errors = row.get("componentErrors")
            if errors is None:
                partial += 1
                continue
            partial += errors.get("coverage") != "captured-prefix"
            for event in errors.get("events", []):
                if not event.get("level") or not event.get("logger"):
                    unclassified += 1
                elif (event["level"] == "ERROR" and event["logger"] ==
                      "org.apache.flink.connector.kafka.sink.internal.KafkaCommitter"):
                    committer += 1
                    kinds.update(set(event.get("kinds", [])))
                else:
                    other += 1
        counts = ", ".join(f"{kind}: {count}" for kind, count in sorted(kinds.items())) or "none observed"
        lines.append(f"| {side} | {committer} | {other} | {unclassified} | {counts} | {partial} |")
    outcomes = {}
    for row in rows:
        sides = outcomes.setdefault(row["scenario"], {"baseline": Counter(), "candidate": Counter()})
        sides[row["side"]][(row["verdict"], row["reason"])] += 1
    differing = sorted(name for name, sides in outcomes.items()
                       if sides["baseline"] != sides["candidate"])
    variable = sorted(name + "/" + side for name, sides in outcomes.items()
                      for side, counts in sides.items() if len(counts) > 1)
    lines += ["", "Outcome distributions differ for: "
              + (", ".join(differing) if differing else "none") + ".",
              "Outcomes varied within: " + (", ".join(variable) if variable else "none") + ".",
              "Equal distributions do not establish correctness. Preserve and investigate failures on either side.",
              "A baseline failure does not establish an environment fault or attribute a regression to the candidate.",
              "A difference is a lead, not proof: faults and checkpoints are timing-based.", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
