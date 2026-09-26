#!/usr/bin/env python3
"""Run canonical scenarios against a locally built Kafka connector and against the release.

The candidate catalog swaps only the subject connector: the given JAR plus every JAR in
its runtime directory. The baseline is the unchanged catalog in scenarios/. Run from
the repository root after `mvn install`; docs/PR-TESTING.md describes the procedure.
"""
import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path

from subject_catalog import artifact_reference, replace_subject, sha256, subject_snippet

DEFAULT_SCENARIOS = ["bounded-eos", "commit-request-lost", "commit-response-lost"]
RELEASED_SHA256 = "6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da"
STAGED_JAR = re.compile(r"flink-stability-connector-\d+-([0-9a-f]{64})\.jar$")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connector-jar", type=Path, required=True, help="Locally built connector JAR")
    parser.add_argument("--runtime-dir", type=Path, required=True,
                        help="Directory holding exactly the connector's runtime dependency JARs")
    parser.add_argument("--output", type=Path, required=True, help="New directory for catalogs and results")
    parser.add_argument("--scenario", action="append", help="Canonical scenario name; repeatable")
    parser.add_argument("--runs", type=int, default=1, help="Runs per scenario and side")
    args = parser.parse_args()
    root = Path.cwd().resolve()
    names = args.scenario or DEFAULT_SCENARIOS
    connector = args.connector_jar.resolve()
    dependencies = sorted(args.runtime_dir.resolve().glob("*.jar"))
    if not dependencies:
        raise SystemExit("No runtime dependency JARs in " + str(args.runtime_dir))
    snippet = subject_snippet(artifact_reference(connector, root),
                              [artifact_reference(jar, root) for jar in dependencies])
    # Validate every scenario before creating any output.
    scenarios = {name: canonical(root, name) for name in names}
    candidates = {name: replace_subject(path.read_text(), snippet) for name, path in scenarios.items()}
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    catalog = output / "candidate-catalog"
    catalog.mkdir()
    for name, path in scenarios.items():
        (catalog / path.name).write_text(candidates[name])
        shutil.copyfile(path.with_name(name + ".expected.yaml"), catalog / (name + ".expected.yaml"))
    manifest = {"connector": str(connector.relative_to(root)), "connectorSha256": sha256(connector),
                "runtimeDependencySha256": {jar.name: sha256(jar) for jar in dependencies},
                "scenarios": names, "runs": args.runs}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    rows = []
    for name in names:
        for side, catalog_root, subject in (("baseline", root / "scenarios", RELEASED_SHA256),
                                            ("candidate", catalog, manifest["connectorSha256"])):
            for run in range(1, args.runs + 1):
                result = run_scenario(root, catalog_root, name, output / side / name / f"run-{run}")
                rows.append(summarize(name, side, run, result, subject))
    summary = render(manifest, rows)
    (output / "summary.md").write_text(summary)
    print(summary)


def canonical(root, name):
    matches = list((root / "scenarios").rglob(name + ".yaml"))
    if len(matches) != 1:
        raise SystemExit(f"Expected one canonical scenario file for {name}, found {len(matches)}")
    return matches[0]


def run_scenario(root, catalog_root, name, directory):
    directory.mkdir(parents=True)
    arguments = f"run --catalog-root {catalog_root} --scenario {name} --artifact-root . --offline"
    with (directory / "stdout.json").open("w") as stdout, (directory / "stderr.log").open("w") as stderr:
        code = subprocess.run(["mvn", "-q", "-o", "exec:java", "-pl", "cli", "-Dexec.args=" + arguments],
                              cwd=root, stdout=stdout, stderr=stderr, check=False).returncode
    (directory / "exit-code.txt").write_text(f"{code}\n")
    try:
        return json.loads((directory / "stdout.json").read_text())
    except ValueError:
        return {"status": "error", "reason": f"no JSON result (exit {code})"}


def find(node, key):
    """Every value stored under key anywhere in a JSON result."""
    if isinstance(node, dict):
        for name, value in node.items():
            if name == key:
                yield value
            yield from find(value, key)
    elif isinstance(node, list):
        for value in node:
            yield from find(value, key)


def summarize(name, side, run, result, expected_subject):
    staged = [STAGED_JAR.search(source) for source in find(result, "expectedSource") if isinstance(source, str)]
    subject = next((match.group(1) for match in staged if match), None)
    counts = {key: next((value for value in find(result, key) if isinstance(value, int)), None)
              for key in ("missing", "duplicates")}
    return {"scenario": name, "side": side, "run": run, "verdict": result.get("status"),
            "reason": result.get("reason"), **counts, "subject": subject,
            "subjectOk": subject == expected_subject}


def render(manifest, rows):
    lines = ["# Connector pull-request gate", "",
             f"Candidate `{manifest['connector']}` (SHA-256 `{manifest['connectorSha256'][:12]}`) with "
             f"{len(manifest['runtimeDependencySha256'])} runtime dependency JARs; baseline: released "
             "flink-connector-kafka 5.0.0-2.2.", "",
             "| Scenario | Side | Run | Verdict | Reason | Missing | Duplicates | Subject JAR |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for row in rows:
        subject = (row["subject"] or "unknown")[:12] + (" ok" if row["subjectOk"] else " WRONG")
        lines.append(f"| {row['scenario']} | {row['side']} | {row['run']} | {row['verdict']} | "
                     f"{row['reason']} | {row['missing']} | {row['duplicates']} | {subject} |")
    verdicts = {}
    for row in rows:
        verdicts.setdefault(row["scenario"], set()).add(row["verdict"])
    differing = sorted(name for name, seen in verdicts.items() if len(seen) > 1)
    lines += ["", "Verdicts differ for: " + (", ".join(differing) if differing else "none") + ".",
              "A single differing run is a lead, not proof: faults and checkpoints are timing-based.", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    main()
