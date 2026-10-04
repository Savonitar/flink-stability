#!/usr/bin/env python3
"""Prepare or resume a serial Kafka regression batch. Preparation never starts Docker."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import uuid

from chaos_profiles import PROFILES
from gate_evidence import fault_requirements
from pr_gate import canonical, local_subject, summarize
from subject_catalog import replace_subject, sha256, with_flink_image, with_producer_max_block

LEGACY_PINS = {
    "control": "e1bf6f60fdde6a8bc8a3dc87b0e8bfb9fa21c11294ec40c0ccb874f26bce0b3b",
    "assume": "d01420b0ba3d19c9bd73abfa1c6ef143cdc3769d526abbd316ee92962f867401",
    "rewrite": "11bfffc1fb5008c4ebb5e33f21d0123591019a5e630c0a4d9772590aecef1676",
}
RELEASE = "6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da"
DISCARD = "e17005ab7dd685446de90634662c842599f3a99119d65f9bab696acc63641397"
ESTIMATES = {"brokers": (4, 10), "targets": (6, 15), "legacy": (12, 30),
             "quick-control": (6, 12), "quick": (60, 120), "new-controls": (12, 30),
             "pooling": (90, 240), "rolling": (32, 80), "packet": (32, 64),
             "parallel": (108, 288), "at-least-once": (32, 64), "savepoint": (24, 60)}
NEW_STAGES = ("packet", "parallel", "at-least-once", "savepoint")


def matrix(stages=None):
    cells = []

    def add(group, names, sides=("release",), runs=1):
        for name in names:
            for side in sides:
                for run in range(1, runs + 1):
                    cells.append(dict(group=group, scenario=name, side=side, run=run,
                                      id=f"{len(cells)+1:03}-{group}-{side}-{name}-{run}"))

    add("brokers", ["broker-eos-control", "broker-eos-kill"])
    add("targets", ["broker-leader-kill", "broker-leader-pause", "broker-coordinator-pause"])
    # Both healthy controls precede either mutant. Preserve all six legacy cells.
    for side in LEGACY_PINS:
        add("legacy", ["commit-request-lost", "commit-response-lost"], (side,))
    add("quick-control", ["broker-eos-control"], ("release", "discard"), 2)
    add("quick", PROFILES["chaos-quick"], ("release", "discard"), 2)
    pooling = [name for name in PROFILES["chaos-full"] if name.startswith("pooling-")]
    rolling = [name for name in PROFILES["chaos-full"] if name.startswith("rolling-")]
    controls = [name for name in pooling + rolling if "control" in name or "bounded-eos" in name]
    add("new-controls", controls)
    add("pooling", [name for name in pooling if name not in controls])
    add("rolling", [name for name in rolling if name not in controls])
    for group, prefix in (("packet", "packet-"), ("parallel", "parallel-"),
                          ("at-least-once", "at-least-once-"), ("savepoint", "savepoint-")):
        names = [name for name in PROFILES["chaos-full"] if name.startswith(prefix)]
        add(group, [name for name in names if "control" in name] + [name for name in names if "control" not in name])
    return cells if not stages else [cell for cell in cells if cell["group"] in stages]


def write_new(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def inside(root, path):
    path = path.absolute()
    if not path.is_relative_to(root):
        raise ValueError("Path must stay inside the harness: " + str(path))
    current = root
    for part in path.relative_to(root).parts:
        if part in ("..", "."):
            raise ValueError("Non-canonical path")
        current /= part
        if current.is_symlink():
            raise ValueError("Symlink input/output refused: " + str(current))
    return path


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def clean_tree(root):
    if git(root, "diff", "HEAD", "--name-only"):
        raise ValueError("Commit the harness changes before freezing or running a batch")
    return git(root, "rev-parse", "HEAD^{tree}")


def prepare(root, output, launcher_path, legacy, quick, stages=None):
    tree = clean_tree(root)
    inputs = {}

    def bind(path, digest=None):
        path = inside(root, path)
        actual = sha256(path)
        if digest is not None and actual != digest:
            raise ValueError("Digest mismatch: " + str(path))
        inputs[str(path)] = actual
        return path

    launcher = json.loads(bind(launcher_path).read_text())
    argv = launcher["argv"]
    if (not isinstance(argv, list) or "-cp" not in argv
            or argv[-1] != "org.savonitar.flink.stability.cli.Main"):
        raise ValueError("Launcher must contain a direct Java CLI argv ending with Main")
    for entry in argv[argv.index("-cp") + 1].split(os.pathsep):
        if not entry.endswith(".jar"):
            raise ValueError("Freeze built JARs, not mutable class directories")
        bind(Path(entry))
    bind(root / "flink-job-generator/target/flink-job-generator.jar")
    builds = {kind: json.loads(bind(directory / "build-evidence.json").read_text())
              for kind, directory in (("legacy", legacy), ("quick", quick))}
    if not builds["legacy"]["matchesTestedArtifactHashes"] or not builds["quick"]["matchesPinnedMutant"]:
        raise ValueError("Calibration build evidence is not pinned")
    for name, digest in builds["legacy"]["recipeInputSha256"].items():
        bind(root / "calibration/connector-mutants" / name, digest)
    for name, digest in builds["quick"]["recipeInputs"].items():
        bind(root / "calibration" / name, digest)
    subjects, snippets = {}, {}
    for side, digest in {**LEGACY_PINS, "release": RELEASE, "discard": DISCARD}.items():
        old = side in LEGACY_PINS
        build, directory = (builds["legacy"], legacy) if old else (builds["quick"], quick)
        jar = Path(build["artifacts"][side]["artifact"]) if old else directory / ("release.jar" if side == "release" else "discard-retriable.jar")
        bind(jar, digest)
        expected = build["runtimeDependencySha256"]
        runtime = inside(root, directory / "runtime")
        if {p.name for p in runtime.glob("*.jar")} != set(expected):
            raise ValueError("Runtime dependency set changed")
        for name, dependency_digest in expected.items():
            bind(runtime / name, dependency_digest)
        subjects[side], snippets[side] = local_subject(root, jar, runtime)

    documents = {}
    cells = matrix(stages)
    for cell in cells:
        source = bind(canonical(root, cell["scenario"]))
        expected = bind(source.with_name(cell["scenario"] + ".expected.yaml"))
        text = with_flink_image(source.read_text(), "docker.io/library/flink:2.2.0")
        if cell["group"] == "legacy":
            anchor = "        transaction_id_naming_strategy: INCREMENTING\n"
            if text.count(anchor) != 1 or "transaction_timeout:" in text:
                raise ValueError("Legacy canonical shape changed")
            text = text.replace(anchor, anchor + "        transaction_timeout: 60s\n")
        if cell["group"].startswith("quick"):
            text = with_producer_max_block(text, 5000)
        text = replace_subject(text, snippets[cell["side"]])
        cell["requirements"] = fault_requirements(text)
        cell["oracleMode"] = "at-least-once" if "    mode: at-least-once" in text else "exactly-once"
        cell["subjectSha256"] = subjects[cell["side"]]["connectorSha256"]
        documents[cell["id"]] = (text, expected.read_text())
    output = inside(root, output)
    output.mkdir(parents=True, exist_ok=False)
    for cell in cells:
        catalog = output / "catalogs" / cell["id"]
        catalog.mkdir(parents=True)
        for suffix, text in zip((".yaml", ".expected.yaml"), documents[cell["id"]]):
            path = catalog / (cell["scenario"] + suffix)
            path.write_text(text)
            bind(path)
        cell["catalog"] = str(catalog)
    packet_pin = json.loads(bind(root / "docs/packet-image-pin.json").read_text())
    manifest = dict(version=2, packetProbe=packet_probe_plan(packet_pin), selectedStages=list(stages or []), root=str(root), sourceTree=tree, sourceCommit=git(root, "rev-parse", "HEAD"),
                    launcher=launcher, inputs=inputs, cells=cells, subjects=subjects,
                    estimatesMinutes=ESTIMATES, preparedAt=datetime.now(timezone.utc).isoformat())
    write_new(output / "manifest.json", manifest)
    return manifest


def verify(manifest):
    root = Path(manifest["root"])
    if clean_tree(root) != manifest["sourceTree"]:
        raise ValueError("Harness tree changed since preparation")
    for path, digest in manifest["inputs"].items():
        if sha256(inside(root, Path(path))) != digest:
            raise ValueError("Frozen input changed: " + path)


def accepted(cell, row):
    if row.get("oracleMode", "exactly-once") != cell.get("oracleMode", "exactly-once"):
        return False
    if not row["subjectOk"] or row["faultStatus"] != ("confirmed" if cell["requirements"] else "not-required"):
        return False
    if (cell["group"], cell["side"], cell["scenario"]) == ("legacy", "assume", "commit-request-lost"):
        return row["verdict"] == "fail" and row["exitCode"] == 1 and isinstance(row["missing"], int) and row["missing"] > 0
    if (cell["group"], cell["side"], cell["scenario"]) == ("legacy", "rewrite", "commit-response-lost"):
        return row["verdict"] == "fail" and row["exitCode"] == 1 and isinstance(row["duplicates"], int) and row["duplicates"] > 0
    if cell["group"] == "quick" and cell["side"] == "discard":
        # The complete matrix checker, not one exit code, decides detection.
        return row["verdict"] in ("pass", "fail") and row["exitCode"] == (0 if row["verdict"] == "pass" else 1)
    if cell.get("oracleMode") == "at-least-once":
        return (row["verdict"], row["exitCode"], row["missing"]) == ("pass", 0, 0) and type(row["duplicates"]) is int and row["duplicates"] >= 0
    return (row["verdict"], row["exitCode"], row["missing"], row["duplicates"]) == ("pass", 0, 0, 0)


def check_quick(root, manifest, completed):
    spec = importlib.util.spec_from_file_location("batch_calibration_check", root / "calibration/chaos-profile-mutant/check.py")
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    answers = []
    for group, control in (("quick-control", True), ("quick", False)):
        rows = []
        for cell in manifest["cells"]:
            if cell["group"] == group:
                row = dict(completed[cell["id"]]["row"])
                row["side"] = "baseline" if cell["side"] == "release" else "candidate"
                rows.append(row)
        report = {"manifest": {"scenarios": ["broker-eos-control"] if control else list(PROFILES["chaos-quick"]),
                               "runs": 2, "plan": {"producerMaxBlockMs": 5000}}, "rows": rows}
        answers.append(checker.check(report, control))
    return answers


def run_cell(root, output, manifest, cell):
    directory = output / "runs" / cell["id"]
    directory.mkdir(parents=True, exist_ok=False)
    argv = manifest["launcher"]["argv"] + ["run", "--catalog-root", cell["catalog"], "--scenario", cell["scenario"],
            "--artifact-root", str(root), "--offline", "--kafka-log-output", str(directory / "kafka-logs")]
    write_new(directory / "started.json", {"at": datetime.now(timezone.utc).isoformat(), "argv": argv})
    with (directory / "stdout.json").open("x") as stdout, (directory / "stderr.log").open("x") as stderr:
        try:
            result = subprocess.run(argv, cwd=root, env=manifest["launcher"]["environment"], stdout=stdout, stderr=stderr, timeout=1800)
            code = result.returncode
        except (OSError, subprocess.TimeoutExpired) as failure:
            stderr.write(str(failure) + "\n")
            code = None
    try:
        result = json.loads((directory / "stdout.json").read_text())
        if not isinstance(result, dict):
            raise ValueError("Not a result object")
    except ValueError:
        result = {"status": "error", "reason": "Missing or malformed JSON; inspect retained logs"}
    row = summarize(cell["scenario"], cell["side"], cell["run"], result, cell["subjectSha256"], code, cell["requirements"])
    value = {"row": row, "accepted": accepted(cell, row), "completedAt": datetime.now(timezone.utc).isoformat(),
             "outputHashes": {name: sha256(directory / name) for name in ("stdout.json", "stderr.log")},
             "attempt": result.get("attempt"), "legacyInterpretation": "transaction/timeout/decision correlation review required" if cell["group"] == "legacy" else None}
    write_new(directory / "completed.json", value)
    return value


def completed_cell(directory):
    result = directory / "completed.json"
    if not directory.exists():
        return None
    if not result.is_file():
        raise ValueError("Interrupted/ambiguous cell; never rerun automatically: " + directory.name)
    value = json.loads(result.read_text())
    if any(sha256(directory / name) != digest for name, digest in value["outputHashes"].items()):
        raise ValueError("Retained result changed: " + directory.name)
    return value


def packet_probe_plan(pin):
    script = """set -eu
ip link set lo up
tc qdisc add dev lo root handle 7f00: netem delay 1ms
iptables -w 2 -N FSCHAOS_PROBE
iptables -w 2 -A FSCHAOS_PROBE -d 127.0.0.1/32 -j DROP
tc -s qdisc show dev lo
iptables -w 2 -nvx -L FSCHAOS_PROBE
iptables -w 2 -F FSCHAOS_PROBE
iptables -w 2 -X FSCHAOS_PROBE
tc qdisc del dev lo root
echo PACKET_NET_ADMIN_OK
"""
    return {"pin": pin, "argv": ["docker", "run", "--rm", "--name", "flink-packet-probe-" + uuid.uuid4().hex[:12],
            "--platform", pin["platform"], "--network", "none", "--cap-drop", "ALL", "--cap-add", "NET_ADMIN",
            "--security-opt", "no-new-privileges:true", "--read-only", "--tmpfs", "/run:rw,nosuid,nodev,size=1m",
            "--pids-limit", "32", "--memory", "128m", "--entrypoint", "/bin/sh", pin["image"], "-ceu", script],
            "inspectArgv": ["docker", "image", "inspect", pin["image"]], "timeoutSeconds": 45}


def ensure_packet_probe(output, manifest):
    directory = output / "packet-probe"
    previous = completed_cell(directory)
    if previous is not None:
        if not previous["accepted"]: raise ValueError("Retained NET_ADMIN probe failed; packet stage stopped")
        return previous
    probe = manifest.get("packetProbe")
    if not probe: raise ValueError("Frozen packet probe is missing")
    directory.mkdir(exist_ok=False)
    write_new(directory / "started.json", {"at": datetime.now(timezone.utc).isoformat(), **probe})
    accepted = False; error = None; code = None
    environment = manifest["launcher"]["environment"]
    try:
        with (directory / "stdout.json").open("x") as stdout, (directory / "stderr.log").open("x") as stderr:
            result = subprocess.run(probe["argv"], cwd=manifest["root"], env=environment, stdout=stdout, stderr=stderr,
                                    timeout=probe["timeoutSeconds"])
            code = result.returncode
        if code != 0 or "PACKET_NET_ADMIN_OK" not in (directory / "stdout.json").read_text().splitlines():
            raise ValueError("NET_ADMIN/netem/iptables probe failed")
        # Docker run retrieves the immutable image if necessary; verify the deployed platform/config afterward.
        with (directory / "image.json").open("x") as stdout, (directory / "image-stderr.log").open("x") as stderr:
            inspected = subprocess.run(probe["inspectArgv"], cwd=manifest["root"], env=environment, stdout=stdout, stderr=stderr, timeout=15)
        image = json.loads((directory / "image.json").read_text())[0] if inspected.returncode == 0 else {}
        pin = probe["pin"]
        accepted = (image.get("Id") == pin["configImageId"] and image.get("Os") + "/" + image.get("Architecture") == pin["platform"]
                    and any(value.endswith("@" + pin["indexDigest"]) for value in image.get("RepoDigests", [])))
        if not accepted: raise ValueError("Packet probe image/platform identity mismatch")
    except (OSError, ValueError, TypeError, IndexError, subprocess.TimeoutExpired) as failure:
        error = str(failure)
    value = {"accepted": accepted, "row": {"verdict": "pass" if accepted else "inconclusive", "exitCode": code}, "error": error,
             "outputHashes": {path.name: sha256(path) for path in directory.iterdir() if path.name != "completed.json" and path.is_file()}}
    write_new(directory / "completed.json", value)
    if not accepted: raise ValueError("NET_ADMIN probe unconfirmed; packet stage stopped: " + str(error))
    return value


def resume(output, limit, stages=None):
    manifest = json.loads((output / "manifest.json").read_text())
    verify(manifest)
    root = Path(manifest["root"])
    if stages and set(stages) - {cell["group"] for cell in manifest["cells"]}:
        raise ValueError("Selected stage is absent from the frozen manifest")
    completed, launched = {}, 0
    for cell in manifest["cells"]:
        if stages and cell["group"] not in stages: continue
        value = completed_cell(output / "runs" / cell["id"])
        if value is None:
            if launched == limit:
                break
            # Stop before the new-feature block unless both quick checks qualify.
            if cell["group"] == "new-controls":
                check_quick(root, manifest, completed)
            if cell["group"] == "packet": ensure_packet_probe(output, manifest)
            value = run_cell(root, output, manifest, cell)
            launched += 1
        completed[cell["id"]] = value
        print(f"{cell['id']}: {value['row']['verdict']} (exit {value['row']['exitCode']})", flush=True)
        if not value["accepted"]:
            raise ValueError("Unclassified result retained; batch stopped at " + cell["id"])
    return manifest, completed


def summary(output, manifest):
    print("| Stage | Cells | Estimated minutes | Completed | Pass / fail / other |")
    print("| --- | ---: | ---: | ---: | --- |")
    counts = Counter(cell["group"] for cell in manifest["cells"])
    for group, count in counts.items():
        outcomes = Counter()
        for cell in manifest["cells"]:
            if cell["group"] != group:
                continue
            directory = output / "runs" / cell["id"]
            if (directory / "completed.json").is_file():
                try:
                    value = completed_cell(directory)
                    outcomes[value["row"]["verdict"]] += 1
                except (ValueError, KeyError):
                    outcomes["invalid"] += 1
        done = sum(outcomes.values())
        low, high = manifest.get("estimatesMinutes", ESTIMATES)[group]
        other = done - outcomes['pass'] - outcomes['fail']
        print(f"| {group} | {count} | {low}–{high} | {done} | {outcomes['pass']} / {outcomes['fail']} / {other} |")
    print("Estimates assume warm images; no automatic retries. Completed does not mean passed.")
    print("Legacy calibration still requires matching transaction/timeout/decision markers, complete fences/oracle and no unsupported marker.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "prepare", "run", "status"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--launcher", type=Path, help="JSON with direct Java CLI argv and an explicit non-secret environment")
    parser.add_argument("--legacy-build", type=Path)
    parser.add_argument("--quick-build", type=Path)
    parser.add_argument("--execute", action="store_true", help="Launch the prepared batch")
    parser.add_argument("--stage", choices=NEW_STAGES, action="append", help="Select a new stage independently; repeatable. Omission includes the original batch first.")
    parser.add_argument("--max-new-cells", type=int, default=157, help="Bound this invocation; completed cells are skipped")
    args = parser.parse_args()
    root = Path.cwd().absolute()
    if args.action == "plan":
        print(json.dumps({"cells": matrix(args.stage), "estimatesMinutes": ESTIMATES}, indent=2))
        return 0
    if args.output is None:
        parser.error("--output is required")
    output = inside(root, args.output)
    if args.action == "prepare":
        if None in (args.launcher, args.legacy_build, args.quick_build):
            parser.error("prepare requires --launcher, --legacy-build and --quick-build")
        manifest = prepare(root, output, args.launcher, args.legacy_build, args.quick_build, args.stage)
        summary(output, manifest)
        return 0
    manifest = json.loads((output / "manifest.json").read_text())
    try:
        if args.action == "run":
            if not args.execute or args.max_new_cells < 1:
                parser.error("run requires --execute and a positive --max-new-cells")
            # An OS file lock releases on exit/crash without deleting evidence.
            import fcntl
            with (output / "runner.lock").open("a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                manifest, completed = resume(output, args.max_new_cells, args.stage)
                write_new(output / ("summary-" + uuid.uuid4().hex + ".json"), {"completed": completed})
    finally:
        summary(output, manifest)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, KeyError, FileExistsError, BlockingIOError) as failure:
        raise SystemExit(str(failure))
