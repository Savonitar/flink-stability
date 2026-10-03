#!/usr/bin/env python3
"""Build two immutable local Kafka connector refs and run the explicit-baseline PR gate."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from chaos_profiles import PROFILES
from kafka_chaos_batch import inside, write_new
from subject_catalog import sha256

# First matching prefix wins. Unknown files (including build/dependency changes) select full.
# Keep this table explicit: a suggestion is reviewable evidence, never a claim of complete coverage.
PATH_PROFILES = (
    ("flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/internal/TransactionalIdFactory", ("pooling", "rolling")),
    ("flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/internal/KafkaCommitter", ("protocol", "pooling", "brokers")),
    ("flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/internal/FlinkKafkaInternalProducer", ("protocol", "packet", "at-least-once")),
    ("flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/", ("pooling", "protocol", "rolling", "at-least-once")),
    ("flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/", ("brokers", "rolling", "packet")),
    ("flink-connector-kafka/src/test/java/org/apache/flink/connector/kafka/sink/", ("pooling", "protocol", "at-least-once")),
    ("flink-connector-kafka/src/test/java/org/apache/flink/connector/kafka/source/", ("brokers", "rolling", "packet")),
)
CENTRAL_SETTINGS = '''<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0">
<mirrors><mirror><id>central-only</id><mirrorOf>*</mirrorOf>
<url>https://repo.maven.apache.org/maven2/</url></mirror></mirrors>
</settings>
'''
DEPENDENCY_PLUGIN = "org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies"


def suggest(paths):
    matches = []
    for path in paths:
        rule = next(((prefix, names) for prefix, names in PATH_PROFILES if path.startswith(prefix)), None)
        matches.append({"path": path, "prefix": rule[0] if rule else None,
                        "profiles": list(rule[1]) if rule else ["chaos-full"]})
    names = list(dict.fromkeys(name for match in matches for name in match["profiles"]))
    if not names or "chaos-full" in names:
        names = ["chaos-full"]
    return {"profiles": names, "matches": matches}


def git(checkout, *args):
    return subprocess.check_output(["git", "-C", str(checkout), *args], text=True).strip()


def checked_checkout(root, checkout):
    checkout = inside(root, checkout)
    # Refuse external gitdir/commondir and alternate object stores before invoking Git.
    metadata = inside(root, checkout / ".git")
    if metadata.is_file():
        value = metadata.read_text().strip()
        if not value.startswith("gitdir: "):
            raise ValueError("Unrecognized gitdir file")
        metadata = inside(root, Path(os.path.abspath(checkout / value[8:])))
    if not metadata.is_dir():
        raise ValueError("Checkout needs local Git metadata")
    common = metadata
    common_file = inside(root, metadata / "commondir")
    if common_file.exists():
        common = inside(root, Path(os.path.abspath(metadata / common_file.read_text().strip())))
    if inside(root, common / "objects/info/alternates").exists():
        raise ValueError("Alternate object stores are unsupported")
    if Path(git(checkout, "rev-parse", "--show-toplevel")) != checkout:
        raise ValueError("--checkout must name the repository root")
    return checkout


def resolve(checkout, reference):
    value = git(checkout, "rev-parse", "--verify", "--end-of-options", reference + "^{commit}")
    if len(value) not in (40, 64) or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("Git returned an invalid commit identity")
    return value


def inspect_tree(checkout, commit):
    entries = git(checkout, "ls-tree", "-r", "--full-tree", commit).splitlines()
    for entry in entries:
        mode, _, remainder = entry.split(" ", 2)
        path = remainder.split("\t", 1)[1]
        if mode in ("120000", "160000"):
            raise ValueError("Symlinks/submodules in build trees are unsupported: " + path)
        if path in (".mvn/maven.config", ".mvn/jvm.config", ".mvn/extensions.xml"):
            raise ValueError("Project Maven overrides require separate review: " + path)


def build_environment(output):
    home = output / "home"
    temporary = output / "tmp"
    home.mkdir(); temporary.mkdir()
    env = {key: os.environ[key] for key in ("PATH", "JAVA_HOME", "LANG", "SYSTEMROOT") if key in os.environ}
    env.update(HOME=str(home), TMPDIR=str(temporary), MAVEN_SKIP_RC="true",
               JAVA_TOOL_OPTIONS='-Duser.home="' + str(home) + '" -Djava.io.tmpdir="' + str(temporary) + '"',
               TZ="UTC")
    return env


def command(argv, cwd, env, directory, name):
    write_new(directory / (name + "-command.json"), {"argv": list(map(str, argv)), "cwd": str(cwd)})
    with (directory / (name + ".log")).open("x") as log:
        result = subprocess.run(list(map(str, argv)), cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
    write_new(directory / (name + "-exit.json"), {"exitCode": result.returncode})
    if result.returncode:
        raise ValueError(name + " failed; evidence retained in " + str(directory))
    return (directory / (name + ".log")).read_text()


def build(root, checkout, output, side, commit, maven, env, settings):
    directory = output / side
    directory.mkdir()
    worktree = directory / "worktree"
    command(["git", "-C", checkout, "worktree", "add", "--detach", worktree, commit], root, None, directory, "worktree")
    if resolve(worktree, "HEAD") != commit:
        raise ValueError("Worktree identity changed")
    module = worktree / "flink-connector-kafka"
    if not (module / "pom.xml").is_file():
        raise ValueError("Expected flink-connector-kafka Maven module")
    repository = directory / "repository"
    argv = [maven, "-B", "--no-transfer-progress", "-s", settings, "-gs", settings,
            "-Dmaven.repo.local=" + str(repository), "-f", worktree / "pom.xml", "-pl", "flink-connector-kafka"]
    command(argv + ["-am", "install", "-DskipTests"], directory, env, directory, "build")
    jars = [p for p in (module / "target").glob("flink-connector-kafka-*.jar")
            if not p.name.endswith(("-tests.jar", "-sources.jar", "-javadoc.jar"))]
    if len(jars) != 1:
        raise ValueError("Expected exactly one primary connector JAR: " + str(jars))
    primary = directory / "connector.jar"
    shutil.copyfile(inside(root, jars[0]), primary)
    runtime = directory / "runtime"
    runtime.mkdir()
    command(argv + [DEPENDENCY_PLUGIN, "-DincludeScope=runtime", "-DoutputDirectory=" + str(runtime)],
            directory, env, directory, "runtime")
    dependencies = sorted(runtime.glob("*.jar"))
    if not dependencies:
        raise ValueError("Empty runtime closure")
    if resolve(worktree, "HEAD") != commit or git(worktree, "diff", "HEAD", "--name-only"):
        raise ValueError("Build changed tracked source or ref")
    return {"commit": commit, "tree": git(worktree, "rev-parse", "HEAD^{tree}"), "worktree": str(worktree),
            "jar": str(primary), "jarSha256": sha256(primary), "runtimeDirectory": str(runtime),
            "runtimeSha256": {p.name: sha256(inside(root, p)) for p in dependencies}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--head", required=True)
    baseline = parser.add_mutually_exclusive_group(required=True)
    baseline.add_argument("--base", help="Explicit baseline ref")
    baseline.add_argument("--merge-base-of", help="Compute merge-base of this ref and --head")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("auto", *PROFILES), default="auto")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--maven", default="mvn")
    parser.add_argument("--dry-run", action="store_true", help="Resolve refs and suggest coverage; no worktrees or builds")
    parser.add_argument("--build-only", action="store_true", help="Retain both builds and the gate command without running Docker")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    root = Path.cwd().absolute()
    output = None
    owns_output = False
    try:
        # Maven executes from per-side directories, so bind a supplied path before changing cwd.
        maven = str(inside(root, Path(args.maven))) if os.sep in args.maven else args.maven
        checkout = checked_checkout(root, args.checkout)
        output = inside(root, args.output)
        if output.exists() or output.is_relative_to(checkout):
            raise ValueError("Use a new output directory outside the connector checkout")
        head = resolve(checkout, args.head)
        reference = resolve(checkout, args.base or args.merge_base_of)
        base = reference if args.base else git(checkout, "merge-base", "--", head, reference)
        base = resolve(checkout, base)
        for commit in (base, head): inspect_tree(checkout, commit)
        paths = subprocess.check_output(["git", "-C", str(checkout), "diff", "--name-only", "-z", "--no-renames", base, head, "--"], text=True).split("\0")
        suggestion = suggest([path for path in paths if path])
        selected = suggestion["profiles"] if args.profile == "auto" else [args.profile]
        names = list(dict.fromkeys(name for profile in selected for name in PROFILES[profile]))
        plan = {"checkout": str(checkout), "candidateCommit": head, "baselineCommit": base,
                "candidateTree": git(checkout, "rev-parse", head + "^{tree}"),
                "baselineTree": git(checkout, "rev-parse", base + "^{tree}"),
                "baselineReference": reference, "baselineMethod": "explicit" if args.base else "merge-base",
                "suggestion": suggestion, "selectedProfiles": selected, "scenarios": names, "runs": args.runs}
        print(json.dumps(plan, indent=2), flush=True)
        if args.dry_run: return 0
        original = (resolve(checkout, "HEAD"), git(checkout, "status", "--porcelain"))
        output.mkdir(parents=True, exist_ok=False)
        owns_output = True
        write_new(output / "plan.json", plan)
        settings = output / "central-settings.xml"
        settings.write_text(CENTRAL_SETTINGS)
        env = build_environment(output)
        versions = command([maven, "-version"], output, env, output, "maven-version")
        java = str(Path(env["JAVA_HOME"]) / "bin/java") if env.get("JAVA_HOME") else "java"
        jdk = command([java, "-version"], output, env, output, "jdk-version")
        builds = {}
        for side, commit in (("baseline", base), ("candidate", head)):
            builds[side] = build(root, checkout, output, side, commit, maven, env, settings)
            write_new(output / (side + "-build.json"), builds[side])
        if original != (resolve(checkout, "HEAD"), git(checkout, "status", "--porcelain")):
            raise ValueError("Input checkout changed while building; inspect retained evidence")
        gate = [sys.executable, str(root / "tools/pr_gate.py"), "--baseline-connector-jar", builds["baseline"]["jar"],
                "--baseline-runtime-dir", builds["baseline"]["runtimeDirectory"], "--connector-jar", builds["candidate"]["jar"],
                "--runtime-dir", builds["candidate"]["runtimeDirectory"], "--output", str(output / "gate"), "--runs", str(args.runs)]
        for name in names: gate += ["--scenario", name]
        manifest = {"plan": plan, "builds": builds, "mavenVersion": versions, "jdkVersion": jdk,
                    "settingsSha256": sha256(settings), "gateCommand": gate,
                    "preparedAt": datetime.now(timezone.utc).isoformat()}
        write_new(output / "manifest.json", manifest)
        if args.build_only: return 0
        # The gate's Maven must use the harness's already prepared runtime/cache. Do not reuse either connector build cache.
        return subprocess.run(gate, cwd=root, check=False).returncode
    except (OSError, ValueError, subprocess.CalledProcessError) as failure:
        if owns_output and (output / "plan.json").exists() and not (output / "failure.json").exists():
            write_new(output / "failure.json", {"error": str(failure)})
        print(str(failure), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
