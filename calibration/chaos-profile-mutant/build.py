#!/usr/bin/env python3
"""Build and unit-check a single-class overlay, entirely offline. Never runs Docker."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("previous_recipe", ROOT.parent / "connector-mutants/build.py")
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)
sha, verify, run = previous.sha, previous.verify, previous.run
NAME, PACKAGE = previous.NAME, previous.PACKAGE
MUTANT_HASH = "e17005ab7dd685446de90634662c842599f3a99119d65f9bab696acc63641397"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--maven", type=Path, required=True)
    parser.add_argument("--settings", type=Path, required=True)
    args = parser.parse_args()
    out = ROOT / "target"
    out.mkdir(exist_ok=False)
    repository = args.repository.resolve()
    release_dir = repository / "org/apache/flink/flink-connector-kafka/5.0.0-2.2"
    primary = release_dir / (NAME + ".jar")
    sources = release_dir / (NAME + "-sources.jar")
    verify(primary, previous.BINARY_HASH)
    verify(sources, previous.SOURCE_HASH)
    java = Path(os.environ["JAVA_HOME"]) / "bin"
    support = out / "build-dependencies"
    command = [args.maven.resolve(), "-B", "-o", "--settings", args.settings.resolve(),
               "--global-settings", args.settings.resolve(), "-Dmaven.repo.local=" + str(repository)]
    run(*command, "-f", ROOT.parent / "connector-mutants/pom.xml", previous.PLUGIN + ":copy-dependencies",
        "-DoutputDirectory=" + str(support), cwd=ROOT)
    resolver = out / "runtime-pom.xml"
    resolver.write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>'
        '<groupId>local.calibration</groupId><artifactId>runtime</artifactId><version>1</version>'
        '<dependencies><dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-kafka</artifactId>'
        '<version>5.0.0-2.2</version></dependency></dependencies></project>')
    runtime = out / "runtime"
    run(*command, "-f", resolver, previous.PLUGIN + ":copy-dependencies", "-DincludeScope=runtime",
        "-DoutputDirectory=" + str(runtime), cwd=ROOT)
    release = out / "release.jar"
    copied_primary = runtime / primary.name
    verify(copied_primary, previous.BINARY_HASH)
    copied_primary.rename(release)
    runtime_jars = sorted(runtime.glob("*.jar"))
    support_jars = sorted(support.glob("*.jar"))
    pins = json.loads((ROOT / "dependency-pins.json").read_text())
    for kind, jars in (("runtime", runtime_jars), ("build", support_jars)):
        actual = {p.name: sha(p) for p in jars}
        if actual != pins[kind]:
            raise SystemExit("Changed " + kind + " dependency hashes")
    classpath = os.pathsep.join(map(str, [release] + runtime_jars + support_jars))
    source = out / "source"
    path = source / (PACKAGE + "KafkaCommitter.java")
    path.parent.mkdir(parents=True)
    with ZipFile(sources) as archive:
        path.write_bytes(archive.read(PACKAGE + "KafkaCommitter.java"))
    run("patch", "--batch", "--forward", "-p1", "-i", ROOT / "discard-retriable.patch", cwd=source)
    classes = out / "classes"
    classes.mkdir()
    run(java / "javac", "--release", "11", "-cp", classpath, "-d", classes, path, cwd=ROOT)
    overlay = PACKAGE + "KafkaCommitter.class"
    if sorted(str(p.relative_to(classes)) for p in classes.rglob("*.class")) != [overlay]:
        raise SystemExit("Expected exactly one compiled class")
    mutant = out / "discard-retriable.jar"
    with ZipFile(release) as original, ZipFile(mutant, "w") as changed:
        for entry in original.infolist():
            changed.writestr(entry, (classes / overlay).read_bytes() if entry.filename == overlay else original.read(entry))
    with ZipFile(release) as original, ZipFile(mutant) as changed:
        if original.namelist() != changed.namelist():
            raise SystemExit("Archive entry list changed")
        changes = [name for name in original.namelist() if original.read(name) != changed.read(name)]
        if changes != [overlay]:
            raise SystemExit("Unexpected changed entries: " + str(changes))
    verify(mutant, MUTANT_HASH)
    tests = out / "test-classes"
    tests.mkdir()
    run(java / "javac", "--release", "11", "-cp", classpath, "-d", tests, ROOT / "DecisionCheck.java", cwd=ROOT)
    for mode, artifact in (("release", release), ("mutant", mutant)):
        cp = os.pathsep.join(map(str, [tests, artifact] + runtime_jars + support_jars))
        run(java / "java", "-cp", cp, "org.apache.flink.connector.kafka.sink.internal.DecisionCheck", mode, cwd=ROOT)
    inputs = [ROOT / "build.py", ROOT / "DecisionCheck.java", ROOT / "discard-retriable.patch", ROOT / "dependency-pins.json", ROOT / "check.py",
              ROOT.parent / "connector-mutants/build.py", ROOT.parent / "connector-mutants/pom.xml"]
    report = {"releaseSha256": sha(release), "sourceSha256": sha(sources), "mutantSha256": sha(mutant),
              "changedEntries": changes, "matchesPinnedMutant": sha(mutant) == MUTANT_HASH,
              "recipeInputs": {str(p.relative_to(ROOT.parent)): sha(p) for p in inputs},
              "runtimeDependencySha256": {p.name: sha(p) for p in runtime_jars},
              "buildDependencySha256": {p.name: sha(p) for p in support_jars},
              "unitChecks": "release/mutant: selected retries, unrelated retry, normal, fenced, unknown; live and recovered",
              "liveCalibration": "NOT RUN; quick must detect missing IDs; release and healthy control must pass"}
    (out / "build-evidence.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
