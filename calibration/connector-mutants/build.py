#!/usr/bin/env python3
"""Offline experimental timing control and two live-producer wrong-decision overlays."""
import json
import hashlib
import subprocess
import os
from pathlib import Path
import shutil
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "target"
PACKAGE = "org/apache/flink/connector/kafka/sink/internal/"
NAME = "flink-connector-kafka-5.0.0-2.2"
SOURCE_HASH = "c05dcdc7d7256575f10f784c728f983b517058daee517ae258021edd1e87ecfe"
BINARY_HASH = "6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da"
PLUGIN = "org.apache.maven.plugins:maven-dependency-plugin:3.8.1"
TESTED_HASHES = {
    "control": "e1bf6f60fdde6a8bc8a3dc87b0e8bfb9fa21c11294ec40c0ccb874f26bce0b3b",
    "assume": "d01420b0ba3d19c9bd73abfa1c6ef143cdc3769d526abbd316ee92962f867401",
    "rewrite": "11bfffc1fb5008c4ebb5e33f21d0123591019a5e630c0a4d9772590aecef1676",
}


def run(*args, cwd=ROOT):
    subprocess.run([str(arg) for arg in args], cwd=cwd, check=True)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(path, expected):
    actual = sha(path)
    if actual != expected:
        raise SystemExit(f"Unexpected SHA-256 for {path.name}: {actual}; expected {expected}")


def main():
    if not os.environ.get("JAVA_HOME"):
        raise SystemExit("Set JAVA_HOME and PATH to the tested JDK 21.0.7 before running.")
    OUT.mkdir(exist_ok=True)
    run(Path(os.environ["JAVA_HOME"]) / "bin/java", "-version")
    run("mvn", "-B", "-o", PLUGIN + ":copy",
        "-Dartifact=org.apache.flink:flink-connector-kafka:5.0.0-2.2:jar:sources",
        "-DoutputDirectory=" + str(OUT))
    verify(OUT / (NAME + "-sources.jar"), SOURCE_HASH)
    build_dependencies = OUT / "build-dependencies"
    if build_dependencies.exists(): shutil.rmtree(build_dependencies)
    run("mvn", "-B", "-o", PLUGIN + ":copy-dependencies",
        "-DoutputDirectory=" + str(build_dependencies))
    runtime = OUT / "runtime"
    if runtime.exists(): shutil.rmtree(runtime)
    resolver = OUT / "runtime-resolver"
    resolver.mkdir(exist_ok=True)
    (resolver / "pom.xml").write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>local.calibration</groupId>
<artifactId>runtime-closure</artifactId><version>1</version><dependencies><dependency>
<groupId>org.apache.flink</groupId><artifactId>flink-connector-kafka</artifactId>
<version>5.0.0-2.2</version></dependency></dependencies></project>''')
    run("mvn", "-B", "-o", "-f", resolver / "pom.xml", PLUGIN + ":copy-dependencies",
             "-DincludeScope=runtime", "-DoutputDirectory=" + str(runtime))
    primary = runtime / (NAME + ".jar")
    verify(primary, BINARY_HASH)
    runtime_jars = sorted(runtime.glob("*.jar"))
    # The release runtime closure wins over conflicting versions needed only to compile/test.
    support_jars = sorted(build_dependencies.glob("*.jar"))
    classpath = os.pathsep.join(map(str, runtime_jars + support_jars))
    java_home = Path(os.environ["JAVA_HOME"])
    artifacts = {}
    for mode in ["control", "assume", "rewrite"]:
        work = OUT / mode
        if work.exists(): shutil.rmtree(work)
        source = work / "source"
        classes = work / "classes"
        classes.mkdir(parents=True)
        with ZipFile(OUT / (NAME + "-sources.jar")) as archive:
            names = ["FlinkKafkaInternalProducer"] + ([] if mode == "control" else ["KafkaCommitter"])
            for name in names:
                path = source / (PACKAGE + name + ".java")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(archive.read(PACKAGE + name + ".java"))
        patches = ["timing-control.patch"]
        if mode == "assume": patches.append("assume-and-discard.patch")
        if mode == "rewrite":
            patches += ["live-replay-buffer.patch", "rewrite-on-timeout.patch"]
            shutil.copyfile(ROOT / "CalibrationReplay.java", source / (PACKAGE + "CalibrationReplay.java"))
        for patch in patches:
            run("patch", "--batch", "--forward", "-p1", "-i", ROOT / patch, cwd=source)
        run(java_home / "bin/javac", "--release", "11", "-cp", classpath,
                 "-d", classes, *sorted(source.rglob("*.java")))
        overlays = {path.relative_to(classes).as_posix(): path.read_bytes()
                    for path in classes.rglob("*.class")}
        artifact = OUT / (NAME + "-experimental-" + mode + ".jar")
        with ZipFile(primary) as original, ZipFile(artifact, "w") as result:
            for entry in original.infolist():
                result.writestr(entry, overlays.pop(entry.filename, original.read(entry)))
            for name, content in sorted(overlays.items()):
                info = ZipInfo(name, (1980, 1, 1, 0, 0, 0))
                info.compress_type = ZIP_DEFLATED
                result.writestr(info, content)
        with ZipFile(primary) as original, ZipFile(artifact) as changed:
            changes = sorted(name for name in changed.namelist()
                             if name not in original.namelist() or changed.read(name) != original.read(name))
            allowed = {PACKAGE + "FlinkKafkaInternalProducer.class",
                       PACKAGE + "FlinkKafkaInternalProducer$TransactionState.class"}
            if mode != "control": allowed.add(PACKAGE + "KafkaCommitter.class")
            if mode == "rewrite": allowed.add(PACKAGE + "CalibrationReplay.class")
            if not set(changes) <= allowed:
                raise SystemExit(f"Unexpected overlay classes: {changes}")
        verify(artifact, TESTED_HASHES[mode])
        artifacts[mode] = {"artifact": str(artifact), "sha256": sha(artifact), "changedEntries": changes,
                           "patchSha256": {name: sha(ROOT / name) for name in patches}}
    tests = OUT / "test-classes"
    tests.mkdir(exist_ok=True)
    run(java_home / "bin/javac", "--release", "17", "-cp",
             artifacts["rewrite"]["artifact"] + os.pathsep + classpath,
             "-d", tests, ROOT / "ExperimentCheck.java")
    for mode, artifact in artifacts.items():
        jars = [tests, artifact["artifact"]] + [path for path in runtime_jars if path != primary] + support_jars
        run(java_home / "bin/java", "-cp", os.pathsep.join(map(str, jars)),
                 "org.apache.flink.connector.kafka.sink.internal.ExperimentCheck", mode)
        snippet = "artifact: " + artifact["artifact"] + "\nruntime_dependencies:\n"
        snippet += "".join("  - " + str(path) + "\n" for path in runtime_jars if path != primary)
        (OUT / (mode + "-subject.yaml")).write_text(snippet)
    report = {"experimental": True, "sourceSha256": SOURCE_HASH, "releaseSha256": BINARY_HASH,
              "matchesTestedArtifactHashes": True, "artifacts": artifacts,
              "recipeInputSha256": {name: sha(ROOT / name) for name in
                                    ["build.py", "pom.xml", "CalibrationReplay.java", "ExperimentCheck.java",
                                     "timing-control.patch", "assume-and-discard.patch",
                                     "live-replay-buffer.patch", "rewrite-on-timeout.patch"]},
              "runtimeDependencySha256": {path.name: sha(path) for path in runtime_jars if path != primary},
              "helperSha256": sha(ROOT / "CalibrationReplay.java"),
              "runtimeValidation": "see README.md (Reproduce, Results); this command runs only unit checks",
              "unitChecks": "shared timing, unchanged commit outcomes, live/recovered decisions, bounded replay",
              "kafkaClientsVersion": "4.2.0"}
    (OUT / "build-evidence.json").write_text(json.dumps(report, indent=2) + "\n")
    print("EXPERIMENT_BUILD_PASS", OUT)


if __name__ == "__main__":
    main()
