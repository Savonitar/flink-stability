#!/usr/bin/env python3
"""Build the pinned recovery-offset mutant offline; run checks without a broker."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent
HARNESS = ROOT.parents[1]
OUT = ROOT / "target"
NAME = "flink-connector-kafka-5.0.0-2.2"
SOURCE_HASH = "c05dcdc7d7256575f10f784c728f983b517058daee517ae258021edd1e87ecfe"
BINARY_HASH = "6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da"
PACKAGE = "org/apache/flink/connector/kafka/source/reader/"
SOURCE_ENTRY = PACKAGE + "KafkaSourceReader.java"
CLASS_ENTRY = PACKAGE + "KafkaSourceReader.class"
PLUGIN = "org.apache.maven.plugins:maven-dependency-plugin:3.8.1"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(path, expected):
    actual = sha(path)
    if actual != expected:
        raise SystemExit(f"Unexpected SHA-256 for {path.name}: {actual}; expected {expected}")


def local_path(path, directory):
    path = path.absolute()
    if ".." in path.parts or not path.is_relative_to(HARNESS):
        raise SystemExit("Maven cache and settings must be inside the harness repository.")
    cursor = HARNESS
    for part in path.relative_to(HARNESS).parts:
        cursor /= part
        if cursor.is_symlink():
            raise SystemExit("Symlink Maven paths are not accepted: " + str(cursor))
    if not (path.is_dir() if directory else path.is_file()):
        raise SystemExit("Required local Maven path does not exist: " + str(path))
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", type=Path, required=True, help="JDK 21 directory")
    parser.add_argument("--maven-repo", type=Path, required=True, help="Existing repository-local Maven cache")
    parser.add_argument("--maven-settings", type=Path, required=True, help="Repository-local Maven settings XML")
    args = parser.parse_args()
    repository = local_path(args.maven_repo, True)
    settings = local_path(args.maven_settings, False)
    java_home = args.java_home.absolute()
    java = java_home / "bin/java"
    javac = java_home / "bin/javac"
    environment = {
        "JAVA_HOME": str(java_home),
        "PATH": str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"),
        "MAVEN_SKIP_RC": "1",
    }
    # Carry explicitly supplied JVM options without inheriting other environment data.
    if "JDK_JAVA_OPTIONS" in os.environ:
        environment["JDK_JAVA_OPTIONS"] = os.environ["JDK_JAVA_OPTIONS"]
    # Preserve previous artifacts and partial failures; the caller must archive them first.
    OUT.mkdir(exist_ok=False)
    user_home = OUT / "java-user-home"
    temporary = OUT / "tmp"
    user_home.mkdir()
    temporary.mkdir()
    jvm_options = ["-Duser.home=" + str(user_home), "-Djava.io.tmpdir=" + str(temporary)]
    javac_options = ["-J" + option for option in jvm_options]
    environment["MAVEN_OPTS"] = " ".join(shlex.quote(option) for option in jvm_options)

    def run(*arguments, cwd=ROOT):
        subprocess.run([str(arg) for arg in arguments], cwd=cwd, env=environment, check=True)

    compiler_version = subprocess.run(
        [str(javac), *javac_options, "-version"], env=environment, check=True,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.strip()
    if not compiler_version.startswith("javac 21."):
        raise SystemExit("The recovery recipe requires JDK 21; found " + compiler_version)
    # Use the supplied settings for both scopes: never implicitly load ~/.m2/settings.xml.
    maven = ["mvn", "-B", "-o", "-ntp", "-s", settings, "-gs", settings,
             "-Dmaven.repo.local=" + str(repository), "-f", ROOT / "pom.xml"]
    run(*maven, PLUGIN + ":copy",
        "-Dartifact=org.apache.flink:flink-connector-kafka:5.0.0-2.2:jar:sources",
        "-DoutputDirectory=" + str(OUT))
    sources = OUT / (NAME + "-sources.jar")
    verify(sources, SOURCE_HASH)
    support = OUT / "build-dependencies"
    runtime = OUT / "runtime"
    run(*maven, PLUGIN + ":copy-dependencies", "-DoutputDirectory=" + str(support))
    run(*maven, PLUGIN + ":copy-dependencies", "-DincludeScope=runtime",
        "-DoutputDirectory=" + str(runtime))
    primary = runtime / (NAME + ".jar")
    verify(primary, BINARY_HASH)
    runtime_jars = sorted(runtime.glob("*.jar"))
    support_jars = sorted(support.glob("*.jar"))
    # Release runtime dependencies take precedence over compile-only transitive versions.
    classpath = os.pathsep.join(map(str, runtime_jars + support_jars))
    source = OUT / "source"
    source_file = source / SOURCE_ENTRY
    source_file.parent.mkdir(parents=True)
    with ZipFile(sources) as archive:
        source_file.write_bytes(archive.read(SOURCE_ENTRY))
    run("patch", "--batch", "--forward", "-p1", "-i", ROOT / "recovery-offset.patch", cwd=source)
    classes = OUT / "classes"
    classes.mkdir()
    run(javac, *javac_options, "--release", "11", "-cp", classpath, "-d", classes, source_file)
    overlays = {path.relative_to(classes).as_posix(): path.read_bytes()
                for path in classes.rglob("*.class")}
    if set(overlays) != {CLASS_ENTRY}:
        raise SystemExit("Unexpected compiled overlay entries: " + str(sorted(overlays)))
    mutant = OUT / (NAME + "-recovery-offset-mutant.jar")
    with ZipFile(primary) as original, ZipFile(mutant, "w") as changed:
        if CLASS_ENTRY not in original.namelist():
            raise SystemExit("Release JAR has no KafkaSourceReader class to replace")
        for entry in original.infolist():
            changed.writestr(entry, overlays.get(entry.filename, original.read(entry)))
    with ZipFile(primary) as original, ZipFile(mutant) as changed:
        if original.namelist() != changed.namelist():
            raise SystemExit("Overlay changed the release JAR entry list")
        changes = [name for name in original.namelist()
                   if original.read(name) != changed.read(name)]
        if changes != [CLASS_ENTRY]:
            raise SystemExit("Unexpected changed JAR entries: " + str(changes))
        for name in ("META-INF/LICENSE", "META-INF/NOTICE"):
            if original.read(name) != changed.read(name):
                raise SystemExit("Overlay changed " + name)

    tests = OUT / "test-classes"
    tests.mkdir()
    run(javac, *javac_options, "--release", "11", "-cp", classpath,
        "-d", tests, ROOT / "RecoveryCheck.java")
    artifacts = {
        "release": {"artifact": str(primary), "sha256": sha(primary), "changedEntries": []},
        "mutant": {"artifact": str(mutant), "sha256": sha(mutant), "changedEntries": changes},
    }
    # Remove every release-primary duplicate, including the build-only dependency copy.
    dependencies = [path for path in runtime_jars + support_jars if path.name != NAME + ".jar"]
    for mode, info in artifacts.items():
        test_classpath = os.pathsep.join(map(str, [tests, info["artifact"]] + dependencies))
        run(java, *jvm_options, "-cp", test_classpath,
            "org.apache.flink.connector.kafka.source.reader.RecoveryCheck", mode)
    recipe_inputs = ["build.py", "pom.xml", "RecoveryCheck.java", "recovery-offset.patch", "README.md"]
    evidence = {
        "experimental": True,
        "sourceSha256": SOURCE_HASH,
        "releaseSha256": BINARY_HASH,
        "artifacts": artifacts,
        "runtimeDependencySha256": {path.name: sha(path) for path in runtime_jars if path != primary},
        "buildDependencySha256": {path.name: sha(path) for path in support_jars},
        "recipeInputSha256": {name: sha(ROOT / name) for name in recipe_inputs},
        "compilerVersion": compiler_version,
        "releaseBytecodeTarget": 11,
        "matchesTestedArtifactHashes": False,
        "runtimeValidation": "pending Docker calibration; this command runs only no-broker checks",
        "unitChecks": "actual snapshot/addSplits APIs, serializer round trip, live and committed offsets, boundaries",
        "kafkaClientsVersion": "4.2.0",
    }
    (OUT / "build-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("RECOVERY_MUTANT_BUILD_PASS", OUT)


if __name__ == "__main__":
    main()
