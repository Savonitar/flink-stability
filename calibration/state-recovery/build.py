#!/usr/bin/env python3
"""Freeze and build both state-recovery workload arms offline, without containers."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[2]
MODULE = "flink-job-generator"
IDENTITY = "META-INF/flink-stability/keyed-prefix-build.properties"
MODES = {"correct": "correct", "mutant": "reset-state-on-restore-v1"}
SENSITIVE_NAME = re.compile(
    r"(^|[._-])(secrets?|credentials?|passwords?|private[-_]?key|keychain|keystore|"
    r"id_rsa|id_ed25519|access[-_]?token|auth[-_]?token|api[-_]?key)([._-]|$)",
    re.IGNORECASE)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def local_path(path, root):
    path = path.absolute()
    if ".." in path.parts or not path.is_relative_to(root):
        raise ValueError("Path must be inside " + str(root))
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise ValueError("Symlink paths are not supported: " + str(candidate))
    return path


def artifact_cache_path(path):
    """The caller supplies the cache itself; never discover or inspect parent Maven settings."""
    path = path.absolute()
    if ".." in path.parts or path.name != "repository":
        raise ValueError("Supply the exact Maven repository/ artifact-cache directory")
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise ValueError("Symlink cache paths are not supported: " + str(candidate))
    if not path.is_dir():
        raise ValueError("The supplied Maven artifact cache does not exist")
    return path


def cache_entry_allowed(name):
    if SENSITIVE_NAME.search(name):
        return False
    return (name.endswith((".jar", ".pom", ".jar.sha1", ".pom.sha1", ".jar.sha256", ".pom.sha256",
                           ".jar.sha512", ".pom.sha512", ".jar.md5", ".pom.md5"))
            or name in ("_remote.repositories", "resolver-status.properties")
            or re.fullmatch(r"maven-metadata(?:-[A-Za-z0-9_.-]+)?\.xml(?:\.(?:sha1|sha256|sha512|md5))?", name)
            is not None)


def seed_cache(source, destination):
    """Clone ordinary artifacts into an isolated cache; never hardlink mutable build inputs."""
    destination.mkdir(parents=True, exist_ok=False)
    clone = None
    if sys.platform == "darwin":
        library = ctypes.CDLL(None, use_errno=True)
        clone = getattr(library, "clonefile", None)
        if clone is not None:
            clone.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int)
            clone.restype = ctypes.c_int
    counts = {"cloned": 0, "copied": 0}
    for directory, subdirectories, filenames in os.walk(source, followlinks=False):
        directory = Path(directory)
        for name in list(subdirectories):
            child = directory / name
            if child.is_symlink():
                raise ValueError("Symlink Maven cache entry is unsupported: " + str(child.relative_to(source)))
            if name.startswith(".") or SENSITIVE_NAME.search(name):
                subdirectories.remove(name)
        for name in sorted(filenames):
            original = directory / name
            if original.is_symlink():
                raise ValueError("Symlink Maven cache entry is unsupported: " + str(original.relative_to(source)))
            if not cache_entry_allowed(name) or not original.is_file():
                continue
            copied = destination / original.relative_to(source)
            copied.parent.mkdir(parents=True, exist_ok=True)
            if clone is not None and clone(os.fsencode(original), os.fsencode(copied), 0) == 0:
                counts["cloned"] += 1
            else:
                shutil.copyfile(original, copied)
                counts["copied"] += 1
    return {"source": str(source), "path": str(destination), "files": sum(counts.values()), **counts}


def source_inputs():
    selected = [ROOT / "pom.xml", ROOT / MODULE / "pom.xml", ROOT / MODULE / "WorkloadBuildGuard.java"]
    for source_root in ("src/main", "src/test", "src/build-identity"):
        directory = ROOT / MODULE / source_root
        for path in sorted(directory.rglob("*")):
            local_path(path, ROOT)
            if path.is_file() and path.suffix in (".java", ".properties"):
                selected.append(path)
    return selected


def jar_contents(path):
    with ZipFile(path) as jar:
        names = jar.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate JAR entries: " + str(path))
        return {name: jar.read(name) for name in names if not name.endswith("/")}


def inspect_pair(correct, mutant):
    arms = {"correct": jar_contents(correct), "mutant": jar_contents(mutant)}
    for arm, entries in arms.items():
        if entries.get(IDENTITY) != ("mode=" + MODES[arm] + "\n").encode("ascii"):
            raise ValueError("Wrong packaged build identity for " + arm)
        manifest = entries.get("META-INF/MANIFEST.MF", b"").replace(b"\r\n ", b"")
        if b"Flink-Stability-Workload-Protocol: v1\r\n" not in manifest:
            raise ValueError("Missing protocol marker for " + arm)
        if b"Flink-Stability-Workload-Capabilities: keyed-prefix-v1\r\n" not in manifest:
            raise ValueError("Missing capability marker for " + arm)
        expected = ("Flink-Stability-Workload-Build-Mode: " + MODES[arm] + "\r\n").encode("ascii")
        if expected not in manifest:
            raise ValueError("Wrong manifest build identity for " + arm)
        for name, content in entries.items():
            if name.startswith(("org/apache/flink/", "org/apache/kafka/")):
                raise ValueError("Workload must remain thin: " + name)
            if name.endswith(".class") and (len(content) < 8 or content[:4] != b"\xca\xfe\xba\xbe"
                                           or int.from_bytes(content[6:8], "big") != 55):
                raise ValueError("Workload class is not release-11 bytecode: " + name)
    if set(arms["correct"]) != set(arms["mutant"]):
        raise ValueError("The two build arms have different JAR entry sets")
    changed = sorted(name for name in arms["correct"] if arms["correct"][name] != arms["mutant"][name])
    if changed != sorted(["META-INF/MANIFEST.MF", IDENTITY]):
        raise ValueError("Unexpected arm differences: " + str(changed))
    return changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--maven-repo", type=Path, required=True,
                        help="Exact existing Maven repository/ artifact cache to clone; no settings are read")
    parser.add_argument("--output", type=Path, required=True, help="Fresh directory under jobs/")
    args = parser.parse_args()
    cache_source = artifact_cache_path(args.maven_repo)
    output = local_path(args.output, ROOT / "jobs")
    if output == ROOT / "jobs":
        raise ValueError("Use a fresh child of jobs/")
    java_home = args.java_home.absolute()
    environment = {"JAVA_HOME": str(java_home),
                   "PATH": str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"),
                   "MAVEN_SKIP_RC": "1"}
    version = subprocess.run([str(java_home / "bin/javac"), "-version"], check=True,
                             env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True).stdout.strip()
    if not version.startswith("javac 21."):
        raise ValueError("JDK 21 is required; found " + version)
    output.mkdir(parents=True, exist_ok=False)
    temporary = output / "tmp"
    temporary.mkdir()
    environment["TMPDIR"] = str(temporary)
    environment["MAVEN_OPTS"] = "-Djava.io.tmpdir=" + str(temporary)
    settings = output / "settings.xml"
    settings.write_text('<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"/>\n', encoding="utf-8")
    selected = source_inputs()
    hashes = {path.relative_to(ROOT).as_posix(): sha(path) for path in selected}
    script = Path(__file__).absolute()
    recipe_hashes = {**hashes, script.relative_to(ROOT).as_posix(): sha(script)}
    snapshots = {}
    for arm in MODES:
        snapshot = output / "sources" / arm
        for source in selected:
            relative = source.relative_to(ROOT)
            destination = snapshot / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            if sha(destination) != hashes[relative.as_posix()]:
                raise ValueError("Source changed while freezing: " + str(relative))
        snapshots[arm] = {"path": str(snapshot), "sha256": hashes}
    shutil.copyfile(script, output / "build-recipe.py")
    if sha(output / "build-recipe.py") != recipe_hashes[script.relative_to(ROOT).as_posix()]:
        raise ValueError("Build recipe changed while freezing")
    repository = output / "cache" / "repository"
    cache = seed_cache(cache_source, repository)
    (output / "source-manifest.json").write_text(json.dumps({
        "version": 1, "sourceSnapshots": snapshots, "recipeInputSha256": recipe_hashes, "cache": cache,
    }, indent=2) + "\n", encoding="utf-8")

    artifacts = {}
    commands = {}
    for arm, mode in MODES.items():
        build = output / "build" / arm
        command = ["mvn", "-B", "-ntp", "-o", "-s", str(settings), "-gs", str(settings),
                   "-Dmaven.repo.local=" + str(repository), "-Dworkload.build.directory=" + str(build),
                   "-f", str(Path(snapshots[arm]["path"]) / MODULE / "pom.xml")]
        if arm == "mutant":
            command.append("-Pstate-reset-on-restore")
        command.append("package")
        commands[arm] = command
        with (output / (arm + "-build.log")).open("wb") as log:
            subprocess.run(command, cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)
        name = "flink-job-generator" + ("-state-reset-on-restore" if arm == "mutant" else "") + ".jar"
        artifact = build / name
        artifacts[arm] = {"path": str(artifact), "sha256": sha(artifact), "buildMode": mode}
    changed = inspect_pair(Path(artifacts["correct"]["path"]), Path(artifacts["mutant"]["path"]))
    evidence = {
        "version": 1, "mutation": MODES["mutant"],
        "mutationScope": "Keyed aggregate only; once per key on first event after genuine restore; initial start, source and sink unchanged",
        "artifacts": artifacts, "changedEntries": changed, "sourceSnapshots": snapshots,
        "recipeInputSha256": recipe_hashes, "commands": commands, "cache": cache,
        "javaVersion": version, "releaseBytecodeTarget": 11,
        "unitChecks": "Both artifact modes pass the module tests; all class bytes identical",
        "runtimeValidation": "pending; no Docker or recovery qualification performed by this recipe",
    }
    (output / "build-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print("STATE_RECOVERY_BUILD_PASS", output / "build-evidence.json")


if __name__ == "__main__":
    main()
