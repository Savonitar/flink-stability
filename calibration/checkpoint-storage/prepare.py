#!/usr/bin/env python3
"""Discover the exact installed runtime classpath and connector closure offline."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).absolute().parents[2]
RECIPE = Path(__file__).absolute().parent
_spec = importlib.util.spec_from_file_location("checkpoint_storage_prepare_framework", RECIPE / "framework.py")
framework = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(framework)


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def prepare(output, repository, java_home, maven, *, root=ROOT, invoke=subprocess.run):
    """Read only the selected artifact cache; never consult user Maven settings."""
    root = framework.safe(root)
    output = framework.safe(output, root / "jobs")
    repository = framework.safe(repository)
    java_home = framework.safe(java_home)
    # Explicit executable symlinks such as Maven's current/bin/mvn are permitted;
    # their resolved destination must still pass the prohibited-path checks.
    framework.require(not framework.FORBIDDEN.intersection(Path(maven).absolute().parts), "Forbidden Maven executable")
    maven = framework.safe(Path(maven).resolve(strict=True))
    framework.require(repository.name == "repository" and repository.is_dir(), "Select the exact Maven repository/ artifact cache")
    framework.require(output != root / "jobs" and not output.exists(), "Use a fresh jobs/ child")
    for executable in (maven, java_home / "bin/java", java_home / "bin/javac"):
        framework.require(executable.is_file() and os.access(executable, os.X_OK), "Missing selected executable")
    output.mkdir(parents=True, exist_ok=False)
    for name in ("tmp", "classes", "home"):
        (output / name).mkdir()
    settings = output / "settings.xml"
    settings.write_bytes(framework.SETTINGS)
    environment = {"HOME": str(output / "home"), "JAVA_HOME": str(java_home),
                   "PATH": str(java_home / "bin") + ":/usr/bin:/bin:/usr/sbin:/sbin",
                   "TMPDIR": str(output / "tmp"), "MAVEN_SKIP_RC": "true",
                   "MAVEN_OPTS": "-Djava.io.tmpdir=" + str(output / "tmp")
                   + " -Duser.home=" + str(output / "home")}
    executions = []

    def run(label, argv):
        receipt = {"argv": argv, "cwd": str(output), "environment": environment, "timeoutSeconds": 120}
        (output / (label + ".command.json")).write_bytes(encoded(receipt))
        with (output / (label + ".stdout.log")).open("xb") as stdout, (output / (label + ".stderr.log")).open("xb") as stderr:
            try:
                result = invoke(argv, cwd=output, env=environment, stdin=subprocess.DEVNULL,
                                stdout=stdout, stderr=stderr, timeout=120, check=False)
                receipt["exitCode"] = result.returncode
            except (OSError, subprocess.TimeoutExpired) as failure:
                receipt.update(exitCode=None, error=str(failure))
        (output / (label + ".execution.json")).write_bytes(encoded(receipt))
        framework.require(receipt["exitCode"] == 0, "Preparation failed: " + label + "; see retained logs")
        executions.append(label)

    run("java-version", [str(java_home / "bin/java"), "-version"])
    version = (output / "java-version.stderr.log").read_text()
    framework.require('version "21.' in version, "JDK 21 is required")
    classpath_file = output / "runtime-classpath.txt"
    run("classpath", [str(maven), "-o", "-B", "-ntp", "-s", str(settings), "-gs", str(settings),
                      "-Dmaven.repo.local=" + str(repository), "-f", str(root / "pom.xml"), "-pl", "cli",
                      "org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath", "-DincludeScope=runtime",
                      "-Dmdep.outputFile=" + str(classpath_file)])
    classpath = framework.read(classpath_file, output, 1024 * 1024).decode().strip()
    paths = classpath.split(os.pathsep)
    framework.require(1 <= len(paths) <= 512 and len(paths) == len(set(paths)), "Invalid runtime classpath")
    for path in paths:
        framework.artifact_relative(Path(path), repository)
        framework.require(Path(path).suffix == ".jar", "Classpath contains a non-JAR")
        framework.jar_entries(Path(path))
    source = output / "ResolveClosure.java"
    source.write_bytes(framework.read(RECIPE / source.name, ROOT))
    (output / "prepare.py").write_bytes(framework.read(Path(__file__), ROOT))
    run("compile-closure", [str(java_home / "bin/javac"), "-J-Duser.home=" + str(output / "home"),
                            "-proc:none", "-cp", classpath,
                            "-d", str(output / "classes"), str(source)])
    closure = output / "maven-closure.txt"
    run("resolve-closure", [str(java_home / "bin/java"), "-Dmaven.repo.local=" + str(repository),
                            "-Duser.home=" + str(output / "home"), "-Djava.io.tmpdir=" + str(output / "tmp"),
                            "-cp", str(output / "classes") + os.pathsep + classpath,
                            "org.savonitar.flink.stability.core.artifact.ResolveClosure", str(closure)])
    artifacts = framework.read(closure, output, 1024 * 1024).decode().splitlines()
    framework.require(1 <= len(artifacts) <= 4096 and artifacts == sorted(set(artifacts)), "Invalid artifact closure")
    for path in artifacts:
        framework.artifact_relative(Path(path), repository)
    inputs = {"repository": str(repository), "classpath_file": str(classpath_file),
              "artifact_files": artifacts, "java": str(java_home / "bin/java")}
    (output / "framework-inputs.json").write_bytes(encoded(inputs))
    files = {path.relative_to(output).as_posix(): hashlib.sha256(framework.read(path, output)).hexdigest()
             for path in sorted(output.rglob("*")) if path.is_file()}
    (output / "preparation.json").write_bytes(encoded({"formatVersion": 1, "offline": True,
                "executions": executions, "files": files, "artifactCount": len(artifacts)}))
    return inputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--java-home", required=True, type=Path)
    parser.add_argument("--maven", required=True, type=Path)
    args = parser.parse_args()
    prepare(args.output, args.repository, args.java_home, args.maven)
    print(Path(args.output).absolute() / "framework-inputs.json")


if __name__ == "__main__":
    main()
