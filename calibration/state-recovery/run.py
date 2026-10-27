#!/usr/bin/env python3
"""Execute each of twelve frozen attempts once, retaining failures and never retrying a window miss."""
import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess

from check import guarded, load, read, require, validate_manifest

ROOT = Path(__file__).absolute().parents[2]


def api_version(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+\.[0-9]+", value):
        raise argparse.ArgumentTypeError("Docker API version must be numeric X.Y, for example 1.44")
    return value


def command(root, batch, cell, directory, maven, repository, settings, docker_api_version=None):
    arguments = shlex.join(["run", "--catalog-root", str(batch / cell["catalog"]),
                            "--scenario", cell["scenario"], "--artifact-root", str(root),
                            "--offline", "--kafka-log-output", str(directory / "kafka-logs")])
    argv = [maven, "-q", "-o", "-s", str(settings), "-gs", str(settings), "-Dmaven.repo.local=" + str(repository),
            "-f", str(root / "pom.xml"), "exec:java", "-pl", "cli", "-Dexec.args=" + arguments]
    if docker_api_version is not None:
        version = api_version(docker_api_version)
        argv.append("-Dapi.version=" + version)
        # Inherit the ordinary environment without enumerating or recording its contents.
        argv = ["/usr/bin/env", "DOCKER_API_VERSION=" + version, *argv]
    return argv


def retain_logs(root, directory):
    result = load(directory / "stdout.json", directory)
    checkpoint_root = guarded(Path(result["attempt"]["checkpointRoot"]), root)
    logs = directory / "logs"
    logs.mkdir(exist_ok=True)
    for pattern in ("flink-stability-output-*.log", "flink-stability-class-load-*.log"):
        for source in sorted(checkpoint_root.glob(pattern)):
            data = read(source, root)
            with (logs / source.name).open("xb") as target:
                target.write(data)


def run(batch, repository, maven="mvn", root=ROOT, docker_api_version=None):
    if docker_api_version is not None:
        docker_api_version = api_version(docker_api_version)
    batch, root = Path(batch).absolute(), Path(root).absolute()
    guarded(batch, root / "jobs")
    repository = Path(repository).absolute()
    guarded(repository, repository)
    require(repository.name == "repository" and repository.is_dir()
            and not {"Documents", "Photos", "Videos", "Downloads", "Desktop", "Pictures"}.intersection(repository.parts),
            "Expected an explicitly selected Maven repository/ artifact cache with installed reactor artifacts")
    manifest = validate_manifest(batch)
    require(not (batch / "execution-started.json").exists(), "This matrix already started; automatic resume/retry is prohibited")
    require(all(not (batch / relative).exists() for cell in manifest["cells"] for relative in cell["attempts"]),
            "An attempt directory already exists")
    with (batch / "execution-started.json").open("x") as receipt:
        json.dump({"attempts": 12, "automaticRetries": 0, "dockerApiVersion": docker_api_version}, receipt)
        receipt.write("\n")
    settings = batch / "execution-settings.xml"
    with settings.open("x") as receipt:
        receipt.write('<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"/>\n')
    for cell in manifest["cells"]:
        for relative in cell["attempts"]:
            validate_manifest(batch)
            directory = guarded(batch / relative, batch)
            directory.mkdir(parents=True, exist_ok=False)
            argv = command(root, batch, cell, directory, maven, repository, settings, docker_api_version)
            (directory / "command.json").write_text(json.dumps({"argv": argv, "cwd": str(directory)}, indent=2) + "\n")
            print("Running", relative, flush=True)
            with (directory / "stdout.json").open("x") as stdout, (directory / "stderr.log").open("x") as stderr:
                try:
                    code = subprocess.run(argv, cwd=directory, stdout=stdout, stderr=stderr, check=False).returncode
                except OSError as failure:
                    stderr.write(str(failure) + "\n")
                    code = "not-started"
            (directory / "exit-code.txt").write_text(str(code) + "\n")
            try:
                retain_logs(root, directory)
            except (ValueError, KeyError, TypeError, OSError) as failure:
                (directory / "log-retention-error.txt").write_text(str(failure) + "\n")
            print("Retained", relative, "exit", code, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch", type=Path)
    parser.add_argument("--maven", default="mvn")
    parser.add_argument("--maven-repo", type=Path, required=True,
                        help="Exact existing repository/ artifact cache with this checkout installed; settings are never discovered")
    parser.add_argument("--docker-api-version", type=api_version,
                        help="Set both DOCKER_API_VERSION and Maven api.version; use 1.44 for Docker Engine 29")
    args = parser.parse_args()
    run(args.batch, args.maven_repo, args.maven, docker_api_version=args.docker_api_version)


if __name__ == "__main__":
    main()
