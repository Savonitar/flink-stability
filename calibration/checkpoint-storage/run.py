#!/usr/bin/env python3
"""Run the frozen twelve-attempt matrix once, serially, retaining every failure."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
from urllib.parse import urlsplit

RECIPE = Path(__file__).absolute().parent
ROOT = RECIPE.parents[1]
_spec = importlib.util.spec_from_file_location("checkpoint_storage_catalogs_runner", RECIPE / "catalogs.py")
catalogs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(catalogs)
require, guarded, read, load, digest = catalogs.require, catalogs.guarded, catalogs.read, catalogs.load, catalogs.digest


def write_json(path, value):
    with Path(path).open("xb") as handle:
        handle.write(catalogs.encoded(value))


def local_endpoint(value):
    parts = urlsplit(value)
    require(parts.scheme == "unix" and not parts.netloc and parts.path.startswith("/")
            and not parts.query and not parts.fragment and "%" not in parts.path
            and ".." not in Path(parts.path).parts and not catalogs.FORBIDDEN.intersection(Path(parts.path).parts),
            "An explicit local Unix Docker endpoint is required")
    return value


def minimal_environment(directory, endpoint, api_version):
    require(re.fullmatch(r"[0-9]+\.[0-9]+", api_version), "Invalid Docker API version")
    return {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(directory / "home"),
            "TMPDIR": str(directory / "tmp"), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC",
            "DOCKER_CONFIG": str(directory / "home/.docker"), "DOCKER_HOST": local_endpoint(endpoint),
            "DOCKER_API_VERSION": api_version}


def execute(argv, directory, environment, timeout, *, popen=subprocess.Popen):
    started = datetime.now(timezone.utc).isoformat()
    tick = time.monotonic()
    result = {"formatVersion": 1, "started": False, "completed": False, "termination": "spawn-error",
              "exitCode": None, "timeoutSeconds": timeout, "startedAt": started}
    with (directory / "stdout.json").open("xb") as stdout, (directory / "stderr.log").open("xb") as stderr:
        process = None
        try:
            process = popen(argv, cwd=directory, env=environment, stdin=subprocess.DEVNULL,
                            stdout=stdout, stderr=stderr, close_fds=True, start_new_session=True)
            result["started"] = True
            result["pid"] = process.pid
            result["exitCode"] = process.wait(timeout=timeout)
            result.update(completed=True, termination="exited")
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as failure:
            result["termination"] = "timeout" if isinstance(failure, subprocess.TimeoutExpired) else "interrupted"
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    result["exitCode"] = process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    try:
                        result["exitCode"] = process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        result["termination"] = "termination-unconfirmed"
        except OSError as failure:
            stderr.write((str(failure) + "\n").encode())
            result["error"] = str(failure)
    result["finishedAt"] = datetime.now(timezone.utc).isoformat()
    result["elapsedSeconds"] = time.monotonic() - tick
    with (directory / "exit-code.txt").open("x") as handle:
        handle.write(str(result["exitCode"] if result["exitCode"] is not None else result["termination"]) + "\n")
    return result


def retain_evidence(root, directory, total_records=30000):
    """Copy local receipts; container paths remain metadata and are never dereferenced."""
    result = load(directory / "stdout.json", directory)
    checkpoint = guarded(result["attempt"]["checkpointRoot"], root)
    require(checkpoint.name.startswith("attempt-") and
            (checkpoint.is_relative_to(directory / "checkpoints") or checkpoint.is_relative_to(root / "checkpoints")),
            "Checkpoint evidence path is outside the attempt namespace")
    sources = {}
    for path in sorted(checkpoint.rglob("*")):
        guarded(path, checkpoint)
        if path.is_file() and path.suffix in {".log", ".jsonl", ".json", ".yaml", ".conf", ".xml", ".txt"}:
            if path.name.startswith(("flink-stability-output-", "flink-stability-class-load-")):
                target, kind = "logs/" + path.name, "flink-log"
            elif path.parent.name == "checkpoint-storage-journals":
                target, kind = "native-journals/" + path.name, "native-journal"
            elif path.parent.name in {"raw-flink-rest", "raw-storage"}:
                target, kind = "raw-evidence/" + path.parent.name + "/" + path.name, "raw-response"
            else:
                target, kind = "checkpoint-evidence/" + str(path.relative_to(checkpoint)), "checkpoint-evidence"
            sources[str(path)] = (target, kind, None)
    def referenced(node):
        if isinstance(node, dict):
            if isinstance(node.get("journalPath"), str) and node["journalPath"]:
                source = guarded(node["journalPath"], checkpoint)
                expected = node.get("journalSha256")
                previous = sources.get(str(source))
                require(previous is None or previous[2] is None or previous[2] == expected, "Conflicting journal hash references")
                sources[str(source)] = ("native-journals/" + source.name, "native-journal", expected)
            for key, value in node.items():
                if key not in {"operations", "markers", "failure", "triggerResult"}:
                    referenced(value)
        elif isinstance(node, list):
            for value in node:
                referenced(value)
    referenced(result["evidence"])
    require(sum(value[1] != "raw-response" for value in sources.values()) <= 512
            and sum(value[1] == "raw-response" for value in sources.values()) <= 8192,
            "Evidence file count limit exceeded")
    receipts, total, journal_bytes, names = [], 0, 0, set()
    for source, (name, kind, expected) in sorted(sources.items()):
        require(name not in names, "Evidence filenames collide")
        names.add(name)
        data = read(source, checkpoint, expected)
        total += len(data)
        if kind == "native-journal":
            journal_bytes += len(data)
            require(len(data) <= 16 * 1024 * 1024 and journal_bytes <= 128 * 1024 * 1024, "Native journal retention exceeds limit")
        require(total <= 256 * 1024 * 1024, "Retained evidence exceeds byte budget")
        destination = directory / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as handle:
            handle.write(data)
        receipts.append({"sourcePath": source, "localPath": name, "sha256": digest(data), "kind": kind})
    evidence = result["evidence"]
    write_json(directory / "configuration-evidence.json", {
        "kind": "derived-cli-result-projection",
        "source": "stdout.json#/evidence/flinkRuntime", "config": evidence.get("flinkRuntime", {}).get("config"),
        "components": [{"runtimeId": value.get("runtimeId"), "effectiveConfiguration": value.get("effectiveConfiguration")}
                       for value in evidence.get("flinkRuntime", {}).get("components", [])]})
    write_json(directory / "input-evidence.json", {"kind": "derived-cli-result-projection",
               "source": "stdout.json#/evidence/input", "input": evidence.get("input")})
    require(type(total_records) is int and total_records == 30000, "Input manifest generation contract differs")
    declared = b"".join((str(number) + "\n").encode() for number in range(total_records))
    with (directory / "input-manifest.txt").open("xb") as handle:
        handle.write(declared)
    write_json(directory / "input-manifest.json", {"version": 1, "kind": "reconstructed-declared-integer-sequence",
               "source": "frozen catalog input_source", "path": "input-manifest.txt", "sha256": digest(declared),
               "firstId": 0, "endExclusive": total_records, "records": total_records,
               "inputReceiptComplete": evidence.get("input", {}).get("complete") is True})
    write_json(directory / "retained-evidence.json", {"version": 1, "files": receipts,
               "checkpointBinaryRetention": "Not required; metadata and state files may be deleted after natural completion."})
    return result


def cleanup_evidence(result, directory, docker, endpoint, environment, *, invoke=subprocess.run):
    evidence = result["evidence"]
    ids = {value["runtimeId"] for value in evidence["flinkRuntime"]["components"]}
    ids.update(value["containerId"] for value in evidence["kafka"]["containers"])
    require(ids and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in ids), "Missing exact container identities")
    storage = evidence["checkpointStorage"]["storageId"]
    require(re.fullmatch(r"storage-[0-9a-f-]{36}", storage), "Missing exact storage identity")
    def inspect(arguments):
        argv = [str(docker), "--host", local_endpoint(endpoint), *arguments]
        try:
            observed = invoke(argv, cwd=directory, env=environment, stdin=subprocess.DEVNULL,
                              capture_output=True, text=True, timeout=15, check=False)
            require(len(observed.stdout) + len(observed.stderr) <= 16384, "Cleanup observation exceeds limit")
            return {"argv": argv, "exitCode": observed.returncode, "stdout": observed.stdout, "stderr": observed.stderr}
        except (OSError, subprocess.TimeoutExpired, ValueError) as failure:
            return {"argv": argv, "exitCode": None, "stdout": "", "stderr": str(failure)}
    def absent(receipt):
        return receipt["exitCode"] == 0 and not receipt["stdout"].strip() and not receipt["stderr"].strip()
    containers = []
    for identity in sorted(ids):
        receipt = inspect(["container", "ls", "-a", "--no-trunc", "--filter", "id=" + identity, "--format", "{{.ID}}"])
        containers.append({"runtimeId": identity, "absent": absent(receipt), "receipt": receipt})
    receipt = inspect(["volume", "ls", "--filter", "label=org.savonitar.flink-stability.storage=" + storage, "--format", "{{.Name}}"])
    value = {"version": 1, "complete": all(row["absent"] for row in containers) and absent(receipt),
             "containers": containers, "storage": {"storageId": storage, "absent": absent(receipt), "receipt": receipt}}
    write_json(directory / "cleanup.json", value)
    return value


def prepare_directory(directory):
    directory.mkdir(parents=True, exist_ok=False)
    for child in ("home/.docker", "tmp"):
        (directory / child).mkdir(parents=True, exist_ok=False)
    with (directory / "home/.docker/config.json").open("x") as handle:
        handle.write('{"auths":{}}\n')
    with (directory / "settings.xml").open("x") as handle:
        handle.write('<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"/>\n')


def command_receipt(directory, argv, environment, framework_hash):
    command = {"argv": argv, "cwd": str(directory), "environment": environment,
               "frameworkManifestSha256": framework_hash,
               "settingsSha256": digest(read(directory / "settings.xml", directory)),
               "dockerConfigSha256": digest(read(directory / "home/.docker/config.json", directory))}
    write_json(directory / "command.json", command)


def preflight(batch, manifest, manifest_hash, framework, endpoint, api_version):
    require(not (batch / "validation-started.json").exists() and not (batch / "validation").exists(),
            "Validation already started; repeated preparation is prohibited")
    write_json(batch / "validation-started.json", {"formatVersion": 1, "manifestSha256": manifest_hash,
               "frameworkManifestSha256": manifest["frameworkFreeze"]["sha256"]})
    validated = []
    complete = True
    for cell in manifest["cells"]:
        require(digest(read(batch / "manifest.json", batch)) == manifest_hash, "Frozen manifest changed during validation")
        catalogs.validate_frozen(batch)
        directory = guarded(batch / "validation" / cell["name"], batch)
        prepare_directory(directory)
        environment = minimal_environment(directory, endpoint, api_version)
        argv = framework.command(batch / "framework", ["validate", "--catalog-root", str(batch / cell["catalog"]),
                    "--scenario", cell["scenario"], "--artifact-root", str(batch), "--offline", "--show-plan"],
                    api_version, work_directory=directory)
        command_receipt(directory, argv, environment, manifest["frameworkFreeze"]["sha256"])
        observed = execute(argv, directory, environment, 120)
        observed.update(frameworkManifestSha256=manifest["frameworkFreeze"]["sha256"],
                        commandSha256=digest(read(directory / "command.json", directory)))
        write_json(directory / "execution.json", observed)
        validated.append(cell["name"])
        if observed["termination"] != "exited" or observed["exitCode"] != 0 or not observed["completed"]:
            complete = False
            break
    write_json(batch / "validation-finished.json", {"formatVersion": 1, "complete": complete, "validated": validated})
    return complete


def run(batch, docker_endpoint, docker, *, root=ROOT, docker_api_version="1.44"):
    root, batch = Path(root).absolute(), Path(batch).absolute()
    guarded(batch, root / "jobs")
    local_endpoint(docker_endpoint)
    docker = guarded(docker, Path(Path(docker).absolute().anchor))
    require(docker.is_file() and os.access(docker, os.X_OK), "Explicit Docker executable is required")
    manifest = catalogs.validate_frozen(batch)
    manifest_hash = digest(read(batch / "manifest.json", batch))
    require(not (batch / "execution-started.json").exists(), "Matrix already started; resume/retry/replacement is prohibited")
    require(not (batch / "attempts").exists(), "Attempt namespace already exists")
    framework = catalogs.module("framework_runner", batch / "evidence/recipe/framework.py")
    framework.validate_framework(batch / "framework")
    if not preflight(batch, manifest, manifest_hash, framework, docker_endpoint, docker_api_version):
        return False
    write_json(batch / "execution-started.json", {"formatVersion": 1, "attempts": 12, "automaticRetries": 0,
               "dockerEndpoint": docker_endpoint, "dockerApiVersion": docker_api_version,
               "manifestSha256": manifest_hash,
               "frameworkManifestSha256": manifest["frameworkFreeze"]["sha256"]})
    attempted, stopped = [], None
    for cell in manifest["cells"]:
        for relative in cell["attempts"]:
            require(digest(read(batch / "manifest.json", batch)) == manifest_hash, "Frozen manifest changed after execution started")
            catalogs.validate_frozen(batch)
            framework.validate_framework(batch / "framework")
            directory = guarded(batch / relative, batch)
            prepare_directory(directory)
            environment = minimal_environment(directory, docker_endpoint, docker_api_version)
            argv = framework.command(batch / "framework", ["run", "--catalog-root", str(batch / cell["catalog"]),
                        "--scenario", cell["scenario"], "--artifact-root", str(batch), "--offline",
                        "--kafka-log-output", str(directory / "kafka-logs")], docker_api_version, work_directory=directory)
            command_receipt(directory, argv, environment, manifest["frameworkFreeze"]["sha256"])
            print("Running", relative, flush=True)
            observed = execute(argv, directory, environment, manifest["outerTimeoutSeconds"])
            observed.update(frameworkManifestSha256=manifest["frameworkFreeze"]["sha256"],
                            commandSha256=digest(read(directory / "command.json", directory)),
                            retentionComplete=False, retentionErrors=[])
            cleanup = {"version": 1, "complete": False, "containers": [], "storage": None}
            try:
                result = retain_evidence(root, directory, manifest["workload"]["totalRecords"])
                observed["retentionComplete"] = True
                cleanup = cleanup_evidence(result, directory, docker, docker_endpoint, environment)
            except (ValueError, KeyError, TypeError, OSError) as failure:
                observed["retentionErrors"].append(str(failure))
                if not (directory / "cleanup.json").exists():
                    cleanup["error"] = str(failure)
                    write_json(directory / "cleanup.json", cleanup)
            write_json(directory / "execution.json", observed)
            attempted.append(relative)
            print("Retained", relative, observed["termination"], "exit", observed["exitCode"], flush=True)
            if observed["termination"] != "exited" or not cleanup["complete"]:
                stopped = relative
                break
        if stopped:
            break
    write_json(batch / "execution-finished.json", {"formatVersion": 1, "attempted": attempted,
               "complete": len(attempted) == 12 and stopped is None, "stoppedAt": stopped, "automaticRetries": 0})
    return stopped is None and len(attempted) == 12


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch", type=Path)
    parser.add_argument("--workspace-root", type=Path, default=ROOT)
    parser.add_argument("--docker-endpoint", required=True)
    parser.add_argument("--docker", type=Path, required=True)
    parser.add_argument("--docker-api-version", default="1.44")
    args = parser.parse_args()
    raise SystemExit(0 if run(args.batch, args.docker_endpoint, args.docker, root=args.workspace_root,
                             docker_api_version=args.docker_api_version) else 1)


if __name__ == "__main__":
    main()
