#!/usr/bin/env python3
"""Bounded, opt-in FUSE feasibility probe on one explicitly local Docker daemon."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time
import uuid

ROOT = Path(__file__).absolute().parents[1]
LABEL = "org.savonitar.flink.checkpoint-fuse"
JAVA_MAIN = "org.savonitar.flink.stability.testcontainers.storage.CheckpointStorageIoProbe"
BASE = b"before-fault"
AFTER = b"-after-heal"
MAX_OUTPUT = 12 * 1024 * 1024
IO_COUNTERS = frozenset(("read", "getattr", "readdir", "access", "flush", "release", "fsync", "fdatasync"))


def io_counters(value):
    require(isinstance(value, dict) and set(value) == IO_COUNTERS, "Missing or invalid native I/O counters")
    for name, counter in value.items():
        require(isinstance(counter, dict) and set(counter) == {"count", "bytes"}
                and all(type(number) is int and 0 <= number <= (1 << 63) - 1 for number in counter.values())
                and (name == "read" or counter["bytes"] == 0)
                and (counter["count"] != 0 or counter["bytes"] == 0), "Invalid native I/O counter: " + name)
    return value


def counters_advance(previous, current):
    return all(current[name][field] >= previous[name][field] for name in IO_COUNTERS for field in ("count", "bytes"))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON key: " + key)
        result[key] = value
    return result


def decoded(data):
    return json.loads(data, object_pairs_hook=unique)


def guarded(path, scope):
    path, scope = Path(path).absolute(), Path(scope).absolute()
    require(".." not in path.parts and path.is_relative_to(scope), "Path escapes its allowed scope")
    cursor = Path(path.anchor)
    for part in path.parts[1:]:
        cursor /= part
        require(not cursor.is_symlink(), "Symlinks are forbidden in evidence paths")
    return path


def publish(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def bundle_identity(path):
    path = guarded(path, ROOT / "jobs")
    receipt = decoded((path / "build-evidence.json").read_bytes())
    require(receipt["status"] == "built" and receipt["networkDisconnectedBeforeSourceCopy"] is True
            and receipt["javaRelease"] == 17 and receipt["libfuseVersion"] == "3.14.0"
            and receipt["cleanup"] and all(item["status"] == "removed" for item in receipt["cleanup"]),
            "Helper build, dependency identity or cleanup is incomplete")
    require(set(receipt["files"]) == {"checkpoint-fuse", "checkpoint-fuse-selftest", "probe.jar"}
            and {file.name for file in (path / "bundle").iterdir()} == set(receipt["files"]), "Unexpected helper bundle entries")
    for name, sha in receipt["files"].items():
        require(re.fullmatch(r"[0-9a-f]{64}", sha), "Invalid bundle digest")
        file = guarded(path / "bundle" / name, path / "bundle")
        require(file.is_file() and file.stat().st_size <= 32 * 1024 * 1024, "Invalid bundle file")
        require(digest(file.read_bytes()) == sha, "Helper bundle hash mismatch: " + name)
        mode = file.stat().st_mode
        require(mode & 0o7022 == 0 and mode & 0o004
                and (name.endswith(".jar") or mode & 0o001), "Helper bundle permissions are not ready for unprivileged use")
    require({"checkpoint-fuse", "probe.jar"} <= receipt["files"].keys(), "Incomplete helper bundle")
    return receipt


class Unsupported(RuntimeError):
    pass


class Docker:
    def __init__(self, output, endpoint, attempt, timeout=300):
        require(re.fullmatch(r"unix:///[^\s]+", endpoint), "Only an explicit local Unix Docker endpoint is supported")
        self.output, self.attempt = output, attempt
        self.deadline = time.monotonic() + timeout
        self.sequence = 0
        self.commands = []
        self.cleanup_deadline = None
        config = output / "docker-client"
        config.mkdir()
        (config / "config.json").write_text("{}\n")
        (output / "commands").mkdir()
        self.prefix = ["docker", "--config", str(config), "--host", endpoint]
        self.environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
                            "DOCKER_CONFIG": str(config), "DOCKER_HOST": endpoint, "DOCKER_API_VERSION": "1.44"}

    def run(self, *args, timeout=12, check=True, cleanup=False):
        limit = min(timeout, (self.cleanup_deadline or (time.monotonic() + 30)) - time.monotonic()) if cleanup else min(timeout, self.deadline - time.monotonic())
        if limit <= 0:
            raise TimeoutError("Probe operation budget expired")
        self.sequence += 1
        stem = self.output / "commands" / f"{self.sequence:04d}"
        argv = self.prefix + list(args)
        receipt = {"argv": argv, "startedMonotonicNs": time.monotonic_ns()}
        with stem.with_suffix(".stdout").open("xb") as out, stem.with_suffix(".stderr").open("xb") as err:
            process = subprocess.Popen(argv, stdout=out, stderr=err, start_new_session=True, env=self.environment)
            try:
                end = time.monotonic() + limit
                while process.poll() is None:
                    if time.monotonic() >= end or out.tell() + err.tell() > MAX_OUTPUT:
                        raise TimeoutError("Docker command deadline or output bound exceeded")
                    time.sleep(0.02)
            except BaseException:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)
                receipt["interrupted"] = True
                raise
            finally:
                receipt.update(exitCode=process.returncode, finishedMonotonicNs=time.monotonic_ns())
                self.commands.append(receipt)
                publish(stem.with_suffix(".json"), receipt)
        stdout, stderr = stem.with_suffix(".stdout").read_bytes(), stem.with_suffix(".stderr").read_bytes()
        require(len(stdout) + len(stderr) <= MAX_OUTPUT, "Docker output exceeded limit")
        if check and process.returncode:
            raise RuntimeError("Docker command failed: " + stderr.decode("utf-8", "replace")[:2048])
        return process.returncode, stdout, stderr


class Java:
    def __init__(self, owner, instance, timeout=120, session=None, root=None):
        session = session or instance
        self.owner, self.instance, self.request = owner, instance, 0
        self.pending = bytearray()
        self.stderr = (owner.output / f"java-{session}.stderr").open("xb")
        self.transcript = (owner.output / f"java-{session}.jsonl").open("xb")
        command = owner.docker.prefix + ["exec", "-i", "--user", "9999:9999", owner.containers[instance],
                   "java", "-Xms16m", "-Xmx64m", "-cp", "/work/bundle/probe.jar", JAVA_MAIN,
                   "--root", root or ("/view/" + owner.namespace), "--raw-root", "/backing/" + owner.namespace,
                   "--evidence-root", "/work/java-evidence", "--timeout-seconds", str(timeout)]
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr, start_new_session=True, env=owner.docker.environment)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        owner.sessions.append(self)
        publish(owner.output / f"java-{session}-command.json", {"argv": command})

    def call(self, label, operation, *args):
        self.request += 1
        line = "\t".join([str(self.request), operation, *args]) + "\n"
        started = time.monotonic_ns()
        self.process.stdin.write(line.encode("ascii"))
        self.process.stdin.flush()
        deadline = min(self.owner.docker.deadline, time.monotonic() + 8)
        while b"\n" not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                raise TimeoutError("Java filesystem operation deadline expired: " + label)
            chunk = os.read(self.process.stdout.fileno(), 8192)
            if not chunk:
                raise RuntimeError("Java probe exited without a receipt: " + label)
            self.pending.extend(chunk)
            require(len(self.pending) <= 65536, "Java receipt bound exceeded")
        raw, unused, rest = self.pending.partition(b"\n")
        self.pending = bytearray(rest)
        response = decoded(raw)
        require(response["requestId"] == self.request and response["operation"] == operation,
                "Java response does not match the submitted request")
        self.transcript.write(raw + b"\n"); self.transcript.flush()
        self.owner.observe(label, self.instance, "java", line.rstrip("\n"), response, started)
        return response

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.close()
                self.process.wait(timeout=2)
            except (OSError, subprocess.TimeoutExpired):
                if self.process.poll() is None:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=3)
        self.selector.close(); self.stderr.close(); self.transcript.close()


class Probe:
    def __init__(self, args):
        self.output = guarded(args.output, ROOT / "jobs")
        require(not self.output.exists(), "Retained probe output must never be overwritten")
        self.bundle = bundle_identity(args.bundle)
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", args.image_id), "Explicit image ID required")
        require(args.platform in ("linux/arm64", "linux/amd64"), "Explicit Linux platform required")
        require(self.bundle["imageId"] == args.image_id and self.bundle["platform"] == args.platform,
                "Helper bundle was built for a different runtime")
        self.output.mkdir(parents=True)
        self.args = args
        self.attempt = "fuse_" + uuid.uuid4().hex[:20]
        self.storage = "storage_" + uuid.uuid4().hex[:16]
        self.namespace = "job_" + uuid.uuid4().hex[:16]
        self.volume = "fsi-" + self.attempt
        self.containers, self.names, self.instances, self.revisions = {}, {}, {}, {}
        self.observations, self.sessions = [], []
        self.faults = {}
        self.docker = Docker(self.output, args.docker_host, self.attempt)
        self.report = {"version": 1, "kind": "checkpoint-fuse-probe", "attemptId": self.attempt,
                       "storageId": self.storage, "namespace": self.namespace, "imageId": args.image_id,
                       "platform": args.platform, "bundle": self.bundle, "observations": self.observations,
                       "instances": self.instances, "status": "failed", "cleanup": []}
        source = self.output / "source"
        source.mkdir()
        harness = {}
        for file in (Path(__file__), ROOT / "tools/checkpoint_storage_probe.py"):
            data = file.read_bytes()
            name = ("adapter-" if file.parent == ROOT / "testcontainers" else "entrypoint-") + file.name
            (source / name).write_bytes(data)
            harness[name] = digest(data)
        self.report["harnessFiles"] = harness
        publish(self.output / "intent.json", {"attemptId": self.attempt, "volume": self.volume,
                "imageId": args.image_id, "dockerHost": args.docker_host, "deadlineSeconds": 300,
                "containerWatchdogSeconds": 420})

    def observe(self, label, instance, kind, request, response, started):
        require(all(row["label"] != label for row in self.observations), "Duplicate observation label")
        self.observations.append({"label": label, "instance": instance, "kind": kind, "request": request,
                                  "response": response, "startedMonotonicNs": started,
                                  "finishedMonotonicNs": time.monotonic_ns()})

    def control(self, instance, label, verb, *args, revision=None, identity=None, check=True, cleanup=False):
        rev = self.revisions[instance] if verb == "STATUS" else self.revisions[instance] + 1
        if revision is not None: rev = revision
        command = " ".join([verb, self.attempt, self.storage, identity or instance, str(rev), *map(str, args)])
        started = time.monotonic_ns()
        code, out, err = self.docker.run("exec", "--user", "0:0", self.containers[instance],
                "/work/bundle/checkpoint-fuse", "--ctl", "/work/control/socket", command, check=False, timeout=4, cleanup=cleanup)
        response = decoded(out)
        self.observe(label, instance, "control", command, response, started)
        require(code in (0, 1, 2), "Control transport failed: " + err.decode("utf-8", "replace")[:256])
        if check: require(response["ok"], "Control rejected: " + str(response))
        if response["ok"]:
            self.revisions[instance] = response["revision"]
            self.faults[instance] = response["faultId"]
        return response

    def setup(self, instance, java_timeout=120):
        name = "fsi-" + self.attempt + "-" + instance
        self.names[instance] = name
        publish(self.output / f"create-{instance}-intent.json", {"name": name, "label": self.attempt})
        code, out, err = self.docker.run("create", "--name", name, "--label", LABEL + "=" + self.attempt,
                "--network", "none", "--user", "0:0", "--cap-drop", "ALL", "--cap-add", "SYS_ADMIN",
                "--device", "/dev/fuse", "--mount", "type=volume,src=" + self.volume + ",dst=/backing",
                "--pids-limit", "128", "--memory", "384m", "--stop-timeout", "2", "--init",
                "--entrypoint", "/bin/sh", self.args.image_id, "-c", "exec sleep 420", check=False)
        if code:
            message = err.decode("utf-8", "replace")
            if "/dev/fuse" in message or "operation not permitted" in message.lower():
                raise Unsupported(message[:2048])
            raise RuntimeError(message[:2048])
        cid = out.decode().strip()
        require(re.fullmatch(r"[0-9a-f]{64}", cid), "Invalid created container identity")
        self.containers[instance] = cid
        self.docker.run("start", cid)
        metadata = decoded(self.docker.run("inspect", cid)[1])[0]
        self.instances[instance] = {"containerId": cid, "inspection": metadata}
        self.docker.run("exec", "--user", "0:0", cid, "/bin/sh", "-c",
            "mkdir -p /work/bundle /work/control /work/java-evidence /view; chmod 700 /backing /work/control; "
            "chmod 700 /view; chmod 777 /work/java-evidence")
        self.docker.run("cp", str(Path(self.args.bundle).absolute() / "bundle") + "/.", cid + ":/work/bundle/", timeout=30)
        hashes = self.docker.run("exec", cid, "/bin/sh", "-c", "cd /work/bundle && sha256sum checkpoint-fuse probe.jar")[1].decode()
        for line in hashes.splitlines():
            sha, file = line.split()
            require(self.bundle["files"][file] == sha, "Copied helper identity mismatch")
        self.instances[instance]["copiedHashes"] = hashes
        command = ["/work/bundle/checkpoint-fuse", "--backing", "/backing", "--mount", "/view",
                   "--control", "/work/control/socket", "--journal", "/work/control/journal.jsonl",
                   "--attempt", self.attempt, "--storage", self.storage, "--instance", instance,
                   "--namespace", self.namespace, "--uid", "9999", "--gid", "9999"]
        self.docker.run("exec", "-d", "--user", "0:0", cid, "/bin/sh", "-c",
                        'exec "$@" > /work/control/daemon.stdout 2> /work/control/daemon.stderr', "fuse", *command)
        self.revisions[instance] = 0
        ready = False
        for unused in range(30):
            code, stdout, stderr = self.docker.run("exec", cid, "test", "-S", "/work/control/socket", check=False)
            if code == 0:
                request = " ".join(["STATUS", self.attempt, self.storage, instance, "0"])
                code, raw, unused = self.docker.run("exec", cid, "/work/bundle/checkpoint-fuse", "--ctl",
                                                   "/work/control/socket", request, check=False)
                if code == 0 and decoded(raw)["state"] == "HEALTHY":
                    status = self.control(instance, instance + ".ready", "STATUS")
                    ready = True
                    break
            time.sleep(0.1)
        if not ready:
            error = self.docker.run("exec", cid, "cat", "/work/control/daemon.stderr", check=False)[1].decode("utf-8", "replace")
            if any(phrase in error.lower() for phrase in ("operation not permitted", "permission denied",
                    "failed to open /dev/fuse", "fuse: device not found")): raise Unsupported(error[:2048])
            raise RuntimeError("FUSE daemon did not become ready: " + error[:2048])
        self.instances[instance]["daemonPid"] = status["daemonPid"]
        self.instances[instance]["mountInfo"] = self.docker.run("exec", cid, "cat", "/proc/self/mountinfo")[1].decode()
        self.instances[instance]["kernel"] = self.docker.run("exec", cid, "uname", "-srmo")[1].decode().strip()
        self.instances[instance]["uidMap"] = self.docker.run("exec", cid, "cat", "/proc/self/uid_map")[1].decode()
        return Java(self, instance, java_timeout)

    def run_cases(self):
        peers = {name: self.setup(name) for name in ("a", "b")}
        b64 = lambda value: base64.b64encode(value).decode("ascii")
        for name, java in peers.items():
            java.call(name + ".hello", "hello")
            java.call(name + ".create", "create", "held-" + name, b64(BASE))
            java.call(name + ".open", "open", "held", "held-" + name)
            java.call(name + ".fsync", "force", "held", "true")
            java.call(name + ".fdatasync", "force", "held", "false")
            java.call(name + ".raw-read", "raw-read", "held-" + name)
            java.call(name + ".raw-write", "raw-write", "escape-" + name, b64(BASE))
        for name, java in peers.items():
            other = "b" if name == "a" else "a"
            java.call(name + ".peer-read", "read", "held-" + other)
        for name in peers: self.control(name, name + ".arm", "ARM", "write_eio", 30000, "create,write")
        for name, java in peers.items():
            java.call(name + ".fault-create", "create", "failed-" + name, b64(BASE))
            java.call(name + ".fault-write", "write", "held", b64(AFTER))
            java.call(name + ".fault-read", "read", "held-" + name)
            java.call(name + ".evidence-write", "evidence-write", "still-writable", b64(BASE))
        for name in peers: self.control(name, name + ".heal", "HEAL", "write_eio")
        for name, java in peers.items():
            java.call(name + ".healed-write", "write", "held", b64(AFTER))
            java.call(name + ".healed-fsync", "force", "held", "true")
            java.call(name + ".healed-create", "create", "new-" + name, b64(AFTER))
            java.call(name + ".close", "close", "held")
            java.call(name + ".rename", "rename", "new-" + name, "renamed-" + name)
            self.control(name, name + ".stale", "ARM", "stale", 1000, "create,write", revision=1, check=False)
            self.control(name, name + ".foreign", "ARM", "foreign", 1000, "create,write", identity="wrong_instance", check=False)
            java.call(name + ".traversal", "read", "../outside")
        for name, java in peers.items():
            other = "b" if name == "a" else "a"
            java.call(name + ".healed-peer-read", "read", "held-" + other)
            java.call(name + ".peer-stat", "stat", "renamed-" + other)
            java.call(name + ".peer-list", "list", "@")
            java.call(name + ".peer-delete", "delete", "renamed-" + other)
            java.call(name + ".missing-created", "read", "failed-" + name)
        scope = Java(self, "a", session="a-scope", root="/view")
        scope.call("a.scope-hello", "hello")
        scope.call("a.outside", "read", "other_namespace/unsafe")
        scope.close()
        # Put a deliberately unsafe backing entry in the private fixture; the view must reject it.
        self.docker.run("exec", self.containers["a"], "ln", "-s", "/work/java-evidence", "/backing/" + self.namespace + "/escape-link")
        for name, java in peers.items(): java.call(name + ".symlink", "read", "escape-link/unknown")
        pid = self.control("a", "a.before-death", "STATUS")["daemonPid"]
        self.instances["a"]["daemonBeforeKill"] = self.docker.run("exec", self.containers["a"], "cat", f"/proc/{pid}/stat")[1].decode()
        self.docker.run("exec", self.containers["a"], "kill", "-KILL", str(pid))
        peers["a"].call("a.dead-read", "read", "held-a")
        peers["a"].call("a.still-alive", "hello")
        self.instances["a"]["mountInfoAfterKill"] = self.docker.run("exec", self.containers["a"], "cat", "/proc/self/mountinfo")[1].decode()
        peers["a"].close()
        self.control("b", "b.expiry-arm", "ARM", "expiry", 500, "create,write")
        time.sleep(0.7)
        self.control("b", "b.expired", "STATUS")
        peers["b"].call("b.expired-create", "create", "expired", b64(BASE))
        self.control("b", "b.expired-heal", "HEAL", "expiry", check=False)
        peers["b"].close()
        self.control("b", "b.expired-stop", "STOP")
        # Expected negative lifecycle subcases retain their terminal ABORTED state.
        for name, kind in (("c", "cancel"), ("d", "deadline")):
            java = self.setup(name, java_timeout=2 if kind == "deadline" else 120)
            hello = java.call(name + ".hello", "hello")
            self.control(name, name + ".arm", "ARM", kind, 10000, "create,write")
            if kind == "cancel": self.docker.run("exec", "--user", "9999:9999", self.containers[name], "kill", "-TERM", str(hello["pid"]))
            code = java.process.wait(timeout=4)
            self.observe(name + ".exit", name, "process", kind, {"exitCode": code}, time.monotonic_ns())
            self.control(name, name + ".abort", "ABORT", kind)
            self.control(name, name + ".heal-rejected", "HEAL", kind, check=False)
            java.close()
            self.control(name, name + ".stop", "STOP")

    def cleanup(self):
        if self.docker.cleanup_deadline is None: self.docker.cleanup_deadline = time.monotonic() + 30
        for session in self.sessions:
            try: session.close()
            except Exception as failure: self.report["cleanup"].append({"error": str(failure)})
        for instance, name in self.names.items():
            try:
                code, out, unused = self.docker.run("inspect", name, check=False, cleanup=True)
                if code: continue
                item = decoded(out)[0]
                require(item["Config"]["Labels"].get(LABEL) == self.attempt, "Refusing unrelated container cleanup")
                cid = item["Id"]
                require(item["Image"] == self.args.image_id
                        and (instance not in self.containers or cid == self.containers[instance]),
                        "Refusing replacement or foreign-image container cleanup")
                # Quiesce before final capture; a stopped daemon cannot append a missing receipt.
                daemon_pid = self.instances.get(instance, {}).get("daemonPid")
                if daemon_pid is not None:
                    stopped = self.docker.run("exec", cid, "test", "!", "-d", f"/proc/{daemon_pid}", check=False, cleanup=True)[0] == 0
                    if not stopped:
                        self.control(instance, instance + ".cleanup-stop", "STOP", cleanup=True)
                        for unused in range(10):
                            stopped = self.docker.run("exec", cid, "test", "!", "-d", f"/proc/{daemon_pid}", check=False, cleanup=True)[0] == 0
                            if stopped: break
                            time.sleep(0.05)
                    self.instances[instance]["daemonStoppedBeforeCapture"] = stopped
                # Capture receipts before removal, including failures and expected daemon death.
                for remote, local in (("journal.jsonl", "journal.jsonl"), ("daemon.stderr", "daemon.stderr")):
                    status, data, err = self.docker.run("exec", cid, "cat", "/work/control/" + remote,
                                                       check=False, cleanup=True)
                    (self.output / (instance + "-" + local)).write_bytes(data)
                    if remote == "journal.jsonl":
                        self.instances.get(instance, {})["journalCaptureExit"] = status
                        self.instances.get(instance, {})["journalSha256"] = digest(data)
                self.docker.run("rm", "-f", cid, timeout=10, cleanup=True)
                self.report["cleanup"].append({"containerId": cid, "instance": instance, "removed": True})
            except Exception as failure:
                self.report["cleanup"].append({"instance": instance, "error": str(failure)})
        try:
            code, out, unused = self.docker.run("volume", "inspect", self.volume, check=False, cleanup=True)
            if code == 0:
                volume = decoded(out)[0]
                require(volume["Labels"].get(LABEL) == self.attempt, "Refusing unrelated volume cleanup")
                self.docker.run("volume", "rm", self.volume, cleanup=True)
                self.report["cleanup"].append({"volume": self.volume, "removed": True})
        except Exception as failure: self.report["cleanup"].append({"volume": self.volume, "error": str(failure)})

    def execute(self):
        try:
            self.report["engine"] = decoded(self.docker.run("version", "--format", "{{json .Server}}")[1])
            code, raw, unused = self.docker.run("image", "inspect", self.args.image_id, check=False)
            if code:
                reference = self.bundle["imageDigest"]
                require(re.fullmatch(r"flink@sha256:[0-9a-f]{64}", reference), "Missing immutable public image reference")
                self.docker.run("pull", "--platform", self.args.platform, reference, timeout=120)
                raw = self.docker.run("image", "inspect", self.args.image_id)[1]
            image = decoded(raw)[0]
            require(image["Id"] == self.args.image_id and image["Os"] + "/" + image["Architecture"] == self.args.platform,
                    "Docker image identity/platform differs from requested pin")
            self.report["image"] = {key: image[key] for key in ("Id", "Os", "Architecture", "RepoDigests")}
            self.docker.run("volume", "create", "--label", LABEL + "=" + self.attempt, self.volume)
            volume = decoded(self.docker.run("volume", "inspect", self.volume)[1])[0]
            require(volume["Labels"].get(LABEL) == self.attempt, "Backing volume ownership mismatch")
            self.run_cases()
            self.report["status"] = "observed"
        except Unsupported as failure:
            self.report.update(status="unsupported", error=str(failure))
        except (InterruptedError, KeyboardInterrupt, TimeoutError) as failure:
            self.report.update(status="aborted", error=str(failure))
            self.docker.cleanup_deadline = time.monotonic() + 30
            for name in self.containers:
                try:
                    if self.faults.get(name):
                        self.control(name, name + ".supervisor-abort", "ABORT", self.faults[name], check=False, cleanup=True)
                    else:
                        self.control(name, name + ".supervisor-stop", "STOP", check=False, cleanup=True)
                except Exception: pass
        except Exception as failure:
            self.report.update(status="failed", error=type(failure).__name__ + ": " + str(failure))
        finally:
            self.cleanup()
            publish(self.output / "observations.json", self.report)
        try:
            checks = qualify(self.report, self.output)
            self.report.update(status="pass", checks=checks)
        except Exception as failure:
            self.report["qualificationError"] = str(failure)
            if self.report["status"] == "observed": self.report["status"] = "failed"
        publish(self.output / "result.json", self.report)
        return self.report


def qualify(report, output):
    """Derive acceptance from raw Java/control/journal evidence; never trust a passed flag."""
    require(report["status"] == "observed", "Probe did not complete normally")
    require(not any("error" in row for row in report["cleanup"]), "Cleanup failed")
    require(len(report["cleanup"]) == 5 and all(row["removed"] for row in report["cleanup"]), "Owned resources were not removed")
    expected_containers = {(name, value["containerId"]) for name, value in report["instances"].items()}
    removed_containers = [(row["instance"], row["containerId"]) for row in report["cleanup"] if "containerId" in row]
    removed_volumes = [row["volume"] for row in report["cleanup"] if "volume" in row]
    require(len(removed_containers) == 4 and set(removed_containers) == expected_containers
            and removed_volumes == ["fsi-" + report["attemptId"]], "Cleanup does not cover the exact owned resources")
    rows = {row["label"]: row for row in report["observations"]}
    require(len(rows) == len(report["observations"]), "Duplicate observation label")
    def response(label): return rows[label]["response"]
    def java(label, status="ok"):
        value = response(label)
        require(value["status"] == status, "Unexpected Java outcome: " + label)
        if status == "io-exception":
            require(value["exceptionClass"] in {"java.io.IOException", "java.nio.file.FileSystemException",
                    "java.nio.file.AccessDeniedException", "java.nio.file.NoSuchFileException",
                    "java.nio.file.FileAlreadyExistsException", "java.nio.file.NotDirectoryException"}
                    and value.get("exceptionMessageTruncated") is False, "Missing complete Java IOException")
        return value
    def control(label, state=None, ok=True):
        value = response(label)
        require(value["ok"] is ok and value["journalComplete"] is True, "Invalid control acknowledgement: " + label)
        require(value["attemptId"] == report["attemptId"] and value["storageId"] == report["storageId"]
                and value["instanceId"] == rows[label]["instance"] and value["namespace"] == report["namespace"],
                "Foreign control identity")
        if state: require(value["state"] == state, "Wrong control state: " + label)
        return value
    journals, journal_counters = {}, {}
    for name in ("a", "b", "c", "d"):
        instance = report["instances"][name]; inspected = instance["inspection"]
        require(inspected["Image"] == report["imageId"] and inspected["Id"] == instance["containerId"]
                and inspected["Config"]["Labels"].get(LABEL) == report["attemptId"], "Container identity mismatch")
        config = inspected["HostConfig"]
        require(config["Privileged"] is False and config["NetworkMode"] == "none"
                and config["CapAdd"] in (["SYS_ADMIN"], ["CAP_SYS_ADMIN"]) and config["CapDrop"] == ["ALL"]
                and not config["Binds"] and not config["PidMode"] and config["IpcMode"] in ("private", "")
                and not config.get("SecurityOpt"),
                "Unexpected container privileges or host exposure")
        require(len(config["Devices"]) == 1 and config["Devices"][0]["PathOnHost"] == "/dev/fuse",
                "Unexpected device exposure")
        mounts = inspected["Mounts"]
        require(len(mounts) == 1 and mounts[0]["Type"] == "volume" and mounts[0]["Destination"] == "/backing"
                and mounts[0]["Name"] == "fsi-" + report["attemptId"],
                "Unexpected mount exposure")
        require(" /view " in instance["mountInfo"] and " - fuse" in instance["mountInfo"], "FUSE mount not observed")
        hello = java(name + ".hello")
        require(hello["uid"] == hello["gid"] == [9999] * 4 and int(hello["capEff"], 16) == 0,
                "JVM did not use the unprivileged Flink identity")
        require(instance["journalCaptureExit"] == 0 and instance["daemonStoppedBeforeCapture"] is True,
                "Journal capture was not complete and quiescent")
        raw = guarded(Path(output) / (name + "-journal.jsonl"), output).read_bytes()
        require(raw.endswith(b"\n") and len(raw) <= 8 * 1024 * 1024
                and digest(raw) == instance["journalSha256"], "Truncated, changed or oversized journal")
        events = [decoded(line) for line in raw.splitlines()]
        require(events and [e["sequence"] for e in events] == list(range(1, len(events) + 1)), "Lost journal receipts")
        require(all(e["journalComplete"] is True and e["instanceId"] == name and e["attemptId"] == report["attemptId"]
                    and e["storageId"] == report["storageId"] and e["namespace"] == report["namespace"]
                    and e["daemonPid"] == instance["daemonPid"] for e in events),
                "Incomplete or foreign journal")
        require(any(event["operation"] == "READY" and event["reason"] ==
                "direct_io=1,writeback_cache=0,entry_timeout=0,attr_timeout=0,negative_timeout=0,kernel_cache=0,auto_cache=0"
                for event in events), "Effective FUSE cache settings were not confirmed")
        snapshots = []
        for event in events:
            operation = event["operation"]
            require(isinstance(operation, str) and operation and operation != "STATUS",
                    "STATUS must not consume the native journal budget")
            if event["result"] >= 0 and event["errno"] == 0 and not operation[0].isupper():
                require(operation in {"create", "write", "truncate", "open-trunc"}
                        or operation == "read" and re.search(r"(?:^|/)chk-[1-9][0-9]*/", event["path"]),
                        "Successful callback outside the individual receipt whitelist")
            if operation[0].isupper():
                counters = io_counters(event.get("counters"))
                require(not snapshots or counters_advance(snapshots[-1][1], counters), "Native counters moved backwards")
                if operation in {"START", "READY"}:
                    require(all(counter["count"] == counter["bytes"] == 0 for counter in counters.values()),
                            "Startup counters are not zero")
                if operation == "SUMMARY":
                    require(snapshots and counters != snapshots[-1][1], "SUMMARY has no new aggregate activity")
                snapshots.append((event["sequence"], counters))
            elif "counters" in event:
                raise ValueError("Unexpected counters on an individual native I/O receipt")
        require(snapshots, "Native lifecycle counters are missing")
        journal_counters[name] = snapshots
        journals[name] = events
    require(len({item["inspection"]["Mounts"][0]["Name"] for item in report["instances"].values()}) == 1,
            "Mounts do not share the same backing volume")
    previous_end, control_counters, control_sequences = 0, {}, {}
    for row in report["observations"]:
        require(row["startedMonotonicNs"] >= previous_end
                and row["finishedMonotonicNs"] >= row["startedMonotonicNs"], "Observation order is invalid")
        previous_end = row["finishedMonotonicNs"]
        if row["kind"] == "java":
            fields = row["request"].split("\t"); value = row["response"]
            require(int(fields[0]) == value["requestId"] and fields[1] == value["operation"], "Java request binding mismatch")
            if fields[1] == "hello":
                require(value["uid"] == value["gid"] == [9999] * 4 and int(value["capEff"], 16) == 0,
                        "A Java session used an unexpected identity or capability")
        elif row["kind"] == "control":
            value = control(row["label"], ok=row["response"]["ok"])
            # Rejected commands still return a complete own-instance receipt.
            require(type(value["sequence"]) is int and 1 <= value["sequence"] <= len(journals[row["instance"]])
                    and value["daemonPid"] == report["instances"][row["instance"]]["daemonPid"],
                    "Control receipt sequence/process identity is invalid")
            event = journals[row["instance"]][value["sequence"] - 1]
            expected = row["request"].split()[0] if value["ok"] else "REJECT"
            require((expected == "STATUS" or event["operation"] == expected)
                    and event["revision"] == value["revision"] and event["state"] == value["state"]
                    and event["faultId"] == value["faultId"], "Control acknowledgement differs from captured journal state")
            counters = io_counters(value.get("counters"))
            previous = control_counters.get(row["instance"])
            require(previous is None or counters_advance(previous, counters), "Control I/O counters moved backwards")
            require(value["sequence"] >= control_sequences.get(row["instance"], 0), "Control sequence moved backwards")
            control_counters[row["instance"]] = counters
            control_sequences[row["instance"]] = value["sequence"]
            earlier = [item for sequence, item in journal_counters[row["instance"]] if sequence <= value["sequence"]]
            later = [item for sequence, item in journal_counters[row["instance"]] if sequence > value["sequence"]]
            require(earlier and counters_advance(earlier[-1], counters)
                    and (not later or counters_advance(counters, later[0])), "Control counters contradict journal snapshots")
            if expected != "STATUS":
                require(event["counters"] == counters, "Mutating/rejected control counters differ from acknowledgement")
    for name in ("a", "b"):
        for suffix in ("create", "open", "fsync", "fdatasync", "evidence-write", "healed-write", "healed-fsync",
                       "healed-create", "close", "rename", "peer-stat", "peer-list", "peer-delete"):
            java(name + "." + suffix)
        for suffix in ("raw-read", "raw-write", "fault-create", "fault-write", "missing-created"):
            java(name + "." + suffix, "io-exception")
        require(all(response(name + "." + suffix)["exceptionClass"] in
                    ("java.io.IOException", "java.nio.file.FileSystemException")
                    for suffix in ("fault-write", "fault-create")), "Java exception does not demonstrate EIO")
        require(response(name + ".fault-write")["phase"] == "write"
                and response(name + ".fault-create")["phase"] == "open", "Fault was not observed at the actual I/O syscall")
        require(all(response(name + "." + suffix)["exceptionClass"] == "java.nio.file.AccessDeniedException"
                    for suffix in ("raw-read", "raw-write")), "Raw backing denial was not observed")
        require(response(name + ".missing-created")["exceptionClass"] == "java.nio.file.NoSuchFileException",
                "Failed create left a visible file")
        require(response(name + ".fault-write")["path"] == response(name + ".open")["path"] == "held-" + name
                and response(name + ".fault-write")["handle"] == response(name + ".open")["handle"],
                "Write did not use the previously opened descriptor")
        require(response(name + ".healed-write")["writtenBytes"] == len(AFTER)
                and response(name + ".healed-create")["writtenBytes"] == len(AFTER), "Short healed write")
        for suffix in ("peer-read", "fault-read"):
            require(java(name + "." + suffix)["sha256"] == digest(BASE), "Existing bytes changed during fault")
        require(java(name + ".healed-peer-read")["sha256"] == digest(BASE + AFTER), "Healed writes not visible across mounts")
        control(name + ".arm", "ARMED"); control(name + ".heal", "HEALTHY")
        require(response(name + ".arm")["revision"] == 1 and response(name + ".heal")["revision"] == 2,
                "Wrong fault revision")
        control(name + ".stale", "HEALTHY", False); control(name + ".foreign", "HEALTHY", False)
        java(name + ".traversal", "rejected")
        require(response(name + ".symlink")["status"] in ("rejected", "io-exception"), "Symlink was followed")
        for operation, path in (("create", "failed-" + name), ("write", "held-" + name)):
            matched = [event for event in journals[name] if event["operation"] == operation
                       and event["path"] == report["namespace"] + "/" + path and event["errno"] == 5
                       and event["revision"] == 1 and event["faultId"] == "write_eio"
                       and event["result"] == -1 and event["returnedBytes"] == 0
                       and event["faultOperation"] == operation
                       and (operation != "write" or event["requestedBytes"] == len(AFTER))
                       and response(name + ".arm")["sequence"] < event["sequence"] < response(name + ".heal")["sequence"]]
            require(len(matched) == 1, "Missing or ambiguous matching FUSE EIO: " + name + "/" + operation)
        observed_counters = [snapshot for unused, snapshot in journal_counters[name]] + [control_counters[name]]
        require(max(value["fsync"]["count"] for value in observed_counters) >= 2
                and max(value["fdatasync"]["count"] for value in observed_counters) >= 1,
                "Successful backing fsync/fdatasync calls were not counted")
        other = "b" if name == "a" else "a"
        for suffix in ("peer-read", "healed-peer-read"):
            require(response(name + "." + suffix)["path"] == "held-" + other
                    and rows[name + "." + suffix]["request"].split("\t")[1:] == ["read", "held-" + other],
                    "Cross-mount read addressed the wrong file")
        require(response(name + ".peer-stat")["size"] == len(AFTER)
                and "renamed-" + other in response(name + ".peer-list")["entries"], "Peer metadata did not reflect rename")
    require(rows["b.arm"]["finishedMonotonicNs"] <= rows["a.fault-create"]["startedMonotonicNs"]
            and rows["b.fault-write"]["finishedMonotonicNs"] <= rows["a.heal"]["startedMonotonicNs"]
            and rows["b.heal"]["finishedMonotonicNs"] <= rows["a.healed-write"]["startedMonotonicNs"],
            "Both mounts were not armed/healed before the associated I/O")
    require(java("a.outside", "io-exception")["exceptionClass"] == "java.nio.file.AccessDeniedException"
            and response("a.outside")["path"] == "other_namespace/unsafe", "Native namespace boundary was not exercised")
    java("a.dead-read", "io-exception"); java("a.still-alive")
    require(response("a.hello")["pid"] == response("a.still-alive")["pid"]
            and " /view " in report["instances"]["a"]["mountInfoAfterKill"]
            and " - fuse" in report["instances"]["a"]["mountInfoAfterKill"], "Daemon death did not preserve the disconnected FUSE view/JVM")
    control("b.expired", "EXPIRED"); control("b.expired-heal", "EXPIRED", False)
    java("b.expired-create", "io-exception")
    require(any(e["operation"] == "EXPIRE" for e in journals["b"]), "Missing lease expiry receipt")
    for name, expected in (("c", 143), ("d", 124)):
        require(response(name + ".exit")["exitCode"] == expected, "Cancellation/watchdog exit was not observed")
        control(name + ".abort", "ABORTED"); control(name + ".heal-rejected", "ABORTED", False)
        require(any(e["operation"] == "ABORT" for e in journals[name]), "Missing abort receipt")
    return ["cross-mount-visibility", "create-and-held-write-eio", "preserved-reads-and-healing",
            "fsync-rename-stat-list-delete", "daemon-death-no-fallback", "cancellation-deadline-lease",
            "namespace-identity-and-evidence-isolation"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--docker-host", required=True)
    args = parser.parse_args(argv)
    previous = {}
    def interrupted(signum, unused):
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP): signal.signal(sig, signal.SIG_IGN)
        raise InterruptedError("Supervisor signal " + str(signum))
    try:
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            previous[sig] = signal.signal(sig, interrupted)
        result = Probe(args).execute()
        print(json.dumps({"status": result["status"], "output": str(args.output), "error": result.get("error") or result.get("qualificationError")}))
        return 0 if result["status"] == "pass" else 1
    finally:
        for sig, handler in previous.items(): signal.signal(sig, handler)


if __name__ == "__main__":
    raise SystemExit(main())
