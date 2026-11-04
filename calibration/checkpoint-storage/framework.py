#!/usr/bin/env python3
"""Freeze the exact installed framework and an explicitly selected offline artifact closure."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile

ROOT = Path(__file__).absolute().parents[2]
MODULES = ("runtime-api", "core", "testcontainers", "cli", "kroxylicious-fault-filter", "synthetic-token-plugin")
VERSION = "0.1.0-SNAPSHOT"
MAIN = "org.savonitar.flink.stability.cli.Main"
SETTINGS = b'<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"/>\n'
MAX_FILE = 256 * 1024 * 1024
MAX_TOTAL = 2 * 1024 * 1024 * 1024
MAX_FILES = 12000
FORBIDDEN = {"Documents", "Downloads", "Desktop", "Pictures", "Photos", "Videos", ".ssh", ".aws", ".gnupg"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def safe(path, root=None):
    path = Path(path).absolute()
    require(".." not in path.parts and not FORBIDDEN.intersection(path.parts), "Forbidden or traversing framework path")
    if root is not None:
        root = Path(root).absolute()
        require(path.is_relative_to(root), "Framework path escapes its declared root")
    for item in (path, *path.parents):
        require(not item.is_symlink(), "Symbolic-link framework path rejected")
    return path


def relative(value):
    require(isinstance(value, str) and bool(value) and "\\" not in value and ":" not in value,
            "Invalid relative framework path")
    path = PurePosixPath(value)
    require(not path.is_absolute() and str(path) == value and all(part not in ("", ".", "..") for part in path.parts),
            "Noncanonical relative framework path")
    require(not FORBIDDEN.intersection(path.parts), "Prohibited relative framework path")
    return value


def read(path, root=None, maximum=MAX_FILE):
    path = safe(path, root)
    require(path.is_file() and path.stat().st_size <= maximum, "Framework file missing, nonregular or over byte bound")
    with path.open("rb") as stream:
        data = stream.read(maximum + 1)
    require(len(data) <= maximum, "Framework file grew beyond byte bound")
    return data


def sha(data):
    return hashlib.sha256(data).hexdigest()


def json_read(path, root=None):
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, "Duplicate JSON key")
            result[key] = value
        return result
    return json.loads(read(path, root, 16 * 1024 * 1024), object_pairs_hook=pairs)


def inventory(directory):
    directory = safe(directory)
    require(directory.is_dir(), "Framework source/classes directory missing")
    files = []
    for current, dirs, names in os.walk(directory, followlinks=False):
        for name in dirs:
            safe(Path(current) / name, directory)
        for name in names:
            path = safe(Path(current) / name, directory)
            require(path.is_file(), "Nonregular framework inventory entry")
            files.append(path)
            require(len(files) <= MAX_FILES, "Framework file count exceeds bound")
    return sorted(files)


def jar_entries(path):
    """Class/resource identity without accepting manifest or index classpath expansion."""
    path = safe(path)
    require(path.stat().st_size <= MAX_FILE, "JAR exceeds byte bound")
    entries = {}
    with zipfile.ZipFile(path) as archive:
        names = set()
        total = 0
        for item in archive.infolist():
            require(item.filename not in names, "Duplicate JAR entry")
            names.add(item.filename)
            relative(item.filename.rstrip("/"))
            require(not stat.S_ISLNK(item.external_attr >> 16), "JAR symlink entry rejected")
            if item.is_dir():
                continue
            require(item.file_size <= MAX_FILE, "JAR entry exceeds byte bound")
            total += item.file_size
            require(total <= MAX_TOTAL and len(entries) < MAX_FILES, "JAR expanded inventory exceeds bound")
            data = archive.read(item)
            if item.filename == "META-INF/INDEX.LIST":
                lines = [line for line in data.decode("utf-8").replace("\r\n", "\n").splitlines() if line]
                require(len(lines) >= 2 and lines[0] == "JarIndex-Version: 1.0" and lines[1] == path.name,
                        "JAR index classpath expansion rejected")
                for package in lines[2:]:
                    relative(package)
                    require(not package.lower().endswith(".jar"), "JAR index classpath expansion rejected")
            if item.filename.upper() == "META-INF/MANIFEST.MF":
                unfolded = data.decode("utf-8").replace("\r\n", "\n").replace("\n ", "")
                require(not any(line.lower().startswith("class-path:") for line in unfolded.splitlines()),
                        "Manifest Class-Path expansion rejected")
            entries[item.filename] = sha(data)
    return entries


def module_classes(module, entries):
    generated = {"META-INF/MANIFEST.MF", f"META-INF/maven/org.savonitar.flink.stability/{module}/pom.xml",
                 f"META-INF/maven/org.savonitar.flink.stability/{module}/pom.properties"}
    return {name: digest for name, digest in entries.items() if name not in generated}


def artifact_relative(path, repository):
    path = safe(path, repository)
    value = relative(path.relative_to(repository).as_posix())
    parts = PurePosixPath(value).parts
    require(len(parts) >= 4 and all(re.fullmatch(r"[A-Za-z0-9_.+-]+", part) for part in parts),
            "Expected an ordinary Maven artifact coordinate path")
    artifact, version, filename = parts[-3:]
    require(path.suffix in (".jar", ".pom") and filename.startswith(artifact + "-" + version)
            and not filename.endswith(("-sources.jar", "-javadoc.jar")), "Only selected runtime JAR/POM artifact files are permitted")
    return value


def freeze_framework(destination, *, repository, classpath_file, artifact_files, java, source_root=ROOT,
                     artifact_list_file=None, recipe_files=()):
    """Copies only explicit cache files; does not run Java, Maven, Docker or resolve dependencies."""
    destination, source_root = safe(destination), safe(source_root)
    repository, java = safe(repository), safe(java)
    require(repository.is_dir() and repository.name == "repository", "Select the exact Maven repository/ directory")
    require(not destination.exists(), "Framework destination must be fresh")
    require(not destination.is_relative_to(repository) and not repository.is_relative_to(destination), "Framework and seed repository must be disjoint")
    java_bytes = read(java)
    classpath_data = read(classpath_file, maximum=1024 * 1024)
    entries = classpath_data.decode("utf-8").strip().split(os.pathsep)
    require(1 <= len(entries) <= 512 and len(entries) == len(set(entries)) and all(entries), "Invalid classpath count or duplicates")
    classpath = [safe(Path(entry), repository) for entry in entries]
    require(all(path.suffix == ".jar" for path in classpath), "Classpath must contain only exact repository JARs")
    requested = [safe(path, repository) for path in artifact_files]
    require(len(requested) == len(set(requested)) and 1 <= len(requested) <= 4096, "Invalid artifact closure count/duplicates")
    for path in (*classpath, *requested):
        artifact_relative(path, repository)
    destination.mkdir(mode=0o700, parents=False)
    frozen_files = {}
    total = 0

    def save(name, data):
        nonlocal total
        name = relative(name)
        require(name not in frozen_files and len(frozen_files) < MAX_FILES, "Duplicate/oversized frozen inventory")
        total += len(data)
        require(total <= MAX_TOTAL, "Frozen framework exceeds total byte bound")
        target = safe(destination / name, destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as output:
            output.write(data)
        target.chmod(0o444)
        frozen_files[name] = {"sha256": sha(data), "bytes": len(data)}
        return frozen_files[name]["sha256"]

    modules, sources, classes, module_paths = [], {}, {}, {}
    for module in MODULES:
        name = f"{module}-{VERSION}.jar"
        target = safe(source_root / module / "target" / name, source_root)
        installed = safe(repository / "org/savonitar/flink/stability" / module / VERSION / name, repository)
        target_data, installed_data = read(target), read(installed)
        require(sha(target_data) == sha(installed_data), "Installed framework differs from packaged target: " + module)
        actual_entries = jar_entries(target)
        class_root = source_root / module / "target/classes"
        expected_entries = {}
        for path in inventory(class_root):
            member = path.relative_to(class_root).as_posix()
            digest = sha(read(path, class_root))
            expected_entries[member] = digest
            classes[path.relative_to(source_root).as_posix()] = digest
        require(expected_entries and module_classes(module, actual_entries) == expected_entries,
                "Packaged framework classes/resources differ from target/classes: " + module)
        frozen = "jars/" + name
        digest = save(frozen, target_data)
        module_paths[installed] = frozen
        modules.append({"module": module, "path": frozen, "sha256": digest,
                        "targetSha256": digest, "installedSha256": digest})
        paths = [source_root / module / "pom.xml", *inventory(source_root / module / "src/main")]
        for path in paths:
            source_name = path.relative_to(source_root).as_posix()
            sources[source_name] = save("sources/" + source_name, read(path, source_root))
    sources["pom.xml"] = save("sources/pom.xml", read(source_root / "pom.xml", source_root))

    runtime_classpath = [module_paths[repository / "org/savonitar/flink/stability/cli" / VERSION / f"cli-{VERSION}.jar"]]
    repository_paths = {}
    for path in sorted(set(requested) | {p for p in classpath if p not in module_paths}):
        name = artifact_relative(path, repository)
        if path.suffix == ".jar":
            jar_entries(path)
        digest = save("repository/" + name, read(path, repository))
        repository_paths[name] = digest
    for path in classpath:
        frozen = module_paths.get(path) or "repository/" + artifact_relative(path, repository)
        if frozen not in runtime_classpath:
            runtime_classpath.append(frozen)
    require(all(any(value == "jars/" + module + "-" + VERSION + ".jar" for value in runtime_classpath)
                for module in ("runtime-api", "core", "testcontainers", "cli")), "Runtime classpath omits a required framework module")
    # These generated markers contain no discovered repository IDs, paths, timestamps or credentials.
    markers = {}
    for name in repository_paths:
        artifact = PurePosixPath(name)
        markers.setdefault(str(artifact.parent), []).append(artifact.name)
    for parent, names in sorted(markers.items()):
        text = "# Generated offline closure\n" + "".join(f"{name}>=\n{name}>central=\n" for name in sorted(names))
        save("repository/" + parent + "/_remote.repositories", text.encode())
    save("settings.xml", SETTINGS)
    save("framework.py", read(Path(__file__)))
    save("inputs/runtime-classpath.txt", classpath_data)
    artifact_list_data = read(artifact_list_file, maximum=1024 * 1024) if artifact_list_file is not None else ("\n".join(str(path) for path in requested) + "\n").encode()
    save("inputs/maven-closure.txt", artifact_list_data)
    recipes = {}
    automatic = [source_root / "calibration/checkpoint-storage" / name for name in ("prepare.py", "ResolveClosure.java")]
    selected_recipes = list(recipe_files) + [path for path in automatic if path.is_file() and path not in recipe_files]
    for path in selected_recipes:
        path = safe(path, source_root)
        name = path.relative_to(source_root).as_posix()
        recipes[name] = "recipe/" + name
        save(recipes[name], read(path, source_root))
    manifest = {"formatVersion": 1, "moduleJars": modules, "sourceSha256": sources,
                "targetClassesSha256": classes, "runtimeClasspath": runtime_classpath,
                "artifactClosureSha256": repository_paths, "files": frozen_files,
                "java": {"path": str(java), "sha256": sha(java_bytes)}, "mainClass": MAIN,
                "mavenRepository": "repository", "settings": "settings.xml",
                "classpathInputSha256": sha(classpath_data),
                "inputs": {"classpath": "inputs/runtime-classpath.txt", "artifactList": "inputs/maven-closure.txt", "recipes": recipes},
                "sourceIdentity": "byte snapshot; no source-revision or compiler-attestation claim"}
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (destination / "manifest.json").chmod(0o444)
    validate_framework(destination)
    return manifest


def validate_framework(root):
    root = safe(root)
    manifest = json_read(root / "manifest.json", root)
    require(manifest.get("formatVersion") == 1 and manifest.get("mainClass") == MAIN
            and manifest.get("mavenRepository") == "repository" and manifest.get("settings") == "settings.xml",
            "Unsupported frozen framework format")
    files = manifest["files"]
    require(isinstance(files, dict) and 1 <= len(files) <= MAX_FILES, "Invalid frozen file inventory")
    actual = {path.relative_to(root).as_posix() for path in inventory(root)}
    require(actual == set(files) | {"manifest.json"}, "Frozen framework file inventory changed")
    total = 0
    for name, receipt in files.items():
        relative(name)
        data = read(root / name, root)
        total += len(data)
        require(total <= MAX_TOTAL and receipt == {"sha256": sha(data), "bytes": len(data)}, "Frozen framework file changed: " + name)
    require(read(root / "settings.xml", root) == SETTINGS, "Settings are not explicitly empty")
    require(manifest["inputs"]["classpath"] == "inputs/runtime-classpath.txt"
            and manifest["inputs"]["artifactList"] == "inputs/maven-closure.txt"
            and files["inputs/runtime-classpath.txt"]["sha256"] == manifest["classpathInputSha256"], "Frozen preparation inputs differ")
    for source, frozen in manifest["inputs"]["recipes"].items():
        require(relative(frozen) == "recipe/" + relative(source) and frozen in files, "Recipe snapshot missing")
    java = manifest["java"]
    require(sha(read(java["path"])) == java["sha256"], "Java launcher changed")
    modules = manifest["moduleJars"]
    require([entry["module"] for entry in modules] == list(MODULES), "Framework module inventory differs")
    require(manifest["sourceSha256"] and manifest["targetClassesSha256"], "Missing source/classes evidence")
    require({"sources/" + relative(name) for name in manifest["sourceSha256"]}
            == {name for name in files if name.startswith("sources/")}, "Source inventory differs")
    require(all(any(name.startswith(module + "/target/classes/") for module in MODULES)
                for name in manifest["targetClassesSha256"]), "Unexpected target class path")
    for name, digest in manifest["sourceSha256"].items():
        require(files["sources/" + relative(name)]["sha256"] == digest, "Source snapshot hash differs")
    for entry in modules:
        module, name = entry["module"], relative(entry["path"])
        require(name == "jars/" + module + "-" + VERSION + ".jar"
                and entry["sha256"] == entry["targetSha256"] == entry["installedSha256"] == files[name]["sha256"],
                "Framework target/installed/frozen byte identities differ")
        prefix = module + "/target/classes/"
        expected = {relative(path[len(prefix):]): digest for path, digest in manifest["targetClassesSha256"].items() if path.startswith(prefix)}
        require(expected and module_classes(module, jar_entries(root / name)) == expected, "Frozen class/resource identity differs")
    cp = manifest["runtimeClasspath"]
    require(1 <= len(cp) <= 512 and len(cp) == len(set(cp)) and cp[0] == "jars/cli-" + VERSION + ".jar", "Invalid frozen runtime classpath")
    require(all("jars/" + module + "-" + VERSION + ".jar" in cp
                for module in ("runtime-api", "core", "testcontainers", "cli")), "Runtime classpath omits required framework module")
    for name in cp:
        require(relative(name).endswith(".jar") and name in files, "Classpath references an unpinned JAR")
        jar_entries(root / name)
    for name, digest in manifest["artifactClosureSha256"].items():
        require(files["repository/" + relative(name)]["sha256"] == digest, "Artifact closure hash differs")
    return manifest


def command(root, args, docker_api_version=None, *, work_directory):
    root, work_directory = safe(root), safe(work_directory)
    manifest = validate_framework(root)
    require(isinstance(args, list) and all(isinstance(arg, str) and "\x00" not in arg for arg in args), "Invalid CLI arguments")
    require(args and args[0] in ("run", "validate") and "--offline" in args, "Frozen CLI requires explicit offline run/validate")
    argv = [manifest["java"]["path"], "-Dmaven.repo.local=" + str(root / "repository"),
            "-Dmaven.user.settings=" + str(root / "settings.xml"), "-Dmaven.global.settings=" + str(root / "settings.xml"),
            "-Duser.home=" + str(work_directory / "home"), "-Djava.io.tmpdir=" + str(work_directory / "tmp"),
            "-Dmaven.multiModuleProjectDirectory=" + str(work_directory)]
    if docker_api_version is not None:
        require(isinstance(docker_api_version, str) and re.fullmatch(r"[0-9]+\.[0-9]+", docker_api_version), "Invalid Docker API version")
        argv.append("-Dapi.version=" + docker_api_version)
    return [*argv, "-cp", os.pathsep.join(str(root / name) for name in manifest["runtimeClasspath"]), MAIN, *args]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--classpath", type=Path, required=True)
    parser.add_argument("--artifact-files", type=Path, required=True, help="Newline list or JSON array of exact absolute selected JAR/POM cache paths")
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--recipe", type=Path, action="append", default=[])
    args = parser.parse_args()
    raw = read(args.artifact_files, maximum=1024 * 1024).decode("utf-8")
    selected = json_read(args.artifact_files) if raw.lstrip().startswith("[") else raw.splitlines()
    require(isinstance(selected, list) and all(isinstance(value, str) and value for value in selected), "Expected artifact path array/list")
    freeze_framework(args.output, repository=args.repository, classpath_file=args.classpath,
                     artifact_files=[Path(value) for value in selected], java=args.java,
                     artifact_list_file=args.artifact_files, recipe_files=args.recipe)
    print(args.output / "manifest.json")


if __name__ == "__main__":
    main()
