"""Exact frozen batch inventory, with narrowly named mutable runner outputs."""
import os
from pathlib import Path, PurePosixPath
import stat

MUTABLE_TREES = {"attempts", "validation"}
RUNNER_RECEIPTS = {"execution-started.json", "execution-finished.json",
                   "validation-started.json", "validation-finished.json"}
MAX_ENTRIES = 100_000


def require(condition, message):
    if not condition:
        raise ValueError(message)


def relative(name):
    require(isinstance(name, str) and name and "\\" not in name and "\0" not in name,
            "Invalid frozen inventory path")
    path = PurePosixPath(name)
    require(not path.is_absolute() and ".." not in path.parts and str(path) == name,
            "Invalid frozen inventory path")
    return name


def validate(batch, frozen_files, framework_files):
    batch = Path(batch).absolute()
    require(isinstance(frozen_files, dict) and isinstance(framework_files, dict), "Missing frozen file inventory")
    expected = {relative(name) for name in frozen_files}
    require(not any(name.split("/")[0] in MUTABLE_TREES or name in RUNNER_RECEIPTS
                    or name == "manifest.json" for name in expected), "Frozen input claims a mutable runner path")
    expected.update("framework/" + relative(name) for name in framework_files)
    expected.update({"manifest.json", "framework/manifest.json"})
    directories = {str(parent) for name in expected for parent in PurePosixPath(name).parents if str(parent) != "."}
    observed = set()
    visited = 0
    for current, children, files in os.walk(batch, followlinks=False):
        for name in [*children, *files]:
            visited += 1
            require(visited <= MAX_ENTRIES, "Frozen batch inventory exceeds entry bound")
            path = Path(current) / name
            rel = path.relative_to(batch).as_posix()
            require(name != "__pycache__" and not name.endswith((".pyc", ".pyo")),
                    "Python bytecode is forbidden anywhere in a frozen batch: " + rel)
            mode = path.lstat().st_mode
            require(not stat.S_ISLNK(mode), "Symbolic link in frozen batch: " + rel)
            mutable = rel.split("/")[0] in MUTABLE_TREES
            if stat.S_ISDIR(mode):
                require(mutable or rel in directories, "Unexpected directory in frozen batch: " + rel)
            else:
                require(stat.S_ISREG(mode), "Nonregular file in frozen batch: " + rel)
                if mutable or rel in RUNNER_RECEIPTS:
                    continue
                require(rel in expected, "Unlisted file in frozen batch: " + rel)
                observed.add(rel)
    require(observed == expected, "Missing frozen batch files: " + ", ".join(sorted(expected - observed)))
