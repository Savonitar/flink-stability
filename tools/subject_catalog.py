"""Swap the released Kafka connector in a canonical scenario for local artifacts.

Shared by the connector-mutant calibration and the pull-request gate. The harness
resolves local references against its artifact root, so every artifact must be a
regular file inside that root.
"""
import hashlib
from pathlib import Path

RELEASED_SUBJECT = ("connectors:\n    kafka:\n"
                    "      artifact: maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def artifact_reference(path, artifact_root):
    path = Path(path).resolve()
    if not path.is_file() or not path.is_relative_to(artifact_root):
        raise SystemExit("Artifact must be a regular file inside --artifact-root: " + str(path))
    return "./" + str(path.relative_to(artifact_root))


def subject_snippet(primary, dependencies):
    """A local primary with explicit runtime dependencies, as the scenario schema needs."""
    return ("artifact: " + primary + "\nruntime_dependencies:\n"
            + "".join("  - " + dependency + "\n" for dependency in dependencies))


def check_released_subject(scenario_text):
    """Split a scenario around its subject block; refuse anything but the released connector."""
    before, remainder = scenario_text.split("\nsubject:\n", 1)
    subject, after = remainder.split("\nworkload:\n", 1)
    if subject.strip() != RELEASED_SUBJECT:
        raise SystemExit("Canonical subject changed; refusing to replace an unrecognized closure.")
    return before, after


def replace_subject(scenario_text, snippet):
    before, after = check_released_subject(scenario_text)
    subject = "  connectors:\n    kafka:\n" + "".join("      " + line + "\n" for line in snippet.splitlines())
    return before + "\nsubject:\n" + subject + "\nworkload:\n" + after
