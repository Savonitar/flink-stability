"""Closed, deterministic state-recovery catalog contract shared by freeze and audit.

The template identities are part of the verifier, rather than values supplied by
the batch manifest. Rehashing an edited catalog or its frozen template therefore
cannot change the qualification workload.
"""
import hashlib
import json
from pathlib import Path
import re

TEMPLATE_SHA256 = "974d92b7dcf0ccc8544ffa66a4221c8bb6a4c3cd342900439095c4ce64e8547f"
RESTART_SHA256 = "8bcc2406b028266573afc9fe7f49eba030ae4fcdfa10af5c32410e429c778721"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def artifact_reference(batch, mode):
    batch = Path(batch).absolute()
    jobs = [parent for parent in batch.parents if parent.name == "jobs"]
    require(len(jobs) == 1 and mode in {"correct", "mutant"}, "Expected an unambiguous jobs/ batch location")
    return "./" + (batch / "artifacts" / (mode + ".jar")).relative_to(jobs[0].parent).as_posix()


def render(template, restart_phase, workload_reference, execution, runtime_identity=None, *, mode="correct"):
    require(hashlib.sha256(template.encode("utf-8")).hexdigest() == TEMPLATE_SHA256,
            "Baseline template differs from the verifier's fixed qualification contract")
    require(hashlib.sha256(restart_phase.encode("utf-8")).hexdigest() == RESTART_SHA256,
            "Restart template differs from the verifier's fixed qualification contract")
    require(template.count('"@WORKLOAD_JAR@"') == 1, "Workload placeholder changed")
    require(execution in {"uninterrupted", "restart"}, "Unknown execution arm")
    require(mode in {"correct", "mutant"}, "Unknown workload arm")
    rendered = template.replace('"@WORKLOAD_JAR@"', json.dumps(workload_reference))
    if mode == "mutant":
        require(rendered.count("\nmeta:\n") == 1, "Metadata boundary changed")
        rendered = rendered.replace("\nmeta:\n", "\nmeta:\n  allow_state_reset_mutant: true\n")
    if runtime_identity is not None:
        for component, indentation in (("kafka", "        "), ("flink", "    ")):
            reference, image_id = runtime_identity[component + "ImageReference"], runtime_identity[component + "ImageId"]
            image_line = indentation + "image: " + reference + "\n"
            require(rendered.count(image_line) == 1 and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id),
                    "Reference image identity differs from the calibration template")
            rendered = rendered.replace(image_line, image_line + indentation + "image_id: " + json.dumps(image_id) + "\n")
    if execution == "restart":
        require(rendered.count("\nterminal_validations:\n") == 1, "Terminal boundary changed")
        rendered = rendered.replace("\nterminal_validations:\n", "\n" + restart_phase + "\nterminal_validations:\n")
    return rendered.encode("utf-8")
