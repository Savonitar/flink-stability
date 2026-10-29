#!/usr/bin/env python3
"""Print a deterministic Markdown qualification report without modifying retained evidence."""
import argparse
import hashlib
import html
import importlib.util
import json
from pathlib import Path
import re

HERE = Path(__file__).absolute().parent
CELLS = ("correct-healthy", "correct-restart", "correct-eio", "mutant-eio")


def escaped(value):
    text = str(value)
    text = re.sub(r"/(?:Users|home)/[^/\s]+", "<home>", text)
    text = html.escape(text, quote=False).replace("\\", "\\\\")
    for character in ("|", "`", "*", "_", "[", "]"):
        text = text.replace(character, "\\" + character)
    return text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")


def integer(value):
    return type(value) is int and value >= 0


def count(value):
    return str(value) if integer(value) else "unknown"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def qualified_report(report):
    rows = report.get("attempts", [])
    return (report.get("qualified") is True and len(rows) == 12
            and {(row.get("cell"), row.get("number")) for row in rows} == {(cell, number) for cell in CELLS for number in range(1, 4)}
            and all(row.get("confirmed") is True for row in rows)
            and all(report.get("confirmedCounts", {}).get(cell) == 3 for cell in CELLS))


def row_counts(row):
    recovery = row.get("stateRecovery")
    restores = None
    if isinstance(recovery, dict) and isinstance(recovery.get("recoveries"), list):
        restores = sum(value.get("confirmed") is True for value in recovery["recoveries"])
    storage = row.get("checkpointStorage") or {}
    native = storage.get("nativeEioOperations")
    if native is None and row.get("confirmed") is True and row.get("cell") in ("correct-healthy", "correct-restart"):
        native = 0
    events = (row.get("componentErrors") or {}).get("eventCount")
    rest = (row.get("flinkRest") or {}).get("errorCount")
    cleanup = (row.get("cleanup") or {}).get("complete")
    return restores, native, events, rest, "confirmed" if cleanup is True else "unconfirmed" if cleanup is False else "unknown"


def render(report, manifest=None, framework=None, launch=None, platform=None):
    manifest, framework, launch = manifest or {}, framework or {}, launch or {}
    rows = report.get("attempts", [])
    indexed = {}
    for row in rows:
        indexed.setdefault((row.get("cell"), row.get("number")), []).append(row)
    lines = ["# Checkpoint-storage qualification", "",
             "**" + ("QUALIFIED" if qualified_report(report) else "NOT QUALIFIED") + "**", "",
             "Fixed matrix: four controls × three attempts. Catalog expectations remain PASS. "
             "A confirmed mutant calibration retains its raw state-mismatch FAIL.", "",
             "| Cell | Attempt | Raw result | Raw reason | Qualification | Confirmed restores | Native EIO | Raw component events | Retained component events | Component audit | REST errors | Cleanup |",
             "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |"]
    missing, duplicate, restores_total, restore_known = 0, 0, 0, 0
    native_total, native_known = 0, 0
    details, component_details = [], []
    for cell in CELLS:
        for number in range(1, 4):
            matches = indexed.get((cell, number), [])
            row = matches[0] if len(matches) == 1 else {}
            if not matches:
                missing += 1
            elif len(matches) > 1:
                duplicate += 1
            restores, native, events, rest, cleanup = row_counts(dict(row, cell=cell))
            component_audit = row.get("componentEventAudit")
            if not isinstance(component_audit, dict):
                component_audit = {}
            component_status = "confirmed" if component_audit.get("confirmed") is True else "unconfirmed" if component_audit else "unknown"
            if integer(restores): restores_total += restores; restore_known += 1
            if integer(native): native_total += native; native_known += 1
            status = str(row.get("rawStatus", "missing" if not matches else "unknown")).upper()
            qualification = "confirmed" if row.get("confirmed") is True and len(matches) == 1 else "unconfirmed"
            values = [cell, number, status, row.get("rawReason", "unavailable"), qualification,
                      count(restores), count(native), count(events), count(component_audit.get("eventCount")), component_status,
                      count(rest), cleanup]
            lines.append("| " + " | ".join(escaped(value) for value in values) + " |")
            if len(matches) != 1:
                details.append(f"{cell} run-{number}: " + ("missing attempt row" if not matches else "duplicate attempt rows"))
            if row.get("error"):
                details.append(f"{cell} run-{number}: " + str(row["error"]))
            if row.get("diagnostics"):
                details.append(f"{cell} run-{number} diagnostics: " + json.dumps(row["diagnostics"], sort_keys=True, ensure_ascii=False))
            label = f"{cell} run-{number}"
            if component_audit:
                component_details.append(label + ": " + component_status + "; coverage " + str(component_audit.get("coverage", "unknown"))
                                         + "; original capture boundary known: " + str(component_audit.get("originalCaptureBoundaryKnown", "unknown"))
                                         + "; complete process lifetime: " + str(component_audit.get("completeProcessLifetime", "unknown")) + ".")
                for event in component_audit.get("events", []):
                    component_details.append(label + " independently extracted event: " + json.dumps(event, sort_keys=True, ensure_ascii=False))
                if component_audit.get("diagnostics"):
                    details.append(label + " component event audit: " + json.dumps(component_audit["diagnostics"], ensure_ascii=False))
                if component_audit.get("limitation"):
                    component_details.append(label + " limitation: " + str(component_audit["limitation"]))
            elif row:
                details.append(label + ": independent component event audit unavailable")
            raw_components = row.get("componentErrors")
            if isinstance(raw_components, dict) and raw_components.get("diagnostics"):
                details.append(label + " original component capture: " + json.dumps(raw_components["diagnostics"], ensure_ascii=False))
    lines += ["", "## Observed totals", "",
              "- Confirmed attempts: " + ", ".join(escaped(cell) + " " + count(report.get("confirmedCounts", {}).get(cell)) + "/3" for cell in CELLS) + ".",
              "- Raw component events: " + count(report.get("componentEventCount")) + "; REST errors: " + count(report.get("restErrorCount"))
              + "; confirmed cleanup receipts: " + count(report.get("cleanupConfirmedCount")) + "/12.",
              "- Independently extracted retained component events: " + count(report.get("retainedComponentEventCount"))
              + "; confirmed component audits: " + count(report.get("componentEventAuditConfirmedCount"))
              + "/" + count(report.get("componentEventAuditAttemptCount")) + " inspected attempts (12 allocated).",
              f"- Confirmed restorations: {restores_total} across {restore_known} attempts with recovery counts; {12 - restore_known} unknown.",
              f"- Native EIO operations: {native_total} across {native_known} attempts with verified storage counts; {12 - native_known} unknown.",
              f"- Missing rows: {missing}; duplicated allocations: {duplicate}.", "",
              "Component events remain observations even when data validation passes; they are not silently subtracted as expected noise.", "",
              "## Independent component event audit", "",
              "The retained-final-prefix comparison checks every recognized event in the complete hashed retained output prefix for each physical process. "
              "It does not establish the original parser read boundary or complete process lifetime. "
              "Raw reported counts and independently extracted counts remain separate; a mismatch or partial capture prevents confirmation.", ""]
    lines += ["- " + escaped(value) for value in component_details] if component_details else ["Independent component event evidence is unavailable."]
    lines += ["", "## Source response audit", "",
              "Current qualification compares typed proof with hash-bound Flink response bodies, exact inspector stdout and Docker Id/State source-token projections. "
              "Docker Config/Env is not retained. These are source-response comparisons, not reserialized typed receipts.", ""]
    for row in rows:
        proof = row.get("rawEvidenceAudit") or {}
        lines.append("- " + escaped(str(row.get("cell", "unknown")) + " run-" + str(row.get("number", "unknown")))
                     + ": " + ("confirmed" if proof.get("confirmed") is True else "unavailable/unconfirmed")
                     + "; captures " + count(proof.get("captureCount")) + "; call groups " + count(proof.get("groupCount")) + ".")
    lines += ["", "Scope: hashes establish retained-byte consistency, not responding-daemon authenticity. "
              "Harness monotonic timestamps and JobManager epoch timestamps remain separate observed clocks. "
              "Terminal Kafka counts are trusted framework-oracle output; the checker does not replay raw Kafka records. "
              "Historical attempts without source captures cannot satisfy this current evidence gate."]
    lines += ["", "## Derived CLI projections", "",
              "Current input-evidence.json and configuration-evidence.json are convenience copies of identified stdout.json fields. "
              "They are checked for self-consistency and are not independent proof or raw source captures. "
              "Older files without a kind field are accepted only with the exact existing stdout source marker and remain explicitly labelled as legacy derived projections.", ""]
    for row in rows:
        projections = row.get("derivedCliProjections") or {}
        label = str(row.get("cell", "unknown")) + " run-" + str(row.get("number", "unknown"))
        if not projections:
            lines.append("- " + escaped(label) + ": derived projection classification unavailable.")
        for projection in projections.get("files", []):
            lines.append("- " + escaped(label + ": " + str(projection.get("path")) + "; source " + str(projection.get("source"))
                         + "; SHA-256 " + str(projection.get("sha256")) + "; legacy representation "
                         + str(projection.get("legacyRepresentation"))) + "; not independent proof.")
    lines += ["",
              "## Exact frozen identities", ""]
    pins = [("Matrix manifest SHA-256", report.get("manifestSha256", "unavailable")),
            ("Framework manifest SHA-256", report.get("frameworkManifestSha256", manifest.get("frameworkFreeze", {}).get("sha256", "unavailable")))]
    pins += [("Verification mode", report.get("verificationMode", "unavailable")),
             ("Renderer SHA-256", report.get("rendererSha256", "unavailable")),
             ("Original frozen renderer SHA-256", report.get("originalRendererSha256", "unavailable")),
             ("Renderer identity mode", report.get("rendererIdentityMode", "unavailable"))]
    pins += [("Current verifier " + name + " SHA-256", value) for name, value in sorted(report.get("verifierSha256", {}).items())]
    if report.get("reAudit"):
        pins.append(("Re-audit scope", report["reAudit"].get("scope", "unavailable")))
    for mode in ("correct", "mutant"):
        artifact = manifest.get("artifacts", {}).get(mode, {})
        pins += [(mode + " workload", artifact.get("path", "unavailable")), (mode + " workload SHA-256", artifact.get("sha256", "unavailable"))]
    helper = manifest.get("helperBundle", {})
    pins += [("Helper manifest SHA-256", helper.get("manifestSha256", "unavailable")), ("Qualified platform", helper.get("platform", "unavailable"))]
    pins += [("Helper " + name + " SHA-256", value) for name, value in sorted(helper.get("files", {}).items())]
    identity = manifest.get("runtimeIdentity", {})
    for component in ("flink", "kafka"):
        pins += [(component + " image", identity.get(component + "ImageReference", "unavailable")),
                 (component + " local image ID", identity.get(component + "ImageId", "unavailable"))]
    pins += [("Connector primary SHA-256", identity.get("connectorSha256", "unavailable")),
             ("Host Java executable SHA-256", framework.get("java", {}).get("sha256", "unavailable")),
             ("Docker API version", launch.get("dockerApiVersion", "unavailable")),
             ("Local Docker endpoint SHA-256", sha(launch["dockerEndpoint"].encode()) if isinstance(launch.get("dockerEndpoint"), str) else "unavailable")]
    for label, value in pins:
        lines.append("- " + escaped(label) + ": " + escaped(value))
    if "platformEvidence" in manifest:
        lines.append("- Platform evidence SHA-256: " + escaped(manifest["platformEvidence"].get("sha256", "unavailable")))
        platform = platform or {}
        docker = platform.get("docker", {})
        for label, value in (("Docker Engine", docker.get("serverVersion", "unavailable")),
                             ("Docker kernel", docker.get("kernelVersion", "unavailable")),
                             ("Docker architecture", docker.get("architecture", "unavailable")),
                             ("Docker operating system", docker.get("operatingSystem", "unavailable")),
                             ("Docker OS type", docker.get("osType", "unavailable")),
                             ("Host OS", platform.get("hostSystem", "unavailable")),
                             ("Host OS release", platform.get("hostRelease", "unavailable")),
                             ("Host architecture", platform.get("hostMachine", "unavailable")),
                             ("Host Python", platform.get("pythonVersion", "unavailable"))):
            lines.append("- " + label + ": " + escaped(value))
    lines += ["", "Local Docker endpoint is displayed by hash; each original command receipt retains the exact endpoint and isolated environment.",
              "The frozen framework manifest retains the ordered runtime classpath, dependency closure, source and configuration hashes.", "",
              "| Connector container path | SHA-256 |", "| --- | --- |"]
    for artifact in identity.get("connectorArtifacts", []):
        lines.append("| " + escaped(artifact.get("containerPath", "unavailable")) + " | " + escaped(artifact.get("sha256", "unavailable")) + " |")
    lines += ["", "Workload contract: " + escaped(json.dumps(manifest.get("workload", {}), sort_keys=True)) + ".", "",
              "## Separate pilot references", "", "| Control | Original workspace path | Frozen result path | SHA-256 |", "| --- | --- | --- | --- |"]
    for cell in CELLS:
        reference = manifest.get("references", {}).get(cell, {})
        lines.append("| " + escaped(cell) + " | " + escaped(manifest.get("referenceSources", {}).get(cell, "unavailable"))
                     + " | " + escaped(reference.get("path", "unavailable")) + " | "
                     + escaped(reference.get("sha256", "unavailable")) + " |")
    lines += ["", "Pilots are separate evidence and do not replace any of the twelve qualification attempts.", "", "## Gaps and diagnostics", ""]
    errors = report.get("errors", [])
    for value in errors:
        details.append(value if isinstance(value, str) else json.dumps(value, sort_keys=True, ensure_ascii=False))
    lines += ["- " + escaped(value) for value in dict.fromkeys(details)] if details else ["No gaps reported by the independent checker."]
    lines += ["", "## Scope and limitations", "",
              "- This matrix covers released Flink 2.2.0, HashMap keyed state, TTL disabled, exactly-once Kafka, one JobManager and one TaskManager, parallelism one and one source/sink partition.",
              "- Qualification is limited to the pinned FUSE helper and Docker Desktop linux/arm64 environment. It does not qualify RocksDB, HA, multiple workers, other platforms, ENOSPC, permanent disk loss or broker storage faults.",
              "- Input IDs are reconstructed from the frozen integer-sequence contract; complete input and read_committed traversal receipts remain required.",
              "- Checkpoint binaries may be deleted after natural completion. Materialization uses pinned inspector and native I/O receipts; absent checkpoint bytes are not represented as retained.",
              "- Explicit re-audit applies the current checker and renderer to original recorded runtime artifacts. It does not qualify changed runtime binaries.",
              "- Artifact hashes identify exact bytes. They do not independently attest a Git revision or compiler provenance. Offline tests, pilot observations and these qualification attempts are separate evidence.", ""]
    return "\n".join(lines)


def generate(batch, *, re_audit=False):
    batch = Path(batch).absolute()
    if {"Documents", "Photos", "Videos", "Downloads", "Desktop", "Pictures"}.intersection(batch.parts):
        return render({"qualified": False, "errors": ["Prohibited evidence path"]}), False
    spec = importlib.util.spec_from_file_location("checkpoint_storage_markdown_checker", HERE / "check.py")
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    manifest, framework, launch, platform = {}, {}, {}, {}
    try:
        report = checker.check_matrix(batch, re_audit=True) if re_audit else checker.check_matrix(batch)
    except (ValueError, KeyError, TypeError, IndexError, OSError, checker.BadZipFile) as failure:
        report = {"qualified": False, "errors": [str(failure)]}
    report.setdefault("verificationMode", "independent-re-audit" if re_audit else "frozen-replay")
    report["rendererIdentityMode"] = "explicit-re-audit" if re_audit else "frozen-replay"
    try:
        report["rendererSha256"] = sha(checker.read(HERE, "report.py"))
    except (ValueError, OSError) as failure:
        report["qualified"] = False
        report.setdefault("errors", []).append("Current renderer identity is unconfirmed: " + str(failure))
    for filename, target in (("manifest.json", manifest), ("framework/manifest.json", framework), ("execution-started.json", launch)):
        try:
            target.update(checker.parse(checker.read(batch, filename)))
        except (ValueError, KeyError, TypeError, OSError):
            pass
    if "manifestSha256" not in report:
        try:
            report["manifestSha256"] = sha(checker.read(batch, "manifest.json"))
        except (ValueError, OSError):
            pass
    if manifest:
        try:
            if "platformEvidence" in manifest:
                platform_receipt = manifest["platformEvidence"]
                platform = checker.parse(checker.read(batch, platform_receipt["path"], platform_receipt["sha256"]))
            expected = manifest["recipeFiles"]["evidence/recipe/report.py"]
            checker.read(batch, "evidence/recipe/report.py", expected)
            report["originalRendererSha256"] = expected
            if not re_audit and report.get("rendererSha256") != expected:
                report["qualified"] = False
                report.setdefault("errors", []).append("Renderer differs from frozen recipe; run the retained report.py")
        except (ValueError, KeyError, TypeError, OSError) as failure:
            report["qualified"] = False
            report.setdefault("errors", []).append("Renderer identity is unconfirmed: " + str(failure))
    return render(report, manifest, framework, launch, platform), qualified_report(report)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch", type=Path)
    parser.add_argument("--re-audit", action="store_true",
                        help="Explicitly use this renderer and checker for original frozen evidence; does not qualify changed runtime binaries")
    args = parser.parse_args(argv)
    markdown, qualified = generate(args.batch, re_audit=args.re_audit)
    print(markdown, end="")
    return 0 if qualified else 1


if __name__ == "__main__":
    raise SystemExit(main())
