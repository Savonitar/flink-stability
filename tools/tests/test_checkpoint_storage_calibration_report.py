"""Markdown rendering preserves raw outcomes and incomplete qualification evidence."""
from contextlib import redirect_stdout
from copy import deepcopy
import importlib.util
from io import StringIO
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from zipfile import BadZipFile

RECIPE = Path(__file__).absolute().parents[2] / "calibration/checkpoint-storage"
spec = importlib.util.spec_from_file_location("checkpoint_storage_report_test", RECIPE / "report.py")
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def fixture():
    rows = []
    for cell in report.CELLS:
        for number in range(1, 4):
            mutant, healthy = cell == "mutant-eio", cell == "correct-healthy"
            rows.append({"cell": cell, "number": number, "confirmed": True,
                         "rawStatus": "fail" if mutant else "pass",
                         "rawReason": "validator.kafka.state-prefix." + ("state-mismatch" if mutant else "match"),
                         "stateRecovery": {"recoveries": [] if healthy else [{"confirmed": True}]},
                         "checkpointStorage": {"nativeEioOperations": 4} if cell.endswith("eio") else {},
                         "componentErrors": {"eventCount": 12 if cell.endswith("eio") else 2},
                         "componentEventAudit": {"confirmed": True, "coverage": "retained-final-prefix",
                             "originalCaptureBoundaryKnown": False, "completeProcessLifetime": False,
                             "eventCount": 12 if cell.endswith("eio") else 2, "events": [], "diagnostics": []},
                         "flinkRest": {"errorCount": 1 if cell.endswith("eio") else 0}, "cleanup": {"complete": True}})
    value = {"qualified": True, "attempts": rows, "errors": [], "confirmedCounts": dict.fromkeys(report.CELLS, 3),
             "componentEventCount": 84, "restErrorCount": 6, "cleanupConfirmedCount": 12,
             "retainedComponentEventCount": 84, "componentEventAuditConfirmedCount": 12, "componentEventAuditAttemptCount": 12,
             "manifestSha256": "a" * 64, "frameworkManifestSha256": "b" * 64}
    manifest = {"workload": {"totalRecords": 30000, "keyCount": 8},
                "artifacts": {mode: {"path": "artifacts/" + mode + ".jar", "sha256": ("c" if mode == "correct" else "d") * 64}
                              for mode in ("correct", "mutant")},
                "helperBundle": {"platform": "linux/arm64", "manifestSha256": "e" * 64,
                                 "files": {"checkpoint-fuse": "f" * 64, "probe.jar": "1" * 64}},
                "runtimeIdentity": {"flinkImageReference": "flink:2.2.0", "flinkImageId": "sha256:" + "2" * 64,
                                    "kafkaImageReference": "apache/kafka:4.0.0", "kafkaImageId": "sha256:" + "3" * 64,
                                    "connectorSha256": "4" * 64,
                                    "connectorArtifacts": [{"containerPath": "/opt/flink/lib/connector.jar", "sha256": "4" * 64}]},
                "references": {cell: {"path": "evidence/references/" + cell + ".json", "sha256": "5" * 64} for cell in report.CELLS}}
    return value, manifest


class CheckpointStorageReportTest(unittest.TestCase):
    def test_complete_report_preserves_three_raw_mutant_failures_and_all_event_counts(self):
        value, manifest = fixture()
        text = report.render(value, manifest, {"java": {"sha256": "6" * 64}},
                             {"dockerApiVersion": "1.44", "dockerEndpoint": "unix:///Users/generated/.docker/run/docker.sock"})
        self.assertIn("**QUALIFIED**", text)
        self.assertEqual(3, sum(line.startswith("| mutant-eio |") and "evidence/references" not in line for line in text.splitlines()))
        self.assertEqual(3, text.count("| FAIL | validator.kafka.state-prefix.state-mismatch | confirmed |"))
        self.assertIn("Raw component events: 84; REST errors: 6; confirmed cleanup receipts: 12/12", text)
        self.assertIn("Independently extracted retained component events: 84; confirmed component audits: 12/12", text)
        self.assertIn("Confirmed restorations: 9 across 12 attempts", text)
        self.assertIn("Native EIO operations: 24 across 12 attempts", text)
        self.assertIn("a" * 64, text)
        self.assertIn("6" * 64, text)
        for cell in report.CELLS:
            self.assertIn("evidence/references/" + cell + ".json", text)
        self.assertNotIn("/Users/generated", text)
        self.assertIn("No gaps reported", text)

    def test_incomplete_report_contains_all_twelve_allocations_and_unknowns_not_zero(self):
        value, manifest = fixture()
        value["qualified"] = False
        value["attempts"] = value["attempts"][:2]
        value["attempts"][0].update(confirmed=False, error="checkpoint window missed")
        value["attempts"][0].pop("checkpointStorage")
        value["confirmedCounts"] = {"correct-healthy": 1}
        value["errors"] = [{"cell": "correct-healthy", "number": 1, "error": "checkpoint window missed"}]
        text = report.render(value, manifest)
        table_rows = [line for line in text.splitlines() if line.startswith(tuple("| " + cell + " |" for cell in report.CELLS)) and "evidence/references" not in line]
        self.assertEqual(12, len(table_rows))
        self.assertIn("**NOT QUALIFIED**", text)
        self.assertIn("MISSING", text)
        self.assertIn("Missing rows: 10", text)
        self.assertIn("checkpoint window missed", text)
        self.assertIn("unknown/3", text)

    def test_inconsistent_qualified_flag_cannot_hide_missing_or_duplicate_rows(self):
        original, unused = fixture()
        for change in ("missing", "duplicate", "unconfirmed", "counts"):
            value = deepcopy(original)
            if change == "missing": value["attempts"].pop()
            if change == "duplicate": value["attempts"][-1] = deepcopy(value["attempts"][0])
            if change == "unconfirmed": value["attempts"][0]["confirmed"] = False
            if change == "counts": value["confirmedCounts"]["mutant-eio"] = 2
            with self.subTest(change=change):
                self.assertFalse(report.qualified_report(value))
                self.assertIn("**NOT QUALIFIED**", report.render(value))

    def test_markdown_and_html_are_escaped_without_losing_failure_details(self):
        value, manifest = fixture()
        value["qualified"] = False
        value["attempts"][0].update(confirmed=False, error="bad | `<script>`\n[next](javascript:alert(1)) /Users/alice/private")
        value["attempts"][0]["rawReason"] = "bad|reason\nnew row"
        text = report.render(value, manifest)
        self.assertIn("bad\\|reason<br>new row", text)
        self.assertIn("&lt;script&gt;", text)
        self.assertIn("\\[next\\]", text)
        self.assertNotIn("<script>", text)
        self.assertNotIn("/Users/alice", text)

    def test_render_is_deterministic_and_does_not_modify_checker_evidence(self):
        value, manifest = fixture(); before = deepcopy((value, manifest))
        first = report.render(value, manifest)
        self.assertEqual(first, report.render(value, manifest))
        self.assertEqual(before, (value, manifest))
        self.assertNotIn("Generated at", first)
        self.assertIn("Checkpoint binaries may be deleted", first)

    def test_independent_events_and_partial_coverage_survive_missing_raw_events(self):
        value, manifest = fixture()
        value.update(qualified=False, componentEventCount=0, retainedComponentEventCount=1,
                     componentEventAuditConfirmedCount=11)
        row = value["attempts"][0]
        row.update(confirmed=False, componentErrors={"eventCount": 0, "events": [], "diagnostics": []})
        event = {"timestamp": "2026-10-27 12:56:07,793", "process": "taskmanager-1#2", "level": "ERROR",
                 "logger": "org.apache.flink.connector.kafka.sink.internal.KafkaCommitter", "producerId": "12",
                 "epoch": "0", "transactionalId": "generated-id", "kinds": ["producer-fenced"],
                 "message": "ProducerFencedException <generated> | transaction evidence"}
        row["componentEventAudit"].update(confirmed=False, eventCount=1, events=[event],
             diagnostics=["Component event list is incomplete", "unfinished line omitted"])
        text = report.render(value, manifest)
        self.assertIn("Raw component events: 0", text)
        self.assertIn("Independently extracted retained component events: 1", text)
        self.assertIn("Component event list is incomplete", text)
        self.assertIn("unfinished line omitted", text)
        for field in ("timestamp", "process", "level", "logger", "producerId", "epoch", "transactionalId", "kinds", "message"):
            self.assertIn('"' + field + '"', text)
        self.assertIn("ProducerFencedException &lt;generated&gt; \\| transaction evidence", text)
        self.assertIn("original capture boundary known: False", text)
        self.assertIn("complete process lifetime: False", text)
        self.assertIn("does not establish the original parser read boundary or complete process lifetime", text)
        row.pop("componentErrors")
        self.assertIn("independently extracted event", report.render(value, manifest))

    def test_optional_platform_and_original_reference_paths_are_explicit(self):
        value, manifest = fixture()
        manifest["referenceSources"] = {cell: "jobs/prior-pilots/" + cell + "/stdout.json" for cell in report.CELLS}
        manifest["platformEvidence"] = {"path": "evidence/platform.json", "sha256": "7" * 64}
        platform = {"docker": {"serverVersion": "29.1.3", "kernelVersion": "6.12.54-linuxkit", "architecture": "aarch64",
                               "operatingSystem": "Docker Desktop", "osType": "linux"},
                    "hostSystem": "Darwin", "hostRelease": "25.5.0", "hostMachine": "arm64", "pythonVersion": "3.13.1"}
        text = report.render(value, manifest, platform=platform)
        for expected in ("29.1.3", "6.12.54-linuxkit", "aarch64", "Docker Desktop", "Darwin", "25.5.0", "arm64", "3.13.1", "7" * 64):
            self.assertIn(expected, text)
        for cell in report.CELLS:
            self.assertIn("jobs/prior-pilots/" + cell + "/stdout.json", text)
            self.assertIn("evidence/references/" + cell + ".json", text)

    def test_stdout_only_cli_returns_nonzero_for_unqualified_report(self):
        for qualified in (False, True):
            with self.subTest(qualified=qualified), patch.object(report, "generate", return_value=("retained report\n", qualified)) as generate, redirect_stdout(StringIO()) as output:
                self.assertEqual(0 if qualified else 1, report.main(["/generated/batch"]))
                generate.assert_called_once_with(Path("/generated/batch"), re_audit=False)
                self.assertEqual("retained report\n", output.getvalue())
        with patch.object(report, "generate", return_value=("re-audit\n", True)) as generate, redirect_stdout(StringIO()):
            self.assertEqual(0, report.main(["/generated/batch", "--re-audit"]))
            generate.assert_called_once_with(Path("/generated/batch"), re_audit=True)

    def test_explicit_reaudit_forwards_mode_and_records_renderer_hash_without_weakening_frozen_hash(self):
        for mode in ("default", "reaudit", "changed-frozen-renderer"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                frozen = root / "evidence/recipe/report.py"
                frozen.parent.mkdir(parents=True)
                original = b"# generated old frozen renderer\n"
                frozen.write_bytes(b"changed" if mode == "changed-frozen-renderer" else original)
                value, manifest = fixture()
                manifest["recipeFiles"] = {"evidence/recipe/report.py": report.sha(original)}
                (root / "manifest.json").write_text(json.dumps(manifest))

                def read(directory, filename, expected=None):
                    data = (directory / filename).read_bytes()
                    if expected is not None and report.sha(data) != expected:
                        raise ValueError("Evidence hash mismatch")
                    return data

                checker = SimpleNamespace(check_matrix=Mock(return_value=value), parse=json.loads, read=read, BadZipFile=BadZipFile)
                spec = SimpleNamespace(loader=SimpleNamespace(exec_module=lambda unused: None))
                with patch.object(report.importlib.util, "spec_from_file_location", return_value=spec), \
                     patch.object(report.importlib.util, "module_from_spec", return_value=checker):
                    text, qualified = report.generate(root, re_audit=mode != "default")
                if mode == "default":
                    checker.check_matrix.assert_called_once_with(root)
                    self.assertFalse(qualified)
                    self.assertIn("Renderer differs from frozen recipe", text)
                else:
                    checker.check_matrix.assert_called_once_with(root, re_audit=True)
                    self.assertEqual(mode == "reaudit", qualified)
                    if mode == "reaudit":
                        self.assertIn(report.sha((RECIPE / "report.py").read_bytes()), text)
                        self.assertIn(report.sha(original), text)
                        self.assertIn("explicit-re-audit", text)
                    else:
                        self.assertIn("Renderer identity is unconfirmed", text)


if __name__ == "__main__":
    unittest.main()
