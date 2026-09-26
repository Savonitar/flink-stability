"""Check gate decisions using recorded-shaped evidence and a local fake Maven command."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
import pr_gate  # noqa: E402


CANDIDATE_BYTES = b"synthetic candidate; never executed"
CANDIDATE_HASH = hashlib.sha256(CANDIDATE_BYTES).hexdigest()


def run_result(subject=CANDIDATE_HASH, status="pass", reason="validator.kafka.id-set.match"):
    source = "/opt/flink/lib/flink-stability-connector-0-" + subject + ".jar"
    return {
        "status": status,
        "reason": reason,
        "evidence": {
            "subjectClasses": {
                "status": "confirmed",
                "expectedSource": source,
                "processes": [{"process": "taskmanager", "sources": {
                    "KafkaSink": [source], "KafkaSource": [source],
                }}],
            },
            "terminalValidation": {"missing": 0, "duplicates": 0},
        },
    }


class SubjectEvidenceTest(unittest.TestCase):
    def summarize(self, result, exit_code=0):
        return pr_gate.summarize("bounded-eos", "candidate", 1, result, CANDIDATE_HASH, exit_code)

    def test_expected_source_alone_cannot_confirm_the_subject(self):
        for status in ("confirmed", "mismatch", "unconfirmed"):
            with self.subTest(status=status):
                result = run_result()
                result["evidence"]["subjectClasses"].update(status=status, processes=[])
                self.assertFalse(self.summarize(result)["subjectOk"])

    def test_matching_observations_require_confirmed_evidence(self):
        result = run_result()
        self.assertTrue(self.summarize(result)["subjectOk"])
        for status in ("mismatch", "unconfirmed", "not-run"):
            with self.subTest(status=status):
                result["evidence"]["subjectClasses"]["status"] = status
                self.assertFalse(self.summarize(result)["subjectOk"])

    def test_foreign_observation_cannot_hide_behind_expected_source(self):
        for foreign in ("/opt/flink/lib/other.jar",
                        "/opt/flink/lib/flink-stability-connector-1-" + "f" * 64 + ".jar"):
            with self.subTest(source=foreign):
                result = run_result()
                result["evidence"]["subjectClasses"]["processes"][0]["sources"]["KafkaSink"].append(foreign)
                self.assertFalse(self.summarize(result)["subjectOk"])

    def test_data_failure_survives_missing_provenance(self):
        result = run_result(status="fail", reason="validator.kafka.id-set.missing-ids")
        result["evidence"]["subjectClasses"]["status"] = "unconfirmed"
        result["evidence"]["terminalValidation"]["missing"] = 147
        result["unrelated"] = {"missing": 999}
        row = self.summarize(result, exit_code=1)
        self.assertEqual("fail", row["verdict"])
        self.assertEqual("validator.kafka.id-set.missing-ids", row["reason"])
        self.assertEqual(147, row["missing"])
        self.assertFalse(row["subjectOk"])


class GateReportTest(unittest.TestCase):
    manifest = {"connector": "candidate.jar", "connectorSha256": CANDIDATE_HASH,
                "runtimeDependencySha256": {}}

    def rows(self, baseline, candidate):
        return [pr_gate.summarize("bounded-eos", side, index, run_result(status=status, reason=reason),
                                  CANDIDATE_HASH, 0 if status == "pass" else 1)
                for side, outcomes in (("baseline", baseline), ("candidate", candidate))
                for index, (status, reason) in enumerate(outcomes, 1)]

    def test_equal_mixed_outcomes_report_variability_without_a_side_difference(self):
        outcomes = [("pass", "match"), ("fail", "missing")]
        report = pr_gate.render(self.manifest, self.rows(outcomes, list(reversed(outcomes))))
        self.assertIn("Outcome distributions differ for: none.", report)
        self.assertIn("Outcomes varied within: bounded-eos/baseline, bounded-eos/candidate.", report)
        self.assertIn("Gate result: NOT PASSED.", report)

    def test_different_counts_and_failure_reasons_are_compared_per_side(self):
        cases = [
            ([("pass", "match"), ("fail", "missing"), ("fail", "missing")],
             [("pass", "match"), ("pass", "match"), ("fail", "missing")]),
            ([("fail", "missing")], [("fail", "duplicates")]),
        ]
        for baseline, candidate in cases:
            with self.subTest(baseline=baseline, candidate=candidate):
                report = pr_gate.render(self.manifest, self.rows(baseline, candidate))
                self.assertIn("Outcome distributions differ for: bounded-eos.", report)

    def test_empty_report_never_passes(self):
        self.assertEqual(1, pr_gate.gate_exit_code([]))
        self.assertIn("Gate result: NOT PASSED.", pr_gate.render(self.manifest, []))


class GateCommandTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix=".gate-test-", dir=ROOT)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        tools = self.root / "tools"
        tools.mkdir()
        for name in ("pr_gate.py", "subject_catalog.py"):
            shutil.copyfile(ROOT / "tools" / name, tools / name)
        scenarios = self.root / "scenarios"
        scenarios.mkdir()
        for suffix in (".yaml", ".expected.yaml"):
            shutil.copyfile(ROOT / "scenarios" / ("bounded-eos" + suffix),
                            scenarios / ("bounded-eos" + suffix))
        (self.root / "candidate.jar").write_bytes(CANDIDATE_BYTES)
        runtime = self.root / "runtime"
        runtime.mkdir()
        (runtime / "dependency.jar").write_bytes(b"synthetic dependency")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.bin / "calls.txt"
        command = self.bin / "mvn"
        command.write_text("#!" + sys.executable + "\n" + '''import json
from pathlib import Path
import sys

root = Path(__file__).resolve().parent
side = "candidate" if "candidate-catalog" in sys.argv[-1] else "baseline"
with (root / "calls.txt").open("a") as calls:
    calls.write(side + "\\n")
fixture = json.loads((root / "fixtures.json").read_text())[side]
sys.stdout.write(fixture["stdout"])
sys.stderr.write("controlled Maven stderr\\n")
raise SystemExit(fixture["exit"])
''')
        command.chmod(0o755)
        self.number = 0

    def command(self, baseline=None, candidate=None, baseline_exit=0, candidate_exit=0, extra=()):
        baseline = run_result(pr_gate.RELEASED_SHA256) if baseline is None else baseline
        candidate = run_result() if candidate is None else candidate
        return self.raw_command(json.dumps(baseline), json.dumps(candidate),
                                baseline_exit, candidate_exit, extra)

    def raw_command(self, baseline, candidate, baseline_exit=0, candidate_exit=0, extra=()):
        fixtures = {"baseline": {"stdout": baseline, "exit": baseline_exit},
                    "candidate": {"stdout": candidate, "exit": candidate_exit}}
        (self.bin / "fixtures.json").write_text(json.dumps(fixtures))
        self.number += 1
        output = self.root / ("output with spaces " + str(self.number))
        result = subprocess.run([
            sys.executable, "-B", str(self.root / "tools/pr_gate.py"),
            "--connector-jar", "candidate.jar", "--runtime-dir", "runtime", "--output", str(output),
            "--scenario", "bounded-eos", *extra,
        ], cwd=self.root, env={"PATH": str(self.bin)}, capture_output=True, text=True, check=False)
        return result, output

    def test_confirmed_passing_runs_exit_zero_and_keep_raw_evidence(self):
        result, output = self.command()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("Gate result: PASS.", result.stdout)
        for side, subject in (("baseline", pr_gate.RELEASED_SHA256), ("candidate", CANDIDATE_HASH)):
            directory = output / side / "bounded-eos/run-1"
            self.assertEqual(run_result(subject), json.loads((directory / "stdout.json").read_text()))
            self.assertEqual("0\n", (directory / "exit-code.txt").read_text())
            self.assertEqual("controlled Maven stderr\n", (directory / "stderr.log").read_text())

    def test_failures_on_either_side_exit_one(self):
        failed = run_result(status="fail", reason="validator.kafka.id-set.missing-ids")
        failed["evidence"]["terminalValidation"]["missing"] = 147
        for baseline_fails, candidate_fails in ((True, False), (False, True), (True, True)):
            with self.subTest(baseline=baseline_fails, candidate=candidate_fails):
                baseline = copy.deepcopy(failed) if baseline_fails else run_result()
                baseline["evidence"]["subjectClasses"] = run_result(
                    pr_gate.RELEASED_SHA256)["evidence"]["subjectClasses"]
                result, output = self.command(
                    baseline=baseline, candidate=failed if candidate_fails else run_result(),
                    baseline_exit=int(baseline_fails), candidate_exit=int(candidate_fails))
                self.assertEqual(1, result.returncode, result.stderr)
                self.assertIn("Gate result: NOT PASSED.", result.stdout)
                self.assertIn("validator.kafka.id-set.missing-ids", (output / "summary.md").read_text())

    def test_inconclusive_and_unverified_subjects_never_pass(self):
        cases = [run_result(status="inconclusive", reason="network-fault.trigger-missed")]
        for status in ("mismatch", "unconfirmed"):
            result = run_result()
            result["evidence"]["subjectClasses"]["status"] = status
            cases.append(result)
        cases.append({"status": "pass", "reason": "match", "evidence": {}})
        for candidate in cases:
            with self.subTest(candidate=candidate):
                result, _ = self.command(candidate=candidate)
                self.assertEqual(1, result.returncode, result.stderr)

    def test_nonzero_maven_exit_cannot_be_overridden_by_passing_json(self):
        result, output = self.command(candidate_exit=7)
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertEqual("7\n", (output / "candidate/bounded-eos/run-1/exit-code.txt").read_text())
        self.assertIn("| pass |", result.stdout)
        self.assertIn("| 7 |", result.stdout)

    def test_absent_or_invalid_json_is_a_reported_error(self):
        for body in ("", "not JSON", "[]", "null", '{}', '{"status":"unknown"}', '{"status":[]}'):
            with self.subTest(body=body):
                result, output = self.raw_command(json.dumps(run_result(pr_gate.RELEASED_SHA256)), body, candidate_exit=1)
                self.assertEqual(1, result.returncode, result.stderr)
                self.assertIn("| error |", result.stdout)
                self.assertEqual(body, (output / "candidate/bounded-eos/run-1/stdout.json").read_text())

    def test_nonpositive_runs_are_rejected_before_output_or_execution(self):
        for runs in ("0", "-1"):
            with self.subTest(runs=runs):
                result, output = self.command(extra=("--runs", runs))
                self.assertEqual(2, result.returncode)
                self.assertIn("--runs must be a positive integer", result.stderr)
                self.assertFalse(output.exists())
                self.assertFalse(self.calls.exists())

    def test_repeated_scenario_names_do_not_repeat_execution(self):
        result, _ = self.command(extra=("--scenario", "bounded-eos"))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(["baseline", "candidate"], self.calls.read_text().splitlines())


if __name__ == "__main__":
    unittest.main()
