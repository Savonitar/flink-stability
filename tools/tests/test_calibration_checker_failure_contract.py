"""Checker CLI failures remain JSON; marker coordinates use JVM physical line boundaries."""
from contextlib import redirect_stdout
from copy import deepcopy
import importlib.util
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


state_fixture = load("line_contract_fixture", Path(__file__).with_name("test_state_recovery_calibration.py"))
state = state_fixture.check
storage = load("storage_failure_contract", ROOT / "calibration/checkpoint-storage/check.py")


class CheckerFailureOutputTest(unittest.TestCase):
    def test_unexpected_exception_still_emits_one_unqualified_json_document(self):
        for checker in (state, storage):
            for error in (RuntimeError("generated runtime failure"), AttributeError("generated missing member"),
                          AssertionError("generated invariant failure")):
                with self.subTest(checker=checker.SCENARIO, error=type(error).__name__), \
                        patch.object(checker, "check_matrix", side_effect=error), \
                        patch("sys.argv", ["check.py", "/generated/batch"]), redirect_stdout(StringIO()) as stdout:
                    self.assertEqual(1, checker.main())
                report = json.loads(stdout.getvalue())
                self.assertIs(False, report["qualified"])
                self.assertEqual([str(error)], report["errors"])

    def test_user_interrupt_is_not_swallowed_as_a_checker_failure(self):
        for checker in (state, storage):
            with patch.object(checker, "check_matrix", side_effect=KeyboardInterrupt), \
                    patch("sys.argv", ["check.py", "/generated/batch"]), redirect_stdout(StringIO()) as stdout:
                with self.assertRaises(KeyboardInterrupt):
                    checker.main()
                self.assertEqual("", stdout.getvalue())


class JvmMarkerLineBoundaryTest(unittest.TestCase):
    def test_lf_crlf_and_cr_all_keep_the_same_physical_marker_coordinates(self):
        markers = state_fixture.fixture()[0]["evidence"]["stateRecovery"]["trace"]["markers"]
        for newline in (b"\n", b"\r\n", b"\r"):
            with self.subTest(newline=newline), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                state_fixture.write_marker_logs(directory, markers)
                path = directory / "logs/flink-stability-output-taskmanager-1-1.log"
                path.write_bytes(path.read_bytes().replace(b"\n", newline))
                self.assertEqual(1, len(state.verify_marker_logs(directory, markers)))

    def test_unicode_and_control_separators_cannot_forge_an_extra_jvm_line(self):
        marker = state_fixture.fixture()[0]["evidence"]["stateRecovery"]["trace"]["markers"][0]
        for separator in ("\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"):
            with self.subTest(separator=repr(separator)), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                state_fixture.write_marker_logs(directory, [marker])
                path = directory / "logs/flink-stability-output-taskmanager-1-1.log"
                path.write_bytes(("prefix" + separator).encode() + path.read_bytes())
                self.assertEqual(1, len(state.verify_marker_logs(directory, [marker])))
                forged = deepcopy(marker)
                forged["lineNumber"] = 2
                with self.assertRaisesRegex(ValueError, "Marker line is missing"):
                    state.verify_marker_logs(directory, [forged])


if __name__ == "__main__":
    unittest.main()
