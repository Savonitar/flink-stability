"""Generated chronology receipts; original live evidence is never read or edited."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).absolute().parents[2]
SPEC = importlib.util.spec_from_file_location("checkpoint_storage_chronology_test", ROOT / "calibration/checkpoint-storage/check.py")
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def interval(seconds, duration=1):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)
    return {"startedAt": start.isoformat(), "finishedAt": (start + timedelta(seconds=duration)).isoformat(),
            "elapsedSeconds": duration}


class ChronologyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.manifest = {"outerTimeoutSeconds": 900, "cells": [
            {"name": name, "attempts": [f"attempts/{name}/run-{i}" for i in range(1, 4)]}
            for name, _, _ in CHECK.CELLS]}
        self.validations = [dict(interval(i * 2), cell=cell["name"])
                            for i, cell in enumerate(self.manifest["cells"])]
        self.paths = [self.root / relative / "execution.json" for cell in self.manifest["cells"] for relative in cell["attempts"]]
        for i, path in enumerate(self.paths):
            self.write(path, interval(10 + i * 2))

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def verify(self):
        return CHECK.validate_execution_order(self.root, self.manifest, self.validations)

    def test_four_validations_then_twelve_serial_attempts(self):
        result = self.verify()
        self.assertTrue(result["confirmed"])
        self.assertEqual(len(result["attempts"]), 12)
        self.assertEqual(result["preflightFinishedAt"], self.validations[-1]["finishedAt"])
        self.assertTrue(all(len(row["executionSha256"]) == 64 for row in result["attempts"]))

    def test_review_counterexample_identical_intervals_before_preflight_rejected(self):
        for path in self.paths:
            self.write(path, interval(-10))
        with self.assertRaisesRegex(ValueError, "precedes preflight"):
            self.verify()

    def test_identical_intervals_after_preflight_still_rejected(self):
        for path in self.paths:
            self.write(path, interval(10))
        with self.assertRaisesRegex(ValueError, "overlaps"):
            self.verify()

    def test_identical_zero_width_intervals_cannot_hide_positive_execution_time(self):
        for path in self.paths:
            self.write(path, dict(interval(10, duration=0), elapsedSeconds=200))
        with self.assertRaisesRegex(ValueError, "execution interval or deadline"):
            self.verify()

    def test_zero_monotonic_elapsed_is_not_a_real_launch_interval(self):
        value = dict(interval(10), elapsedSeconds=0)
        with self.assertRaisesRegex(ValueError, "execution interval or deadline"):
            CHECK.execution_interval(value, 900, "generated")

    def test_compressed_or_stretched_wall_intervals_cannot_replace_elapsed_time(self):
        for duration, elapsed in ((1, 200), (200, 1), (1, 2.001), (2.001, 1)):
            with self.subTest(duration=duration, elapsed=elapsed):
                self.write(self.paths[0], dict(interval(10, duration), elapsedSeconds=elapsed))
                with self.assertRaisesRegex(ValueError, "differs from monotonic"):
                    self.verify()

    def test_adjacent_clock_sampling_allows_at_most_one_second_of_drift(self):
        for elapsed in (0.01, 0.9, 1, 1.1, 2):
            with self.subTest(elapsed=elapsed):
                self.write(self.paths[0], dict(interval(10), elapsedSeconds=elapsed))
                self.assertTrue(self.verify()["confirmed"])

    def test_every_adjacent_attempt_overlap_rejected(self):
        for i in range(1, 12):
            with self.subTest(attempt=i):
                original = self.paths[i].read_bytes()
                self.write(self.paths[i], interval(10 + (i - 1) * 2))
                with self.assertRaisesRegex(ValueError, "overlaps"):
                    self.verify()
                self.paths[i].write_bytes(original)

    def test_late_preflight_and_overlapping_validations_rejected(self):
        for offset in (20, 4):
            with self.subTest(offset=offset):
                previous = self.validations[-1]
                self.validations[-1] = dict(interval(offset), cell=previous["cell"])
                with self.assertRaises(ValueError):
                    self.verify()
                self.validations[-1] = previous

    def test_missing_validation_and_execution_receipts_rejected(self):
        last = self.validations.pop()
        with self.assertRaisesRegex(ValueError, "preflight chronology"):
            self.verify()
        self.validations.append(last)
        self.paths[-1].unlink()
        with self.assertRaises(ValueError):
            self.verify()

    def test_naive_reversed_nonfinite_and_wrong_typed_intervals_rejected(self):
        mutations = [dict(interval(10), startedAt="2026-01-01T00:00:10"),
                     dict(interval(10), finishedAt="2026-01-01T00:00:09+00:00"),
                     dict(interval(10), elapsedSeconds=True), dict(interval(10), elapsedSeconds=-1),
                     dict(interval(10), elapsedSeconds=901), dict(interval(10), elapsedSeconds=float("nan")),
                     dict(interval(10), startedAt=0)]
        for value in mutations:
            with self.subTest(value=value), self.assertRaises(ValueError):
                CHECK.execution_interval(value, 900, "generated")

    def test_offset_aware_comparison_uses_actual_instant(self):
        value = interval(10)
        value["startedAt"] = "2026-01-01T01:00:10+01:00"
        value["finishedAt"] = "2026-01-01T01:00:11+01:00"
        self.write(self.paths[0], value)
        self.assertTrue(self.verify()["confirmed"])


class VerifierIdentityTest(unittest.TestCase):
    def test_changed_verifier_requires_explicit_reaudit(self):
        current = CHECK.verifier_identity()
        recipes = {"evidence/recipe/" + name: value for name, value in current.items()}
        self.assertEqual(current, CHECK.verify_recipe_identity(recipes))
        original = deepcopy(recipes)
        original["evidence/recipe/check.py"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "explicitly use --re-audit"):
            CHECK.verify_recipe_identity(original)
        self.assertEqual(current, CHECK.verify_recipe_identity(original, re_audit=True))
        self.assertEqual(original["evidence/recipe/check.py"], "0" * 64)


if __name__ == "__main__":
    unittest.main()
