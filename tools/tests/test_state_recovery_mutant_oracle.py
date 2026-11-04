"""Independent reset-mutant arithmetic and adversarial evidence; no live services."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location("state_recovery_mutant_fixture",
                                            Path(__file__).with_name("test_state_recovery_calibration.py"))
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
check = base.check


def mutant_fixture(total=6000, saved_max=15):
    """Generate expected samples by replaying independent counters, including uneven key prefixes."""
    args = base.fixture("mutant", "restart")
    result, unused_code, unused_cell, manifest = args
    manifest["workload"]["totalRecords"] = total
    evidence = result["evidence"]
    oracle = evidence["terminalValidation"]
    for field in ("expected", "observed", "distinctExpected"):
        oracle[field] = total
    evidence["input"].update(records=total, observed=total)
    report = evidence["stateRecovery"]
    report["totalRecords"] = total
    proof = report["recoveries"][0]
    ready = proof["readiness"]
    ready["totalRecords"] = total
    count, sums = [0] * 8, [0] * 8
    for event_id in range(saved_max + 1):
        key = event_id % 8
        count[key] += 1
        sums[key] += event_id + 1
    ready["committedPrefix"].update(observedRecords=saved_max + 1, maximumId=saved_max,
                                   perKeyCounts={str(key): count[key] for key in range(8)},
                                   endOffsets={"0": saved_max + 3})
    saved = ready["completedSnapshots"]["42"]["snapshot"]["progress"]
    saved.update(minimumObservedCount=min(count), processedThisAttempt=saved_max + 1, maximumInputId=saved_max)
    first = {}
    for event_id in range(saved_max + 1, saved_max + 9):
        first[event_id % 8] = event_id
    for row in proof["restoredKeys"]:
        value = row["keyProgress"]
        key = value["key"]
        value.update(inputId=first[key], previousCount=count[key], previousSum=sums[key],
                     newCount=1, newSum=first[key] + 1)
    proof["postRestoreProgress"]["progress"]["maximumInputId"] = max(first.values())
    expected_count, expected_sum, actual_count, actual_sum = ([0] * 8 for unused in range(4))
    samples = []
    for event_id in range(total):
        key = event_id % 8
        expected_count[key] += 1
        expected_sum[key] += event_id + 1
        if event_id == first[key]:
            actual_count[key] = actual_sum[key] = 0
        actual_count[key] += 1
        actual_sum[key] += event_id + 1
        if event_id > saved_max and len(samples) < 100:
            samples.append({"partition": 0, "offset": event_id + 500, "id": event_id,
                            "expectedKey": key, "actualKey": key,
                            "expectedCount": expected_count[key], "actualCount": actual_count[key],
                            "expectedSum": expected_sum[key], "actualSum": actual_sum[key]})
    oracle["statePrefix"].update(mismatches=total - saved_max - 1, mismatchSamples=samples)
    return args


class MutantOracleProofTest(unittest.TestCase):
    def assert_rejected(self, args):
        with self.assertRaises((ValueError, KeyError)):
            check.audit_attempt(*args)

    def test_fixed_reset_samples_cover_every_key_and_later_updates(self):
        for total, saved_max in ((32, 15), (6000, 15), (30000, 624), (30000, 749), (30000, 905)):
            with self.subTest(total=total, saved_max=saved_max):
                args = mutant_fixture(total, saved_max)
                row = check.audit_attempt(*args)
                samples = row["terminalValidation"]["statePrefix"]["mismatchSamples"]
                self.assertTrue(row["confirmed"])
                self.assertEqual(row["rawStatus"], "fail")
                self.assertEqual({sample["actualKey"] for sample in samples}, set(range(8)))
                self.assertEqual(len(samples), min(100, total - saved_max - 1))
                self.assertGreater(samples[-1]["actualCount"], 1)

    def test_review_counterexample_with_impossible_total_and_negative_values_is_rejected(self):
        args = mutant_fixture(30000, 624)
        state = args[0]["evidence"]["terminalValidation"]["statePrefix"]
        state["mismatches"] = 30001
        state["mismatchSamples"][0].update(actualCount=-100, actualSum=-100)
        self.assert_rejected(args)

    def test_mismatch_total_is_exact_corrupted_suffix_not_just_bounded_positive(self):
        for value in (-1, False, True, 0, 1, 5983, 5985, 6000, 6001, 5984.0, "5984", 1 << 63):
            with self.subTest(value=value):
                args = mutant_fixture()
                args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatches"] = value
                self.assert_rejected(args)

    def test_every_sample_field_rejects_boolean_negative_and_noninteger(self):
        for field in check.STATE_SAMPLE_FIELDS:
            for value in (True, -1, 1.0, "1"):
                with self.subTest(field=field, value=value):
                    args = mutant_fixture()
                    args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"][0][field] = value
                    self.assert_rejected(args)

    def test_samples_require_exact_fields_and_complete_bounded_count(self):
        for change in (lambda rows: rows.pop(), lambda rows: rows.append(deepcopy(rows[-1])),
                       lambda rows: rows.clear(), lambda rows: rows[0].pop("actualSum"),
                       lambda rows: rows[0].update(extra=1)):
            args = mutant_fixture()
            change(args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"])
            self.assert_rejected(args)

    def test_actual_arithmetic_is_bound_to_each_reset_key(self):
        for ordinal in range(100):
            for field in ("actualKey", "actualCount", "actualSum", "expectedKey", "expectedCount", "expectedSum"):
                with self.subTest(ordinal=ordinal, field=field):
                    args = mutant_fixture()
                    args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"][ordinal][field] += 1
                    self.assert_rejected(args)

    def test_samples_reject_impossible_ranges_and_nonzero_partition(self):
        for field, value in (("id", 6000), ("actualKey", 8), ("expectedKey", 8), ("actualCount", 0),
                             ("actualSum", 0), ("actualCount", 6001), ("actualSum", 1 << 63),
                             ("offset", 1 << 63), ("partition", 1)):
            with self.subTest(field=field, value=value):
                args = mutant_fixture()
                args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"][0][field] = value
                self.assert_rejected(args)

    def test_duplicate_or_out_of_order_coordinates_and_ids_are_rejected(self):
        for change in (lambda rows: rows.__setitem__(1, deepcopy(rows[0])),
                       lambda rows: rows[1].update(offset=rows[0]["offset"]),
                       lambda rows: rows[1].update(offset=rows[0]["offset"] - 1),
                       lambda rows: rows.reverse()):
            args = mutant_fixture()
            change(args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"])
            self.assert_rejected(args)

    def test_kafka_offsets_can_have_transaction_gaps_and_exceed_input_count(self):
        args = mutant_fixture()
        for ordinal, sample in enumerate(args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"]):
            sample["offset"] = 9000 + ordinal * 2
        self.assertTrue(check.audit_attempt(*args)["confirmed"])

    def test_first_sample_cannot_skip_initial_corruption_or_claim_pre_restore_data(self):
        args = mutant_fixture()
        rows = args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"]
        rows[0] = deepcopy(rows[8])
        self.assert_rejected(args)
        args = mutant_fixture()
        args[0]["evidence"]["terminalValidation"]["statePrefix"]["mismatchSamples"][0].update(
            id=8, expectedKey=0, actualKey=0, expectedCount=2, actualCount=1, expectedSum=10, actualSum=9)
        self.assert_rejected(args)

    def test_restored_numeric_receipts_cannot_use_boolean_equal_to_one(self):
        for field in check.KEY_PROGRESS_FIELDS:
            args = mutant_fixture()
            args[0]["evidence"]["stateRecovery"]["recoveries"][0]["restoredKeys"][0]["keyProgress"][field] = True
            with self.subTest(field=field):
                self.assert_rejected(args)

    def test_reset_receipt_must_follow_its_restored_transition_in_same_execution(self):
        for change in (lambda row: row.update(lineNumber=1),
                       lambda row: row.update(executionAttempt=2),
                       lambda row: row["process"].update(runtimeId="foreign"),
                       lambda row: row.update(restoredCheckpointId=43)):
            args = mutant_fixture()
            change(args[0]["evidence"]["stateRecovery"]["recoveries"][0]["mutantResets"][0])
            self.assert_rejected(args)

    def test_skipped_input_cannot_be_hidden_by_self_consistent_reset_receipts(self):
        args = mutant_fixture()
        proof = args[0]["evidence"]["stateRecovery"]["recoveries"][0]
        transition = proof["restoredKeys"][0]["keyProgress"]
        transition["inputId"] += 8
        transition["newSum"] += 8
        proof["postRestoreProgress"]["progress"]["maximumInputId"] = transition["inputId"]
        self.assert_rejected(args)

    def test_correct_and_uninterrupted_controls_remain_accepted(self):
        for mode, execution in (("correct", "uninterrupted"), ("correct", "restart"), ("mutant", "uninterrupted")):
            with self.subTest(mode=mode, execution=execution):
                self.assertTrue(check.audit_attempt(*base.fixture(mode, execution))["confirmed"])


if __name__ == "__main__":
    unittest.main()
