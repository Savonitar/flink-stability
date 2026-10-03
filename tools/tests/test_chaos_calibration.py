import copy
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('calibration_check', ROOT / 'calibration/chaos-profile-mutant/check.py')
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


def report(control=False):
    names = ['broker-eos-control'] if control else list(check.PROFILES['chaos-quick'])
    rows = [dict(scenario=name, side=side, run=1, verdict='pass', exitCode=0, missing=0,
                 duplicates=0, subjectOk=True, faultStatus='not-required' if control else 'confirmed')
            for name in names for side in ('baseline', 'candidate')]
    if not control:
        next(row for row in rows if row['scenario'] == 'commit-request-lost' and row['side'] == 'candidate').update(
            verdict='fail', exitCode=1, missing=10)
    return {'manifest': {'scenarios': names, 'runs': 1, 'plan': {'producerMaxBlockMs': 5000}}, 'rows': rows}


class CalibrationCheckTest(unittest.TestCase):
    def test_requires_matching_healthy_base_and_actual_data_loss(self):
        self.assertEqual('CALIBRATION_DETECTED', check.check(report()))
        for change in ({'missing': 0}, {'verdict': 'inconclusive'}, {'subjectOk': False},
                       {'faultStatus': 'unavailable'}, {'exitCode': 7}):
            value = report()
            candidate = next(row for row in value['rows'] if row['missing'])
            candidate.update(change)
            with self.assertRaises(ValueError):
                check.check(value)

    def test_incomplete_or_failing_baseline_cannot_qualify(self):
        for change in ('missing', 'duplicate', 'baseline-fail', 'wrong-setting'):
            value = report()
            if change == 'missing': value['rows'].pop()
            if change == 'duplicate': value['rows'].append(copy.deepcopy(value['rows'][0]))
            if change == 'baseline-fail': value['rows'][0].update(verdict='fail', exitCode=1, missing=1)
            if change == 'wrong-setting': value['manifest']['plan']['producerMaxBlockMs'] = None
            with self.assertRaises(ValueError):
                check.check(value)

    def test_control_requires_both_sides_to_pass(self):
        self.assertEqual('CONTROL_PASS', check.check(report(True), True))
        value = report(True)
        value['rows'][1].update(verdict='fail', exitCode=1, missing=1)
        with self.assertRaises(ValueError):
            check.check(value, True)
