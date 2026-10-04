import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('pool_check', Path(__file__).with_name('check.py'))
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


def batch(offset, epoch, identifier=None, marker=None):
    return {'baseOffset': offset, 'lastOffset': offset, 'producerId': 1,
            'producerEpoch': epoch, 'transactional': True, 'control': marker is not None,
            'records': [{'offset': offset, 'sequence': offset, 'timestamp': 1,
                         'canonicalId': identifier, 'marker': {'type': marker} if marker else None}]}


class EvidenceCheckTest(unittest.TestCase):
    def test_two_committed_transactions_on_one_session_keep_both_copies(self):
        transactions, pending = check.physical_transactions([
            batch(0, 0, 42), batch(1, 0, marker='COMMIT'),
            batch(2, 0, 42), batch(3, 0, marker='COMMIT')])
        self.assertFalse(pending)
        self.assertEqual([42, 42], [r['canonicalId'] for t in transactions for r in t['records']])

    def test_fencing_abort_may_use_newer_epoch(self):
        transactions, pending = check.physical_transactions([batch(0, 1, 42), batch(1, 2, marker='ABORT')])
        self.assertFalse(pending)
        self.assertEqual('ABORT', transactions[0]['marker'])
        self.assertEqual(1, transactions[0]['records'][0]['epoch'])

    def test_unresolved_records_remain_open(self):
        transactions, pending = check.physical_transactions([batch(0, 1, 42)])
        self.assertFalse(transactions)
        self.assertEqual(42, pending[1][0]['canonicalId'])

    def test_ambiguous_or_impossible_history_is_rejected(self):
        for batches in [[batch(0, 1, 42), batch(1, 0, marker='COMMIT')],
                        [batch(0, 0, 42), batch(0, 0, 43)],
                        [batch(0, 0, marker='UNKNOWN')]]:
            with self.assertRaises(ValueError):
                check.physical_transactions(batches)


class MatrixCheckTest(unittest.TestCase):
    def matrix(self, change=None, fault_runs=3):
        from unittest.mock import patch
        root = Path(__file__).absolute().parents[2]
        controls, faults = root / 'jobs/fixture-controls', root / 'jobs/fixture-faults'
        build = {'releaseSha256': check.RELEASE, 'sourceSha256': check.SOURCE,
                 'artifacts': {'mutant': {'sha256': 'm' * 64, 'changedEntries': [check.ENTRY]}},
                 'runtimeDependencySha256': {'client.jar': 'c' * 64}}

        def load(path, scope, digest=None):
            if path.name == 'manifest.json':
                fault = path.parent == faults
                return {'scenarios': [f'pool-reuse-inflight-{"kill" if fault else "control"}-v{v}' for v in [1, 2]],
                        'runs': fault_runs if fault else 1,
                        'baseline': {'connectorSha256': check.RELEASE, 'runtimeDependencySha256': {'client.jar': 'c' * 64}},
                        'candidate': {'connectorSha256': 'm' * 64, 'runtimeDependencySha256': {'client.jar': 'c' * 64}}}
            failed = 'candidate' in path.parts and 'pool-reuse-inflight-kill-v1' in path.parts
            return {'status': 'fail' if failed else 'pass',
                    'reason': 'validator.kafka.id-set.duplicate-ids' if failed else 'validator.kafka.id-set.match',
                    'attempt': {'status': 'fail' if failed else 'pass'},
                    'evidence': {}}

        def audit(run, expected_hash, fault):
            raw = load(run / 'stdout.json', run)
            row = {'verdict': raw['status'], 'reason': raw['reason'], 'exitCode': int(raw['status'] == 'fail'),
                   'hangingTransactionFinding': False, 'fencedResumeObserved': True,
                   'terminalValidation': {'missing': 0, 'malformed': 0, 'unexpected': 0},
                   'duplicateBlock': {'contiguous': True, 'count': 300, 'first': 100, 'last': 399},
                   'killWindow': {'reused': [{'marker': 'ABORT' if 'baseline' in run.parts else 'COMMIT',
                                             'recordCount': 300, 'firstId': 100, 'lastId': 399,
                                             'sameSessionRecommitSignature': True}]}}
            if change:
                change(run, row)
            return row

        with patch.object(check, 'load', side_effect=load), patch.object(check, 'audit_run', side_effect=audit):
            return check.check_matrix(controls, faults, build)

    def test_complete_valid_matrix_can_qualify(self):
        report = self.matrix()
        self.assertTrue(report['qualified'])
        self.assertTrue(report['observedMatrixSatisfied'])
        self.assertEqual([], report['errors'])
        self.assertEqual('README.md#calibration-contract', report['signatureContract'])

    def test_missing_same_session_signature_cannot_qualify(self):
        def change(run, row):
            if 'candidate' in run.parts and 'pool-reuse-inflight-kill-v1' in run.parts:
                row['killWindow']['reused'][0]['sameSessionRecommitSignature'] = False
        report = self.matrix(change)
        self.assertFalse(report['qualified'])
        self.assertTrue(any('signature absent' in error for error in report['errors']))

    def test_all_missed_windows_cannot_be_replaced_by_passing_oracles(self):
        def change(run, row):
            if 'pool-reuse-inflight-kill-v2' in run.parts:
                raise ValueError('checkpoint-window.missed: less than 3 seconds remaining')
        report = self.matrix(change)
        self.assertFalse(report['qualified'])
        self.assertEqual(6, len(report['windowMisses']))
        self.assertTrue(any('only 0 confirmed attempts' in error for error in report['errors']))

    def test_observation_failure_blocks_qualification_despite_enough_confirmed_replacements(self):
        def change(run, row):
            if ('candidate' in run.parts and 'pool-reuse-inflight-kill-v2' in run.parts
                    and run.name == 'run-1'):
                result = {'evidence': {'taskManagerKills': [{
                    'confirmed': True, 'checkpointWindow': {'observationFailure': 'HTTP 500'}}]}}
                check.window(run, result, [], [], {})
        report = self.matrix(change, fault_runs=4)
        self.assertEqual(3, report['confirmedCounts']['candidate/pool-reuse-inflight-kill-v2'])
        self.assertFalse(report['qualified'])
        self.assertFalse(report['observedMatrixSatisfied'])
        self.assertEqual([], report['windowMisses'])
        self.assertEqual(1, len(report['errors']))
        self.assertIn('checkpoint-window.observation-infrastructure: HTTP 500', report['errors'][0])

    def test_failing_control_cannot_qualify(self):
        def change(run, row):
            if 'pool-reuse-inflight-control-v1' in run.parts:
                row['verdict'] = 'fail'
        self.assertFalse(self.matrix(change)['qualified'])

    def test_accepts_old_and_current_partition_receipts_but_not_partial_multi_partition_proof(self):
        sample = {'topic': 'output', 'partition': 0}
        self.assertEqual(sample, check.single_partition_sample(sample))
        self.assertEqual(sample, check.single_partition_sample([sample]))
        for samples in [[], [sample, {'topic': 'output', 'partition': 1}], None]:
            with self.assertRaises(ValueError):
                check.single_partition_sample(samples)


class BuildPathTest(unittest.TestCase):
    def test_explicit_cache_can_be_outside_checkout_while_symlinks_and_missing_paths_reject(self):
        from tempfile import TemporaryDirectory
        from unittest.mock import patch
        build_spec = importlib.util.spec_from_file_location('pool_build', Path(__file__).with_name('build.py'))
        build = importlib.util.module_from_spec(build_spec)
        build_spec.loader.exec_module(build)
        root = Path(__file__).absolute().parents[2]
        parent = root / 'jobs'
        parent.mkdir(exist_ok=True)
        with TemporaryDirectory(dir=parent) as temporary:
            directory = Path(temporary)
            checkout = directory / 'checkout'
            checkout.mkdir()
            cache = directory / 'external-cache'
            cache.mkdir()
            settings = directory / 'explicit-settings.xml'
            settings.write_text('<settings/>')
            with patch.object(build, 'HARNESS', checkout):
                self.assertEqual(cache, build.local_path(cache, True))
                self.assertEqual(settings, build.local_path(settings, False))
                link = directory / 'linked-cache'
                link.symlink_to(cache, target_is_directory=True)
                for path, is_directory in [(link, True), (directory / 'missing', True), (cache, False)]:
                    with self.assertRaises(SystemExit):
                        build.local_path(path, is_directory)


if __name__ == '__main__':
    unittest.main()
