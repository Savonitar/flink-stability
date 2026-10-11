"""Retained physical evidence must not invent transaction or visibility boundaries."""
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
sys.path.insert(0, str(ROOT / 'tools'))
import analyze_attempt as analyzer


class AnalyzeAttemptTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='.analysis-test-', dir=ROOT)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        shutil.copytree(ROOT / 'tools/tests/fixtures/analyze_attempt', self.root, dirs_exist_ok=True)
        self.result = self.root / 'result.json'
        self.logs = self.root / 'kafka-logs'

    def rewrite(self, name, update, rehash=True):
        path = self.logs / name
        data = json.loads(path.read_text())
        update(data)
        path.write_text(json.dumps(data))
        if rehash:
            result = json.loads(self.result.read_text())
            receipt = result['evidence']['kafkaLogs']['decoded'][0] if name.startswith('decoded') else result['evidence']['kafkaLogs']['transactions']
            receipt['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            self.result.write_text(json.dumps(result))

    def analyze(self, **kwargs):
        return analyzer.analyze(self.result, self.logs, **kwargs)

    def test_real_shaped_fixture_explains_all_requested_sections(self):
        report = self.analyze()
        self.assertEqual([], report['diagnostics'])
        self.assertEqual('fail', report['verdict']['status'])
        self.assertEqual(2, report['oracle']['duplicates'])
        self.assertEqual([{'partition': 0, 'offset': 3, 'rawValue': '1'},
                          {'partition': 0, 'offset': 4, 'rawValue': '2'}], report['oracle']['duplicateSamples'])
        self.assertEqual([], report['oracle']['malformedSamples'])
        self.assertEqual([], report['oracle']['unexpectedSamples'])
        self.assertEqual(1, report['checkpoints']['restoredCheckpoints'])
        window = report['killWindows'][0]
        self.assertTrue(window['restWindowConfirmed'])
        self.assertEqual(2, window['checkpointId'])
        for key, start in [('brokerBeforeKill', 1000), ('brokerAfterKill', 1500)]:
            transaction = window[key][0]['transactions'][0]
            self.assertEqual(('Ongoing', 7, 2, start), tuple(transaction[field] for field in ('state', 'producerId', 'epoch', 'startTimeMs')))
        groups = report['transactions']
        self.assertEqual(3, len(groups))
        self.assertEqual([2, 5, 7], [group['marker']['offset'] for group in groups])
        self.assertEqual(['COMMIT', 'COMMIT', 'ABORT'], [group['outcome'] for group in groups])
        self.assertEqual((7, 2), (groups[0]['producerId'], groups[0]['producerEpoch']))
        self.assertEqual((7, 2), (groups[1]['producerId'], groups[1]['producerEpoch']))
        self.assertNotEqual(groups[0]['transaction'], groups[1]['transaction'])
        self.assertEqual('after-prior-marker', groups[1]['startBoundary'])
        self.assertEqual([1, 2], [item['id'] for item in report['duplicateIds']])
        self.assertEqual('proven-in-retained-bytes', report['duplicateBlockProof']['status'])
        self.assertEqual(4, len(report['transactionStateHistory']))
        self.assertEqual(1, len(report['taskManagerMarkers'][0]['matches']))

    def test_repeated_regexes_override_upstream_default_without_vendor_assumptions(self):
        report = self.analyze(markers=['optional test marker', 'Recovered transaction'])
        self.assertEqual([1, 2], [match['lineNumber'] for match in report['taskManagerMarkers'][0]['matches']])
        result = subprocess.run([sys.executable, str(ROOT / 'tools/analyze_attempt.py'),
                                 str(self.result), str(self.logs), '--json'], capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual('fail', json.loads(result.stdout)['verdict']['status'])
        invalid = subprocess.run([sys.executable, str(ROOT / 'tools/analyze_attempt.py'),
                                  str(self.result), str(self.logs), '--marker', '['], capture_output=True, text=True)
        self.assertEqual(2, invalid.returncode)
        self.assertNotIn('Traceback', invalid.stderr)

    def test_jobmanager_markers_keep_line_order_and_scope_separate(self):
        path = self.root / 'flink-logs/flink-stability-output-jobmanager-1-1.log'
        path.write_text('Completed checkpoint 1\n'
                        'Declining checkpoint 2\n'
                        'Declining checkpoint 3\n'
                        'Restarting job\n'
                        'Restoring job from checkpoint 1\n')
        patterns = [r'Completed checkpoint', r'Declining checkpoint', r'Restart', r'Restor']
        report = self.analyze(jobmanager_markers=patterns)
        self.assertEqual([], report['diagnostics'])
        self.assertEqual(1, len(report['taskManagerMarkers'][0]['matches']))
        jobmanager = report['jobManagerMarkers'][0]
        self.assertEqual(str(path), jobmanager['file'])
        self.assertEqual([1, 2, 3, 4, 5], [match['lineNumber'] for match in jobmanager['matches']])
        self.assertEqual(['Restoring job from checkpoint 1'], [match['line'] for match in jobmanager['matches']
                                                             if r'Restor' in match['patterns']])
        self.assertIn('JobManager log: ' + str(path), analyzer.render(report))
        cli = subprocess.run([sys.executable, str(ROOT / 'tools/analyze_attempt.py'),
                              str(self.result), str(self.logs), '--json',
                              '--jobmanager-marker', 'Completed checkpoint',
                              '--jobmanager-marker', 'Restoring'], capture_output=True, text=True)
        self.assertEqual(0, cli.returncode, cli.stderr)
        self.assertEqual([1, 5], [match['lineNumber'] for match in json.loads(cli.stdout)['jobManagerMarkers'][0]['matches']])
        invalid = subprocess.run([sys.executable, str(ROOT / 'tools/analyze_attempt.py'),
                                  str(self.result), str(self.logs), '--jobmanager-marker', '['],
                                 capture_output=True, text=True)
        self.assertEqual(2, invalid.returncode)
        self.assertNotIn('Traceback', invalid.stderr)

    def test_jobmanager_logs_are_optional_but_requested_missing_or_symlinked_logs_are_reported(self):
        self.assertEqual([], self.analyze()['jobManagerMarkers'])
        report = self.analyze(jobmanager_markers=['checkpoint'])
        self.assertTrue(any(item['code'] == 'flink.logs-missing' and 'JobManager' in item['message']
                            for item in report['diagnostics']))
        path = self.root / 'flink-logs/flink-stability-output-jobmanager-1-1.log'
        path.symlink_to(self.root / 'flink-logs/flink-stability-output-taskmanager-1-1.log')
        report = self.analyze(jobmanager_markers=['transaction'])
        self.assertEqual([], report['jobManagerMarkers'])
        self.assertTrue(any(item['code'] == 'flink.log-unreadable' and 'symlink' in item['message']
                            for item in report['diagnostics']))

    def test_unresolved_sink_transactions_are_separate_from_the_data_verdict(self):
        data = json.loads(self.result.read_text())
        data['status'] = 'pass'
        unresolved = [{'transactionalId': 'fixture-sink-0-0', 'state': 'Ongoing',
                       'producerId': 7, 'producerEpoch': 2, 'partitions': ['output-0']}]
        data['evidence']['sinkTransactions'].update(unresolved=unresolved)
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertEqual('pass', report['verdict']['status'])
        self.assertEqual(unresolved, report['sinkTransactions']['unresolved'])
        self.assertEqual(1, report['sinkTransactions']['total'])
        self.assertIn('Sink transactions after fence:', analyzer.render(report))
        data['evidence']['sinkTransactions'] = {'status': 'not-listed'}
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertIsNone(report['sinkTransactions']['unresolved'])
        self.assertIsNone(report['sinkTransactions']['total'])
        data['evidence']['sinkTransactions'] = {'status': 'listed', 'total': 'unknown', 'unresolved': [{}]}
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertIsNone(report['sinkTransactions']['unresolved'])
        self.assertIsNone(report['sinkTransactions']['total'])
        self.assertIn('kafka.unresolved-unavailable', [item['code'] for item in report['diagnostics']])
        self.assertIn('kafka.transaction-total-unavailable', [item['code'] for item in report['diagnostics']])

    def test_noncontiguous_or_extra_copies_do_not_prove_a_two_transaction_block(self):
        def noncontiguous(data):
            data['logs'][0]['segment']['batches'][2]['records'][1]['canonicalId'] = 4
        self.rewrite('decoded-0.json', noncontiguous)
        report = self.analyze()
        self.assertEqual([1], [item['id'] for item in report['duplicateIds']])
        # One repeated ID is a one-record block; no claim about IDs 2 and 4 is made.
        self.assertEqual(1, report['duplicateBlockProof']['copies'][0]['count'])
        extra = copy.deepcopy(report['transactions'][0])
        extra['transaction'] = 'a-third-transaction'
        duplicates, proof = analyzer.duplicate_evidence([*report['transactions'], extra])
        self.assertFalse(proof['sameTwoTransactions'])
        self.assertEqual('not-proven', proof['status'])
        self.assertEqual(3, len(duplicates[0]['copies']))

    def test_newer_epoch_abort_marker_closes_old_data_and_preserves_both_epochs(self):
        self.rewrite('decoded-0.json', lambda data: data['logs'][0]['segment']['batches'][5].update(producerEpoch=1))
        report = self.analyze()
        self.assertEqual([], report['diagnostics'])
        transaction = report['transactions'][-1]
        self.assertEqual(3, len(report['transactions']))
        self.assertEqual('ABORT', transaction['outcome'])
        self.assertEqual(0, transaction['producerEpoch'])
        self.assertEqual(1, transaction['marker']['producerEpoch'])
        self.assertEqual([{'offset': 6, 'id': 3}], transaction['records'])

    def test_new_session_after_commit_keeps_prior_marker_boundary(self):
        def update(data):
            for batch in data['logs'][0]['segment']['batches'][2:4]:
                batch['producerEpoch'] = 3
        self.rewrite('decoded-0.json', update)
        report = self.analyze()
        self.assertEqual([], report['diagnostics'])
        first, second = report['transactions'][:2]
        self.assertEqual((2, 3), (first['producerEpoch'], second['producerEpoch']))
        self.assertEqual('after-prior-marker', second['startBoundary'])
        self.assertEqual([1, 2], [record['id'] for record in second['records']])

    def test_older_epoch_marker_does_not_close_newer_data(self):
        self.rewrite('decoded-0.json', lambda data: data['logs'][0]['segment']['batches'][4].update(producerEpoch=1))
        report = self.analyze()
        self.assertEqual('NO_RETAINED_MARKER', report['transactions'][-2]['outcome'])
        self.assertEqual([], report['transactions'][-1]['records'])
        self.assertIn('kafka.marker-epoch-regression', [item['code'] for item in report['diagnostics']])
        self.assertEqual('not-proven-incomplete-evidence', report['duplicateBlockProof']['status'])

    def test_marker_does_not_choose_between_multiple_open_epochs(self):
        def update(data):
            batches = data['logs'][0]['segment']['batches']
            batches[1].update(control=False)
            batches[1]['records'][0].update(canonicalId=9, marker=None)
            batches[2]['producerEpoch'] = 3
            batches[3]['producerEpoch'] = 4
        self.rewrite('decoded-0.json', update)
        report = self.analyze()
        self.assertEqual(['NO_RETAINED_MARKER', 'NO_RETAINED_MARKER', 'COMMIT'],
                         [item['outcome'] for item in report['transactions'][:3]])
        self.assertEqual([], report['transactions'][2]['records'])
        self.assertIn('kafka.marker-ambiguous', [item['code'] for item in report['diagnostics']])

    def test_interleaved_nonduplicate_record_breaks_contiguous_block_proof(self):
        transactions = self.analyze()['transactions']
        second = transactions[1]
        second['records'] = [{'offset': 3, 'id': 1}, {'offset': 4, 'id': 9}, {'offset': 5, 'id': 2}]
        _, proof = analyzer.duplicate_evidence(transactions)
        self.assertTrue(proof['sameTwoTransactions'])
        self.assertFalse(proof['copies'][1]['offsetsContiguous'])
        self.assertEqual('not-proven', proof['status'])

    def test_aborted_physical_replays_are_not_reported_as_committed_duplicate_block(self):
        self.rewrite('decoded-0.json', lambda data: data['logs'][0]['segment']['batches'][3]['records'][0]['marker'].update(type='ABORT'))
        report = self.analyze()
        self.assertEqual(2, len(report['duplicateIds']))
        self.assertEqual('not-applicable', report['duplicateBlockProof']['status'])

    def test_capture_gaps_do_not_merge_reused_epochs_across_unknown_boundaries(self):
        self.rewrite('decoded-0.json', lambda data: data['logs'][0]['segment']['batches'].pop(1))
        report = self.analyze()
        self.assertIn('kafka.offset-gap', [item['code'] for item in report['diagnostics']])
        self.assertEqual('NO_RETAINED_MARKER', report['transactions'][0]['outcome'])
        self.assertEqual('capture-gap', report['transactions'][1]['startBoundary'])

    def test_duplicate_capture_does_not_invent_duplicate_records(self):
        data = json.loads(self.result.read_text())
        data['evidence']['kafkaLogs']['decoded'].append(copy.deepcopy(data['evidence']['kafkaLogs']['decoded'][0]))
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertEqual(3, len(report['transactions']))
        self.assertEqual(2, len(report['duplicateIds'][0]['copies']))
        self.assertTrue(all(item['severity'] == 'info' for item in report['diagnostics']))

    def test_missing_hash_mismatch_and_malformed_json_are_explicit(self):
        self.rewrite('decoded-0.json', lambda data: data.update(detail='changed'), rehash=False)
        report = self.analyze()
        self.assertEqual([], report['transactions'])
        self.assertTrue(any('SHA-256' in item['message'] for item in report['diagnostics']))
        self.result.write_text('{"status":"pass","status":"fail"}')
        report = self.analyze()
        self.assertIsNone(report['verdict']['status'])
        self.assertTrue(any('Duplicate JSON key' in item['message'] for item in report['diagnostics']))

    def test_untrusted_external_paths_and_symlinks_are_not_read(self):
        data = json.loads(self.result.read_text())
        data['evidence']['kafkaLogs']['decoded'][0]['evidence'] = '/outside-evidence-roots/decoded-0.json'
        data['attempt']['checkpointRoot'] = '/outside-evidence-roots/logs'
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertTrue(any('outside the explicit evidence roots' in item['message'] for item in report['diagnostics']))
        target = self.logs / 'decoded-0.json'
        content = target.read_text()
        target.unlink()
        replacement = self.root / 'replacement.json'
        replacement.write_text(content)
        target.symlink_to(replacement)
        data['evidence']['kafkaLogs']['decoded'][0]['evidence'] = 'decoded-0.json'
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertTrue(any('symlink' in item['message'] for item in report['diagnostics']))

    def test_complete_retention_copy_supplies_default_logs_without_trusting_new_roots(self):
        copied = self.root / 'retained/checkpoints'
        shutil.copytree(self.root / 'flink-logs', copied)
        data = json.loads(self.result.read_text())
        data['attempt']['checkpointRoot'] = '/outside-evidence-roots/original-checkpoints'
        data['evidence']['checkpointRetention'] = {'status': 'complete', 'checkpointRoot': 'retained/checkpoints'}
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertEqual(1, len(report['taskManagerMarkers'][0]['matches']))
        self.assertTrue(Path(report['taskManagerMarkers'][0]['file']).is_relative_to(copied))
        self.assertEqual([], report['diagnostics'])
        data['evidence']['checkpointRetention']['checkpointRoot'] = '/outside-evidence-roots/retained'
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertEqual([], report['taskManagerMarkers'])
        self.assertTrue(any('outside the explicit evidence roots' in item['message'] for item in report['diagnostics']))
        report = self.analyze(flink_log_dir=self.root / 'flink-logs')
        self.assertEqual(1, len(report['taskManagerMarkers'][0]['matches']))

    def test_incomplete_retention_does_not_replace_original_log_root(self):
        data = json.loads(self.result.read_text())
        data['evidence']['checkpointRetention'] = {'status': 'incomplete', 'checkpointRoot': 'missing-copy'}
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertEqual(1, len(report['taskManagerMarkers'][0]['matches']))
        self.assertEqual([], report['diagnostics'])

    def test_unknown_batch_marker_and_bad_partition_are_not_guessed(self):
        self.rewrite('decoded-0.json', lambda data: data['logs'][0]['segment']['batches'][1]['records'][0]['marker'].update(type='UNKNOWN'))
        report = self.analyze()
        self.assertIn('kafka.batch-malformed', [item['code'] for item in report['diagnostics']])
        self.rewrite('decoded-0.json', lambda data: data.update(expectedPartitionRoot=42))
        report = self.analyze()
        self.assertEqual([], report['transactions'])
        self.assertIn('kafka.partition-malformed', [item['code'] for item in report['diagnostics']])

    def test_malformed_snapshot_and_prefix_produce_diagnostics(self):
        data = json.loads(self.result.read_text())
        data['evidence']['sinkTransactions']['transactionalIdPrefix'] = 12
        window = data['evidence']['taskManagerKills'][0]['checkpointWindow']
        window['restWindowConfirmed'] = 'true'
        window['brokerAfterKill'][0]['transactions'][0]['startTimeMs'] = 'unknown'
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        codes = [item['code'] for item in report['diagnostics']]
        self.assertIn('kafka.sink-prefix-missing', codes)
        self.assertIn('kill.window-fields-malformed', codes)
        self.assertIn('kill.transaction-fields-malformed', codes)

    def test_receipts_cannot_read_arbitrary_json_under_an_evidence_root(self):
        data = json.loads(self.result.read_text())
        data['evidence']['kafkaLogs']['decoded'][0]['evidence'] = 'unrelated.json'
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertIn('evidence.receipt-name-unsupported', [item['code'] for item in report['diagnostics']])

    def test_missing_evidence_does_not_turn_null_counts_into_zero(self):
        self.result.write_text(json.dumps({'status': 'inconclusive', 'reason': 'fixture', 'evidence': {}}))
        report = self.analyze()
        self.assertIsNone(report['oracle']['duplicates'])
        self.assertIsNone(report['checkpoints']['restoredCheckpoints'])
        self.assertEqual([], report['transactions'])
        self.assertTrue(report['diagnostics'])

    def test_real_pass_renderer_shape_omits_empty_defect_samples(self):
        data = json.loads(self.result.read_text())
        data['status'] = 'pass'
        terminal = data['evidence']['terminalValidation']
        terminal.update(status='pass', reason='validator.kafka.id-set.match', duplicates=0, observed=2)
        del terminal['duplicateSamples']
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        for name in ('duplicateSamples', 'malformedSamples', 'unexpectedSamples'):
            self.assertEqual([], report['oracle'][name])
        self.assertFalse(any(item['code'].startswith('oracle.samples-') for item in report['diagnostics']))

    def test_omitted_samples_stay_unknown_without_completed_zero_count_snapshot(self):
        original = json.loads(self.result.read_text())
        for completed, snapshot_complete, count in ((False, True, 0), (True, False, 0),
                                                   (True, True, 2), (True, True, None)):
            with self.subTest(completed=completed, snapshot_complete=snapshot_complete, count=count):
                data = copy.deepcopy(original)
                terminal = data['evidence']['terminalValidation']
                terminal.update(completed=completed, snapshotComplete=snapshot_complete, duplicates=count)
                del terminal['duplicateSamples']
                self.result.write_text(json.dumps(data))
                report = self.analyze()
                self.assertIsNone(report['oracle']['duplicateSamples'])
                self.assertTrue(any(item['code'] == 'oracle.samples-unavailable'
                                    and 'duplicateSamples' in item['message'] for item in report['diagnostics']))

    def test_all_nonempty_defect_samples_preserve_actual_renderer_coordinates_and_values(self):
        data = json.loads(self.result.read_text())
        terminal = data['evidence']['terminalValidation']
        terminal.update(malformed=1, unexpected=1,
                        malformedSamples=[{'partition': 0, 'offset': 8, 'rawValue': 'not-an-id'}],
                        unexpectedSamples=[{'partition': 0, 'offset': 9, 'rawValue': '999'}])
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        for name in ('duplicateSamples', 'malformedSamples', 'unexpectedSamples'):
            self.assertEqual(terminal[name], report['oracle'][name])
        terminal['malformedSamples'] = [{'id': 1}]
        self.result.write_text(json.dumps(data))
        report = self.analyze()
        self.assertIsNone(report['oracle']['malformedSamples'])
        self.assertIn('oracle.samples-malformed', [item['code'] for item in report['diagnostics']])


if __name__ == '__main__':
    unittest.main()
