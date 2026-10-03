import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kafka_chaos_batch as batch


class KafkaChaosBatchTests(unittest.TestCase):
    def test_prepare_cli_allows_default_all_stages_and_explicit_selection(self):
        root = Path.cwd().absolute()
        for selected in ([], ['--stage', 'packet']):
            argv = ['batch', 'prepare', '--output', str(root / 'unused-output'),
                    '--launcher', 'launcher.json', '--legacy-build', 'legacy', '--quick-build', 'quick', *selected]
            with patch.object(sys, 'argv', argv), patch.object(batch, 'prepare', return_value={}) as prepare, \
                    patch.object(batch, 'summary'):
                self.assertEqual(0, batch.main())
                self.assertEqual(['packet'] if selected else None, prepare.call_args.args[-1])

    def test_copy_accepts_both_equivalent_images_but_not_changed_versions(self):
        full = 'docker.io/library/flink:2.2.0'
        for image in ('flink:2.2.0', full):
            document = '  flink:\n    image: ' + image + '\n'
            copied = batch.with_flink_image(document, full)
            self.assertEqual('  flink:\n    image: ' + full + '\n', copied)
            self.assertEqual(copied, batch.with_flink_image(copied, full))
        for document in ('    image: flink:2.2.01\n', '    image: flink:2.3.0\n',
                         '    image: flink:2.2.0\n    image: ' + full + '\n'):
            with self.assertRaises(SystemExit):
                batch.with_flink_image(document, full)

    def test_matrix_order_and_controls_preserve_calibration_contracts(self):
        cells = batch.matrix()
        self.assertEqual(157, len(cells))
        self.assertEqual(157, len({cell['id'] for cell in cells}))
        self.assertEqual(['broker-eos-control', 'broker-eos-kill'], [c['scenario'] for c in cells[:2]])
        self.assertEqual(['broker-leader-kill', 'broker-leader-pause', 'broker-coordinator-pause'], [c['scenario'] for c in cells[2:5]])
        legacy = [c for c in cells if c['group'] == 'legacy']
        self.assertEqual(['control', 'control', 'assume', 'assume', 'rewrite', 'rewrite'], [c['side'] for c in legacy])
        quick = [c for c in cells if c['group'] == 'quick']
        self.assertEqual(set(batch.PROFILES['chaos-quick']), {c['scenario'] for c in quick})
        self.assertEqual(40, len(quick))
        controls = [i for i, c in enumerate(cells) if c['group'] == 'new-controls']
        faults = [i for i, c in enumerate(cells) if c['group'] in ('pooling', 'rolling')]
        self.assertEqual(6, len(controls))
        self.assertLess(max(controls), min(faults))

    def test_new_stages_are_independent_append_only_and_controls_precede_faults(self):
        cells=batch.matrix()
        self.assertTrue(all(cell['group'] not in batch.NEW_STAGES for cell in cells[:99]))
        self.assertEqual(['packet','parallel','at-least-once','savepoint'],list(dict.fromkeys(c['group'] for c in cells[99:])))
        for stage,count in [('packet',8),('parallel',36),('at-least-once',8),('savepoint',6)]:
            selected=batch.matrix([stage]);self.assertEqual(count,len(selected))
            self.assertTrue(all(c['group']==stage for c in selected))
            controls=[i for i,c in enumerate(selected) if 'control' in c['scenario']]
            faults=[i for i,c in enumerate(selected) if 'control' not in c['scenario']]
            self.assertLess(max(controls),min(faults))

    def test_at_least_once_batch_accepts_counted_duplicates_only_with_matching_mode(self):
        cell=dict(group='at-least-once',side='release',scenario='alo',requirements={},oracleMode='at-least-once')
        row=dict(subjectOk=True,faultStatus='not-required',verdict='pass',exitCode=0,missing=0,duplicates=12,oracleMode='at-least-once')
        self.assertTrue(batch.accepted(cell,row))
        for changed in [dict(missing=1),dict(duplicates=None),dict(oracleMode='exactly-once'),dict(verdict='inconclusive')]:
            self.assertFalse(batch.accepted(cell,{**row,**changed}))

    def test_packet_probe_is_isolated_pinned_and_failure_is_never_retried(self):
        pin=json.loads((Path(__file__).resolve().parents[2]/'docs/packet-image-pin.json').read_text())
        plan=batch.packet_probe_plan(pin);argv=plan['argv']
        self.assertEqual('none',argv[argv.index('--network')+1]);self.assertEqual('ALL',argv[argv.index('--cap-drop')+1])
        self.assertEqual('NET_ADMIN',argv[argv.index('--cap-add')+1]);self.assertIn('--read-only',argv)
        self.assertNotIn('--privileged',argv);self.assertNotIn('-v',argv);self.assertNotIn('--mount',argv)
        self.assertIn(pin['image'],argv)
        with tempfile.TemporaryDirectory() as temporary:
            output=Path(temporary);manifest=dict(root=str(output),packetProbe=plan,launcher={'environment':{}})
            def failed(argv,**kwargs):
                kwargs['stdout'].write('probe denied\n')
                return __import__('subprocess').CompletedProcess(argv,1)
            with patch.object(batch.subprocess,'run',side_effect=failed) as run:
                for _ in range(2):
                    with self.assertRaisesRegex(ValueError,'probe'):batch.ensure_packet_probe(output,manifest)
                self.assertEqual(1,run.call_count)
            self.assertTrue((output/'packet-probe/completed.json').is_file())

    def test_packet_probe_success_requires_marker_and_exact_image_identity(self):
        pin=json.loads((Path(__file__).resolve().parents[2]/'docs/packet-image-pin.json').read_text())
        for correct in (True,False):
            with tempfile.TemporaryDirectory() as temporary:
                output=Path(temporary);manifest=dict(root=str(output),packetProbe=batch.packet_probe_plan(pin),launcher={'environment':{}})
                def success(argv,**kwargs):
                    if argv[1]=='run':kwargs['stdout'].write('PACKET_NET_ADMIN_OK\n')
                    else:json.dump([dict(Id=pin['configImageId'] if correct else 'foreign',Os='linux',Architecture='arm64',RepoDigests=[pin['image']])],kwargs['stdout'])
                    return __import__('subprocess').CompletedProcess(argv,0)
                with patch.object(batch.subprocess,'run',side_effect=success) as run:
                    if correct:
                        self.assertTrue(batch.ensure_packet_probe(output,manifest)['accepted'])
                        self.assertTrue(batch.ensure_packet_probe(output,manifest)['accepted'])
                    else:
                        with self.assertRaisesRegex(ValueError,'identity'):batch.ensure_packet_probe(output,manifest)
                    self.assertEqual(2,run.call_count)

    def test_failed_probe_prevents_any_packet_scenario_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            output=Path(temporary)
            batch.write_new(output/'manifest.json',dict(root=str(output),cells=[dict(id='packet',group='packet')]))
            with patch.object(batch,'verify'),patch.object(batch,'completed_cell',return_value=None), \
                    patch.object(batch,'ensure_packet_probe',side_effect=ValueError('probe failed')),patch.object(batch,'run_cell') as run:
                with self.assertRaisesRegex(ValueError,'probe'):batch.resume(output,1,['packet'])
                run.assert_not_called()

    def test_mutant_exit_one_is_not_detection_without_the_metric_and_evidence(self):
        cell = dict(group='legacy', side='assume', scenario='commit-request-lost', requirements={'networkFaults': 1})
        row = dict(subjectOk=True, faultStatus='confirmed', verdict='fail', exitCode=1, missing=12, duplicates=0)
        self.assertTrue(batch.accepted(cell, row))
        for key, value in [('missing', None), ('missing', 0), ('subjectOk', False), ('faultStatus', 'unavailable'), ('exitCode', 2)]:
            self.assertFalse(batch.accepted(cell, {**row, key: value}))
        cell['side'] = 'control'
        self.assertFalse(batch.accepted(cell, row))

    def test_retained_results_are_never_overwritten_and_interrupted_cells_are_not_repeated(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / 'cell'
            self.assertIsNone(batch.completed_cell(directory))
            directory.mkdir()
            with self.assertRaisesRegex(ValueError, 'Interrupted/ambiguous'):
                batch.completed_cell(directory)
            (directory / 'stdout.json').write_text('{}')
            digest = batch.sha256(directory / 'stdout.json')
            record = {'accepted': True, 'outputHashes': {'stdout.json': digest}}
            batch.write_new(directory / 'completed.json', record)
            self.assertEqual(record, batch.completed_cell(directory))
            with self.assertRaises(FileExistsError):
                batch.write_new(directory / 'completed.json', {})
            (directory / 'stdout.json').write_text('{"changed": true}')
            with self.assertRaisesRegex(ValueError, 'Retained result changed'):
                batch.completed_cell(directory)

    def test_changed_frozen_input_or_tree_fails_before_a_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jar = root / 'engine.jar'
            jar.write_bytes(b'first')
            manifest = dict(root=str(root), sourceTree='tree', inputs={str(jar): batch.sha256(jar)})
            with patch.object(batch, 'clean_tree', return_value='tree'):
                batch.verify(manifest)
                jar.write_bytes(b'second')
                with self.assertRaisesRegex(ValueError, 'Frozen input changed'):
                    batch.verify(manifest)
            with patch.object(batch, 'clean_tree', return_value='different'):
                with self.assertRaisesRegex(ValueError, 'Harness tree changed'):
                    batch.verify(manifest)

    def test_resume_skips_completed_cells_and_stops_at_a_failed_control(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            cells = [dict(id='first', group='brokers'), dict(id='second', group='brokers'), dict(id='third', group='targets')]
            batch.write_new(output / 'manifest.json', dict(root=str(output), cells=cells))
            first = dict(accepted=True, row=dict(verdict='pass', exitCode=0))
            failure = dict(accepted=False, row=dict(verdict='inconclusive', exitCode=2))
            with patch.object(batch, 'verify'), patch.object(batch, 'completed_cell', side_effect=[first, None]), \
                    patch.object(batch, 'run_cell', return_value=failure) as run, contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, 'Unclassified result'):
                    batch.resume(output, 3)
                run.assert_called_once()
                self.assertEqual('second', run.call_args.args[-1]['id'])

    def test_bounded_invocation_does_not_start_the_next_cell(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            cells = [dict(id='first', group='brokers'), dict(id='second', group='brokers')]
            batch.write_new(output / 'manifest.json', dict(root=str(output), cells=cells))
            value = dict(accepted=True, row=dict(verdict='pass', exitCode=0))
            with patch.object(batch, 'verify'), patch.object(batch, 'completed_cell', return_value=None), \
                    patch.object(batch, 'run_cell', return_value=value) as run, contextlib.redirect_stdout(io.StringIO()):
                _, completed = batch.resume(output, 1)
                self.assertEqual(['first'], list(completed))
                run.assert_called_once()

    def test_symlinks_cannot_escape_the_harness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'link').symlink_to(root / 'missing')
            with self.assertRaisesRegex(ValueError, 'Symlink'):
                batch.inside(root, root / 'link' / 'file')


if __name__ == '__main__':
    unittest.main()
