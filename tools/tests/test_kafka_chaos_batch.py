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
    def test_matrix_order_and_controls_preserve_calibration_contracts(self):
        cells = batch.matrix()
        self.assertEqual(99, len(cells))
        self.assertEqual(99, len({cell['id'] for cell in cells}))
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
