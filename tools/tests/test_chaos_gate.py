import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools'))
import pr_gate
from chaos_profiles import PROFILES
from gate_evidence import fault_requirements, fault_status, data_difference, committer_counts
import test_pr_gate as fixtures

class ChaosProfileTest(unittest.TestCase):
    def test_explicit_profiles_have_unique_existing_names_and_cover_quick(self):
        self.assertEqual(10, len(PROFILES['chaos-quick']))
        self.assertEqual(72, len(PROFILES['chaos-full']))
        for name in PROFILES['chaos-full']:
            if name.startswith('broker-'):
                self.assertTrue(name.endswith(('-v1', '-v2')), name)
                self.assertNotIn(name[:-3], PROFILES['chaos-full'])
        self.assertTrue(set(PROFILES['chaos-quick']) <= set(PROFILES['chaos-full']))
        for names in PROFILES.values():
            self.assertEqual(len(names), len(set(names)))
            for name in names:
                path = pr_gate.canonical(ROOT, name)
                self.assertTrue(path.with_name(name + '.expected.yaml').is_file())
                self.assertEqual(not name.startswith(('broker-eos-control', 'pooling-broker-control', 'rolling-control')), bool(fault_requirements(path.read_text())), name)
        self.assertNotIn('protocol-add-partitions-concurrent-v2', PROFILES['chaos-full'])

    def test_calibration_override_accepts_every_profile_catalog_and_rejects_ambiguity(self):
        for name in PROFILES['chaos-full']:
            text = pr_gate.canonical(ROOT, name).read_text()
            altered = pr_gate.with_producer_max_block(text, 5000)
            self.assertEqual(1, altered.count('--producerMaxBlockMs'))
            with self.assertRaises(SystemExit):
                pr_gate.with_producer_max_block(altered, 5000)
        with self.assertRaises(SystemExit):
            pr_gate.with_producer_max_block('no job', 5000)

    def test_pooling_recovery_requires_both_protocol_and_process_effects(self):
        text = pr_gate.canonical(ROOT, 'pooling-list-transactions-delay-v1').read_text()
        self.assertEqual({'networkFaults': 1, 'taskManagerKills': 1}, fault_requirements(text))
        self.assertEqual('unavailable', fault_status({'networkFaults': [{'triggered': True}]}, fault_requirements(text)))

    def test_rolling_restart_requires_all_six_physical_operations(self):
        text = pr_gate.canonical(ROOT, 'rolling-fixed-v1').read_text()
        self.assertEqual({'brokerOperations': 6}, fault_requirements(text))
        self.assertEqual('unavailable', fault_status({'brokerOperations': [{'confirmed': True}] * 2}, fault_requirements(text)))

    def test_missing_or_false_effect_is_not_confirmed(self):
        for key, field, count in [('networkFaults','triggered',1),('brokerOperations','confirmed',2),('taskManagerKills','confirmed',1)]:
            self.assertEqual('unavailable',fault_status({},{key:count}))
            self.assertEqual('confirmed',fault_status({key:[{field:True}]*count},{key:count}))
            self.assertEqual('unconfirmed',fault_status({key:[{field:False}]*count},{key:count}))
        self.assertEqual('not-required',fault_status({},{}))

    def test_candidate_only_data_failure_needs_complete_verified_base(self):
        baseline=pr_gate.summarize('x','baseline',1,fixtures.run_result(),fixtures.CANDIDATE_HASH,0)
        failure=fixtures.run_result(status='fail');failure['evidence']['terminalValidation']['missing']=17
        candidate=pr_gate.summarize('x','candidate',1,failure,fixtures.CANDIDATE_HASH,1)
        self.assertEqual('yes',data_difference([baseline,candidate],1)['status'])
        for change in ({'missing':None},{'subjectOk':False},{'faultStatus':'unavailable'}):
            self.assertEqual('unknown',data_difference([dict(baseline,**change),candidate],1)['status'])
        self.assertEqual('unknown',data_difference([baseline,candidate],2)['status'])
        self.assertEqual('no',data_difference([dict(baseline,missing=1),candidate],1)['status'])
        self.assertEqual('unknown',data_difference([],1)['status'])

    def test_committer_warnings_and_missing_coverage_are_separate(self):
        self.assertIsNone(committer_counts(None)['WARN'])
        value=committer_counts({'coverage':'partial','events':[
            {'logger':'org.apache.flink.connector.kafka.sink.internal.KafkaCommitter','level':'WARN'},
            {'logger':'org.apache.flink.connector.kafka.sink.internal.KafkaCommitter','level':'ERROR'},
            {'logger':'other.KafkaCommitter','level':'WARN'}]})
        self.assertEqual({'ERROR':1,'WARN':1,'coverage':'partial'},value)

    def test_dry_run_never_invokes_a_subprocess_or_creates_output(self):
        output=ROOT/'jobs/dry-run-must-not-exist'
        argv=['pr_gate.py','--profile','chaos-quick','--runs','2','--dry-run','--connector-jar','missing.jar','--runtime-dir','missing',
              '--baseline-connector-jar','parent.jar','--baseline-runtime-dir','parent-runtime','--output',str(output)]
        with patch.object(sys,'argv',argv),patch('pr_gate.Path.cwd',return_value=ROOT),patch('pr_gate.subprocess.run',side_effect=AssertionError('Unexpected execution')):
            self.assertEqual(0,pr_gate.main())
        self.assertFalse(output.exists())

class ChaosCommandTest(fixtures.GateCommandTest):
    def test_calibration_setting_is_identical_and_explicit(self):
        original = (self.root / 'scenarios/bounded-eos.yaml').read_text()
        invocation, output = self.command(extra=('--producer-max-block-ms', '5000'))
        self.assertEqual(0, invocation.returncode, invocation.stderr)
        manifest = json.loads((output / 'manifest.json').read_text())
        self.assertEqual(5000, manifest['plan']['producerMaxBlockMs'])
        for side in ('baseline', 'candidate'):
            text = (output / (side + '-catalog/bounded-eos.yaml')).read_text()
            self.assertIn('--producerMaxBlockMs, "5000"', text)
        self.assertEqual(original, (self.root / 'scenarios/bounded-eos.yaml').read_text())

    def test_invalid_calibration_setting_is_rejected_before_execution(self):
        for value in ('0', '-1', '2147483648'):
            invocation, output = self.command(extra=('--producer-max-block-ms', value))
            self.assertEqual(2, invocation.returncode)
            self.assertFalse(output.exists())
            self.assertFalse(self.calls.exists())

    def test_dry_run_has_no_output_or_maven_calls(self):
        result,output=self.command(extra=('--dry-run','--runs','2'))
        self.assertEqual(0,result.returncode,result.stderr);self.assertIn('"totalRuns": 4',result.stdout)
        self.assertFalse(output.exists());self.assertFalse(self.calls.exists())

    def test_profile_rejects_release_baseline_before_running(self):
        result,output=self.command(extra=('--profile','chaos-quick'))
        self.assertEqual(2,result.returncode);self.assertIn('explicit PR parent',result.stderr)
        self.assertFalse(output.exists());self.assertFalse(self.calls.exists())

    def test_missing_effect_cannot_pass_and_json_retains_counts(self):
        result=fixtures.run_result();result['evidence'].pop('taskManagerKills')
        invocation,output=self.command(candidate=result)
        self.assertEqual(1,invocation.returncode)
        summary=json.loads((output/'summary.json').read_text())
        self.assertEqual('unavailable',summary['rows'][1]['faultStatus']);self.assertEqual('unknown',summary['candidateOnlyDataFailure']['status'])

    def test_full_image_name_changes_copies_only(self):
        invocation,output=self.command(extra=('--flink-image','docker.io/library/flink:2.2.0'))
        self.assertEqual(0,invocation.returncode,invocation.stderr)
        for side in ('baseline','candidate'):
            self.assertIn('image: docker.io/library/flink:2.2.0',(output/(side+'-catalog/bounded-eos.yaml')).read_text())
        self.assertIn('image: flink:2.2.0',(self.root/'scenarios/bounded-eos.yaml').read_text())
