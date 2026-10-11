"""Docker-free supervisor tests: no real Java, Docker, Git or child processes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kafka_lifecycle_probe as probe

NID, CID, HISTORIC_NETWORK, HISTORIC_CONTAINER = ('a' * 64, 'b' * 64, 'c' * 64, 'd' * 64)
IMAGE = 'sha256:' + 'e' * 64
DIGEST = 'example/kafka@sha256:' + 'f' * 64


def put(directory, name, value):
    (directory / name).write_text(json.dumps(value) if isinstance(value, dict) else value)


def evidence(directory, case):
    directory.mkdir()
    for name in ['startup-submitted', 'created-launch', 'host-top']:
        put(directory, name + '.json', {'recorded': True})
    put(directory, 'image.json', {'id': IMAGE})
    put(directory, 'ready.json', {'containerId': CID, 'imageId': IMAGE})
    for name in ['namespace-pid-one.txt', 'selected-broker.properties', 'broker-config.raw',
            'kafka-final.log', 'kafka-continuous.log']:
        put(directory, name, 'retained textual evidence\n')
    for name in ['broker-config.transfer', 'starter.transfer']:
        put(directory, name + '.json', {'status': 'completed', 'bytesCaptured': 27})
    for name in ['pid1-status', 'pid1-stat', 'pid1-cmdline', 'pid1-namespace']:
        put(directory, name + '.command.json', {'status': 'completed', 'exitCode': 0})
        put(directory, name + '.stdout', 'observed\n')
        put(directory, name + '.stderr', '')
    if case in probe.CASES[:2]:
        put(directory, 'actual-starter.sh', '#!/bin/sh\nexec vendor\n')
    if case != 'default-apache':
        put(directory, 'runtime-evidence.json', {'imageId': IMAGE})
    for name in ['term-submitted', 'term-returned']:
        put(directory, name + '.json', {'containerId': CID})
    put(directory, 'states.jsonl', json.dumps({'containerId': CID, 'imageId': IMAGE,
        'state': {'Running': False, 'Pid': 0, 'OOMKilled': False, 'ExitCode': 143}}) + '\n')
    put(directory, 'result.json', {'accepted': True, 'containerId': CID, 'exitCode': 143,
        'shutdownConfirmedBeforeCleanup': True})
    put(directory, 'observation-ended.json', {'accepted': True, 'containerId': CID})


def final_evidence(directory):
    put(directory, 'cleanup.json', {'ownedContainerRemoved': True, 'containerId': CID})
    put(directory, 'safety-cleanup.json', {'authorizationReceived': True, 'safetyFallback': False,
        'runtimeCleanupCompleted': True, 'callbackCleanupCompleted': True})
    put(directory, 'log-evidence.json', {'complete': True, 'healthyAfterCleanupAndClose': True,
        'naturalCompletion': True, 'transportFailed': False, 'cancelled': False})


class FakeChild:
    def __init__(self, system, argv, intent_path):
        self.system, self.argv, self.pid, self.returncode = system, argv, 123456 + len(system.children), None
        self.intent_path = intent_path
        self.started = False
        self.preflight = '--verify-evidence' in argv
        self.identity = {'pid': self.pid, 'pgid': self.pid, 'kind': 'fake-start', 'start': self.pid}

    def start_if_authorized(self):
        kind = 'preflight' if self.preflight else 'java'
        if self.started or not (self.intent_path.parent / (kind + '-exec-authorized.json')).exists():
            return
        self.started = True
        argv, system = self.argv, self.system
        intent = probe.receipt(self.intent_path)
        probe.publish(self.intent_path.parent / (kind + '-exec-submitted.json'), {'nonce': intent['nonce']})
        if self.preflight:
            output = Path(argv[-1]); output.mkdir()
            for name in ['startup-submitted', 'ready', 'runtime-evidence']:
                put(output, name + '.json', {'serializable': True})
        else:
            properties = dict(value[len('-Dflink.kafka.lifecycle.'):].split('=', 1)
                for value in argv if value.startswith('-Dflink.kafka.lifecycle.'))
            self.output = Path(properties['output'])
            assert not self.output.exists(), 'Java owns evidence directory creation'
            assert properties['session'] == system.session and properties['networkId'] == NID
            system.containers.add(CID)
            system.now += system.live_delay
            evidence(self.output, properties['case'])
            if system.mode in ('startup-timeout', 'observation-timeout', 'interrupted', 'missing-handshake'):
                (self.output / 'observation-ended.json').unlink()
            if system.mode == 'startup-timeout':
                (self.output / 'term-submitted.json').unlink()
            if system.mode == 'malformed-boundary':
                put(self.output, 'observation-ended.json', '{incomplete')
            if system.mode == 'failed-observation':
                put(self.output, 'observation-ended.json', {'accepted': False, 'containerId': CID})
            if system.mode == 'missing-handshake':
                self.returncode = 1

    def poll(self):
        self.start_if_authorized()
        return self.returncode

    def wait(self, timeout):
        self.start_if_authorized()
        if self.returncode is not None:
            return self.returncode
        if self.preflight:
            self.system.now += self.system.preflight_delay
            if self.system.mode == 'preflight-timeout':
                self.system.now += timeout
                raise subprocess.TimeoutExpired(self.argv, timeout)
            self.returncode = 1 if self.system.mode == 'preflight-failure' else 0
            return self.returncode
        if self.system.mode == 'cleanup-timeout':
            self.system.now += timeout
            raise subprocess.TimeoutExpired(self.argv, timeout)
        assert (self.output / 'cleanup-authorized').read_text().strip() == self.system.session
        assert (self.output.parent / 'boundary-observed.json').exists()
        self.system.handshake_observed_before_authorization = True
        self.system.containers.discard(CID)
        final_evidence(self.output)
        if self.system.mode == 'partial-success':
            (self.output / 'log-evidence.json').unlink()
        elif self.system.mode == 'bad-log-marker':
            value = json.loads((self.output / 'log-evidence.json').read_text()); value['transportFailed'] = True
            put(self.output, 'log-evidence.json', value)
        elif self.system.mode == 'malformed-cleanup':
            put(self.output, 'cleanup.json', '{')
        elif self.system.mode == 'partial-publication':
            put(self.output, 'log-evidence.json.partial', '{')
        elif self.system.mode == 'missing-required':
            (self.output / 'namespace-pid-one.txt').unlink()
        elif self.system.mode == 'safety-fallback':
            put(self.output, 'safety-cleanup.json', {'authorizationReceived': False, 'safetyFallback': True,
                'runtimeCleanupCompleted': True, 'callbackCleanupCompleted': True})
        self.returncode = 1 if self.system.mode == 'java-failure' else 0
        return self.returncode


class FakeSystem:
    def __init__(self, mode='success'):
        self.mode, self.now, self.live_delay = mode, 0.0, 0
        self.preflight_delay = 0
        self.owner_alive = True
        self.identity_overrides = {}
        self.orphan_groups = {}
        self.session = None
        self.containers, self.networks = {HISTORIC_CONTAINER}, {HISTORIC_NETWORK}
        self.calls, self.children, self.signals, self.deleted = [], [], [], []
        self.handshake_observed_before_authorization = False
        self.interrupted = False
        self.patch_bytes = b''
        self.untracked_bytes = b''

    def monotonic(self): return self.now

    def sleep(self, seconds):
        if self.mode == 'interrupted' and not self.interrupted:
            self.interrupted = True
            raise KeyboardInterrupt('synthetic interruption')
        self.now += seconds

    def popen(self, argv, **kwargs):
        assert kwargs['start_new_session'] is True
        assert kwargs['stdout'] is not subprocess.PIPE and kwargs['stderr'] is not subprocess.PIPE
        assert argv[-2] == '--_child-start'
        intent_path = Path(argv[-1]); intent = probe.receipt(intent_path)
        child = FakeChild(self, intent['argv'], intent_path)
        self.children.append(child)
        probe.publish(intent_path.parent / (intent['kind'] + '-process.json'),
            {'nonce': intent['nonce'], 'session': intent['session'], 'identity': child.identity})
        return child

    def process_identity(self, pid, timeout=2):
        if pid in self.identity_overrides: return self.identity_overrides[pid]
        if pid == os.getpid():
            return {'pid': pid, 'pgid': 1, 'kind': 'fake-start', 'start': 99} if self.owner_alive else None
        for child in self.children:
            if child.pid == pid and child.returncode is None: return child.identity
        return None

    def group_members(self, pgid, timeout=2):
        return self.orphan_groups.get(pgid, {c.pid for c in self.children if c.pid == pgid and c.returncode is None})

    def signal_identity(self, identity, signum, timeout=2):
        assert self.process_identity(identity['pid']) == identity
        child = next(child for child in self.children if child.pid == identity['pid'])
        self.signal_group(child, signum)

    def signal_group(self, child, signum):
        assert child in self.children
        self.signals.append((child.pid, signum))
        child.returncode = -signum

    def run(self, argv, **kwargs):
        self.calls.append((argv, kwargs['timeout']))
        if argv[0] == 'git':
            tail = argv[3:]
            output = ('1' * 40) if tail in (['rev-parse', 'HEAD'], ['rev-parse', 'HEAD^{tree}']) else ''
            if not kwargs['text']:
                output = self.patch_bytes if tail[0] == 'diff' else self.untracked_bytes
            return subprocess.CompletedProcess(argv, 0, output, '')
        assert argv[:3] == ['docker', '--host', 'unix:///fake.sock']
        require = kwargs['env']
        assert require['TESTCONTAINERS_RYUK_DISABLED'] == 'true'
        assert require['DOCKER_HOST'] == 'unix:///fake.sock'
        args = argv[3:]; output = ''; error = ''; code = 0
        labelled = '--filter' in args
        if args[:2] == ['image', 'inspect']:
            value = {'Id': IMAGE if self.mode != 'wrong-image' else 'sha256:' + '0' * 64,
                'Os': 'linux', 'Architecture': 'amd64', 'RepoDigests': [DIGEST]}
            if self.mode == 'variant-present': value['Variant'] = 'v8'
            output = json.dumps(value)
        elif args[:2] == ['ps', '-q']:
            output = HISTORIC_CONTAINER if self.mode == 'competing-workload' else ''
        elif args[:2] == ['ps', '-aq']:
            output = '\n'.join(sorted(self.containers - {HISTORIC_CONTAINER} if labelled else self.containers))
        elif args[:2] == ['network', 'ls']:
            output = '\n'.join(sorted(self.networks - {HISTORIC_NETWORK} if labelled else self.networks))
        elif args[:2] == ['network', 'create']:
            self.session = args[args.index('--label') + 1].split('=', 1)[1]
            self.networks.add(NID)
            if self.mode == 'ambiguous-network':
                raise subprocess.TimeoutExpired(argv, kwargs['timeout'])
            output = 'not-an-id' if self.mode == 'malformed-network-id' else NID
        elif args[:2] == ['network', 'inspect']:
            nid = args[2]
            if nid not in self.networks:
                code, error = 1, 'network ' + nid + ' not found'
            else:
                output = json.dumps({'Id': nid, 'Session': self.session, 'Case': 'default-apache',
                    'Containers': {CID: {}} if CID in self.containers else {}})
        elif args[:3] == ['inspect', '--type', 'container']:
            cid = args[3]
            if cid not in self.containers:
                code, error = 1, 'No such container: ' + cid
            else:
                output = json.dumps({'Id': cid, 'Image': IMAGE, 'Session': self.session if self.mode != 'foreign-label' else 'foreign',
                    'Case': 'default-apache', 'Networks': {'owned': {'NetworkID': NID if self.mode != 'foreign-network' else HISTORIC_NETWORK,
                        'Aliases': ['kafka-lifecycle']}}, 'NetworkMode': NID, 'Binds': [], 'Running': False, 'Status': 'exited'})
        elif args[:2] == ['rm', '--force']:
            cid = args[-1]
            assert cid != HISTORIC_CONTAINER
            self.deleted.append(cid); self.containers.discard(cid)
        elif args[:2] == ['network', 'rm']:
            nid = args[-1]
            assert nid != HISTORIC_NETWORK
            if self.mode == 'cleanup-failure':
                code, error = 1, 'synthetic network removal failed'
            else:
                self.deleted.append(nid); self.networks.discard(nid)
        else:
            raise AssertionError('Unexpected command ' + repr(argv))
        return subprocess.CompletedProcess(argv, code, output, error)


class LifecycleSupervisorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.java = self.root / 'java'; self.java.write_bytes(b'fake-java')
        self.classes = self.root / 'classes'; self.classes.mkdir()
        (self.classes / 'Probe.class').write_bytes(b'fake-class')
        self.args = argparse.Namespace(case='default-apache', java=self.java, classpath=str(self.classes),
            image='example/kafka:tag', image_id=IMAGE, digest=DIGEST, platform='linux/amd64',
            source_root=self.root, output=self.root / 'output', docker='docker', docker_host='unix:///fake.sock')

    def run_probe(self, mode='success', overall=None, live_delay=0, preflight_delay=0):
        system = FakeSystem(mode); system.live_delay = live_delay; system.preflight_delay = preflight_delay
        supervisor = probe.Supervisor(self.args, system)
        if overall is not None: supervisor.overall = overall
        with patch('subprocess.run', side_effect=AssertionError('Real command forbidden')), \
             patch('subprocess.Popen', side_effect=AssertionError('Real child forbidden')), \
             patch.object(probe, 'resolve_classpath', return_value={'classpath': self.args.classpath}), \
             patch.object(probe, 'validate_output', return_value={'validated': True}):
            result = supervisor.execute()
        return result, system

    def test_argument_array_has_exact_eight_properties_and_opt_in(self):
        for case in probe.CASES:
            self.args.case = case
            argv = probe.java_command(self.args, 'unique-session', NID, self.root / 'evidence with spaces')
            self.assertEqual(str(self.java), argv[0])
            props = [item for item in argv if item.startswith('-Dflink.kafka.lifecycle.')]
            self.assertEqual(8, len(props))
            self.assertIn('-Dflink.kafka.lifecycle=true', argv)
            self.assertIn('-Dflink.kafka.lifecycle.output=' + str(self.root / 'evidence with spaces'), argv)

    def test_success_requires_observed_boundary_then_authorization_and_independent_absence(self):
        result, system = self.run_probe()
        self.assertTrue(result['accepted'])
        self.assertTrue(system.handshake_observed_before_authorization)
        self.assertEqual(CID, json.loads((self.args.output / 'ready-observed.json').read_text())['containerId'])
        self.assertEqual({HISTORIC_CONTAINER}, system.containers)
        self.assertEqual({HISTORIC_NETWORK}, system.networks)
        self.assertEqual([NID], system.deleted)
        self.assertTrue(all(timeout <= 10 for _, timeout in system.calls))

    def test_preflight_uses_exact_java_classpath_before_any_live_resources(self):
        result, system = self.run_probe()
        self.assertTrue(result['accepted'])
        argv = system.children[0].argv
        self.assertEqual(str(self.java), argv[0])
        self.assertIn('-Dflink.kafka.lifecycle=false', argv)
        self.assertEqual(self.args.classpath, argv[argv.index('-cp') + 1])
        self.assertIn('--verify-evidence', argv)
        self.assertEqual(2, len(system.children))

    def test_existing_output_or_authorization_is_never_reused(self):
        self.args.output.mkdir(); put(self.args.output, 'cleanup-authorized', 'stale')
        with self.assertRaises(FileExistsError): self.run_probe()
        self.assertEqual('stale', (self.args.output / 'cleanup-authorized').read_text())

    def test_preflight_failure_creates_no_network_or_live_process(self):
        result, system = self.run_probe('preflight-failure')
        self.assertFalse(result['accepted'])
        self.assertEqual(1, len(system.children))
        self.assertFalse(any('create' in argv for argv, _ in system.calls))

    def test_wrong_pin_is_rejected_before_creation(self):
        result, system = self.run_probe('wrong-image')
        self.assertFalse(result['accepted'])
        self.assertFalse(any('create' in argv for argv, _ in system.calls))

    def test_optional_variant_lookup_and_full_present_variant_matching(self):
        self.assertIn('{{json (index . "Variant")}}', probe.IMAGE_FORMAT)
        self.args.platform = 'linux/amd64/v8'
        result, _ = self.run_probe('variant-present')
        self.assertTrue(result['accepted'])
        self.args.output = self.root / 'variant-mismatch'
        self.args.platform = 'linux/amd64'
        result, system = self.run_probe('variant-present')
        self.assertFalse(result['accepted'])
        self.assertFalse(any('create' in argv for argv, _ in system.calls))

    def test_competing_running_container_prevents_resource_creation(self):
        result, system = self.run_probe('competing-workload')
        self.assertFalse(result['accepted'])
        self.assertFalse(any('create' in argv for argv, _ in system.calls))

    def test_startup_timeout_leaves_a_fresh_cleanup_budget(self):
        result, system = self.run_probe('startup-timeout')
        self.assertFalse(result['accepted'])
        self.assertEqual('startup', result['firstFailure']['phase'])
        self.assertTrue(result['cleanup']['passed'])
        self.assertIn(CID, result['cleanup']['forcedContainerRemovals'])
        self.assertGreaterEqual(result['elapsedSeconds'], 120)

    def test_observation_timeout_and_cleanup_death_do_not_pass_stop(self):
        result, system = self.run_probe('observation-timeout', live_delay=20)
        self.assertFalse(result['accepted'])
        self.assertEqual('observation', result['firstFailure']['phase'])
        self.assertFalse(result['shutdownObservationAccepted'])
        self.assertGreaterEqual(result['elapsedSeconds'], 140)
        self.assertTrue(result['cleanup']['passed'])

    def test_cleanup_timeout_reaps_owned_group_and_preserves_failure(self):
        result, system = self.run_probe('cleanup-timeout')
        self.assertFalse(result['accepted'])
        self.assertEqual('TimeoutExpired', result['firstFailure']['type'])
        self.assertTrue(result['cleanup']['childReaped'])
        self.assertEqual([(123457, signal.SIGTERM)], system.signals)

    def test_overall_limit_reserves_cleanup_when_observation_is_shortened(self):
        result, system = self.run_probe('observation-timeout', overall=180, live_delay=119)
        self.assertFalse(result['accepted'])
        self.assertLessEqual(result['elapsedSeconds'], 180)
        self.assertEqual(120, result['phases'][-1]['elapsedSeconds'])
        self.assertTrue(result['cleanup']['passed'])

    def test_preflight_timeout_reaps_only_that_child(self):
        result, system = self.run_probe('preflight-timeout')
        self.assertFalse(result['accepted'])
        self.assertEqual(1, len(system.children))
        self.assertEqual([(123456, signal.SIGTERM)], system.signals)
        self.assertEqual([], system.deleted)

    def test_interruption_keeps_failure_and_cleans_only_owned_resources(self):
        result, system = self.run_probe('interrupted')
        self.assertFalse(result['accepted'])
        self.assertEqual('KeyboardInterrupt', result['firstFailure']['type'])
        self.assertTrue(result['cleanup']['passed'])
        self.assertEqual({CID, NID}, set(system.deleted))

    def test_ambiguous_network_creation_is_reconciled_by_exact_labels(self):
        result, system = self.run_probe('ambiguous-network')
        self.assertFalse(result['accepted'])
        self.assertEqual(1, len(system.children))
        self.assertEqual([NID], system.deleted)
        self.assertTrue(result['cleanup']['passed'])

    def test_malformed_created_id_is_not_used_for_destructive_commands(self):
        result, system = self.run_probe('malformed-network-id')
        self.assertFalse(result['accepted'])
        self.assertEqual([NID], system.deleted)
        self.assertTrue(all('not-an-id' not in argv for argv, _ in system.calls))

    def test_successful_observation_followed_by_cleanup_failure_is_failure(self):
        result, system = self.run_probe('cleanup-failure')
        self.assertTrue(result['shutdownObservationAccepted'])
        self.assertFalse(result['accepted'])
        self.assertFalse(result['cleanup']['passed'])
        self.assertIn(NID, system.networks)
        self.assertIsNotNone(result['firstFailure'])
        self.assertEqual('cleanup', result['firstFailure']['phase'])

    def test_mismatched_labels_are_never_cleanup_authority(self):
        result, system = self.run_probe('foreign-label')
        self.assertFalse(result['accepted'])
        self.assertNotIn(CID, system.deleted)
        self.assertFalse((self.args.output / 'evidence/cleanup-authorized').exists())

    def test_foreign_network_membership_is_never_cleanup_authority(self):
        result, system = self.run_probe('foreign-network')
        self.assertFalse(result['accepted'])
        self.assertNotIn(CID, system.deleted)

    def test_missing_handshake_cannot_pass_even_if_result_exists(self):
        result, system = self.run_probe('missing-handshake')
        self.assertFalse(result['accepted'])
        self.assertFalse((self.args.output / 'evidence/cleanup-authorized').exists())

    def test_failed_observation_still_gets_bounded_cleanup_authorization(self):
        result, system = self.run_probe('failed-observation')
        self.assertFalse(result['accepted'])
        self.assertTrue(system.handshake_observed_before_authorization)
        self.assertTrue(result['cleanup']['passed'])

    def test_malformed_boundary_cannot_authorize_cleanup(self):
        result, system = self.run_probe('malformed-boundary')
        self.assertFalse(result['accepted'])
        self.assertFalse((self.args.output / 'evidence/cleanup-authorized').exists())

    def test_partial_or_malformed_success_is_rejected(self):
        for mode in ['partial-success', 'bad-log-marker', 'malformed-cleanup', 'partial-publication', 'missing-required', 'java-failure', 'safety-fallback']:
            with self.subTest(mode=mode):
                self.args.output = self.root / mode
                result, _ = self.run_probe(mode)
                self.assertFalse(result['accepted'])

    def test_receipt_parser_rejects_duplicates_trailing_json_and_nonfinite_values(self):
        for value in ['{"accepted":true,"accepted":false}', '{} {}', '{"x":NaN}', '[]']:
            with self.assertRaises(ValueError): probe.object_json(value)

    def test_build_identity_detects_changed_or_added_compiled_bytes(self):
        before = probe.build_files(self.args.classpath, self.java)
        (self.classes / 'Additional.class').write_bytes(b'new')
        self.assertNotEqual(before, probe.build_files(self.args.classpath, self.java))

    def test_source_identity_preserves_exact_patch_bytes_and_nul_delimited_names(self):
        self.args.output.mkdir()
        name = ' trailing-space.java '
        (self.root / name).write_bytes(b'source bytes')
        system = FakeSystem(); system.untracked_bytes = name.encode() + b'\0'
        system.patch_bytes = b'diff --git a/x b/x\n+changed  \r\n'
        supervisor = probe.Supervisor(self.args, system)
        before = supervisor.source_build_identity()
        self.assertEqual(hashlib.sha256(system.patch_bytes).hexdigest(), before['source']['diffSha256'])
        self.assertIn(name, before['source']['untrackedFiles'])
        system.patch_bytes = system.patch_bytes.rstrip() + b'\n'
        self.assertNotEqual(before['source']['diffSha256'], supervisor.source_build_identity()['source']['diffSha256'])

    def test_reap_requires_exact_owned_process_group_and_escalates_only_that_group(self):
        process = unittest.mock.Mock(pid=5151)
        process.poll.return_value = None
        with patch('os.getpgid', return_value=999), patch('os.killpg') as kill:
            with self.assertRaises(ValueError): probe.System.signal_group(process, signal.SIGTERM)
            kill.assert_not_called()

    def test_hash_blocks_and_directory_walk_consume_the_same_deadline(self):
        def expired(): raise TimeoutError('same phase deadline')
        with self.assertRaises(TimeoutError): probe.digest(self.java, expired)
        with self.assertRaises(TimeoutError): probe.build_files(self.args.classpath, self.java, expired)
        checks = []
        probe.build_files(self.args.classpath, self.java, lambda: checks.append(True))
        self.assertGreater(len(checks), 5)

    def test_final_evidence_hashing_uses_the_existing_deadline(self):
        directory = self.root / 'evidence'
        evidence(directory, 'default-apache'); final_evidence(directory)
        hashing = False
        original = probe.digest
        def deadline():
            if hashing: raise TimeoutError('existing cleanup deadline')
        def hash_file(path, check=lambda: None):
            nonlocal hashing
            hashing = True
            return original(path, check)
        with patch.object(probe, 'digest', side_effect=hash_file):
            with self.assertRaisesRegex(TimeoutError, 'existing cleanup deadline'):
                probe.required_evidence(directory, 'default-apache', CID, IMAGE, deadline)

    def test_disappeared_group_is_benign_only_when_owned_child_exit_is_confirmed(self):
        process = unittest.mock.Mock(pid=5151)
        process.poll.side_effect = [None, 0]
        with patch('os.getpgid', side_effect=ProcessLookupError), patch('os.killpg') as kill:
            probe.System.signal_group(process, signal.SIGTERM)
            kill.assert_not_called()
        process.poll.side_effect = [None, None]
        with patch('os.getpgid', side_effect=ProcessLookupError):
            with self.assertRaises(ProcessLookupError): probe.System.signal_group(process, signal.SIGTERM)

    def test_preflight_and_lifecycle_exit_codes_remain_distinct(self):
        result, _ = self.run_probe('preflight-failure')
        self.assertEqual(1, result['preflightExitCode'])
        self.assertIsNone(result['lifecycleExitCode'])
        self.assertIsNone(result['wrapperExitCodes']['java'])

    def test_preflight_does_not_consume_live_startup_budget(self):
        result, _ = self.run_probe('startup-timeout', preflight_delay=25)
        self.assertEqual({'phase': 'startup', 'elapsedSeconds': 25}, result['phases'][1])
        self.assertGreaterEqual(result['elapsedSeconds'], 145)
        self.assertTrue(result['cleanup']['passed'])

    def test_output_policy_rejection_precedes_output_and_process_creation(self):
        system = FakeSystem()
        with patch.object(probe, 'resolve_classpath', return_value={'classpath': self.args.classpath}), \
             patch.object(probe, 'validate_output', side_effect=ValueError('unsafe output')):
            with self.assertRaisesRegex(ValueError, 'unsafe output'):
                probe.Supervisor(self.args, system).execute()
        self.assertFalse(self.args.output.exists())
        self.assertEqual([], system.calls)
        self.assertEqual([], system.children)

    def test_report_normalized_classpath_is_used_for_actual_preflight(self):
        self.args.surefire_report = self.root / 'report.xml'
        self.args.classpath = None
        system = FakeSystem()
        with patch.object(probe, 'resolve_classpath', return_value={'classpath': str(self.classes)}) as resolve, \
             patch.object(probe, 'validate_output', return_value={'validated': True}):
            result = probe.Supervisor(self.args, system).execute()
        self.assertTrue(result['accepted'])
        self.assertEqual(self.args.surefire_report, resolve.call_args.kwargs['surefire_report'])
        self.assertIn(str(self.classes), system.children[0].argv)

    def test_sighup_uses_same_interruption_and_repeated_signal_suppression(self):
        handlers = {}
        def register(signum, handler):
            handlers[signum] = handler
            return signal.SIG_DFL
        def interrupted_reconcile(_):
            with self.assertRaisesRegex(InterruptedError, 'Supervisor signal'):
                handlers[signal.SIGHUP](signal.SIGHUP, None)
            self.assertTrue(all(handlers[s] == signal.SIG_IGN for s in
                (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)))
            return {'reconciled': False}
        with patch.object(signal, 'signal', side_effect=register), \
             patch.object(probe, 'reconcile', side_effect=interrupted_reconcile), patch('builtins.print'):
            self.assertEqual(1, probe.main(['--reconcile', str(self.root)]))

    def test_reconciliation_is_idempotent_and_preserves_original_verdict(self):
        _, system = self.run_probe('cleanup-failure')
        original = (self.args.output / 'completed.json').read_bytes()
        system.owner_alive = False; system.mode = 'success'
        first = probe.reconcile(self.args.output, system)
        second = probe.reconcile(self.args.output, system)
        self.assertTrue(first['reconciled'], first)
        self.assertTrue(second['reconciled'], second)
        self.assertNotEqual(first['receipt'], second['receipt'])
        self.assertEqual(original, (self.args.output / 'completed.json').read_bytes())
        self.assertEqual({HISTORIC_NETWORK}, system.networks)

    def test_reconciliation_refuses_active_supervisor_without_docker(self):
        _, system = self.run_probe('cleanup-failure')
        calls = len(system.calls)
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertEqual(calls, len(system.calls))
        self.assertIn(NID, system.networks)

    def test_reconciliation_refuses_lifetime_lock_even_with_absent_pid(self):
        _, system = self.run_probe()
        system.owner_alive = False
        calls = len(system.calls)
        with (self.args.output / 'supervisor.lock').open() as lock:
            probe.fcntl.flock(lock, probe.fcntl.LOCK_EX | probe.fcntl.LOCK_NB)
            result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertEqual(calls, len(system.calls))

    def test_reconciliation_refuses_reused_child_pid_and_orphan_descendants(self):
        _, system = self.run_probe('cleanup-failure')
        system.owner_alive = False
        child = system.children[-1]
        system.identity_overrides[child.pid] = dict(child.identity, start=999)
        calls = len(system.calls)
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertEqual([], system.signals)
        self.assertEqual(calls, len(system.calls))
        system.identity_overrides.clear()
        system.orphan_groups[child.pid] = {987654}
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertEqual(calls, len(system.calls))

    def test_reconciliation_refuses_ambiguous_bootstrap_creation(self):
        _, system = self.run_probe('cleanup-failure')
        system.owner_alive = False
        (self.args.output / 'java-process.json').unlink()
        calls = len(system.calls)
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertEqual(calls, len(system.calls))

    def test_reconciliation_reaps_identity_proven_child_before_any_docker(self):
        _, system = self.run_probe('cleanup-failure')
        system.owner_alive = False; system.mode = 'success'
        child = system.children[-1]; child.returncode = None
        original_run = system.run
        def after_reap(argv, **kwargs):
            self.assertIsNotNone(child.returncode)
            return original_run(argv, **kwargs)
        system.run = after_reap
        result = probe.reconcile(self.args.output, system)
        self.assertTrue(result['reconciled'], result)
        self.assertEqual([(child.pid, signal.SIGTERM)], system.signals)

    def test_reconciliation_refuses_foreign_labels_and_legacy_evidence(self):
        _, system = self.run_probe('foreign-label')
        system.owner_alive = False
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertNotIn(CID, system.deleted)
        original = probe.receipt(self.args.output / 'invocation.json')
        original.pop('schemaVersion')
        put(self.args.output, 'invocation.json', original)
        calls = len(system.calls)
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertEqual(calls, len(system.calls))

    def test_bootstrap_requires_durable_identity_and_authorization_before_exec(self):
        identity = {'pid': os.getpid(), 'pgid': os.getpid(), 'start': [5, 1], 'kind': 'fake-start'}
        owner = {'pid': 888, 'pgid': 888, 'start': [4, 1], 'kind': 'fake-start'}
        intent = {'session': 'session', 'kind': 'java', 'nonce': 'a' * 32,
            'supervisor': owner, 'argv': [str(self.java), '-version']}
        path = self.root / 'java-launch-intent.json'; probe.write(path, intent)
        def authorize(_):
            self.assertEqual(identity, probe.receipt(self.root / 'java-process.json')['identity'])
            self.assertFalse((self.root / 'java-exec-submitted.json').exists())
            probe.publish(self.root / 'java-exec-authorized.json', {'nonce': intent['nonce']})
        def exec_check(java, argv, environment):
            self.assertTrue((self.root / 'java-exec-submitted.json').exists())
            self.assertEqual(intent['argv'], argv)
        with patch.object(probe.System, 'process_identity', side_effect=lambda pid: identity if pid == os.getpid() else owner), \
             patch.object(probe.time, 'sleep', side_effect=authorize), patch.object(probe.os, 'execve', side_effect=exec_check):
            probe.child_start(path)

    def test_bootstrap_without_authorization_cannot_exec_after_supervisor_dies(self):
        identity = {'pid': os.getpid(), 'pgid': os.getpid(), 'start': [5, 1], 'kind': 'fake-start'}
        intent = {'session': 'session', 'kind': 'java', 'nonce': 'a' * 32,
            'supervisor': {'pid': 888}, 'argv': [str(self.java), '-version']}
        path = self.root / 'java-launch-intent.json'; probe.write(path, intent)
        with patch.object(probe.System, 'process_identity', side_effect=lambda pid: identity if pid == os.getpid() else None), \
             patch.object(probe.os, 'execve') as execute:
            with self.assertRaisesRegex(ValueError, 'Supervisor disappeared'):
                probe.child_start(path)
            execute.assert_not_called()
        self.assertTrue((self.root / 'java-process.json').exists())

    def test_linux_missing_boot_identity_is_not_process_absence(self):
        fields = ['0'] * 20; fields[2] = '123'; fields[19] = '123456'
        with patch.object(probe.sys, 'platform', 'linux'), \
             patch.object(Path, 'read_text', side_effect=['123 (java) ' + ' '.join(fields), FileNotFoundError()]):
            with self.assertRaises(FileNotFoundError): probe.System.process_identity(123)
        with patch.object(probe.sys, 'platform', 'linux'), patch.object(Path, 'read_text', side_effect=FileNotFoundError()):
            self.assertIsNone(probe.System.process_identity(123))

    def test_linux_kernel_identity_rejects_malformed_boot_start_or_group(self):
        for group, start, boot in [('123', '4', ''), ('123', '4', '-' * 36), ('0', '4', 'a' * 8 + '-1234-1234-1234-' + 'a' * 12),
                ('123', '0', 'a' * 8 + '-1234-1234-1234-' + 'a' * 12)]:
            fields = ['0'] * 20; fields[2] = group; fields[19] = start
            with self.subTest(group=group, start=start, boot=boot), patch.object(probe.sys, 'platform', 'linux'), \
                 patch.object(Path, 'read_text', side_effect=['123 (java) ' + ' '.join(fields), boot]):
                with self.assertRaises(ValueError): probe.System.process_identity(123)

    def test_launch_states_do_not_attribute_bootstrap_or_unconfirmed_exit_to_java(self):
        supervisor = probe.Supervisor(self.args, FakeSystem())
        self.args.output.mkdir()
        self.assertEqual('not-submitted', supervisor.launch_state('java'))
        supervisor.processes['java'] = unittest.mock.Mock()
        self.assertEqual('bootstrap-only', supervisor.launch_state('java'))
        put(self.args.output, 'java-exec-submitted.json', {'nonce': 'a' * 32})
        self.assertEqual('exec-submitted-unconfirmed', supervisor.launch_state('java'))
        put(self.args.output, 'java-exec-failed.json', {'failureType': 'FileNotFoundError'})
        self.assertEqual('exec-failed', supervisor.launch_state('java'))
        self.args.output.joinpath('java-exec-failed.json').unlink()
        supervisor.evidence.mkdir(); put(supervisor.evidence, 'startup-submitted.json', {'started': True})
        self.assertEqual('fixture-confirmed', supervisor.launch_state('java'))

    def test_snapshot_input_mutation_is_rechecked_at_identity_boundary(self):
        self.args.output.mkdir()
        supervisor = probe.Supervisor(self.args, FakeSystem())
        supervisor.input_classpath = str(self.classes)
        supervisor.inputs = {'classpath': str(self.classes), 'sourceInputs': {'snapshot.java': 'before'}}
        with patch.object(probe, 'resolve_classpath', return_value={'classpath': str(self.classes),
                'sourceInputs': {'snapshot.java': 'after'}}):
            with self.assertRaisesRegex(ValueError, 'source snapshot'):
                supervisor.source_build_identity()

    def test_reconciliation_rejects_linked_run_ancestors_and_output_parent_before_writing(self):
        _, system = self.run_probe()
        system.owner_alive = False
        link = self.root / 'alias'; link.symlink_to(self.args.output, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'Symlink'):
            probe.reconcile(link, system)
        destination = self.root / 'destination'; destination.mkdir()
        (self.args.output / 'reconciliations').symlink_to(destination, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'Symlink'):
            probe.reconcile(self.args.output, system)
        self.assertEqual([], list(destination.iterdir()))

    def test_reconciliation_rejects_foreign_intent_and_contradictory_resource_evidence(self):
        _, system = self.run_probe('cleanup-failure')
        system.owner_alive = False
        creation = self.args.output / 'resource-creation-submitted.json'
        put(self.args.output, creation.name, {'session': 'foreign'})
        calls = len(system.calls)
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertEqual(calls, len(system.calls))
        creation.unlink()
        result = probe.reconcile(self.args.output, system)
        self.assertFalse(result['reconciled'])
        self.assertNotIn('noResourceCreationSubmitted', result)
        self.assertEqual(calls, len(system.calls))


if __name__ == '__main__': unittest.main()
