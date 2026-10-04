"""Copied runtime subjects preserve fault contracts and retain every selected pin."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools'))
import pr_gate
import subject_catalog as subjects
import test_pr_gate as fixtures


class RuntimeSubstitutionTest(unittest.TestCase):
    def setUp(self):
        self.canonical = (ROOT / 'scenarios/pooling/pool-reuse-inflight-kill-v1.yaml').read_text()
        self.parser = argparse.ArgumentParser()
        subjects.add_runtime_arguments(self.parser)

    def options(self, *arguments):
        return subjects.runtime_substitutions(self.parser.parse_args(arguments), ROOT)

    def test_complete_copy_keeps_every_fault_timing_and_expectation_input(self):
        common, candidate = self.options(
            '--flink-image', 'local/flink:private', '--flink-image-id', 'sha256:' + 'a' * 64,
            '--flink-line', '2.9', '--runtime-jar', '/opt/flink/lib/flink-dist-private.jar=' + 'b' * 64,
            '--kafka-image', 'local/kafka:private', '--kafka-image-id', 'sha256:' + 'c' * 64,
            '--kafka-launch', 'generic-kraft', '--broker-config', 'transaction.two.phase.commit.enable=true',
            '--flink-config', 'execution.checkpointing.unaligned.enabled=false',
            '--flink-config', 'pipeline.name=private subject',
            '--candidate-flink-config', 'execution.checkpointing.unaligned.enabled=true',
            '--transaction-version', 'broker-default', '--transaction-id-naming-strategy', 'connector-default')
        baseline = subjects.with_runtime_substitutions(self.canonical, common)
        result = subjects.with_runtime_substitutions(self.canonical, common, candidate)
        self.assertEqual(self.canonical.split('\nphases:\n')[1], result.split('\nphases:\n')[1])
        self.assertIn('program_args: [--processingDelayMs, "5", --snapshotAsyncDelayMs, "8000"]', result)
        self.assertIn('transaction_version: "broker-default"', result)
        self.assertIn('transaction_id_naming_strategy: connector-default', result)
        self.assertIn('runtime_jar: {"container_path": "/opt/flink/lib/flink-dist-private.jar", "sha256": "' + 'b' * 64, result)
        self.assertIn('launch: {"type": "generic-kraft"}', result)
        self.assertIn('"transaction.two.phase.commit.enable": "true"', result)
        self.assertEqual(baseline.replace('"execution.checkpointing.unaligned.enabled": "false"',
                                  '"execution.checkpointing.unaligned.enabled": "true"'), result)

    def test_empty_configuration_values_remain_explicit_strings(self):
        common, _ = self.options('--flink-config', 'pipeline.jars=')
        self.assertEqual({'pipeline.jars': ''}, common['flinkConfig'])
        self.assertIn('config: {"pipeline.jars": ""}',
                      subjects.with_runtime_substitutions(self.canonical, common))

    def test_omitted_options_return_identical_catalog_bytes(self):
        common, candidate = self.options()
        self.assertEqual(self.canonical, subjects.with_runtime_substitutions(self.canonical, common, candidate))

    def test_rejects_ambiguous_or_unpinned_inputs(self):
        for flags in (
            ('--flink-line', '2.9'), ('--flink-line', '02.9'),
            ('--flink-image-id', 'sha256:abc'), ('--kafka-image-id', 'latest'),
            ('--flink-image', 'image with spaces'), ('--kafka-image', ''),
            ('--runtime-jar', '/opt/flink/lib/flink-dist-private.jar=BAD'),
            ('--runtime-jar', '/opt/flink/../dist.jar=' + 'b' * 64),
            ('--runtime-jar', 'relative.jar=' + 'b' * 64),
            ('--runtime-jar', '/opt/flink/lib/not-a-runtime.jar=' + 'b' * 64),
            ('--flink-config', 'bad'), ('--flink-config', 'key=bad\x1bvalue'),
            ('--broker-config', 'key=value\nlistener=elsewhere'),
            ('--candidate-flink-config', 'key=true', '--candidate-flink-config', 'key=false'),
            ('--workload-jar', '/tmp/workload-outside-artifact-root.jar'),
        ):
            with self.subTest(flags=flags), self.assertRaises(SystemExit):
                self.options(*flags)

    def test_refuses_to_discard_existing_configuration_or_guess_unknown_shapes(self):
        common, _ = self.options('--flink-config', 'pipeline.name=custom')
        for document in (self.canonical.replace('    taskmanagers: 1', '    config: {existing: value}\n    taskmanagers: 1'),
                         self.canonical.replace('  flink:\n', '  flink: {}\n')):
            with self.assertRaises(SystemExit):
                subjects.with_runtime_substitutions(document, common)
        common, _ = self.options('--kafka-image', 'apache/kafka:4.0.0')
        with self.assertRaises(SystemExit):
            subjects.with_runtime_substitutions(self.canonical.replace('      main:', '      other: {}\n      main:'), common)

    def test_image_subject_requires_absolute_jar_and_exact_digest(self):
        descriptor, snippet = subjects.image_subject('image:/opt/flink/lib/subject.jar=' + 'd' * 64)
        self.assertEqual('image', descriptor['origin'])
        self.assertEqual('image', descriptor['dependencyMode'])
        self.assertIsNone(descriptor['runtimeDependencySha256'])
        self.assertNotIn('runtime_dependencies', snippet)
        for value in ('maven:g:a:v', 'image:relative.jar=' + 'a' * 64,
                      'image:/opt/flink/lib/../subject.jar=' + 'a' * 64,
                      'image:/opt/flink/lib/subject.jar=abc',
                      'image:/opt/flink/lib/bad:name.jar=' + 'a' * 64):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                subjects.image_subject(value)


class CustomRuntimeCommandTest(unittest.TestCase):
    setUp = fixtures.GateCommandTest.setUp
    command = fixtures.GateCommandTest.command
    raw_command = fixtures.GateCommandTest.raw_command

    def test_prepare_only_retains_full_manifest_both_hashes_and_untouched_canonical(self):
        original = (self.root / 'scenarios/bounded-eos.yaml').read_bytes()
        (self.root / 'workload.jar').write_bytes(b'custom workload fixture')
        result, output = self.command(extra=(
            '--prepare-only', '--baseline-connector-jar', 'candidate.jar', '--baseline-runtime-dir', 'runtime',
            '--flink-image', 'private/runtime:custom',
            '--flink-image-id', 'sha256:' + 'a' * 64, '--flink-line', '2.9',
            '--runtime-jar', '/opt/flink/lib/flink-dist-private.jar=' + 'b' * 64,
            '--kafka-image', 'private/broker:custom', '--kafka-image-id', 'sha256:' + 'c' * 64,
            '--kafka-launch', 'generic-kraft', '--broker-config', 'transaction.two.phase.commit.enable=true',
            '--flink-config', 'pipeline.name=shared', '--candidate-flink-config', 'pipeline.name=candidate',
            '--workload-jar', 'workload.jar', '--transaction-version', 'broker-default',
            '--transaction-id-naming-strategy', 'connector-default'))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertFalse(self.calls.exists())
        manifest = json.loads((output / 'manifest.json').read_text())
        common = manifest['plan']['runtimeSubstitutions']
        self.assertEqual('2.9', common['flinkLine'])
        self.assertEqual('sha256:' + 'a' * 64, common['flinkImageId'])
        self.assertEqual('private/broker:custom', common['kafkaImage'])
        self.assertEqual('sha256:' + 'c' * 64, common['kafkaImageId'])
        self.assertEqual({'pipeline.name': 'candidate'}, manifest['plan']['candidateFlinkConfig'])
        self.assertEqual(hashlib.sha256(b'custom workload fixture').hexdigest(), common['workloadJarSha256'])
        for side in ('baseline', 'candidate'):
            catalog = output / (side + '-catalog')
            self.assertEqual(subjects.sha256(catalog / 'bounded-eos.yaml'),
                             manifest['catalogHashes'][side]['bounded-eos']['scenarioSha256'])
            self.assertEqual((self.root / 'scenarios/bounded-eos.expected.yaml').read_bytes(),
                             (catalog / 'bounded-eos.expected.yaml').read_bytes())
            text = (catalog / 'bounded-eos.yaml').read_text()
            self.assertIn('jar: "./workload.jar"', text)
            self.assertEqual(original.decode().split('\nphases:\n')[1], text.split('\nphases:\n')[1])
        self.assertEqual(original, (self.root / 'scenarios/bounded-eos.yaml').read_bytes())

    def test_image_subject_command_needs_no_local_jar_or_runtime_directory(self):
        output = self.root / 'image-prepared'
        reference = 'image:/opt/flink/lib/subject.jar=' + 'd' * 64
        result = subprocess.run([sys.executable, '-B', 'tools/pr_gate.py', '--subject', reference,
                                 '--baseline-subject', reference, '--output', str(output),
                                 '--scenario', 'bounded-eos', '--prepare-only',
                                 '--candidate-flink-config', 'pipeline.name=candidate'],
                                cwd=self.root, env={'PATH': str(self.bin)}, capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertFalse(self.calls.exists())
        manifest = json.loads((output / 'manifest.json').read_text())
        for side in ('baseline', 'candidate'):
            self.assertEqual('image', manifest[side]['origin'])
            self.assertIn('artifact: image:/opt/flink/lib/subject.jar',
                          (output / (side + '-catalog/bounded-eos.yaml')).read_text())

    def test_custom_runtime_requires_explicit_matching_subject_modes(self):
        for extra in (('--flink-config', 'pipeline.name=test'),
                      ('--flink-image', 'private/flink:build'),
                      ('--flink-image', 'docker.io/library/flink:2.2.0', '--flink-config', 'pipeline.name=test'),
                      ('--candidate-flink-config', 'pipeline.name=test'),
                      ('--baseline-subject', 'image:/opt/flink/lib/subject.jar=' + 'd' * 64)):
            result, output = self.command(extra=extra)
            self.assertEqual(2, result.returncode, result.stderr)
            self.assertFalse(output.exists())
            self.assertFalse(self.calls.exists())
        output = self.root / 'impossible-image-pair'
        result = subprocess.run([sys.executable, '-B', 'tools/pr_gate.py',
                                 '--subject', 'image:/opt/flink/lib/subject.jar=' + 'a' * 64,
                                 '--baseline-subject', 'image:/opt/flink/lib/subject.jar=' + 'b' * 64,
                                 '--output', str(output), '--scenario', 'bounded-eos', '--prepare-only'],
                                cwd=self.root, env={'PATH': str(self.bin)}, capture_output=True, text=True)
        self.assertEqual(2, result.returncode, result.stderr)
        self.assertIn('different SHA-256 pins', result.stderr)
        self.assertFalse(output.exists())
        self.assertFalse(self.calls.exists())

    def test_differing_local_dependency_bytes_are_rejected_before_output(self):
        (self.root / 'different-runtime').mkdir()
        (self.root / 'different-runtime/dependency.jar').write_bytes(b'not the shared closure')
        result, output = self.command(extra=('--baseline-connector-jar', 'candidate.jar',
                                             '--baseline-runtime-dir', 'different-runtime',
                                             '--flink-config', 'pipeline.name=strict'))
        self.assertNotEqual(0, result.returncode)
        self.assertIn('dependency bytes must match', result.stderr)
        self.assertFalse(output.exists())
        self.assertFalse(self.calls.exists())


class ImageSubjectEvidenceTest(unittest.TestCase):
    def test_hash_and_origin_must_both_match_for_image_subject(self):
        subject, _ = subjects.image_subject('image:/opt/flink/lib/subject.jar=' + fixtures.CANDIDATE_HASH)
        result = fixtures.run_result()
        result['evidence']['subjectClasses']['processes'][0]['sources'] = {
            'KafkaSink': ['/opt/flink/lib/subject.jar'], 'KafkaSource': ['/opt/flink/lib/subject.jar']}
        result['evidence']['connectorPrimaries'] = [{
            'artifact': subject['connector'], 'origin': 'image',
            'declaredSha256': fixtures.CANDIDATE_HASH, 'observedSha256': fixtures.CANDIDATE_HASH}]
        def summarize():
            return pr_gate.summarize('bounded-eos', 'candidate', 1, result, fixtures.CANDIDATE_HASH, 0, subject=subject)
        self.assertTrue(summarize()['subjectOk'])
        primary = result['evidence']['connectorPrimaries'][0]
        primary['observedSha256'] = 'f' * 64
        self.assertFalse(summarize()['subjectOk'])
        primary['observedSha256'] = fixtures.CANDIDATE_HASH
        result['evidence']['subjectClasses']['processes'][0]['sources']['KafkaSink'].append('/elsewhere.jar')
        self.assertFalse(summarize()['subjectOk'])


if __name__ == '__main__':
    unittest.main()
