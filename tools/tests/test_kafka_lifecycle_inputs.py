"""Input validation with synthetic classes/JARs/XML and fake Git; no subprocesses."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from xml.etree import ElementTree as ET
from zipfile import ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kafka_lifecycle_inputs as inputs


class LifecycleInputsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / 'source'; self.source.mkdir()
        self.jobs = self.source / 'jobs'; self.jobs.mkdir()
        self.build = self.jobs / 'build'
        self.classes = self.build / 'testcontainers/target/classes'
        self.tests = self.build / 'testcontainers/target/test-classes'
        self.write(self.tests / inputs.LAUNCHER, b'fixture bytecode')
        self.write(self.classes / inputs.PRODUCTION, b'production bytecode')
        self.source_files = [Path('testcontainers/src/main/java') / (inputs.PACKAGE + 'VerifiedKafkaContainer.java'),
            Path('testcontainers/src/test/java') / (inputs.PACKAGE + 'KafkaGracefulStopDockerTest.java'),
            Path('testcontainers/src/test/java') / (inputs.PACKAGE + 'KafkaLifecycleInspection.java'),
            Path('pom.xml'), Path('testcontainers/pom.xml')]
        for relative in self.source_files:
            self.write(self.source / relative, b'current source')
            self.write(self.build / relative, b'current source')
        self.jars = []
        for artifact in inputs.JACKSON:
            path = self.root / (artifact + '-2.18.4.jar')
            self.jar(path, artifact)
            self.jars.append(path)
        self.paths = [self.tests, self.classes, *self.jars]
        self.report = self.build / 'testcontainers/target/surefire-reports/fixture.xml'
        self.report.parent.mkdir()
        self.git_calls = []
        self.tracked = b''
        self.ignored = True

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)

    @staticmethod
    def jar(path, artifact, version='2.18.4', metadata=True, group='com.fasterxml.jackson.core'):
        with ZipFile(path, 'w') as archive:
            archive.writestr(inputs.JACKSON[artifact], b'Jackson bytecode')
            if metadata:
                archive.writestr('META-INF/maven/com.fasterxml.jackson.core/' + artifact + '/pom.properties',
                    'groupId=' + group + '\nartifactId=' + artifact + '\nversion=' + version + '\n')

    def classpath(self):
        return os.pathsep.join(map(str, self.paths))

    def resolve(self, **kwargs):
        if not kwargs:
            kwargs['classpath'] = self.classpath()
        return inputs.resolve_classpath(self.source, **kwargs)

    def xml(self, raw=None):
        root = ET.Element('testsuite', name=inputs.SUITE, tests='2', failures='0', errors='0', skipped='1')
        properties = ET.SubElement(root, 'properties')
        for name in ('surefire.test.class.path', 'java.class.path'):
            ET.SubElement(properties, 'property', name=name, value=raw if raw is not None else self.classpath())
        # These are the literal names emitted by the pinned Surefire/JUnit provider.
        ET.SubElement(root, 'testcase', name='evidenceSerializationWorksWithoutDocker(Path)', classname=inputs.SUITE)
        ET.SubElement(ET.SubElement(root, 'testcase',
            name='productionKafkaIsPidOneAndTerminatesAfterStopBeforeCleanup', classname=inputs.SUITE), 'skipped')
        return root

    def report_input(self, root=None):
        ET.ElementTree(root if root is not None else self.xml()).write(self.report, encoding='utf-8', xml_declaration=True)
        return self.resolve(surefire_report=self.report)

    def git(self, argv, **kwargs):
        self.git_calls.append(argv)
        self.assertEqual(['git', '-C', str(self.source)], argv[:3])
        self.assertTrue(0 < kwargs['timeout'] <= 10)
        if argv[3] == 'ls-files':
            return subprocess.CompletedProcess(argv, 0, self.tracked, b'')
        self.assertEqual('check-ignore', argv[3])
        return subprocess.CompletedProcess(argv, 0 if self.ignored else 1, b'', b'')

    def policy(self, path, **kwargs):
        return inputs.validate_output(path, self.source, self.resolve(), run=self.git, **kwargs)

    def test_explicit_input_retains_order_source_binding_and_freshness_limit(self):
        result = self.resolve()
        self.assertEqual(list(map(str, self.paths)), result['entries'])
        self.assertEqual(self.classpath(), result['rawClasspath'])
        self.assertFalse(result['compilationProvenanceClaimed'])
        self.assertTrue(result['freshBuildRequired'])
        self.assertFalse(result['sourceBinding']['compilationFreshnessProven'])
        self.assertEqual(5, len(result['sourceBinding']['matchedSources']))
        self.assertEqual({'2.18.4'}, {value['version'] for value in result['jacksonProviders'].values()})

    def test_report_preserves_hash_and_raw_value_but_removes_empty_separator_entries(self):
        raw = os.pathsep + self.classpath() + os.pathsep
        result = self.report_input(self.xml(raw))
        self.assertEqual(raw, result['rawClasspath'])
        self.assertEqual(self.classpath(), result['classpath'])
        self.assertEqual(hashlib.sha256(self.report.read_bytes()).hexdigest(), result['report']['sha256'])
        self.assertEqual(inputs.SERIALIZATION_TEST, result['report']['executedSerializationTest'])

    def test_explicit_input_rejects_empty_entries(self):
        for value in ('', os.pathsep + self.classpath(), self.classpath() + os.pathsep):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.resolve(classpath=value)

    def test_rejects_duplicate_classpath_entries(self):
        self.paths.append(self.tests)
        with self.assertRaisesRegex(ValueError, 'Duplicate normalized'):
            self.resolve()

    def test_rejects_missing_relative_and_parent_traversal_paths(self):
        for path in ('relative.jar', str(self.root / 'missing.jar'), str(self.classes / '..' / 'classes')):
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.resolve(classpath=self.classpath() + os.pathsep + path)

    def test_rejects_symlinked_entry_and_ancestor(self):
        link = self.root / 'linked'; link.symlink_to(self.build, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'Symlink'):
            self.resolve(classpath=str(link / 'testcontainers/target/test-classes') + os.pathsep + self.classpath())

    def test_missing_and_duplicate_launcher_providers_are_rejected(self):
        fixture = self.tests / inputs.LAUNCHER
        fixture.unlink()
        with self.assertRaisesRegex(ValueError, 'Missing or duplicate'):
            self.resolve()
        self.write(fixture, b'fixture')
        duplicate = self.root / 'duplicate'; self.write(duplicate / inputs.LAUNCHER, b'fixture')
        self.paths.append(duplicate)
        with self.assertRaisesRegex(ValueError, 'Missing or duplicate'):
            self.resolve()

    def test_external_checkout_launcher_is_not_silently_accepted(self):
        other = self.root / 'other-checkout'
        shutil.copytree(self.build, other)
        self.paths[:2] = [other / 'testcontainers/target/test-classes', other / 'testcontainers/target/classes']
        with self.assertRaisesRegex(ValueError, 'inside the selected checkout'):
            self.resolve()

    def test_production_classes_must_share_the_launcher_build(self):
        other = self.jobs / 'other/testcontainers/target/classes'
        self.write(other / inputs.PRODUCTION, b'production')
        self.paths[1] = other
        with self.assertRaisesRegex(ValueError, 'share one build'):
            self.resolve()

    def test_in_tree_snapshot_must_match_fixture_helper_and_production_sources(self):
        for relative in self.source_files:
            path = self.build / relative
            with self.subTest(path=relative):
                path.write_bytes(b'other checkout source')
                with self.assertRaisesRegex(ValueError, 'Build source differs'):
                    self.resolve()
                path.write_bytes(b'current source')

    def test_source_mutation_is_not_hidden_by_matching_build_paths(self):
        (self.source / self.source_files[0]).write_bytes(b'changed selected source')
        with self.assertRaisesRegex(ValueError, 'Build source differs'):
            self.resolve()

    def test_extra_snapshot_production_source_is_rejected(self):
        self.write(self.build / 'testcontainers/src/main/java/Extra.java', b'other')
        with self.assertRaisesRegex(ValueError, 'inventory differs'):
            self.resolve()

    def test_renamed_wrong_version_jar_is_rejected_using_its_metadata(self):
        self.jar(self.jars[0], 'jackson-annotations', version='2.17.0')
        with self.assertRaisesRegex(ValueError, 'Incompatible Jackson'):
            self.resolve()

    def test_duplicate_jackson_provider_is_rejected_even_under_another_filename(self):
        duplicate = self.root / 'innocent.jar'; self.jar(duplicate, 'jackson-core')
        self.paths.append(duplicate)
        with self.assertRaisesRegex(ValueError, 'Missing or duplicate'):
            self.resolve()

    def test_missing_or_wrong_provider_metadata_is_not_accepted_from_filename(self):
        for metadata, group in ((False, 'com.fasterxml.jackson.core'), (True, 'wrong.group')):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                self.jar(self.jars[0], 'jackson-annotations', metadata=metadata, group=group)
                self.resolve()

    def test_invalid_classpath_archive_is_rejected(self):
        self.jars[0].write_bytes(b'not a ZIP')
        with self.assertRaisesRegex(ValueError, 'Invalid classpath archive'):
            self.resolve()

    def test_wrong_suite_or_testcase_class_is_rejected(self):
        for wrong in ('suite', 'case'):
            root = self.xml()
            (root if wrong == 'suite' else root.find('testcase')).set('name' if wrong == 'suite' else 'classname', 'wrong')
            with self.subTest(wrong=wrong), self.assertRaises(ValueError):
                self.report_input(root)

    def test_failures_and_errors_are_rejected_even_when_counts_claim_zero(self):
        for name in ('failure', 'error'):
            root = self.xml(); ET.SubElement(root.find('testcase'), name)
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.report_input(root)
        for name in ('failures', 'errors'):
            root = self.xml(); root.set(name, '1')
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.report_input(root)

    def test_serialization_must_be_present_executed_and_unique(self):
        for mode in ('missing', 'skipped', 'duplicate'):
            root = self.xml()
            if mode == 'missing': root.find('testcase').set('name', 'other()')
            elif mode == 'skipped':
                ET.SubElement(root.find('testcase'), 'skipped'); root.set('skipped', '2')
            else:
                ET.SubElement(root, 'testcase', name=inputs.SERIALIZATION_TEST, classname=inputs.SUITE)
                root.set('tests', '3')
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.report_input(root)

    def test_report_count_and_unexpected_skip_are_rejected(self):
        root = self.xml(); root.set('tests', '20')
        with self.assertRaises(ValueError): self.report_input(root)
        root = self.xml(); root.findall('testcase')[1].set('name', 'anotherSkippedTest()')
        with self.assertRaises(ValueError): self.report_input(root)

    def test_ambiguous_missing_and_conflicting_properties_are_rejected(self):
        for mode in ('duplicate', 'missing', 'conflict'):
            root = self.xml(); props = root.find('properties')
            if mode == 'duplicate': ET.SubElement(props, 'property', name='surefire.test.class.path', value=self.classpath())
            elif mode == 'missing': props.remove(props[0])
            else: props[1].set('value', str(self.classes))
            with self.subTest(mode=mode), self.assertRaises(ValueError): self.report_input(root)

    def test_report_dtd_and_malformed_xml_are_rejected(self):
        for value in (b'<!DOCTYPE testsuite><testsuite/>', b'<incomplete'):
            self.report.write_bytes(value)
            with self.assertRaises((ValueError, ET.ParseError)):
                self.resolve(surefire_report=self.report)

    def test_ignored_output_is_checked_without_creating_anything(self):
        output = self.jobs / 'run'
        result = self.policy(output)
        self.assertEqual('ignored-in-tree', result['policy'])
        self.assertFalse(output.exists())
        self.assertEqual(['ls-files', 'check-ignore'], [call[3] for call in self.git_calls])

    def test_external_output_is_allowed_without_git_calls(self):
        output = self.root / 'external-run'
        self.assertEqual('external', self.policy(output)['policy'])
        self.assertFalse(output.exists())
        self.assertEqual([], self.git_calls)

    def test_tracked_and_unignored_output_is_rejected_before_creation(self):
        output = self.jobs / 'run'
        self.tracked = b'jobs/run/removed-but-tracked.txt\0'
        with self.assertRaisesRegex(ValueError, 'tracked'): self.policy(output)
        self.tracked = b''; self.ignored = False
        with self.assertRaisesRegex(ValueError, 'ignored'): self.policy(output)
        self.assertFalse(output.exists())

    def test_stale_output_and_symlink_ambiguity_are_rejected(self):
        stale = self.jobs / 'stale'; stale.mkdir()
        with self.assertRaisesRegex(ValueError, 'fresh'): self.policy(stale)
        link = self.root / 'link'; link.symlink_to(self.jobs, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'Symlink'): self.policy(link / 'new')

    def test_output_must_not_overlap_classpath_or_build_inputs(self):
        for output in (self.tests / 'run', self.classes / 'run', self.classes.parent / 'run',
                self.build / 'testcontainers/src/main/run', self.source / 'testcontainers/src/main/run'):
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, 'overlaps'):
                self.policy(output)
            self.assertFalse(output.exists())

    def test_direct_source_root_build_still_allows_ignored_jobs_output(self):
        direct_tests = self.source / 'testcontainers/target/test-classes'
        direct_classes = self.source / 'testcontainers/target/classes'
        self.write(direct_tests / inputs.LAUNCHER, b'fixture')
        self.write(direct_classes / inputs.PRODUCTION, b'production')
        self.paths[:2] = [direct_tests, direct_classes]
        result = self.policy(self.jobs / 'direct-build-run')
        self.assertEqual('ignored-in-tree', result['policy'])

    def test_input_validation_uses_existing_deadline(self):
        def expired(): raise TimeoutError('preflight bound')
        with self.assertRaises(TimeoutError): inputs.resolve_classpath(self.source, classpath=self.classpath(), check=expired)
        with self.assertRaises(TimeoutError):
            inputs.validate_output(self.jobs / 'run', self.source, self.resolve(), run=self.git, check=expired)
        self.assertEqual([], self.git_calls)


if __name__ == '__main__':
    unittest.main()
