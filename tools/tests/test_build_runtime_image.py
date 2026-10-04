"""Docker-free image assembly and workload stale-output regression tests."""
from pathlib import Path
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools'))
import build_runtime_image as builder


def jar(path, classes=None, version='2.2.0', extra=None, multi_release=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, 'w') as output:
        output.writestr('META-INF/MANIFEST.MF', 'Manifest-Version: 1.0\nImplementation-Version: ' + version + '\n'
                        + ('Multi-Release: true\n' if multi_release else ''))
        for name, major in (classes or {'example/Runtime.class': 55}).items():
            output.writestr(name, struct.pack('>IHH', 0xcafebabe, 0, major))
        for name, content in (extra or {}).items():
            output.writestr(name, content)


class RuntimeImageBuilderTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.dist = self.root / 'dist'
        for name in ('bin/flink', 'bin/jobmanager.sh', 'bin/taskmanager.sh', 'bin/config-parser-utils.sh', 'conf/config.yaml'):
            p = self.dist / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('# fixture\n')
            p.chmod(0o755 if name.startswith('bin/') else 0o644)
        self.runtime = self.dist / 'lib/flink-dist-2.2.0.jar'
        jar(self.runtime)
        self.connector = self.root / 'connector.jar'
        jar(self.connector, {'org/apache/flink/connector/kafka/sink/KafkaSink.class': 55})
        self.closure = self.root / 'closure'
        self.closure.mkdir()
        jar(self.closure / 'dependency.jar')
        self.output = self.root / 'output'

    def prepare(self, **kwargs):
        return builder.prepare(self.dist, self.connector, self.closure, self.output, **kwargs)

    def test_staging_is_explicit_immutable_and_manifest_hashes_cover_inputs(self):
        (self.dist / 'plugins/empty').mkdir(parents=True)
        before = builder.sha256(self.runtime)
        manifest = self.prepare()
        self.assertEqual('flink:2.2.0-java17', manifest['baseImage'])
        self.assertEqual(before, builder.sha256(self.runtime))
        self.assertEqual(before, manifest['runtimeJar']['sha256'])
        self.assertEqual(3, len(manifest['imageJars']))
        for entry in manifest['inputs']:
            self.assertEqual(entry['sha256'], builder.sha256(self.output / 'context' / entry['contextPath']))
        self.assertFalse((self.dist / 'Dockerfile').exists())
        self.assertTrue((self.output / 'context/dist/plugins/empty').is_dir())
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.prepare()

    def test_classfile_java_floor_and_inactive_multirelease_entries(self):
        jar(self.runtime, {'example/Runtime.class': 65})
        with self.assertRaisesRegex(ValueError, 'requires Java 21'):
            self.prepare(java=17)
        result = self.prepare(java=21)
        self.assertEqual(65, result['runtimeJar']['maxActiveClassMajor'])
        another = self.root / 'multi.jar'
        jar(another, {'example/Base.class': 55, 'META-INF/versions/21/example/Base.class': 65},
            extra={'META-INF/versions/': b'', 'META-INF/versions/21/': b''}, multi_release=True)
        self.assertEqual(55, builder.jar_info(another, 17)['maxActiveClassMajor'])
        self.assertEqual(65, builder.jar_info(another, 21)['maxActiveClassMajor'])
        jar(another, {'example/Base.class': 55, 'META-INF/versions/21/example/Base.class': 66})
        self.assertEqual(55, builder.jar_info(another, 21)['maxActiveClassMajor'])

    def test_duplicate_connector_classes_in_dist_or_closure_are_rejected(self):
        for directory in (self.dist / 'lib', self.closure):
            duplicate = directory / 'duplicate.jar'
            jar(duplicate, {'org/apache/flink/streaming/connectors/kafka/Legacy.class': 55})
            with self.assertRaisesRegex(ValueError, 'Exactly the supplied connector'):
                self.prepare()
            duplicate.unlink()

    def test_off_classpath_distribution_examples_are_preserved(self):
        example = self.dist / 'examples/streaming/StateMachineExample.jar'
        jar(example, {'org/apache/flink/streaming/connectors/kafka/Legacy.class': 55})
        manifest = self.prepare()
        self.assertIn('/opt/flink/examples/streaming/StateMachineExample.jar', manifest['imageJars'])
        self.assertEqual(builder.sha256(example), builder.sha256(self.output / 'context/dist/examples/streaming/StateMachineExample.jar'))

    def test_missing_connector_classes_and_separate_runtime_jars_are_rejected(self):
        jar(self.connector)
        with self.assertRaisesRegex(ValueError, 'Exactly the supplied connector'):
            self.prepare()
        jar(self.connector, {'org/apache/flink/connector/kafka/Subject.class': 55})
        jar(self.closure / 'flink-runtime-2.2.0.jar')
        with self.assertRaisesRegex(ValueError, 'Separate flink-runtime'):
            self.prepare()

    def test_filename_collisions_unknown_dist_entries_and_nested_closure_are_rejected(self):
        collision = self.closure / self.runtime.name
        jar(collision)
        with self.assertRaisesRegex(ValueError, 'collides'):
            self.prepare()
        collision.unlink()
        (self.dist / 'unrelated.txt').write_text('not a distribution file')
        with self.assertRaisesRegex(ValueError, 'unexpected entries'):
            self.prepare()
        (self.dist / 'unrelated.txt').unlink()
        jar(self.closure / 'nested/other.jar')
        with self.assertRaisesRegex(ValueError, 'flat directory'):
            self.prepare()

    def test_symlinks_and_output_inside_input_are_rejected(self):
        (self.dist / 'conf/link').symlink_to(self.connector)
        with self.assertRaisesRegex(ValueError, 'regular files'):
            self.prepare()
        (self.dist / 'conf/link').unlink()
        with self.assertRaisesRegex(ValueError, 'outside the input'):
            builder.prepare(self.dist, self.connector, self.closure, self.dist / 'build')
        link = self.root / 'dist-link'
        link.symlink_to(self.dist, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'Symlinks'):
            builder.prepare(link, self.connector, self.closure, self.output)
        embedded = self.dist / 'opt'
        embedded.mkdir()
        with self.assertRaisesRegex(ValueError, 'nonoverlapping inputs'):
            builder.prepare(self.dist, self.connector, embedded, self.output)

    def test_custom_distribution_version_requires_explicit_public_base(self):
        self.runtime.unlink()
        self.runtime = self.dist / 'lib/flink-dist_2.12-2.0-SNAPSHOT.jar'
        jar(self.runtime, version='2.0-SNAPSHOT')
        with self.assertRaisesRegex(ValueError, 'use --base-image'):
            self.prepare()
        result = self.prepare(java=21, base_image='flink:2.2.0-java21')
        self.assertEqual('2.0-SNAPSHOT', result['flinkVersion'])
        self.assertEqual('flink:2.2.0-java21', result['baseImage'])

    def test_dockerfile_replaces_stock_tree_and_uses_official_parser(self):
        text = builder.dockerfile('flink:2.2.0-java21@sha256:' + 'a' * 64, True)
        self.assertIn('rm -rf /opt/flink && mkdir -p /opt/flink', text)
        self.assertIn('COPY --chown=flink:flink dist/ /opt/flink/', text)
        self.assertNotIn('ENTRYPOINT', text)
        self.assertIn('USER flink', text)
        for option in ('rest.address', 'rest.bind-address', 'jobmanager.bind-host', 'taskmanager.bind-host'):
            self.assertIn('-repKV ' + option + ',localhost,0.0.0.0', text)
        self.assertIn('-rmKV taskmanager.host=localhost', text)
        self.assertNotIn('closure/', builder.dockerfile('flink:2.2.0-java17', False))

    def test_build_is_local_and_finished_image_hashes_are_observed(self):
        manifest = self.prepare()
        inspections = 0
        calls = []
        def run(command, **kwargs):
            nonlocal inspections
            calls.append((command, kwargs))
            stdout, stderr, code = '', '', 0
            if 'inspect' in command:
                inspections += 1
                if inspections == 1:
                    code = 1
                else:
                    stdout = json.dumps([{'Id': 'sha256:' + 'a' * 64, 'RepoDigests': ['flink@sha256:' + 'b' * 64]}])
            elif '-XshowSettings:properties' in command:
                stderr = 'java.specification.version = 17\n'
            elif 'find /opt/flink' in command[-1]:
                stdout = ''.join(digest + '  ' + path + '\n' for path, digest in manifest['imageJars'].items())
            return subprocess.CompletedProcess(command, code, stdout, stderr)
        with mock.patch.object(builder.subprocess, 'run', side_effect=run), mock.patch.dict(os.environ, {'DOCKER_HOST': 'unix:///var/run/docker.sock'}):
            result = builder.build(self.output, manifest, 'local/subject:test')
        self.assertEqual('verified', result['status'])
        self.assertEqual(result['imageJars'], result['observedImageJars'])
        for _, kwargs in calls:
            self.assertEqual('0', kwargs['env']['DOCKER_BUILDKIT'])
            self.assertEqual(str(self.output / 'docker-config'), kwargs['env']['DOCKER_CONFIG'])
            self.assertNotIn('HOME', kwargs['env'])
        self.assertEqual([], list((self.output / 'docker-config').iterdir()))

    def test_remote_daemon_is_rejected_before_any_docker_call(self):
        manifest = self.prepare()
        with mock.patch.dict(os.environ, {'DOCKER_HOST': 'tcp://remote:2375'}), mock.patch.object(builder.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'local Unix-socket'):
                builder.build(self.output, manifest, 'local/subject:test')
            run.assert_not_called()


class WorkloadBuildGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        home = os.environ.get('JAVA_HOME')
        cls.java = str(Path(home) / 'bin/java') if home else shutil.which('java')
        if not cls.java or subprocess.run([cls.java, '-version'], capture_output=True).returncode:
            raise unittest.SkipTest('A local JDK is required for the workload source guard tests')

    def test_changed_versions_connector_or_release_require_clean_and_preserve_markers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            target = root / 'target'
            settings = ['2.2.0', 'org.apache.flink', '5.0.0-2.2', '11', '4.0.0']
            def run(values):
                return subprocess.run([self.java, '-Duser.home=' + str(root), str(ROOT / 'flink-job-generator/WorkloadBuildGuard.java'),
                                       str(target), *values], env={'PATH': os.defpath}, capture_output=True, text=True)
            self.assertEqual(0, run(settings).returncode)
            self.assertEqual('2.2.0\n', (target / 'flink.version').read_text())
            (target / 'classes').mkdir()
            original = (target / 'workload-build.signature').read_bytes()
            for index, replacement in enumerate(['2.0.0', 'vendor.flink', 'custom', '17', '4.1.0']):
                changed = settings.copy()
                changed[index] = replacement
                failed = run(changed)
                self.assertNotEqual(0, failed.returncode)
                self.assertIn('clean package', failed.stderr)
                self.assertEqual(original, (target / 'workload-build.signature').read_bytes())
            self.assertEqual(0, run(settings).returncode)
            shutil.rmtree(target)
            settings[3] = '17'
            self.assertEqual(0, run(settings).returncode)
            self.assertIn('maven.compiler.release=17', (target / 'workload-build.signature').read_text())

    def test_unmarked_compiled_output_requires_clean(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory).resolve() / 'target'
            (target / 'classes').mkdir(parents=True)
            result = subprocess.run([self.java, str(ROOT / 'flink-job-generator/WorkloadBuildGuard.java'),
                                     str(target), '2.2.0', 'org.apache.flink', '5.0.0-2.2', '11', '4.0.0'],
                                    env={'PATH': os.defpath}, capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)
            self.assertIn('unrecorded', result.stderr)
            self.assertFalse((target / 'flink.version').exists())


if __name__ == '__main__':
    unittest.main()
