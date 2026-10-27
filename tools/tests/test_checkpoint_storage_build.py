import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock


REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('checkpoint_storage_build', REPO / 'testcontainers/checkpoint_storage_build.py')
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


class CheckpointStorageBuildTest(unittest.TestCase):
    def setUp(self):
        self.lock = build.load_lock(REPO / build.NATIVE / 'dependencies.json')

    def assert_invalid_lock(self, change):
        lock = copy.deepcopy(self.lock)
        change(lock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / 'lock.json'
            path.write_text(json.dumps(lock))
            with self.assertRaises(ValueError):
                build.load_lock(path)

    def receipts(self):
        return '\n'.join('\t'.join(str(package[key]) for key in
                         ('package', 'version', 'architecture', 'size', 'sha256'))
                         for package in self.lock['packages'])

    def image_receipt(self):
        return json.dumps([{'Id': build.IMAGE_ID, 'Os': 'linux', 'Architecture': 'arm64',
                            'RepoDigests': [build.IMAGE_DIGEST]}])

    def test_cached_image_is_not_refreshed(self):
        commands = Mock()
        commands.docker_run.return_value = self.image_receipt()
        self.assertEqual(build.IMAGE_ID, build.ensure_image(commands)['Id'])
        self.assertEqual(1, commands.docker_run.call_count)

    def test_missing_image_downloads_only_pinned_digest_then_verifies(self):
        with tempfile.TemporaryDirectory() as directory:
            stderr = Path(directory) / 'stderr'
            stderr.write_text('Error response from daemon: No such image: ' + build.IMAGE_ID + '\n')
            commands = Mock()
            commands.docker_run.side_effect = [build.CommandFailure('inspect', 1, stderr), '', self.image_receipt()]
            build.ensure_image(commands)
            self.assertEqual(['pull', '--platform', build.PLATFORM, build.IMAGE_DIGEST],
                             commands.docker_run.call_args_list[1].args[1])
            self.assertEqual(['image', 'inspect', build.IMAGE_ID], commands.docker_run.call_args.args[1])

    def test_socket_failure_does_not_trigger_a_pull(self):
        with tempfile.TemporaryDirectory() as directory:
            stderr = Path(directory) / 'stderr'
            stderr.write_text('permission denied connecting to Docker socket\n')
            commands = Mock()
            commands.docker_run.side_effect = build.CommandFailure('inspect', 1, stderr)
            with self.assertRaises(build.CommandFailure):
                build.ensure_image(commands)
            self.assertEqual(1, commands.docker_run.call_count)

    def test_image_with_other_platform_or_digest_is_rejected(self):
        for update in ({'Architecture': 'amd64'}, {'RepoDigests': []}, {'Id': 'sha256:wrong'}):
            image = json.loads(self.image_receipt())[0]
            image.update(update)
            commands = Mock()
            commands.docker_run.return_value = json.dumps([image])
            with self.assertRaises(RuntimeError):
                build.ensure_image(commands)

    def test_checked_in_lock_is_complete_and_image_bound(self):
        self.assertGreater(len(self.lock['packages']), 4)
        self.assertEqual(build.IMAGE_ID, self.lock['imageId'])
        build.validate_package_receipts(self.receipts(), self.lock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / 'lock.json'
            path.write_text(json.dumps(self.lock))
            self.assertEqual(self.lock, build.load_lock(path))

    def test_remote_or_nonliteral_endpoint_is_rejected(self):
        for endpoint in ('tcp://localhost:2375', 'ssh://host', 'unix://host/run/docker.sock',
                         'unix:///run/../docker.sock', 'unix:///run/docker.sock?x=1', 'unix:///run/%64ocker.sock'):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                build.local_endpoint(endpoint)
        self.assertEqual('unix:///var/run/docker.sock', build.local_endpoint('unix:///var/run/docker.sock'))

    def test_lock_rejects_image_or_package_boundary_changes(self):
        for change in (lambda lock: lock.update(imageId='sha256:' + '0' * 64),
                       lambda lock: lock['packages'][0].update(url='https://example.com/package.deb'),
                       lambda lock: lock['packages'][0].update(version='1;curl example.com'),
                       lambda lock: lock['packages'].append(lock['packages'][0]),
                       lambda lock: lock['roots'].update(gcc='missing')):
            self.assert_invalid_lock(change)

    def test_package_receipts_reject_missing_duplicate_or_modified_download(self):
        lines = self.receipts().splitlines()
        modified = lines[:]
        columns = modified[0].split('\t')
        columns[-1] = '0' * 64
        modified[0] = '\t'.join(columns)
        for text in ('\n'.join(lines[1:]), '\n'.join(lines + lines[:1]), '\n'.join(modified), 'bad'):
            with self.subTest(text=text[:30]), self.assertRaises(ValueError):
                build.validate_package_receipts(text, self.lock)

    def test_new_dynamic_runtime_dependency_is_rejected(self):
        libc = '0x00000001 (NEEDED) Shared library: [libc.so.6]\n'
        self.assertEqual(['libc.so.6'], build.validate_elf(libc))
        with self.assertRaises(ValueError):
            build.validate_elf(libc + '0x000001 (NEEDED) Shared library: [libfuse3.so.3]')
        with self.assertRaises(ValueError):
            build.validate_elf('')

    def test_source_inputs_refuse_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / 'actual').write_text('generated fixture')
            (root / 'alias').symlink_to(root / 'actual')
            with self.assertRaises(ValueError):
                build.checked_path(root / 'alias')

    def test_personal_directory_refused_before_filesystem_inspection(self):
        with self.assertRaises(ValueError):
            build.checked_path('/Users/example/Documents/not-read', exists=False)

    def test_build_environment_is_explicit_and_config_is_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            commands = build.Commands(Path(directory), 'unix:///var/run/docker.sock', Path('/bin/docker'), 60)
            self.assertEqual([], list(commands.config.iterdir()))
            self.assertNotIn('HOME', commands.environment)
            self.assertNotIn('AWS_ACCESS_KEY_ID', commands.environment)
            self.assertEqual('unix:///var/run/docker.sock', commands.environment['DOCKER_HOST'])

    def test_cleanup_requires_exact_owned_label_and_image(self):
        with tempfile.TemporaryDirectory() as directory:
            commands = build.Commands(Path(directory), 'unix:///var/run/docker.sock', Path('/bin/docker'), 60)
            for item in ({'Image': build.IMAGE_ID, 'Config': {'Labels': {build.LABEL: 'other'}}},
                         {'Image': 'sha256:wrong', 'Config': {'Labels': {build.LABEL: 'owned'}}}):
                commands.docker_run = Mock(return_value=json.dumps([item]))
                with self.assertRaises(RuntimeError):
                    commands.remove_owned('owned')
                self.assertEqual(1, commands.docker_run.call_count)
            commands.docker_run = Mock(return_value=json.dumps([
                {'Image': build.IMAGE_ID, 'Config': {'Labels': {build.LABEL: 'owned'}}}]))
            commands.remove_owned('owned')
            self.assertEqual(['rm', '-f', 'owned'], commands.docker_run.call_args.args[1])

    def test_deadline_stops_new_work_before_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            commands = build.Commands(Path(directory), 'unix:///var/run/docker.sock', Path('/missing/docker'), 60)
            commands.deadline = 0
            with self.assertRaises(TimeoutError):
                commands.run('never-run', ['/missing/docker'])
            self.assertEqual([], commands.receipts)

    def test_retained_package_cache_must_match_complete_closure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            package = root / 'generated.deb'
            package.write_bytes(b'generated package fixture')
            lock = {'packages': [{'sha256': build.sha256(package)}]}
            build.validate_package_cache(root, lock)
            package.write_bytes(b'changed package fixture')
            with self.assertRaises(ValueError):
                build.validate_package_cache(root, lock)


if __name__ == '__main__':
    unittest.main()
