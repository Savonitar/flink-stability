import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
import zipfile
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
            stderr.write_text('Error response from daemon: No such image: ' + build.IMAGE_DIGEST + '\n')
            commands = Mock()
            commands.docker_run.side_effect = [build.CommandFailure('inspect', 1, stderr), '', self.image_receipt()]
            build.ensure_image(commands)
            self.assertEqual(['pull', '--platform', build.PLATFORM, build.IMAGE_DIGEST],
                             commands.docker_run.call_args_list[1].args[1])
            self.assertEqual(['image', 'inspect', build.IMAGE_DIGEST], commands.docker_run.call_args.args[1])

    def test_containerd_index_identity_keeps_pinned_repository_and_platform(self):
        image = json.loads(self.image_receipt())[0]
        image['Id'] = build.IMAGE_DIGEST.split('@')[1]
        commands = Mock()
        commands.docker_run.return_value = json.dumps([image])
        self.assertEqual(image, build.ensure_image(commands))
        self.assertEqual(['image', 'inspect', build.IMAGE_DIGEST], commands.docker_run.call_args.args[1])

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

    def test_created_container_identity_is_bound_before_start_and_cleanup_rejects_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            commands = build.Commands(Path(directory), 'unix:///var/run/docker.sock', Path('/bin/docker'), 60)
            image = json.loads(self.image_receipt())[0]
            image['Id'] = build.IMAGE_DIGEST.split('@')[1]
            container = {'Image': build.IMAGE_ID, 'Config': {'Image': image['Id'], 'Labels': {build.LABEL: 'owned'}}}
            commands.docker_run = Mock(return_value=json.dumps([container]))
            receipt = commands.bind_owned('owned', image)
            self.assertEqual(build.IMAGE_ID, receipt['containerImageId'])
            self.assertEqual(image['Id'], receipt['inspectedImageId'])
            container['Image'] = image['Id']
            commands.docker_run = Mock(return_value=json.dumps([container]))
            with self.assertRaises(RuntimeError):
                commands.remove_owned('owned')
            self.assertEqual(1, commands.docker_run.call_count)

    def test_package_install_and_source_copy_require_receipts_and_disconnected_network(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            packages = root / 'packages'
            packages.mkdir()
            package = packages / 'fixture.deb'
            package.write_bytes(b'generated fixture package')
            lock = {'packages': [{'package': 'fixture', 'version': '1', 'architecture': 'arm64',
                                 'size': package.stat().st_size, 'sha256': build.sha256(package)}]}
            receipt = '\t'.join(str(lock['packages'][0][key]) for key in
                                ('package', 'version', 'architecture', 'size', 'sha256'))
            command = Mock()
            command.docker_run.return_value = json.dumps([{'NetworkSettings': {'Networks': {}}}])
            build.install_verified_packages(command, 'owned', lock, receipt, packages, root / 'frozen')
            self.assertEqual(['disconnect-network', 'verify-network-before-install', 'apt-install-locked',
                              'verify-network-before-source', 'copy-frozen-source'],
                             [call.args[0] for call in command.docker_run.call_args_list])
            self.assertIn('--no-download', command.docker_run.call_args_list[2].args[1])
            command.reset_mock()
            with self.assertRaises(ValueError):
                build.install_verified_packages(command, 'owned', lock, 'bad receipt', packages, root / 'frozen')
            command.docker_run.assert_not_called()
            package.write_bytes(b'changed')
            with self.assertRaises(ValueError):
                build.install_verified_packages(command, 'owned', lock, receipt, packages, root / 'frozen')
            command.docker_run.assert_not_called()

    def test_network_missing_connected_or_reappearing_prevents_next_sensitive_build_step(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            package = root / 'fixture.deb'
            package.write_bytes(b'generated')
            lock = {'packages': [{'package': 'fixture', 'version': '1', 'architecture': 'arm64',
                                 'size': package.stat().st_size, 'sha256': build.sha256(package)}]}
            receipt = '\t'.join(str(lock['packages'][0][key]) for key in
                                ('package', 'version', 'architecture', 'size', 'sha256'))
            for broken in ({}, {'NetworkSettings': {'Networks': {'bridge': {}}}}):
                command = Mock()
                command.docker_run.side_effect = ['', json.dumps([broken])]
                with self.assertRaises(RuntimeError):
                    build.install_verified_packages(command, 'owned', lock, receipt, root, root / 'frozen')
                self.assertNotIn('apt-install-locked', [call.args[0] for call in command.docker_run.call_args_list])
            command = Mock()
            command.docker_run.side_effect = ['', json.dumps([{'NetworkSettings': {'Networks': {}}}]), '',
                                              json.dumps([{'NetworkSettings': {'Networks': {'bridge': {}}}}])]
            with self.assertRaises(RuntimeError):
                build.install_verified_packages(command, 'owned', lock, receipt, root, root / 'frozen')
            self.assertNotIn('copy-frozen-source', [call.args[0] for call in command.docker_run.call_args_list])

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

    def test_toolchain_requires_identified_jdk21_before_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / 'lib').mkdir()
            (root / 'lib/modules').write_bytes(b'generated module fixture')
            for release in ('JAVA_VERSION="17.0.1"\nIMPLEMENTOR="Generated vendor"\n',
                            'JAVA_VERSION="21.0.1"\n',
                            'JAVA_VERSION="21.0.1"\nIMPLEMENTOR="A"\nIMPLEMENTOR="B"\n'):
                (root / 'release').write_text(release)
                commands = Mock()
                with self.subTest(release=release), self.assertRaises((RuntimeError, ValueError)):
                    build.java_toolchain(commands, root, {})
                commands.run.assert_not_called()


class ProbeJarReproducibilityTest(unittest.TestCase):
    def test_real_selected_jdk_builds_identical_probe_archives_in_distinct_directories(self):
        configured = os.environ.get('JAVA_HOME')
        javac = shutil.which('javac') if not configured else None
        if not configured and not javac:
            self.skipTest('JDK21 not available; set JAVA_HOME to run the real compiler fixture')
        try:
            java_home = build.checked_path(configured, directory=True) if configured else build.executable(javac).parent.parent
            java_tools = {name: build.checked_path(java_home / 'bin' / name) for name in ('java', 'javac', 'jar')}
            release = build.checked_path(java_home / 'release').read_text(encoding='utf-8')
        except (OSError, ValueError):
            self.skipTest('An explicit complete JDK21 is unavailable')
        if not any(line.startswith('JAVA_VERSION="21') for line in release.splitlines()):
            self.skipTest('Selected JAVA_HOME/javac is not JDK21')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            archives, identities, receipts = [], [], []
            for index in (1, 2):
                output = root / ('build-' + str(index))
                output.mkdir()
                (output / 'bundle').mkdir()
                source = output / 'source'
                source.mkdir()
                # Reverse creation order and change mtimes; neither is an archive input.
                for file in ([build.JAVA, build.INSPECTOR] if index == 1 else [build.INSPECTOR, build.JAVA]):
                    destination = source / file.name
                    destination.write_bytes((REPO / file).read_bytes())
                    os.utime(destination, (946684800 * index, 946684800 * index))
                commands = build.Commands(output, 'unix:///var/run/docker.sock', Path('/not-used/docker'), 60)
                identities.append(build.java_toolchain(commands, java_home, java_tools))
                receipts.append(build.compile_probe(commands, java_tools, source, output))
                archives.append((output / 'bundle/probe.jar').read_bytes())
                if index == 1:
                    view, evidence = output / 'view', output / 'probe-evidence'
                    view.mkdir()
                    evidence.mkdir()
                    smoke = commands.run('java-archive-smoke', [str(java_tools['java']), '-jar',
                        str(output / 'bundle/probe.jar'), '--root', str(view), '--raw-root', str(output / 'raw'),
                        '--evidence-root', str(evidence), '--timeout-seconds', '10'], stdin='1\thello\n2\tquit\n')
                    self.assertEqual(['ok', 'ok'], [row['status'] for row in map(json.loads, smoke.splitlines())])
                with zipfile.ZipFile(output / 'bundle/probe.jar') as jar:
                    names = jar.namelist()
                    self.assertEqual('META-INF/MANIFEST.MF', names[0])
                    self.assertEqual(sorted(names[1:]), names[1:])
                    self.assertEqual(build.JAR_MANIFEST, jar.read(names[0]))
                    self.assertNotIn(b'Created-By:', jar.read(names[0]))
                    self.assertTrue(all(entry.date_time == build.JAR_TIMESTAMP for entry in jar.infolist()))
                    self.assertTrue(all(entry.compress_type == zipfile.ZIP_STORED for entry in jar.infolist()))
                    self.assertIn(build.MAIN.replace('.', '/') + '$HeldFile.class', names)
            self.assertEqual(archives[0], archives[1])
            self.assertEqual(receipts[0], receipts[1])
            self.assertEqual(identities[0], identities[1])
            self.assertTrue(identities[0]['vendor'])
            self.assertEqual(build.sha256(java_home / 'lib/modules'), identities[0]['modulesSha256'])
            for name, tool in identities[0]['tools'].items():
                self.assertEqual(build.sha256(java_tools[name]), tool['sha256'])
                self.assertIn('21', tool['version'])
                self.assertIn('build', tool['runtimeVersion'])


if __name__ == '__main__':
    unittest.main()
