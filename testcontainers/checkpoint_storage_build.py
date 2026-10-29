#!/usr/bin/env python3
"""Build the isolated PR3 helper bundle against a pinned local Flink image.

Only public locked packages enter the networked build container. Repository
sources enter after its network is disconnected. No Docker build, host bind,
daemon socket mount, image mutation, credential configuration, or publication.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
from urllib.parse import unquote, urlsplit


REPO = Path(__file__).resolve().parents[1]
NATIVE = Path('testcontainers/src/main/native/checkpoint-fuse')
JAVA = Path('testcontainers/src/main/java/org/savonitar/flink/stability/testcontainers/storage/CheckpointStorageIoProbe.java')
INSPECTOR = JAVA.with_name('CheckpointStateInspector.java')
MAIN = 'org.savonitar.flink.stability.testcontainers.storage.CheckpointStorageIoProbe'
NATIVE_SOURCES = ('core.c', 'core.h', 'jail.c', 'jail.h', 'checkpoint_fuse.c', 'selftest.c')
IMAGE_ID = 'sha256:2e93ded2cb7bf1a50f71b663d5e3ea01420bcc4e549cbf666d05120f939713c7'
IMAGE_DIGEST = 'flink@sha256:7ecfab9d5e8ec6987a248b32f6605b5fe9bcb7347aea0d7b1bc3976588408681'
PLATFORM = 'linux/arm64'
FLAGS = ('-std=c11', '-D_FILE_OFFSET_BITS=64', '-O2', '-Wall', '-Wextra', '-Werror', '-pthread')
LABEL = 'org.savonitar.checkpoint-fuse.build'
SHA = re.compile(r'[0-9a-f]{64}')
PACKAGE = re.compile(r'[a-z0-9][a-z0-9+.-]*')
VERSION = re.compile(r'[A-Za-z0-9:.+~_-]+')
APT_OPTIONS = ['-o', 'Dir::Etc::sourcelist=/work/ubuntu.sources', '-o', 'Dir::Etc::sourceparts=-',
               '-o', 'Acquire::AllowInsecureRepositories=false', '-o', 'APT::Get::AllowUnauthenticated=false']
SOURCES = ('Types: deb\nURIs: https://ports.ubuntu.com/ubuntu-ports\n'
           'Suites: noble noble-updates noble-security\nComponents: main universe\n'
           'Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n')


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def checked_path(value, directory=False, exists=True):
    path = Path(os.path.abspath(value))
    denied = {'Documents', 'Downloads', 'Desktop', 'Photos', 'Pictures', 'Videos'}
    # Refuse before resolving or inspecting any entry underneath a personal tree.
    if any(p in denied for p in path.parts):
        raise ValueError('Personal media/document directories are not build inputs')
    for entry in (path, *path.parents):
        if entry.is_symlink():
            raise ValueError('Symlink paths are not accepted')
    if exists and not (path.is_dir() if directory else path.is_file()):
        raise ValueError('Expected an existing ' + ('directory' if directory else 'regular file'))
    return path


def local_endpoint(value):
    parsed = urlsplit(value)
    if parsed.scheme != 'unix' or parsed.netloc or parsed.query or parsed.fragment or not parsed.path.startswith('/'):
        raise ValueError('Docker endpoint must be an explicit local unix:/// socket')
    if unquote(parsed.path) != parsed.path or '..' in Path(parsed.path).parts:
        raise ValueError('Docker socket path must be literal and normalized')
    return value


def executable(value):
    path = Path(os.path.abspath(value))
    for _ in range(8):
        if any(part in {'Documents', 'Downloads', 'Desktop', 'Photos', 'Pictures', 'Videos'} for part in path.parts):
            raise ValueError('Personal directories are not executable inputs')
        # Docker Desktop normally installs /usr/local/bin/docker as this kind
        # of explicit binary alias. Source/output trees still reject symlinks.
        if not path.is_symlink():
            return checked_path(path)
        target = path.readlink()
        path = Path(os.path.abspath(target if target.is_absolute() else path.parent / target))
    raise ValueError('Executable symlink chain is too long')


def load_lock(path):
    lock = json.loads(checked_path(path).read_text())
    if (lock.get('schemaVersion') != 1 or lock.get('imageId') != IMAGE_ID
            or lock.get('imageDigest') != IMAGE_DIGEST or lock.get('platform') != PLATFORM
            or lock.get('sources') != SOURCES):
        raise ValueError('Dependency lock does not match the pinned build boundary')
    packages = lock.get('packages')
    if not isinstance(packages, list) or not 1 <= len(packages) <= 128:
        raise ValueError('Invalid dependency closure size')
    seen = set()
    for package in packages:
        name, version = package.get('package', ''), package.get('version', '')
        filename, url = package.get('filename', ''), package.get('url', '')
        if (not PACKAGE.fullmatch(name) or not VERSION.fullmatch(version) or name in seen
                or package.get('architecture') not in ('arm64', 'all')
                or not SHA.fullmatch(package.get('sha256', ''))
                or type(package.get('size')) is not int or not 0 < package['size'] < 100_000_000
                or not filename.startswith('pool/') or '..' in Path(filename).parts
                or not re.fullmatch(r'[A-Za-z0-9/_.+~:-]+\.deb', filename)
                or unquote(url) != 'https://ports.ubuntu.com/ubuntu-ports/' + filename):
            raise ValueError('Invalid or duplicate locked package')
        seen.add(name)
    roots = lock.get('roots', {})
    if set(roots) != {'gcc', 'libc6-dev', 'libfuse3-dev', 'pkg-config'}:
        raise ValueError('Unexpected build roots')
    for name, version in roots.items():
        if not any(p['package'] == name and p['version'] == version for p in packages):
            raise ValueError('Root is absent from closure')
    return lock


def validate_package_receipts(text, lock):
    expected = {p['package']: p for p in lock['packages']}
    observed = set()
    for line in text.splitlines():
        columns = line.split('\t')
        if len(columns) != 5:
            raise ValueError('Malformed package receipt')
        name, version, architecture, size, digest = columns
        package = expected.get(name)
        if (package is None or name in observed or version != package['version']
                or architecture != package['architecture'] or size != str(package['size'])
                or digest != package['sha256']):
            raise ValueError('Downloaded package differs from locked closure: ' + name)
        observed.add(name)
    if observed != set(expected):
        raise ValueError('Incomplete downloaded package closure')


def validate_elf(text):
    needed = re.findall(r'\(NEEDED\).*\[([^]]+)\]', text)
    if not needed or set(needed) - {'libc.so.6', 'ld-linux-aarch64.so.1'}:
        raise ValueError('Helper depends on libraries outside the original runtime libc: ' + ', '.join(needed))
    return needed


def validate_package_cache(directory, lock):
    expected = {p['sha256'] for p in lock['packages']}
    observed = set()
    for file in directory.iterdir():
        checked_path(file)
        digest = sha256(file)
        if file.suffix != '.deb' or digest not in expected or digest in observed:
            raise ValueError('Copied package cache differs from verified closure')
        observed.add(digest)
    if observed != expected:
        raise ValueError('Copied package cache is incomplete')


class CommandFailure(RuntimeError):
    def __init__(self, label, returncode, stderr):
        super().__init__(label + ' failed; see ' + str(stderr))
        self.returncode = returncode
        self.stderr = stderr


def ensure_image(command):
    try:
        raw = command.docker_run('image-inspect', ['image', 'inspect', IMAGE_DIGEST])
    except CommandFailure as error:
        missing = re.search(r'(?:^|\n)(?:Error(?: response from daemon)?: )?No such (?:image|object): '
                            + re.escape(IMAGE_DIGEST) + r'\s*$', error.stderr.read_text(), re.IGNORECASE)
        if error.returncode != 1 or not missing:
            raise
        command.docker_run('pull-pinned-image', ['pull', '--platform', PLATFORM, IMAGE_DIGEST])
        raw = command.docker_run('image-inspect-after-pull', ['image', 'inspect', IMAGE_DIGEST])
    inspected = json.loads(raw)[0]
    if (inspected.get('Id') not in (IMAGE_ID, IMAGE_DIGEST.split('@')[1]) or inspected.get('Os') != 'linux'
            or inspected.get('Architecture') != 'arm64' or IMAGE_DIGEST not in inspected.get('RepoDigests', [])):
        raise RuntimeError('Local base image does not match dependency lock identity')
    return inspected


class Commands:
    def __init__(self, output, endpoint, docker, timeout):
        self.output = output
        self.log = output / 'commands'
        self.log.mkdir()
        self.config = output / 'docker-config'
        self.config.mkdir(mode=0o700)
        self.docker = [str(docker), '--config', str(self.config), '--host', endpoint]
        self.environment = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'DOCKER_API_VERSION': '1.44',
                            'DOCKER_CONFIG': str(self.config), 'DOCKER_HOST': endpoint,
                            'TMPDIR': str(output / 'tmp'), 'LANG': 'C.UTF-8'}
        (output / 'tmp').mkdir()
        self.deadline = time.monotonic() + timeout
        self.receipts = []
        self.owned_images = {}

    def run(self, label, command, *, stdin=None, cleanup=False, limit=300):
        remaining = 40 if cleanup else self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Build deadline expired before ' + label)
        number = len(self.receipts) + 1
        stem = f'{number:03d}-{label}'
        stdout, stderr = self.log / (stem + '.stdout'), self.log / (stem + '.stderr')
        record = {'label': label, 'argv': command, 'stdout': str(stdout.relative_to(self.output)),
                  'stderr': str(stderr.relative_to(self.output))}
        self.receipts.append(record)
        started = time.monotonic()
        try:
            with stdout.open('wb') as out, stderr.open('wb') as err:
                result = subprocess.run(command, input=stdin.encode() if stdin is not None else None,
                                        stdout=out, stderr=err, env=self.environment,
                                        timeout=min(limit, remaining), check=False)
            record['exitCode'] = result.returncode
            if result.returncode:
                raise CommandFailure(label, result.returncode, stderr)
            return stdout.read_text()
        except subprocess.TimeoutExpired as error:
            record['timedOut'] = True
            raise TimeoutError('Deadline expired during ' + label) from error
        finally:
            record['elapsedSeconds'] = round(time.monotonic() - started, 3)

    def docker_run(self, label, args, **kwargs):
        return self.run(label, self.docker + args, **kwargs)

    def remove_owned(self, name):
        result = self.docker_run('cleanup-inspect', ['inspect', name], cleanup=True)
        item = json.loads(result)[0]
        expected = self.owned_images.get(name)
        allowed = (expected,) if expected else (IMAGE_ID, IMAGE_DIGEST.split('@')[1])
        if item.get('Config', {}).get('Labels', {}).get(LABEL) != name or item.get('Image') not in allowed:
            raise RuntimeError('Refusing to remove a container without exact build ownership')
        self.docker_run('cleanup-remove', ['rm', '-f', name], cleanup=True)

    def bind_owned(self, name, inspected_image):
        item = json.loads(self.docker_run('verify-created-image', ['inspect', name]))[0]
        if (item.get('Config', {}).get('Labels', {}).get(LABEL) != name
                or item.get('Config', {}).get('Image') != inspected_image['Id']
                or item.get('Image') not in (IMAGE_ID, IMAGE_DIGEST.split('@')[1])):
            raise RuntimeError('Created build container does not match pinned image ownership')
        self.owned_images[name] = item['Image']
        return {'container': name, 'containerImageId': item['Image'], 'inspectedImageId': inspected_image['Id'],
                'repoDigests': inspected_image['RepoDigests']}


def install_verified_packages(command, name, lock, packages, package_directory, frozen):
    """No package installation or source transfer precedes complete receipt and network checks."""
    validate_package_receipts(packages, lock)
    validate_package_cache(package_directory, lock)
    command.docker_run('disconnect-network', ['network', 'disconnect', 'bridge', name])
    for stage in ('before-install', 'before-source'):
        network = json.loads(command.docker_run('verify-network-' + stage, ['inspect', name]))[0]
        if network.get('NetworkSettings', {}).get('Networks') != {}:
            raise RuntimeError('Builder network isolation is unconfirmed before ' + stage)
        if stage == 'before-install':
            specs = [p['package'] + '=' + p['version'] for p in lock['packages']]
            command.docker_run('apt-install-locked', ['exec', '-e', 'DEBIAN_FRONTEND=noninteractive', name,
                               'apt-get', *APT_OPTIONS, '--yes', '--no-install-recommends', '--no-download', 'install', *specs])
    command.docker_run('copy-frozen-source', ['cp', str(frozen), name + ':/work/source'])


def freeze_sources(output, lock_path):
    frozen = output / 'source'
    frozen.mkdir()
    pairs = [(REPO / NATIVE / name, frozen / name) for name in NATIVE_SOURCES]
    pairs += [(REPO / JAVA, frozen / JAVA.name), (REPO / INSPECTOR, frozen / INSPECTOR.name), (lock_path, frozen / 'dependencies.json'),
              (Path(__file__), frozen / 'checkpoint_storage_build.py')]
    hashes = {}
    for source, target in pairs:
        checked_path(source)
        before = sha256(source)
        shutil.copyfile(source, target)
        if sha256(source) != before or sha256(target) != before:
            raise RuntimeError('Source changed while freezing: ' + source.name)
        hashes[str(source.relative_to(REPO))] = before
    return frozen, hashes


def build(args):
    endpoint = local_endpoint(args.docker_host)
    output = checked_path(args.output, exists=False)
    jobs = checked_path(REPO / 'jobs', directory=True, exists=False)
    if (jobs.exists() and not jobs.is_dir()) or jobs not in output.parents or output.exists():
        raise ValueError('Output must be a new directory under repository jobs/')
    java_home = checked_path(args.java_home, directory=True)
    docker = executable(args.docker)
    java_tools = {tool: checked_path(java_home / 'bin' / tool) for tool in ('java', 'javac', 'jar')}
    lock_path = checked_path(REPO / NATIVE / 'dependencies.json')
    lock = load_lock(lock_path)
    output.mkdir(parents=True)
    command = Commands(output, endpoint, docker, args.timeout_seconds)
    evidence = {'schemaVersion': 1, 'status': 'failed', 'imageId': IMAGE_ID,
                'imageDigest': IMAGE_DIGEST, 'platform': PLATFORM, 'files': {}, 'commands': command.receipts,
                'dependencyLockSha256': sha256(lock_path), 'compilerFlags': list(FLAGS),
                'linkage': 'static libfuse3; dynamic runtime libc', 'javaRelease': 17,
                'dockerEndpoint': endpoint, 'cleanup': [], 'networkDisconnectedBeforeSourceCopy': False,
                'networkDisconnectedBeforePackageInstall': False, 'containerImageIdentities': []}
    owned = []
    failure = None
    try:
        frozen, evidence['sourceHashes'] = freeze_sources(output, lock_path)
        inspected = ensure_image(command)
        evidence['imageId'] = inspected['Id']
        evidence['imageInspection'] = {'Id': inspected['Id'], 'Os': inspected['Os'],
                                       'Architecture': inspected['Architecture'], 'RepoDigests': inspected['RepoDigests']}
        store = json.loads(command.docker_run('image-store-backend', ['info', '--format',
                            '{"Driver":{{json .Driver}},"DriverStatus":{{json .DriverStatus}}}']))
        statuses = store.get('DriverStatus') or []
        driver_type = [row[1] for row in statuses if isinstance(row, list) and len(row) == 2 and row[0] == 'driver-type']
        if not isinstance(store.get('Driver'), str) or not store['Driver'] or len(driver_type) > 1:
            raise RuntimeError('Docker image-store backend is unconfirmed')
        evidence['imageIdentity'] = {'declaredConfigId': IMAGE_ID, 'inspectedImageId': inspected['Id'],
                'repoDigests': inspected['RepoDigests'], 'matchForm': 'config-id' if inspected['Id'] == IMAGE_ID else 'repo-digest',
                'storageDriver': store['Driver'], 'driverType': driver_type[0] if driver_type else None}
        name = 'checkpoint-fuse-build-' + uuid.uuid4().hex[:16]
        # Record ownership before create: an interrupted response can still create it.
        owned.append(name)
        command.docker_run('create-builder', ['create', '--name', name, '--label', LABEL + '=' + name,
                           '--network', 'bridge', '--user', '0:0', '--security-opt', 'no-new-privileges:true',
                           '--platform', PLATFORM, '--entrypoint', '/bin/sleep', inspected['Id'], str(args.timeout_seconds + 120)])
        evidence['containerImageIdentities'].append(command.bind_owned(name, inspected))
        command.docker_run('start-builder', ['start', name])
        command.docker_run('prepare-package-area', ['exec', name, 'mkdir', '-p', '/work/debs', '/work/receipts', '/work/bundle'])
        command.docker_run('apt-source', ['exec', '-i', name, '/bin/sh', '-c', 'cat > /work/ubuntu.sources'], stdin=SOURCES)
        command.docker_run('apt-update', ['exec', name, 'apt-get', *APT_OPTIONS, 'update'])
        evidence['signedIndexReceipt'] = command.docker_run('signed-index-hashes', ['exec', name, '/bin/sh', '-c',
                'sha256sum /usr/share/keyrings/ubuntu-archive-keyring.gpg /var/lib/apt/lists/*InRelease']).strip()
        specs = [p['package'] + '=' + p['version'] for p in lock['packages']]
        command.docker_run('apt-download', ['exec', name, 'apt-get', *APT_OPTIONS,
                           '--yes', '--no-install-recommends', '--download-only', 'install', *specs])
        receipt_script = '''set -eu
for file in /var/cache/apt/archives/*.deb; do
  package=$(dpkg-deb -f "$file" Package)
  version=$(dpkg-deb -f "$file" Version)
  architecture=$(dpkg-deb -f "$file" Architecture)
  size=$(stat -c %s "$file")
  digest=$(sha256sum "$file"); digest=${digest%% *}
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' "$package" "$version" "$architecture" "$size" "$digest"
  cp "$file" /work/debs/
done
'''
        packages = command.docker_run('package-sha256', ['exec', name, '/bin/sh', '-c', receipt_script])
        validate_package_receipts(packages, lock)
        evidence['downloadedPackageCount'] = len(lock['packages'])
        command.docker_run('copy-public-packages', ['cp', name + ':/work/debs', str(output / 'packages')])
        install_verified_packages(command, name, lock, packages, output / 'packages', frozen)
        evidence['networkDisconnectedBeforePackageInstall'] = True
        evidence['networkDisconnectedBeforeSourceCopy'] = True
        compile_script = '''set -eu
cd /work/source
gcc --version > /work/receipts/compiler.txt
pkg-config --modversion fuse3 > /work/receipts/libfuse-version.txt
pkg-config --cflags fuse3 > /work/receipts/fuse-cflags.txt
pkg-config --static --libs fuse3 > /work/receipts/fuse-static-libs.txt
gcc FLAGS $(pkg-config --cflags fuse3) core.c jail.c checkpoint_fuse.c -Wl,-Bstatic $(pkg-config --static --libs fuse3) -Wl,-Bdynamic -o /work/bundle/checkpoint-fuse
gcc FLAGS core.c jail.c selftest.c -o /work/bundle/checkpoint-fuse-selftest
readelf -d /work/bundle/checkpoint-fuse > /work/receipts/daemon-dynamic.txt
readelf --version-info /work/bundle/checkpoint-fuse > /work/receipts/daemon-symbol-versions.txt
readelf -d /work/bundle/checkpoint-fuse-selftest > /work/receipts/selftest-dynamic.txt
/work/bundle/checkpoint-fuse-selftest > /work/receipts/native-selftest.json
/work/bundle/checkpoint-fuse --version > /work/receipts/helper-version.txt
'''.replace('FLAGS', ' '.join(FLAGS))
        (frozen / 'compile.sh').write_text(compile_script)
        command.docker_run('compile-native', ['exec', name, '/bin/sh', '-c', compile_script])
        command.docker_run('copy-native-bundle', ['cp', name + ':/work/bundle', str(output / 'bundle')])
        command.docker_run('copy-native-receipts', ['cp', name + ':/work/receipts', str(output / 'native-receipts')])
        receipts = output / 'native-receipts'
        evidence['neededLibraries'] = validate_elf((receipts / 'daemon-dynamic.txt').read_text())
        validate_elf((receipts / 'selftest-dynamic.txt').read_text())
        evidence['compiler'] = (receipts / 'compiler.txt').read_text().strip()
        evidence['libfuseVersion'] = (receipts / 'libfuse-version.txt').read_text().strip()
        evidence['fuseStaticLibraries'] = (receipts / 'fuse-static-libs.txt').read_text().strip()
        evidence['helperVersion'] = (receipts / 'helper-version.txt').read_text().strip()
        # Classpath and sourcepath are explicit empty directories; no ambient sources.
        classes, empty = output / 'java-classes', output / 'empty-sourcepath'
        classes.mkdir()
        empty.mkdir()
        evidence['javac'] = command.run('javac-version', [str(java_tools['javac']), '-version']).strip()
        if not re.fullmatch(r'javac 21(?:\.[0-9A-Za-z.+_-]+)?', evidence['javac']):
            raise RuntimeError('Use JDK21 for the standalone Java probe build')
        command.run('compile-java', [str(java_tools['javac']), '--release', '17', '-encoding', 'UTF-8',
                    '-classpath', str(classes), '-sourcepath', str(empty), '-d', str(classes), str(frozen / JAVA.name), str(frozen / INSPECTOR.name)])
        command.run('package-java', [str(java_tools['jar']), '--create', '--file', str(output / 'bundle/probe.jar'),
                    '--main-class', MAIN, '--date=2026-01-01T00:00:00Z', '-C', str(classes), '.'])
        # Run native binaries with the pristine runtime, proving the builder's libc
        # upgrade did not add a runtime dependency. This container has no network.
        smoke = 'checkpoint-fuse-smoke-' + uuid.uuid4().hex[:16]
        owned.append(smoke)
        command.docker_run('create-smoke', ['create', '--name', smoke, '--label', LABEL + '=' + smoke,
                           '--network', 'none', '--cap-drop', 'ALL', '--user', '0:0',
                           '--security-opt', 'no-new-privileges:true', '--platform', PLATFORM,
                           '--entrypoint', '/bin/sleep', inspected['Id'], '120'])
        evidence['containerImageIdentities'].append(command.bind_owned(smoke, inspected))
        command.docker_run('start-smoke', ['start', smoke])
        command.docker_run('copy-smoke-bundle', ['cp', str(output / 'bundle'), smoke + ':/bundle'])
        evidence['pristineNativeSelftest'] = command.docker_run('pristine-native-selftest', ['exec', smoke, '/bundle/checkpoint-fuse-selftest']).strip()
        evidence['pristineHelperVersion'] = command.docker_run('pristine-helper-version', ['exec', smoke, '/bundle/checkpoint-fuse', '--version']).strip()
        evidence['runtimeJava'] = command.docker_run('runtime-java-version', ['exec', smoke, '/bin/sh', '-c', 'java -version 2>&1']).strip()
        command.docker_run('java-smoke-directories', ['exec', smoke, 'mkdir', '-m', '777', '/tmp/io-view', '/tmp/io-evidence'])
        java_smoke = command.docker_run('pristine-java-probe', ['exec', '-i', '--user', 'flink', smoke,
                'java', '-jar', '/bundle/probe.jar', '--root', '/tmp/io-view', '--raw-root', '/tmp/io-raw',
                '--evidence-root', '/tmp/io-evidence', '--timeout-seconds', '20'], stdin='1\thello\n2\tquit\n', limit=30)
        evidence['pristineJavaProbe'] = [json.loads(line) for line in java_smoke.splitlines()]
        if (len(evidence['pristineJavaProbe']) != 2 or any(p.get('status') != 'ok' for p in evidence['pristineJavaProbe'])
                or [p.get('operation') for p in evidence['pristineJavaProbe']] != ['hello', 'quit']):
            raise RuntimeError('Probe did not complete the pristine-runtime hello/quit smoke')
        bundle = output / 'bundle'
        evidence['files'] = {p.name: sha256(checked_path(p)) for p in sorted(bundle.iterdir())}
        if set(evidence['files']) != {'checkpoint-fuse', 'checkpoint-fuse-selftest', 'probe.jar'}:
            raise RuntimeError('Unexpected helper bundle contents')
        evidence['status'] = 'built'
    except BaseException as error:
        failure = error
        evidence['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        for name in reversed(owned):
            try:
                command.remove_owned(name)
                evidence['cleanup'].append({'container': name, 'status': 'removed'})
            except BaseException as error:
                evidence['cleanup'].append({'container': name, 'status': 'failed', 'error': str(error)})
                evidence['status'] = 'failed'
                if failure is None:
                    failure = error
        (output / 'build-evidence.json').write_text(json.dumps(evidence, indent=2, sort_keys=True) + '\n')
    if failure is not None:
        raise failure
    return evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--java-home', required=True, type=Path)
    parser.add_argument('--docker-host', default='unix:///var/run/docker.sock')
    parser.add_argument('--docker', default='/usr/local/bin/docker', type=Path)
    parser.add_argument('--timeout-seconds', default=900, type=int)
    args = parser.parse_args(argv)
    if not 60 <= args.timeout_seconds <= 1800:
        parser.error('--timeout-seconds must be 60..1800')
    try:
        result = build(args)
    except (OSError, ValueError, RuntimeError, TimeoutError, KeyboardInterrupt) as error:
        print('Helper build failed: ' + str(error), file=sys.stderr)
        return 1
    print(json.dumps({'status': result['status'], 'files': result['files']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
