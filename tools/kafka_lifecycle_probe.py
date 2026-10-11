#!/usr/bin/env python3
"""Run one explicitly pinned, externally supervised Kafka lifecycle probe. No retries."""
import argparse
import ctypes
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

from kafka_lifecycle_inputs import ordinary_path, resolve_classpath, validate_output

CASES = ('default-apache', 'custom-apache', 'generic-apache', 'generic-confluent')
MAIN = 'org.savonitar.flink.stability.testcontainers.KafkaGracefulStopDockerTest'
LABEL = 'org.savonitar.kafka-lifecycle'
ID = re.compile(r'[0-9a-f]{64}\Z')
IMAGE_ID = re.compile(r'sha256:[0-9a-f]{64}\Z')
LIMITS = {'preflight': 60, 'startup': 120, 'observation': 120, 'cleanup': 60, 'overall': 360}
IMAGE_FORMAT = '{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}},"Variant":{{json (index . "Variant")}},"RepoDigests":{{json .RepoDigests}}}'
CONTAINER_FORMAT = ('{"Id":{{json .Id}},"Image":{{json .Image}},'
    '"Session":{{json (index .Config.Labels "' + LABEL + '")}},'
    '"Case":{{json (index .Config.Labels "' + LABEL + '.case")}},'
    '"Networks":{{json .NetworkSettings.Networks}},"NetworkMode":{{json .HostConfig.NetworkMode}},'
    '"Binds":{{json .HostConfig.Binds}},"Running":{{json .State.Running}},"Status":{{json .State.Status}}}')
NETWORK_FORMAT = ('{"Id":{{json .Id}},"Session":{{json (index .Labels "' + LABEL + '")}},'
    '"Case":{{json (index .Labels "' + LABEL + '.case")}},"Containers":{{json .Containers}}}')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def object_json(text):
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, 'Duplicate JSON field: ' + key)
            result[key] = value
        return result
    value = json.loads(text, object_pairs_hook=pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError('Nonfinite JSON number')))
    require(isinstance(value, dict), 'Expected a JSON object')
    return value


def receipt(path):
    ordinary_path(path)
    require(path.is_file() and not path.is_symlink(), 'Missing or linked receipt: ' + path.name)
    return object_json(path.read_text())


def write(path, value):
    with path.open('x') as output:
        json.dump(value, output, indent=2, allow_nan=False)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())


def publish(path, value):
    """Process identity must be complete and durable before execution is authorized."""
    temporary = path.with_name(path.name + '.partial')
    write(temporary, value)
    require(not path.exists(), 'Refusing to overwrite process evidence')
    os.rename(temporary, path)


def digest(path, check=lambda: None):
    check()
    require(path.is_file() and not path.is_symlink(), 'Expected an ordinary input file: ' + str(path))
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            check()
            result.update(block)
    check()
    return result.hexdigest()


def build_files(classpath, java, check=lambda: None):
    """Record the actual launcher bytes; this does not prove their compilation provenance."""
    files = {str(java): digest(java, check)}
    directories = {}
    entries = classpath.split(os.pathsep)
    require(all(entries), 'Empty classpath entries are not permitted')
    for entry in entries:
        check()
        path = Path(entry)
        require(path.is_absolute() and not path.is_symlink(), 'Use explicit absolute classpath entries')
        if path.is_dir():
            members = {}
            for member in path.rglob('*'):
                check()
                require(not member.is_symlink(), 'Linked classpath member is not permitted')
                if member.is_file():
                    members[str(member.relative_to(path))] = digest(member, check)
                    files[str(member)] = members[str(member.relative_to(path))]
            require(bool(members), 'Empty compiled classpath directory')
            directories[str(path)] = members
        else:
            files[str(path)] = digest(path, check)
    return {'classpath': entries, 'files': files, 'directories': directories}


def java_command(args, session, network, evidence):
    properties = dict(zip(('case', 'image', 'imageId', 'platform', 'digest', 'session', 'networkId', 'output'),
        (args.case, args.image, args.image_id, args.platform, args.digest, session, network, str(evidence))))
    return [str(args.java), '-ea', '-Dapi.version=1.44', '-Dflink.kafka.lifecycle=true',
        *['-Dflink.kafka.lifecycle.' + key + '=' + value for key, value in properties.items()],
        '-cp', args.classpath, MAIN]


def required_evidence(directory, case, container, image, check=lambda: None):
    check()
    names = ['startup-submitted.json', 'image.json', 'ready.json', 'created-launch.json', 'host-top.json',
        'namespace-pid-one.txt', 'selected-broker.properties', 'term-submitted.json', 'term-returned.json',
        'states.jsonl', 'kafka-final.log', 'kafka-continuous.log', 'result.json', 'observation-ended.json',
        'cleanup.json', 'safety-cleanup.json', 'log-evidence.json', 'broker-config.raw', 'broker-config.transfer.json']
    inspections = ['pid1-status', 'pid1-stat', 'pid1-cmdline', 'pid1-namespace']
    for name in inspections:
        names.extend([name + '.stdout', name + '.stderr', name + '.command.json'])
    if case in ('default-apache', 'custom-apache'):
        names.extend(['actual-starter.sh', 'starter.transfer.json'])
    if case != 'default-apache':
        names.append('runtime-evidence.json')
    require(not list(directory.glob('*.partial')), 'Incomplete atomic evidence publication')
    parsed = {}
    for name in names:
        check()
        path = directory / name
        require(path.is_file() and not path.is_symlink(), 'Missing required evidence: ' + name)
        require(path.stat().st_size > 0 or name.endswith('.stderr'), 'Empty required evidence: ' + name)
        if name.endswith('.json'):
            parsed[name] = receipt(path)
            check()
    for name in inspections:
        value = parsed[name + '.command.json']
        require(value.get('status') == 'completed' and type(value.get('exitCode')) is int
            and value['exitCode'] == 0, 'Incomplete PID inspection: ' + name)
    for name in ['broker-config.transfer.json'] + (['starter.transfer.json'] if case in CASES[:2] else []):
        require(parsed[name].get('status') == 'completed', 'Incomplete file transfer: ' + name)
    result, cleanup, marker = (parsed[name] for name in ['result.json', 'cleanup.json', 'log-evidence.json'])
    require(result.get('accepted') is True and result.get('shutdownConfirmedBeforeCleanup') is True
        and type(result.get('exitCode')) is int and result.get('containerId') == container,
        'Shutdown observation was not accepted before cleanup')
    require(parsed['observation-ended.json'].get('accepted') is True
        and parsed['observation-ended.json'].get('containerId') == container, 'Observation boundary differs')
    require(cleanup.get('ownedContainerRemoved') is True and cleanup.get('containerId') == container,
        'Fixture cleanup was not confirmed')
    safety = parsed['safety-cleanup.json']
    require(safety.get('authorizationReceived') is True and safety.get('safetyFallback') is False
        and safety.get('runtimeCleanupCompleted') is True and safety.get('callbackCleanupCompleted') is True,
        'Safety fallback cannot substitute for authorized successful cleanup')
    for key, expected in {'complete': True, 'healthyAfterCleanupAndClose': True, 'naturalCompletion': True,
            'transportFailed': False, 'cancelled': False}.items():
        require(marker.get(key) is expected, 'Continuous capture postcondition failed: ' + key)
    require(parsed['image.json'].get('id') == image and parsed['ready.json'].get('imageId') == image
        and parsed['ready.json'].get('containerId') == container, 'Image/container evidence differs')
    for name in ['term-submitted.json', 'term-returned.json']:
        require(parsed[name].get('containerId') == container, 'TERM target differs')
    final = None
    with (directory / 'states.jsonl').open() as states:
        for line in states:
            check()
            if line.strip():
                final = object_json(line)
    require(final is not None, 'Missing post-TERM states')
    require(final.get('containerId') == container and final.get('imageId') == image, 'Final state identity differs')
    state = final.get('state', {})
    require(state.get('Running') is False and state.get('Pid') == 0 and state.get('OOMKilled') is False
        and type(state.get('ExitCode')) is int and state['ExitCode'] == result['exitCode'],
        'No stopped non-OOM state before cleanup')
    return {name: digest(directory / name, check) for name in names}


class System:
    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(time.sleep)
    run = staticmethod(subprocess.run)
    popen = staticmethod(subprocess.Popen)

    @staticmethod
    def process_identity(pid, timeout=2):
        """Use kernel start identity, never second-resolution ps lstart or a PID alone."""
        require(type(pid) is int and pid > 0, 'Invalid process ID')
        if sys.platform.startswith('linux'):
            try:
                raw = Path('/proc', str(pid), 'stat').read_text()
            except FileNotFoundError:
                return None
            fields = raw[raw.rindex(')') + 2:].split()
            # Missing boot identity is uncertainty, not proof that this PID is absent.
            boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            require(bool(re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', boot)),
                'Kernel boot identity unavailable')
            group, start = int(fields[2]), int(fields[19])
            require(group > 0 and start > 0, 'Invalid kernel process identity')
            return {'pid': pid, 'pgid': group, 'kind': 'linux-start-ticks', 'start': start, 'boot': boot}
        try:
            if sys.platform == 'darwin':
                class BsdInfo(ctypes.Structure):
                    _fields_ = [(name, ctypes.c_uint32) for name in
                        ['flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid', 'ruid', 'rgid', 'svuid', 'svgid', 'reserved']]
                    _fields_ += [('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32)]
                    _fields_ += [(name, ctypes.c_uint32) for name in ['nfiles', 'pgid', 'jobc', 'tdev', 'tpgid', 'nice']]
                    _fields_ += [('startSeconds', ctypes.c_uint64), ('startMicroseconds', ctypes.c_uint64)]
                library = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
                call = library.proc_pidinfo
                call.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
                call.restype = ctypes.c_int
                value = BsdInfo()
                size = call(pid, 3, 0, ctypes.byref(value), ctypes.sizeof(value))
                if size == 0:
                    os.kill(pid, 0)  # Existence only; no signal delivery.
                    raise RuntimeError('Kernel process-start identity unavailable')
                require(size == ctypes.sizeof(value) == 136 and value.pid == pid and value.startSeconds > 0
                    and value.startMicroseconds < 1000000, 'Unexpected libproc process identity')
                return {'pid': pid, 'pgid': value.pgid, 'kind': 'darwin-start-timeval',
                    'start': [value.startSeconds, value.startMicroseconds]}
            raise RuntimeError('Strong process identity unsupported on this platform')
        except ProcessLookupError:
            return None

    @staticmethod
    def group_members(pgid, timeout=2):
        result = subprocess.run(['/bin/ps', '-axo', 'pid=,pgid='], text=True, capture_output=True, timeout=timeout)
        require(result.returncode == 0, 'Process-group inventory failed')
        rows = [line.split() for line in result.stdout.splitlines() if line.strip()]
        require(all(len(row) == 2 and all(x.isdigit() for x in row) for row in rows), 'Malformed process-group inventory')
        return {int(pid) for pid, group in rows if int(group) == pgid}

    @staticmethod
    def signal_identity(identity, signum, timeout=2):
        require(System.process_identity(identity['pid'], timeout) == identity, 'Process-start identity changed')
        require(identity['pgid'] == identity['pid'], 'Recorded child is not a group leader')
        os.killpg(identity['pgid'], signum)

    @staticmethod
    def signal_group(process, signum):
        if process.poll() is not None:
            return
        try:
            require(os.getpgid(process.pid) == process.pid, 'Child process-group identity changed')
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            # Popen.poll reaps this child; disappearance alone is not proof of exit.
            if process.poll() is None:
                raise


class Supervisor:
    def __init__(self, args, system=None):
        self.args, self.system = args, system or System()
        self.output = args.output
        self.evidence = self.output / 'evidence'
        self.session = 'kafka-lifecycle-' + uuid.uuid4().hex
        self.start = self.system.monotonic()
        self.overall = self.start + LIMITS['overall']
        self.deadline = self.start + LIMITS['preflight']
        self.phase = 'preflight'
        self.child = None
        self.processes = {'preflight': None, 'java': None}
        self.process_records = {}
        self.networks, self.containers = set(), set()
        self.candidate_containers = set()
        self.network = None
        self.boundary = None
        self.creation_attempted = False
        self.baseline = None
        self.identity = None
        self.cleanup_deadline = None
        self.first_failure = None
        self.cleanup_errors = []
        self.commands = 0
        self.phases = [{'phase': 'preflight', 'elapsedSeconds': 0}]
        self.forced_removals = []
        self.env = dict(os.environ)
        self.env.pop('DOCKER_AUTH_CONFIG', None)
        self.env.update(DOCKER_HOST=args.docker_host, DOCKER_API_VERSION='1.44',
            DOCKER_CONFIG=str(self.output / 'docker-config'), TESTCONTAINERS_RYUK_DISABLED='true',
            TESTCONTAINERS_CHECKS_DISABLE='true')

    def remaining(self, maximum=10):
        remaining = min(self.deadline, self.overall) - self.system.monotonic()
        if remaining <= 0:
            raise TimeoutError(self.phase + ' deadline exhausted')
        return min(maximum, remaining)

    def failure(self, error):
        if self.first_failure is None:
            self.first_failure = {'phase': self.phase, 'type': type(error).__name__, 'message': str(error)}

    def cleanup_failure(self, description, error):
        self.failure(error)
        self.cleanup_errors.append(description + ': ' + repr(error))

    def command(self, argv, allow_missing=None, record=True, raw=False):
        self.commands += 1
        item = {'argv': argv, 'phase': self.phase, 'elapsedSeconds': self.system.monotonic() - self.start}
        try:
            result = self.system.run(argv, env=self.env, capture_output=True, text=not raw, timeout=self.remaining())
            stderr = result.stderr.decode('utf-8', errors='replace') if isinstance(result.stderr, bytes) else result.stderr
            item.update(returncode=result.returncode, stdout=result.stdout if record else '<identity only>',
                stderr=stderr)
            if result.returncode:
                missing = allow_missing and any(message in stderr for message in
                    ['No such container: ' + allow_missing, 'No such object: ' + allow_missing,
                     'network ' + allow_missing + ' not found', 'No such network: ' + allow_missing])
                if missing:
                    return None
                raise RuntimeError('Command failed; see commands.jsonl: ' + argv[0])
            self.remaining()
            return result.stdout
        except BaseException as error:
            item['error'] = type(error).__name__ + ': ' + str(error)
            raise
        finally:
            with (self.output / 'commands.jsonl').open('a') as stream:
                stream.write(json.dumps(item) + '\n')

    def docker(self, *args, **kwargs):
        return self.command([self.args.docker, '--host', self.args.docker_host, *args], **kwargs)

    def ids(self, kind, labelled=False):
        filters = ['--filter', 'label=' + LABEL + '=' + self.session,
            '--filter', 'label=' + LABEL + '.case=' + self.args.case] if labelled else []
        text = self.docker(*(['ps', '-q' if kind == 'running' else '-aq', '--no-trunc'] if kind in ('containers', 'running') else
            ['network', 'ls', '-q', '--no-trunc']), *filters)
        values = text.split()
        require(all(ID.fullmatch(value) for value in values) and len(values) == len(set(values)), 'Malformed resource inventory')
        return set(values)

    def inventory(self):
        return {'containers': sorted(self.ids('containers')), 'networks': sorted(self.ids('networks')),
            'running': sorted(self.ids('running'))}

    def source_build_identity(self):
        inputs = None
        if hasattr(self, 'inputs'):
            inputs = resolve_classpath(self.args.source_root, classpath=self.input_classpath,
                surefire_report=getattr(self.args, 'surefire_report', None), check=self.remaining)
            require(inputs == self.inputs, 'Launcher source snapshot, report, or input inventory changed')
        def git(*args, raw=False):
            value = self.command(['git', '-C', str(self.args.source_root), *args], record=False, raw=raw)
            return value if raw else value.strip()
        source = {key: git(*args) for key, args in {'head': ('rev-parse', 'HEAD'),
            'tree': ('rev-parse', 'HEAD^{tree}'), 'status': ('status', '--porcelain', '--untracked-files=all')}.items()}
        source['diffSha256'] = hashlib.sha256(git('diff', 'HEAD', '--binary', raw=True)).hexdigest()
        source['root'] = str(self.args.source_root)
        untracked = [os.fsdecode(name) for name in git('ls-files', '--others', '--exclude-standard', '-z', raw=True).split(b'\0') if name]
        source['untrackedFiles'] = {name: digest(self.args.source_root / name, self.remaining) for name in untracked}
        return {'source': source, 'build': build_files(self.args.classpath, self.args.java, self.remaining), 'inputs': inputs,
            'compilationProvenanceClaimed': False}

    def reap(self):
        if self.child is None or self.child.poll() is not None:
            return
        self.system.signal_group(self.child, signal.SIGTERM)
        try:
            self.child.wait(timeout=self.remaining(2))
        except subprocess.TimeoutExpired:
            self.system.signal_group(self.child, signal.SIGKILL)
            self.child.wait(timeout=self.remaining(2))
        require(self.child.poll() is not None, 'Owned Java process was not reaped')

    def java(self, argv, prefix, wait=False):
        argv = [argv[0], '-Duser.home=' + str(self.output / 'java-home'),
            '-Djava.io.tmpdir=' + str(self.output / 'tmp'), *argv[1:]]
        with (self.output / (prefix + '.stdout')).open('x') as stdout, (self.output / (prefix + '.stderr')).open('x') as stderr:
            self.remaining()
            intent = {'version': 1, 'session': self.session, 'kind': prefix, 'nonce': uuid.uuid4().hex,
                'argv': argv, 'supervisor': self.supervisor_identity}
            intent_path = self.output / (prefix + '-launch-intent.json')
            write(intent_path, intent)
            bootstrap = [sys.executable, '-B', str(Path(__file__).resolve()), '--_child-start', str(intent_path)]
            self.child = self.system.popen(bootstrap, env=self.env, stdout=stdout, stderr=stderr, start_new_session=True)
            self.processes[prefix] = self.child
            record_path = self.output / (prefix + '-process.json')
            while not record_path.exists():
                require(self.child.poll() is None, 'Child bootstrap exited before durable process identity')
                self.system.sleep(min(.02, self.remaining()))
            record = receipt(record_path)
            require(record['nonce'] == intent['nonce'] and record['identity']['pid'] == self.child.pid
                and record['identity']['pgid'] == self.child.pid
                and self.system.process_identity(self.child.pid, self.remaining(2)) == record['identity'],
                'Child process-start identity differs')
            self.process_records[prefix] = record
            publish(self.output / (prefix + '-exec-authorized.json'), {'nonce': intent['nonce']})
            if wait:
                self.child.wait(timeout=self.remaining(30))
                require(self.child.returncode == 0, 'Exact-classpath evidence preflight failed')
            else:
                self.observe()

    def resolve_image(self):
        value = object_json(self.docker('image', 'inspect', self.args.image, '--format', IMAGE_FORMAT))
        observed = '/'.join([value.get('Os', ''), value.get('Architecture', ''), *([value['Variant']] if value.get('Variant') else [])])
        require(value.get('Id') == self.args.image_id and observed == self.args.platform, 'Selected image ID/platform differs')
        require(isinstance(value.get('RepoDigests'), list) and value['RepoDigests'].count(self.args.digest) == 1,
            'Explicit digest not uniquely present on selected image')
        write(self.output / 'selected-image.json', {'reference': self.args.image, 'requestedDigest': self.args.digest, 'resolved': value})

    def inspect_network(self, nid):
        require(ID.fullmatch(nid) and nid not in self.baseline['networks'], 'Protected or invalid network ID')
        text = self.docker('network', 'inspect', nid, '--format', NETWORK_FORMAT, allow_missing=nid)
        if text is None:
            return None
        value = object_json(text)
        require(value.get('Id') == nid and value.get('Session') == self.session and value.get('Case') == self.args.case,
            'Network ownership differs')
        require(isinstance(value.get('Containers'), dict), 'Missing network attachments')
        self.networks.add(nid)
        return value

    def inspect_container(self, cid):
        require(ID.fullmatch(cid) and cid not in self.baseline['containers'], 'Protected or invalid container ID')
        text = self.docker('inspect', '--type', 'container', cid, '--format', CONTAINER_FORMAT, allow_missing=cid)
        if text is None:
            return None
        value = object_json(text)
        require(value.get('Id') == cid and value.get('Image') == self.args.image_id
            and value.get('Session') == self.session and value.get('Case') == self.args.case, 'Container ownership differs')
        require(value.get('Binds') in (None, []) and 'Binds' in value, 'Unexpected container bind mounts')
        attached = value.get('Networks')
        require(isinstance(attached, dict), 'Missing container networks')
        membership = any(isinstance(net, dict) and net.get('NetworkID') in self.networks
            and 'kafka-lifecycle' in (net.get('Aliases') or []) for net in attached.values())
        detached = value.get('Running') is False and value.get('Status') in ('created', 'exited', 'dead') \
            and value.get('NetworkMode') in self.networks and not attached
        require(membership or detached, 'Container network membership differs')
        self.containers.add(cid)
        return value

    def begin_cleanup(self):
        if self.cleanup_deadline is None:
            self.cleanup_deadline = min(self.system.monotonic() + 60, self.overall)
            self.phase = 'cleanup'
            self.phases.append({'phase': self.phase, 'elapsedSeconds': self.system.monotonic() - self.start})
        self.deadline = self.cleanup_deadline

    def observe(self):
        while True:
            self.remaining()
            ready = self.evidence / 'ready.json'
            if not self.candidate_containers and ready.exists():
                value = receipt(ready)
                cid = value.get('containerId')
                require(isinstance(cid, str) and ID.fullmatch(cid), 'Malformed ready container ID')
                self.candidate_containers.add(cid)
                write(self.output / 'ready-observed.json', {'containerId': cid, 'ownershipProven': False})
                require(self.inspect_container(cid) is not None, 'Ready container disappeared before ownership inspection')
            ended = self.evidence / 'observation-ended.json'
            if ended.exists():
                value = receipt(ended)
                require(type(value.get('accepted')) is bool and isinstance(value.get('containerId'), str), 'Malformed observation boundary')
                self.begin_cleanup()
                require(self.inspect_network(self.network) is not None, 'Owned network disappeared before cleanup handshake')
                cid = value['containerId']
                if cid != 'null':
                    require(self.inspect_container(cid) is not None or value['accepted'] is False, 'Accepted container disappeared before cleanup')
                else:
                    require(value['accepted'] is False, 'Accepted observation has no container')
                self.boundary = value
                write(self.output / 'boundary-observed.json', {'elapsedSeconds': self.system.monotonic() - self.start, 'observation': value})
                with (self.evidence / 'cleanup-authorized').open('x') as token:
                    token.write(self.session + '\n'); token.flush(); os.fsync(token.fileno())
                if value['accepted'] is not True:
                    self.failure(ValueError('Fixture observation was unaccepted'))
                # Leave time to reap and reconcile if the fixture cannot clean itself.
                self.child.wait(timeout=max(0.001, self.remaining(60) - 10))
                require(self.child.returncode == 0, 'Lifecycle Java returned nonzero')
                return
            if self.child.poll() is not None:
                raise RuntimeError('Java exited without the observation/cleanup handshake')
            term = self.evidence / 'term-submitted.json'
            if self.phase == 'startup' and term.exists():
                value = receipt(term)
                require(isinstance(value.get('containerId'), str) and ID.fullmatch(value['containerId']), 'Malformed TERM receipt')
                self.phase = 'observation'
                self.deadline = min(self.system.monotonic() + 120, self.overall - 60)
                self.phases.append({'phase': self.phase, 'elapsedSeconds': self.system.monotonic() - self.start})
            self.system.sleep(min(.05, self.remaining()))

    def cleanup(self):
        self.begin_cleanup()
        try:
            self.reap()
        except BaseException as error:
            self.cleanup_failure('Child reaping', error)
        if self.child is not None and self.child.poll() is None:
            self.cleanup_failure('Resource deletion refused', RuntimeError('Java remains alive'))
            return
        for record in self.process_records.values():
            try:
                require(not self.system.group_members(record['identity']['pgid'], self.remaining(2)),
                    'Owned process group still has members; resource deletion refused')
            except BaseException as error:
                self.cleanup_failure('Process-group absence', error)
                return
        if self.creation_attempted:
            try:
                # A timeout or malformed create reply can still leave a labelled resource.
                for nid in sorted(self.ids('networks', True)):
                    self.inspect_network(nid)
                for cid in sorted(self.ids('containers', True) | self.containers | self.candidate_containers):
                    if self.inspect_container(cid) is not None:
                        self.failure(RuntimeError('Fixture left a container; forced removal is cleanup, not STOP'))
                        self.forced_removals.append(cid)
                        self.docker('rm', '--force', '--volumes', cid)
                for cid in sorted(self.containers):
                    require(self.inspect_container(cid) is None, 'Named owned container still exists')
                for nid in sorted(self.networks):
                    value = self.inspect_network(nid)
                    if value is not None:
                        require(not value['Containers'], 'Owned network has attachments; removal refused')
                        self.docker('network', 'rm', nid)
                for nid in sorted(self.networks):
                    require(self.inspect_network(nid) is None, 'Named owned network still exists')
                require(not self.ids('containers', True) and not self.ids('networks', True), 'Labelled resources remain')
            except BaseException as error:
                self.cleanup_failure('Resource cleanup', error)
        if self.baseline is not None:
            try:
                final = self.inventory()
                write(self.output / 'postflight.json', final)
                require(not final['running'], 'Running containers remain after cleanup')
                require(final == self.baseline, 'Inventory differs: protected resource missing or unexplained resource remains')
            except BaseException as error:
                self.cleanup_failure('Independent absence/preservation check', error)

    def execute(self):
        self.input_classpath = self.args.classpath
        self.inputs = resolve_classpath(self.args.source_root, classpath=self.args.classpath,
            surefire_report=getattr(self.args, 'surefire_report', None), check=self.remaining)
        self.args.classpath = self.inputs['classpath']
        policy = validate_output(self.output, self.args.source_root, self.inputs,
            run=self.system.run, check=self.remaining, protected_paths=(self.args.java,))
        # Reserving this directory is exclusive; neither preflight nor live evidence exists yet.
        self.output.mkdir(parents=False, exist_ok=False)
        with (self.output / 'supervisor.lock').open('x') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return self.execute_locked(policy)

    def execute_locked(self, policy):
        for name in ['docker-config', 'java-home', 'tmp']:
            (self.output / name).mkdir()
        write(self.output / 'docker-config/config.json', {})
        self.supervisor_identity = self.system.process_identity(os.getpid(), self.remaining(2))
        require(self.supervisor_identity is not None, 'Supervisor start identity unavailable')
        write(self.output / 'invocation.json', {'schemaVersion': 2, 'case': self.args.case, 'session': self.session,
            'startedAt': datetime.now(timezone.utc).isoformat(), 'attempts': 1, 'automaticRetry': False,
            'limitsSeconds': LIMITS, 'dockerHost': self.args.docker_host, 'dockerExecutable': self.args.docker,
            'image': {'reference': self.args.image, 'id': self.args.image_id, 'digest': self.args.digest, 'platform': self.args.platform},
            'supervisor': self.supervisor_identity, 'classpathInput': self.inputs, 'outputPolicy': policy})
        evidence_hashes = {}
        try:
            self.identity = self.source_build_identity()
            write(self.output / 'source-build.json', self.identity)
            self.java([str(self.args.java), '-ea', '-Dapi.version=1.44', '-Dflink.kafka.lifecycle=false',
                '-cp', self.args.classpath, MAIN, '--verify-evidence', str(self.output / 'serialization-check')], 'preflight', wait=True)
            for name in ['startup-submitted.json', 'ready.json', 'runtime-evidence.json']:
                receipt(self.output / 'serialization-check' / name)
            require(self.source_build_identity() == self.identity, 'Source/build changed during exact-classpath preflight')
            self.baseline = self.inventory()
            write(self.output / 'preflight.json', self.baseline)
            require(not self.baseline['running'], 'Competing running container before resource creation')
            require(not self.ids('containers', True) and not self.ids('networks', True), 'Session label collision')
            self.resolve_image()
            self.phase = 'startup'
            self.deadline = min(self.system.monotonic() + LIMITS['startup'], self.overall - LIMITS['cleanup'])
            self.phases.append({'phase': self.phase, 'elapsedSeconds': self.system.monotonic() - self.start})
            self.creation_attempted = True
            write(self.output / 'resource-creation-submitted.json', {'session': self.session})
            nid = self.docker('network', 'create', '--label', LABEL + '=' + self.session,
                '--label', LABEL + '.case=' + self.args.case, self.session).strip()
            require(ID.fullmatch(nid), 'Malformed created network ID; reconciliation required')
            self.network = nid
            write(self.output / 'network-created.json', {'id': nid})
            require(self.inspect_network(nid) is not None, 'Created network disappeared')
            require(not self.evidence.exists(), 'Java evidence destination must remain fresh')
            write(self.output / 'launch-submitted.json', {'session': self.session, 'case': self.args.case})
            self.java(java_command(self.args, self.session, nid, self.evidence), 'java')
            require(self.boundary is not None and self.boundary['accepted'] is True, 'No accepted observation boundary')
            evidence_hashes = required_evidence(self.evidence, self.args.case, self.boundary['containerId'], self.args.image_id, self.remaining)
        except BaseException as error:
            self.failure(error)
        finally:
            self.cleanup()
            if self.identity is not None:
                try:
                    require(self.source_build_identity() == self.identity, 'Source/build identity changed during invocation')
                except BaseException as error:
                    self.cleanup_failure('Final source/build check', error)
        elapsed = self.system.monotonic() - self.start
        if elapsed > 360:
            self.failure(TimeoutError('Overall deadline exhausted'))
        success = (self.first_failure is None and not self.cleanup_errors and bool(evidence_hashes)
            and self.child is not None and self.child.poll() == 0 and elapsed <= 360)
        launches = {kind: self.launch_state(kind) for kind in self.processes}
        result = {'accepted': success, 'case': self.args.case, 'session': self.session,
            'firstFailure': self.first_failure, 'cleanup': {'passed': not self.cleanup_errors,
                'errors': self.cleanup_errors, 'forcedContainerRemovals': self.forced_removals,
                'childReaped': self.child is None or self.child.poll() is not None},
            'shutdownObservationAccepted': self.boundary is not None and self.boundary['accepted'] is True,
            'preflightExitCode': self.processes['preflight'].poll() if launches['preflight'] == 'fixture-confirmed' else None,
            'lifecycleExitCode': self.processes['java'].poll() if launches['java'] == 'fixture-confirmed' else None,
            'launchStates': launches,
            'wrapperExitCodes': {key: value.poll() if value is not None else None for key, value in self.processes.items()},
            'elapsedSeconds': elapsed, 'phases': self.phases, 'evidenceHashes': evidence_hashes,
            'ownedContainerIds': sorted(self.containers), 'ownedNetworkIds': sorted(self.networks)}
        write(self.output / 'completed.json', result)
        return result

    def launch_state(self, kind):
        if self.processes[kind] is None:
            return 'not-submitted'
        if (self.output / (kind + '-exec-failed.json')).exists():
            return 'exec-failed'
        marker = self.output / ('serialization-check/startup-submitted.json' if kind == 'preflight'
            else 'evidence/startup-submitted.json')
        if marker.is_file():
            return 'fixture-confirmed'
        if (self.output / (kind + '-exec-submitted.json')).exists():
            return 'exec-submitted-unconfirmed'
        return 'bootstrap-only'


def child_start(intent_path):
    """Persist kernel identity before Java can execute; no Docker access here."""
    intent = receipt(intent_path)
    kind, nonce = intent['kind'], intent['nonce']
    require(kind in ('preflight', 'java') and re.fullmatch('[0-9a-f]{32}', nonce), 'Invalid child launch intent')
    directory = intent_path.parent
    identity = System.process_identity(os.getpid())
    require(identity is not None and identity['pid'] == identity['pgid'], 'Bootstrap must own a new process group')
    publish(directory / (kind + '-process.json'), {'nonce': nonce, 'session': intent['session'], 'identity': identity})
    deadline = time.monotonic() + 10
    authorization = directory / (kind + '-exec-authorized.json')
    while not authorization.exists():
        require(System.process_identity(intent['supervisor']['pid']) == intent['supervisor'], 'Supervisor disappeared before Java authorization')
        require(time.monotonic() < deadline, 'Java execution authorization timed out')
        time.sleep(.02)
    require(receipt(authorization).get('nonce') == nonce, 'Stale Java execution authorization')
    require(System.process_identity(intent['supervisor']['pid']) == intent['supervisor'], 'Supervisor disappeared before Java exec')
    publish(directory / (kind + '-exec-submitted.json'), {'nonce': nonce})
    try:
        os.execve(intent['argv'][0], intent['argv'], dict(os.environ))
    except BaseException as error:
        publish(directory / (kind + '-exec-failed.json'), {'nonce': nonce, 'failureType': type(error).__name__})
        raise


def stop_recorded_child(record, system, remaining):
    identity = record['identity']
    require(identity['pid'] == identity['pgid'], 'Ambiguous child process group')
    actual = system.process_identity(identity['pid'], remaining(2))
    if actual is None:
        require(not system.group_members(identity['pgid'], remaining(2)), 'Leader absent but live group members remain')
        return
    require(actual == identity, 'Recorded PID was reused or process-start identity differs')
    for signum in (signal.SIGTERM, signal.SIGKILL):
        system.signal_identity(identity, signum, remaining(2))
        end = system.monotonic() + remaining(2)
        while system.monotonic() < end:
            current = system.process_identity(identity['pid'], remaining(2))
            if current is None:
                require(not system.group_members(identity['pgid'], remaining(2)), 'Orphan descendants remain; deletion refused')
                return
            require(current == identity, 'Process identity changed while stopping orphan')
            system.sleep(min(.02, remaining()))
    raise TimeoutError('Recorded Java group did not terminate')


def reconcile(run_directory, system=None):
    """Cleanup-only recovery. Original receipts and the original verdict are immutable."""
    system = system or System()
    run_directory = ordinary_path(run_directory)
    require(run_directory.is_dir(), 'Invalid existing run directory')
    lock_path = ordinary_path(run_directory / 'supervisor.lock')
    require(lock_path.is_file(), 'Missing ordinary supervisor lock')
    outputs = run_directory / 'reconciliations'
    ordinary_path(outputs, existing=False)
    outputs.mkdir(exist_ok=True)
    output = outputs / uuid.uuid4().hex
    output.mkdir()
    start = system.monotonic()
    result = {'reconciled': False, 'originalRun': str(run_directory), 'originalVerdictModified': False,
        'deadlineSeconds': 60, 'errors': []}
    def remaining(maximum=10):
        value = start + 60 - system.monotonic()
        if value <= 0: raise TimeoutError('Reconciliation deadline exhausted')
        return min(maximum, value)
    try:
        with lock_path.open('r') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            original = receipt(run_directory / 'invocation.json')
            require(original.get('schemaVersion') == 2, 'Legacy run lacks safe reconciliation identity')
            require(re.fullmatch(r'kafka-lifecycle-[0-9a-f]{32}', original['session']) and original['case'] in CASES,
                'Invalid recorded session/case')
            require(original['dockerHost'].startswith('unix:///') and IMAGE_ID.fullmatch(original['image']['id']), 'Invalid recorded endpoint/image')
            owner = original['supervisor']
            actual = system.process_identity(owner['pid'], remaining(2))
            require(actual is None, 'Active supervisor or reused supervisor PID; reconciliation refused')
            for kind in ['preflight', 'java']:
                intent_path = run_directory / (kind + '-launch-intent.json')
                process_path = run_directory / (kind + '-process.json')
                if intent_path.exists():
                    intent, process = receipt(intent_path), receipt(process_path)
                    require(intent['session'] == process['session'] == original['session']
                        and intent['nonce'] == process['nonce'] and intent['kind'] == kind,
                        'Ambiguous or incomplete child creation records')
                    stop_recorded_child(process, system, remaining)
                else:
                    require(not process_path.exists(), 'Process record without launch intent')
            write(output / 'processes-quiescent.json', {'confirmed': True})
            if (run_directory / 'resource-creation-submitted.json').exists():
                require(receipt(run_directory / 'resource-creation-submitted.json').get('session') == original['session'],
                    'Resource creation intent belongs to another session')
                baseline = receipt(run_directory / 'preflight.json')
                require(all(isinstance(baseline.get(key), list) and all(ID.fullmatch(x) for x in baseline[key])
                    for key in ('containers', 'networks', 'running')), 'Missing protected baseline inventory')
                image = original['image']
                args = argparse.Namespace(case=original['case'], image=image['reference'], image_id=image['id'],
                    digest=image['digest'], platform=image['platform'], docker=original['dockerExecutable'],
                    docker_host=original['dockerHost'], output=output)
                recovery = Supervisor(args, system)
                recovery.session = original['session']
                recovery.start = start; recovery.overall = start + 60
                recovery.phase = 'cleanup'; recovery.deadline = start + 60; recovery.cleanup_deadline = start + 60
                recovery.baseline = baseline; recovery.creation_attempted = True
                (output / 'docker-config').mkdir(); write(output / 'docker-config/config.json', {})
                for name in ['ready-observed.json', 'evidence/ready.json', 'evidence/observation-ended.json']:
                    if (run_directory / name).exists():
                        cid = receipt(run_directory / name).get('containerId')
                        if cid != 'null':
                            require(isinstance(cid, str) and ID.fullmatch(cid), 'Malformed saved container candidate')
                            recovery.candidate_containers.add(cid)
                # Saved IDs remain candidates until the same inspect/label/network proof succeeds.
                if (run_directory / 'network-created.json').exists():
                    nid = receipt(run_directory / 'network-created.json')['id']
                    recovery.inspect_network(nid)
                recovery.cleanup()
                result['cleanupErrors'] = recovery.cleanup_errors
                result['removedContainers'] = recovery.forced_removals
                result['ownedContainerIds'] = sorted(recovery.containers)
                result['ownedNetworkIds'] = sorted(recovery.networks)
                require(not recovery.cleanup_errors, 'Reconciliation cleanup incomplete')
            else:
                require(not any((run_directory / name).exists() for name in ['java-launch-intent.json',
                    'network-created.json', 'ready-observed.json', 'launch-submitted.json', 'evidence']),
                    'Resource or lifecycle evidence without resource creation intent')
                result['noResourceCreationSubmitted'] = True
            remaining()
            result['reconciled'] = True
    except BaseException as error:
        result['errors'].append({'type': type(error).__name__, 'message': str(error)})
    result['elapsedSeconds'] = system.monotonic() - start
    write(output / 'completed.json', result)
    result['receipt'] = str(output / 'completed.json')
    return result


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument('--reconcile', type=Path)
    value.add_argument('--case', choices=CASES)
    value.add_argument('--java', type=Path)
    inputs = value.add_mutually_exclusive_group()
    inputs.add_argument('--classpath')
    inputs.add_argument('--surefire-report', type=Path)
    value.add_argument('--image')
    value.add_argument('--image-id')
    value.add_argument('--digest')
    value.add_argument('--platform')
    value.add_argument('--source-root', type=Path)
    value.add_argument('--output', type=Path)
    value.add_argument('--docker', default='docker')
    value.add_argument('--docker-host', default='unix:///var/run/docker.sock')
    return value


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) == 2 and argv[0] == '--_child-start':
        child_start(Path(argv[1]))
        return 0
    args = parser().parse_args(argv)
    if args.reconcile is None:
        require(all([args.java, args.source_root, args.output, args.case, args.image, args.image_id, args.digest, args.platform])
            and (args.classpath is not None or args.surefire_report is not None), 'Explicit launch arguments are required')
        require(args.java.is_absolute() and args.source_root.is_absolute() and args.output.is_absolute(), 'Use absolute Java/source/output paths')
        require(IMAGE_ID.fullmatch(args.image_id), 'An explicit sha256 image configuration ID is required')
        require(re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', args.digest), 'An explicit repository digest is required')
        require(re.fullmatch(r'linux/[a-z0-9_]+(?:/[a-zA-Z0-9_]+)?', args.platform), 'Use the complete observed Linux platform')
        require(args.docker_host.startswith('unix:///'), 'Only an explicit local Unix Docker endpoint is supported')
    else:
        require(not any([args.case, args.java, args.classpath, args.surefire_report, args.image, args.image_id,
            args.digest, args.platform, args.source_root, args.output]), 'Reconciliation uses recorded inputs only')
    previous = {}
    def interrupted(signum, frame):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        raise InterruptedError('Supervisor signal ' + str(signum))
    try:
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]:
            previous[signum] = signal.signal(signum, interrupted)
        result = reconcile(args.reconcile) if args.reconcile is not None else Supervisor(args).execute()
        print(json.dumps(result))
        return 0 if result.get('reconciled', result.get('accepted')) else 1
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == '__main__':
    sys.exit(main())
