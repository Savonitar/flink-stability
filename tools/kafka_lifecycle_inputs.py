"""Read-only launcher and output checks shared by both lifecycle input modes.

A matching source snapshot and successful Surefire report are prerequisites,
not proof that the compiled classes are fresh. Build from the selected sources.
"""
import hashlib
import os
from pathlib import Path
import subprocess
from xml.etree import ElementTree
from zipfile import ZipFile, BadZipFile

SUITE = 'org.savonitar.flink.stability.testcontainers.KafkaGracefulStopDockerTest'
SERIALIZATION_TEST = 'evidenceSerializationWorksWithoutDocker(Path)'
LIVE_TEST = 'productionKafkaIsPidOneAndTerminatesAfterStopBeforeCleanup'
PACKAGE = 'org/savonitar/flink/stability/testcontainers/'
LAUNCHER = PACKAGE + 'KafkaGracefulStopDockerTest.class'
PRODUCTION = PACKAGE + 'VerifiedKafkaContainer.class'
JACKSON_VERSION = '2.18.4'
JACKSON = {
    'jackson-annotations': 'com/fasterxml/jackson/annotation/JsonCreator.class',
    'jackson-core': 'com/fasterxml/jackson/core/JsonFactory.class',
    'jackson-databind': 'com/fasterxml/jackson/databind/ObjectMapper.class',
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def ordinary_path(value, *, existing=True):
    path = Path(value)
    require(path.is_absolute() and '..' not in path.parts, 'Use an unambiguous absolute path: ' + str(path))
    require(not any(part.is_symlink() for part in (path, *path.parents)), 'Symlink path is not permitted: ' + str(path))
    require(path.resolve(strict=False) == path, 'Use the canonical path: ' + str(path))
    if existing:
        require(path.is_file() or path.is_dir(), 'Missing or nonordinary input: ' + str(path))
    return path


def sha256(path, check):
    check()
    ordinary_path(path)
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            check()
            result.update(block)
    check()
    return result.hexdigest()


def entries(raw, *, from_report):
    require(isinstance(raw, str) and bool(raw) and '\0' not in raw, 'Missing or invalid classpath')
    values = raw.split(os.pathsep)
    if from_report:
        values = [value for value in values if value]
    else:
        require(all(values), 'Explicit classpath must not contain empty entries')
    require(bool(values), 'Classpath has no entries')
    normalized = [str(ordinary_path(value)) for value in values]
    require(len(normalized) == len(set(normalized)), 'Duplicate normalized classpath entry')
    return normalized


def report_classpath(path, check):
    path = ordinary_path(path)
    require(path.is_file() and path.stat().st_size <= 16 * 1024 * 1024, 'Invalid or oversized Surefire report')
    check()
    raw = path.read_bytes()
    require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(), 'DTD/entity declarations are not permitted')
    root = ElementTree.fromstring(raw)
    check()
    require(root.tag == 'testsuite' and root.get('name') == SUITE, 'Unexpected Surefire suite')
    counts = {}
    for name in ('tests', 'failures', 'errors', 'skipped'):
        value = root.get(name, '')
        require(value.isdecimal(), 'Missing or invalid Surefire count: ' + name)
        counts[name] = int(value)
    require(counts['failures'] == counts['errors'] == 0, 'Surefire suite has failures or errors')
    cases = root.findall('testcase')
    require(counts['tests'] == len(cases) and bool(cases), 'Surefire test count differs')
    require(all(case.get('classname') == SUITE for case in cases), 'Unexpected testcase class')
    require(not root.findall('.//failure') and not root.findall('.//error'), 'Surefire report contains a test failure')
    skipped = [case for case in cases if case.find('skipped') is not None]
    require(len(skipped) == counts['skipped'] and all(case.get('name') == LIVE_TEST for case in skipped),
        'Only the opt-in live test may be skipped')
    serialization = [case for case in cases if case.get('name') == SERIALIZATION_TEST]
    require(len(serialization) == 1 and serialization[0].find('skipped') is None,
        'Serialization test must have executed successfully exactly once')
    properties = {}
    for item in root.findall('properties/property'):
        key = item.get('name')
        require(key and key not in properties and item.get('value') is not None, 'Ambiguous Surefire properties')
        properties[key] = item.get('value')
    require('surefire.test.class.path' in properties, 'Missing surefire.test.class.path')
    classpath = properties['surefire.test.class.path']
    if 'java.class.path' in properties:
        require(entries(properties['java.class.path'], from_report=True) == entries(classpath, from_report=True),
            'Conflicting Surefire classpath properties')
    return classpath, {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest(), 'suite': SUITE,
        'counts': counts, 'executedSerializationTest': SERIALIZATION_TEST}


def properties(data):
    result = {}
    for line in data.decode('utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith(('#', '!')):
            continue
        require('=' in line, 'Unsupported Jackson version metadata')
        key, value = line.split('=', 1)
        key, value = key.strip(), value.strip()
        require(key and key not in result, 'Ambiguous Jackson version metadata')
        result[key] = value
    return result


def providers(paths, check):
    wanted = {LAUNCHER, PRODUCTION, *JACKSON.values()}
    found = {name: [] for name in wanted}
    jackson = {}
    for path in paths:
        check()
        archive = None
        try:
            if path.is_dir():
                present = {name for name in wanted if (path / name).exists()}
                for name in present:
                    require(ordinary_path(path / name).is_file(), 'Invalid class provider')
                def read_metadata(name):
                    item = ordinary_path(path / name)
                    require(item.is_file() and item.stat().st_size <= 65536, 'Invalid Jackson version metadata')
                    return item.read_bytes()
            else:
                archive = ZipFile(path)
                names = archive.namelist()
                present = wanted.intersection(names)
                require(all(names.count(name) == 1 for name in present), 'Duplicate class inside classpath archive')
                def read_metadata(name):
                    require(names.count(name) == 1 and archive.getinfo(name).file_size <= 65536,
                        'Missing or ambiguous Jackson version metadata')
                    return archive.read(name)
            for name in present:
                found[name].append(str(path))
            for artifact, name in JACKSON.items():
                if name in present:
                    check()
                    metadata = properties(read_metadata('META-INF/maven/com.fasterxml.jackson.core/' + artifact + '/pom.properties'))
                    require(metadata.get('groupId') == 'com.fasterxml.jackson.core'
                        and metadata.get('artifactId') == artifact and metadata.get('version') == JACKSON_VERSION,
                        'Incompatible Jackson provider: ' + artifact)
                    jackson.setdefault(artifact, []).append({'path': str(path), 'version': metadata['version'], 'class': name})
        except (BadZipFile, KeyError) as error:
            raise ValueError('Invalid classpath archive or provider metadata: ' + str(path)) from error
        finally:
            if archive is not None:
                archive.close()
    require(all(len(values) == 1 for values in found.values()), 'Missing or duplicate launcher/Jackson provider')
    return found, {artifact: values[0] for artifact, values in jackson.items()}


def source_binding(source_root, build_root, check):
    main = Path('testcontainers/src/main')
    files = []
    for member in (source_root / main).rglob('*'):
        check()
        require(not member.is_symlink(), 'Linked production source is not permitted')
        if member.is_file():
            files.append(member.relative_to(source_root))
    require(bool(files), 'Missing selected production sources')
    snapshot_files = []
    for member in (build_root / main).rglob('*'):
        check()
        require(not member.is_symlink(), 'Linked build source is not permitted')
        if member.is_file():
            snapshot_files.append(member.relative_to(build_root))
    require(set(files) == set(snapshot_files), 'Build production-source inventory differs from selected checkout')
    files.extend(Path('testcontainers/src/test/java') / (PACKAGE + name + '.java')
        for name in ('KafkaGracefulStopDockerTest', 'KafkaLifecycleInspection'))
    files.extend([Path('pom.xml'), Path('testcontainers/pom.xml')])
    hashes = {}
    for relative in sorted(files):
        selected = sha256(source_root / relative, check)
        require(sha256(build_root / relative, check) == selected, 'Build source differs from selected checkout: ' + str(relative))
        hashes[str(relative)] = selected
    return {'buildRoot': str(build_root), 'selectedSourceRoot': str(source_root), 'matchedSources': hashes,
        'compilationFreshnessProven': False}


def resolve_classpath(source_root, *, classpath=None, surefire_report=None, check=lambda: None):
    """Validate either explicit CP or a successful report, preserving original input."""
    source_root = ordinary_path(source_root)
    require(source_root.is_dir(), 'Source root must be a directory')
    require((classpath is None) != (surefire_report is None), 'Choose exactly one classpath input mode')
    report = None
    if surefire_report is not None:
        classpath, report = report_classpath(surefire_report, check)
    normalized = entries(classpath, from_report=report is not None)
    paths = [Path(value) for value in normalized]
    found, jackson = providers(paths, check)
    launcher, production = Path(found[LAUNCHER][0]), Path(found[PRODUCTION][0])
    require(launcher.is_dir() and launcher.name == 'test-classes' and launcher.parent.name == 'target'
        and launcher.parent.parent.name == 'testcontainers' and launcher.is_relative_to(source_root),
        'Lifecycle launcher must be built inside the selected checkout')
    require(production == launcher.parent / 'classes' and production.is_dir(), 'Launcher and production classes must share one build')
    build_root = launcher.parents[2]
    binding = source_binding(source_root, build_root, check)
    targets = {str(parent) for path in paths for parent in (path, *path.parents)
        if parent.name == 'target'}
    if build_root != source_root:
        targets.add(str(build_root))
    check()
    return {'mode': 'surefire-report' if report else 'classpath', 'classpath': os.pathsep.join(normalized),
        'rawClasspath': classpath, 'entries': normalized, 'report': report, 'jacksonProviders': jackson,
        'launcher': {'path': str(launcher), 'class': LAUNCHER, 'productionPath': str(production)},
        'sourceBinding': binding, 'buildRoots': sorted(targets),
        'sourceInputs': [str(source_root / value) for value in
            ('testcontainers/src', 'testcontainers/pom.xml', 'pom.xml')],
        'freshBuildRequired': True, 'compilationProvenanceClaimed': False}


def validate_output(output, source_root, inputs, *, run=subprocess.run, check=lambda: None, protected_paths=()):
    """Reject paths that could invalidate source/build identity, before any writes."""
    check()
    source_root = ordinary_path(source_root)
    output = ordinary_path(output, existing=False)
    require(not output.exists(), 'Output must be fresh')
    require(output.parent.is_dir(), 'Output parent must already exist')
    require(not source_root.is_relative_to(output), 'Output overlaps the selected source root')
    protected = [*inputs['entries'], *inputs['buildRoots'], *inputs['sourceInputs'], *protected_paths]
    if inputs.get('report'):
        protected.append(inputs['report']['path'])
    for value in protected:
        path = ordinary_path(value)
        require(not output.is_relative_to(path) and not path.is_relative_to(output), 'Output overlaps classpath/build inputs')
    policy = 'external'
    if output.is_relative_to(source_root):
        relative = str(output.relative_to(source_root))
        def git(*args):
            remaining = check()
            timeout = min(10, remaining) if isinstance(remaining, (int, float)) else 10
            require(timeout > 0, 'Input-validation deadline exhausted')
            result = run(['git', '-C', str(source_root), *args], capture_output=True, timeout=timeout)
            check()
            return result
        tracked = git('ls-files', '-z', '--', relative)
        require(tracked.returncode == 0 and not tracked.stdout, 'Output path contains tracked source files')
        ignored = git('check-ignore', '-q', '--', relative + '/')
        require(ignored.returncode == 0, 'In-tree output must be ignored by Git')
        policy = 'ignored-in-tree'
    return {'path': str(output), 'policy': policy, 'fresh': True, 'sourceAndBuildOverlap': False}
