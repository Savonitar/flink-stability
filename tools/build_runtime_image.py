#!/usr/bin/env python3
"""Build one local runtime subject from an explicit, validated staging directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile

KAFKA_PREFIXES = ('org/apache/flink/connector/kafka/', 'org/apache/flink/streaming/connectors/kafka/')
DIST_NAME = re.compile(r'flink-dist(?:_[A-Za-z0-9][A-Za-z0-9._+-]*)?-[A-Za-z0-9][A-Za-z0-9._+-]*\.jar')
SAFE_NAME = re.compile(r'[A-Za-z0-9_.+-]+')
VERSION = re.compile(r'[0-9]+\.[0-9]+(?:\.[0-9]+)?[A-Za-z0-9._+-]*')
STANDARD_ROOTS = {'bin', 'conf', 'lib', 'opt', 'plugins', 'examples', 'licenses', 'usrlib', 'log',
                  'LICENSE', 'NOTICE', 'README.txt', 'README.md'}


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def checked_path(value, directory=False):
    path = Path(os.path.abspath(value))
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError(f'Symlinks are not accepted: {part}')
    if not (path.is_dir() if directory else path.is_file()):
        raise ValueError(f'Expected {"directory" if directory else "regular file"}: {path}')
    return path


def tree_files(root):
    files = []
    for path in sorted(root.rglob('*')):
        if path.is_symlink() or not (path.is_dir() or path.is_file()):
            raise ValueError(f'Distribution may contain only regular files and directories: {path}')
        if any(not SAFE_NAME.fullmatch(p) or p in ('.', '..') for p in path.relative_to(root).parts):
            raise ValueError(f'Unsupported distribution filename: {path}')
        if path.is_file():
            files.append(path)
    return files


def jar_info(path, java):
    if path.name.startswith('flink-runtime') and path.suffix == '.jar':
        raise ValueError(f'Separate flink-runtime JAR is forbidden; use the distribution JAR: {path.name}')
    maximum = 0
    connector = False
    with zipfile.ZipFile(path) as jar:
        names = jar.namelist()
        if len(names) != len(set(names)):
            raise ValueError(f'Duplicate ZIP entries in {path.name}')
        manifest = jar.read('META-INF/MANIFEST.MF').decode('utf-8') if 'META-INF/MANIFEST.MF' in names else ''
        multi_release = re.search(r'^Multi-Release: true\r?$', manifest, re.MULTILINE | re.IGNORECASE) is not None
        for name in names:
            # Check every class that the selected Java can load; later multi-release entries are inactive.
            normalized = name
            if name.startswith('META-INF/versions/') and name.endswith('.class'):
                pieces = name.split('/', 3)
                if len(pieces) != 4 or not pieces[2].isdigit():
                    raise ValueError(f'Invalid multi-release class entry: {path.name}: {name}')
                normalized = pieces[3]
                active = multi_release and int(pieces[2]) <= java
            else:
                active = True
            connector |= normalized.endswith('.class') and normalized.startswith(KAFKA_PREFIXES)
            if active and name.endswith('.class'):
                with jar.open(name) as stream:
                    header = stream.read(8)
                if len(header) != 8 or header[:4] != b'\xca\xfe\xba\xbe':
                    raise ValueError(f'Invalid class file: {path.name}: {name}')
                minor, major = int.from_bytes(header[4:6], 'big'), int.from_bytes(header[6:8], 'big')
                if minor == 65535 or major > java + 44:
                    raise ValueError(f'{path.name}: {name} requires Java {major - 44}'
                                     f'{" preview" if minor == 65535 else ""}; selected Java is {java}')
                maximum = max(maximum, major)
        match = re.search(r'^Implementation-Version: ([^\r\n]+)', manifest, re.MULTILINE)
        version = match[1] if match else None
    return {'sha256': sha256(path), 'maxActiveClassMajor': maximum, 'kafkaConnectorClasses': connector,
            'implementationVersion': version}


def prepare(dist, connector, closure, output, java=17, base_image=None):
    dist, connector, closure = checked_path(dist, True), checked_path(connector), checked_path(closure, True)
    if dist == closure or dist in closure.parents or closure in dist.parents or dist in connector.parents or closure in connector.parents:
        raise ValueError('Distribution, connector and closure must be separate nonoverlapping inputs')
    output = Path(os.path.abspath(output))
    if output == dist or dist in output.parents or output == closure or closure in output.parents:
        raise ValueError('Build output must be outside the input trees')
    for parent in (output, *output.parents):
        if parent.is_symlink():
            raise ValueError(f'Symlink output parent: {parent}')
    if output.exists():
        raise ValueError(f'Build output already exists; preserve it and choose a fresh directory: {output}')
    unexpected = {p.name for p in dist.iterdir()} - STANDARD_ROOTS
    if unexpected:
        raise ValueError('Not a clean Flink distribution; unexpected entries: ' + ', '.join(sorted(unexpected)))
    files = tree_files(dist)
    if any(p.relative_to(dist).parts[0] == 'log' for p in files):
        raise ValueError('Use a clean distribution without existing runtime logs')
    for name in ('bin/flink', 'bin/jobmanager.sh', 'bin/taskmanager.sh', 'bin/config-parser-utils.sh', 'conf/config.yaml'):
        checked_path(dist / name)
        if name.startswith('bin/') and not os.access(dist / name, os.X_OK):
            raise ValueError(f'Distribution script is not executable: {name}')
    jars = [p for p in files if p.suffix == '.jar']
    runtime = [p for p in jars if p.parent == dist / 'lib' and DIST_NAME.fullmatch(p.name)]
    if len(runtime) != 1:
        raise ValueError('Distribution lib must contain exactly one flink-dist JAR')
    dependencies = tree_files(closure)
    if any(p.is_dir() for p in closure.iterdir()) or any(p.parent != closure or p.suffix != '.jar' for p in dependencies):
        raise ValueError('Closure must be a flat directory of JAR files')
    if connector.suffix != '.jar' or not SAFE_NAME.fullmatch(connector.name):
        raise ValueError('Connector must have a simple .jar filename')
    added = [connector, *dependencies]
    names = {p.name for p in (dist / 'lib').iterdir()}
    for path in added:
        if path.name in names:
            raise ValueError(f'Connector/closure filename collides with another library: {path.name}')
        names.add(path.name)
    information = {p: jar_info(p, java) for p in [*jars, *added]}
    copies = [p for p, info in information.items()
              if (p.parent == dist / 'lib' or p in added) and info['kafkaConnectorClasses']]
    if copies != [connector]:
        raise ValueError('Exactly the supplied connector JAR must contain Kafka connector classes in /opt/flink/lib; found: '
                         + ', '.join(str(p) for p in copies))
    runtime_info = information[runtime[0]]
    version = runtime_info['implementationVersion']
    if version is None or not VERSION.fullmatch(version):
        raise ValueError('Distribution JAR requires a valid Implementation-Version manifest entry')
    base_image = base_image or f'flink:{version}-java{java}'
    if not re.fullmatch(r'flink:[0-9]+\.[0-9]+\.[0-9]+-java' + str(java), base_image):
        raise ValueError('Base must be an explicit public flink:<release>-java<N>; use --base-image for private versions')
    context = output / 'context'
    (context / 'dist').mkdir(parents=True)
    (context / 'connector').mkdir()
    (context / 'closure').mkdir()
    for directory in sorted(p for p in dist.rglob('*') if p.is_dir()):
        destination = context / 'dist' / directory.relative_to(dist)
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copymode(directory, destination, follow_symlinks=False)
    inventory = []
    for source in [*files, *added]:
        relative = (Path('dist') / source.relative_to(dist) if source in files else
                    Path('connector' if source == connector else 'closure') / source.name)
        destination = context / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        before = sha256(source)
        shutil.copy2(source, destination, follow_symlinks=False)
        if destination.is_symlink() or before != sha256(destination) or before != sha256(source):
            raise ValueError(f'Input changed during staging: {source}')
        inventory.append({'contextPath': str(relative), 'sha256': before, 'size': destination.stat().st_size})
    manifest = {'format': 'flink-stability-runtime-image-v1', 'status': 'prepared', 'java': java,
                'flinkVersion': version, 'baseImage': base_image, 'inputs': inventory,
                'runtimeJar': {'containerPath': '/opt/flink/lib/' + runtime[0].name, **runtime_info},
                'connector': {'containerPath': '/opt/flink/lib/' + connector.name, **information[connector]},
                'imageJars': {'/opt/flink/' + str(p.relative_to(dist)): info['sha256']
                              for p, info in information.items() if p in jars}}
    manifest['imageJars'].update({'/opt/flink/lib/' + p.name: information[p]['sha256'] for p in added})
    write_json(output / 'manifest.json', manifest)
    return manifest


def dockerfile(base, has_closure):
    # The official flink-docker 2.2 image applies these changes through its distribution parser.
    # https://github.com/apache/flink-docker/blob/master/2.2/scala_2.12-java21-ubuntu/Dockerfile
    return f'''FROM {base}
USER root
WORKDIR /
RUN rm -rf /opt/flink && mkdir -p /opt/flink
COPY --chown=flink:flink dist/ /opt/flink/
COPY --chown=flink:flink connector/ /opt/flink/lib/
''' + ('COPY --chown=flink:flink closure/ /opt/flink/lib/\n' if has_closure else '') + '''RUN mkdir -p /opt/flink/log /opt/flink/plugins && chown -R flink:flink /opt/flink
USER flink
WORKDIR /opt/flink
RUN /bin/bash /opt/flink/bin/config-parser-utils.sh /opt/flink/conf /opt/flink/bin /opt/flink/lib \\
    -repKV rest.address,localhost,0.0.0.0 \\
    -repKV rest.bind-address,localhost,0.0.0.0 \\
    -repKV jobmanager.bind-host,localhost,0.0.0.0 \\
    -repKV taskmanager.bind-host,localhost,0.0.0.0 \\
    -rmKV taskmanager.host=localhost
'''


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def build(output, manifest, tag):
    if not re.fullmatch(r'[a-z0-9]+(?:[._/-][a-z0-9]+)*:[A-Za-z0-9_][A-Za-z0-9_.-]*', tag):
        raise ValueError('Use an explicit image name:tag, without registry credentials, ports or digests')
    host = os.environ.get('DOCKER_HOST', 'unix:///var/run/docker.sock')
    if not host.startswith('unix:///') or any(c.isspace() for c in host):
        raise ValueError('Only a local Unix-socket Docker daemon is supported; remote builders are not used')
    config = output / 'docker-config'
    config.mkdir()
    env = {'PATH': os.defpath + ':/opt/homebrew/bin:/usr/local/bin', 'DOCKER_CONFIG': str(config),
           'DOCKER_HOST': host, 'DOCKER_BUILDKIT': '0', 'DOCKER_API_VERSION': '1.44'}
    docker = ['docker', '--config', str(config), '--host', host]
    commands = []

    def run(args, label, check=True):
        command = docker + args
        completed = subprocess.run(command, env=env, capture_output=True, text=True)
        (output / f'{label}.stdout').write_text(completed.stdout)
        (output / f'{label}.stderr').write_text(completed.stderr)
        commands.append({'argv': command, 'exitCode': completed.returncode})
        write_json(output / 'commands.json', commands)
        if check and completed.returncode:
            raise ValueError(f'Docker {label} failed; see {output / (label + ".stderr")}')
        return completed

    if run(['image', 'inspect', tag], 'existing-tag', False).returncode == 0:
        raise ValueError('Output image tag already exists; select a new tag rather than replacing it')
    base = run(['image', 'inspect', manifest['baseImage']], 'base-inspect', False)
    if base.returncode:
        run(['pull', manifest['baseImage']], 'base-pull')
        base = run(['image', 'inspect', manifest['baseImage']], 'base-inspect')
    identity = json.loads(base.stdout)[0]
    digests = identity.get('RepoDigests', [])
    if not digests:
        raise ValueError('Public base has no registry digest; cannot pin the build base')
    digest = digests[0].split('@', 1)[1]
    pinned = manifest['baseImage'] + '@' + digest
    manifest.update(baseImageId=identity['Id'], baseImageDigest=digest, imageTag=tag)
    (output / 'context/Dockerfile').write_text(dockerfile(pinned, any(i['contextPath'].startswith('closure/') for i in manifest['inputs'])))
    write_json(output / 'manifest.json', manifest)
    run(['build', '--network=none', '--pull=false', '--tag', tag, str(output / 'context')], 'build')
    image = json.loads(run(['image', 'inspect', tag], 'image-inspect').stdout)[0]
    image_id = image['Id']
    java = run(['run', '--rm', '--network=none', '--entrypoint', 'java', image_id,
                '-XshowSettings:properties', '-version'], 'image-java')
    actual_java = re.search(r'java.specification.version\s*=\s*([0-9]+)', java.stdout + java.stderr)
    if not actual_java or int(actual_java[1]) != manifest['java']:
        raise ValueError('Built image Java version does not match --java')
    hashes = run(['run', '--rm', '--network=none', '--entrypoint', '/bin/sh', image_id, '-ec',
                  "find /opt/flink -type f -name '*.jar' -exec sha256sum {} +"], 'image-jar-hashes')
    observed = {}
    for line in hashes.stdout.splitlines():
        match = re.fullmatch(r'([0-9a-f]{64})  (/.+)', line)
        if not match or match[2] in observed:
            raise ValueError('Invalid or duplicate image JAR hash observation')
        observed[match[2]] = match[1]
    if observed != manifest['imageJars']:
        raise ValueError('Built image JAR paths/bytes differ from the complete staged inventory')
    manifest.update(status='verified', imageId=image_id, observedImageJars=observed)
    write_json(output / 'manifest.json', manifest)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dist_dir')
    parser.add_argument('connector_jar')
    parser.add_argument('closure_dir')
    parser.add_argument('tag')
    parser.add_argument('--java', type=int, choices=(17, 21), default=17)
    parser.add_argument('--base-image', help='Public flink:<release>-java<N> base for a private distribution version')
    parser.add_argument('--output', type=Path, help='Fresh output directory; defaults to ignored jobs/runtime-images/build-*')
    args = parser.parse_args(argv)
    if args.output is None:
        default_root = Path(__file__).resolve().parents[1] / 'jobs/runtime-images'
        for parent in (default_root, *default_root.parents):
            if parent.is_symlink():
                parser.exit(1, f'Symlink output parent: {parent}\n')
        default_root.mkdir(parents=True, exist_ok=True)
        checked_path(default_root, True)
        output = Path(tempfile.mkdtemp(prefix='build-', dir=default_root))
    else:
        output = args.output
    if args.output is None:
        output.rmdir()  # prepare creates the fresh directory, never overwrites one.
    output = Path(os.path.abspath(output))
    try:
        manifest = prepare(args.dist_dir, args.connector_jar, args.closure_dir, output, args.java, args.base_image)
        manifest = build(output, manifest, args.tag)
    except (ValueError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, f'{error}\n')
    print(json.dumps({'imageId': manifest['imageId'], 'runtimeJar': manifest['runtimeJar'],
                      'connector': manifest['connector'], 'manifest': str(output / 'manifest.json')}, indent=2))


if __name__ == '__main__':
    main()
