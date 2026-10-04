"""Swap the released Kafka connector in a canonical scenario for local artifacts.

Shared by the connector-mutant calibration and the pull-request gate. The harness
resolves local references against its artifact root, so every artifact must be a
regular file inside that root.
"""
import hashlib
import re
from pathlib import Path

RELEASED_SUBJECT = ("connectors:\n    kafka:\n"
                    "      artifact: maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def artifact_reference(path, artifact_root):
    path = Path(path).resolve()
    if not path.is_relative_to(artifact_root) or not path.is_file():
        raise SystemExit("Artifact must be a regular file inside --artifact-root: " + str(path))
    return "./" + str(path.relative_to(artifact_root))


def subject_snippet(primary, dependencies):
    """A local primary with explicit runtime dependencies, as the scenario schema needs."""
    return ("artifact: " + primary + "\nruntime_dependencies:\n"
            + "".join("  - " + dependency + "\n" for dependency in dependencies))


def check_released_subject(scenario_text):
    """Split a scenario around its subject block; refuse anything but the released connector."""
    before, remainder = scenario_text.split("\nsubject:\n", 1)
    subject, after = remainder.split("\nworkload:\n", 1)
    if subject.strip() != RELEASED_SUBJECT:
        raise SystemExit("Canonical subject changed; refusing to replace an unrecognized closure.")
    return before, after


def replace_subject(scenario_text, snippet):
    before, after = check_released_subject(scenario_text)
    subject = "  connectors:\n    kafka:\n" + "".join("      " + line + "\n" for line in snippet.splitlines())
    return before + "\nsubject:\n" + subject + "\nworkload:\n" + after


def with_producer_max_block(document, milliseconds):
    """Opt-in calibration setting, restricted to the bundled single-job catalog shape."""
    pattern = r'(?m)^(      program_args: \[)([^\]\n]*)(\]\s*)$'
    matches = list(re.finditer(pattern, document))
    if len(matches) != 1 or "--producerMaxBlockMs" in document:
        raise SystemExit("Expected one unmodified inline program_args list for calibration")
    match = matches[0]
    arguments = match.group(2).rstrip()
    addition = (", " if arguments else "") + '--producerMaxBlockMs, "' + str(milliseconds) + '"'
    return document[:match.start(2)] + arguments + addition + document[match.end(2):]


def with_flink_image(document, image):
    """Use the full spelling in a copy while rejecting changed canonical image inputs."""
    if image != "docker.io/library/flink:2.2.0":
        raise SystemExit("Unsupported Flink image override")
    pattern = r"(?m)^    image: (?:docker\.io/library/)?flink:2\.2\.0[ \t]*$"
    if len(re.findall(pattern, document)) != 1:
        raise SystemExit("Expected one canonical Flink 2.2 image")
    return re.sub(pattern, "    image: " + image, document)


# These substitutions operate on copies of the deliberately small canonical
# mapping shape. Unknown/inline parent shapes reject rather than guessing YAML.
def _mapping_bounds(lines, path):
    start, end, indent = 0, len(lines), 0
    for key in path:
        pattern = re.compile(r'^' + ' ' * indent + re.escape(key) + r':(?:[ \t]*(?:#.*)?)?\n?$')
        matches = [index for index in range(start, end) if pattern.fullmatch(lines[index])]
        if len(matches) != 1:
            raise SystemExit('Expected one block mapping for ' + '.'.join(path))
        start = matches[0] + 1
        end = next((index for index in range(start, end)
                    if lines[index].strip() and not lines[index].lstrip().startswith('#')
                    and len(lines[index]) - len(lines[index].lstrip()) <= indent), end)
        indent += 2
    return start, end, indent


def _set_mapping_values(document, path, values):
    """Replace only named direct children; JSON values are valid YAML scalars/maps."""
    import json
    for key, value in values.items():
        lines = document.splitlines(keepends=True)
        start, end, indent = _mapping_bounds(lines, path)
        prefix = ' ' * indent + key + ':'
        matches = [index for index in range(start, end) if lines[index].startswith(prefix)]
        if len(matches) > 1:
            raise SystemExit('Duplicate mapping key: ' + '.'.join((*path, key)))
        encoded = value if key == 'image' and isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_./:@-]+', value) else json.dumps(value, ensure_ascii=True)
        rendered = prefix + ' ' + encoded + '\n'
        if matches:
            first = matches[0]
            last = next((index for index in range(first + 1, end)
                         if lines[index].strip() and not lines[index].lstrip().startswith('#')
                         and len(lines[index]) - len(lines[index].lstrip()) <= indent), end)
            # Preserve blank separation between the mapping and the next section.
            while last > first + 1 and not lines[last - 1].strip():
                last -= 1
            lines[first:last] = [rendered]
        else:
            lines.insert(start, rendered)
        document = ''.join(lines)
    return document


def _single_job_value(document, key, value):
    import json
    pattern = r'(?m)^      ' + re.escape(key) + r':[^\n]*$'
    if len(re.findall(pattern, document)) != 1:
        raise SystemExit('Expected one canonical workload job ' + key)
    return re.sub(pattern, lambda _: '      ' + key + ': ' + json.dumps(value), document)


def _image_path(value, label):
    from pathlib import PurePosixPath
    path = PurePosixPath(value)
    if (re.fullmatch(r'/(?:[A-Za-z0-9_.+-]+/)*[A-Za-z0-9_.+-]+\.jar', value) is None
            or str(path) != value
            or '..' in path.parts or not value.endswith('.jar')
            or any(character.isspace() or ord(character) < 32 for character in value)):
        raise SystemExit(label + ' must be a normalized absolute .jar path inside the image')
    return value


def path_digest(value, label):
    path, separator, digest = value.rpartition('=')
    if not separator or re.fullmatch('[0-9a-f]{64}', digest) is None:
        raise SystemExit(label + ' must be <absolute-image-jar-path>=<64-lowercase-hex-sha256>')
    return {'container_path': _image_path(path, label), 'sha256': digest}


def runtime_jar_pin(value):
    pin = path_digest(value, '--runtime-jar')
    if re.fullmatch(r'/opt/flink/lib/flink-dist-[A-Za-z0-9][A-Za-z0-9._+-]*\.jar', pin['container_path']) is None:
        raise SystemExit('--runtime-jar must directly name /opt/flink/lib/flink-dist-<version>.jar')
    return pin


def image_subject(value):
    """An image subject has no host artifact or harness-installed dependency closure."""
    if not value.startswith('image:'):
        raise SystemExit('--subject/--baseline-subject currently require image:<path>=<sha256>')
    pin = path_digest(value[len('image:'):], 'Image subject')
    reference = 'image:' + pin['container_path']
    return ({'connector': reference, 'connectorSha256': pin['sha256'], 'origin': 'image',
             'dependencyMode': 'image', 'runtimeDependencySha256': None},
            'artifact: ' + reference + '\nsha256: ' + pin['sha256'])


def configuration_pairs(values, option):
    result = {}
    for value in values or ():
        key, separator, setting = value.partition('=')
        if (not separator or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', key) is None
                or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in setting)):
            raise SystemExit(option + ' requires key=value with a single-line value')
        if key in result:
            raise SystemExit(option + ' repeats key: ' + key)
        result[key] = setting
    return result


def add_runtime_arguments(parser):
    parser.add_argument('--flink-image', help='Common Flink image reference in both copied catalogs')
    parser.add_argument('--flink-image-id', help='Common local Docker image ID, sha256:<64 hex>')
    parser.add_argument('--flink-line', help='Explicit common major.minor line; requires image ID and runtime JAR')
    parser.add_argument('--runtime-jar', help='Runtime image JAR pin: /absolute/path.jar=<sha256>')
    parser.add_argument('--kafka-image', help='Common Kafka image reference')
    parser.add_argument('--kafka-image-id', help='Common local Kafka Docker image ID')
    parser.add_argument('--kafka-launch', choices=('apache-kafka', 'generic-kraft'))
    parser.add_argument('--broker-config', action='append', default=[], metavar='KEY=VALUE')
    parser.add_argument('--flink-config', action='append', default=[], metavar='KEY=VALUE')
    parser.add_argument('--candidate-flink-config', action='append', default=[], metavar='KEY=VALUE',
                        help='Explicit candidate-only overlay on the common Flink configuration')
    parser.add_argument('--workload-jar', type=Path, help='Common protocol-v1 workload JAR inside the artifact root')
    parser.add_argument('--transaction-version', choices=('1', '2', 'broker-default'),
                        help='Explicit common transaction feature selection; omission preserves the catalog')
    parser.add_argument('--transaction-id-naming-strategy', choices=('INCREMENTING', 'POOLING', 'connector-default'),
                        help='Explicit common naming choice; omission preserves the catalog')


def runtime_substitutions(args, root):
    """Validate and retain every explicit common input before writing any catalogs."""
    for name in ('flink_image_id', 'kafka_image_id'):
        value = getattr(args, name)
        if value is not None and re.fullmatch(r'sha256:[0-9a-f]{64}', value) is None:
            raise SystemExit('--' + name.replace('_', '-') + ' must be a full local Docker image ID')
    if args.flink_line is not None:
        if re.fullmatch(r'(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)', args.flink_line) is None:
            raise SystemExit('--flink-line must be major.minor')
        if not args.flink_image_id or not args.runtime_jar:
            raise SystemExit('--flink-line requires --flink-image-id and --runtime-jar')
    for name in ('flink_image', 'kafka_image'):
        value = getattr(args, name)
        if value is not None and (not value or any(character.isspace() or ord(character) < 32 for character in value)):
            raise SystemExit('--' + name.replace('_', '-') + ' must be a nonempty image reference')
    common = {
        'flinkImage': args.flink_image, 'flinkImageId': args.flink_image_id,
        'flinkLine': args.flink_line,
        'runtimeJar': runtime_jar_pin(args.runtime_jar) if args.runtime_jar else None,
        'kafkaImage': args.kafka_image, 'kafkaImageId': args.kafka_image_id,
        'kafkaLaunch': args.kafka_launch,
        'brokerConfig': configuration_pairs(args.broker_config, '--broker-config'),
        'flinkConfig': configuration_pairs(args.flink_config, '--flink-config'),
        'workloadJar': artifact_reference(args.workload_jar, root) if args.workload_jar else None,
        'workloadJarSha256': sha256(args.workload_jar) if args.workload_jar else None,
        'transactionVersion': (int(args.transaction_version) if args.transaction_version in ('1', '2')
                               else args.transaction_version),
        'transactionIdNamingStrategy': args.transaction_id_naming_strategy,
    }
    candidate_config = configuration_pairs(args.candidate_flink_config, '--candidate-flink-config')
    return common, candidate_config


def with_runtime_substitutions(document, common, candidate_config=None):
    """Only declared runtime/subject choices vary; faults, timing and expectations are untouched."""
    flink = {field: common[key] for key, field in (
        ('flinkImage', 'image'), ('flinkImageId', 'image_id'), ('flinkLine', 'line'),
        ('runtimeJar', 'runtime_jar')) if common.get(key) is not None}
    config = {**common.get('flinkConfig', {}), **(candidate_config or {})}
    if config:
        # Replacing an existing unknown config would silently discard a catalog input.
        if re.search(r'(?m)^    config:', document):
            raise SystemExit('Canonical Flink config already exists; refusing to replace it')
        flink['config'] = config
    if flink:
        document = _set_mapping_values(document, ('setup', 'flink'), flink)
    kafka = {field: common[key] for key, field in (
        ('kafkaImage', 'image'), ('kafkaImageId', 'image_id'), ('transactionVersion', 'transaction_version'))
        if common.get(key) is not None}
    if common.get('kafkaLaunch') is not None:
        kafka['launch'] = {'type': common['kafkaLaunch']}
    if common.get('brokerConfig'):
        if re.search(r'(?m)^        broker_config:', document):
            raise SystemExit('Canonical broker config already exists; refusing to replace it')
        kafka['broker_config'] = common['brokerConfig']
    if kafka:
        lines = document.splitlines(keepends=True)
        start, end, _ = _mapping_bounds(lines, ('setup', 'kafka', 'clusters'))
        clusters = [match.group(1) for line in lines[start:end]
                    if (match := re.fullmatch(r'      ([a-z0-9-]+):[^\n]*\n?', line))]
        if len(clusters) != 1:
            raise SystemExit('Runtime substitutions require exactly one Kafka cluster')
        document = _set_mapping_values(document, ('setup', 'kafka', 'clusters', clusters[0]), kafka)
    if common.get('workloadJar') is not None:
        document = _single_job_value(document, 'jar', common['workloadJar'])
    naming = common.get('transactionIdNamingStrategy')
    if naming is not None:
        pattern = r'(?m)^        transaction_id_naming_strategy:[^\n]*$'
        if len(re.findall(pattern, document)) != 1:
            raise SystemExit('Naming substitution requires one explicitly transactional canonical sink')
        document = re.sub(pattern, lambda _: '        transaction_id_naming_strategy: ' + naming, document)
    return document
