#!/usr/bin/env python3
"""Explain retained attempt evidence; never rerun a scenario or change its verdict.

Usage: analyze_attempt.py result.json kafka-log-output [--flink-log-dir DIR]
Decoded JSON receipts are verified when hashes are present. Paths in JSON are
untrusted and must stay inside the explicitly supplied evidence roots. Physical
commit markers describe retained bytes, not read-committed visibility or blame.
"""
import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import sys

DEFAULT_MARKER = r'org\.apache\.flink\.connector\.kafka\.sink(?:\.internal)?\.KafkaCommitter\b.*(?i:recover)'
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_LOG_BYTES = 4 * 1024 * 1024
MAX_RECORDS = 100_000


class EvidenceError(ValueError):
    pass


def integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def guarded_path(value, roots=None):
    """Check lexical containment before inspecting any path component or symlink."""
    path = Path(value)
    if '..' in path.parts:
        raise EvidenceError('Parent traversal is not an evidence reference')
    path = Path(os.path.abspath(path))
    if roots is not None and not any(path.is_relative_to(root) for root in roots):
        raise EvidenceError('Reference is outside the explicit evidence roots')
    private = {'Documents', 'Downloads', 'Desktop', 'Photos', 'Pictures', 'Videos'}
    parts = path.parts
    if ((len(parts) > 3 and parts[1] in ('Users', 'home') and parts[3] in private)
            or any(part in ('.ssh', '.aws', '.gnupg', '.kube', '.keychain', 'Keychains') for part in parts)
            or path.name in ('credentials', 'id_rsa', 'id_ed25519', '.env')):
        raise EvidenceError('Private or credential path is not an evidence input')
    cursor = Path(path.anchor)
    for part in path.parts[1:]:
        cursor /= part
        if cursor.is_symlink():
            raise EvidenceError('Evidence symlinks are not followed')
    return path


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError('Duplicate JSON key: ' + key)
        result[key] = value
    return result


class Reader:
    def __init__(self, roots):
        self.roots = roots
        self.diagnostics = []

    def diagnostic(self, code, message, source=None, severity='warning'):
        item = {'code': code, 'message': str(message), 'severity': severity}
        if source is not None:
            item['source'] = str(source)
        self.diagnostics.append(item)

    def read_json(self, value, sha256=None):
        try:
            path = guarded_path(value, self.roots)
            if not path.is_file() or path.stat().st_size > MAX_JSON_BYTES:
                raise EvidenceError('Missing regular JSON file or evidence size limit exceeded')
            payload = path.read_bytes()
            if sha256 is not None:
                if not isinstance(sha256, str) or re.fullmatch('[0-9a-f]{64}', sha256) is None:
                    raise EvidenceError('Malformed SHA-256 receipt')
                if hashlib.sha256(payload).hexdigest() != sha256:
                    raise EvidenceError('SHA-256 receipt does not match retained bytes')
            value = json.loads(payload, object_pairs_hook=unique_object,
                               parse_constant=lambda token: (_ for _ in ()).throw(EvidenceError('Invalid JSON number ' + token)))
            if not isinstance(value, dict):
                raise EvidenceError('Expected a JSON object')
            return value
        except (OSError, ValueError, RecursionError) as error:
            self.diagnostic('evidence.unreadable', error, value)
            return None

    def receipt(self, receipt, root, label):
        if not isinstance(receipt, dict) or not isinstance(receipt.get('evidence'), str):
            self.diagnostic('evidence.receipt-missing', label + ' has no evidence path')
            return None
        path = Path(receipt['evidence'])
        expected_name = (re.fullmatch(r'decoded-[0-9]+\.json', path.name) is not None
                         if label == 'Kafka decode' else path.name == 'transactions.json')
        if not expected_name:
            self.diagnostic('evidence.receipt-name-unsupported', label + ' does not name a retained evidence JSON file', path)
            return None
        if not path.is_absolute():
            path = root / path
        if receipt.get('sha256') is None:
            self.diagnostic('evidence.hash-missing', label + ' has no SHA-256 receipt', path)
        return self.read_json(path, receipt.get('sha256'))


def object_section(value, name, reader):
    section = value.get(name)
    if not isinstance(section, dict):
        reader.diagnostic('evidence.section-missing', 'Missing or malformed ' + name)
        return {}
    return section


def oracle_samples(terminal, reader):
    samples = {}
    for key, count in (('missingSamples', 'missing'), ('duplicateSamples', 'duplicates'),
                       ('malformedSamples', 'malformed'), ('unexpectedSamples', 'unexpected')):
        if key not in terminal:
            if (terminal.get('completed') is True and terminal.get('snapshotComplete') is True
                    and integer(terminal.get(count)) and terminal[count] == 0):
                samples[key] = []
            else:
                samples[key] = None
                reader.diagnostic('oracle.samples-unavailable', key + ' not retained; physical copies are reported separately')
            continue
        value = terminal[key]
        valid = isinstance(value, list)
        if valid:
            valid = all(integer(item) for item in value) if key == 'missingSamples' else all(
                isinstance(item, dict) and integer(item.get('partition')) and item['partition'] >= 0
                and integer(item.get('offset')) and item['offset'] >= 0
                and 'rawValue' in item and (item['rawValue'] is None or isinstance(item['rawValue'], str))
                for item in value)
        samples[key] = value if valid else None
        if not valid:
            reader.diagnostic('oracle.samples-malformed', key + ' does not contain retained oracle sample records')
    return samples


def summarize_snapshots(value, label, reader):
    if not isinstance(value, list):
        reader.diagnostic('kill.snapshot-missing', label + ' is missing or malformed')
        return None
    snapshots = []
    for snapshot in value:
        if not isinstance(snapshot, dict):
            reader.diagnostic('kill.snapshot-malformed', label + ' contains a non-object')
            continue
        if (not isinstance(snapshot.get('topic'), str) or not integer(snapshot.get('partition'))
                or not integer(snapshot.get('logEndOffset'))):
            reader.diagnostic('kill.snapshot-fields-malformed', label + ' topic, partition or end offset is unavailable/malformed')
        transactions = snapshot.get('transactions')
        if not isinstance(transactions, list):
            reader.diagnostic('kill.transactions-missing', label + ' has no transaction descriptions')
            transactions = []
        selected = []
        for transaction in transactions:
            if not isinstance(transaction, dict):
                reader.diagnostic('kill.transaction-malformed', label + ' contains a non-object transaction')
                continue
            fields = ('transactionalId', 'state', 'producerId', 'epoch', 'startTimeMs', 'timeoutMs')
            missing = [key for key in fields if key not in transaction]
            if missing:
                reader.diagnostic('kill.transaction-fields-missing', label + ': ' + ', '.join(missing))
            if (not all(isinstance(transaction.get(key), str) for key in ('transactionalId', 'state'))
                    or not all(integer(transaction.get(key)) for key in ('producerId', 'epoch', 'startTimeMs', 'timeoutMs'))):
                reader.diagnostic('kill.transaction-fields-malformed', label + ' transaction identity/state/time is unavailable or malformed')
            selected.append({key: transaction.get(key) for key in fields})
        snapshots.append({'topic': snapshot.get('topic'), 'partition': snapshot.get('partition'),
                          'logEndOffset': snapshot.get('logEndOffset'),
                          'producers': snapshot.get('producers'), 'transactions': selected})
    return snapshots


def decoded_batches(evidence, log_root, reader):
    receipts = evidence.get('decoded')
    if not isinstance(receipts, list) or not receipts:
        reader.diagnostic('kafka.decoded-missing', 'No decoded JSON receipts were retained')
        return []
    batches, observed, record_count = [], {}, 0
    for receipt in receipts:
        decoded = reader.receipt(receipt, log_root, 'Kafka decode')
        if decoded is None:
            continue
        if (decoded.get('status') != 'PARSED_AND_DECODED'
                or decoded.get('allLogsDecoded') is not True
                or decoded.get('inputVerified') is not True or decoded.get('tarComplete') is not True):
            reader.diagnostic('kafka.decode-incomplete', 'Decode/tar/input verification is not complete', receipt.get('evidence'))
            continue
        partition_root = decoded.get('expectedPartitionRoot')
        match = re.fullmatch(r'([A-Za-z0-9._-]+)-(\d+)', partition_root) if isinstance(partition_root, str) else None
        if not match:
            reader.diagnostic('kafka.partition-malformed', 'Missing or malformed decoded partition root')
            continue
        topic, partition = match.group(1), int(match.group(2))
        logs = decoded.get('logs')
        if not isinstance(logs, list) or not logs:
            reader.diagnostic('kafka.logs-missing', 'Decoded receipt has no segment records')
            continue
        for log in logs:
            if (not isinstance(log, dict) or log.get('decoded') is not True
                    or not isinstance(log.get('segment'), dict)
                    or not isinstance(log['segment'].get('batches'), list)):
                reader.diagnostic('kafka.segment-malformed', 'Missing decoded segment/batches')
                continue
            for batch in log['segment']['batches']:
                try:
                    if not isinstance(batch, dict):
                        raise EvidenceError('Batch is not an object')
                    fields = ('baseOffset', 'lastOffset', 'producerId', 'producerEpoch')
                    if not all(integer(batch.get(key)) for key in fields):
                        raise EvidenceError('Batch identity or offset is missing/malformed')
                    if (batch['baseOffset'] < 0 or batch['lastOffset'] < batch['baseOffset']
                            or not isinstance(batch.get('transactional'), bool)
                            or not isinstance(batch.get('control'), bool)
                            or not isinstance(batch.get('records'), list)):
                        raise EvidenceError('Invalid batch range, flags or records')
                    offsets = []
                    for record in batch['records']:
                        if not isinstance(record, dict) or not integer(record.get('offset')):
                            raise EvidenceError('Malformed record offset')
                        offset = record['offset']
                        if offset < batch['baseOffset'] or offset > batch['lastOffset']:
                            raise EvidenceError('Record offset outside its batch')
                        offsets.append(offset)
                        if record.get('canonicalId') is not None and not integer(record['canonicalId']):
                            raise EvidenceError('Malformed canonical record ID')
                        if batch['control']:
                            marker = record.get('marker')
                            if not isinstance(marker, dict) or marker.get('type') not in ('COMMIT', 'ABORT'):
                                raise EvidenceError('Unknown or missing control marker')
                        elif record.get('marker') is not None:
                            raise EvidenceError('Data batch contains a control marker')
                    if offsets != sorted(set(offsets)):
                        raise EvidenceError('Duplicate or unordered record offsets')
                    if batch['control'] and (not batch['transactional'] or len(offsets) != 1):
                        raise EvidenceError('Control batch must contain exactly one transactional marker')
                    key = (topic, partition, batch['baseOffset'])
                    signature = json.dumps(batch, sort_keys=True)
                    if key in observed:
                        if observed[key] != signature:
                            raise EvidenceError('Conflicting batches at the same partition offset')
                        reader.diagnostic('kafka.repeated-capture', 'Identical physical batch counted once',
                                          str(key), severity='info')
                        continue
                    observed[key] = signature
                    record_count += len(offsets)
                    if record_count > MAX_RECORDS:
                        raise EvidenceError('Aggregate decoded record limit exceeded')
                    batches.append({'topic': topic, 'partition': partition, 'batch': batch})
                except EvidenceError as error:
                    reader.diagnostic('kafka.batch-malformed', error, receipt.get('evidence'))
    return batches


def group_transactions(batches, output_topic, reader):
    groups, open_groups, ordinals, previous_end, last_closed = [], {}, defaultdict(int), {}, {}
    for item in sorted(batches, key=lambda value: (value['topic'], value['partition'], value['batch']['baseOffset'])):
        if item['topic'] != output_topic:
            continue
        batch, partition = item['batch'], item['partition']
        if batch['baseOffset'] <= previous_end.get(partition, -1):
            reader.diagnostic('kafka.offset-overlap', 'Overlapping physical batch ranges', f'{output_topic}-{partition}')
            continue
        if partition in previous_end and batch['baseOffset'] != previous_end[partition] + 1:
            reader.diagnostic('kafka.offset-gap', 'Capture gap may hide a transaction boundary', f'{output_topic}-{partition}')
            for identity in list(open_groups):
                if identity[0] == partition:
                    del open_groups[identity]
                    last_closed[identity] = 'capture-gap'
        previous_end[partition] = batch['lastOffset']
        if not batch['transactional'] or batch['producerId'] < 0 or batch['producerEpoch'] < 0:
            reader.diagnostic('kafka.nontransactional-output', 'Output batch has no valid transactional producer identity')
            continue
        identity = (partition, batch['producerId'], batch['producerEpoch'])
        group = open_groups.get(identity)
        if group is None:
            ordinals[identity] += 1
            group = {'transaction': f'{output_topic}:{partition}/{identity[1]}:{identity[2]}#{ordinals[identity]}',
                     'topic': output_topic, 'partition': partition, 'producerId': identity[1],
                     'producerEpoch': identity[2], 'occurrence': ordinals[identity],
                     'startBoundary': last_closed.get(identity, 'first-retained-batch'),
                     'firstOffset': batch['baseOffset'], 'lastOffset': batch['lastOffset'],
                     'batches': [], 'records': [], 'marker': None}
            open_groups[identity] = group
            groups.append(group)
        group['lastOffset'] = batch['lastOffset']
        group['batches'].append({key: batch[key] for key in ('baseOffset', 'lastOffset', 'control')})
        for record in batch['records']:
            if batch['control']:
                group['marker'] = {'offset': record['offset'], **record['marker']}
                del open_groups[identity]
                last_closed[identity] = 'after-prior-marker'
            else:
                group['records'].append({'offset': record['offset'], 'id': record.get('canonicalId')})
                if record.get('canonicalId') is None:
                    reader.diagnostic('kafka.id-unavailable', 'Output record has no decoded canonical ID',
                                      f'{output_topic}-{partition}@{record["offset"]}')
    for group in groups:
        group['outcome'] = group['marker']['type'] if group['marker'] else 'NO_RETAINED_MARKER'
    return groups


def duplicate_evidence(transactions):
    copies = defaultdict(list)
    for transaction in transactions:
        for record in transaction['records']:
            if record['id'] is not None:
                copies[record['id']].append({'transaction': transaction['transaction'],
                                            'partition': transaction['partition'], 'offset': record['offset'],
                                            'outcome': transaction['outcome']})
    duplicates = [{'id': identity, 'copies': values} for identity, values in sorted(copies.items()) if len(values) > 1]
    committed = {item['id']: [copy for copy in item['copies'] if copy['outcome'] == 'COMMIT'] for item in duplicates}
    committed = {identity: values for identity, values in committed.items() if len(values) > 1}
    proof = {'scope': 'COMMIT-marked physical copies; visibility is not inferred',
             'status': 'not-applicable' if not committed else 'not-proven',
             'idsContiguous': None, 'sameTwoTransactions': None, 'copies': []}
    if committed:
        ids = sorted(committed)
        proof['idsContiguous'] = all(right == left + 1 for left, right in zip(ids, ids[1:]))
        pairs = [tuple(sorted(copy['transaction'] for copy in values)) for values in committed.values()]
        same_pair = all(len(pair) == 2 and pair[0] != pair[1] and pair == pairs[0] for pair in pairs)
        proof['sameTwoTransactions'] = same_pair
        ordered_ids = []
        if same_pair:
            for transaction in pairs[0]:
                block = sorted((copy['offset'], identity) for identity, values in committed.items()
                               for copy in values if copy['transaction'] == transaction)
                offsets, sequence = [entry[0] for entry in block], [entry[1] for entry in block]
                contiguous = all(right == left + 1 for left, right in zip(offsets, offsets[1:]))
                proof['copies'].append({'transaction': transaction, 'firstOffset': offsets[0],
                                        'lastOffset': offsets[-1], 'count': len(offsets), 'offsetsContiguous': contiguous})
                ordered_ids.append(sequence)
            proof['sameIdOrder'] = ordered_ids[0] == ordered_ids[1]
            if proof['idsContiguous'] and proof['sameIdOrder'] and all(copy['offsetsContiguous'] for copy in proof['copies']):
                proof['status'] = 'proven-in-retained-bytes'
    return duplicates, proof


def transaction_history(evidence, log_root, prefix, reader):
    data = reader.receipt(evidence.get('transactions'), log_root, 'Coordinator history')
    if data is None:
        return []
    if not isinstance(prefix, str) or not prefix:
        reader.diagnostic('kafka.sink-prefix-missing', 'No sink transactional ID prefix; use --sink-prefix')
        return []
    observations = object_section(data, 'observations', reader)
    if observations.get('decoded') is not True:
        reader.diagnostic('kafka.history-incomplete', 'Coordinator decoder reports incomplete/unsupported history')
    records = observations.get('coordinatorRecords')
    if not isinstance(records, list):
        reader.diagnostic('kafka.history-missing', 'No retained coordinator records')
        return []
    history, seen = [], {}
    for item in records:
        if not isinstance(item, dict) or not isinstance(item.get('record'), dict) or not isinstance(item.get('source'), dict):
            reader.diagnostic('kafka.history-malformed', 'Malformed coordinator record/source')
            continue
        record, source = item['record'], item['source']
        if not isinstance(record.get('transactionalId'), str):
            reader.diagnostic('kafka.history-id-unavailable', 'Coordinator transactional ID could not be decoded')
            continue
        if not record['transactionalId'].startswith(prefix):
            continue
        if source.get('topic') != '__transaction_state' or not integer(source.get('partition')) or not integer(item.get('offset')):
            reader.diagnostic('kafka.history-source-malformed', 'Coordinator source/offset is malformed')
            continue
        value = record.get('decoded')
        if record.get('status') not in ('DECODED', 'TOMBSTONE') or (record['status'] == 'DECODED' and not isinstance(value, dict)):
            reader.diagnostic('kafka.history-semantics-unavailable', record.get('detail', record.get('status')))
        entry = {'transactionalId': record['transactionalId'], 'partition': source['partition'],
                 'offset': item['offset'], 'status': record.get('status'), 'value': value}
        key = (source['partition'], item['offset'])
        if key in seen:
            if seen[key] != entry:
                reader.diagnostic('kafka.history-conflict', 'Conflicting coordinator records at one offset', str(key))
            continue
        seen[key] = entry
        history.append(entry)
    if not history:
        reader.diagnostic('kafka.history-prefix-absent', 'No retained coordinator record for sink prefix ' + prefix)
    return sorted(history, key=lambda item: (item['partition'], item['offset']))


def taskmanager_markers(directory, patterns, reader):
    if directory is None:
        reader.diagnostic('flink.logs-missing', 'No authorized TaskManager log directory; use --flink-log-dir')
        return []
    try:
        directory = guarded_path(directory, reader.roots)
        if not directory.is_dir():
            raise EvidenceError('TaskManager log directory is missing')
        paths = sorted(directory.glob('flink-stability-output-taskmanager-*-*.log'))
    except (OSError, ValueError) as error:
        reader.diagnostic('flink.logs-unreadable', error, directory)
        return []
    if not paths:
        reader.diagnostic('flink.logs-missing', 'No retained TaskManager output logs', directory)
    matches = []
    for path in paths:
        try:
            path = guarded_path(path, reader.roots)
            if not path.is_file() or path.stat().st_size > MAX_LOG_BYTES:
                raise EvidenceError('Missing regular log or retained log size limit exceeded')
            text = path.read_text(errors='strict')
            found = []
            for number, line in enumerate(text.splitlines(), 1):
                matching = [regex.pattern for regex in patterns if regex.search(line)]
                if matching:
                    found.append({'lineNumber': number, 'patterns': matching, 'line': line})
            matches.append({'file': str(path), 'matches': found})
        except (OSError, ValueError) as error:
            reader.diagnostic('flink.log-unreadable', error, path)
    return matches


def analyze(result_path, log_root, *, evidence_roots=(), flink_log_dir=None,
            markers=(), output_topic=None, sink_prefix=None):
    result_path, log_root = guarded_path(result_path), guarded_path(log_root)
    roots = [result_path.parent, log_root, *(guarded_path(path) for path in evidence_roots)]
    if flink_log_dir is not None:
        flink_log_dir = guarded_path(flink_log_dir)
        roots.append(flink_log_dir)
    reader = Reader(roots)
    result = reader.read_json(result_path) or {}
    if result.get('status') not in ('pass', 'fail', 'inconclusive'):
        reader.diagnostic('result.verdict-unavailable', 'Scenario verdict is missing or malformed')
    evidence = object_section(result, 'evidence', reader)
    terminal = object_section(evidence, 'terminalValidation', reader)
    job = object_section(evidence, 'flinkJob', reader)
    counts = ('expected', 'observed', 'distinctExpected', 'missing', 'duplicates', 'malformed', 'unexpected')
    for key in counts:
        if not integer(terminal.get(key)) or terminal[key] < 0:
            reader.diagnostic('oracle.count-unavailable', 'Terminal oracle count unavailable: ' + key)
    kills = evidence.get('taskManagerKills')
    if not isinstance(kills, list):
        reader.diagnostic('kill.evidence-missing', 'TaskManager kill evidence missing/malformed')
        kills = []
    windows = []
    for kill in kills:
        if not isinstance(kill, dict):
            reader.diagnostic('kill.evidence-malformed', 'Kill entry is not an object')
            continue
        window = kill.get('checkpointWindow')
        if not isinstance(window, dict):
            reader.diagnostic('kill.window-unavailable', 'Kill has no checkpoint-window evidence')
            continue
        if not integer(window.get('checkpointId')) or not isinstance(window.get('restWindowConfirmed'), bool):
            reader.diagnostic('kill.window-fields-malformed', 'Checkpoint ID or confirmed flag is missing/malformed')
        windows.append({'checkpointId': window.get('checkpointId'),
                        'restWindowConfirmed': window.get('restWindowConfirmed'),
                        'observationFailure': window.get('observationFailure'),
                        'restoredCheckpoint': kill.get('restoredCheckpoint'),
                        'brokerBeforeKill': summarize_snapshots(window.get('brokerBeforeKill'), 'brokerBeforeKill', reader),
                        'brokerAfterKill': summarize_snapshots(window.get('brokerAfterKill'), 'brokerAfterKill', reader)})
    logs = object_section(evidence, 'kafkaLogs', reader)
    if logs.get('status') != 'collected':
        reader.diagnostic('kafka.capture-incomplete', 'Kafka capture status: ' + str(logs.get('status')))
    for message in logs.get('diagnostics', []) if isinstance(logs.get('diagnostics'), list) else []:
        reader.diagnostic('kafka.capture-diagnostic', message)
    batches = decoded_batches(logs, log_root, reader)
    if output_topic is None:
        topics = {item['topic'] for item in batches if item['topic'] != '__transaction_state'
                  and item['batch']['transactional'] and not item['batch']['control']}
        if len(topics) == 1:
            output_topic = topics.pop()
        else:
            reader.diagnostic('kafka.output-topic-unavailable', 'Cannot select one transactional output topic; use --output-topic')
    transactions = group_transactions(batches, output_topic, reader)
    if not transactions:
        reader.diagnostic('kafka.transactions-missing', 'No decoded output transactions')
    duplicates, block = duplicate_evidence(transactions)
    committed_extra = sum(max(0, sum(copy['outcome'] == 'COMMIT' for copy in item['copies']) - 1)
                          for item in duplicates)
    if integer(terminal.get('duplicates')) and terminal['duplicates'] != committed_extra:
        reader.diagnostic('oracle.physical-copy-count-differs',
                          'Oracle duplicate count differs from retained COMMIT-marked extra copies; physical markers do not establish read-committed visibility')
    if any(item['code'].startswith(('kafka.batch-', 'kafka.offset-', 'kafka.decode-', 'kafka.id-', 'evidence.')) for item in reader.diagnostics):
        if block['status'] == 'proven-in-retained-bytes':
            block['status'] = 'not-proven-incomplete-evidence'
    prefix = sink_prefix
    if prefix is None and isinstance(evidence.get('sinkTransactions'), dict):
        prefix = evidence['sinkTransactions'].get('transactionalIdPrefix')
    history = transaction_history(logs, log_root, prefix, reader)
    if flink_log_dir is None:
        retained = evidence.get('checkpointRetention')
        if isinstance(retained, dict) and retained.get('status') == 'complete':
            retained_root = retained.get('checkpointRoot')
            if isinstance(retained_root, str) and retained_root:
                flink_log_dir = retained_root
            else:
                reader.diagnostic('flink.retained-root-malformed', 'Complete checkpoint retention has no checkpointRoot path')
        if flink_log_dir is None and isinstance(result.get('attempt'), dict):
            flink_log_dir = result['attempt'].get('checkpointRoot')
        if isinstance(flink_log_dir, str) and not Path(flink_log_dir).is_absolute():
            flink_log_dir = result_path.parent / flink_log_dir
        elif flink_log_dir is not None and not isinstance(flink_log_dir, str):
            reader.diagnostic('flink.log-root-malformed', 'checkpointRoot is not a path string')
            flink_log_dir = None
    compiled = [re.compile(pattern) for pattern in (markers or [DEFAULT_MARKER])]
    marker_matches = taskmanager_markers(flink_log_dir, compiled, reader)
    report = {'scope': 'Retained physical evidence only; markers do not prove consumer visibility or fault attribution.',
              'scenario': result.get('scenario'),
              'verdict': {key: result.get(key) for key in ('status', 'reason', 'message')},
              'attempt': result.get('attempt'), 'expectation': result.get('expectation'),
              'oracle': {**{key: terminal.get(key) for key in ('status', 'reason', 'mode', 'completed', 'snapshotComplete', *counts)},
                         **oracle_samples(terminal, reader)},
              'checkpoints': {key: job.get(key) for key in ('status', 'completedCheckpoints', 'restoredCheckpoints', 'latestRestoredCheckpoint')},
              'killWindows': windows, 'outputTopic': output_topic, 'transactions': transactions,
              'duplicateIds': duplicates, 'duplicateBlockProof': block,
              'physicalCommittedExtraCopies': committed_extra,
              'sinkPrefix': prefix, 'transactionStateHistory': history,
              'taskManagerMarkers': marker_matches, 'diagnostics': reader.diagnostics}
    if not all(integer(job.get(key)) for key in ('completedCheckpoints', 'restoredCheckpoints')):
        reader.diagnostic('flink.checkpoint-counts-unavailable', 'Checkpoint/restore counts are missing or malformed')
    return report


def render(report):
    lines = [report['scope'], 'Verdict: ' + json.dumps(report['verdict'], sort_keys=True),
             'Oracle: ' + json.dumps(report['oracle'], sort_keys=True),
             'Checkpoints/restores: ' + json.dumps(report['checkpoints'], sort_keys=True)]
    for window in report['killWindows']:
        lines.append('Kill window: ' + json.dumps(window, sort_keys=True))
    lines.append('Output topic: ' + str(report['outputTopic']))
    for transaction in report['transactions']:
        summary = {key: value for key, value in transaction.items() if key != 'records'}
        ids = [record['id'] for record in transaction['records'] if record['id'] is not None]
        summary.update(recordCount=len(transaction['records']), idSamples=ids[:10])
        lines.append('Transaction: ' + json.dumps(summary, sort_keys=True))
    for item in report['duplicateIds']:
        lines.append('Duplicate physical ID: ' + json.dumps(item, sort_keys=True))
    lines.append('Duplicate block proof: ' + json.dumps(report['duplicateBlockProof'], sort_keys=True))
    for item in report['transactionStateHistory']:
        lines.append('Transaction state: ' + json.dumps(item, sort_keys=True))
    for process in report['taskManagerMarkers']:
        lines.append('TaskManager log: ' + process['file'] + ' matches=' + str(len(process['matches'])))
        for match in process['matches']:
            lines.append(str(match['lineNumber']) + ': ' + match['line'])
    lines.extend('Diagnostic: ' + json.dumps(item, sort_keys=True) for item in report['diagnostics'])
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('result', type=Path)
    parser.add_argument('kafka_log_output', type=Path)
    parser.add_argument('--evidence-root', action='append', type=Path, default=[], help='Additional authorized retained-evidence root')
    parser.add_argument('--flink-log-dir', type=Path, help='Explicit retained TaskManager output directory')
    parser.add_argument('--marker', action='append', default=[], help='TaskManager Python regex; repeatable, replaces the upstream recovery default')
    parser.add_argument('--output-topic', help='Required when decoded transactional output topic is ambiguous')
    parser.add_argument('--sink-prefix', help='Explicit sink prefix when absent from the result')
    parser.add_argument('--json', action='store_true', help='Print the structured analysis instead of text')
    args = parser.parse_args(argv)
    try:
        report = analyze(args.result, args.kafka_log_output, evidence_roots=args.evidence_root,
                         flink_log_dir=args.flink_log_dir, markers=args.marker,
                         output_topic=args.output_topic, sink_prefix=args.sink_prefix)
    except (ValueError, OSError, re.error) as error:
        parser.error(str(error))
    print(json.dumps(report, indent=2) if args.json else render(report), end='\n' if args.json else '')
    # This is a diagnostic tool, not a replacement oracle or a PASS/FAIL exit gate.
    return 0 if report['verdict']['status'] is not None else 2


if __name__ == '__main__':
    sys.exit(main())
