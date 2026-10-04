#!/usr/bin/env python3
"""Audit retained pool-reuse runs. Never launch Docker or change an expected outcome."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).absolute().parents[2]
sys.path.insert(0, str(ROOT / 'tools'))
from pr_gate import summarize

RELEASE = '6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da'
SOURCE = 'c05dcdc7d7256575f10f784c728f983b517058daee517ae258021edd1e87ecfe'
ENTRY = 'org/apache/flink/connector/kafka/sink/internal/ProducerPoolImpl.class'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path, scope, digest=None):
    path = Path(path).absolute()
    scope = Path(scope).absolute()
    require('..' not in path.parts and path.is_relative_to(scope), 'Evidence path escapes scope')
    cursor = path.anchor and Path(path.anchor)
    for part in path.parts[1:]:
        cursor /= part
        require(not cursor.is_symlink(), 'Symlink evidence rejected')
    require(path.stat().st_size <= 64 * 1024 * 1024, 'Evidence exceeds byte bound')
    data = path.read_bytes()
    if digest:
        require(hashlib.sha256(data).hexdigest() == digest, 'Evidence hash mismatch: ' + str(path))
    return data


def load(path, scope, digest=None):
    return json.loads(read(path, scope, digest))


def physical_transactions(batches):
    """One-partition data ranges closed by explicit markers; retain aborted/open ranges too."""
    pending, finished = defaultdict(list), []
    last = -1
    for batch in sorted(batches, key=lambda item: item['baseOffset']):
        require(batch['baseOffset'] > last, 'Overlapping output log batches')
        last = batch['lastOffset']
        require(batch['transactional'], 'Nontransactional output')
        pid, epoch = batch['producerId'], batch['producerEpoch']
        for record in batch['records']:
            if batch['control']:
                marker = record['marker']['type']
                require(marker in ('COMMIT', 'ABORT'), 'Unknown control marker')
                records = pending.pop(pid, [])
                require(all(item['epoch'] <= epoch for item in records), 'Marker cannot close a newer epoch')
                finished.append({'producerId': pid, 'markerEpoch': epoch, 'marker': marker,
                                 'markerOffset': record['offset'], 'markerTimestamp': record['timestamp'],
                                 'records': records})
            else:
                require(isinstance(record['canonicalId'], int), 'Malformed output ID in physical evidence')
                pending[pid].append(dict(record, epoch=epoch))
    return finished, dict(pending)


def transaction_evidence(run, result):
    evidence = result['evidence']['kafkaLogs']
    require(evidence['status'] == 'collected' and not evidence['diagnostics'], 'Incomplete Kafka capture')
    receipt = evidence['transactions']
    require(receipt['status'] == 'DECODED', 'Coordinator history not decoded')
    history = load(receipt['evidence'], run, receipt['sha256'])['observations']
    require(history['decoded'] and not history['diagnostics'], 'Unknown transaction semantics')
    archives = {item['path']: item for item in evidence['archives']}
    batches = []
    for receipt in evidence['decoded']:
        archive = archives[receipt['archive']]
        read(archive['path'], run, archive['sha256'])
        require(archive['workerFinished'] and archive['status'] == 'TRANSPORT_EOF', 'Incomplete archive')
        decoded = load(receipt['evidence'], run, receipt['sha256'])
        require(decoded['inputVerified'] and decoded['tarComplete'] and decoded['allLogsDecoded'], 'Incomplete segment')
        if archive['topic'] == 'output':
            require(archive['partition'] == 0, 'Calibration requires one output partition')
            for log in decoded['logs']:
                batches.extend(log['segment']['batches'])
    require(batches and batches[0]['baseOffset'] == 0, 'Output history does not start at zero')
    transactions, pending = physical_transactions(batches)
    visible = [record['canonicalId'] for txn in transactions if txn['marker'] == 'COMMIT' for record in txn['records']]
    counts = Counter(visible)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    oracle = result['evidence']['terminalValidation']
    if oracle.get('snapshotComplete'):
        require(len(visible) == oracle['observed'], 'Physical commit reconstruction disagrees with read_committed count')
        require(sum(count - 1 for count in counts.values()) == oracle['duplicates'], 'Physical duplicates disagree with oracle')
        require(len(set(range(oracle['expected'])) - counts.keys()) == oracle['missing'], 'Physical missing IDs disagree with oracle')
    return history['coordinatorRecords'], transactions, pending, duplicates


def logs(run, expected_hash, incarnations):
    directory = run / 'logs'
    require(directory.is_dir(), 'Attempt logs/ missing')
    outputs, receipts = {}, {}
    for path in directory.glob('*.log'):
        data = read(path, run)
        text = data.decode('utf-8', errors='replace')
        receipts[path.name] = hashlib.sha256(data).hexdigest()
        if path.name.startswith('flink-stability-output-'):
            outputs[path.name] = text
    for number in range(1, incarnations + 1):
        match = [path for path in directory.glob('*.log')
                 if 'output-' not in path.name and f'taskmanager-1-{number}' in path.name]
        require(len(match) == 1, 'Missing/ambiguous TaskManager class log')
        lines = read(match[0], run).decode().splitlines()
        pool = [line for line in lines if 'org.apache.flink.connector.kafka.sink.internal.ProducerPoolImpl source:' in line]
        require(pool and all(expected_hash in line for line in pool), 'ProducerPoolImpl origin mismatch')
    return outputs, receipts


def single_partition_sample(sample):
    # Historical receipts used one object; current receipts enumerate every sink partition.
    samples = sample if isinstance(sample, list) else [sample]
    require(len(samples) == 1 and isinstance(samples[0], dict),
            'Calibration requires exactly one observed sink partition')
    return samples[0]


def window(run, result, histories, physical, output_logs):
    kills = result['evidence']['taskManagerKills']
    require(len(kills) == 1 and kills[0]['confirmed'], 'Kill effect not confirmed')
    kill = kills[0]
    observation = kill['checkpointWindow']
    require(observation['observationFailure'] is None,
            'checkpoint-window.observation-infrastructure: ' + str(observation['observationFailure']))
    k = observation['checkpointId']
    for sample in ['armed', 'beforeKill', 'afterKill']:
        stats = observation[sample]
        active = [item for item in stats['history'] if item['status'] == 'IN_PROGRESS']
        completed = stats['latest']['completed']
        require(len(active) == 1 and active[0]['id'] == k and completed['id'] == k-1
                and completed['status'] == 'COMPLETED'
                and completed['latest_ack_timestamp'] < active[0]['trigger_timestamp'],
                'checkpoint-window.missed: REST window')
    require(kill.get('restoredCheckpoint') == k - 1, 'Wrong restored checkpoint')
    before, after = kill['jobManagerTimeBeforeKill'], kill['jobManagerTimeAfterKill']
    require(before <= after < kill['restoredAtMillis'], 'Invalid kill/restore clock bounds')
    stats = observation['beforeKill']
    trigger = next(entry['trigger_timestamp'] for entry in stats['history'] if entry['id'] == k)
    vertices = [vertex for vertex in stats['observedJobVertices'] if 'Kafka Sink: Writer' in vertex['name']]
    require(len(vertices) == 1, 'Ambiguous sink vertex')
    task = stats['observedCheckpointDetails']['tasks'][vertices[0]['id']]
    require(task['id'] == k and task['num_subtasks'] == task['num_acknowledged_subtasks'] == 1
            and task['latest_ack_timestamp'] >= trigger,
            'checkpoint-window.missed: sink snapshot not acknowledged')
    require(task['latest_ack_timestamp'] <= before, 'Sink acknowledgement after kill')
    original = [text for name, text in output_logs.items() if 'taskmanager-1-1' in name]
    restored = [text for name, text in output_logs.items() if 'taskmanager-1-2' in name]
    require(len(original) == len(restored) == 1, 'Missing original/restored logs')
    matches = re.findall(r'POOL_REUSE_SNAPSHOT_ASYNC_START checkpoint=' + str(k) + r' delayMs=(\d+) timeMs=(\d+)', original[0])
    require(len(matches) == 1, 'Missing/ambiguous async snapshot start')
    delay, start = map(int, matches[0])
    margin = start + delay - after
    require(trigger <= start <= before and margin >= 3000, 'checkpoint-window.missed: less than 3 seconds remaining')
    require(not re.search(r'POOL_REUSE_SNAPSHOT_ASYNC_DONE checkpoint=' + str(k) + r'\b', original[0]), 'Killed checkpoint completed async work')
    by_id = defaultdict(list)
    for item in histories:
        record = item['record']
        require(record['status'] == 'DECODED', 'Unknown coordinator record')
        by_id[record['transactionalId']].append(dict(record['decoded'], offset=item['offset']))
    reused = []
    for identity, history in by_id.items():
        history.sort(key=lambda item: item['offset'])
        committed = [entry for entry in history if entry['stateName'] == 'CompleteCommit' and entry['lastUpdateTimestampMs'] < trigger]
        ongoing = [entry for entry in history if entry['stateName'] == 'Ongoing' and trigger <= entry['startTimestampMs'] <= before]
        if not committed or not ongoing:
            continue
        require(len(ongoing) == 1, 'Multiple candidate reused transactions')
        old, current = committed[-1], ongoing[0]
        require(old['producerId'] == current['producerId'], 'Reused producer ID changed unexpectedly')
        require(kill['restoredAtMillis'] < current['startTimestampMs'] + current['timeoutMs'], 'Restore after broker transaction timeout')
        require(current['timeoutMs'] == 7200000, 'Harness transaction timeout differs from 7200000 ms')
        before_broker = single_partition_sample(observation['brokerBeforeKill'])
        after_broker = single_partition_sample(observation['brokerAfterKill'])
        require(before_broker['topic'] == after_broker['topic'] == 'output'
                and before_broker['partition'] == after_broker['partition'] == 0, 'Wrong broker observation partition')
        producer_before = [p for p in before_broker['producers'] if p['producerId'] == current['producerId']]
        producer_after = [p for p in after_broker['producers'] if p['producerId'] == current['producerId']]
        require(len(producer_before) == len(producer_after) == 1, 'Missing/ambiguous reused producer at kill')
        pb, pa = producer_before[0], producer_after[0]
        require(pb['epoch'] == pa['epoch'] == current['producerEpoch'], 'Reused producer epoch changed across kill')
        first_offset = pb['transactionStartOffset']
        require(first_offset is not None and first_offset == pa['transactionStartOffset'], 'Reused transaction not open across kill')
        require(pb['lastSequence'] <= pa['lastSequence'], 'Producer sequence moved backwards across kill')
        chunks = [txn for txn in physical if txn['producerId'] == current['producerId']
                  and txn['records'] and txn['records'][0]['offset'] == first_offset]
        require(len(chunks) == 1, 'No unique physical reused transaction')
        chunk = chunks[0]
        records = chunk['records']
        require(all(record['epoch'] == pa['epoch'] for record in records), 'Multiple sessions in reused block')
        require(records[-1]['sequence'] == pa['lastSequence'], 'Reused block extends beyond post-kill producer state')
        require(records[-1]['offset'] < after_broker['logEndOffset'], 'Reused block extends beyond post-kill log end')
        before_records = [record for record in records if record['sequence'] <= pb['lastSequence']]
        require(200 <= len(before_records) <= len(records) <= 900,
                'checkpoint-window.missed: reused block outside 200..900 records at kill')
        ids = [record['canonicalId'] for record in records]
        require(ids == list(range(ids[0], ids[0] + len(ids))), 'Reused records are not one contiguous sequence')
        preceding = [txn for txn in physical if txn['producerId'] == old['producerId']
                     and txn['marker'] == 'COMMIT' and txn['markerTimestamp'] < trigger and txn['records']]
        require(preceding, 'Prior committed data transaction absent')
        prior_chunk = max(preceding, key=lambda txn: txn['markerOffset'])
        checkpoint_epochs = {record['epoch'] for record in prior_chunk['records']}
        require(len(checkpoint_epochs) == 1, 'Ambiguous checkpoint session epoch')
        checkpoint_epoch = next(iter(checkpoint_epochs))
        resumes = re.findall(r'Attempting to resume transaction ' + re.escape(identity) + r' with producerId (\d+) and epoch (\d+)', restored[0])
        require((str(old['producerId']), str(checkpoint_epoch)) in resumes, 'Missing stale session resume on restored attempt')
        same_epoch = checkpoint_epoch == current['producerEpoch']
        recommits = [entry for entry in history if entry['stateName'] == 'CompleteCommit'
                     and entry['producerId'] == old['producerId'] and entry['producerEpoch'] == checkpoint_epoch
                     and entry['lastUpdateTimestampMs'] > after and entry['offset'] > current['offset']]
        signature = same_epoch and bool(recommits) and chunk['marker'] == 'COMMIT'
        reused.append({'transactionalId': identity, 'producerId': old['producerId'],
                       'checkpointEpoch': checkpoint_epoch, 'reusedEpoch': current['producerEpoch'],
                       'previousCommit': old, 'reusedOngoing': current,
                       'recommit': recommits[0] if recommits else None,
                       'recordCount': len(ids), 'recordsObservedBeforeKill': len(before_records), 'transactionStartOffset': first_offset, 'lastSequenceBeforeKill': pb['lastSequence'], 'lastSequenceAfterKill': pa['lastSequence'], 'firstId': ids[0], 'lastId': ids[-1],
                       'marker': chunk['marker'], 'sameSessionRecommitSignature': signature})
    require(len(reused) == 1, 'Expected exactly one reused transactional ID')
    return {'confirmed': True, 'checkpointId': k, 'restoredCheckpoint': k - 1,
            'triggerTimestamp': trigger, 'sinkAckTimestamp': task['latest_ack_timestamp'],
            'asyncStartTimestamp': start, 'asyncDelayMs': delay, 'killBeforeMs': before,
            'killAfterMs': after, 'minimumCompletionMarginMs': margin,
            'restoreTimestamp': kill['restoredAtMillis'], 'reused': reused}


def audit_run(run, expected_hash, fault):
    result = load(run / 'stdout.json', run)
    exit_code = int(read(run / 'exit-code.txt', run))
    row = summarize(result['scenario'], '', 0, result, expected_hash, exit_code)
    require(row['subjectOk'], 'Subject origin unconfirmed')
    outputs, receipts = logs(run, expected_hash, 2 if fault else 1)
    history, physical, pending, duplicates = transaction_evidence(run, result)
    unresolved = result['evidence']['sinkTransactions']
    require(unresolved['status'] == 'listed', 'Sink transaction listing unavailable')
    report = {'verdict': result['status'], 'reason': result['reason'], 'exitCode': exit_code,
              'terminalValidation': result['evidence']['terminalValidation'],
              'componentErrors': result['evidence']['componentErrors'],
              'flinkRest': result['evidence'].get('flinkRest'),
              'unresolvedTransactions': unresolved['unresolved'], 'logSha256': receipts,
              'duplicateBlock': {'count': len(duplicates), 'first': duplicates[0] if duplicates else None,
                                 'last': duplicates[-1] if duplicates else None,
                                 'contiguous': bool(duplicates) and duplicates == list(range(duplicates[0], duplicates[-1]+1))}}
    if fault:
        open_chunks = [{'producerId': pid, 'marker': 'OPEN', 'records': records} for pid, records in pending.items()]
        report['killWindow'] = window(run, result, history, physical + open_chunks, outputs)
        report['fencedResumeObserved'] = any('KafkaCommitter' in line and ('fenced' in line.lower() or 'invalid' in line.lower())
                                            for name, text in outputs.items() if 'taskmanager-1-2' in name for line in text.splitlines())
    else:
        require(not result['evidence']['taskManagerKills'], 'Control contains a kill')
    report['hangingTransactionFinding'] = bool(unresolved['unresolved']) or 'timeout' in result['reason']
    return report


def check_matrix(controls, faults, build, supplement=None):
    require(build['releaseSha256'] == RELEASE and build['sourceSha256'] == SOURCE, 'Unpinned release/source')
    require(build['artifacts']['mutant']['changedEntries'] == [ENTRY], 'Not a one-class mutant')
    report = {'qualified': False, 'runs': [], 'errors': [],
              'releaseSha256': RELEASE, 'mutantSha256': build['artifacts']['mutant']['sha256'],
              'signatureDefinition': 'Same transactional ID and producer ID/epoch: CompleteCommit before k, Ongoing after k, stale resume after kill, second CompleteCommit closing the reused record block.',
              'signatureContract': 'README.md#calibration-contract', 'windowMisses': [], 'confirmedCounts': {}}
    directories = [(controls, False, False), (faults, True, False)]
    if supplement is not None:
        directories.append((supplement, True, True))
    for directory, fault, amended in directories:
        manifest = load(directory / 'manifest.json', directory)
        names = [f'pool-reuse-inflight-{"kill" if fault else "control"}-v{version}' for version in ([2] if amended else [1, 2])]
        require(manifest['scenarios'] == names, 'Wrong matrix scenario list')
        require(manifest['runs'] >= (3 if fault and not amended else 1), 'Insufficient independent repetitions')
        for side in ['baseline', 'candidate']:
            expected_hash = RELEASE if side == 'baseline' else report['mutantSha256']
            require(manifest[side]['connectorSha256'] == expected_hash, 'Wrong matrix connector')
            closure = manifest[side]['runtimeDependencySha256']
            require(closure is not None and {Path(path).name: digest for path, digest in closure.items()} == build['runtimeDependencySha256'], 'Wrong runtime closure')
            for name in names:
                for repetition in range(1, manifest['runs'] + 1):
                    run = directory / side / name / f'run-{repetition}'
                    item = {'batch': directory.name, 'side': side, 'scenario': name, 'run': repetition, 'directory': str(run)}
                    try:
                        raw = load(run / 'stdout.json', run)
                        item.update({'verdict': raw['status'], 'reason': raw['reason'], 'rawAttempt': raw['attempt'],
                                     'terminalValidation': raw['evidence'].get('terminalValidation'),
                                     'componentErrors': raw['evidence'].get('componentErrors'),
                                     'sinkTransactions': raw['evidence'].get('sinkTransactions')})
                        item.update(audit_run(run, expected_hash, fault))
                        if side == 'baseline' or not fault:
                            require(item['verdict'] == 'pass' and item['exitCode'] == 0, 'Release/control did not pass')
                            require(not item['hangingTransactionFinding'], 'Release/control has unresolved transactions')
                            if fault:
                                require(item['killWindow']['reused'][0]['marker'] == 'ABORT', 'Release did not abort reused block')
                        elif name.endswith('-v1'):
                            require(item['verdict'] == 'fail' and item['exitCode'] == 1
                                    and item['reason'] == 'validator.kafka.id-set.duplicate-ids', 'TV1 mutant did not expose duplicate IDs')
                            require(all(item['terminalValidation'][key] == 0 for key in ['missing', 'unexpected', 'malformed']), 'TV1 mutant has other data defects')
                            block = item['duplicateBlock']; reused = item['killWindow']['reused'][0]
                            require(block['contiguous'] and block['count'] == reused['recordCount']
                                    and block['first'] == reused['firstId'] and block['last'] == reused['lastId'], 'Duplicate block differs from reused records')
                            require(reused['sameSessionRecommitSignature'], 'Same-session stale re-commit signature absent')
                        else:
                            require(item['fencedResumeObserved'], 'TV2 fenced re-commit not observed')
                        key = side + '/' + name
                        report['confirmedCounts'][key] = report['confirmedCounts'].get(key, 0) + 1
                        item['qualificationVerdict'] = 'confirmed'
                    except (ValueError, KeyError, OSError, StopIteration) as error:
                        item['qualificationError'] = str(error)
                        if str(error).startswith('checkpoint-window.missed:'):
                            item['qualificationVerdict'] = 'inconclusive'
                            report['windowMisses'].append(f'{directory.name}/{side}/{name}/{repetition}: {error}')
                            if side == 'baseline' and item.get('rawAttempt', {}).get('status') != 'pass':
                                report['errors'].append(f'{side}/{name}/{repetition}: baseline data failure even though window missed')
                        else:
                            report['errors'].append(f'{side}/{name}/{repetition}: {error}')
                    report['runs'].append(item)
    for side in ['baseline', 'candidate']:
        for fault in [False, True]:
            for version in [1, 2]:
                name = f'pool-reuse-inflight-{"kill" if fault else "control"}-v{version}'
                count = report['confirmedCounts'].get(side + '/' + name, 0)
                if count < (3 if fault else 1):
                    report['errors'].append(f'{side}/{name}: only {count} confirmed attempts')
    report['observedMatrixSatisfied'] = not report['errors']
    report['qualified'] = not report['errors']
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--controls', type=Path, required=True)
    parser.add_argument('--faults', type=Path, required=True)
    parser.add_argument('--supplement', type=Path, help='Retained TV2 timing amendment, in addition to every original run')
    parser.add_argument('--build-evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = check_matrix(args.controls.absolute(), args.faults.absolute(), load(args.build_evidence, ROOT), args.supplement.absolute() if args.supplement else None)
    with args.output.open('x') as output:
        json.dump(report, output, indent=2); output.write('\n')
    print('CALIBRATION_ESTABLISHED' if report['qualified'] else 'CALIBRATION_NOT_ESTABLISHED')
    return 0 if report['qualified'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
