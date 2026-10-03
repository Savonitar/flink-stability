"""Fault receipts and paired data comparison for the PR gate; no runtime operations."""
from collections import Counter
import re

COMMITTER = "org.apache.flink.connector.kafka.sink.internal.KafkaCommitter"


def fault_requirements(text):
    """Read only the atomic fault forms used by the explicit profiles; never infer success."""
    required = Counter()
    lines = text.splitlines()
    for index, line in enumerate(lines):
        match = re.match(r"^(\s*)- (network_fault|broker_fault|packet_fault|kill|restart|leader_fault):", line)
        if not match:
            continue
        indent, kind = len(match[1]), match[2]
        block = [line]
        for following in lines[index + 1:]:
            if following.strip() and not following.lstrip().startswith('#') and len(following) - len(following.lstrip()) <= indent:
                break
            block.append(following)
        body = '\n'.join(block)
        if kind == 'packet_fault': required['packetFaults'] += 1
        elif kind == 'leader_fault': required['flinkHa'] += 1
        elif kind == 'network_fault':
            required['networkFaults'] += 1
            if re.search(r'^\s+restart:\s*\{\s*component:\s*taskmanager\b', body, re.M):
                required['taskManagerKills'] += 1
        elif kind == 'broker_fault': required['brokerOperations'] += 6 if re.search(r'mode:\s*rolling-restart\b', body) else 2
        elif re.search(r'role:\s*taskmanager\b', body): required['taskManagerKills'] += 1
        elif re.search(r'role:\s*broker\b|component:\s*kafka\b', body): required['brokerOperations'] += 1
    return dict(required)


def fault_status(evidence, required=None):
    keys = {'packetFaults': 'confirmed', 'networkFaults': 'triggered', 'brokerOperations': 'confirmed', 'taskManagerKills': 'confirmed'}
    if required is None:
        required = {key: len(evidence.get(key) or []) for key in keys if evidence.get(key)}
        if not required: return 'unavailable'
    if not required: return 'not-required'
    missing = False
    for key, count in required.items():
        if key == 'flinkHa':
            ha = evidence.get('flinkHa') or {}
            if len(ha.get('leaderFaults') or []) < count: missing = True
            elif ha.get('status') != 'confirmed': return 'unconfirmed'
            continue
        items = evidence.get(key)
        if not isinstance(items, list) or len(items) < count:
            missing = True
        if isinstance(items, list) and any(item.get(keys[key]) is not True for item in items):
            return 'unconfirmed'
    return 'unavailable' if missing else 'confirmed'


def committer_counts(errors):
    if not isinstance(errors, dict): return {'ERROR': None, 'WARN': None, 'coverage': 'unavailable'}
    counts = Counter(event.get('level') for event in errors.get('events', []) if event.get('logger') == COMMITTER)
    return {level: counts[level] for level in ('ERROR', 'WARN')} | {'coverage': errors.get('coverage', 'unavailable')}


def data_difference(rows, expected_runs=None):
    """Candidate-only positive counts require a complete, verified zero baseline for that metric."""
    names = sorted({row['scenario'] for row in rows})
    findings, unresolved = [], []
    for name in names:
        sides = {side: [row for row in rows if row['scenario'] == name and row['side'] == side] for side in ('baseline', 'candidate')}
        complete = all(sides.values()) and len(sides['baseline']) == len(sides['candidate'])
        if expected_runs is not None: complete &= all(len(values) == expected_runs for values in sides.values())
        complete &= all(row['subjectOk'] and row.get('faultStatus') in ('confirmed', 'not-required')
                        and row['verdict'] in ('pass', 'fail') and row['exitCode'] == (0 if row['verdict'] == 'pass' else 1)
                        for values in sides.values() for row in values)
        modes = {row.get('oracleMode', 'exactly-once') for values in sides.values() for row in values}
        complete &= len(modes) == 1 and modes <= {'exactly-once', 'at-least-once'}
        metrics = ('missing',) if modes == {'at-least-once'} else ('missing', 'duplicates')
        for metric in metrics:
            known = complete and all(type(row.get(metric)) is int and row[metric] >= 0 for values in sides.values() for row in values)
            if not known:
                unresolved.append(name + '/' + metric)
            elif any(row[metric] > 0 for row in sides['candidate']) and all(row[metric] == 0 for row in sides['baseline']):
                findings.append(name + '/' + metric)
    return {'status': 'yes' if findings else 'unknown' if unresolved or not names else 'no', 'findings': findings, 'unresolved': unresolved}


def coverage_table(rows):
    lines = ['| Scenario | Side | Verdicts | Missing (sum) | Duplicates (sum) | KafkaCommitter ERROR | KafkaCommitter WARN | Log coverage | Fault effect |',
             '| --- | --- | --- | --- | --- | --- | --- | --- | --- |']
    def total(values):
        return str(sum(values)) if values and all(type(value) is int and value >= 0 for value in values) else 'unknown'
    for name in dict.fromkeys(row['scenario'] for row in rows):
        for side in ('baseline', 'candidate'):
            selected = [row for row in rows if row['scenario'] == name and row['side'] == side]
            outcomes = ', '.join(f'{key}: {value}' for key, value in sorted(Counter(row['verdict'] for row in selected).items())) or 'unavailable'
            logs = [committer_counts(row.get('componentErrors')) for row in selected]
            effects = ', '.join(f'{key}: {value}' for key, value in sorted(Counter(row.get('faultStatus', 'unavailable') for row in selected).items())) or 'unavailable'
            lines.append(f"| {name} | {side} | {outcomes} | {total([r.get('missing') for r in selected])} | {total([r.get('duplicates') for r in selected])} | "
                         f"{total([v['ERROR'] for v in logs])} | {total([v['WARN'] for v in logs])} | {', '.join(sorted({v['coverage'] for v in logs})) or 'unavailable'} | {effects} |")
    return lines
