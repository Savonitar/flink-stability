#!/usr/bin/env python3
"""Check retained calibration summaries. Does not launch scenarios or infer a PASS."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools'))
from chaos_profiles import PROFILES
from gate_evidence import data_difference


def check(report, control=False):
    manifest, rows = report['manifest'], report['rows']
    names = ['broker-eos-control'] if control else list(PROFILES['chaos-quick'])
    runs = manifest['runs']
    if not isinstance(runs, int) or runs < 1 or manifest['scenarios'] != names:
        raise ValueError('Wrong scenario matrix or repetition count')
    if manifest['plan'].get('producerMaxBlockMs') != 5000:
        raise ValueError('Expected shared calibration max.block.ms=5000')
    expected = {(name, side, run) for name in names for side in ('baseline', 'candidate')
                for run in range(1, runs + 1)}
    actual = [(row['scenario'], row['side'], row['run']) for row in rows]
    if len(actual) != len(expected) or set(actual) != expected:
        raise ValueError('Missing or duplicate matrix cells')
    for row in rows:
        if not row['subjectOk'] or row['faultStatus'] != ('not-required' if control else 'confirmed'):
            raise ValueError('Unverified artifact or fault: ' + row['scenario'])
        if row['side'] == 'baseline' or control:
            if (row['verdict'], row['exitCode'], row['missing'], row['duplicates']) != ('pass', 0, 0, 0):
                raise ValueError('Release/healthy control did not pass: ' + row['scenario'])
        elif row['verdict'] not in ('pass', 'fail') or row['exitCode'] != (0 if row['verdict'] == 'pass' else 1):
            raise ValueError('Candidate has an unresolved execution: ' + row['scenario'])
    if not control:
        eligible = {'commit-request-lost', 'commit-response-lost', 'broker-coordinator-pause'}
        eligible.update(name for name in names if name.startswith(('protocol-', 'coordinator-')))
        failures = [row for row in rows if row['side'] == 'candidate' and row['scenario'] in eligible
                    and row['verdict'] == 'fail' and isinstance(row['missing'], int) and row['missing'] > 0]
        difference = data_difference(rows, runs)
        if not failures or difference['status'] != 'yes' or difference['unresolved']:
            raise ValueError('No complete, candidate-only missing-ID result in the target fault classes')
    return 'CONTROL_PASS' if control else 'CALIBRATION_DETECTED'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('summary', type=Path)
    parser.add_argument('--control', action='store_true')
    parser.add_argument('--build-evidence', type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.summary.read_text())
    build = json.loads(args.build_evidence.read_text())
    if not build['matchesPinnedMutant']:
        raise SystemExit('Unpinned mutant build')
    for side, key in [('baseline', 'releaseSha256'), ('candidate', 'mutantSha256')]:
        subject = report['manifest'][side]
        if subject['connectorSha256'] != build[key]:
            raise SystemExit('Wrong calibration artifact: ' + side)
        dependencies = subject['runtimeDependencySha256']
        if dependencies is None or {Path(p).name: digest for p, digest in dependencies.items()} != build['runtimeDependencySha256']:
            raise SystemExit('Wrong runtime closure: ' + side)
    try:
        print(check(report, args.control))
    except (ValueError, KeyError, TypeError) as failure:
        raise SystemExit('CALIBRATION_NOT_ESTABLISHED: ' + str(failure))


if __name__ == '__main__':
    main()
