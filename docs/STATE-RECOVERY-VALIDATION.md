# PR2 state-recovery qualification — 2026-10-26

The fixed twelve-attempt matrix qualified: nine raw PASS results and three raw
mutant state-mismatch FAIL results. Every attempt retained all 6,000 IDs exactly
once with zero missing, duplicate, unexpected or malformed IDs. The checker
returned exit code 0 with no errors. No qualification attempt was retried or
replaced.

## Scope and artifacts

This qualification covers one JobManager, one TaskManager, parallelism one, one
input/output partition, `K=8`, and `N=6000` generated integer records. It uses
Flink 2.2.0, Kafka 4.0.0, Kafka connector 5.0.0-2.2, HashMap state without TTL,
filesystem checkpoints every 10 seconds, and an exactly-once Kafka sink with
INCREMENTING transaction naming.

The correct and reset-state-on-restore workload JARs were frozen with their
source copies, build receipts and SHA-256 identities. Both retain the same class
bytes, UID and state serializer. The mutant changes only the first keyed
aggregate transition for each key after genuine restore; initial startup does
not activate it. The artifact differences are limited to their manifest build
mode and private packaged identity resource.

- Correct workload SHA-256: `4c8ca4211b01d26ba0c49a2060259c117b95016fef798b4a0c187a332edd9711`.
- Mutant workload SHA-256: `0ad8e5eeec5deb8e732a346dffc54c3675a6cb7c086e5c1bfa23a1389798d54a`.
- [Sanitized build receipt](evidence/state-recovery-build-20261026-02/build-evidence.json).
- [Published frozen-batch manifest](evidence/state-recovery-qualification-20261026-01/manifest.json).
- [Compact final checker report](evidence/state-recovery-qualification-20261026-01/final-report.summary.json).

Pinned Docker image IDs:

- Flink: `sha256:2e93ded2cb7bf1a50f71b663d5e3ea01420bcc4e549cbf666d05120f939713c7`.
- Kafka: `sha256:2cecec9e3ea2d63fd5d1da1ab95e6bb7c33a7ff81053c4eb46f4176cd0bad8c9`.

The manifest also records the connector primary hash and full installed closure.
The original local evidence directories are ignored by Git; raw artifacts and logs
remain unchanged. The [publication manifest](evidence/checkpoint-storage-publication-manifest.json)
records original SHA-256 values separately from the sanitized-copy SHA-256 values,
with explicit omissions. Published copies cannot rerun the full raw-evidence check.
The [recipe](../calibration/state-recovery/README.md) documents
build, freeze, execution and checker commands. This batch used JDK 21 and
`run.py --docker-api-version 1.44`, setting both Docker client version contexts.

## Offline checks and retained pilots

JDK 21 `mvn -o -B -ntp install` completed successfully: 1,488 reported Java tests,
one opt-in Kafka graceful-stop Docker test skipped because
`flink.kafka.lifecycle` was unset. Python tooling checks passed all 258 tests.
CLI `validate --offline --show-plan` accepted all four pilot catalogs after the
reactor was installed; output is retained under
`jobs/state-recovery-validation-20261026-01/`. These checks establish
offline coverage; the live evidence below has its own qualification gate.

All four pilot02 cells independently confirmed their required outcomes: three
PASS controls and one raw mutant-restart FAIL. That mutant attempt retained all
6,000 IDs exactly once with zero ID defects, 4,514 state mismatches, eight reset
receipts, and a confirmed restore of checkpoint 2. Pilot evidence:
`jobs/state-recovery-pilots-20261026-02/`. Pilots are not part of the twelve-attempt
qualification batch.

Earlier attempts remain retained and excluded from qualification. Pilot01's
baseline lacked the required Kafka receipt; its restart attempt was INCONCLUSIVE
because restored checkpoint 2 had not been proven before injection. Build01
failed because the cache filter excluded a required public Maven dependency;
build02 succeeded after the filter correction. None of these earlier failures
or misses was relabeled as a qualified result.

## Fixed qualification matrix

Every catalog expectation remains PASS. The mutant-restart cell must retain raw
state-mismatch FAIL, including its nonzero CLI exit status; the independent
checker recognizes that failure as evidence of oracle sensitivity.

| Cell | Required result | Confirmed / fixed attempts | Observed raw outcomes |
| --- | --- | --- | --- |
| Correct, uninterrupted | PASS | `3 / 3` | `3 PASS` |
| Correct, restart | PASS with checkpoint recovery proof | `3 / 3` | `3 PASS` |
| Mutant, uninterrupted | PASS with no activation | `3 / 3` | `3 PASS` |
| Mutant, restart | Raw state-mismatch FAIL, zero ID defects, eight reset receipts | `3 / 3` | `3 FAIL: state-mismatch` |

Final totals: `9` PASS, `3` raw FAIL,
`0` INCONCLUSIVE, `0` missing or
invalid attempts. The target matrix is nine PASS and three raw mutant FAIL.
Checker exit code: `0`; qualification:
`qualified`.

All six restart attempts restored checkpoint 2 from the pre-kill proven set.
Each mutant restart emitted exactly eight reset receipts. State mismatch totals
were 4,821, 4,466 and 5,083; every control had zero state mismatches. The mutant
restart CLI exit codes remained 1, and their PASS expectations remained unmet.

Each counted attempt must retain exact artifact/runtime identities, complete
input and state-oracle evidence, natural FINISHED completion, physical fencing,
and valid lifecycle/cleanup evidence. Restart cells additionally require a
committed all-key prefix, a completed checkpoint proven before kill, the bound
replacement execution, and subsequent events for every key. Missing windows,
capture failures, infrastructure failures and cleanup failures leave the matrix
incomplete; their raw data outcomes remain visible.

A confirmed matrix qualifies only this bounded state-recovery case and the
oracle's sensitivity to the deliberate aggregate reset. It does not qualify
FUSE, checkpoint-storage I/O faults, other topologies/backends, rescaling, or
other Flink/Kafka/connector versions. The later
[FUSE prototype](CHECKPOINT-STORAGE-FUSE-PROTOTYPE.md) has its own separate
qualification; it does not extend this state-recovery matrix.
