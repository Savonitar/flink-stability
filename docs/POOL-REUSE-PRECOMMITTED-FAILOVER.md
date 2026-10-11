# Tolerated checkpoint failure, then graceful failover

The four `pool-reuse-precommitted-{failover,control}-{v1,v2}` catalogs test an
exactly-once invariant: restoring a completed checkpoint must not publish a later
transaction that reused one of that checkpoint's transactional IDs. They use
Apache Flink 2.2.0, `org.apache.flink:flink-connector-kafka:5.0.0-2.2`, Apache Kafka
4.0, POOLING naming and this repository's
[same-ID calibration mutant](../calibration/pool-reuse-mutant/README.md).
All four expected-result siblings require PASS. A duplicate failure from the
intentionally broken TV1 mutant is calibration evidence, never an inverted
expectation that makes the gate pass.

These catalogs complement the [inflight-kill cells](POOL-REUSE-CALIBRATION.md).
The workload fails checkpoints and then its own task; the TaskManager process
survives and ordinary task cleanup runs. There is no `kill`, `restart` or other
fault step. `restart_strategy: { type: flink-default }` lets Flink restart the
checkpointed job in place. The single phase only awaits RUNNING; terminal
validation checks all 14,000 input IDs after the job finishes and the harness
fences every writer process.

## Why graceful cleanup leaves a recovery hazard

At completed checkpoint 1, the committer commits transactional ID X and the pool
recycles it. At barrier 2, the writer precommits its current producer and takes X
again for the next record interval. Checkpoint 2 then fails asynchronously. A
nonzero checkpoint-failure tolerance lets the task keep processing, while the
latest restore point remains checkpoint 1.

At barrier 3, the writer precommits X and hands its committable downstream before
selecting another producer Y. X now holds the records between barriers 2 and 3.
An unrelated task exception triggers graceful failover. In the released connector,
`ExactlyOnceKafkaWriter.close()` calls `abortCurrentProducer`, which aborts Y.
It does not abort every precommitted producer; the source comment also explains
why doing so would lose data in batch execution. Pooled producers close with a
zero timeout in `FlinkKafkaInternalProducer.close()`, and the committer's close
method does not abort its pending committables. X's prepared transaction can
therefore remain open on the broker after task cleanup.

Recovery from checkpoint 1 initializes `CommitterOperator` by committing the
committables restored from that checkpoint before new records flow. This includes
X's old producer ID and epoch. `KafkaCommitter.getRecoveryProducer` resumes that
stored session. The writer's `TransactionAbortStrategyImpl` recovery sweep skips
IDs listed as precommitted in restored writer state; it cannot be assumed to
abort X before the restored commit.

`ProducerPoolImpl` in released 5.0.0-2.2 resets the transactional ID and calls
`initTransactions` on every reuse. X's later session has a newer epoch, so the
old checkpoint's resume is fenced. The recovery warning in
`org/apache/flink/connector/kafka/sink/internal/KafkaCommitter.java` in
`flink-connector-kafka-5.0.0-2.2-sources.jar` includes the distinctive fragment
`it is expected in some cases (e.g., aborted checkpoints in previous attempt)`.
The catalogs capture that fragment as the optional `restored-commit-fenced`
TaskManager marker; its absence alone does not change the data verdict.

The same-ID mutant skips this reinitialization. With Kafka transaction version 1,
X's later transaction can retain its old producer ID and epoch. The restored
commit then succeeds, publishing records that the restored source subsequently
replays: one contiguous duplicate block, approximately a checkpoint interval.
Transaction version 2 advances the epoch at commit, so this mutant's stale resume
is fenced too. That protection does not make ID-only recovery safe: an
implementation that resumes the current transaction by ID without checking the
saved epoch can publish the wrong transaction on either version. The invariant
belongs in a general exactly-once catalog, beyond this particular mutation.

## Workload timeline and controls

Both versions keep the kill cells' 5 ms processing delay, 12 s checkpoint interval,
8 s asynchronous snapshot delay, 14,000 integer IDs, one sink partition and the
harness default 7,200,000 ms transaction timeout (two hours). The fault catalogs
pass these extra workload arguments:

```text
--failAsyncSnapshotForCheckpoints 2,3
--failAfterSnapshotOfCheckpoint 3
--failAfterSnapshotDelayMs 2000
```

| Event | Writer/committer state | Checkpoint result |
| --- | --- | --- |
| Barrier 1 and its delayed completion | X is committed and recycled | 1 completes; restore point is 1 |
| Barrier 2 | Writer precommits the next ID and takes X again | 2 fails on the asynchronous snapshot thread |
| Records between barriers 2 and 3 | X collects approximately one interval of records | Failure is tolerated; processing continues |
| Barrier 3 | Writer precommits X, emits its committable and selects Y | 3 also fails asynchronously |
| First element at least 2,000 ms after synchronous snapshot 3 returns | Workload throws on the task thread; close aborts current Y | One in-place restart restores 1 |
| Restore, before replay | Committer resumes X using checkpoint 1's producer ID/epoch | Old resume is fenced or commits the later transaction |

Failing checkpoint 3 as well removes the race with its asynchronous completion:
checkpoint 3 can never replace checkpoint 1 as the restore point. The configured
8 s delay still applies to every unlisted checkpoint, including checkpoint 1.
For listed checkpoints, the raw state future is cancelled and its wrapper throws
`POOL_REUSE_SNAPSHOT_ASYNC_FAIL checkpoint=<id>` on the asynchronous caller thread,
before any delay. This declines a checkpoint without failing the task.
The task trigger arms only after the synchronous snapshot for the exact requested
ID returns and fires on the first eligible subsequent element. Job checkpoint IDs
continue increasing across the restart, so restored execution cannot arm it again.

`setup.flink.config` sets
`execution.checkpointing.tolerable-failed-checkpoints: "2"`. Flink's
`CheckpointFailureManager` counts `CHECKPOINT_ASYNC_EXCEPTION`,
`CHECKPOINT_DECLINED`, `CHECKPOINT_EXPIRED` and `IO_EXCEPTION` against tolerance;
`TASK_FAILURE` and `JOB_FAILOVER_REGION` do not consume it. Both asynchronous
failures are therefore tolerated before the planned task exception.

The controls fail only checkpoint 2 and never arm the task failure. Checkpoint 3
and later checkpoints can complete normally; both connector subjects must keep
exact output. Controls use the same tolerance, input and timings, and require
the checkpoint-2 marker. Zero delay still uses the original plain map when none
of the new options is present; fault injection selects the snapshot-aware operator
even with zero delay. Its existing UID and protocol-v1 workload manifest remain
unchanged.

Custom Flink configuration selects the explicit standard shell launcher described
in [SPEC-001 R5.2b](specs/SPEC-001-scenario-schema.md). Check the resolved plan and
`evidence.flinkRuntime.config`, and verify `effectiveConfiguration` receipts and
configuration-loading logs for the key on every JobManager and TaskManager
incarnation. An environment value alone is insufficient. A confirmed fault run
has one TaskManager incarnation despite its task restart.

## Confirmation from retained evidence

`restWindowConfirmed` and the `checkpoint-in-progress` await belong to physical
kill cells. Their eligible checkpoint must be the latest completed checkpoint plus
one; they cannot arm this cell after tolerated checkpoint 2. Do not use them or
the old kill calibration checker's timing/block-size limits to qualify this cell.
A passing terminal oracle alone is insufficient. Confirm a fault repetition only
when retained evidence establishes all of the following:

1. All three required markers match in the same, single TaskManager incarnation:
   checkpoint 2 failure, checkpoint 3 failure and the planned failure after 3.
   The `POOL_REUSE_SNAPSHOT_SYNC checkpoint=3` line precedes the planned-failure
   line by at least the configured 2,000 ms, comparing their `timeMs` values.
   Verify that the writer actually precommitted X at barrier 3; the throttle's
   synchronous marker alone does not establish the sink's barrier progress.
2. The complete JobManager log shows checkpoint 1 completed, checkpoints 2 and 3
   declined, no `Completed checkpoint 3`, one restart, and restore from
   checkpoint 1. Count scheduler restart transitions, not repeated exception
   stack traces. Missing or truncated logs cannot establish absence or exactly
   one restart. Record the planned-failure-to-restore time separately.
3. Decoded sink-partition history binds X to a producer ID and epoch, shows its
   checkpoint-1 COMMIT marker, and a later transaction opening after that marker.
   Its barrier-2-to-3 record block ends with ABORT on the released connector or
   COMMIT on the TV1 mutant. Join producer/epoch records, transaction-state history
   and runtime lines rather than equating a producer ID alone with a transaction.
   Use coordinator lifecycle timestamps, control-marker timestamps and physical
   offsets for ordering: data-record timestamps preserve source event time and
   cannot date the later sink writes relative to checkpoint barriers.
   For the TV1 mutant, require exactly one implicated transactional ID and one
   contiguous duplicate block equal to the reused interval; record its measured
   size rather than applying the kill cells' 200–900-record range.
4. Record `evidence.sinkTransactions.unresolved` after the fence separately from
   the data verdict, including explicit unavailable evidence. In TV2, confirm
   stale-resume fencing and report any unresolved transaction as its own finding.
   Also confirm subject class origins, complete oracle capture and process fence.

A completed checkpoint 3, or planned failure before the writer's barrier 3, is a
**window miss**, even with a data PASS. Missing evidence leaves confirmation
unestablished. Preserve the attempt's original verdict and every data failure;
window classification never converts a FAIL into PASS. Misses do not count toward
the three required confirmed repetitions per subject and transaction version.

## Expected matrix

| Subject | Transaction version | Expected observation |
| --- | --- | --- |
| Released 5.0.0-2.2 | 1 and 2 | PASS; restored-commit fencing warning observed |
| Same-ID mutant | 1 | FAIL through `kafka.id-set`, reason `validator.kafka.id-set.duplicate-ids`; one contiguous block on exactly one transactional ID, roughly one checkpoint interval |
| Same-ID mutant | 2 | PASS; stale resume fenced; unresolved transactions reported separately |
| Both no-failover controls | 1 and 2 | PASS for both subjects |

This table defines expectations; measured results appear below. Canonical and copied
expected-result siblings all remain `outcome: pass`; the deliberately broken
TV1 mutant makes the ordinary gate fail, as intended.

## Run and retain both arms

Use a fresh directory inside the repository and build the current harness and
workload first. Do not clean a directory containing retained evidence. The existing
[mutant build recipe](../calibration/pool-reuse-mutant/README.md) requires JDK 21,
an explicitly selected isolated offline Maven cache and explicit settings. It
refuses to overwrite its output. Build and retain its `build-evidence.json`,
`release.jar`, `flink-connector-kafka-5.0.0-2.2-pool-reuse-mutant.jar` and `runtime/`.
If those prerequisites are unavailable, report the mutant arm as not run.

Validate the released Maven subject, showing the passed-through configuration:

```sh
for mode in failover control; do
  for version in 1 2; do
    mvn -q -o exec:java -pl cli \
      -Dexec.args="validate --catalog-root scenarios --scenario pool-reuse-precommitted-$mode-v$version --artifact-root . --offline --show-plan"
  done
done
```

Prepare both local-JAR arms without starting containers. The baseline JAR below
is the recipe's hash-verified released 5.0.0-2.2 binary; its dependencies match
the mutant's closure. `--prepare-only` preserves config, log markers, workload
arguments, phases and expected results in both copied catalogs.

```sh
evidence=jobs/validation/pool-reuse-precommitted-YYYYMMDD-HHMMSS
mkdir -p "$evidence"
python3 tools/pr_gate.py --prepare-only \
  --baseline-connector-jar calibration/pool-reuse-mutant/target/release.jar \
  --baseline-runtime-dir calibration/pool-reuse-mutant/target/runtime \
  --connector-jar calibration/pool-reuse-mutant/target/flink-connector-kafka-5.0.0-2.2-pool-reuse-mutant.jar \
  --runtime-dir calibration/pool-reuse-mutant/target/runtime \
  --scenario pool-reuse-precommitted-failover-v1 \
  --scenario pool-reuse-precommitted-failover-v2 \
  --scenario pool-reuse-precommitted-control-v1 \
  --scenario pool-reuse-precommitted-control-v2 \
  --output "$evidence/prepared"
```

Run each fault cell at least three times per arm and version, with one of each
control per arm (12 fault attempts and four controls). Each catalog already has
`health_retry_limit: 0`. Use the explicit runner so every attempt retains both
checkpoints and decoded broker logs; the gate's ordinary runner does not currently
pass `--retain-checkpoints`. Preserve failed exit codes and continue the matrix:

```sh
for side in baseline candidate; do
  for mode in control failover; do
    repeats=1
    if [ "$mode" = failover ]; then repeats=3; fi
    for version in 1 2; do
      scenario=pool-reuse-precommitted-$mode-v$version
      for run in $(seq 1 "$repeats"); do
        attempt="$evidence/$side/$scenario/run-$run"
        mkdir -p "$attempt"
        mvn -q -o exec:java -pl cli \
          -Dexec.args="run --catalog-root $evidence/prepared/$side-catalog --scenario $scenario --artifact-root . --offline --kafka-log-output $attempt/kafka-logs --retain-checkpoints $attempt/retained" \
          > "$attempt/result.json" 2> "$attempt/stderr.log"
        code=$?
        printf '%s\n' "$code" > "$attempt/exit-code.txt"
      done
    done
  done
done
```

Run this loop without shell `errexit`; a mutant TV1 failure is an expected
observation. If Testcontainers rejects the host's Docker API version, apply
`DOCKER_API_VERSION=1.44` and Maven `-Dapi.version=1.44` to the same run command.
Do not rerun over existing output paths. Add fresh numbered attempts for misses
and keep every original attempt. Checkpoint retention copies only state remaining
at the fence; a bounded job may have already disposed its checkpoint files.

Analyze each retained attempt, selecting both component log streams:

```sh
python3 tools/analyze_attempt.py "$attempt/result.json" "$attempt/kafka-logs" \
  --marker 'POOL_REUSE_SNAPSHOT_(SYNC|ASYNC_FAIL)' \
  --marker 'POOL_REUSE_PLANNED_FAILURE' \
  --marker 'it is expected in some cases' \
  --jobmanager-marker '(?i:checkpoint|restart|restor|failover)' \
  --json > "$attempt/analysis.json"
```

The report keeps `taskManagerMarkers`, ordered `jobManagerMarkers` with source
file and line number, decoded producer/epoch transactions and commit/abort markers,
duplicate blocks, and a separate `sinkTransactions` section. Use
`--flink-log-dir DIR` if retained Flink logs are outside the inferred checkpoint
root. Marker selection is diagnostic; review the complete logs and bind all of
the confirmation evidence yourself. No automatic new window verdict is implied.
Fencing can append an ABORT marker at a newer epoch than the data: the report
retains the data epoch under `transactions[].producerEpoch` and marker epoch
under `transactions[].marker.producerEpoch`. It joins a newer marker only to one
unambiguous open transaction for that partition and producer ID. Missing or
ambiguous joins remain diagnostic; physical markers alone do not establish
transactional-ID attribution, checkpoint timing or read-committed visibility.

Retain this layout, plus artifact hashes and the exact build/run commands:

```text
pool-reuse-precommitted-<batch>/
  build-evidence.json, commands.log, versions.txt, artifacts/
  prepared/{manifest.json,baseline-catalog/,candidate-catalog/}
  baseline/<scenario>/run-<n>/
    result.json, stderr.log, exit-code.txt, analysis.json
    kafka-logs/                  # raw/decoded broker segments and receipts
    retained/checkpoints/        # available checkpoints and Flink process logs
  candidate/<scenario>/run-<n>/  # same layout
  window-audit.json              # classifications, block sizes and restart timings
```

## Verification status

The retained 2026-10-11 batch at
`jobs/validation/precommitted-failover-20261011-1805/` ran all 16 attempts from
the requested `main` baseline `4e8a927` with this change. All 12 fault windows
were confirmed, with zero misses or unconfirmed attempts; all four controls
passed without failover. The canonical data verdicts remain 13 PASS and three
TV1 mutant duplicate-ID FAIL results.

| Subject | TV1 fault | TV2 fault | No-failover controls |
| --- | --- | --- | --- |
| Released 5.0.0-2.2 | 3 confirmed PASS; recycled interval aborted | 3 confirmed PASS; recycled interval aborted | PASS for both versions |
| Same-ID mutant | 3 confirmed duplicate-ID FAIL | 3 confirmed PASS; stale resume fenced | PASS for both versions |

The TV1 mutant duplicated contiguous blocks of 1,934 IDs (3176–5109), 1,928 IDs
(3402–5329) and 1,946 IDs (2003–3948). Each block exactly matched one later
transaction on one recycled transactional ID, ending in a COMMIT marker. Each
released fault and TV2 mutant fault emitted one fenced-recovery warning (nine
warnings total); the TV1 mutant faults emitted none. Post-fence
`evidence.sinkTransactions.unresolved` was empty in all 16 attempts, recorded
separately from the data verdicts.

Across the 12 faults, synchronous snapshot 3 preceded the planned failure by
2,000–2,004 ms. Each job had one restart transition lasting 1,008–1,087 ms and
restored checkpoint 1 within 1,056–1,136 ms of the planned failure. The required
markers matched, checkpoints 2 and 3 declined, checkpoint 3 never completed,
and the tolerance key was confirmed on every runtime incarnation.

Both arms used explicit local connector JARs. All 16 attempts had the same 11
non-primary dependency hashes in the same order, workload hash and Flink image
identity; the connector primary differed as intended. Subject origins were
confirmed. The identity/retention audit verified all 64 retained process and
class-loading log files against their size/hash receipts. **No binary checkpoint
state was retained:** Flink had disposed it after the bounded jobs reached
FINISHED, so each completed checkpoint-retention receipt contains only four
process/class-loading logs. The restore evidence comes from the retained runtime
logs and transaction history, not recovered checkpoint binaries.

The batch stores attempts under `faults/<release|mutant>-v<1|2>-<n>/` and
`controls/<release|mutant>-v<1|2>-<n>/`, with command, result, stderr, exit code,
broker captures, retained logs, `analysis.json` and `window-audit-v2.json`.
`batch-window-audit.json` summarizes the matrix;
`batch-identity-retention-audit.json` verifies common artifacts and retained
files. `audit-corrections.json` preserves an interpretation correction: the first
pair's older audits incorrectly compared source event timestamps with sink
barriers. The replacement audits use coordinator timestamps and physical offsets.
Both attempts are confirmed windows; this correction is not a window miss and
did not change runtime inputs or data verdicts.

On this baseline, full Maven verify passed on JDK 21, all 230 Python tooling tests
passed, all four released-Maven CLI `validate --show-plan` checks passed, and the
local-JAR `--prepare-only` comparison retained the tolerance key and log markers.
Earlier pilots in `jobs/validation/precommitted-failover-20261011-1745/` used a
prior baseline and are preserved separately; they do not count toward this
matrix. Historical kill-cell results also do not qualify this cell.
Raw evidence is retained locally and is not distributed with this repository.
These observations qualify this recorded batch, not every future timing window.

## Follow-ups

A harness-driven graceful stop would broaden the invariant beyond a workload
exception. Implement the schema's `stop` operation, currently rejected by the
runner as `runner.phase.step-unsupported`; add an explicit REST cancel/rescale
step and a `checkpoint-failed` await condition. These are separate capabilities,
not new step kinds or conditions introduced by this change. A future automated
window auditor can apply the retained-evidence rule above without altering the
existing kill calibration contract or suppressing original data verdicts.
