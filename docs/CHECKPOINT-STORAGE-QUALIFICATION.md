# Checkpoint-storage qualification

**Status: QUALIFIED on 2026-10-27 for Docker Desktop `linux/arm64`.** The fixed
PR 5 matrix `jobs/checkpoint-storage-qualification-20261027-01` confirmed all
twelve allocations: nine raw PASS results and three raw mutant state-mismatch
FAIL results. Every run retained all 30,000 IDs with zero ID defects. There were
no retries, replacement attempts, missing windows or audit errors. All four
Docker-free catalog validations completed before the live attempts. Artifacts,
framework and recipes remained frozen throughout execution.

This document records the methods and identities for that batch. The
[PR 4 integration guide](CHECKPOINT-STORAGE-FAULTS.md) describes the executable
fault and retained integration pilots; the
[implementation plan](STATE-RECOVERY-STORAGE-FAULT-PLAN.md#pr-5--live-qualification-and-reproducible-evidence)
keeps prototype, integration and qualification as separate gates. The
[recipe guide](../calibration/checkpoint-storage/README.md) explains how to
prepare a new batch. Links below open [published sanitized copies and compact
summaries](evidence/README.md). The original `jobs/` evidence remains local and
unchanged. Printed receipt/result hashes identify those **raw originals**;
the [publication manifest](evidence/checkpoint-storage-publication-manifest.json)
records each copy's separate SHA-256 and its omissions. The copies omit raw
journals, binaries and detailed transcripts, so they cannot rerun the full audit.

## Current source-response contract and historical scope

The October 27 matrix and its review reports are historical qualification of the
recorded artifacts and verifier. They retained typed Flink REST, inspector and
physical-fence receipts, but did not retain the corresponding original REST or
Docker response bytes. A typed JSON receipt is a derived result, even when its
file is hashed. The historical hashes and outcomes below remain unchanged.

New FUSE attempts retain decoded Flink HTTP response bodies, exact inspector
stdout and Docker inspect `Id`/`State` source-token projections. The projection
excludes Docker `Config`/`Env` and is labelled as a projection. Current checking
compares every required typed counterpart with those bytes: readiness's original
checkpoint overview, six-stage job/history observations, configuration and failed
details, trigger acknowledgement/status, pre/post-kill clocks, materialization,
the final job observation and physical process states. Capture hashes, byte limits,
request identities, distinct call groups and ARM/kill/HEAL ordering are checked.
Missing source captures cannot be replaced with a reserialized typed object.

The current catalog contract is version 2 and requires explicit mutant opt-in.
The current checker, including `--re-audit`, rejects the old batch: it lacks that
contract and the new source captures. Use its preserved historical verifier to
reproduce the historical result; a fresh batch is necessary to qualify current
runtime changes. The October 27 re-audit described below is not a claim that the
present checker requalifies the old matrix.

Hashes establish retained-byte consistency, not responding-daemon authenticity.
Harness monotonic timestamps and JobManager epoch times are separate observations,
without an independent clock-attestation or cross-clock synchronization claim.
Terminal Kafka counts and state verdicts remain framework-oracle output; the
Python checker checks those reported contracts, mismatch arithmetic and supporting
receipts, but does not replay raw Kafka records. Keyed file sizes/hashes come from
the pinned inspector's retained stdout, supported by native I/O receipts; raw
checkpoint file contents are not independently replayed by this checker.

## Review corrections and independent re-audit

The subsequent review identified one runtime deadline defect and three checker
gaps. The runtime now bounds storage lock acquisition and Docker calls, permits
final capture/cleanup after an abandoned startup, and rejects late worker results.
Five regression tests use drivers that deliberately ignore interruption. The
checker now rejects impossible mutant totals/sample values, binds the corrupted
suffix to the restored per-key reset markers, compares the complete recognized
component-event set with the hashed retained logs, and verifies preflight/live
interval ordering. Independent component observations survive a failed audit in
both JSON and Markdown.

A further chronology regression rejects zero-width launch intervals, even when
all twelve use the same timestamp after preflight. Wall-clock duration must be
positive and agree with positive monotonic elapsed time within one second;
larger drift leaves chronology unconfirmed. The additional checks also reject
artificially compressed/stretched intervals. All 415 Python tests and the
October 27 review verifier's re-audit of the twelve original attempts passed with this guard.

The original matrix, manifests, frozen recipes and reports remain unchanged.
The October 27 review verifier's explicit `--re-audit` produced a separate
[JSON report](evidence/checkpoint-storage-review-fixes-20261027-01/re-audit-final.summary.json)
and [Markdown report](evidence/checkpoint-storage-review-fixes-20261027-01/re-audit-final.md):
all twelve original attempts still qualify, with no errors. All eighteen
reported component events match the independent extraction from 33 physical
process logs (3,901,852 bytes), and all preflight/live intervals are ordered.
This verifies the **retained final log prefixes**; the original collector's exact
earlier byte boundary and complete process lifetime remain unproven and are
explicitly reported. The re-audit identifies that review verifier separately
from the original recipe and does not qualify changed runtime binaries.

The updated JSON and Markdown reports were
[reproduced byte-for-byte](evidence/checkpoint-storage-review-fixes-20261027-01/re-audit-reproduction.json)
with another Python hash seed. Separate
[memory-only counterexample checks](evidence/checkpoint-storage-review-fixes-20261027-01/counterexamples.json)
on original run evidence reject 30,001 mismatches for 30,000 records, negative
mutant count/sum, deleted component events, overlapping attempts and live
intervals before preflight. The omitted component events remain visible in the
failed check's independent audit. No retained input was modified.

Offline verification after these fixes: full JDK 21 reactor `mvn install`
(including `verify`) passed 1,616 tests, with zero failures/errors and one opt-in
Docker test skipped; all 411 Python tests passed. The exact commands and logs
are retained in
[the review verification directory](evidence/checkpoint-storage-review-fixes-20261027-01/README.md).
The first offline installation was denied writing to the Maven cache; the
authorized retry completed successfully. A first targeted test compile had a
test-fixture generic-type error; the corrected targeted run passed all 27 tests.
Neither failed preparation launched a live attempt.

The changed runtime then passed one fresh, separately frozen `correct-eio`
[live smoke and independent audit](evidence/checkpoint-storage-review-fixes-20261027-01/live-smoke-02/smoke-finished.summary.json).
Its Docker-free validation passed before the only live launch; the run took
280.67 seconds, with no retry. Checkpoint 1 was the materialized baseline,
checkpoint 2 failed, the TaskManager was physically replaced, and the new
process restored checkpoint 1. All six fault stages and final storage capture
were confirmed. The terminal oracle found exactly 30,000 IDs across eight keys,
zero ID defects and zero state mismatches. Twenty native EIO operations were all
`create` failures on the original TaskManager; this smoke does not extend the
observed fault scope to `write`/`fsync`. Both producer-fenced component events
(INFO TransactionManager and ERROR KafkaCommitter) remain recorded, with zero
REST errors. Cleanup confirmed all four exact containers and the owned backing
volume absent.

The new framework manifest SHA-256 is
`1f90f7f89331f0676a4f41615497aea06d726f83d9b179b4557437b79f523505`;
its `testcontainers` JAR SHA-256 is
`bdd69ad7b13a4d15b54cc6e5c9d78702fef67d1047f9eb7dcbce3c68b313101a`.
This is one live regression smoke, not a new twelve-run qualification of the
changed runtime. The first smoke preparation was preserved unused after a
missing parser file was noticed before launch; the corrected fresh preparation
includes and hash-checks all checker dependencies.

## Fixed comparison

| Cell | Workload and operation | Attempts | Required raw result |
| --- | --- | ---: | --- |
| `correct-healthy` | Correct stateful workload; healthy FUSE; uninterrupted | 3 | PASS, `validator.kafka.state-prefix.match` |
| `correct-restart` | Correct workload; healthy FUSE; one physical TaskManager replacement | 3 | PASS, `validator.kafka.state-prefix.match` |
| `correct-eio` | Correct workload; checkpoint create/write EIO; armed kill, heal and replacement | 3 | PASS, `validator.kafka.state-prefix.match` |
| `mutant-eio` | Reset-state-on-restore workload; the same EIO experiment | 3 | FAIL, `validator.kafka.state-prefix.state-mismatch` |

Every scenario expectation remains PASS, including the mutant. Qualification
requires three observed mutant state-mismatch failures with complete exact-ID
and fault evidence. It preserves their raw FAIL and expectation mismatch.
Missing, duplicate, unexpected or malformed IDs, an incomplete traversal, or an
unconfirmed fault cannot replace the required state-only failure.

All cells use 30,000 ordered integer inputs, eight keys, parallelism one, a
requested 5 ms processing delay, one input partition and one output partition.
The state backend is HashMap with TTL disabled; checkpoint and Kafka delivery
are exactly-once. One JobManager and one TaskManager use the released Flink
2.2.0 distribution. Checkpoints have a 10-second interval, zero inline-state
threshold, concurrency one and tolerated failure count ten. Natural completion
has a five-minute limit. Atomic EIO operations have a 90-second shared deadline
and a 120-second fault lease. The outer process limit is 900 seconds.

The runner launches one allocation at a time. There are no automatic retries,
resume, replacement attempts or expectation changes. An ordinary non-pass
result remains in its allocated slot. A timeout, uncertain process termination
or unconfirmed cleanup stops further launches and leaves an incomplete matrix.
The four `validate --show-plan --offline` calls precede the execution-started
receipt; their
[completion receipt](evidence/checkpoint-storage-qualification-20261027-01/validation-finished.json)
lists all four cells and has SHA-256
`faaecca77664d1b08402cf668dea44c701c58a00957410cb0020333eb7bd3619`.

## Frozen identities

| Receipt or artifact | Original raw receipt / artifact SHA-256 |
| --- | --- |
| [Matrix manifest](evidence/checkpoint-storage-qualification-20261027-01/manifest.json) | `c9f673f7cd883796b58e8a4dd70e61646e50a8bfaab026cc07bf4d7d1448d945` |
| [Framework manifest](evidence/checkpoint-storage-qualification-20261027-01/framework/manifest.summary.json) | `dab6678930cf0e5b40af97748bd431a39ccc5968ed4ff5523c1572be09242cac` |
| [Execution-started receipt](evidence/checkpoint-storage-qualification-20261027-01/execution-started.json) | `1fc24f57c9d37d49218dc0f8615423f0438705751cfa618f43d8af1b27f527fe` |
| Correct workload, `artifacts/correct.jar` | `4c8ca4211b01d26ba0c49a2060259c117b95016fef798b4a0c187a332edd9711` |
| Reset-on-restore workload, `artifacts/mutant.jar` | `0ad8e5eeec5deb8e732a346dffc54c3675a6cb7c086e5c1bfa23a1389798d54a` |
| Workload build evidence | `0b5553d8ba108112d919dccde6de8ecb0bd103b5963d3311ba3fcf9c02496456` |
| Helper `build-evidence.json` | `9ab2001ccc55e700a76c12d0ba64d4824d1089d21e85adffa5216d75d8582ecf` |
| Helper `checkpoint-fuse` | `49d01b4f62b6a7bd8256779777cf1e6d193d2e1d7365796c115f4ffb66c85f66` |
| Helper `checkpoint-fuse-selftest` | `bbdd6a07da940c9d9db135958ec7f1caf8502f03767203f349aa65b5c7a3b9d5` |
| Helper `probe.jar`, including checkpoint inspector | `2fd880248a7515b08ee419e1ff875cc1edff8e0f784878aa55a0aaa0a1780983` |
| Primary Kafka connector | `6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da` |
| Raw runtime classpath input | `08ff2791279777ffbaff810c510705af83a0fae40384867c53cf2d277f9f7785` |
| Selected JDK 21.0.7 Amazon Java executable | `e09233a5d606938176734fb4eb70407659a8fede3073949c495631f43612e65c` |

The matrix manifest retains the complete twelve-artifact connector closure,
each scenario and expectation hash, helper and workload source snapshots, and
all frozen recipe hashes. The framework manifest retains 76 ordered runtime
classpath entries and 111 explicit Maven artifact-closure files. Each of the
six framework JARs matched its target and installed artifact byte-for-byte;
packaged class/resource contents were checked against the captured build
outputs. Execution uses these frozen copies and an isolated resolver repository.
These checks identify bytes and source snapshots; they do not attest a Git
revision or establish compiler provenance.

| Frozen framework module | JAR SHA-256 |
| --- | --- |
| `runtime-api` | `ebdab6dd421b335be2b4bbd41eef099f4859764c88646b3183298be45b911b86` |
| `core` | `aaad9b1eaea5391c9e251a5ad800db613d20e01285aac42924950465ce100505` |
| `testcontainers` | `6c35bbcad564f1978753bc6f79421f3e89e7970843521f70b3139fdcd4c4cc62` |
| `cli` | `bb858bdb5f54650867602a7dd4e047f2c49654604b68e27efc35a311b7b8a1e0` |
| `kroxylicious-fault-filter` | `7222efbddc3d7a058ed9469901f72b2702723e644224d17b320431a9cbce78da` |
| `synthetic-token-plugin` | `3edba08d054b17ad60f4b8915c48026aa05953df4a0407467ee2f052c480bdf0` |

The 27 October 2026 matrix used Docker Engine 29.1.3 with the classic graphdriver
image store. The released `flink:2.2.0` image ID
`sha256:2e93ded2cb7bf1a50f71b663d5e3ea01420bcc4e549cbf666d05120f939713c7`
is its arm64 config digest. The containerd image store reports the index digest
`sha256:7ecfab9d5e8ec6987a248b32f6605b5fe9bcb7347aea0d7b1bc3976588408681`.
Current provisioning accepts an author-declared `image_id` matching the
container image ID or its inspected `RepoDigests`, records the matched form and
store backend, and verifies optional content-addressable `image_digest` against
`RepoDigests`. This compatibility change does not requalify the historical
matrix on a different store.
Kafka is `apache/kafka:4.0.0`, resolved to
`sha256:2cecec9e3ea2d63fd5d1da1ab95e6bb7c33a7ff81053c4eb46f4176cd0bad8c9`.
The helper and Flink image platform pin is `linux/arm64`.

The [platform receipt](evidence/checkpoint-storage-qualification-20261027-01/evidence/platform.json)
has SHA-256
`979f1decd92cd412925ce4074554708f7689f3de765f14d383bbd1e8a467d465`:

| Platform field | Recorded value |
| --- | --- |
| Docker Engine | `29.1.3` |
| Docker kernel | `6.12.54-linuxkit` |
| Docker architecture | `aarch64` |
| Docker operating system / OS type | `Docker Desktop` / `linux` |
| Host OS / release / architecture | `Darwin` / `25.5.0` / `arm64` |
| Host Python | `3.13.1 (main, Dec  3 2024, 17:59:52) [Clang 16.0.0 (clang-1600.0.26.4)]` |
| Docker API setting for both clients | `1.44` |

The explicit local Unix endpoint is retained in `execution-started.json` and
each attempt's command receipt. The runner supplies a minimal environment,
fresh per-attempt home and temporary directories, empty Docker authentication
configuration and empty Maven settings. It launches the frozen direct Java
classpath with offline artifact resolution; Maven, mutable `target/classes`
and the ambient Maven cache are not inputs to these twelve JVM launches.

## Evidence and commands

The frozen runner entry point used for this batch is shown below. This is a
record of the invocation shape, not a command to rerun the active or completed
batch. `CHECKPOINT_DOCKER_ENDPOINT` identifies the explicit local Unix endpoint
recorded in the launch receipt; `CHECKPOINT_DOCKER` identifies the selected local
Docker executable. Any later experiment requires a new freeze and output path.

```sh
python3 -B jobs/checkpoint-storage-qualification-20261027-01/evidence/recipe/run.py \
  jobs/checkpoint-storage-qualification-20261027-01 \
  --workspace-root . \
  --docker-endpoint "$CHECKPOINT_DOCKER_ENDPOINT" \
  --docker "$CHECKPOINT_DOCKER" \
  --docker-api-version 1.44
```

`-B` prevents generated `__pycache__` files from changing the frozen inventory.
The runner verifies frozen identities before launch and preserves exact argv,
environment, exit status, stdout/stderr and bounded process-termination receipts.
Each attempt keeps the raw CLI result, component and class-origin logs, state
markers, effective configuration, input receipt, exact reconstructed integer
manifest, native journals and a hashed retained-file index. Read-only Docker
queries check that each exact Flink/Kafka container ID and owned storage volume
is absent after cleanup.

The historical independent checker rechecks fixed inputs and recipe identities,
command isolation, natural FINISHED completion and physical writer-fence receipts.
It checks the framework's reported complete fixed Kafka `read_committed` traversal
and exact state-prefix result; it does not replay the Kafka records. Recovery
requires the old/new physical and execution identities, the completed baseline
checkpoint, restored state and progress for every key. EIO cells additionally
require the external keyed state file materialization, exactly correlated
failed checkpoint, actual native `errno=5` create/write receipts after ARM,
physical stop before HEAL, a healthy replacement daemon and a later successful
checkpoint. Raw journals must match their hashes, contiguous sequences and
all referenced captured prefixes. Missing or contradictory evidence fails
qualification.

After the batch finishes, run the frozen checker and renderer with fresh output
names. Both return nonzero for an unqualified or incomplete matrix. No-clobber
prevents overwriting an earlier report:

```sh
set -C
python3 -B jobs/checkpoint-storage-qualification-20261027-01/evidence/recipe/check.py \
  jobs/checkpoint-storage-qualification-20261027-01 \
  > jobs/checkpoint-storage-qualification-20261027-01/new-check-report.json
python3 -B jobs/checkpoint-storage-qualification-20261027-01/evidence/recipe/report.py \
  jobs/checkpoint-storage-qualification-20261027-01 \
  > jobs/checkpoint-storage-qualification-20261027-01/new-report.md
```

The renderer reports every allocated row, including missing and error rows;
component-error events, REST errors, cleanup status, restoration and native EIO
counts remain visible. Producer-fencing observations do not disappear because
data validation passed. Both reports were generated twice, with
`PYTHONHASHSEED=0` and `PYTHONHASHSEED=1`, and matched byte-for-byte. All four
checker/renderer invocations exited zero; the
[reproduction receipt](evidence/checkpoint-storage-qualification-20261027-01/reports/reproduction.json)
retains the comparison and command hashes.

| Published report copy or summary | Original raw report SHA-256 |
| --- | --- |
| [Independent JSON report](evidence/checkpoint-storage-qualification-20261027-01/reports/final/qualification.summary.json) | `ed45ddf31f7af59e0c838f7a301cf1af8037c7b35c6f0df730cdcacce7f4a1c5` |
| [Generated Markdown report](evidence/checkpoint-storage-qualification-20261027-01/reports/final/qualification.md) | `30f96ec95304dd144c3ae246f5a0346e80669ac36b5c299b261b79bb3ee5ce11` |
| [Separate read-only audit](evidence/checkpoint-storage-qualification-review-20261027-01/review-finished.json) | `9d12741a99e800bd65fe3c924ad9682b2a0789bb2ca6b77ba389e31991148f06` |
| [Independent arithmetic and native-operation supplement](evidence/checkpoint-storage-qualification-review-20261027-01/supplemental-summary.json) | `8f12d0a1ab203e1764501da885c8d9f1b99c2c7a02ac763898d1f5ceb4c0b921` |

## Results

All twelve allocations are independently confirmed. Each job naturally reached
FINISHED, complete `read_committed` traversal observed 30,000 exact IDs, and
missing, duplicate, unexpected and malformed counts were all zero. The three
mutant failures retain their PASS catalog expectation and `matched: false`.
Earlier pilots do not replace any of these slots.

| Cell | Attempt | Raw result | Completed checkpoints | Restored checkpoint | State mismatches | Component events |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| `correct-healthy` | 1 | PASS | 20 | — | 0 | 0 |
| `correct-healthy` | 2 | PASS | 20 | — | 0 | 0 |
| `correct-healthy` | 3 | PASS | 20 | — | 0 | 0 |
| `correct-restart` | 1 | PASS | 21 | 2 | 0 | 2 |
| `correct-restart` | 2 | PASS | 21 | 2 | 0 | 2 |
| `correct-restart` | 3 | PASS | 22 | 2 | 0 | 2 |
| `correct-eio` | 1 | PASS | 22 | 1 | 0 | 2 |
| `correct-eio` | 2 | PASS | 22 | 1 | 0 | 2 |
| `correct-eio` | 3 | PASS | 22 | 1 | 0 | 2 |
| `mutant-eio` | 1 | FAIL: state-mismatch | 22 | 1 | 29,375 | 2 |
| `mutant-eio` | 2 | FAIL: state-mismatch | 22 | 1 | 29,250 | 2 |
| `mutant-eio` | 3 | FAIL: state-mismatch | 22 | 1 | 29,094 | 2 |

The nine recoveries retain 72 restored-key receipts. All six EIO attempts
materialized checkpoint 1, correlated EIO with failed checkpoint 2, stopped
the old TaskManager while armed, healed storage and restored checkpoint 1 in a
new physical process. Each has 20 native EIO operations and a subsequent
successful checkpoint after restored-state progress. **All 120 observed EIO
operations were `create`; this matrix does not independently qualify the
`write` or `fsync` failure paths.** The earlier isolated FUSE write probe is
separate evidence.

The mutants retain exactly 24 reset receipts, eight per run and only after
restoration. Their first mismatching IDs are 625, 750 and 906. Independent
arithmetic checked all 300 retained mismatch samples and the exact suffix
counts `30000 - firstMismatchingId`. For example, run 1's ID 625 requires count
79 and sum 24,806, while the reset mutant produced count 1 and sum 626.

There are **18 producer-fenced component events**, one TransactionManager INFO
and one KafkaCommitter ERROR in each recovery attempt. Their physical process
log lines are retained and matched; correct final data does not establish that
these events are harmless. REST errors and checker errors are zero.

The audit covered 111 journal references, 87 unique raw journal files and
172,024,081 journal bytes. Exact absence queries confirmed cleanup of all 45
distinct Flink/Kafka containers and 12 owned storage volumes. Summed JVM run
time was 2,986.88 seconds; artifact checks and preflight are additional.

## Separate offline checks and retained pilots

The [PR 5 Python regression log](evidence/checkpoint-storage-qualification-preparation-20261027-03/python-unittest-final.log.summary.json)
reports **367 passing tests**. This covers preparation/freezing, bounded runner
behavior, failure retention, independent evidence checking and deterministic
reporting alongside existing Python tooling. It does not demonstrate live
fault qualification.

The prior [PR 4 JDK 21 full-reactor verification](evidence/checkpoint-storage-integration-checks-20261027-01/maven-verify-final.log.summary.json)
reported **1,611 Java tests, zero failures/errors and one skipped opt-in Docker
lifecycle test**. PR 5 uses that same production Java source baseline and adds
Python qualification tooling, a preparation-only Java helper and documentation.
This is the retained Java regression
result, not a claim that Java tests were rerun for each qualification attempt.

The successful reference results were frozen under `evidence/references/` and
remain linked to their original paths by `referenceSources` in the manifest:

| Reference | Published result summary | Original raw result SHA-256 |
| --- | --- | --- |
| Correct healthy | [PR 4 batch 02, healthy pilot 01](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/healthy-fuse/run/pilot-01/stdout.summary.json) | `e17a408c9f0f6f5ecd39b0bd99129e2fff67ab9ce2b89b15780606aaa0d73db4` |
| Correct healthy restart | [Qualification pilot, healthy restart 01](evidence/checkpoint-storage-qualification-pilots-20261027-01/attempts/healthy-restart/run/pilot-01/stdout.summary.json) | `49da248550df5b46bab26df1b46777efb775e99779f42d7686786ed5bf843e37` |
| Correct EIO | [PR 4 batch 02, correct EIO pilot 03](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/correct-eio/run/pilot-03/stdout.summary.json) | `e9625a6a0cfe4f5e828a5b251479c0dad01d608e07c9711682a21fa472aab03a` |
| Mutant EIO | [PR 4 batch 02, mutant EIO pilot 01](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/mutant-eio/run/pilot-01/stdout.summary.json) | `b7642c06315649293e444a71b575749b65eba3e227a3f5347b92e13537bf6d25` |

The healthy restart pilot passed with FINISHED, one restoration, 21 completed
checkpoints, all 30,000 expected IDs and no state mismatch. Its two
producer-fenced component events remain retained. The PR 4 guide records the
other three references, including the mutant's raw FAIL and the earlier
integration misses/failures.

The following preparation history is also preserved:

| Retained attempt | Outcome and scope |
| --- | --- |
| [Healthy-restart validate pilot 01](evidence/checkpoint-storage-qualification-pilots-20261027-01/attempts/healthy-restart/validate/pilot-01/command.json) | Rejected before CLI execution because target and installed framework artifacts differed. It was not a live run or a successful validation. The original attempt remains; a separate validate pilot 02 passed after installing the same checkout. |
| [Direct-Java pilot 01 inputs](evidence/checkpoint-storage-direct-java-pilot-20261027-01/launcher-inputs.json) | Rejected before CLI or container startup because importing without `-B` created unlisted `__pycache__` files inside the frozen inventory. The [fresh pilot's derivation receipt](evidence/checkpoint-storage-direct-java-pilot-20261027-02/derivation.json) records the correction; the earlier attempt was not overwritten. |
| [Direct-Java pilot 02 result](evidence/checkpoint-storage-direct-java-pilot-20261027-02/run/stdout.summary.json) | PASS, `validator.kafka.state-prefix.match`; direct frozen-classpath launch exited zero and exact cleanup was confirmed. Result SHA-256: `5aa949dd37be70a691981fab82ce575592c3a38b9e186e4662580aeed41f511c`. This launch check is separate from the twelve allocations. |

## Scope and limitations

Qualification applies to these exact artifacts and this Docker
Desktop `linux/arm64` environment. Native Linux, other CPU architectures,
RocksDB/incremental state, state TTL, higher parallelism, multiple partitions,
HA, savepoints and fault combinations are outside this matrix. Three fixed
attempts per cell are a bounded comparison, not a reliability estimate.

The configured fault targets checkpoint create/write operations; all observed
EIO in this matrix occurred on `create`. It does not establish coverage of
write-only failure, physical disk disappearance, ENOSPC, `fsync` failure,
silent corruption, Kafka log-volume loss or remote object-storage failures.
Only isolated FUSE views and an owned backing volume are involved.

Checkpoint metadata and keyed-state files are inspected through FUSE while
live, with their sizes/hashes and matching native I/O receipts retained. Their
binary bytes are unavailable after owned-volume cleanup, so this evidence
cannot support a later independent binary checkpoint rehash or offline restore.
The independent terminal oracle and retained restoration receipts remain the
data/provenance checks. Component errors, failed checkpoints, REST errors,
aborted windows, missing proof and cleanup failures must remain visible even
when a final data result passes.
