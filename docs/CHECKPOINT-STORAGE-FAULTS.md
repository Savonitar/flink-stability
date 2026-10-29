# Checkpoint storage faults

Historical evidence links open [sanitized copies and compact summaries](evidence/README.md).
Printed receipt/result hashes identify the **raw originals**, retained unchanged
under local `jobs/`. The [publication manifest](evidence/checkpoint-storage-publication-manifest.json)
records separate sanitized-copy hashes and omissions. These copies exclude full
native journals, binaries and detailed transcripts and cannot rerun the raw audit.

PR 4 adds an opt-in Flink checkpoint write-failure experiment to the native
scenario runner. A FUSE rule returns real `EIO` for checkpoint creates/writes;
the runner identifies the failed checkpoint and kills the sole TaskManager
while the rule remains armed. It heals the surviving storage instance, starts a
replacement TaskManager, then proves restoration and state correctness.

**Pilot outcomes: healthy PASS, correct EIO/replacement PASS, reset-state mutant
raw FAIL.** The mutant exposed state corruption despite a complete exact-ID
result. Both atomic pilots retained two producer-fenced component events,
including a KafkaCommitter ERROR, described below. The
[standalone FUSE prototype](CHECKPOINT-STORAGE-FUSE-PROTOTYPE.md) has its own
qualified scope. The subsequent
[PR 5 matrix](CHECKPOINT-STORAGE-QUALIFICATION.md) separately confirmed all twelve
fixed attempts with reproducible reports; these pilots were not substituted
for qualification attempts.

## Executable contract

The first slice requires one JobManager, one TaskManager, parallelism one,
`keyed-prefix-v1`, HashMap state, disabled state TTL, a single ordered generated
input partition, and the `kafka.state-prefix` terminal oracle. At least twice the
key count must be supplied so a nonempty prefix and a later suffix are possible
for every key. Readiness must still prove that the fault landed before the
remaining input was exhausted.

The released Flink 2.2 runtime and helper bundle must have the same pinned image
identity. The native lock supports `linux/arm64`. `fuse.image_id` must equal
`setup.flink.image_id`; both are author-declared, verified inputs. Before Flink
starts, provisioning records the container image ID, image `RepoDigests`, the
matching identity form and Docker's storage driver / optional `driver-type`.
The pin may match either observed form; `setup.flink.image_digest`, when
present, must match a repository digest.

The 27 October 2026 qualification ran on Docker Engine 29.1.3 with the classic
graphdriver image store. Its
`sha256:2e93ded2cb7bf1a50f71b663d5e3ea01420bcc4e549cbf666d05120f939713c7`
is the arm64 config digest. The containerd image store reports the index digest
`sha256:7ecfab9d5e8ec6987a248b32f6605b5fe9bcb7347aea0d7b1bc3976588408681`.
Thus `image_id` is backend-dependent; `image_digest` is content-addressable.
The example defaults to that index digest, with a shared `flink_image_id`
parameter for both declarations. Identity compatibility is covered by offline
tests; the retained matrix does not qualify a containerd live run.
HA, custom runtime JARs, token-provider fixtures, loops and combinations with
other faults are outside this slice. The same FUSE provisioning can also run
an uninterrupted control or one ordinary TaskManager kill/restart control.

The [catalog example](../scenarios/checkpoint-storage-fault.yaml) declares:

```yaml
checkpointing:
  interval: 10s
  mode: EXACTLY_ONCE
  small_file_threshold: 0
  max_concurrent: 1
  tolerable_failures: 10
  storage:
    type: filesystem
    fuse:
      bundle: ${helper_bundle}
      manifest_sha256: ${helper_manifest_sha256}
      image_id: ${flink_image_id}
      platform: linux/arm64
```

Its atomic phase operation is:

```yaml
- checkpoint_storage_fault:
    job: state-job
    timeout: 90s
    lease: 120s
```

Both durations must be positive and at most 120 seconds, with the lease at
least as long as the operation timeout. `max_concurrent` must be one,
`small_file_threshold` zero, and `tolerable_failures` in `1..10`. These options
and the older inline-threshold aliases cannot be supplied as competing runtime
configuration or workload arguments. Authors select a bundle directory relative
to the artifact root; the runner generates the storage namespace and mount
paths. Absolute bundle paths and traversal are rejected.

The canonical Flink keys are
`execution.checkpointing.data-inline-threshold`,
`execution.checkpointing.max-concurrent-checkpoints`, and
`execution.checkpointing.tolerable-failed-checkpoints`. In released Flink 2.2,
the inline threshold defaults to 20 KiB. Failure tolerance covers JobManager
I/O failures, asynchronous TaskManager checkpoint failures and checkpoint
timeouts; a synchronous TaskManager failure can still cause immediate failover.
An unexpected restart before the deliberate kill does not qualify this window.
See the [released checkpoint configuration definitions](https://github.com/apache/flink/blob/release-2.2.0/flink-core/src/main/java/org/apache/flink/configuration/CheckpointingOptions.java).

## Build the helper and prepare the catalog

Use JDK 21, Maven 3.8+, Python 3.10+ and an explicitly selected local Unix Docker
endpoint. Docker Desktop and native Linux have separate qualification scopes.
Missing FUSE support or mount capability is a failed prerequisite; the adapter
does not substitute another filesystem or use the backing directory directly.

The [helper builder](../testcontainers/checkpoint_storage_build.py) creates a
separate bundle in a new output directory. It freezes native and Java sources,
uses the pinned image and locked public Ubuntu package closure, compiles the
native helper, and compiles both Java I/O probe and checkpoint metadata inspector
with `--release 17`. It retains compiler/command receipts, dependency and source
hashes, native self-test output and artifact hashes in `build-evidence.json`.
It does not rebuild Flink or produce a derived runtime image. Earlier PR 3
bundles without `CheckpointStateInspector` are insufficient for this operation.

Run from the repository root and choose fresh names for every build and attempt:

```sh
CHECKPOINT_JDK21=/path/to/jdk-21
CHECKPOINT_DOCKER=/path/to/docker
CHECKPOINT_BUNDLE=jobs/checkpoint-storage-helper-local-04
export JAVA_HOME="$CHECKPOINT_JDK21"
export DOCKER_HOST=unix:///var/run/docker.sock

python3 testcontainers/checkpoint_storage_build.py \
  --java-home "$CHECKPOINT_JDK21" \
  --docker "$CHECKPOINT_DOCKER" \
  --docker-host "$DOCKER_HOST" \
  --output "$CHECKPOINT_BUNDLE"

mvn -B -ntp install

CHECKPOINT_MANIFEST_SHA256=$(python3 -c \
  'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' \
  "$CHECKPOINT_BUNDLE/build-evidence.json")

mvn -q -ntp exec:java -pl cli \
  -Dexec.args="validate --catalog-root scenarios --scenario checkpoint-storage-fault --artifact-root . --parameter helper_bundle=$CHECKPOINT_BUNDLE --parameter helper_manifest_sha256=$CHECKPOINT_MANIFEST_SHA256 --show-plan"
```

The bundle's manifest hash, individual file hashes, platform and image ID are
checked during preparation. Copied helper bytes and each actual container image
are checked again before the Flink process starts. `validate --show-plan` is
Docker-free; it cannot establish FUSE availability or a successful fault.
The helper builder permits necessary public image downloads by immutable digest
and verifies the resulting identity. Maven `-o` and CLI `--offline` constrain
their Maven artifact resolution only; they are not Docker image no-pull flags.

## Run one retained attempt

The native runner requires a local Unix Docker endpoint and preserves the
normal single-result JSON contract. The following selects a fresh empty Docker
client configuration and retains stdout and stderr separately. On Docker Engine
29, the two API-version settings follow the existing
[repository requirements](../README.md#requirements).

```sh
CHECKPOINT_RUN=jobs/checkpoint-storage-run-local-01
mkdir "$CHECKPOINT_RUN"
mkdir "$CHECKPOINT_RUN/docker-config"
export DOCKER_CONFIG="$PWD/$CHECKPOINT_RUN/docker-config"
export DOCKER_API_VERSION=1.44

mvn -q -ntp -Dapi.version=1.44 exec:java -pl cli \
  -Dexec.args="run --catalog-root scenarios --scenario checkpoint-storage-fault --artifact-root . --parameter helper_bundle=$CHECKPOINT_BUNDLE --parameter helper_manifest_sha256=$CHECKPOINT_MANIFEST_SHA256" \
  > "$CHECKPOINT_RUN/result.json" 2> "$CHECKPOINT_RUN/stderr.log"
```

The example generates 30,000 input records for eight keys, with a 5 ms workload
delay and a ten-second checkpoint interval. Those values provide a target
window; they do not replace the live readiness and checkpoint checks. The 5 ms
delay is a requested sleep, not a throughput guarantee: JVM scheduling and the
rest of record processing can make each record take longer. A completed
`run` exits zero only for a passing scenario verdict. Preparation/setup failures
may exit with stderr diagnostics before producing a result JSON.

Keep the result, referenced attempt directory and helper build together. Native
journals are retained under the attempt's `checkpoint-storage-journals/`
directory; the result references their paths, sizes and hashes. Do not rebuild
an artifact referenced by an old run, rerun into an old output directory, or use
`mvn clean` to remove retained checkpoints/logs. A later rerun is a new attempt,
not a replacement for an earlier miss or failure.

The JSON result references each complete journal with `journalPath`,
`journalSha256`, `journalBytes` and `lastSequence`. Its `operationCount` and
`operationCounts` summarize the full journal by operation, state and errno;
`operationsRepresentation: bound-subset-v1` identifies the compact inline list.
That list retains lifecycle/control acknowledgements, every errno=5 create/write,
and reads/writes of the recorded materialized checkpoint files. Commands remain
inline and are bounded at 1,024 per instance. The in-memory validator still sees
every operation. The independent checker reads the hash-bound raw journals,
verifies the exact subset and counts, then uses all records for its audit.

A regression with 10,000 operations renders below 32 KiB per snapshot and
224 KiB for seven copies. Real result size depends on the required EIO and
materialization receipts, commands and other evidence; these fixture bounds are
not a universal size cap. Full journals are retained as files instead of being
repeated in every stage's stdout JSON.

## Six-stage causal proof

The [core state machine](../core/src/main/java/org/savonitar/flink/stability/core/execution/CheckpointStorageFault.java)
retains six ordered samples under one monotonic operation deadline:

| Stage | Required observations |
| --- | --- |
| `BEFORE_ARM` | Healthy verified JobManager/TaskManager FUSE instances; live keyed progress and committed prefix; latest completed checkpoint N with no in-flight checkpoint; exact external keyed-state materialization. |
| `ARMED` | Matching fault identity and revision-one acknowledgements from both instances, each bound to its exact native ARM journal sequence; unchanged checkpoint boundary and original running execution. |
| `FAILED_CHECKPOINT` | One acknowledged trigger POST, exactly one new failed checkpoint, bound workload snapshot receipt, retained REST failure cause and a matching real TaskManager create/write `errno=5` operation under that job/checkpoint path. |
| `STOPPED` | The fault remains armed; the original TaskManager is physically dead and processing subtasks are inactive; no competing checkpoint has completed. A single JobManager restore decision for N may already appear before replacement resources exist. |
| `HEALED` | Explicit revision-two healing of surviving instances, bound to the native HEAL journal receipt; the stopped instance remains accounted for by physical death; checkpoint boundaries still exclude a competing completion. |
| `RECOVERED` | A different TaskManager incarnation with a fresh healthy FUSE instance; restoration of N, subsequent events for all keys, and a later successful checkpoint with bound SNAPSHOT and COMPLETED receipts. |

A trigger ID is not a checkpoint ID. Flink's failed asynchronous trigger result
contains a failure cause without a checkpoint ID. The correlator therefore uses
the before/after history and count deltas, requires one retained new failed
checkpoint, and binds it to the original workload process and native receipt.
It never retries the POST after a lost or ambiguous acknowledgement. Automatic
checkpoints are included in this accounting; sleeping longer does not make an
ambiguous window valid.

The REST adapter reads bounded history, exact checkpoint details and checkpoint
configuration. It rejects contradictory IDs, statuses/counts, duplicate JSON
keys, trailing JSON and oversized responses. REST confirms single checkpoint
concurrency and failure tolerance, but does not expose the inline threshold.
JobManager restoration statistics can advance before a new TaskManager is
deployed; they are not themselves evidence that restored keyed state was used.

After the atomic phase, the job must finish naturally. The existing physical
writer fence, fixed Kafka high-watermark boundary and complete `read_committed`
traversal remain mandatory. The independent state-prefix oracle checks the
per-key count and prefix sum alongside exact IDs; workload receipts establish
provenance and do not decide data correctness.

## Proving that keyed state reached a file

The [checkpoint inspector](../testcontainers/src/main/java/org/savonitar/flink/stability/testcontainers/storage/CheckpointStateInspector.java)
runs as UID/GID `9999:9999` with a 128 MiB JVM heap. It reads checkpoint N's
`_metadata` through the FUSE view and invokes the released runtime's
`Checkpoints.loadCheckpointMetadata` through reflection. It selects exactly the
`flink-stability-keyed-prefix-v1` operator, requires its single subtask and
nonempty managed keyed state, and accepts only `KeyGroupsStateHandle` delegates
of type `FileStateHandle` or `RelativeFileStateHandle`.

Every selected state file must be a nonempty regular file inside that exact
namespace, job ID and `chk-N` directory. The inspector rejects inline or unknown
handles, foreign checkpoints, traversal, symlinks and duplicate references. It
reads and hashes the actual files through FUSE and emits their paths, sizes and
SHA-256 values together with the metadata hash. Bounds are 16 MiB of metadata,
128 selected state files and 64 MiB of selected state in total. Relative handles
are resolved by Flink against the checkpoint's external pointer.

The core combines this receipt with a successful original-TaskManager write and
an inspector read of the same state path in the native journals. Consequently,
a completed checkpoint, an existing `_metadata`, a nonzero task `state_size`,
or an unrelated Kafka source/sink state file is insufficient by itself. This
first inspector deliberately does not accept incremental RocksDB/shared-file
layouts or other state-handle formats.

## Filesystem and lifecycle boundaries

The [container adapter](../testcontainers/src/main/java/org/savonitar/flink/stability/testcontainers/FuseCheckpointStorageRuntime.java)
creates one fresh named backing volume. Each Flink incarnation mounts its own
FUSE view at `/flink/checkpoints`; the job uses the generated namespace below it.
The root-owned backing path `/flink/checkpoint-backing` has mode `0700`, and raw
read/write denial is checked with an unprivileged JVM. Native control and
journals live under `/run/flink-checkpoint-storage`. Process logs and retained
evidence use the separate existing attempt evidence bind at `/flink/evidence`.
The checkpoint backing volume is not a host bind.

Containers receive `/dev/fuse` and only `SYS_ADMIN`, with other capabilities
dropped and `no-new-privileges` enabled. The actual Flink JVM UID/GID and zero
effective capabilities are checked independently. There is no privileged
container, host namespace, host block device or Docker socket mount. Unlike the
isolated PR 3 I/O probe, integrated Flink containers use the owned cluster network
for Flink/Kafka traffic.

The rule fails namespace-scoped create/write operations, including writes on
existing descriptors. Existing reads, metadata operations and cleanup remain
available, and healthy force operations reach the backing filesystem. The
native helper disables writeback/data/metadata caching and records the effective
policy. This models an application-visible checkpoint filesystem write error;
it does not establish coverage of physical disk disappearance, `fsync` failures,
silent corruption, Kafka log-volume loss or remote object-storage behavior.

On partial setup, missing acknowledgements, deadlines or failed evidence, the
attempt retains the error and requests bounded abort/cleanup. Expired or aborted
rules cannot be healed into a successful experiment. The adapter captures the
stopped incarnation's journal before removing its container and preserves
contiguous prefixes across later captures. The integrated adapter explicitly
sets journal bounds of 16 MiB and 25,000 records per incarnation; the retained
journal-copy budget is 128 MiB. The helper's minimum configurable limits are
64 KiB and 10 records. Individual records cover mutations, errors,
control/lifecycle events and checkpoint-file reads. Successful ordinary reads,
getattr, readdir, access, flush and release instead accumulate per-instance
counters; STATUS reports these without consuming a journal record, and SUMMARY
records flush pending counters after 1,000 aggregated operations or one second
of activity. Fault-family results and control acknowledgements are synced before
reply; aggregate summaries do not force a sync per read.

These caps imply a workload-dependent duration limit: available records divided
by journal records/second, or available bytes divided by journal bytes/second,
whichever runs out first. At 100 records/second averaging 1 KiB, 16 MiB allows
less than 164 seconds after control/reserved overhead; 25,000 records alone would
allow less than 250 seconds. Aggregation reduces metadata traffic but does not
remove the duration limit for frequent writes. Exhaustion emits a terminal
incomplete receipt, refuses mutations with EIO and continues forwarding reads
and metadata; it can never qualify a successful experiment. An EXPIRED rule also
continues injecting EIO until cleanup; expiry does not heal the view.

This journaling revision requires a new helper build. Historical helper hashes
and retained attempts remain unchanged. Exact owned-volume cleanup is attempted
even after an uncertain creation response. Cleanup failure remains visible in
the completed result.

Storage operations create their deadline before acquiring the timed evidence
lock. Docker copy, inspection and exec calls run through a bounded driver-call
boundary; a transport that ignores interruption cannot keep the caller holding
the evidence lock indefinitely. A capture that cannot acquire the lock returns
the last immutable snapshot with explicit incomplete/non-quiescent status.
Cleanup closes the lifecycle independently of that lock; abandoned workers
cannot publish a late successful result or launch the next command. Volume
creation/removal retain their ordering even if creation returns late. These
bounds cover driver and lock waits; they do not preempt a stalled host filesystem
while writing local evidence.

## Result interpretation and remaining qualification

The JSON evidence includes `checkpointStorage`, `checkpointStorageFaults`,
state-recovery evidence and the terminal oracle. Each fault reports its stage
samples, transitions, materialization, trigger response, failed checkpoint,
kill/replacement identities, recovery, errors, `confirmed` and
`finalCaptureConfirmed`. Final confirmation requires a complete quiescent
capture after the physical fence, with the earlier receipts preserved.

Missing EIO, conflicting checkpoint history, a newer completed checkpoint,
premature failover, expired/aborted daemon state, incomplete logs, a missing
replacement identity or missing restored-key progress cannot become PASS merely
because the final output happens to be correct. The reason
`checkpoint-storage.effect-unconfirmed` identifies incomplete storage proof.
Independent data/state failures and `verification.*` failures remain visible;
they are not erased by incomplete fault evidence.

## Retained verification and artifacts

The final JDK 21 reactor
[verify](evidence/checkpoint-storage-integration-checks-20261027-01/maven-verify-final.log.summary.json)
passed after the pilot corrections: 1,611 Java tests reported, zero
failures/errors, one opt-in Docker lifecycle test skipped; elapsed time was
21.779 seconds. This is the retained PR 4 regression check; the later
[review verification](CHECKPOINT-STORAGE-QUALIFICATION.md#review-corrections-and-independent-re-audit)
records the deadline correction and its fresh live smoke separately.

The earlier full JDK 21 reactor install in
[install 04](evidence/checkpoint-storage-integration-checks-20261027-01/maven-install-04.log.summary.json)
completed successfully: 1,602 Java tests reported, zero failures/errors, one
opt-in Docker lifecycle test skipped. Coverage includes released Flink REST
serialization, response limits/deadlines, real checkpoint metadata round trips,
relative handles, inline-state rejection, path isolation, native journal parsing
and the causal state machine. This is offline implementation coverage, not live
FUSE qualification.

Subsequent focused checks also passed:

- [Install 05](evidence/checkpoint-storage-integration-checks-20261027-01/maven-install-05.log.summary.json):
  55 runtime-identity and storage-fault tests, including the corrected effective
  configuration pairing and causal evidence checks.
- [Install 06](evidence/checkpoint-storage-integration-checks-20261027-01/maven-install-06.log.summary.json):
  nine adapter tests, including the explicit 16 MiB/25,000-record journal limits.
- [Install 07](evidence/checkpoint-storage-integration-checks-20261027-01/maven-install-07.log.summary.json):
  77 verdict, runtime-identity and storage-fault tests, including bounded polling
  while checkpoint history is pending after a failed trigger.
- [Python tooling](evidence/checkpoint-storage-integration-checks-20261027-01/python-unittest.log.summary.json):
  all 314 tests passed.

The integrated pilots use
[helper build 04](evidence/checkpoint-storage-helper-build-20261027-04/build-evidence.json),
which adds the metadata inspector to `probe.jar`. Its native helper bytes remain
the same as the qualified PR 3 build. Both workloads come from the separate
[state-recovery build 02](evidence/state-recovery-build-20261026-02/build-evidence.json).
The correct and mutant JARs are frozen copies; the mutant still resets only on
genuine restoration, and its expected-result catalog remains PASS.

| Frozen artifact or receipt | SHA-256 |
| --- | --- |
| Helper build 04 `build-evidence.json` | `9ab2001ccc55e700a76c12d0ba64d4824d1089d21e85adffa5216d75d8582ecf` |
| `checkpoint-fuse` | `49d01b4f62b6a7bd8256779777cf1e6d193d2e1d7365796c115f4ffb66c85f66` |
| `checkpoint-fuse-selftest` | `bbdd6a07da940c9d9db135958ec7f1caf8502f03767203f349aa65b5c7a3b9d5` |
| `probe.jar`, including metadata inspector | `2fd880248a7515b08ee419e1ff875cc1edff8e0f784878aa55a0aaa0a1780983` |
| Workload build 02 `build-evidence.json` | `0b5553d8ba108112d919dccde6de8ecb0bd103b5963d3311ba3fcf9c02496456` |
| Correct workload | `4c8ca4211b01d26ba0c49a2060259c117b95016fef798b4a0c187a332edd9711` |
| Reset-state-on-restore workload | `0ad8e5eeec5deb8e732a346dffc54c3675a6cb7c086e5c1bfa23a1389798d54a` |

The first two healthy-control pilots remain retained with their original
outcomes:

| Attempt | Observed outcome and scope |
| --- | --- |
| [Batch 01, pilot 01](evidence/checkpoint-storage-integration-pilots-20261027-01/attempts/healthy-fuse/run/pilot-01/stdout.summary.json) | INCONCLUSIVE, `infrastructure.flink-start-failed`, because effective configuration evidence did not match. The configuration pairing was corrected; the original result remains unchanged. |
| [Batch 01, pilot 02](evidence/checkpoint-storage-integration-pilots-20261027-01/attempts/healthy-fuse/run/pilot-02/stdout.summary.json) | FAIL, `verification.flink.job-completion-timeout`, with the job still RUNNING at the three-minute completion limit. Flink reported 17 completed checkpoints and no restoration/failure. Both journals were complete after the physical fence, with 9,187 and 2,369 records. These observations do not make the incomplete run pass. |

The requested 5 ms per-record sleep did not imply completion of all 30,000
records within three minutes. A fresh
[batch 02 manifest](evidence/checkpoint-storage-integration-pilots-20261027-02/manifest.json)
keeps the same workload/helper bytes, input, key count, checkpoint settings,
fault timeout/lease and PASS expectations. Its only scenario changes are the
natural completion budget from three to five minutes and references relocated
to the new batch. The
[derivation receipt](evidence/checkpoint-storage-integration-pilots-20261027-02/evidence/derivation.json)
binds those changes to the parent manifest. Framework JARs and source/class
hashes are captured separately for each attempt; the adapter's updated journal
limits are framework changes, not changes to the frozen helper artifact.

The first batch 02 healthy control passed independent evidence review:

| Attempt | Observed outcome and scope |
| --- | --- |
| [Batch 02, healthy-fuse pilot 01](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/healthy-fuse/run/pilot-01/stdout.summary.json) | PASS, `validator.kafka.state-prefix.match`. Flink FINISHED with 21 completed checkpoints, no restores and no failures. The terminal oracle traversed the fixed boundary and found exactly 30,000 expected IDs, zero missing/unexpected/malformed/duplicate IDs and zero state mismatches across eight keys. Physical fencing and quiescent storage capture were confirmed; component errors and storage diagnostics were empty. |

The result SHA-256 is
`e17a408c9f0f6f5ecd39b0bd99129e2fff67ab9ce2b89b15780606aaa0d73db4`.
Both exact referenced journal files matched their byte lengths and SHA-256,
with contiguous sequences and every retained operation matching its raw record:

| Journal | Records | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| JobManager | 10,886 | 7,727,674 | `83353691e76adcd65f92e2225c8acfe00fd7e5fda02778c46e3c8d039274e257` |
| TaskManager | 2,761 | 1,891,511 | `e18c461253ac0a172c13f1335b778062013fa44f569809dda286d3be0d997d83` |

The journals contained only STARTING/HEALTHY states and no EIO. Both JVMs ran
with UID/GID 9999 and no effective capabilities; raw-backing reads and writes
were denied. All 21 frozen batch files matched the manifest. The
[framework snapshot](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/healthy-fuse/run/pilot-01/framework/manifest.summary.json)
has SHA-256 `be8fca98347dd11df538dc701bac14a7028de62df97ac320037ac3eb6d117b2c`;
its six copied JARs matched the recorded target/installed hashes, and all 752
captured class/resource hashes matched entries inside those JARs. Runtime image,
helper, workload and twelve-artifact connector closure pins also matched.
These binary checks do not establish a source revision.

The atomic-fault pilots are also retained with their original failures:

| Attempt | Observed outcome and scope |
| --- | --- |
| [Batch 02, correct-eio pilot 01](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/correct-eio/run/pilot-01/stdout.summary.json) | FAIL, `verification.flink.job-completion-timeout`. Checkpoint 2 was the completed baseline. The failed trigger's stack identified checkpoint 3 and `Input/output error`; native TaskManager receipts recorded EIO creates under `chk-3`. The immediate FAILED_CHECKPOINT history still reported total/completed/failed counts 2/2/0, so the runner rejected an `Ambiguous failed checkpoint window` and issued ABORT. No planned TaskManager kill/replacement was confirmed. Continued checkpoint failures and automatic recovery preceded the completion timeout; the terminal oracle did not run. |
| [Batch 02, correct-eio pilot 02](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/correct-eio/run/pilot-02/stdout.summary.json) | FAIL, `verification.flink.job-completion-timeout`. Completed checkpoint 1 and the unique EIO failure of checkpoint 2 were correlated, and the physical TaskManager kill was recorded. The STOPPED sample contained checkpoints 3–6, all FAILED and triggered after the kill; totals were 6 attempted, 1 completed, 5 failed and 0 in progress. The runner rejected `Checkpoint boundary changed before healing`, issued ABORT and never healed or started a replacement. The terminal oracle did not run. |

Pilot 01 result SHA-256 is
`f145b0c07873a7ed839fdaedd6dd9d7dd7abe174e5d529dc0cdbc91b2381deee`.
Inspection of the released Flink 2.2 bytecode confirmed that checkpoint
statistics can be cached for three seconds by default:
`RestOptions.CACHE_CHECKPOINT_STATISTICS_TIMEOUT` falls back to
`WebOptions.REFRESH_INTERVAL`. The early history observation was therefore
insufficient to correlate the failed trigger. The runner now polls pending
history within the existing deadline; the strict final correlation requirements
and the original pilot result remain unchanged.

Pilot 02 result SHA-256 is
`3488c465a17f86444b80106fac70d2e06511b3b9cf9d2a9fabd77b22c29e0ca8`.
Its later failed checkpoints arose while the JobManager detected the killed
TaskManager's loss. The unchanged-count requirement was too strict for this
window. The corrected boundary accounts for every new post-kill FAILED
checkpoint: the increase in total equals the increase in failed, original
checkpoints remain retained, trigger times meet the kill bound, counters are
monotonic, and no checkpoint is newly completed or still in progress. Pending
in-progress history is polled within the existing deadline. Pilot 03 supplies
the following integrated observation after that correction.

| Attempt | Observed outcome and scope |
| --- | --- |
| [Batch 02, correct-eio pilot 03](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/correct-eio/run/pilot-03/stdout.summary.json) | PASS, `validator.kafka.state-prefix.match`. All six causal stages and final capture were confirmed: completed checkpoint 1, failed checkpoint 2, physical kill, heal, replacement and restoration from checkpoint 1. Flink FINISHED with 22 completed checkpoints and one restoration. Exactly 30,000 IDs and eight keys passed with zero data defects or state mismatches. |

The result SHA-256 is
`e9625a6a0cfe4f5e828a5b251479c0dad01d608e07c9711682a21fa472aab03a`.
Independent review matched all 16 stage/final journal references to 12 raw
files, including byte lengths, hashes, contiguous sequences, every operation
projection and each prefix of the final capture. Both ARM acknowledgements and
the surviving JobManager's HEAL acknowledgement matched their native journal
receipts. The old TaskManager recorded 20 EIO create operations under `chk-2`.
Checkpoints 3–4 failed after the kill without advancing the completed baseline.

The metadata inspector identified the exact keyed operator's external state in
checkpoint 1: 2,638 bytes of metadata and a 2,068-byte managed keyed state file.
The retained inspector hashes and successful JobManager reads, original
TaskManager writes and replacement TaskManager reads agree on the paths and
sizes. State-file bytes were not retained after cleanup for a later independent
rehash. Restoration identity and all eight restored-key arithmetic receipts
were independently checked.

| Final journal | Records | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| JobManager | 11,932 | 9,172,904 | `e327304d27e60060431de78ae9ed1e81e7a7c839e94d66ab7877de4c2a891059` |
| Original TaskManager | 647 | 466,910 | `d0b80fbc5c080563bbc4762697a3fc6da717bb00a921a138195b621a0c5f7b84` |
| Replacement TaskManager | 4,612 | 3,224,894 | `4ae8d65a42330214ce3dc7c6f1c254770828afaeb9e491825ca03ef5a701d057` |

All three storage instances were complete and quiescent after their physical
fences, with empty storage/fault diagnostics. Artifact pins and the six copied
framework JARs, including 752 class/resource entries, matched the
[framework manifest](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/correct-eio/run/pilot-03/framework/manifest.summary.json),
SHA-256 `13a7a79f53b0420a879ee050c7a2f7ccf0da9bcf5f80194a860c1755261b2f7c`.

The paired state-reset mutant demonstrated the oracle's sensitivity:

| Attempt | Observed outcome and scope |
| --- | --- |
| [Batch 02, mutant-eio pilot 01](evidence/checkpoint-storage-integration-pilots-20261027-02/attempts/mutant-eio/run/pilot-01/stdout.summary.json) | Raw FAIL, `validator.kafka.state-prefix.state-mismatch`, against the unchanged PASS expectation. Flink FINISHED with 21 completed checkpoints and one restoration from checkpoint 1. All 30,000 IDs were present exactly once with no malformed/unexpected IDs, but 29,671 records had incorrect state. All six fault stages, final capture and recovery provenance were confirmed. |

Its result SHA-256 is
`b7642c06315649293e444a71b575749b65eba3e227a3f5347b92e13537bf6d25`.
The first mismatch was ID 329: expected count 42 and sum 6,972, observed count 1
and sum 330. Eight restored-key receipts proved correct nonzero prior state;
eight paired reset receipts then proved the deliberate reset on the replacement
execution after restoration from checkpoint 1. No reset occurred on initial
execution. All 100 retained mismatch samples matched the reset arithmetic.

The mutant's framework manifest and all six copied framework JARs were byte
identical to correct pilot 03. Workload JAR differences were confined to the
manifest and immutable build-mode resource; class bytes were identical. The
same independent checks covered materialization, ARM/HEAL bindings, 20 native
EIO creates, the physical fences and all 16 journal references. Twelve unique
journal files matched their lengths, hashes, operation projections and final
prefixes. All three storage instances were complete and quiescent with empty
storage/fault diagnostics.

| Mutant final journal | Records | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| JobManager | 11,414 | 8,771,050 | `1b36c03862c21851c43a667b1823d072249e284f882d7fb0cbbe77d7ae05fb0f` |
| Original TaskManager | 647 | 466,910 | `2801157f7918cd36bbe0287d75524adb7627a8602cebf3c97b38e3eace825b56` |
| Replacement TaskManager | 4,481 | 3,135,037 | `dc6c6d9acfc15ba6846526e28620065ddb8262f63dcb0d3b38c43822d3a3d8c9` |

**Each atomic pilot retained two producer-fenced component events.** Both events
were captured on `taskmanager-1#2` for transactional ID `keyed-prefix-0-1`: an INFO
event from Kafka's TransactionManager and an ERROR from Flink's KafkaCommitter
reporting that it could not commit a fenced producer's transaction. Component
coverage is `captured-prefix`. The correct pilot's exact terminal oracle passed;
the mutant failed state arithmetic. These observations do not establish that
the component events are harmless or that other recovery paths preserve
correctness.

The retained [cleanup check](evidence/checkpoint-storage-integration-checks-20261027-01/cleanup-check.json)
covered all seven attempts: all 22 exact container IDs were absent, and each of
the seven owned storage-volume label queries returned no volumes. The receipt
records `allRemoved: true`; no retained run files were removed.

**PR 5 qualification subsequently completed:** the
[separate frozen matrix](CHECKPOINT-STORAGE-QUALIFICATION.md) confirmed all twelve
attempts on Docker Desktop `linux/arm64`, with nine raw PASS results and three
raw state-mismatch FAIL mutants. Each retained all 30,000 IDs. All 120 observed
native EIO operations were `create`, and 18 producer-fenced component events
remain visible. The earlier pilot misses/failures remain retained and do not
count as qualified cells; other syscall and platform paths remain separate.
