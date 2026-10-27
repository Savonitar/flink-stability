# State recovery and checkpoint-storage fault implementation plan

Status: PR 0 / PR 1 implemented and verified offline; PR 2 implemented and
[live-qualified](STATE-RECOVERY-VALIDATION.md), 2026-10-26. The fixed matrix
confirmed all twelve attempts: nine PASS controls and three raw mutant state
FAIL results, with zero ID defects. PR 2 full-reactor installation reported
1,488 Java tests (one opt-in Docker test skipped), and Python passed 258 tests. The earlier
PR 0 / PR 1 verification covered contract/capability checks, the stateful workload
and independent oracle. JDK 21 `mvn -o -B -ntp verify` passed; the final
`mvn -o -B -ntp install` reran all 1,205 Java tests successfully.
`python3 -m unittest discover -s tools/tests -v` passed all 240 tests.
After installation, CLI `validate --offline --show-plan` accepted both
`state-prefix-baseline` and the legacy `bounded-eos` catalog entries with
`--catalog-root scenarios --artifact-root .`. Local HTTP test servers required
permission outside the socket-restricted sandbox. That PR 0 / PR 1 verification
did not run Docker. The
[`state-prefix-baseline`](../scenarios/state-prefix-baseline.yaml) is an
uninterrupted control, not recovery qualification. PR 2 now has the automatic
readiness gate, typed recovery report, separately built reset-state mutant and
[calibration recipe/checker](../calibration/state-recovery/README.md). Its live
gate passed the retained twelve-attempt matrix below. PR 3's isolated
[FUSE prototype](CHECKPOINT-STORAGE-FUSE-PROTOTYPE.md) was implemented and
qualified on Docker Desktop `linux/arm64` on 2026-10-27: all seven standalone
probe cases passed, including create/write EIO and explicit healing. Native
Linux qualification is separate. PR 4 integrated Flink checkpoint-storage faults
remain unimplemented.

The delivery order is **state-dependent workload and independent oracle → restore
calibration → FUSE prototype → integrated checkpoint-storage experiment**. Each
gate below must pass before the dependent stage proceeds. Historical roadmap
files are not the checklist for this work.

## Goal and first supported slice

Demonstrate that a temporary filesystem write failure during checkpointing,
followed by TaskManager replacement, preserves both Kafka exactly-once output
and the application's accumulated keyed state.

Start with the existing Flink 2.2.0 / Kafka 4.0.0 / connector 5.0.0-2.2 baseline:
one JobManager, one TaskManager, parallelism one, one input and one output
partition, HashMap state, TTL disabled, filesystem checkpoints, and an EOS sink.
Use the existing INCREMENTING transaction naming strategy first. Run one attempt
at a time, with `runs: 1` and `health_retry_limit: 0`.

The first storage fault is **EIO on checkpoint-file create/write operations**.
Reads of an existing completed checkpoint remain available. This models a
filesystem I/O failure, not permanent disk loss, bit corruption, ENOSPC, or a
power-loss durability guarantee. RocksDB, rescaling, HA, multiple workers,
broker-disk loss, other concurrent faults, and native suite execution are later
slices. A Flink filesystem plugin is not a substitute for the requested FUSE
prototype.

## Existing boundaries to preserve

- [The workload](../flink-job-generator/src/main/java/org/savonitar/flink/stability/flinkjob/FlinkKafkaEosJob.java)
  preserves the existing `ValueState` integer passthrough by default. The new
  opt-in keyed-prefix behavior emits state-dependent values; the legacy output
  does not prove restoration of that application state.
- [Input preparation](../core/src/main/java/org/savonitar/flink/stability/core/execution/kafka/KafkaInputPreparer.java)
  already acknowledges and reconciles bounded integer input. Reuse this manifest.
- [The current oracle](../core/src/main/java/org/savonitar/flink/stability/core/validation/kafka/KafkaIdSetValidator.java)
  keeps the existing `kafka.id-set` path. The new `kafka.state-prefix` composite
  validator is integrated through execution/result/CLI types.
- [Flink containers](../testcontainers/src/main/java/org/savonitar/flink/stability/testcontainers/FlinkContainer.java)
  share checkpoint storage that also contains class-loading/process evidence.
  Never apply a storage fault to that entire existing root.
- Keep natural bounded completion, the irreversible physical writer fence, one
  fixed raw Kafka high-watermark boundary, complete `read_committed` traversal,
  artifact hashes and class origins, and the existing stdout/exit-code contract.

## PR 0 — Freeze the narrow contract and independent fixtures

Update SPEC-001, SPEC-002 where necessary, SPEC-005, the JSON schemas, and
handwritten fixture documents. The field/type names below are now implemented
for the first package; execution remains limited to the explicitly supported slice.

1. Add an optional typed job behavior, `behavior: { type: keyed-prefix-v1,
   key_count: 8 }`. Omission retains the current integer passthrough behavior.
   Deliver it through reserved workload configuration; `program_args` cannot
   override it.
2. Add one composite terminal validator, `kafka.state-prefix`, bound to that job,
   its sink, and the reconciled input manifest. It checks ID integrity and state
   values in one traversal. Do not require a second independent `kafka.id-set`
   read of the same topic.
3. Keep workload protocol v1 and add an explicit JAR capability declaration for
   `keyed-prefix-v1`. Require it only when this behavior is selected. An old v1
   passthrough JAR must reject during preparation, before Docker.
4. Reject unsupported combinations explicitly: partial input evidence, input
   faults, TTL, at-least-once mode, multiple source partitions or parallelism
   above one in this first slice, and restore/savepoint transitions between
   passthrough and keyed-prefix behavior.
5. Define bounded typed progress/snapshot evidence, identified by JobID, operator
   UID, execution attempt, subtask, and checkpoint ID. It supports the recovery
   window gate; it is not a replacement for the external data oracle.

**Done when:** independent fixtures cover valid controls, old-JAR mismatch,
behavior/validator mismatch and unsupported combinations; all existing scenario
defaults and output formats retain their meaning. Schema acceptance must not
advertise execution before the following implementation is available.

## PR 1 — Stateful workload and independent terminal oracle

### Workload

Keep the generated input IDs `i = 0..N-1`. With `K=8`, set `key = i mod K`.
For each key, read `ValueState(count, sum)`, increment count and add `i+1` to sum,
then emit one record:

```json
{"version":1,"id":4,"key":0,"count":3,"prefixSum":9}
```

This example uses `K=2`. Its expected output sums for IDs `0..5` are
`1, 2, 4, 6, 9, 12`.

The output must be computed from restored state, never reconstructed from ID.
Use checked signed 64-bit arithmetic, a stable operator UID and explicit state
type/serializer. The new operator gets a distinct UID/state name; preserve the
old operator's UID and default behavior. Keep the JAR thin.

Initially use one source partition and parallelism one, so per-key order is
unambiguous. For a later multi-partition extension, the existing producer mapping
`partition = i mod P` permits `K mod P = 0`; that extension needs its own tests
and live qualification and is not silently enabled here.

### Oracle

For `i = k + s*K`, calculate independently:

```text
expectedKey   = k
expectedCount = s + 1
expectedSum   = (s + 1)*(k + 1) + K*s*(s + 1)/2
```

The oracle uses the complete reconciled manifest and these equations. It must
not call the workload's accumulation function or derive expected values from
observed output. Verify the manifest's input order/partition preconditions.

Read one fenced Kafka snapshot and compare records by ID, not output arrival
order. Require exact ID coverage, no duplicates, and the correct key, count and
sum for every record. Strictly decode the versioned format, reject duplicate or
unknown JSON fields and non-integral/out-of-range numbers, and bound record
size, total records, evidence samples, and the entire verification deadline.
Retain the existing one-million-record cap; set the new payload limit to 1 KiB
and mismatch samples to at most 100. Bound aggregate retained payload bytes to
128 MiB as well as record count; these are retention limits, not a promise about
Kafka client's total heap usage.

Change `KafkaClientSnapshotReader` / `KafkaTopicSnapshot` explicitly: the current
reader truncates values above 64 characters before validation and uses a string
deserializer. Preserve that existing ID-mode behavior; the new format needs
bounded byte retention and strict UTF-8 decoding before information is lost.
Oversized/malformed records retain only bounded diagnostic samples and must not
become valid truncated JSON. Total-byte exhaustion is a verification failure.
Test valid state records longer than 64 characters, invalid UTF-8, oversized
records, byte-budget exhaustion and the unchanged legacy ID path.

Use distinct reasons for malformed records, unexpected/missing/duplicate IDs,
and `validator.kafka.state-prefix.state-mismatch`. Report all defect totals even
when one reason has precedence. An incomplete snapshot or verification timeout
retains `verification.*`; it cannot satisfy an expected state-mismatch result.

Introduce only the typed result/plan alternatives needed for `kafka.id-set` and
`kafka.state-prefix`. Preserve existing JSON for existing scenarios; new state
results add bounded ID/state evidence and validator identity. Extend strict
expected-result matching for the new validator without accepting arbitrary
execution failures as expected oracle failures.

**Main modules:** `flink-job-generator` configuration/pipeline; `core` schema,
artifact capability validation, executable plans, Kafka validation,
`V1ScenarioExecutor`, `V1ScenarioExecutionResult`, `ScenarioVerdict`; `cli` result
rendering and diagnostics. Keep runtime contracts JDK-only.

**Done when:** handwritten examples and adversarial fixtures detect wrong counts,
sums, keys, reset prefixes, lost/duplicated IDs, malformed payloads, incomplete
reads and deadlines. A stream with every ID exactly once but wrong accumulated
state must fail. Full reactor verification passes after the cross-module change.

## PR 2 — Prove recovery sensitivity before adding FUSE

The implemented [state-recovery recipe](../calibration/state-recovery/README.md)
provides separate catalogs and a separately built calibration mutant. The mutant
resets only the keyed aggregate, once per key on its first event
after restore. Source offsets and sink transaction recovery remain unchanged.
Use the same UID, serializer and configuration on both arms; record the artifact
hashes and mutation scope. A normal initial start must not activate the mutant.
The `state-reset-on-restore` Maven profile produces a distinct classifier. The
correct and mutant JARs have identical class bytes and differ only in the
manifest build mode and their private packaged identity resource. The operator
reads that resource from its own code-source JAR with missing/duplicate/format
checks; there is no runtime argument or ambient-property switch. `build.py`
freezes source/recipe copies and hashes, uses separate build directories and
clones/copies an explicitly selected Maven artifact cache into the fresh output.
It supplies empty Maven settings and never reads user settings or credentials.

Before injecting a kill, require RUNNING, a committed output prefix covering
every key, bounded input still remaining, and a completed checkpoint containing
nonzero progress for those keys. Bind the snapshot/progress evidence to that
checkpoint and the current execution attempt. Use bounded observations rather
than a sleep as the proof of readiness. After replacement, bind the restored
checkpoint to the pre-fault progress and require subsequent events for those keys.
The gate runs automatically before each keyed TaskManager kill, under one `45s`
deadline. It retains checkpoint candidates tied to the target process and current
execution attempt; the actual restored checkpoint must be one of those candidates.
The latest REST completed checkpoint must have matching receipts, with no visible
checkpoint still in flight or newer unaccounted snapshot. The already validated
committed prefix is retained while fresh checkpoint/process observations are
polled. That final observation supplies the kill baseline, avoiding a second full
job observation after readiness. A checkpoint completing in the remaining
read-to-kill race is still a window miss if its identity was not admitted before
injection; later logs never retroactively supply pre-kill proof.
If readiness is missing, the executor skips injection and remaining phase steps,
then still attempts natural completion, fencing and the independent terminal
oracle. A missing proof downgrades passing data to INCONCLUSIVE and preserves an
observed data FAIL. Kill scenarios require `N >= 2*K` at compilation; uninterrupted
`N < K` controls remain supported using final snapshot progress for reachable keys.

`evidence.stateRecovery` reports the workload hash, JobID, key/input counts,
physical-fence confirmation, bounded process-bound trace, readiness attempts,
recoveries and observation errors. START identifies build mode; SNAPSHOT plus
COMPLETED binds pre-kill state to a checkpoint; RESTORED_KEY records the actual
first transition for each key after restore; MUTANT_RESET proves the intentional
reset; PROGRESS records first all-key coverage in an incarnation. These receipts
prove provenance while the independent oracle decides state correctness.

| Workload artifact | Uninterrupted | TaskManager kill and restore |
| --- | --- | --- |
| Correct workload | PASS | PASS, recovery confirmed |
| State-reset-on-restore mutant | PASS | FAIL with state mismatch; ID checks still pass |

Initial qualification batch: three attempts per cell, twelve attempts total,
after any separately retained pilots. Qualification requires three confirmed
results with the required outcome per cell. A window miss is retained and leaves
the matrix incomplete; it does not count as a confirmed result. Keep expected-result catalogs at PASS;
the intended mutant FAIL is calibration evidence. Do not invert its expectation
or discard misses. Missing recovery/progress proof prevents PASS, while an
observed data failure remains visible.

The dedicated [`calibration/state-recovery/check.py`](../calibration/state-recovery/check.py)
and independent test fixtures check
workload artifact identity, complete ID/state totals, window/recovery evidence,
controls and matrix completeness. Existing connector-oriented `pr_gate.py` and
`analyze_attempt.py` do not automatically understand the new state result; use
the CLI plus this checker until they receive an explicit extension.

**Done when:** the full matrix establishes both ordinary restoration and oracle
sensitivity. FUSE implementation starts only after this gate passes.

## PR 3 — Isolated FUSE feasibility prototype

Implemented and qualified on the first target; see the
[retained evidence and execution guide](CHECKPOINT-STORAGE-FUSE-PROTOTYPE.md).
Normal Maven CI remains independent of FUSE and Docker. The contract below is
implemented by a separate native helper bundle and an opt-in container probe;
it has not yet been wired into Flink provisioning.

Build a small passthrough daemon using pinned libfuse3/native dependencies and a
bounded Java I/O probe. Put native adapter sources/build assets under
`testcontainers`, with an opt-in probe entry point such as
`tools/checkpoint_storage_probe.py`; normal Maven CI must not require FUSE or
Docker. Record helper hashes, platform, image identity, kernel and mount settings.

### Mount and privilege design

- Mount FUSE inside each container that accesses checkpoint storage, over a
  common backing directory/volume belonging to the new attempt. Do not depend
  on sidecar mount propagation between Docker Desktop containers.
- Present the generated checkpoint namespace through FUSE. Put daemon control,
  journals, process/class-loading logs and retained evidence outside that mount.
  Flink configuration references only the FUSE view; any raw backing access is
  adapter-internal and never a fallback path. Verify that the JVM identity cannot
  read or write the raw backing path directly.
- Use only `/dev/fuse` and the required mount capability. Do not use
  `--privileged`, host namespaces, host block devices or the Docker socket.
  Launch the JVM with its normal unprivileged identity after mount setup.
- Pin direct-I/O behavior, disable writeback and metadata caching for this first
  prototype, and test cross-container visibility. Record that these choices
  affect filesystem performance; this is not a performance benchmark.
- Probe the actual execution host. If FUSE/capabilities are unavailable, stop
  with an explicit unsupported result. Do not substitute a Java filesystem
  plugin, silently bypass FUSE, or claim success on an untested Docker platform.

Prefer a separately hash-verified helper/dependency bundle copied into the
existing distribution, without rebuilding Flink JARs. If a derived container
image is necessary, use the existing custom-runtime identity contract and run
all matching controls on that same pinned image.

### Fault and probe contract

First mode: fail `create/write` with `EIO` within the exact job checkpoint
namespace. Existing reads, stat and cleanup remain available. Forward healthy
`fsync` to the backing filesystem; do not depend on a FUSE `release` callback to
simulate an application-visible close error.

Rules carry attempt/storage identity, fault ID, revision, allowed operations,
and a bounded lease. Journals retain relative path, operation, instance, revision,
timestamp, result/errno and receipt sequence. Truncation or lost receipts is
explicit and invalidates complete fault confirmation.

The standalone probe must demonstrate:

1. Two containers with the intended JVM UID can read each other's writes.
2. Both new-file creation and writes through an already-open descriptor fail
   under the rule, with a real Java `IOException` and matching FUSE receipt.
3. Previously stored bytes remain readable; disarming restores successful writes.
4. Healthy `fsync`, rename, list/stat and deletion meet the checkpoint filesystem's
   required semantics, including cross-mount visibility.
5. Killing the daemon causes an explicit failure, with no direct-path fallback.
6. Cancellation, deadline and watchdog expiry leave a bounded cleanup path and an
   aborted/expired receipt. Automatic healing never turns an aborted run into PASS.
7. Out-of-namespace paths, symlinks/path traversal and stale instance/revision
   commands are rejected; logs and evidence remain writable during the fault.

**Done when:** these probes pass on the actual target environment. Native Linux
and Docker Desktop qualification are reported independently. Only then wire the
adapter into Flink provisioning.

## PR 4 — Typed storage control and checkpoint-failure orchestration

Add `StorageFaultControl` targets/receipts in `runtime-api`; provisioning,
mount/helper lifecycle and cleanup in `testcontainers`; and the bounded phase
state machine plus REST checkpoint correlation in `core`.

Extend checkpoint observations to retain a specific checkpoint's status and
failure details, and to bind it to the single acknowledged REST trigger. Reuse
the existing checkpoint-trigger/status primitives; never replay an uncertain
POST. A generic failed-checkpoint count is insufficient evidence.

The current failed-trigger response carries a failure cause without a checkpoint
ID. Do not assume otherwise or derive that ID from the trigger ID. Pin and verify
`max-concurrent-checkpoints = 1`; collect checkpoint history and the workload's
snapshot receipts, and require exactly one unambiguous new checkpoint between
the confirmed pre-trigger and post-failure observations. A competing automatic
checkpoint, missing history or contradictory receipt makes the association
unconfirmed. Cover the failure-response-without-ID and multiple-candidate cases
in tests.

Introduce one narrow atomic phase operation, provisionally
`checkpoint_storage_fault`, targeting the job's generated storage and using the
fixed write-EIO / single-TaskManager-replacement sequence below. This prevents
scenario authors accidentally healing before the recovery boundary. Do not add
a generic arbitrary-path disk-manipulation language or an unrestricted
multi-validator framework.

Add an explicit opt-in FUSE provisioning setting and preflight the first-slice
topology. Force and verify checkpoint externalization using the pinned Flink
configuration's small-file threshold, or an equivalently proven materialization
contract. A completed checkpoint alone is not proof that application state was
written to a separate file. Require state-file/materialization evidence and a
matching real failed operation; a synthetic probe failure does not count.

### Atomic experiment sequence

1. Establish healthy FUSE instances and the stateful progress gate. Select the
   latest completed checkpoint **N**, with nonzero progress and enough input left;
   require no known in-flight checkpoint before arming.
2. Arm every required instance and receive acknowledgements for the same revision.
   Retain checkpoint observations around the arm boundary.
3. Trigger a checkpoint once. Confirm a specific checkpoint newer than N fails
   because a matching checkpoint create/write encountered EIO. Preserve both the
   REST failure and filesystem receipt. Timestamp overlap alone is insufficient;
   correlate the serialized checkpoint attempt, job namespace and failure cause.
4. **Keep the fault armed.** Kill the sole TaskManager, confirm physical death
   and absence of active data-processing tasks, and verify that no checkpoint
   newer than N completed. A newer completion makes this attempt a window miss.
5. Disarm surviving instances; a deliberately stopped instance is accounted for
   by its confirmed death. Verify healthy storage, then recreate the TaskManager
   with a freshly verified healthy FUSE instance.
6. Confirm restoration of **N**, processing of further events for the same keys,
   and a subsequent successful checkpoint. Then require natural FINISHED,
   physical writer fencing, complete Kafka traversal and the composite oracle.
7. Retain fault/recovery/subject evidence and clean up through the existing
   ownership and deadline boundaries, including on partial setup or failure.

All steps share a declared bounded operation budget; the lease cannot silently
expire before the intended kill. Automatic checkpoints are accounted for: an
unexpected completion, ambiguous trigger or lost checkpoint history invalidates
the required window, rather than being hidden by a longer sleep/interval.
Use explicit checkpoint-failure tolerance in the new catalog and its controls so
the planned kill remains the recovery trigger; unexpected earlier task failure
is retained and does not qualify this particular window.

Record attempt-scoped monotonic ordering and per-instance receipt sequences;
wall-clock timestamps alone do not prove cross-process causality. Report all
unexpected restarts, component errors and cleanup failures separately.

**Done when:** fake-runtime tests cover the full state machine, late checkpoint
completion, unknown POST outcome, missing EIO, instance replacement, deadline,
daemon death and cleanup. Existing runtime/connector identity checks still
cover every incarnation. Missing fault evidence prevents PASS without erasing
an independently observed data/state FAIL or any `verification.*` failure.

## PR 5 — Live qualification and reproducible evidence

Use the same FUSE configuration, externalization settings, workload parameters,
checkpoint tolerance and artifact set across the appropriate controls:

| Cell | Expected observation |
| --- | --- |
| Correct workload, healthy FUSE, uninterrupted | PASS |
| Correct workload, healthy FUSE, TM kill/restore | PASS with state recovery confirmed |
| Correct workload, checkpoint EIO then TM replacement | PASS with exact fault/checkpoint/recovery sequence confirmed |
| Restore-reset mutant, same checkpoint EIO sequence | State-mismatch FAIL with ID integrity preserved |

Run three attempts per cell: twelve qualification attempts after separate pilots.
Qualification requires three confirmed results per cell; a miss remains in the
record and leaves the matrix incomplete.
Capture every exit code, original result, stdout/stderr, FUSE journal/control
receipts, Flink checkpoint/restore observations, process/class-origin evidence,
input manifest and configuration/artifact identities in fresh directories under
`jobs/`. Preserve misses and failures; extra attempts use new numbered paths and
do not erase the original matrix. Do not rebuild artifacts referenced by a run.

Require all ordinary cells to pass with complete confirmation and every mutant
fault cell to fail specifically on state. Keep catalog expectations at PASS.
Binary checkpoint retention may be unavailable after natural completion; record
that limitation rather than manufacturing recovery evidence from absent files.
Document the qualified platform and exact artifacts; qualification does not
extend automatically to RocksDB, HA, multiple workers or another FUSE mode.

## Validation and delivery boundaries

- Workload changes: focused generator tests; oracle/plan changes: focused core
  tests; cross-module/result-contract changes: `mvn -B -ntp verify` on JDK 21.
- Python probe/tool changes: their corresponding `tools/tests` suite on Python
  3.10+. Normal CI remains Docker-free; privileged probes/live matrices are
  explicit separate runs.
- Before CLI validation/live use, install the same checkout's reactor artifacts
  with `mvn -B -ntp install`; validate all new catalogs with `--show-plan`.
  Avoid root `clean` when evidence exists. Use isolated artifacts/caches for
  frozen comparison batches.
- Documentation changes: validate links and run `git diff --check`.
The first implementation task is PR 0 plus PR 1's workload/oracle vertical.
There is no need to wait for suite-runner work, and no need to build a generic
disk-chaos platform before proving that the new oracle detects lost state.

## References

- [Current executable subset and omissions](../README.md#current-executable-subset)
- [Scenario contract](specs/SPEC-001-scenario-schema.md)
- [Expected-result contract](specs/SPEC-002-expected-result-schema.md)
- [Semantic validation cases](specs/SPEC-005-semantic-validation-test-cases.md)
- [Existing graceful-failover calibration](POOL-REUSE-PRECOMMITTED-FAILOVER.md)
- [Linux FUSE I/O modes](https://docs.kernel.org/filesystems/fuse/fuse-io.html)
- [Docker FUSE capability example](https://docs.docker.com/engine/containers/run/#runtime-privilege-and-linux-capabilities)
- [Flink checkpoint stream materialization](https://github.com/apache/flink/blob/742220a3f4e7a9a92ca27e840b57f15d4c41a6ac/flink-runtime/src/main/java/org/apache/flink/runtime/state/filesystem/FsCheckpointStreamFactory.java)
