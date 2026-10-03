# Savepoint and restore: first executable slice

A lifecycle run uses one prepared input manifest, one output topic, one connector
classpath and one transaction prefix across two Flink JobIDs. The new atomic
`savepoint_restore` phase step stops the running EOS job with a canonical savepoint,
then submits the same uploaded workload JAR from that exact returned location.
It is the final phase step; this first slice excludes HA, token experiments, other
faults, repeated lifecycle steps, arbitrary state paths and connector JAR changes.
Warmup must observe RUNNING and a completed checkpoint. The stop request uses
`drain: false` so a final watermark cannot prematurely finish bounded input.

The runtime sends one POST to `/jobs/{jobid}/stop` with a caller-generated trigger
ID, polls only `/jobs/{jobid}/savepoints/{triggerid}`, and retains the acknowledged
trigger, returned location and old job's FINISHED observation. An unknown POST
outcome is never retried. The location must remain under the attempt's existing
`file:/flink/checkpoints/` mount. There are no new host mounts. Resubmission supplies
`savepointPath`, `allowNonRestoredState: false` and `claimMode: NO_CLAIM`, retaining
the source stopping offsets, consumer group, transactional prefix, operator UIDs,
max parallelism, checkpoint configuration and program arguments. Only requested
parallelism and the permitted naming-strategy switch change. Every operation shares
one bounded lifecycle deadline; stop or submit uncertainty retains evidence and
prevents a success claim.

Confirmation requires a distinct restored JobID, RUNNING state, the exact external
savepoint path in restored-checkpoint metadata and observed target parallelism.
Final completion and process fencing target the restored job, then the ordinary
exact-ID oracle reads the entire output topic against the original manifest. It
therefore detects losses or duplicates on either side of the savepoint boundary.
All Flink processes are fenced on failure too; the savepoint and raw REST evidence
are retained. A green oracle cannot replace missing lifecycle confirmation.

The initial catalog covers 1→1, 4→2, 1→4 and INCREMENTING→POOLING at parallelism 1,
with topology-matched controls. Two TaskManagers provision enough slots for rescale
cases, and four Kafka partitions keep all source/sink subtasks active. The published
5.0.0-2.2 `TransactionNamingStrategy` source permits switching to POOLING using a
savepoint, requires Kafka 3.0+ and target-topic read access, and forbids the reverse
switch. These scenarios use Kafka 4.0.0 and the same connector JAR throughout.
Tests cover REST requests/receipts, no mutation retries, failed/ambiguous stop,
foreign savepoint paths, lost restoration proof, capacity checks and exact-ID
lifecycle accounting. Offline validation is not live calibration.

**Connector upgrade remains design only.** A later slice must fence all old
connector JVMs after the savepoint, independently verify candidate classpath and
class origins, retain both artifact/dependency hashes, rebuild the Flink runtime
without changing Kafka or the input/output manifests, and restore with stable UIDs
and an explicitly documented serializer compatibility contract. Both pre-upgrade
and post-upgrade jobs contribute to one final oracle. No candidate restore should
start after an unconfirmed fence, and no fallback may silently reuse the baseline
JAR or discard unmatched state.
