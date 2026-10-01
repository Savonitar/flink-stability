# HA and delegation-token fault scenarios

This engine slice adds a ZooKeeper-backed pair of JobManagers and a synthetic
delegation-token provider/receiver installed as a real Flink plugin. It is intended
to expose recovery, acquisition and distribution behavior during leadership changes.
It does not modify Flink or put upstream-only unit tests in the engine.

```yaml
setup:
  flink:
    image: flink:2.2.0
    jobmanagers: 2
    taskmanagers: 2
    high_availability:
      zookeeper_image: zookeeper:3.9.3
      session_timeout: 6s
    token_provider:
      renewal_interval: 2s
      retry_backoff: 2s
```

The workload must use filesystem checkpoint storage. All Flink processes share an
attempt-owned directory containing checkpoints and HA metadata. Leader discovery
reads the published ZooKeeper sessions and addresses, then matches them to unique
container incarnations. A standby HTTP response is insufficient proof of leadership.
The attempt-owned ZooKeeper fixture is anonymous; HA Flink processes explicitly
disable ZooKeeper SASL, and authenticated ZooKeeper is outside this fixture's coverage.
The fixture explicitly permits sessions from two to sixty seconds and retains the
timeout actually negotiated by each Flink incarnation. Missing or mismatched session
evidence prevents confirmed HA evidence. Ordered leadership samples include initial,
routing, fault and pre-fence observations. Identical consecutive routing samples share
one entry with a count and time range. This sampled history cannot rule out elections
between observations. A no-fault control rejects an observed leadership change or gap
and requires coherent initial and pre-fence leaders.

One operation selects the current leader, applies its fault, holds it and heals it:

```yaml
- leader_fault:
    mode: isolate-zookeeper
    duration: 15s
    timeout: 2m
    token_fault:
      mode: delay
      delay: 5s
```

Modes are `kill`, `pause` and `isolate-zookeeper`. Isolation closes the target's
existing ZooKeeper TCP connections and blocks reconnection while its JVM remains
alive. Other Flink/Kafka/token-service paths remain available. Kill recreates the
logical slot, and pause resumes the same process. Every operation retains its
application/healing evidence and errors.
The operation timeout is shared by the initial job observation, fault work,
leadership discovery and the recovered-job observation. Cleanup always
gets separate bounded budgets: five seconds to heal the token service and thirty
seconds to heal the process or network gate. Both are attempted independently;
cleanup cannot turn an expired observation deadline into a confirmed transfer.
An invocation is limited to 100 leader faults, including expanded loops. A pending
TaskManager kill must be healed first; simultaneous RM/TM loss is outside this slice.
No long-run thread/timer leak result follows from these bounded catalogs.

Optional token modes are `delay`, `fail` and `linkage-error`. They are enabled before
disrupting the leader and healed after the hold interval. FAIL returns HTTP 503;
LINKAGE_ERROR makes the fixture throw `LinkageError` from token acquisition. This
does not simulate an arbitrary broken plugin dependency. Initialization and receipt
telemetry remain available while acquisition fails. The service is bound to host
loopback and exposed only through the explicit Testcontainers host tunnel.

The plugin uses Flink's 2.2 provider/receiver SPI, with no shaded Flink classes or real
credentials. Its token lifetime is twice `renewal_interval`; the Flink renewal ratio
is explicitly 0.5. Issuance, receipt, process identity, fault revision, timestamps and
request concurrency are recorded. This supports observing bursts; it does not define
a universal acceptable request rate. Saturation or truncated evidence prevents PASS.
In JSON, `evidence.flinkHa.tokenEvents` stores each complete event value once.
Snapshots refer to that table through ordered `eventRanges` pairs with an inclusive
start and exclusive end, plus `eventCount` and their original counters/flags.
Concatenate those ranges to reconstruct a snapshot. Conflicting observations with
the same sequence number remain separate events; deduplication does not conceal
contradictions or reorder the trace.
Optional `retry_backoff` (1 second–5 minutes) sets a controlled fixed retry interval.
It sets the legacy retry-backoff key and the newer initial/max-backoff keys to the
same value; omitting it preserves the selected Flink version's defaults. The token
catalogs explicitly use two seconds so recovery can be observed within their bounded
workload. They do not test default or exponential retry timing.

Version-specific callback APIs and authenticated token use by Kafka are outside this
fixture's coverage.

The catalogs under `scenarios/ha/` retain the existing exact-ID oracle and use
3,000 records, parallelism four and four Kafka partitions. The single-fault catalogs use a 100 ms source delay. The repeated-recovery pair
uses 300 ms and a five-minute completion timeout to leave time for three independent
transfers. These are new recovery windows; existing catalogs are unchanged.

| Catalog | Fault |
| --- | --- |
| `ha-eos-control` | HA without an injected fault or token fixture |
| `ha-eos-kill` | Kill and replace the active JM |
| `ha-eos-pause` | Pause and resume the active JM |
| `ha-eos-isolate` | Disconnect the living leader from ZooKeeper |
| `ha-repeat-control` | Matched longer workload without injected faults |
| `ha-repeat-isolate` | Three independently proven leadership transfers in one job |
| `ha-token-control` | Healthy plugin acquisition/distribution |
| `ha-token-delay` | Leader isolation with slow acquisition |
| `ha-token-failure` | Leader isolation with acquisition failures |
| `ha-token-linkage` | Leader isolation with acquisition LinkageError |

After building with JDK 21, an example run is:

```bash
mvn -q exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario ha-token-delay --artifact-root . --offline"
```

The runtime uses Flink/Kafka/ZooKeeper containers and explicit host TCP/HTTP services.
The first local ten-case matrix ran on engine `7e6cef3`, Flink 2.2.0, Kafka 4.0.0,
ZooKeeper 3.9.3 and Kafka connector 5.0.0-2.2. Each catalog ran once, with unchanged
PASS expectations, workload, fault settings and completion deadlines.

| Cases | Observed result |
| --- | --- |
| HA control, token control, repeated-recovery control | PASS: exactly 3,000 IDs, no missing, duplicate, malformed or unexpected IDs |
| JM kill, pause, isolation; token delay and HTTP 503; repeated isolation | FAIL: bounded job completion timed out. Declared leadership/recovery evidence was confirmed; delay and HTTP 503 also confirmed acquisition on the new RM and healthy issuance/TM receipt after healing. |
| Token LinkageError | FAIL: bounded job completion timed out; leadership and token recovery evidence was unconfirmed. The new RM reported fatal startup after the injected acquisition error; healthy issuance later occurred on the original RM. |

The repeated-isolation case confirmed all three declared transfers with distinct
sessions. Its logs contained four restores across JobManagers, while the final
REST snapshot counted two on the final coordinator; those are different scopes.
The failed cases did not reach the terminal data oracle. They establish neither
missing records nor exact final data. Their FAIL results remain authoritative.

For the kill, pause, isolation and repeated-isolation cases, all restored source
readers finished their bounded splits but received no new `NoMoreSplits` event;
checkpoints continued without job completion. Inspection of the provisioned release
connector bytecode supports a lost enumerator completion flag after restoration and
empty partition discovery. This remains a connector hypothesis pending a targeted
reproduction and verified correction, not a reason to change the catalogs.

The four token traces had at most one concurrent acquisition and two request starts
in any one-second window, under the explicit two-second settings. These bounded
observations do not establish a general storm threshold or long-run leak behavior.
Component findings also include fenced Kafka commits, metric collisions, ZooKeeper
leader-latch errors and a shutdown-time token delivery failure. A passing data
oracle does not dismiss those observations.

All 81 recorded owned container IDs were confirmed absent. Separate cleanup
follow-ups preserved the original results; no completed catalog was replayed.
The explicit ZooKeeper SASL setting documented above was added after this matrix
to address authentication errors against the anonymous fixture. A separately
authorized control on corrected engine `97c671e` used the same Flink image and
unchanged `ha-eos-control` catalog: exact 3,000 IDs, FINISHED, no restores, and
complete runtime, class-origin and process-fence evidence. Both JMs and both TMs
loaded the explicit setting and established ZooKeeper sessions; the full log
contained none of the previous SASL/JAAS/authentication errors. All eight recorded
owned containers were confirmed absent. This verifies the anonymous-fixture
configuration fix for initial processes; replacement configuration is covered by
unit tests. The original matrix failures and other component findings remain open.

A selected experimental Flink 2.4 build still requires its existing image/runtime-JAR
pins and a matching local workload/connector closure; a release 2.2 run says nothing
about a particular upstream PR.

The result separates runtime image identity, HA effect, process health and final
record-set verification. `evidence.processFence` includes per-process outcomes and
actual state observations before the first terminal kill, after declared kills and
after terminal kills, with available exit/OOM/finished-at metadata. Partial fence
results and failed inspections remain visible after cleanup. Unexpected exits and
OOM kills prevent a clean PASS; an existing data or completion FAIL keeps its
original reason. An unconfirmed election alone does not invalidate a proven image
identity. These corrective evidence checks do not add exit metadata retroactively
to the earlier matrix; a new runtime execution is required to verify them.

PASS requires exact final data plus complete experiment evidence. For leader faults,
that includes changed sessions on another physical JM, a confirmed effective fault,
healing, and recovery of the same submitted job. A requested token fault additionally
requires a request from the new RM incarnation that actually experienced the fault,
then healthy issuance/receipt. For HTTP failures, the plugin acknowledges receiving
the matching synthetic response; a server-side response attempt alone is insufficient.
Repeated operations need independent sessions and restore observations in execution
order, with append-only token traces. Missing proof stays inconclusive. Component errors and
unexpected recovery amplification remain findings even when the exact-ID oracle passes.

When `proof_scope` is omitted, the final healthy-token gate requires one exact
healthy token on every latest provisioned TaskManager incarnation, with the declared
participant count and verified initialization/plugin origin. Receipts on older
incarnations or receipts for different tokens cannot fill missing participants.
The opt-in recovery barriers additionally require fresh delivery within their
specific fault/recovery boundaries.

Separately named experiments can opt into a stronger, explicit token expectation:

```yaml
token_provider:
  renewal_interval: 2s
  retry_backoff: 2s
  proof_scope: submitted-job  # alternatively: bootstrap
```

Omission applies the unscoped all-TM gate without job-registration binding. Neither scope is inferred from the
Flink version, and `legacy` is not a schema value. Both explicit scopes require a
fresh completed request after the post-submit trace watermark and exact-token
receipt by every expected live TM incarnation. The exact post-submit snapshot is
retained, and later traces must preserve that observed prefix. `bootstrap` requires BOOTSTRAP
context. `submitted-job` requires JOB context matching the JobID returned by actual
submission and the compiled workload alias, together with the issuing provider's
generation and matching acknowledged REGISTER history. The provider cannot choose
the expected JobID. A BOOTSTRAP token cannot satisfy submitted-job proof.

The common plugin stays compiled against the Flink 2.2 SPI and exposes registration
hooks for runtimes that dispatch them. Its bounded lifecycle journal and immutable
request context are recorded as typed evidence; the receiver has a separate
participant identity. Requests, outcomes, finishes, fault acknowledgements,
issuance and receipts must correlate in the selected scope. The same predicate
governs healthy controls and online/final checkpoint barriers. Conflicting aliases,
unexpected second jobs, overflow, invalid coverage or missing TM receipts remain
negative evidence even if later tokens are healthy. Missing proof is INCONCLUSIVE;
an existing exact-ID or completion FAIL keeps its result.

Explicit healthy controls add no checkpoint or phase. A paired experiment must run
and pass both healthy controls before starting fault cells. These checks do not
establish callback-API behavior, authentication by Kafka, or absence of stale queued
work inside one provider process. A release runtime that never dispatches job
registration hooks cannot satisfy submitted-job proof merely by issuing tokens.

The new `ha-token-repeat-control`, `ha-token-repeat-delay`,
`ha-token-repeat-failure` and `ha-token-repeat-linkage` catalogs keep the released
5.0.0-2.2 connector and PASS expectations. They use the explicit 300 ms workload and
five-minute completion window. The fault cells request three 15-second leadership
isolations with `recovery_barrier: token-checkpoint`; delay mode keeps the existing
five-second acquisition delay. The original ten catalogs are unchanged. The healthy
control has no injected fault; a comparison batch separately requires its healthy
issuance to reach both provisioned TMs.

Each opt-in fault first waits for a newly issued healthy token and receipt on every
expected live TM. After healing, another new request from the observed current RM
process must deliver the same token to all TMs, followed by one explicitly triggered
checkpoint completing under its exact trigger ID. The next fault cannot overtake
that barrier. All waits share the declared two-minute fault budget. An ambiguous
checkpoint submission is retained and never retried; an unconfirmed barrier stops
later phase actions and leaves the terminal fence/oracle intact. Partial evidence
and data failures remain visible. Sampled leadership must remain unchanged from the
start of pre-fault token readiness through actual fault selection, and from the
healed leader's token-readiness sample through the post-checkpoint sample. An observed
change or gap within either interval rejects the barrier, even if the leader returns.
Adjacent barriers enforce ordering: the next readiness wait starts after the previous
checkpoint barrier completes. Final evaluation also requires unchanged retained
leadership samples from that completed checkpoint through the next readiness sample.
Fault scenarios do not impose a global no-election
rule outside those guarded intervals. Additional observed transitions remain evidence
and findings; neither exactly N elections/restores nor continuous stability between
samples is established. Token
telemetry identifies a process/provider instance, not an internal RM session, so
this does not prove the absence of stale queued work within that process.

These new catalogs have not yet established a released-connector runtime outcome.
The retained original F9 bounded-completion failures remain authoritative. Local
investigation may generate separately labeled copies selecting a hash-pinned
experimental connector repair; that does not change these defaults or establish a
fix in the released connector. No checkpoint-drain completion workaround is used.
