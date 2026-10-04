# Validation and evidence

Run from the repository root with JDK 21 and the Maven dependencies already cached:

```sh
mvn -B -o clean install
```

`clean` removes runtime logs and checkpoints as well as build output. Preserve any
evidence needed for comparison before cleaning. The command executes unit and
adapter tests; it does not start Docker. The filter JAR is built at `process-classes`
so downstream resource embedding also works in a clean reactor `test` lifecycle.

The parent pins the lifecycle plugin versions. Compiler 3.13.0 is used throughout
the reactor; other new pins preserve the previously observed Maven defaults
(child clean 2.5, resources 2.6, jar 2.4 and install 2.4). Explicit module jar 3.4.1
overrides and the root-only clean 3.3.2 configuration remain in effect. This fixes
compiler selection without combining it with unrelated lifecycle upgrades.

Incremental compilation was checked with two isolated minimal projects: first compile
an empty interface and an unchanged test class implementing it, then add an abstract
method to the interface and repeat `test-compile` without `clean`. Compiler 3.1
accepted the stale test class; 3.13.0 recompiled it and reported the missing method.
The test source bytes and timestamp were unchanged. This verifies that specific
regression; it is not a guarantee that every incremental-build case is detected.
Full clean builds remain the release check.

## Regression coverage

| Contract | Tests | Failure the test must detect |
| --- | --- | --- |
| Expected-failure validity | `ScenarioVerdictTest`, `V1ScenarioExecutorTest` | A pinned data failure becomes green despite missing subject, fault, phase, fence, or complete oracle evidence. |
| Kill timing | `TaskManagerKillEffectTest`, `ExecutablePhaseExecutorTest`, `FlinkRestApiClientTest` | Recovery predating injection or its matching failure is attributed to the kill; a failure alone is called a restart. |
| Subject entry classes | `ExecutableScenarioPlanCompilerTest` | A multi-release artifact supplies an unchecked versioned entry class. |
| Subject process inventory | `SubjectClassOriginsTest`, `FlinkContainerTest`, `ClusterManagerTest` | A missing predecessor log disappears, split-incarnation evidence passes, or startup retries reuse log paths. |
| Counted fault deadlines | `ProxyFaultInjectorTest`, `FaultRuleBookTest`, `V1ScenarioExecutorTest` | A late drop qualifies, polling latency changes proxy completion time, or a missed fault gives a green verdict. |
| Successful response loss | `EndTxnFaultFilterTest`, `ProxyFaultInjectorTest` | A broker error consumes the occurrence or proves a committed transaction. |
| Client retry observation | `EndTxnFaultFilterTest`, `FaultRuleBookTest`, `V1ScenarioExecutorTest`, `RunScenarioCommandTest` | A different EndTxn identity is reported as a retry, post-fence collection or JSON reporting loses a witness, or unavailable retry evidence changes the verdict. |
| Proxy isolation and healing | `EndTxnFaultFilterTest`, `FaultRuleBookTest`, `ProxyFaultInjectorTest` | Connections steal correlation IDs, concurrent claims exceed the count, or a missing heal acknowledgement succeeds. |
| Schema and capabilities | `SpecificationLoaderTest`, `ExecutableScenarioPlanCompilerTest` | Invalid timing fields or unsupported routes/topologies reach execution. |
| Flink runtime image identity | Runtime target, container provisioning, execution, verdict and renderer tests | A different image behind the same tag starts, a replacement loses its identity, or missing/mixed image evidence yields a passing verdict. |
| Unset ZooKeeper leader records | `FlinkHaRuntimeTest` | Null or empty leader data aborts election observation, creates false leadership evidence, or extends its existing deadline; nonempty malformed records must still fail. |

These tests use controlled clocks, fake runtime boundaries, and Kafka adapter test
clients where applicable. They do not establish container discovery/reconnection
behavior or sensitivity to a faulty connector.

## Offline Kafka segment inspection

Inspect a stable local copy of a Kafka partition log segment without starting a
broker or Docker:

```sh
mvn -q -o exec:java -pl cli -Dexec.args="inspect-kafka-log --input jobs/evidence/output-0/00000000000000000000.log"
```

The `kafka-log-segment-v1` JSON binds decoded physical records to the captured
file's SHA-256. It retains batch producer IDs, epochs, sequences, record offsets,
raw key/value bytes as Base64, canonical numeric IDs when present, and COMMIT/ABORT
markers. Producer IDs and epochs alone do not establish a transaction's outcome
or map it to a transactional ID; absent markers cannot establish a commit.

The current decoder accepts only version 2 uncompressed batches, up to 64 MiB
and 100,000 records, with at most 100,000 batches and 1 MiB per batch. The latter
limits bound empty-batch metadata and transient record/header allocation. Reduce
file bytes and record limits with `--max-bytes` and `--max-records`.
It rejects symbolic links, changed input metadata, corruption, truncated tails,
unsupported batches/markers and exceeded limits. A nonzero exit and
`complete: false` preserve that limitation. `complete: true` describes only this
file, including an empty file; it is not evidence of complete partition coverage,
read-committed visibility or data correctness. Retain the original file beside
the JSON. The command does not collect live broker files or change runtime results.

Decoder regressions use Kafka's actual record builders for transactional data and
markers. These local tests do not establish broker-copy consistency or historical
transaction attribution. Live collection must happen before the attempt removes
its owned Kafka container and must retain independent coverage and identity evidence.

The adapter also has an internal, opt-in archive capability for an attempt's retained
Kafka owner. It requires an observed owner identity, an explicitly supplied log root
and the caller's shared deadline; collection is opt-in through `run --kafka-log-output DIR`.
The runner binds declared source/sink partitions to its owned broker and the registered
Apache Kafka `/tmp/kafka-logs` layout; it never searches alternative roots.
Only completed transport carries a hash, and even that does not establish valid tar,
an atomic broker snapshot or transaction visibility. Copying never holds a lock
needed by container cleanup. A canceled worker can leave a changing `.part` file,
which remains abandoned and cannot become complete evidence after the caller returns.
The raw archive ceiling is 128 MiB; a collecting driver must additionally cap the
whole experiment and validate the archive offline before drawing conclusions.

The opt-in adapter can also inventory an exact partition and copy only an observed
`[0-9]{20}.log` member. It uses fixed, shell-free `/usr/bin/find` and `/bin/stat`
commands supported by the pinned Kafka image: NUL-delimited names are validated
before metadata requests. Inventories retain bounded command output, exit status,
owner identity and file size/mtime/inode. Links, foreign/duplicate names and malformed
metadata reject; rollover suffixes mark unstable coverage. Each inventory is limited
to 64 KiB, 64 entries and 32 logs; one log payload is at most 16 MiB, with 64 KiB extra allowance
for archive framing. Names, root/file stats and two identity checks cost at most 68
logical operations per inventory (find/stat each reserve three Docker requests). The collecting driver must share its overall byte/call/deadline
budgets across inventories and copies and stop on unfinished work.

A file copy remains raw transport evidence until a separate strict single-file TAR
reader verifies its one expected regular member and framing. Metadata comparison helpers can expose changes; the runner does not claim an atomic
snapshot or complete partition coverage. `run --kafka-log-output DIR` invokes inventory, selective copy, strict USTAR reading
and `KafkaLogSegmentDecoder` after the data oracle and before cleanup. It shares a
60s deadline, 512 Docker requests and 64 MiB raw-byte budget across partitions, plus
64 MiB decoded JSON and 100,000 record/batch limits. DIR must be new with an existing
parent. Receipts and partial diagnostics appear under `evidence.kafkaLogs`; decoded
physical facts remain separate hashed JSON files. Collection never changes verdicts.
Fake-driver coverage is in `SelectiveKafkaLogCaptureTest`, strict archive cases in
`KafkaLogArchiveReaderTest`, and caller/cleanup/CLI cases in `V1ScenarioExecutorTest`
and `RunScenarioCommandTest`. The public Flink 2.2.0 controls described in
[CUSTOM-RUNTIME-SUBJECTS.md](CUSTOM-RUNTIME-SUBJECTS.md#verification-scope) retained
and decoded broker archives successfully. That covers those image combinations;
another broker layout still needs its own capture evidence.

## Optional real-container runs

These commands start Docker workloads. Build the same checkout first and retain
each command's result and logs in a fresh output directory:

```sh
mvn -q -o exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario bounded-eos --artifact-root . --offline"
mvn -q -o exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario selftest-duplicates --artifact-root . --offline"
mvn -q -o exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario commit-request-lost --artifact-root . --offline"
mvn -q -o exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario commit-response-lost --artifact-root . --offline"
mvn -q -o exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario network-no-match --artifact-root . --offline"
```

The first four should return a passing verdict with confirmed subject/fault evidence.
`selftest-duplicates` should retain a failing attempt with duplicate IDs. Its ten-second
checkpoint interval widens a timing window; it is not a deterministic event trigger.

The no-match control normally returns exit 1, verdict `inconclusive`, reason
`network-fault.trigger-missed`, and a complete matching terminal oracle. If an actual
abort occurs, inspect the proxy events: that run did not exercise the no-match case.
The expected-result schema does not encode expected inconclusive outcomes, so this
control intentionally has an expected-pass sibling that must remain unmatched.

For each reported run, retain the exact command, commit and dirty-tree state, JDK/Maven
and Docker versions, image references, prepared artifact digests, JSON result, and
the referenced attempt directory with class-load and proxy event files. Record
observations separately from inferences. A suppressed successful response proves
response loss; it does not by itself prove which retry the client sent later.

For each dropped EndTxn message, the proxy can record a matching request observed
after the fault heals, using the transactional ID, producer ID, producer epoch,
and commit/abort flag. The runner collects these optional witnesses after the
process fence and reports them under `evidence.networkFaults`, including
`retryObservedAtMillis`, `retryAfterMillis`, and `retryClientId`. A witness proves
only that the proxy observed a repeated request identity; it does not prove
forwarding, broker acceptance, or application replay, and reused identity fields
do not uniquely identify a transaction. Missing retry evidence does not change
the verdict. An unreadable event file adds the diagnostic
`network-fault.retry-evidence-unavailable` without changing the verdict
([SPEC-004 K6.12](specs/SPEC-004-kroxylicious-fault-model.md)).

The connector-mutant matrix is in
[`calibration/connector-mutants/`](../calibration/connector-mutants/README.md). It runs a
healthy control and two wrong-decision variants of the released connector against both
EndTxn scenarios, and its README records the last results. Historical run counts and CLI
byte-comparison claims without their commands and retained outputs are not substitutes
for rerunning these checks on the current changes.

The [recovery-mutant recipe](../calibration/recovery-mutant/README.md) adds a separate
source checkpoint-offset mutation for `bounded-eos`. Its four-cell matrix keeps the
canonical recovery scenario intact and compares it with an explicitly separate no-kill
control. The mutant must lose exactly one ID after recovery and remain harmless without
recovery; the release must pass both sides. The recipe records the completed four-cell
calibration and its exact artifact/evidence boundary. New builds require their own
identity checks. The mutation tests source checkpoint restoration,
not the sink's treatment of pending transactions. The JSON terminal evidence exposes the
oracle's bounded `missingSamples` so a missing ID can be matched to the corrupted offset.

## Findings and sensitivity

The harness exists to find defects in Flink, Kafka, and connectors. A failing or odd run
is therefore a finding until someone explains it.

- The following stay open findings:
  - a violated data guarantee;
  - an error from a component under test, such as an HTTP 500 from Flink's REST API;
  - an `inconclusive` result whose cause is not understood.

  An `infrastructure` or `inconclusive` reason says where a check stopped, not who is at
  fault.
- Before changing anything, keep the original scenario, versions, parameters, and run
  outputs. A later passing run does not cancel an earlier observation.
- Do not change an expectation, fault placement, timing, checkpointing, or load only to
  make a run pass. Such scenario changes are regression-contract changes
  ([SPEC-002 E5.6](specs/SPEC-002-expected-result-schema.md)). After one, re-run the
  applicable calibration matrix and preserve its expected outcomes. Designated mutant
  cells must retain their pinned data failures with complete phase, fence, and oracle
  evidence, confirmed subject origins, and confirmed fault effects. Healthy controls
  and harmless mutant cells must still pass. In the connector-mutant calibration,
  `assume committed` fails on request loss and `rewrite` fails on response loss; each
  passes the other fault side. Keep the new run evidence with the scenario change.
- A harness fix must explain the original failure. A run that turned green after the
  change does not explain it.
- Record an evidence-backed disposition for each finding:
  - **Fixed:** a harness or component defect is demonstrated, and its fix is verified
    against the original reproducer.
  - **Upstream-tracked:** a component defect is reported with a linked ticket, retained
    run evidence, and a preserved reproducer. Reporting is not a fix; keep it tracked
    until a fix is verified or a documented contract decision explains the behavior.
  - **Explained infrastructure failure:** retained evidence identifies the concrete
    infrastructure cause. Record whether it is resolved or needs follow-up; an
    `infrastructure` label alone does not establish the cause.
  - **Outside the documented guarantee:** a cited precondition is shown to be violated
    by the original scenario. Keep the scenario and evidence with that explanation.
