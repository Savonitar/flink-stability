# Validation and evidence

Run from the repository root with JDK 21 and the Maven dependencies already cached:

```sh
mvn -B -o clean install
```

`clean` removes runtime logs and checkpoints as well as build output. Preserve any
evidence needed for comparison before cleaning. The command executes unit and
adapter tests; it does not start Docker. The filter JAR is built at `process-classes`
so downstream resource embedding also works in a clean reactor `test` lifecycle.

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

These tests use controlled clocks, fake runtime boundaries, and Kafka adapter test
clients where applicable. They do not establish container discovery/reconnection
behavior or sensitivity to a faulty connector.

## Optional real-container runs

These commands start Docker workloads. Run them only when container execution is
authorized, after building the same checkout:

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
