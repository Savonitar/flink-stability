# Flink Stability Testing Framework

An experimental fault-injection and correctness-testing harness for [Apache Flink](https://flink.apache.org/)
and the systems around it, starting with Kafka. It supports on-demand testing of
changes and exploratory fault experiments to find violations of data guarantees.
It starts a real Flink cluster and Apache Kafka broker in Docker, executes a typed
scenario, prevents Flink from writing any more output, and then validates the final
Kafka state for missing, duplicate, unexpected, or malformed record IDs.

The current prototype intentionally supports one narrow but real vertical: a bounded
Flink 2.2 exactly-once job using Kafka 4.0 and Kafka connector 5.0.0-2.2. Unsupported
topologies and scenario features fail before Docker starts.

## Execution boundary

The supported path is:

```text
v1 YAML
  -> structural and semantic validation
  -> immutable artifact/dependency preparation
  -> Kafka topic creation and reconciled input preload
  -> connector installation and Flink job execution
  -> natural FINISHED
  -> irreversible TaskManager-then-JobManager process fence
  -> fixed read_uncommitted Kafka high-watermark boundary
  -> read_committed traversal of that exact boundary
  -> exact terminal oracle and structured JSON result
```

Important properties of this boundary:

- scenario and expected-result documents are selected by `meta.name` from a complete catalog;
- connector Maven closures and local artifacts are resolved, hashed, and privately staged;
- the connector classpath is copied and verified before each Flink process starts;
- the subject connector's primary artifact must be the only source of the Kafka
  connector classes the workload runs; a second copy in a dependency or the workload
  JAR is rejected before Docker starts;
- the bundled workload JAR is thin and declares `Flink-Stability-Workload-Protocol: v1`;
- generated input is acknowledged and reconciled before its exclusive stopping offsets are used;
- terminal Kafka validation never runs unless the Flink process fence succeeds;
- timeouts, partial evidence, cleanup failures, and validation failures have stable reason codes;
- operational logs use stderr and the command result is emitted as one JSON document on stdout.

## Requirements

- JDK 21 (the build fails in the `validate` phase on an older JDK)
- Maven 3.8+
- Docker Desktop or another Docker daemon reachable by Testcontainers for `run`

`validate` is Docker-free. It checks the broad v1 document, semantic, and artifact
contract. `run` additionally checks the narrower capabilities implemented by the current
prototype, so a valid broad-v1 scenario may still be rejected as not yet executable. The
first execution may need to pull Flink and Kafka images.

## Build

Build and test every module, including the thin workload JAR:

```bash
mvn clean install
```

See [validation and evidence](docs/VALIDATION.md) for regression coverage,
optional container runs, and the no-match control. Unit tests alone do not show that
the scenarios catch real connector defects. The
[connector-mutant calibration](calibration/connector-mutants/README.md) checks this in
real containers: two deliberately wrong commit decisions must fail the EndTxn scenarios,
and a healthy control must pass them.

The workload artifact is written directly to:

```text
flink-job-generator/target/flink-job-generator.jar
```

It contains the workload classes and protocol marker but does not shade Flink, Kafka,
or the connector under test.

### Continuous integration

GitHub Actions runs `mvn -B verify` on JDK 21 for pushes to `main` and for pull
requests ([`ci.yml`](.github/workflows/ci.yml)). These tests do not start Docker.
The real-container `bounded-eos` run is a separate, manually triggered workflow
([`bounded-eos.yml`](.github/workflows/bounded-eos.yml)) that uploads the JSON result
and the runner log.

### Test fixtures

Complete YAML test documents live in each module's `src/test/resources/spec/valid/`
directory. Tests read fresh copies and edit specific fields for semantic variations;
avoid replacements that depend on YAML indentation or field ordering. Keep short
syntax-focused inputs, such as duplicate keys and malformed YAML, inline and pass
their raw text to the loader. Fixtures are handwritten independently of production
serialization. Deliberate schema changes must update the relevant fixtures and
assertions; moving a fixture to a resource does not make it version-independent.

## Validate a scenario

The Maven `exec:java` commands expect the preceding `mvn clean install` to have
installed the same reactor version. Rebuild after source changes.

```bash
mvn -q exec:java -pl cli \
  -Dexec.args="validate --catalog-root scenarios --scenario bounded-eos \
  --artifact-root ."
```

Select a suite with `--suite NAME`. Repeat `-p NAME=VALUE` for submit-time scalar
overrides. Add `--offline` to restrict Maven artifact resolution to the local cache.

Validation recursively loads the catalog, resolves parameters/defaults and expected
results, checks semantic references and capability rules, resolves dependency closures,
and stages and verifies every artifact. It never starts Docker.

Exit status:

- `0`: valid
- `1`: validation or resolution failure
- `2`: invalid syntax or unknown target

## Run the bounded prototype

```bash
mvn -q exec:java -pl cli \
  -Dexec.args="run --catalog-root scenarios --scenario bounded-eos \
  --artifact-root ."
```

The top-level result status is the scenario verdict: `pass`, `fail`, or
`inconclusive`. Only `pass` exits `0`; execution, validation, or infrastructure
failures exit `1`, and usage errors exit `2`. The attempt's own result appears under
`attempt`. A negative control pins an expected failure, so it passes only when its
attempt fails exactly as pinned with complete phase, fence, and oracle evidence,
confirmed subject origins, and confirmed fault effects. Missing or unconfirmed
evidence makes the verdict `inconclusive` while retaining the attempt's data
failure. A conclusive result with a different outcome is `expectation.mismatch`.

The executable reference pairs are:

- [`scenarios/bounded-eos.yaml`](scenarios/bounded-eos.yaml) with
  [`bounded-eos.expected.yaml`](scenarios/bounded-eos.expected.yaml): an
  exactly-once job survives a mid-stream TaskManager kill with exact output;
- [`scenarios/selftest-duplicates.yaml`](scenarios/selftest-duplicates.yaml) with
  [`selftest-duplicates.expected.yaml`](scenarios/selftest-duplicates.expected.yaml):
  a negative control. An at-least-once sink is expected to duplicate output when
  recovery replays records written after the last checkpoint, so the oracle must
  report `validator.kafka.id-set.duplicate-ids`. The 10 s checkpoint interval and
  2 s wait create a timing-based window; they do not guarantee duplication on every
  machine or run;
- [`scenarios/commit-response-lost.yaml`](scenarios/commit-response-lost.yaml) and
  [`scenarios/commit-request-lost.yaml`](scenarios/commit-request-lost.yaml): a
  Kroxylicious proxy in front of the sink drops the first matching commit request
  or successful response after the fault is armed, following checkpoint warmup.
  The output must still be exact.

## Current executable subset

The first runner supports:

- one plain scenario, one run, and no health retry;
- one Apache Kafka 4.0 broker with the input and sink topics;
- one Flink 2.2 JobManager and one TaskManager;
- one auto-started protocol-v1 job with parallelism `1` and an `EXACTLY_ONCE` or
  `AT_LEAST_ONCE` Kafka sink;
- one verified connector closure;
- bounded generated integer input, capped at 1,000,000 records for the in-memory runner;
- the currently registered wait/await, loop, and named TaskManager kill/restart phase operations;
- an optional Kroxylicious proxy in front of the job sink, with counted `drop-request` and
  `drop-response` faults on transaction commits and aborts (`end-txn`)
  ([SPEC-004 §11](docs/specs/SPEC-004-kroxylicious-fault-model.md));
- one terminal `kafka.id-set` validator, with an expected outcome of `pass` or an
  expected `kafka.id-set` failure.

A TaskManager kill counts only if Flink shows that it affected the job: a failure on
a TaskManager that hosted active subtasks, followed by a checkpoint restore, or by a
restart when no checkpoint existed yet. Otherwise a passing oracle becomes
`inconclusive` with `taskmanager.kill.effect-unconfirmed`
([SPEC-001 R6.12a](docs/specs/SPEC-001-scenario-schema.md)). The JSON result reports
this under `evidence.taskManagerKills` and `evidence.flinkJob`, and lists the sink's
unresolved Kafka transactions after the fence under `evidence.sinkTransactions`.

A network fault counts only if the proxy dropped every requested message before the
step's `trigger_deadline`. Otherwise a passing oracle becomes `inconclusive` with
`network-fault.trigger-missed`. `evidence.networkFaults` lists each dropped message,
and for a dropped response the broker's answer that the client never saw.

The schema and planning layer describe more than this execution subset. Unsupported
features reject explicitly; they are not ignored or approximated.

## Scenario structure

A scenario declares:

- infrastructure under `setup`;
- connector artifacts under `subject`;
- typed jobs under `workload`;
- ordered actions and waits under `phases`;
- post-fence checks under `terminal_validations`.

The workload receives resolved source/sink endpoints, bounded stopping offsets, state,
checkpoint, transaction, and watermark settings through typed Flink configuration.
`program_args` is an ordered job-owned list and cannot override the reserved workload
configuration namespace.

See [SPEC-001](docs/specs/SPEC-001-scenario-schema.md) for the contract and
[SPEC-005](docs/specs/SPEC-005-semantic-validation-test-cases.md) for its semantic
validation cases.

## Modules

| Module | Contents |
| --- | --- |
| `cli` | `run` and Docker-free `validate` entry points plus JSON/diagnostic rendering |
| `core` | schema/catalog planning, artifact preparation, runtime-neutral execution orchestration, and terminal validation |
| `runtime-api` | JDK-only runtime targets, lifecycle contracts, and immutable runtime evidence shared across orchestration and providers |
| `testcontainers` | Kafka/Flink/proxy process lifecycle, connector installation, fencing, and cleanup |
| `kroxylicious-fault-filter` | the Kroxylicious filter plugin that drops the messages a network fault selects |
| `flink-job-generator` | thin Flink 2.2 protocol-v1 workload used by the executable example |

The `core` module keeps its pre-execution stages in explicit package boundaries:

| Package | Responsibility |
| --- | --- |
| `core.spec.document` | raw specification documents, structural validation, and catalog loading |
| `core.spec.resolution` | parameter/default resolution, expectation and suite planning, compatibility checks, and semantic preflight |
| `core.artifact` | artifact/Maven resolution, connector closure locks and bundles, prepared plans, and workload protocol validation |
| `core.execution.plan` | compilation of a prepared scenario into the narrow executable-runner plan |

Document value constructors remain package-private. Code outside `core.spec.document`
creates in-memory documents through `SpecificationLoader`, preserving structural
validation as the package boundary.

## Not implemented yet

- controlled-unbounded cutoff plus drain/stop;
- executable suites and baseline/candidate experiments;
- multi-broker or multi-job execution;
- network faults other than counted EndTxn drops, and proxies for other clients;
- savepoint/restore and upgrade execution;
- broader state-backend and topology support;
- health retries, OCI digest capture, and the complete replay-grade report;
- automatic real-Docker regression coverage on pull requests and pushes, including
  EndTxn faults and the 1,000,000-record load boundary.

## Status

Early and experimental. The bounded path runs against real Flink 2.2 and Kafka 4.0
containers. An earlier version of `bounded-eos` killed its TaskManager only after the job
had finished, so its `pass` did not show recovery. The runner now requires evidence that
each kill disrupted the job and that Flink recovered it, and `bounded-eos` passes with a
checkpoint restore after its mid-stream kill. The framework is not yet a general-purpose
or production-ready stability-testing system.

## License

Licensed under the [Apache License 2.0](LICENSE).

This is an independent personal project. It is not affiliated with or endorsed by the
Apache Software Foundation; Apache Flink and Apache Kafka are trademarks of the ASF.
