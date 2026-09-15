# Flink Stability Testing Framework

An experimental chaos-testing harness for [Apache Flink](https://flink.apache.org/).
It starts a real Flink cluster and Apache Kafka broker in Docker, executes a typed
scenario, prevents Flink from writing any more output, and then validates the final
Kafka state.

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
- the bundled workload JAR is thin and declares `Flink-Stability-Workload-Protocol: v1`;
- generated input is acknowledged and reconciled before its exclusive stopping offsets are used;
- terminal Kafka validation never runs unless the Flink process fence succeeds;
- timeouts, partial evidence, cleanup failures, and validation failures have stable reason codes;
- operational logs use stderr and the command result is emitted as one JSON document on stdout.

## Requirements

- JDK 21
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

The workload artifact is written directly to:

```text
flink-job-generator/target/flink-job-generator.jar
```

It contains the workload classes and protocol marker but does not shade Flink, Kafka,
or the connector under test.

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

The result status is `pass`, `fail`, or `inconclusive`. Only `pass` exits `0`;
execution, validation, or infrastructure failures exit `1`, and usage errors exit `2`.

The executable reference pair is:

- [`scenarios/bounded-eos.yaml`](scenarios/bounded-eos.yaml)
- [`scenarios/bounded-eos.expected.yaml`](scenarios/bounded-eos.expected.yaml)

## Current executable subset

The first runner accepts exactly:

- one plain scenario, one run, and no health retry;
- one Apache Kafka 4.0 broker with the input and sink topics;
- one Flink 2.2 JobManager and one TaskManager;
- one auto-started protocol-v1 job with parallelism `1`;
- one verified connector closure;
- bounded generated integer input, capped at 1,000,000 records for the in-memory runner;
- the currently registered wait/await, loop, and named TaskManager kill/restart phase operations;
- one terminal `kafka.id-set` validator and an expected outcome of `pass`.

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
| `testcontainers` | Kafka/Flink process lifecycle, connector installation, fencing, and cleanup |
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
- proxies and network faults;
- savepoint/restore and upgrade execution;
- broader state-backend and topology support;
- health retries, OCI digest capture, and the complete replay-grade report;
- automated real-Docker failure injection and the 1,000,000-record load boundary.

## Status

Early and experimental. The bounded happy path is implemented and has passed against real
Flink 2.2 and Kafka 4.0 containers, including a TaskManager kill/restart and exact
post-fence validation. The framework is not yet a general-purpose or production-ready
stability-testing system.

## License

Licensed under the [Apache License 2.0](LICENSE).

This is an independent personal project. It is not affiliated with or endorsed by the
Apache Software Foundation; Apache Flink and Apache Kafka are trademarks of the ASF.
