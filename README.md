<p align="center">
  <img src="docs/assets/flink-stability-logo.png" width="360" alt="Project mascot: a happy squirrel beside an acorn resting on a spirit level with a centered bubble">
</p>
<p align="center">
  <strong>Independent hobby project</strong><br>
  Not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
</p>

# Flink Stability Testing Framework

Apache, Apache Flink, Flink, and the Apache Flink logo are trademarks of
the [Apache Software Foundation](https://www.apache.org/). The squirrel artwork above
is this project's mascot, not the official Apache Flink logo.

An experimental fault-injection and correctness-testing harness for [Apache Flink](https://flink.apache.org/)
and the systems around it, starting with Kafka. It supports on-demand testing of
changes and exploratory fault experiments to find violations of data guarantees.
It starts a real Flink cluster and Apache Kafka broker in Docker, executes a typed
scenario, prevents Flink from writing any more output, and then validates the final
Kafka state for missing, duplicate, unexpected, or malformed record IDs.

The current prototype supports bounded Flink 2.2 jobs using Kafka 4.0 and Kafka
connector 5.0.0-2.2, including distributed TaskManager recovery, ZooKeeper-backed
JobManager HA, and synthetic delegation-token acquisition and distribution faults.
Unsupported topologies and scenario features fail before Docker starts.
Experimental Flink 2.4 builds require explicit image/JAR pins and a local or
image-supplied connector; see [runtime build testing](docs/FLINK-RUNTIME-TESTING.md) for the compatibility
and build-evidence requirements. Explicitly declared custom Flink lines, pinned
broker images, image-supplied connectors and rebuilt workloads use the
[custom runtime subject contract](docs/CUSTOM-RUNTIME-SUBJECTS.md); acceptance of
those declarations does not claim live support for an untested build.

## Execution boundary

The supported path is:

```text
v1 YAML
  -> structural and semantic validation
  -> immutable artifact/dependency preparation
  -> Kafka topic creation and reconciled input preload
  -> connector verification and Flink job execution
  -> natural FINISHED
  -> irreversible TaskManager-then-JobManager process fence
  -> fixed read_uncommitted Kafka high-watermark boundary
  -> read_committed traversal of that exact boundary
  -> exact terminal oracle and structured JSON result
```

Important properties of this boundary:

- scenario and expected-result documents are selected by `meta.name` from a complete catalog;
- connector Maven closures and local artifacts are resolved, hashed, and privately staged;
- staged connector classpaths are copied and verified before each Flink process starts;
  image-supplied subjects are verified in place;
- each created Flink container's actual Docker image ID is checked before process start;
  an optional `setup.flink.image_id` pins the intended local build, and every initial or
  replacement process must use the same image throughout the attempt;
- optional `setup.flink.runtime_jar` verifies the distribution JAR's bytes in every
  container and requires matching runtime class-load sources for every incarnation;
- the subject connector's primary artifact must be the only source of the Kafka
  connector classes the workload runs; a second copy in a dependency or the workload
  JAR is rejected before Docker starts;
- the bundled workload JAR is thin and declares `Flink-Stability-Workload-Protocol: v1`;
- generated input is acknowledged and reconciled before its exclusive stopping offsets are used;
- optional Kafka-cluster `transaction_version: 1` or `2` selects and verifies the
  finalized feature level before proxy/input/Flink startup; `broker-default` observes
  that level without changing it. Failed selection or observation prevents PASS and
  retains its error;
- terminal Kafka validation never runs unless the Flink process fence succeeds;
- timeouts, partial evidence, cleanup failures, and validation failures have stable reason codes;
- operational logs and Log4j internal status diagnostics use stderr, including during JVM
  shutdown; the command result remains exactly one JSON document on stdout through process
  exit. Diagnostic warnings do not change the command's exit status or relax result parsing.

The [distributed recovery guide](docs/DISTRIBUTED-RECOVERY.md) and
[HA and token fault guide](docs/HA-TOKEN-TESTING.md) describe their separate catalogs,
evidence gates and observed results. These capabilities retain the same exact-ID
oracle. A passing data check does not dismiss component errors or establish that
every recovery or token path was exercised.

A requested Kafka transaction feature selection or `broker-default` observation
must be confirmed by the broker before the workload starts. A declared version alone does not prove that the broker accepted
the transition or that a transaction behaved correctly.

The Flink REST client retains non-success HTTP response status and full error bodies
in `evidence.flinkRest`, including errors recovered by deadline-bound GET retries.
Retries share the existing deadline; submission/upload POSTs are never replayed.
After a confirmed physical fence, expiry of the HA leader-resolution budget during
bounded completion retains `verification.flink.job-completion-timeout`; other
resolver failures retain `verification.flink.job-terminalization-failed`. An
unconfirmed fence reports `verification.flink.process-fence-failed`. Correct timeout
classification does not repair a stalled job or establish final data completeness.
A successful retry does not close an unexplained component finding.

## Requirements

- JDK 21 (the build fails in the `validate` phase on an older JDK)
- Maven 3.8+
- Docker Desktop or another Docker daemon reachable by Testcontainers for `run`

The adapter uses [Testcontainers 1.21.4](https://github.com/testcontainers/testcontainers-java/releases/tag/1.21.4),
whose upstream release includes Docker Engine 29 compatibility. A regression checks that configured copies finish before
the pre-start verification hook and that a failed hook prevents container start.
This update also changes the default helper images: Ryuk 0.11.0 to 0.12.0 and
sshd 1.2.0 to 1.3.0. The host-port tunnel carries HA ZooKeeper-gate and token-service
traffic, so compatibility checks must include a live HA token control.
Live compatibility still depends on the Docker engine and images used for a run.

If a Docker Engine 29 host rejects container startup with an API-version
negotiation error (for example, a client API version below the daemon's minimum),
set both the Docker client environment and the Java client's Maven property:

```sh
export DOCKER_API_VERSION=1.44
mvn -Dapi.version=1.44 ...
```

Apply both settings to the same build or scenario-run command; setting only one
does not configure every Docker client used by the harness.

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
[connector-mutant calibration](calibration/connector-mutants/README.md) demonstrated
this on its recorded 2026-09-26 engine: two deliberately wrong commit decisions
failed the sensitive EndTxn scenarios, while the controls passed. The current
common filter still requires the recipe's complete six-cell requalification.
The separate
[recovery-mutant calibration](calibration/recovery-mutant/README.md) checks that
`bounded-eos` detects a source checkpoint-offset error after recovery while the
uninterrupted control remains exact. These calibrations do not establish sensitivity
of every distributed or HA scenario.

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

To run these scenarios against a locally built Kafka connector, such as a pull
request under review, see [testing a connector pull request](docs/PR-TESTING.md).
For a compatible custom Flink image, see
[testing a Flink runtime build](docs/FLINK-RUNTIME-TESTING.md). Experimental 2.4 builds
also require a runtime JAR pin and a local connector closure; identity evidence alone
does not establish coverage of a PR's changed behavior.

## Current executable subset

The runner supports:

- one plain scenario, one run, and no health retry;
- one Kafka cluster with one or three brokers and the input/sink topics, using
  Apache Kafka 4.0 or a pinned custom image with an explicit launch contract;
- Flink 2.2, explicitly pinned experimental 2.4, or a declared and pinned custom
  runtime subject, with one JobManager or a
  ZooKeeper-backed HA pair, and 1–16 TaskManagers;
- one protocol-v1 job definition, started automatically, with positive parallelism up to the provisioned
  capacity (two slots per TaskManager), and an `EXACTLY_ONCE` or `AT_LEAST_ONCE` Kafka sink;
- one verified connector closure;
- bounded generated integer input, capped at 1,000,000 records for the in-memory runner;
- the currently registered wait/await, loop, and named TaskManager kill/restart phase operations;
- named broker kill/restart and bounded broker faults selected by broker name,
  partition leader or sink transaction coordinator, including rolling restarts;
- HA current-leader kill, pause and ZooKeeper isolation, with per-fault leadership
  and same-job recovery evidence;
- a synthetic token plugin with delayed, HTTP-failing or LinkageError acquisition,
  optional explicit bootstrap/submitted-job proof, and token/checkpoint recovery barriers;
- an optional Kroxylicious proxy in front of the job sink, with counted request/response
  loss, delay and protocol errors for the supported Kafka APIs
  ([SPEC-004](docs/specs/SPEC-004-kroxylicious-fault-model.md));
- experimental [packet loss, delay/jitter and blackhole faults](docs/PACKET-FAULTS.md)
  between a selected TaskManager and Kafka, with identity, traffic and healing evidence;
- one final [savepoint/restore transition](docs/SAVEPOINT-LIFECYCLE.md), using the
  same workload and connector JARs, optionally rescaling or switching
  INCREMENTING to POOLING; this slice excludes HA, tokens and other faults;
- one terminal `kafka.id-set` validator, with an expected outcome of `pass` or an
  expected `kafka.id-set` failure.

Implementation does not establish live qualification. The broker/protocol
profiles and common filter retain the [calibration gaps](docs/PR-TESTING.md#live-qualification-is-still-pending)
described in the PR guide. Packet faults require their isolated NET_ADMIN probe
and live qualification; savepoint/restore has offline validation but no qualified
live matrix reported in its guide.

A TaskManager kill counts only if Flink shows that it affected the job: a failure on
the targeted TaskManager ResourceID that hosted a RUNNING subtask, followed by a checkpoint restore, or by a
restart when no checkpoint existed yet. Otherwise a passing oracle becomes
`inconclusive` with `taskmanager.kill.effect-unconfirmed`
([SPEC-001 R6.12a](docs/specs/SPEC-001-scenario-schema.md)). The JSON result reports
this under `evidence.taskManagerKills`, retains old/new process identities under
`evidence.taskManagerRestarts`, records `evidence.flinkJob`, and lists the sink's
unresolved Kafka transactions after the fence under `evidence.sinkTransactions`.

The separate [distributed recovery scenarios](docs/DISTRIBUTED-RECOVERY.md) use two
TaskManagers, parallelism four, four Kafka partitions and a named kill/restart.
They retain the same exact-ID oracle; existing canonical scenarios keep their parameters.

Token proof is explicit: `setup.flink.token_provider.proof_scope` accepts `bootstrap`
or `submitted-job`; omission preserves the existing service-wide evidence check.
Both explicit scopes require a fresh completed request after submission and matching token
receipts from every expected live TaskManager incarnation. Submitted-job proof also
binds the actual returned JobID, workload alias and acknowledged provider registration
history. A runtime that does not dispatch the registration hooks cannot satisfy it
merely by issuing tokens. Missing proof prevents PASS while a recorded data or
completion failure remains FAIL. See the [token guide](docs/HA-TOKEN-TESTING.md) for
the complete contract and healthy-control prerequisites. This fixture does not
test callback-triggered acquisition, Kafka authentication or isolation between jobs.

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
| `synthetic-token-plugin` | a thin Flink provider/receiver fixture for synthetic token acquisition, registration and delivery evidence |
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
- native execution of schema-defined suites and baseline/candidate experiments
  (`tools/pr_gate.py` separately repeats selected scenarios with two connector artifacts);
- concurrent or independently defined multiple jobs;
- packet faults outside the implemented TaskManager-to-Kafka slice and proxies
  for clients beyond the routed Kafka sink;
- connector or Flink upgrades across a savepoint; the implemented lifecycle
  transition keeps both workload and connector artifacts unchanged;
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
