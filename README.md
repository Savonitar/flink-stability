# Flink Stability Testing Framework

A chaos-testing harness for [Apache Flink](https://flink.apache.org/). It spins up a real
Flink cluster and Kafka broker in Docker, subjects a running job to failures — killed
TaskManagers, restarts, savepoint-based version upgrades, rescaling — and then verifies
that the output topic still contains exactly the records it should: no losses, no
duplicates.

Failure scenarios are declared in YAML, so reproducing a suspected exactly-once bug is a
matter of writing a file rather than writing code.

## How it works

The runner drives three things over the course of a scenario:

- **Testcontainers** brings up a `confluentinc/cp-kafka` broker and one or more Flink
  containers on a shared Docker network. The Flink image is chosen per step, which is what
  makes cross-version upgrade testing possible.
- **The Flink REST API** (`FlinkRestClient`) triggers savepoints and tracks job state.
  Checkpoints are bind-mounted to `./checkpoints` on the host so a job can be restored into
  a container that did not write them.
- **A Kafka consumer** replays the output topic at the end and asserts record count and ID
  uniqueness.

The bundled test job (`flink-job-generator`) reads `input-topic`, optionally sleeps per
record to keep the pipeline in flight, and writes to `flink-output` with
`DeliveryGuarantee.EXACTLY_ONCE`.

## Requirements

- **JDK 21**
- **Maven 3.8+**
- **Docker**, running and reachable by Testcontainers, for `run` only

Docker-free `validate` needs only JDK and Maven. Executed scenarios pull Flink and
Kafka images on first run, so expect the initial execution to take a few minutes.

## Getting started

Build all modules, including the shaded job JAR that scenarios submit:

```bash
mvn clean install
```

Run the bundled example — a 1.19 job that takes a savepoint, restarts, survives ten
TaskManager kills, and is then validated for exactly-once delivery:

```bash
mvn exec:java -pl cli -Dexec.args="run --scenario scenarios/example.yaml"
```

The `run` command above still uses the original executable format. The v1
scenario/suite contract can already be checked end to end without Docker:

```bash
mvn exec:java -pl cli \
  -Dexec.args="validate --catalog-root path/to/catalog --scenario scenario-name \
  --artifact-root ."
```

Select a suite with `--suite suite-name`. Repeat `-p NAME=VALUE` for submit-time
scalar overrides, and add `--offline` to restrict Maven connector resolution to
the local cache. Validation recursively loads the complete catalog, resolves
parameters and expected results, checks semantic references and capabilities,
then stages and checksums every local/Maven artifact. It does not start Docker.
Exit status is `0` for a valid target, `1` for validation failure, and `2` for
invalid command syntax or an unknown scenario/suite name.

## Writing a scenario

A scenario is a list of named phases, each a list of steps, executed in order. A phase with
`repeat: N` runs its steps N times — useful for hammering a job with repeated failures.

```yaml
scenario:
  phases:
    - name: startup
      steps:
        - type: start
          component: kafka
        - type: wait
          wait_ms: 10000
        - type: start
          component: flink
          image: flink:1.19
          jar: ./flink-job-generator/target/flink-job-generator-0.1.0-SNAPSHOT.jar
          args:
            - --bootstrapServers kafka:9095
            - --processingDelayMs 250
          parallelism: 2
          checkpoint_interval: 1000

    - name: recovery_loop
      repeat: 10
      steps:
        - type: kill
          component: taskmanager
        - type: wait
          wait_ms: 10000
        - type: start
          component: taskmanager

    - name: validation
      steps:
        - type: validate
          validations:
            - type: kafka-count
              topic: flink-output
              expected_records: 1000
            - type: kafka-unique-ids
              topic: flink-output
              expected_records: 1000
```

### Step types

| `type` | Purpose | Relevant fields |
| --- | --- | --- |
| `start` | Start a component, or submit a job to Flink | `component`, `image`, `jar`, `args`, `parallelism`, `checkpoint_interval`, `restore_from_savepoint` |
| `stop` | Stop a component gracefully | `component` |
| `kill` | Kill a component abruptly | `component` |
| `savepoint` | Trigger a savepoint and record its path | `job` |
| `wait` | Sleep before the next step | `wait_ms` |
| `validate` | Run assertions against Kafka | `validations` |

`component` is one of `kafka`, `flink`, or `taskmanager`. Setting
`restore_from_savepoint: true` on a `start` step resumes from the most recent savepoint,
which is how an upgrade across two `image` values is expressed.

### Validation types

| `type` | Asserts |
| --- | --- |
| `kafka-count` | The topic holds exactly `expected_records` records |
| `kafka-unique-ids` | Those records carry `expected_records` distinct IDs — i.e. no duplicates |

## Modules

| Module | Contents |
| --- | --- |
| `cli` | picocli entry points for the legacy runner and Docker-free v1 validation |
| `core` | v1 schema/catalog/planning/preflight plus the legacy runner and Flink REST client |
| `testcontainers` | Flink and Kafka container lifecycle management |
| `flink-job-generator` | The exactly-once Kafka job used as the test subject |

## Status

Early and experimental. The v1 contracts, catalog loader, parameter resolver,
semantic preflight, suite planner, and artifact preparation have automated coverage.
The Docker runner still executes the legacy format; wiring the first narrow v1
execution vertical is the next implementation stage.

## License

Licensed under the [Apache License 2.0](LICENSE).

This is an independent personal project. It is not affiliated with or endorsed by the
Apache Software Foundation; Apache Flink and Apache Kafka are trademarks of the ASF.
