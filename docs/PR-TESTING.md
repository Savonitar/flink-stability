# Testing a Kafka connector pull request

When a change to `apache/flink-connector-kafka` looks risky, build it and run it as the
subject connector. `tools/pr_gate.py` runs each chosen scenario twice:
- the baseline runs with the released connector 5.0.0-2.2;
- the candidate runs with the build under test.

## What the scenarios cover

The default scenarios exercise the exactly-once sink within the first runner's limits:
one Kafka 4.0 broker, one Flink 2.2 JobManager and TaskManager, parallelism 1, and
bounded input.

- `bounded-eos`: a TaskManager is killed mid-stream, and the job recovers from a
  checkpoint.
- `commit-request-lost`: a proxy drops the first transaction-commit request.
- `commit-response-lost`: the broker commits, and the proxy drops its successful response.

They suit a change to the commit, recovery, or transactional-producer code, such as
`KafkaCommitter`, `KafkaWriter`, `FlinkKafkaInternalProducer`, or transaction naming. They
say nothing about sources, partitioning, parallelism above 1, or broker failover. The
build under test must run on Flink 2.2 with Kafka 4.0.

The [connector-mutant calibration](../calibration/connector-mutants/README.md) shows that
these scenarios catch two wrong commit decisions. A passing candidate shows only that
these faults did not break exactly-once in these runs.

## Procedure

Work from the repository root with JDK 21 and Docker, after `mvn clean install`. Keep
the build under `jobs/pr/<number>/`, which git ignores, because the harness reads local
artifacts only from inside its artifact root.

1. Fetch the pull request. This downloads the connector repository from GitHub.

   ```bash
   N=123
   git clone https://github.com/apache/flink-connector-kafka.git jobs/pr/$N/src
   git -C jobs/pr/$N/src fetch origin pull/$N/head:pr-$N
   git -C jobs/pr/$N/src checkout pr-$N
   ```

2. Build the connector module and collect its runtime dependencies. Use the JDK that the
   connector's own build requires.

   ```bash
   mvn -B -f jobs/pr/$N/src/pom.xml -pl flink-connector-kafka -am package -DskipTests
   mvn -B -f jobs/pr/$N/src/pom.xml -pl flink-connector-kafka dependency:copy-dependencies \
     -DincludeScope=runtime -DoutputDirectory=$PWD/jobs/pr/$N/runtime
   ```

   Copy the module's main JAR, not its `-tests` or `-sources` JARs, from
   `jobs/pr/$N/src/flink-connector-kafka/target/` to `jobs/pr/$N/`.

3. Run the gate. Each run starts fresh containers and takes about a minute.

   ```bash
   python3 tools/pr_gate.py \
     --connector-jar jobs/pr/$N/flink-connector-kafka-<version>.jar \
     --runtime-dir jobs/pr/$N/runtime \
     --output jobs/pr/$N/gate-$(date +%Y%m%d-%H%M) --runs 3
   ```

   Add `--scenario` to choose scenarios; it may repeat.

## Reading the result

`summary.md` in the output directory lists every run. The raw results are next to it:
`stdout.json`, `stderr.log`, and the exit code. First check two things:

1. Every row's "Subject JAR" column says `ok`. The hash comes from the Flink processes'
   class-load evidence: candidate runs must have loaded the build under test, and baseline
   runs the release.
2. The baseline passes. If it does not, the environment is unhealthy and the comparison
   is void.

Then compare the verdicts:

- **Candidate fails, baseline passes.** A `fail` with `missing-ids` or `duplicate-ids` is a
  lead. Re-run it, and read that run's evidence before you report it.
- **`inconclusive`.** The experiment was invalid, for example because a fault missed its
  window. It says nothing about the connector.

The candidate's classpath is the connector JAR plus exactly the JARs in `--runtime-dir`.
A pull request that changes dependencies is therefore tested with its own dependencies.

## Reporting

A summary for the pull request names:
- the scenarios and the number of runs;
- the verdicts on both sides;
- the candidate's SHA-256.

Posting on the pull request is public. The maintainer decides whether to post and what.
