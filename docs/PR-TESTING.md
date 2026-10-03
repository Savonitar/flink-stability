# Testing a Kafka connector pull request

For changes to Flink itself, see [testing a Flink runtime build](FLINK-RUNTIME-TESTING.md).
The connector gate below changes connector artifacts only.

When a change to `apache/flink-connector-kafka` looks risky, build it and run it as the
subject connector. `tools/pr_gate.py` runs each chosen scenario on both sides:

- the baseline runs with the released connector 5.0.0-2.2 by default, or an explicit local build;
- the candidate runs with the build under test.

## What the scenarios cover

The three default scenarios exercise bounded recovery and the exactly-once sink with:
one Kafka 4.0 broker, one Flink 2.2 JobManager and TaskManager, parallelism 1, and
bounded input.

- `bounded-eos`: a TaskManager is killed mid-stream, and the job recovers from a
  checkpoint.
- `commit-request-lost`: a proxy drops the first transaction-commit request.
- `commit-response-lost`: the broker commits, and the proxy drops its successful response.

They suit a change to the commit, recovery, or transactional-producer code, such as
`KafkaCommitter`, `KafkaWriter`, `FlinkKafkaInternalProducer`, or transaction naming.
`bounded-eos` also exercises bounded source checkpoint restoration. These defaults
do not cover multiple partitions, parallelism above 1, JobManager failover or broker
failover. The build under test must run on Flink 2.2 with Kafka 4.0.

The [connector-mutant calibration](../calibration/connector-mutants/README.md) shows that
the EndTxn scenarios catch two wrong commit decisions. The
[recovery-mutant calibration](../calibration/recovery-mutant/README.md) separately
checks a source checkpoint-offset error across TaskManager recovery. A passing
candidate shows only that the selected faults did not break the checked guarantees
in those runs.

## Choose coverage for the change

The engine also executes the following separate catalogs. Select their canonical
names with repeated `--scenario` options when comparing connector artifacts:

| Change under test | Scenarios and evidence |
| --- | --- |
| Recovery across workers and partitions | `distributed-eos` and `distributed-eos-no-fault`: two TaskManagers, parallelism four, four Kafka partitions and a named worker kill/restart. See [distributed recovery](DISTRIBUTED-RECOVERY.md). |
| Recovery after a JobManager leadership change | `ha-eos-control`, `ha-eos-kill`, `ha-eos-pause`, `ha-eos-isolate`: a ZooKeeper-backed pair, matched process identities, leader sessions and recovery of the submitted job. See [HA scenarios](HA-TOKEN-TESTING.md). |
| Token acquisition/distribution during HA recovery | `ha-token-control`, `ha-token-delay`, `ha-token-failure`, `ha-token-linkage`; the `ha-token-repeat-*` catalogs add token/checkpoint barriers around repeated transfers. See [token faults and proof](HA-TOKEN-TESTING.md). |

These are executable capabilities, not promises that the released connector passes
every catalog. The HA guide retains observed bounded-completion failures. The
distributed recovery run also observed repeated restores after one worker kill.
Neither a passing exact-ID oracle nor a later green run closes those findings.
The default calibration matrices do not establish sensitivity of these additional
catalogs, and none exercises a multi-broker topology or a state-sensitive oracle.

For a Flink runtime PR, prepare separate image/runtime-JAR pins and compatible
connector/workload artifacts as described in [runtime build testing](FLINK-RUNTIME-TESTING.md).
`pr_gate.py` swaps connector artifacts only; it does not construct a runtime pair,
change token proof scope, or gate fault cells on both healthy controls passing.

Per-job token experiments must explicitly select
`setup.flink.token_provider.proof_scope: submitted-job` in separately named catalogs.
The engine binds the expected JobID to the actual submit response and the alias to
the compiled workload. Proof requires registration history, a fresh completed
request after the retained post-submit trace, and exact-token receipt by every
expected live TaskManager. `bootstrap` is a separate explicit scope; omission keeps
the existing service-wide check and does not infer per-job support from a runtime
version. Run and pass both healthy controls before starting paired fault cells.
A baseline without registration hooks cannot pass submitted-job proof; report that
capability boundary separately from a regression. This fixture does not establish
callback behavior, authenticated token use, multi-job isolation or complete shutdown
journal delivery. Direct API tests do not replace real runtime propagation evidence.

## Procedure

Work from the repository root with Python 3.9 or newer, JDK 21, and Docker, after
`mvn clean install`. Keep the build under `jobs/pr/<number>/`, which git ignores,
because the harness reads local artifacts only from inside its artifact root.

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

3. Run the gate. Each run starts fresh containers; HA and repeated-recovery catalogs
   can take several minutes.

   ```bash
   python3 tools/pr_gate.py \
     --connector-jar jobs/pr/$N/flink-connector-kafka-<version>.jar \
     --runtime-dir jobs/pr/$N/runtime \
     --output jobs/pr/$N/gate-$(date +%Y%m%d-%H%M) --runs 3
   ```

   Add `--scenario` to choose scenarios; repeat the option for different names.
   Repeated names are deduplicated. `--runs` must be positive and specifies the
   number of runs per scenario on each side; its default is 1.

## Select a causal baseline

For a PR comparison, build its parent with the same Flink/dependency versions and
build procedure as the candidate. Comparing only with release 5.0.0-2.2 can confound
the PR with Flink or Jackson version changes. Supply both optional baseline flags:

```bash
python3 tools/pr_gate.py \
  --baseline-connector-jar jobs/pr/321/baseline/connector.jar \
  --baseline-runtime-dir jobs/pr/321/baseline/runtime \
  --connector-jar jobs/pr/321/candidate/connector.jar \
  --runtime-dir jobs/pr/321/candidate/runtime \
  --scenario bounded-eos --output jobs/pr/321/parent-head-gate
```

Each runtime directory must contain the intended nonempty dependency closure inside
the artifact root. Both local sides use the same path validation, primary SHA-256
pin in their generated catalogs, and observed class-origin/hash check. The engine
locks and verifies both closures. The flags must appear together; omission keeps
the canonical released baseline and its automatic Maven closure. Scenario faults,
load, timings and expectations are copied unchanged.

`manifest.json` records `baseline` and `candidate` objects with connector references,
full SHA-256 values, dependency mode and per-file runtime hashes. For the default
release, `runtimeDependencySha256: null` means automatic resolution, not an empty
closure. The summary identifies both subjects and hashes. Equal identities alone
do not establish a causal comparison: retain the parent/head build provenance and
check every dependency difference before attributing a result to the PR.

## Reading the result

`summary.md` in the output directory lists every run. Each run's directory retains
`stdout.json`, `stderr.log`, and `exit-code.txt`. First check two things:

1. Every row's "Subject JAR" column says `ok`. This requires confirmed class-load
   evidence and observed class sources matching the expected artifact hash. The
   configured `expectedSource` alone does not prove which JAR ran. Candidate runs
   must have loaded the build under test, and baseline runs the selected baseline.
2. Inspect every baseline failure. It may expose a release bug, a harness defect,
   or an environment problem. Preserve its evidence and investigate the cause;
   a failing baseline does not establish an unhealthy environment or let us
   attribute the finding to the pull request.

Then compare the verdicts:

- **Candidate fails, baseline passes.** A `fail` with `missing-ids` or `duplicate-ids` is a
  lead. Re-run it, and read that run's evidence before you report it.
- **`inconclusive`.** The experiment lacks evidence needed for a conclusive verdict,
  for example because a fault missed its window. Inspect the raw attempt and its
  diagnostics: a recorded data failure remains a finding to investigate even when
  missing evidence prevents attribution.

Keep completion failures separate from data conclusions. After a confirmed physical
fence, an explicit HA leader-resolution deadline expiry during completion reports
`verification.flink.job-completion-timeout`; an ordinary resolver failure reports
`verification.flink.job-terminalization-failed`. An unconfirmed fence reports
`verification.flink.process-fence-failed`. Correct timeout classification does not show
that recovery completed or that final output was complete. Missing HA/token proof
cannot turn a recorded data or completion FAIL into a successful result.

The summary compares the counts of each `(verdict, reason)` pair between baseline
and candidate, and reports variability within each side separately. Matching
distributions do not establish correctness; differing distributions are leads,
not proof of a regression, because fault and checkpoint timing varies.

The gate exits `0` only when every requested run has a passing scenario verdict,
confirmed subject evidence, and subprocess exit code `0`. It exits `1` for any
failing, inconclusive, or error result, inconsistent subprocess exit, or unconfirmed
subject. Invalid command-line arguments, including a non-positive `--runs`, exit
`2`. Preserve both the summary and raw evidence when the gate fails.

The candidate's classpath is the connector JAR plus exactly the JARs in `--runtime-dir`.
A pull request that changes dependencies is therefore tested with its own dependencies.

## Testing the gate

Run the Python checks without Docker:

```bash
python3 -m unittest discover -s tools/tests -v
```

## Reporting

A summary for the pull request names:

- the scenarios and the number of runs;
- the verdicts on both sides;
- both connector SHA-256 values and the selected baseline provenance.

Posting on the pull request is public. The maintainer decides whether to post and what.
