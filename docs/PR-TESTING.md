# Testing a Kafka connector pull request

For changes to Flink itself, see [testing a Flink runtime build](FLINK-RUNTIME-TESTING.md).
The ordinary connector gate below changes connector artifacts. The
[custom runtime subject guide](CUSTOM-RUNTIME-SUBJECTS.md) adds shared runtime
substitutions and an explicit candidate-only Flink configuration overlay, with
strict source/dependency checks for that comparison mode.

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

The historical [connector-mutant calibration](../calibration/connector-mutants/README.md)
showed that the EndTxn scenarios caught two wrong commit decisions on the
2026-09-26 engine. The current common filter still requires the complete six-cell
requalification described below. The
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
catalogs. These older calibration matrices do not cover the new broker/protocol profiles below.

For a Flink runtime PR, prepare separate image/runtime-JAR pins and compatible
connector/workload artifacts as described in [runtime build testing](FLINK-RUNTIME-TESTING.md).
`pr_gate.py` can apply one shared runtime configuration to both arms; it does not
construct a pair of different Flink images, change token proof scope, or gate fault
cells on both healthy controls passing. Custom runtime mode requires an explicit
baseline and permits only the connector primary and declared candidate Flink
configuration to differ.

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

The four `pool-reuse-inflight-{kill,control}-{v1,v2}` catalogs exercise POOLING
reuse while the next checkpoint is pending. Select them explicitly with repeated
`--scenario` flags. Their [calibration report](POOL-REUSE-CALIBRATION.md) explains
the asynchronous snapshot delay, required kill-window audit, pinned single-class
mutant, measured timings and retained live results. A terminal PASS alone does
not establish that this window was hit; retain and audit its checkpoint and broker
observations. The generic gate preserves the raw verdict, while the calibration
checker rejects a margin below three seconds as `checkpoint-window.missed`.
Unexpected data failures keep their original failing verdict and reason even
when window evidence is missing. REST observation failures retain their separate
infrastructure reason and original error evidence. A successful calibration audit
means the evidence satisfies the recipe's [qualification rules](../calibration/pool-reuse-mutant/README.md#calibration-contract).

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


## Chaos profiles against a PR parent

`--profile chaos-quick` selects ten scenarios: TaskManager recovery; commit request
and response loss; partition-leader kill and pause; coordinator pause; coordinator
failure during commit in TV2; EndTxn delay and retriable coordinator rejection;
and Produce response loss. Quick selects the explicit TV1 broker-target variants.
Full omits the five unversioned broker aliases; they remain selectable with `--scenario`.
With `--runs 2` this is **40 independent runs** (two
sides), roughly **60–120 minutes** with warm images and artifacts. This is a
planning estimate, not measured throughput or a timeout. Pulls, startup and
recovery can increase it.

`--profile chaos-full` contains 130 scenarios: the quick profile plus explicit
TV1/TV2 broker and protocol catalogs, POOLING and rolling faults, packet faults,
parallelism-four and at-least-once variants, and savepoint/restore catalogs with
their controls. AddPartitionsToTxn is TV1 only: the TV2 sink normally does not
send it. With two runs per side, it plans **520 independent attempts**. The gate's
generic 1.5–3 minute estimate gives 780–1560 minutes (13–26 hours); longer fault
holds and lifecycle operations can exceed that estimate. The sole authoritative membership lists are in
[`tools/chaos_profiles.py`](../tools/chaos_profiles.py); adding a filename does
not silently expand either profile. Version copies change only scenario identity
and the explicit transaction feature level; original catalogs remain untouched.

Profiles require both `--baseline-*` arguments: build the PR parent and candidate
separately and preserve their commit IDs/build provenance. The gate checks both
sides identically, but cannot infer parentage from a JAR hash. Explicit
`--scenario` still works, including the previous released default baseline;
`--profile` and `--scenario` are mutually exclusive.

After placing both builds inside the harness root, one gate command is:

```bash
python3 tools/pr_gate.py --profile chaos-quick --runs 2 \
  --baseline-connector-jar jobs/pr/parent/connector.jar \
  --baseline-runtime-dir jobs/pr/parent/runtime \
  --connector-jar jobs/pr/head/connector.jar --runtime-dir jobs/pr/head/runtime \
  --flink-image docker.io/library/flink:2.2.0 --output jobs/pr/chaos-quick
```

Use `--profile chaos-full` and a fresh output directory for full coverage. Add
`--dry-run` to print the plan only: no output files, artifact builds, Maven calls,
Docker access or scenario execution. Planning does not require the JARs to exist;
execution validates their paths, SHA-256 and closures before creating output.
The plan always prints before execution and is retained in `manifest.json`.
The optional full-image spelling changes only the two generated catalog copies.
All runs collect `--kafka-log-output` into separate run directories.

The first summary table has one row per **scenario and side**: verdict counts,
summed missing/duplicate counts, KafkaCommitter ERROR and WARN counts, log coverage,
and fault confirmation. Counts are sums across repetitions, not unique IDs across
runs. Unavailable counts remain `unknown`; partial log counts are lower bounds on
the captured prefix, not proof that no other errors occurred. Only the exact
`org.apache.flink.connector.kafka.sink.internal.KafkaCommitter` logger contributes
to the two committer columns. Per-run rows and raw stdout/stderr remain available.
The machine-readable equivalent is `summary.json`.

`Candidate-only losses or duplicates: YES` means at least one candidate run has a
positive count for a metric for which **every corresponding baseline run has a
known zero**, with matching side coverage, valid subject origins and confirmed
fault effects. Missing or inconclusive evidence yields `UNKNOWN`, never an
invented zero. `NO` means no such candidate-only metric was observed, not that
both builds passed; shared failures remain failures. The same missing-ID count
on both sides does not establish the same missing IDs or assign blame.

The gate verifies every expected TaskManager/network/broker receipt in these
profiles, including both fault and heal for broker_fault. A reported PASS with
missing/false effect receipts is not a passing gate. Component errors are
observations and do not independently change the exact-ID verdict. Every baseline
failure remains a finding; do not silently remove its scenario from the profile.

## Calibrate chaos coverage before trusting it

The [single-class retriable-discard recipe](../calibration/chaos-profile-mutant/README.md)
builds release 5.0.0-2.2 with only `KafkaCommitter.class` changed. Its wrong decision
is to discard a pending commit after a retriable exception. The Kafka client usually
absorbs coordinator error replies internally; the shared, explicit
`--producer-max-block-ms 5000` calibration setting lets a prolonged uncertain commit
reach the committer as `TimeoutException`. This flag adds `--producerMaxBlockMs` to
both **copied** bundled workloads and records it in the plan. It is absent by default;
use it for calibration, not implicitly for a PR. Broker request timeout, transaction
timeout, fault rules, checkpointing, load and expectations remain unchanged.

Require release PASS across `chaos-quick`, a verified mutant FAIL with positive
missing IDs in a protocol/coordinator scenario, and a passing no-fault paired control.
A failed command alone is not calibration. The recipe's `check.py` rejects missing
cells, unconfirmed effects, unknown counts, wrong artifact/closure hashes and failed
healthy controls. This profile's live qualification remains open; the recipe
reports build and decision checks, without a qualified live matrix. Those checks
establish mutation and artifact identity, not profile sensitivity or release health.

## Live qualification is still pending

The current common filter remains unqualified against the historical
[connector-mutant six-cell matrix](../calibration/connector-mutants/README.md#revalidation-required-after-the-proxy-replacement).
The qualification gate requires all six cells on the current harness, even
though canonical EndTxn YAML is unchanged: both controls and both harmless mutant
cells must PASS, assume/request must FAIL with missing IDs, and rewrite/response
must FAIL with duplicates. Every cell also needs complete interpretation evidence.
The published 2026-09-26 results apply to their recorded engine; the new mutant
does not substitute for this still-open qualification gate.

The broker-fault, protocol and coordinator-commit profiles and the new mutant
have offline/fake/serialization coverage, without a qualified live matrix reported
here. The public single-broker smoke results in the
[custom runtime guide](CUSTOM-RUNTIME-SUBJECTS.md#verification-scope) do not qualify
these fault profiles. Post-append Produce timeout qualification is also open.
`error-response` acts before broker forwarding;
`error-after-append` is a separate response-path action. Keep all live results
and classify anomalies before interpreting any profile as qualified.

## One-command local checkout comparison

`tools/connector_pr_test.py` resolves local refs, builds an explicit baseline and
candidate in separate detached Git worktrees, and invokes `tools/pr_gate.py` with
both artifacts. Keep the connector checkout and a new output directory inside this
repository. It does not fetch, push, commit, delete worktrees, or modify the input
checkout's working tree. For example, after installing the harness with JDK 21:

```bash
python3 tools/connector_pr_test.py \
  --checkout jobs/pr/321/connector --head pr-head --merge-base-of main \
  --output jobs/pr/321/comparison --profile auto --runs 2
```

Use `--base <ref>` instead of `--merge-base-of` for an explicit baseline. The tool
resolves both arguments to commits before creating worktrees; a merge-base compares
the head with its actual common ancestor, rather than whatever `main` later points
to. `--dry-run` performs read-only ref/diff inspection and prints coverage without
creating output. `--build-only` retains both builds and the exact gate command,
without launching the chaos runs. Each failure retains its worktrees, logs and
failure record; retry with a new output directory.

Each side gets a separate Maven repository, isolated HOME and JVM user directory.
Both Maven settings scopes mirror every artifact/plugin repository to
`https://repo.maven.apache.org/maven2/`. Builds use `-DskipTests`, the connector
module plus required reactor modules, and a pinned dependency-copy plugin. No
snapshots or dependencies unavailable from Central are silently fetched elsewhere.
The manifest records resolved commits/trees, both primary JAR SHA-256 values, every
runtime dependency hash, settings hash, Maven/JDK output, commands and selected
scenarios. The gate uses the already-installed harness and its existing offline
Maven launch, independently of the two connector build caches. Select the build JDK
with the normal `JAVA_HOME`/`PATH`; the harness must run with JDK 21.
`--maven` accepts a command on PATH or a path inside this repository. Relative paths
are bound to the harness root before entering the isolated build directories.

Project `.mvn` JVM/argument/extension overrides, symlinks, submodules and alternate
object stores are refused for separate review. The mirror constrains Maven
resolution, not arbitrary network code executed by a build plugin. Use only a
trusted checkout; this tool is not an OS sandbox.

The `brokers`, `protocol`, `pooling`, `rolling`, `packet`, and `at-least-once`
profiles are explicit scenario lists in `tools/chaos_profiles.py`, including the
applicable controls and parallel variants. `chaos-quick` remains the explicit
10-scenario list; `chaos-full` currently includes 130 scenarios. An automatic
suggestion uses the following first-matching path-prefix table (under
`flink-connector-kafka/src/`); the exact table and each matched path are retained in
the tool and plan. An unknown path or empty diff selects `chaos-full`. Multiple
suggested profiles form a deduplicated union. Suggestions are conservative starting
points, not proof that a changed behavior is covered.

| Path prefix | Suggested profiles |
| --- | --- |
| `main/java/org/apache/flink/connector/kafka/sink/internal/TransactionalIdFactory` | pooling, rolling |
| `main/java/org/apache/flink/connector/kafka/sink/internal/KafkaCommitter` | protocol, pooling, brokers |
| `main/java/org/apache/flink/connector/kafka/sink/internal/FlinkKafkaInternalProducer` | protocol, packet, at-least-once |
| `main/java/org/apache/flink/connector/kafka/sink/` | pooling, protocol, rolling, at-least-once |
| `main/java/org/apache/flink/connector/kafka/source/` | brokers, rolling, packet |
| `test/java/org/apache/flink/connector/kafka/sink/` | pooling, protocol, at-least-once |
| `test/java/org/apache/flink/connector/kafka/source/` | brokers, rolling, packet |
| Anything else, including POM/build changes | chaos-full |
