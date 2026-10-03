# Chaos profile calibration: discard an uncertain retriable commit

Status: build and decision checks only; live calibration is pending authorization.
This recipe never starts containers. Do not interpret offline checks as observed
missing IDs or a passing released connector.

The subject is the unmodified release 5.0.0-2.2 versus an overlay that changes exactly
`org/apache/flink/connector/kafka/sink/internal/KafkaCommitter.class`. The patch
mistakenly treats a retriable commit failure as a completed request: it closes the
producer, tells the writer to discard it, and omits `retryLater()`. This models a
lost committable after a commit of uncertain outcome. It affects `TimeoutException`,
`ConcurrentTransactionsException`, `CoordinatorLoadInProgressException` and
`NotCoordinatorException`; unrelated retries, normal commits and fatal errors retain
release behavior. Both live and recovered producer paths are checked without a broker.
The recovered producer reference is cleared after close so a second error from a
closed producer does not substitute for the intended missing-data failure.

The Kafka client normally consumes coordinator retry errors inside its transaction
manager. One short injected error is therefore insufficient to exercise the
committer's catch. The calibration uses Kafka's supported `max.block.ms=5000` on
**both** workloads. Request timeout stays at the client's default 30000 ms. A lost
EndTxn request can outlive the caller's five-second wait: the release retains the
committable, whereas the mutant closes the sender before a retry can commit it.
The designated sensitive cell is `commit-request-lost` (also included in quick);
coordinator and newer protocol cells give additional coverage, without claiming
that each must lose records. A lost response after a broker commit may be harmless.
No impossible producer-fencing response is injected. There is no timing patch in the
connector and no unconditional record deletion. The shorter caller wait is explicit
calibration configuration, not a default or an edit to canonical scenario timings.

Pinned inputs and output:

- Released JAR: `6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da`.
- Released sources: `c05dcdc7d7256575f10f784c728f983b517058daee517ae258021edd1e87ecfe`.
- Mutant: `e17005ab7dd685446de90634662c842599f3a99119d65f9bab696acc63641397`.
- All runtime and compile/test dependencies: `dependency-pins.json`, verified before
  compilation. Recipe inputs and archive entry comparison are retained in build evidence.
- Compile using approved JDK 21.0.7-amzn, `javac --release 11`; release classes and ZIP
  entry metadata are preserved, and only the patched class is replaced.

## Prepare without Docker

From the harness root after its complete isolated build, set `JAVA_HOME` and `PATH`
to the approved JDK. Supply the build's repository, Central-only settings and Maven
executable. Reuse `connector-mutants/pom.xml` and its digest helpers. The cache must
already contain the released source JAR and dependency plugin 3.8.1. The recipe is
offline and refuses to overwrite an existing `target`:

```bash
python3 calibration/chaos-profile-mutant/build.py \
  --repository /path/inside/repository/to/isolated-build/repository \
  --maven /path/inside/repository/to/mvn \
  --settings /path/inside/repository/to/isolated-build/settings.xml
```

Outputs: `target/release.jar`, `target/discard-retriable.jar`, `target/runtime/`
(without the primary connector) and `target/build-evidence.json`.
Install the tested harness modules into the isolated Maven repository before using
`exec:java`; keep the same Maven/JDK/settings environment for the gate.

## Live commands — prepared, not executed

Run from the tested harness source root. Both commands copy catalogs with the full
Flink image spelling; neither modifies the original catalogs. Outputs must be new.
The gate deliberately exits 1 when it detects the mutant; run the checker separately
instead of masking an arbitrary failure with `|| true`.

```bash
python3 tools/pr_gate.py --profile chaos-quick --runs 2 \
  --baseline-connector-jar calibration/chaos-profile-mutant/target/release.jar \
  --baseline-runtime-dir calibration/chaos-profile-mutant/target/runtime \
  --connector-jar calibration/chaos-profile-mutant/target/discard-retriable.jar \
  --runtime-dir calibration/chaos-profile-mutant/target/runtime \
  --producer-max-block-ms 5000 --flink-image docker.io/library/flink:2.2.0 \
  --output jobs/calibration/chaos-quick
python3 calibration/chaos-profile-mutant/check.py jobs/calibration/chaos-quick/summary.json \
  --build-evidence calibration/chaos-profile-mutant/target/build-evidence.json

python3 tools/pr_gate.py --scenario broker-eos-control --runs 2 \
  --baseline-connector-jar calibration/chaos-profile-mutant/target/release.jar \
  --baseline-runtime-dir calibration/chaos-profile-mutant/target/runtime \
  --connector-jar calibration/chaos-profile-mutant/target/discard-retriable.jar \
  --runtime-dir calibration/chaos-profile-mutant/target/runtime \
  --producer-max-block-ms 5000 --flink-image docker.io/library/flink:2.2.0 \
  --output jobs/calibration/healthy
python3 calibration/chaos-profile-mutant/check.py jobs/calibration/healthy/summary.json \
  --control --build-evidence calibration/chaos-profile-mutant/target/build-evidence.json
```

Add `--dry-run` to either gate command to print its plan without touching outputs.
Require `CALIBRATION_DETECTED` **and** `CONTROL_PASS`. Release rows must all PASS
with zero missing/duplicate IDs; mutant must FAIL with positive missing IDs in at
least one protocol/coordinator cell with confirmed subject and fault evidence.
Unavailable results, a missed fault, a failure only in TaskManager recovery, duplicate
matrix rows, or a failed control do not establish this calibration. Preserve raw JSON,
logs, `.log` artifacts and manifests; record any anomaly in the open findings before
investigation. Expectations remain PASS on both sides so data failures stay visible.
