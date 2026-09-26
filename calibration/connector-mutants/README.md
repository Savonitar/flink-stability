# Connector ambiguity calibration

This standalone recipe builds a shared timing control and two deliberately wrong
Kafka connector decisions, then checks that the canonical EndTxn scenarios catch
them. Assuming success must lose records when the EndTxn request never reached the
broker. Rewriting must duplicate records when the successful commit response was
lost. The [results](#results) are bounded calibration evidence, not proof of general
connector correctness or stability.

The sources and binary are pinned to
`org.apache.flink:flink-connector-kafka:5.0.0-2.2`. The actual release runtime closure
uses Kafka clients **4.2.0**. Flink 2.2.0 and Objenesis are needed only to compile or
test; generated subject snippets deploy the release runtime closure alone. The
recipe overlays a few classes while preserving other upstream JAR contents,
including LICENSE and NOTICE. It does not build a connector fork or install into
the Maven cache.

## Reproduce

Use JDK **21.0.7** in `JAVA_HOME` and `PATH`, Python 3, Maven, and `patch`. The
source artifact (`org.apache.flink:flink-connector-kafka:5.0.0-2.2:jar:sources`),
release dependencies, and dependency plugin 3.8.1 must already be cached. The
recipe is offline and rejects source/binary changes. It also requires each
rebuilt JAR to have the exact hash of the runtime-tested artifact.

```sh
python3 calibration/connector-mutants/build.py
```

`target/build-evidence.json` records hashes and checks; `target/*-subject.yaml`
contains each local primary and its explicit runtime dependencies. All generated
files are ignored. Standalone checks exercise shared settings, ordinary commit
success/fencing/unknown-error/non-timeout retry, live versus recovered timeout
handling, Kafka's actual uncancelled deadline primitive, and replay bounds/copies/
commit/cleanup. They create no Kafka client or broker.

Runtime reproduction uses the canonical `commit-request-lost` and
`commit-response-lost` scenarios. Build the harness as described in the repository
README, then generate a fresh catalog directory:

```sh
python3 calibration/connector-mutants/catalogs.py \
  --harness-root . --artifact-root . --output jobs/connector-calibration
```

The script preserves each canonical name and expected PASS contract, makes the
sink `transaction_timeout: 60s` explicit in every cell, and substitutes only the
primary connector between variants. It verifies artifacts, refuses an existing
output directory, and captures source/patch/build/template hashes. The evidence
archive includes the imported `tools/subject_catalog.py` helper and its hash in
each catalog's `sourceHashes`. Artifact paths
resolve against `--artifact-root`, which must equal `--harness-root` and contain
all selected JARs, so the canonical workload path also resolves correctly.

From that harness/artifact root, with its normal Maven build available:

```sh
for mode in control assume rewrite; do
  for side in request response; do
    cell="jobs/connector-calibration/$mode-$side"
    scenario="commit-$side-lost"
    mvn -q -o exec:java -pl cli \
      -Dexec.args="validate --catalog-root $cell --scenario $scenario --artifact-root . --offline"
    if mvn -q -o exec:java -pl cli \
      -Dexec.args="run --catalog-root $cell --scenario $scenario --artifact-root . --offline" \
      > "$cell/stdout.json" 2> "$cell/stderr.log"; then
      printf '0\n' > "$cell/exit-code.txt"
    else
      printf '%s\n' "$?" > "$cell/exit-code.txt"
    fi
  done
 done
```

Run serially. The two killed-mutant cells intentionally return failure under the
unchanged PASS contracts. Retain stdout, stderr, attempt checkpoint directories
(class-load and raw proxy-event files), and catalog evidence before another
`mvn clean` removes runtime artifacts. Evaluate raw attempt outcomes; do not
convert expected failures into green scenario results to claim calibration.

## Injected decisions and limits

All artifacts apply the same `timing-control.patch`: transactional producers use
`max.block.ms=5000` and `request.timeout.ms=30000`. The healthy control keeps the
upstream committer. The shorter commit wait lets a dropped EndTxn reach its
exception path; settings alone do not prove that happened. The marker
`CALIBRATION_COMMIT_TIMEOUT tx=...` is emitted to stderr only after a timeout from
the actual `KafkaProducer.commitTransaction()` call. Kafka 4.2.0 bytecode and the
local check confirm that this deadline does not cancel the pending operation.

The explicit shared scenario timeout, 60 seconds, lets a healthy 30-second request
retry complete while allowing abandoned transactions to expire within the
2-minute oracle bound. The runner's terminal fence does not abort them; the
usual 2-hour timeout would leave their last stable offset pinned too long.
Both healthy controls must pass with these shared settings.

- **Assume committed:** after a live producer times out, force-close it to stop
  later internal retries, discard it from the pool, and complete the commit
  request without retry. The marker is `CALIBRATION_ASSUME_COMMITTED tx=...`.
- **Rewrite:** retain immutable byte-array payloads, stop the original sender,
  then commit the buffer using a fresh transaction ID. The original producer is
  discarded and the commit request completes. The marker is
  `CALIBRATION_REWRITE_COMPLETE tx=... records=N`.

The unsuccessful backchannel notification in both mutants means “discard this
closed producer”; the Flink commit request itself is deliberately successful.
These mutations include abandonment of the old producer, not just swallowing a
catch-branch exception. Later retries cannot conceal the injected wrong decision.

Replay supports live, chained, byte-array transactions only: at most 10,000
records, 16 MiB of conservatively counted payloads, and 128 headers per record.
It copies keys, values, headers, topic, requested partition, and timestamp.
Unsupported values, overflow, absent/recovered payloads, or a failed replay fail
explicitly; there is no second replay attempt or recovery serialization. These
runs used one partition and one task, with no TaskManager kill. Require matching
fault/timeout/decision transaction IDs, complete fault/fence/oracle evidence,
selected-class origins, and no `CALIBRATION_UNSUPPORTED` marker before interpreting
an individual mutant run. Counts vary with which checkpoint the fault hits.
`target/build-evidence.json` records the artifact hashes.

## Results

Last run: 2026-09-26 on `main` at `7ee3d12`, JDK 21.0.7. The build reproduced all
three tested artifact hashes, and the standalone checks passed. Each cell ran once with
3,000 records.

| Artifact | `commit-request-lost` | `commit-response-lost` |
| --- | --- | --- |
| control (release + shared timing) | pass, exact output | pass, exact output |
| assume committed | **fail**, 147 missing IDs | pass, exact output |
| rewrite | pass, exact output | **fail**, 157 duplicate IDs |

Each wrong decision is caught on the fault side where it is wrong. On the other side it
is harmless: assuming a commit is right when the broker did commit, and rewriting is
right when nothing was committed.

Every cell passed the interpretation checks above:
- entry classes loaded only from the cell's own artifact;
- the fault dropped its message before the trigger deadline;
- the dropped transaction, the timeout marker, and the decision marker named the same
  transactional ID;
- no `CALIBRATION_UNSUPPORTED` marker appeared.
