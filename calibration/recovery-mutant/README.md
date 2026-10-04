# Source checkpoint recovery calibration

This recipe builds a deliberately incorrect Kafka source from the released
`org.apache.flink:flink-connector-kafka:5.0.0-2.2`. For a bounded split whose next
offset is `k`, it saves `k+1` in the checkpoint instead. Live reader state and the
offsets committed to Kafka remain unchanged. A TaskManager recovery then skips
one input record; uninterrupted execution stays correct.

The scope is **source checkpoint restoration**, not sink commit atomicity or
pending-transaction recovery. Dropping a restored sink committable would not
reliably calibrate the canonical scenario: its transaction may already have
committed before the kill.

## Build without a broker

Use JDK 21, Python 3.9 or later, Maven, and `patch`. Supply an existing
Maven cache and settings file inside this repository explicitly. Maven runs
offline with those settings for both user and global scopes; the recipe does not
select `~/.m2`. Cache/settings symlinks are rejected before following them. Maven
RC loading is disabled, JVM user-home and temporary paths live under `target`,
and only PATH plus explicit JDK settings are carried into subprocesses. The release
source JAR, dependencies and dependency plugin 3.8.1
must already be cached. Set `JDK_21_HOME` to the JDK 21 directory and the Maven
variables below to the existing cache and settings file:

```sh
python3 calibration/recovery-mutant/build.py \
  --java-home "$JDK_21_HOME" \
  --maven-repo "$REPOSITORY_LOCAL_MAVEN_CACHE" \
  --maven-settings "$REPOSITORY_LOCAL_MAVEN_SETTINGS"
```

The command refuses an existing `target` directory. Preserve prior artifacts and
evidence before preparing a fresh build. It verifies the same released source and
binary hashes as `../connector-mutants/`, compiles one replacement class, and
checks that **only `KafkaSourceReader.class` changes**. Every other archive entry,
including LICENSE and NOTICE, remains byte-identical. It does not apply the
EndTxn timing patches, alter producer timeouts, or install a fork into Maven.

`target/build-evidence.json` records source, binary, recipe, runtime-dependency,
build-dependency and actual output hashes. Runtime dependencies come from the
release closure, including Kafka clients 4.2.0; Flink 2.2.0 dependencies are used
to compile and check the overlay. The mutant hash is recorded as a newly built
artifact, **not a previously Docker-tested hash**.

`RecoveryCheck.java` runs once with the unmodified release and once with the
mutant. It uses the real reader's `addSplits`, `snapshotState`, checkpoint
notification, and split serialization/restoration APIs. A recording fetcher
replaces Kafka I/O and refuses to construct a Kafka client. Checks cover:

- original split and live state are unchanged;
- repeated snapshots do not accumulate changes in live state;
- Kafka checkpoint-offset bookkeeping and commit notification keep offset `k`;
- a serialized checkpoint handed to a new reader starts at `k+1` only for the mutant;
- empty, unbounded, sentinel, finished and maximum-long offsets are handled without
  overflow or falsely claiming a mutation;
- both enabled and disabled Kafka offset-commit paths use the checkpoint hook.

These checks do not prove that Flink restores the state in a real cluster.

## Generate and interpret the calibration matrix

After building the engine, generate four fresh catalogs:

```sh
python3 calibration/recovery-mutant/catalogs.py \
  --harness-root . --artifact-root . --output jobs/recovery-calibration
```

The generator substitutes the connector primary and explicit release dependency
closure. The kill cells retain the canonical `bounded-eos` workload, timing,
checkpoint settings and faults. Separate no-kill controls remove only the
`taskmanager-recovery` phase. Every cell retains the byte-identical canonical
**PASS** expectation. No expected-result file is changed to make the mutant green.

| Artifact | Canonical TaskManager recovery | Separate no-kill control |
| --- | --- | --- |
| release | PASS, exact output | PASS, exact output |
| mutant | FAIL, `validator.kafka.id-set.missing-ids` | PASS, exact output |

For the canonical single partition, 3,000 records and one restore, the designated
mutant cell must observe 2,999 distinct expected IDs, exactly one missing ID and
zero duplicate, unexpected or malformed records. Its nonzero command exit and
raw failure must be preserved. The other three cells must pass with exact output.
Run the cells serially. Retain each catalog,
manifest, build evidence, command exit, stdout JSON, stderr and all attempt logs
before `mvn clean` removes runtime artifacts.

## Required recovery evidence

The mutant logs these markers from `KafkaSourceReader`:

```text
CALIBRATION_RECOVERY_SNAPSHOT checkpoint=<id> topic=<topic> partition=<n> from=<k> to=<k+1> stop=<end> process=<hostname>
CALIBRATION_RECOVERY_ADD_SPLIT topic=<topic> partition=<n> start=<offset> stop=<end> process=<hostname>
```

The snapshot marker is emitted only for a concrete `0 <= k < end`. The second
marker follows the unchanged `super.addSplits` call: it records the offset handed
to the reader/fetcher, not an independent observation of a Kafka consumer seek.
The existing `KafkaPartitionSplitReader` DEBUG message `SplitsChange handling
result` can additionally show the consumer's starting position. `process` reads
only the container's `HOSTNAME`; missing or ambiguous identity cannot establish
which incarnation emitted a marker.

A valid killed-mutant cell must establish all of the following:

- complete input, phase, process-fence and terminal-oracle evidence, a confirmed
  kill of an active TaskManager, and a checkpoint restored after that kill;
- one eligible snapshot marker for that restored checkpoint, from the original
  container, and a replacement-container split marker with the same topic,
  partition and bound, starting at the corrupted offset;
- marker hostnames correlated with the recorded container IDs, and class-load
  evidence for `KafkaSourceReader` from the selected primary in both TaskManager
  incarnations, alongside the engine's normal connector provenance checks;
- exactly one missing-ID sample equal to `k`. In this one-partition generated
  input, the engine reconciles record ID with Kafka offset before executing Flink.

Markers alone do not demonstrate recovery: the uninterrupted mutant also writes
corrupted checkpoint copies. Its no-kill control must have no restore and still
produce exact output. If the restored checkpoint has no eligible active split,
the marker is missing, the mutant survives, or other failures occur, calibration
is incomplete. Preserve that result rather than changing the canonical schedule
to obtain a preferred count.

## Validation status

The four-cell Docker calibration completed on 2026-09-27 using engine revision
`98c3684` with additional renderer and specification changes. It was not a
clean-checkout build of that revision or of current main.
The release and mutant no-kill controls passed all 3,000 records; release-kill
also passed. Mutant-kill retained its raw FAIL and exactly one missing ID, 19.
Checkpoint 1 saved source offset 19 as 20, and the replacement reader restored
offset 20. All 18 recorded owned containers were absent after cleanup.

The tested release SHA-256 was
`6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da`;
the tested mutant SHA-256 was
`5e58c26c9553f5f14a7b7ffac8f2d8bf3594c6c9e4a8b3c0ecd416fbd6046a4c`.
Only `KafkaSourceReader.class` differed. The original catalogs, command outputs,
recovery logs and cleanup evidence were retained for this historical result.
Both kill cases also logged a KafkaCommitter fenced-commit error. This calibration
does not classify those component errors or validate sink transaction recovery.

New recipe builds remain unvalidated until their exact input/output identities
are checked and any required runtime validation is recorded. A local integration
build does not transfer the historical Docker result to a new engine revision.
