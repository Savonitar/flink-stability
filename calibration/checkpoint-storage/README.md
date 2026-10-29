# Checkpoint-storage qualification

New batches use `catalogContractVersion: 2`. Only mutant catalogs explicitly set
`meta.allow_state_reset_mutant: true`. Reference pilots and live attempts must
report the selected `buildMode` and matching `allowStateResetMutant` value.
Older frozen batches remain historical qualification under their retained
verifier. The current checker, including `--re-audit`, rejects their older
contract rather than inferring consent from artifact names or changing receipts.

This recipe freezes and runs PR5's twelve-attempt qualification matrix for the
integrated FUSE checkpoint write-EIO experiment. It uses the correct and
reset-on-restore workload JARs from the retained PR4 pilots. The
[PR4 guide](../../docs/CHECKPOINT-STORAGE-FAULTS.md) describes the experiment;
[the implementation plan](../../docs/STATE-RECOVERY-STORAGE-FAULT-PLAN.md)
records the separate prototype, integration and qualification gates.

| Cell | Workload and operation | Required raw result |
| --- | --- | --- |
| `correct-healthy` | Correct workload, healthy FUSE, uninterrupted | PASS |
| `correct-restart` | Correct workload, healthy FUSE, one TaskManager replacement | PASS |
| `correct-eio` | Correct workload, checkpoint create/write EIO, replacement, heal | PASS |
| `mutant-eio` | Reset-on-restore mutant, the same EIO/replacement experiment | FAIL: `validator.kafka.state-prefix.state-mismatch` |

Each cell has three attempts. Every catalog retains its PASS expectation,
including the mutant. The checker recognizes the deliberate mutant failure;
it does not rewrite the raw FAIL or the expectation mismatch. Missing IDs,
duplicates, unexpected IDs, malformed records or incomplete traversal cannot
substitute for the required state-only failure.

The fixed workload has 30,000 input records, eight keys, parallelism one, a
5 ms requested processing delay and a 10-second checkpoint interval. Natural
completion has a five-minute deadline. One JobManager and one TaskManager use
the pinned released Flink 2.2.0 image on Docker Desktop `linux/arm64`; Kafka and
the complete connector closure remain pinned. Native Linux, other platforms,
ENOSPC, permanent disk loss and broker-storage faults are outside this matrix.

## Prepare the framework once

Run the normal full reactor verification and installation with JDK 21 before
freezing. The freezer requires each of these six module JARs to match its
installed Maven artifact exactly: `runtime-api`, `core`, `testcontainers`,
`cli`, `kroxylicious-fault-filter` and `synthetic-token-plugin`. It also compares
all packaged class/resource bytes with `target/classes` and retains source
byte snapshots. These hashes identify bytes; they do not attest a Git revision
or prove compiler provenance.

Select an explicit artifact cache and fresh preparation directory:

```sh
python3 -B calibration/checkpoint-storage/prepare.py \
  --output jobs/checkpoint-storage-preparation \
  --repository /path/to/repository \
  --java-home /path/to/jdk-21 \
  --maven /path/to/mvn
```

`prepare.py` runs the pinned Maven dependency plugin's runtime-classpath goal
and the checked-in `ResolveClosure.java` helper offline. It uses empty Maven
settings and a fresh home directory. Its retained outputs include the exact
classpath, the connector JAR/POM closure, commands, exit codes and
`framework-inputs.json`. It reads only the selected artifact cache; it does
not discover user settings or credentials. Missing cached dependencies fail
preparation without an online retry.

`framework.py` copies the six module JARs, the exact runtime classpath and only
explicitly listed Maven artifact files. The frozen repository receives empty
settings and generated artifact-origin markers. Existing cache tracking files
and repository credentials are not copied. JAR manifest classpath expansion,
foreign JAR indexes, path traversal and symlinks are rejected; an index naming
only its own exact JAR is preserved without changing that artifact.

## Freeze the four controls

Use a fresh output directory under `jobs/`. Supply the retained PR4 artifact
manifest and a JSON object mapping all four cell names to successful separate
pilot result files. For `mutant-eio`, successful calibration means the required
raw state-mismatch FAIL with complete fault, restore and ID evidence. Example
reference-file shape, with generated paths to replace with the actual pilots:

```json
{
  "correct-healthy": "jobs/pilots/correct-healthy/stdout.json",
  "correct-restart": "jobs/pilots/correct-restart/stdout.json",
  "correct-eio": "jobs/pilots/correct-eio/stdout.json",
  "mutant-eio": "jobs/pilots/mutant-eio/stdout.json"
}
```

```sh
python3 -B calibration/checkpoint-storage/catalogs.py \
  --parent-manifest jobs/pilot-batch/manifest.json \
  --references jobs/qualification-inputs/references.json \
  --framework-inputs jobs/checkpoint-storage-preparation/framework-inputs.json \
  --platform-evidence jobs/checkpoint-storage-preparation/platform.json \
  --output jobs/checkpoint-storage-qualification
```

The resulting manifest pins the workload arms, helper build and files, runtime
identity, four catalog/expectation pairs, reference pilots, all recipe files and
the frozen framework manifest. The framework includes the preparation sources,
raw classpath input and explicit artifact list. No qualification attempt has
started at this point. Keep missed windows and earlier pilot failures in their
original directories; they are not qualification attempts and are not deleted.
The optional `--platform-evidence` copies an explicitly captured JSON receipt
for Docker Engine/kernel/architecture and host OS/Python versions, pins its hash
and includes those values in the Markdown report. Original pilot paths are
recorded relative to the workspace alongside their hashed frozen copies.

## Run exactly twelve attempts

Use the frozen runner and an explicit local Unix Docker endpoint. Substitute
the endpoint and Docker executable already selected for the local environment:

```sh
python3 -B jobs/checkpoint-storage-qualification/evidence/recipe/run.py \
  jobs/checkpoint-storage-qualification \
  --workspace-root . \
  --docker-endpoint unix:///var/run/docker.sock \
  --docker /path/to/docker \
  --docker-api-version 1.44
```

Use `python3 -B` for frozen Python entry points so imports cannot create unlisted `__pycache__` files in the frozen inventory. The runner invokes `java -cp` using only frozen JARs. It passes the isolated
Maven repository to the CLI resolver and requires `--offline`. Each attempt
has its own working directory, empty home/Docker configuration, temporary
files and checkpoint evidence. Both Docker API settings are explicit. Maven
is not involved in the twelve launches; mutable `target/classes`, installed
reactor JARs and the shared Maven cache are not launch inputs.

Before each attempt the frozen manifests and bytes are checked again. Execution
is sequential, with no retries and no resume. An ordinary non-pass result is
retained and does not authorize another attempt. The runner stops if execution
is interrupted, exceeds its outer deadline, or resource cleanup cannot be
confirmed. To investigate or change a recipe, prepare a separate fresh batch;
never overwrite the frozen manifest or rerun an existing attempt directory.

## Check evidence without rewriting outcomes

Run the retained checker without creating Python bytecode beside frozen inputs:

```sh
python3 -B jobs/checkpoint-storage-qualification/evidence/recipe/check.py \
  jobs/checkpoint-storage-qualification
```

The independent checker requires physical writer fencing, natural bounded-job
completion and a complete fixed `read_committed` snapshot. Recovery cells also
need explicit pre-kill readiness, exact physical replacement, restored keyed
state and post-restore progress. EIO cells additionally need completed external
keyed checkpoint materialization, the bound failed checkpoint and native EIO
receipts, the armed kill/heal order, a new healthy daemon and a subsequent
successful checkpoint.

Per-attempt command, exit code, stdout/stderr, component/class-loading logs,
state markers, immutable FUSE journals and their hashes remain available.
Component-error events and expected producer-fencing observations remain in
reports even when terminal data validation passes. Cleanup failures, missing
proof, timeout and missed fault windows do not qualify as success.

The current checker also binds mutant totals and all retained mismatch samples
to the per-key `RESTORED_KEY`/`MUTANT_RESET` transitions: the mismatch count must
equal the corrupted suffix, and actual count/sum must match the reset arithmetic.
It independently extracts the complete recognized component-event set from every
hashed retained physical-process output. Reported and extracted sets must match;
omissions, duplicates, extra events and partial coverage cannot qualify. The
audited scope is the **retained final prefix**. Historical receipts do not record
the earlier component collector's exact byte boundary or the whole process
lifetime; the report preserves that limitation instead of inventing a boundary.
The recorded intervals must put all four preflight validations before the first
live attempt and all twelve live attempts in non-overlapping allocation order.
Every wall-clock interval and monotonic elapsed duration must be positive. Their
durations must agree within one second, allowing for adjacent clock sampling;
larger clock changes leave chronology unconfirmed. Zero-width or artificially
compressed intervals cannot stand in for a real launch. These are checks of
recorded clock evidence, not independent clock attestation.

An updated verifier must not replace a historical frozen recipe. To audit an
existing batch with the current stricter checker, explicitly select re-audit and
write to a new location:

```sh
set -C
python3 -B calibration/checkpoint-storage/check.py \
  jobs/checkpoint-storage-qualification --re-audit > jobs/new-re-audit.json
python3 -B calibration/checkpoint-storage/report.py \
  jobs/checkpoint-storage-qualification --re-audit > jobs/new-re-audit.md
```

Re-audit still checks the original frozen files, runtime artifacts and hashes.
Its output identifies both the original recipe and the executing verifier. It
qualifies only the recorded runtime bytes; changed Java code needs fresh live
verification. Default replay continues to require the exact frozen checker and
renderer.

The final checker report must distinguish nine raw PASS controls from three
raw mutant state FAIL results. Report the exact frozen artifact identities,
all twelve outcomes, component errors and platform limitations separately from
offline tests and earlier pilot observations.

Run the retained checker for JSON, or its deterministic renderer for Markdown.
Both return a nonzero exit status when the matrix is unqualified. The Markdown
renderer preserves all twelve allocations, missing/error rows and the mutant's
raw FAIL; it does not alter the original result files. Choose a new output name
and enable shell no-clobber so an earlier report cannot be overwritten:

```sh
set -C
python3 -B jobs/checkpoint-storage-qualification/evidence/recipe/check.py \
  jobs/checkpoint-storage-qualification > jobs/checkpoint-storage-qualification/new-check-report.json
python3 -B jobs/checkpoint-storage-qualification/evidence/recipe/report.py \
  jobs/checkpoint-storage-qualification > jobs/checkpoint-storage-qualification/new-report.md
```
