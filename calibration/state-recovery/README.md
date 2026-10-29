# State recovery calibration

New batches use `catalogContractVersion: 2`. Only mutant catalogs explicitly set
`meta.allow_state_reset_mutant: true`; correct controls omit it. The checker
requires both the observed `buildMode` and the explicit `allowStateResetMutant`
report field. Older frozen batches predate this contract and remain historical
qualification under their retained verifier; the current checker rejects them.
Do not add fields to, regenerate, or relabel their retained receipts.

This recipe qualifies the state oracle before storage fault experiments. It uses
the released Flink 2.2.0 distribution, Kafka 4.0.0 and Kafka connector 5.0.0-2.2,
with one input partition, one output partition, parallelism one, eight keys and
6,000 generated integer records. Each record is delayed by 10 ms; checkpoints
run every ten seconds. The readiness decision uses observed progress and a
completed checkpoint, rather than the elapsed delay.

| Workload | Uninterrupted | TaskManager replacement |
| --- | --- | --- |
| Correct | Three PASS results | Three PASS results with confirmed restore |
| State-reset mutant | Three PASS results | Three raw FAIL results with state mismatch and zero ID defects |

Every expected-result catalog remains `outcome: pass`. The mutant recovery cell
must fail its scenario expectation. The checker recognizes that raw failure as
calibration evidence; it does not reinterpret the scenario as PASS. Twelve
attempts are fixed before qualification starts. Missing windows and infrastructure
failures remain in the report and leave qualification incomplete. The runner
never retries, resumes, replaces or removes an attempt.

## Build and pilot

Use JDK 21 and an explicit Maven artifact cache. `build.py` copies only permitted
artifact-cache files to a fresh build directory, freezes its source inputs and
builds both workload arms offline. It supplies empty Maven settings and does not
read user settings or credentials. Select a fresh directory for every build:

```sh
python3 calibration/state-recovery/build.py \
  --java-home /path/to/jdk-21 \
  --maven-repo /path/to/repository \
  --output jobs/state-recovery-build
```

The two JARs have identical class bytes. Their only differences are the manifest
and private `META-INF/flink-stability/keyed-prefix-build.properties` resource.
The mutant clears only the keyed aggregate, once per key on its first event
after a genuine restore. Initial startup, source offsets and sink recovery use
the same code and configuration. Neither a program argument nor an ambient
resource can enable the mutant. `build-evidence.json` records both JAR hashes,
source and recipe hashes, changed entries, build commands and bytecode target.

Before qualification, use separate retained pilot directories to establish the
correct uninterrupted baseline and the restore window. `baseline.yaml` is a
template: replace its single `@WORKLOAD_JAR@` placeholder with the selected
workspace-relative JAR reference. Pin `image_id` alongside both Kafka and Flink
`image` fields using locally verified Docker image IDs. The Kafka pin enables
the per-container verified runtime receipt required by the checker; an unpinned
legacy Kafka result without that receipt cannot be a qualification reference.
`catalogs.render(..., runtime_identity=...)` can insert both pins into a pilot
template from an explicitly supplied verified identity map. For a restart pilot, insert
`restart-phase.yaml` immediately before `terminal_validations`. Copy
`state-recovery.expected.yaml` unchanged. The runner automatically applies the
state readiness gate to a TaskManager kill in this behavior; no extra YAML gate
or sleep is required. Pilot output must use the final code, recipe and artifacts.

Run normal affected-module and full-reactor checks and install the final reactor
before running the CLI. Do not rebuild the frozen workload JARs or edit the
harness while qualification is running. A pilot is not one of the twelve
qualification attempts.

## Freeze, execute and check

Freeze after the correct uninterrupted pilot has passed and the required restore
pilots have established the window. The reference result supplies the observed
Flink/Kafka image IDs, connector primary hash and complete installed connector
closure. Unknown identities are rejected. The freezer validates that the
reference is the correct workload's uninterrupted state-oracle run and inserts
both image IDs into every frozen cell catalog.

```sh
python3 calibration/state-recovery/catalogs.py \
  --build-evidence jobs/state-recovery-build/build-evidence.json \
  --reference-result jobs/state-recovery-pilot/stdout.json \
  --output jobs/state-recovery-qualification

python3 calibration/state-recovery/run.py jobs/state-recovery-qualification \
  --maven-repo /path/to/repository

python3 calibration/state-recovery/check.py jobs/state-recovery-qualification \
  > jobs/state-recovery-qualification-report.json
```

Choose a new report filename; shell redirection can overwrite an existing file.
The checker writes only to stdout and exits zero only for a complete qualified
matrix. It does not launch Docker, modify evidence or change expectations.
The runner uses the explicitly selected Maven `repository/` cache, generated
empty settings, the current installed reactor and a separate working directory
per attempt. Maven/CLI offline settings concern artifact resolution; they do not
disable public Docker image downloads permitted by the task.

For Docker Engine 29, add `--docker-api-version 1.44` to `run.py`. The runner
sets both `DOCKER_API_VERSION=1.44` and Maven `-Dapi.version=1.44` on every
attempt and records the exact invocation. It does not read or record the rest
of the environment. Without the option, existing client defaults are retained.

`manifest.json` freezes four cell catalogs, three fixed attempt paths per cell,
both workload JARs, reference runtime identity, the build receipt and recipe/source
copies. Attempt directories retain `stdout.json`, `stderr.log`, `exit-code.txt`,
the exact command and copies of the registered class/output logs. Original
checkpoint evidence remains in its original attempt directory. Existing batch
or attempt directories are rejected; an interrupted batch is retained as
incomplete and is not automatically resumed.

## Qualification evidence

The independent Python checker requires all of the following for every counted
attempt:

- The selected workload JAR hash and build mode, the pinned images and connector
  closure, and subject class origins from every TaskManager incarnation.
- Complete reconciled input, natural `FINISHED` completion, completed physical
  fencing of every incarnation, healthy lifecycle evidence and resolved sink
  transactions.
- A complete exactly-once state snapshot with expected/observed/distinct counts
  equal to 6,000, zero malformed/unexpected/duplicate/missing IDs and exact state
  totals. The mutant restart arm also needs bounded mismatch samples whose
  expected values satisfy independently recomputed arithmetic.
- Before kill: RUNNING, a fully traversed valid committed prefix covering all
  keys, remaining input, and completed nonzero keyed progress from the same
  checkpoint, process and execution attempt.
- After replacement: the exact saved checkpoint, a distinct physical process
  and execution attempt, subsequent events for every key, and first-transition
  receipts matching the saved state. Mutant reset receipts must occur once per
  key in that restored attempt and match the corresponding transitions.
- Typed marker evidence matching the retained raw process log at its recorded
  line number. Missing, truncated or contradictory provenance cannot qualify.

All raw data failures, component errors and REST observations remain visible
even when a missing proof prevents qualification. A confirmed matrix establishes
this bounded recovery case and oracle sensitivity; it does not qualify FUSE or
checkpoint-storage faults.

Offline checker tests:

```sh
python3 -m unittest discover -s tools/tests -p test_state_recovery_calibration.py -v
```

## Qualified local batch

The [2026-10-26 qualification](../../docs/STATE-RECOVERY-VALIDATION.md) confirmed
all twelve frozen attempts: three per cell, nine raw PASS results and three raw
mutant state-mismatch FAIL results. Every run retained all 6,000 IDs exactly once.
The mutant restart mismatch totals were 4,821, 4,466 and 5,083, with eight reset
receipts per attempt; all six restart runs restored their pre-kill proven
checkpoint 2. The checker returned exit code 0 and no errors. No qualification
attempt was retried or replaced. Earlier unqualified pilots and the initial
failed build remain separately retained.

This qualifies the pinned single-worker HashMap recovery slice described above.
The [FUSE prototype](../../docs/CHECKPOINT-STORAGE-FUSE-PROTOTYPE.md) has a separate
qualified Docker Desktop probe. Integrated Flink checkpoint-storage faults still
require their own implementation and qualification.
