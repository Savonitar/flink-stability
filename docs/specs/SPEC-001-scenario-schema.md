# SPEC-001 — Scenario Schema v1: Requirements

**Status:** requirements captured; **all 12 questions in §11 answered**. The v1
JSON Schema and companion specs now exist as draft implementation contracts.
Structural/resolved validation and artifact/connector-closure lock-and-bundle
preparation—including target-specific bindings and deterministic side-wide
classpath manifests—are implemented. The Testcontainers infrastructure now
copies and pre-start verifies connector manifests/JARs and provides deterministic
component lifecycle primitives. The public `run` command now implements the
first narrow bounded execution vertical: it capability-checks and prepares one
plain scenario, creates exact Kafka input evidence, runs the supported Flink
phases, establishes the terminal process fence, performs `kafka.id-set`
verification, and emits a structured result/evidence summary. The public
contract also fixes the workload protocol, omitted job defaults,
checkpoint-storage boundary, Kafka compatibility line, terminal write fence,
and validation deadlines. Broader v1 execution capabilities, runtime OCI-digest
capture, the complete R8.2 replay report, and health/repetition/experiment verdict
logic remain roadmap work.

**Last updated:** 2026-08-26.

This document captures *what the scenario format must express* and why. The JSON
Schema captures structural constraints; this requirements document remains the
normative design rationale and semantic contract. Requirements are numbered so
later work can cite them.

## Non-goals

- Not a complete broad-v1 runner implementation. Structural loading, cross-file
  catalog checks, parameter/expectation resolution, semantic preflight, suite
  planning, artifact preparation, connector locks/bundle bindings, and connector
  classpath preparation now implement the pre-provisioning contract. The
  Testcontainers layer has deterministic named JobManager and TaskManager slots;
  exact kill/stop/start operations; retryable cleanup; per-manager checkpoint
  storage; and connector copy, host/container checksum, manifest, and pre-start
  lifecycle verification. `run` wires those primitives for exactly one
  compiler-approved bounded plain-scenario subset and emits its machine-readable
  attempt result/evidence. Suites, experiments, repetitions, broader phase/input/
  validator capabilities, OCI-digest capture, complete replay-report assembly,
  and health/verdict processing remain roadmap work.

## Prior art consulted

| System | Relevant idea | What we take / reject |
| --- | --- | --- |
| [Jepsen](https://github.com/jepsen-io/jepsen) | Generator + nemesis + **history** + checker. The checker analyses a recorded history offline; correctness is separate from execution. | Take the separation of fault injection from oracle. Reject full history-checking for v1 — exactly-once is a set-equality property over final output, so a terminal-state oracle is adequate. Revisit if ordering properties are ever tested. |
| [Kafka Trogdor](https://github.com/apache/kafka/blob/trunk/trogdor/README.md) | Faults and workloads are *uniform tasks* with `class`, `startMs`, `durationMs`; JSON specs over a coordinator/agent REST split. | Take uniform treatment of faults and workloads as schedulable things. **Reject `startMs` wall-clock scheduling** — the plan requires observable triggers, not timers. Trogdor's time-based model is exactly what produces flaky race-window tests. |
| [Chaos Mesh](https://chaos-mesh.org/docs/next/create-chaos-mesh-workflow/) / [LitmusChaos](https://docs.litmuschaos.io/docs/user-guides/construct-experiment) | Kubernetes CRD shape (`apiVersion`/`kind`/`metadata`/`spec`); workflows nest serial and parallel steps; Litmus splits `ChaosExperiment` (definition) / `ChaosEngine` (binding) / `ChaosResult` (outcome). | Take the `format`/`kind`/`meta` envelope and the definition/binding/result split. Reject nested parallel orchestration in v1 — sequential phases are enough and parallel steps make triggers ambiguous. |
| [Antithesis / FoundationDB DST](https://antithesis.com/docs/resources/deterministic_simulation_testing/) | Full determinism (clock, scheduling, randomness) so any bug replays exactly from a seed. | Cannot be reached with Docker containers and real Kafka. Take the *goal*: record everything needed to replay a run — seed, images by digest, parameters, artifact checksums. Accept that replay is best-effort, and say so rather than implying determinism we do not have. |

---

## 1. Document envelope

- **R1.1** Every document declares `format` (schema version) and `kind`.
  v1 supports `kind: scenario`, `kind: suite`, and `kind: expected-result`; the
  envelope must leave room for additional kinds later.
- **R1.1a** A v1 runner accepts only `format: v1`. A missing, malformed, or
  unsupported format version is rejected before document dispatch or provisioning;
  a runner never attempts best-effort interpretation of a newer format.
- **R1.2** Every document has a `meta` block with a stable `name`.
  `kind: scenario` also requires a human-readable `description`. `kind: suite`
  and `kind: expected-result` may omit `description` when the surrounding file
  name and fields are already self-explanatory.
- **R1.2a** A scenario's `meta.tickets` is an optional list of neutral tracking
  references (for example `FLINK-xxxxx`). A ticket link alone does not declare
  that the scenario is a bug reproduction, negative control, or expected
  failure; that intent is expressed only by the expected-result sibling (R1.6,
  R3.5). Suite and expected-result metadata do not accept `tickets` in v1.
- **R1.3** A scenario's `meta.name` is stable forever once committed, is globally
  unique lower-kebab-case, and equals its filename basename without `.yaml`. It
  is the key used by suites and the run report. Expected-result names are derived
  as specified in R1.3a.
- **R1.3a** Every v1 scenario has exactly one expected-result sibling. For a
  scenario at `X.yaml`, its sibling is `X.expected.yaml` in the same directory.
  The sibling has `kind: expected-result`, `meta.name: X.expected`, and
  `meta.scenario: X`, where `X` is the scenario's `meta.name`. Before any
  provisioning, semantic validation rejects a missing sibling, orphan expected
  result, duplicate target, or any name/path/scenario mismatch. Recursive
  scenario discovery dispatches on `kind` and never executes `*.expected.yaml`.
- **R1.3b** An expected-result sibling has a required `default` expectation and
  optional parameter-selected `cases`. For an experiment, an expectation is a
  complete `{ baseline, candidate }` map; for a scenario without `experiment`,
  it is one complete `{ outcome, oracle?, reason? }` map. A plain `fail` still
  requires its oracle and reason (R3.5).

  A case has `when`, a non-empty map of declared parameter names to literal
  values, plus a complete replacement expectation of the applicable shape. For
  an experiment, `when` must not name a key in `experiment.varies`, because that
  value differs by side. The runner resolves parameters first, selects the single
  case whose `when` map entirely matches the effective common values, or uses
  `default` when no case matches. Document validation rejects a reference to an
  undeclared parameter, a value that fails parameter type/capability validation,
  or two cases whose shared keys do not contradict one another (an overlapping
  pair). It also rejects a case whose replacement expectation equals `default`.
  Cases are selected by parameters, never by suite-entry `as`, so the same
  scenario behaves consistently in a suite, a direct run, and a CLI-parameterized
  run.
- **R1.3c** A plain scenario's expected-result sibling therefore has this minimal
  shape; parameter-selected `cases`, when needed, replace `default` with the same
  one-outcome shape:

  ```yaml
  default:
    outcome: pass
  ```

  A plain expected failure instead declares `outcome: fail` plus its required
  `oracle` and `reason`.
- **R1.4** Unknown fields are rejected, not ignored. A typo must fail loudly
  before any container starts.
- **R1.5** One canonical format. The v1 envelope and schema in this document are
  the only accepted scenario format. The pre-v1 loader/model, transitional
  `setup:`/`nemesis:` sketch, and dead `chaos` / `validate` fields are deleted;
  the runner never guesses, aliases, or translates an obsolete document.
- **R1.5a** The implementation has no backward-compatibility command, runtime,
  or workload-artifact branch. `run` means the v1 execution path; the old
  loader/runner, Confluent-based Kafka harness, Flink 1.19 workload, and shaded
  legacy JAR are not retained. A future compatibility layer requires an
  explicit new decision and tests rather than dormant adapters in production.

### Bug reproductions imply an expectation

- **R1.6** A scenario that intentionally reproduces a known bug declares that
  intent through an **inverted expectation** in its expected-result sibling: it
  must *fail* against the affected version and *pass* against the fixed one. A
  linked ticket may explain the history, but the expectation is the contract.
- **R1.7** Bug-reproduction scenarios with pinned expected failures are the
  strongest available negative controls, because they catch bugs the oracle was
  not designed to catch. If the behavior corresponds to a real upstream issue,
  list that issue in `meta.tickets` for traceability. They belong in the
  `selftest` suite alongside the synthetic controls, not only in the regression
  suite.

---

## 2. Parameterization

**Purpose.** `parameters` lets one scenario file run under many configurations
without copy-pasting it. Values come from three places: a submit-time CLI
override, a suite binding at membership (Q6), or `experiment.varies` splitting a
run into baseline and candidate (§3).

It is not only convenience. **The differential experiment model depends on it**:
`experiment.varies: [flink_state_backend]` requires `flink_state_backend` to be a
named thing that can hold different values. If the backend were hardcoded in
`setup`, there would be nothing to vary, and the baseline/candidate mechanism
could not exist.

- **R2.1** A scenario declares named `parameters` with types and defaults. The
  same file then runs under many configurations without editing. A scenario with
  no `parameters` block is simply concrete, and that is valid.
- **R2.2** Parameters are overridable at submit time. Submitting a run with
  different parameters must not require changing the scenario spec.
- **R2.2a** **An override naming an undeclared parameter is an error**, reported
  before any container starts — never silently ignored. Ignoring
  `--parameter kafka_timeout=30s` because no such parameter exists would leave the
  operator believing they tested a configuration they did not: the same
  false-pass hazard this specification guards against everywhere else. The error
  names the unknown key and lists the declared ones.
- **R2.3** Parameters are referenceable in value positions across the document
  (images, versions, backend types, job options, topic settings). A whole-scalar
  template such as `${input_record_count}` preserves the resolved parameter type;
  embedded templates are allowed only inside strings, such as
  `flink:${flink_version}` or `input-${suffix}`. JSON Schema checks the raw
  document shape, and semantic validation checks the resolved value shape; for
  example `topic: input-${suffix}` is accepted structurally but the resolved
  value must still be a valid Kafka topic name. Map keys and scenario-local
  aliases are not parameterizable, because references must be statically
  discoverable before interpolation.
- **R2.4** The **effective, fully-resolved** parameter set is recorded in the run
  report. A report that shows only the template is not reproducible.
- **R2.5** A parameter declares a **name, type, and default** — or `required: true`
  in place of a default, for values with no safe fallback
  (`flink_connector_artifact` for PR gating cannot default to anything without
  silently testing the wrong thing).
  Types are about the *shape* of a value: `string`, `integer`, `boolean`,
  `duration`, with bounds such as `min`/`max` only where they apply. `enum` is
  not a v1 parameter type; supported named values are harness capabilities. A
  parameter may include a human-readable `description`, but descriptions are
  optional and never required for schema validity.
- **R2.6** **A scenario does not enumerate legal values for a parameter.**
  Validity is a property of the harness, not of a scenario. The harness owns a
  **capability registry** of supported values for typed semantic fields: state
  backends, delivery guarantees, Kafka transaction ID naming strategies, and
  restore modes. It validates the fully resolved field values before any
  container starts. Parameter names should describe what they configure, but
  capability validation is based on where the resolved value is used, not on a
  magic parameter name. Rationale:
  - For a parameter named in `experiment.varies`, a domain is redundant: the
    experiment already states both values explicitly.
  - For any other parameter, a scenario-local list is a stale copy of the
    harness's own capability list, and every scenario author would have to
    maintain it.
  - It yields a better error. Not "not in [hashmap, rocksdb]", which reads as
    arbitrary, but "harness does not support state backend 'forst'" — naming the
    real problem and the real fix.
  Parameters with no registry entry (`input_record_count`,
  `processing_delay_ms`) are validated by type alone.
- **R2.7** Resolution order, from lowest to highest precedence, is: scenario
  default, suite binding, submit-time CLI override, then experiment-side
  override. The runner records the resulting effective values for each side.
  A submit-time parameter override on a suite is global and strict: it is
  applied to every entry, and the suite is rejected if any referenced scenario
  does not declare that parameter. v1 has no transient alias-qualified override;
  entry-specific configurations belong in committed suite bindings.
- **R2.8** An experiment side may override parameters only, never arbitrary
  document paths. It may override only keys named in `experiment.varies`; an
  unlisted key is a scenario-definition error before provisioning.
- **R2.9** A submit-time override naming a key in `experiment.varies` is
  rejected. It would redefine the comparison while the expected-result document
  continued to describe the committed one.
- **R2.10** The runner compares fully resolved baseline and candidate
  configurations, excluding generated per-run isolation names, as a defence
  against implementation error. A difference outside `experiment.varies` is an
  error before provisioning.
- **R2.11** A `duration` is a positive integer followed by exactly one of `ms`,
  `s`, `m`, or `h`. Decimals, compound forms, and ISO-8601 durations are rejected.

> **Note — parameterization is core to v1.** Differential experiments (§3)
> require parameterization. What stays out of v1 is the automatic *expansion* of
> a matrix into many runs (R9.2), not parameterization itself.

---

## 3. Differential experiments (the golden-config model)

**The central idea in this specification.** A single run tells you a
configuration passed or failed. It does not tell you *what caused* the failure,
and it cannot distinguish a real defect from a broken harness. A controlled pair
can do both.

The model: state a claim that must hold, then run two configurations that differ
only in declared dimensions — a known-good **baseline** and the **candidate**
under test.

### What `baseline` is for

**R3.0a** The baseline is a known-good reference that establishes the systematic
validity of the scenario, workload, and oracle for a comparison. It is not a
measurement of instantaneous environment health; same-attempt health signals
(R8.4) perform that role. A configuration does not "graduate" to baseline merely
by being proven correct — a long, stable pass history is what makes a baseline
failure informative.

### Two scenario shapes: diagnosis and universal invariant

**R3.0b** `experiment:` is optional (R3.1), and its absence is meaningful.

| | Diagnosis | Universal invariant |
| --- | --- | --- |
| Claim | "I suspect `hashmap` breaks EOS" | "EOS holds for every supported backend" |
| Structure | baseline + candidate pair | N runs, one per value |
| Expected | candidate fails with a pinned reason | all pass |
| `experiment:` block | yes | **no** |
| Source of reference | the designated baseline | the other N−1 runs |

The last row matters: **with three or more runs differing in a single key, the
reference is free.** One failure among two passes proves the apparatus was healthy;
all three failing points at the environment. No designated baseline is needed, so
a universal-invariant scenario needs no new schema — it is a plain scenario plus
suite bindings (§10.3).

Use `experiment:` only when one side is genuinely suspect. Reaching for it to
express "these should all work" produces an arbitrary asymmetry that means
nothing.

**R3.0c** When a diagnosis experiment's candidate starts passing — the bug is
fixed — the scenario **changes kind**. It does not promote the candidate to
baseline; the expected outcome in the sibling `.expected.yaml` flips from `fail`
to `pass`, and the scenario becomes a regression guard. That flip is a
regression-contract change and must be reviewed as one: someone confirms the bug
is genuinely fixed rather than merely not reproducing today. Keeping expectations
in a separate committed file (Q5) is what makes this visible as its own diff.

### Naming: `baseline` and `candidate`, not `control` and `treatment`

**R3.0** The two sides are named `baseline` and `candidate` in the schema.

"Control" and "treatment" come from experimental design — in a clinical trial the
control group gets a placebo and the treatment group gets the drug. The concept
transfers exactly; the vocabulary does not. Two problems with it here:

1. "Treatment" reads as *the broken one*, which is wrong. In connector PR gating
   both sides are expected to pass — the candidate is simply the side under test,
   not the side expected to fail.
2. The plan already says "baseline" and "candidate" for PR gating. Two vocabularies
   for one mechanism (R3.9) is one too many.

Where this document says control/treatment conceptually, the schema field names
are `baseline` and `candidate`.

- **R3.1** A scenario may declare an `experiment` with a human-readable `claim`
  — the invariant asserted to hold regardless of the varying dimension. Example:
  *"exactly-once holds regardless of state backend."* The claim may interpolate
  pair-common parameters (for example, `EOS holds on Flink ${flink_version}`),
  and the report records the resolved claim. It must not interpolate a key in
  `experiment.varies`, because the pair has no single effective value for that
  key; the varying dimension belongs in universal prose such as “regardless of
  state backend.”
- **R3.2** The experiment declares `varies`: the set of parameter keys permitted
  to differ between baseline and candidate. A set, not necessarily one key.
- **R3.3** Every parameter outside `varies` is identical across the pair by
  construction (R2.8). The resolved-configuration comparison in R2.10 catches
  any implementation error before execution.
- **R3.4** Each side declares only its **parameter overrides**. Expected outcomes
  live in the sibling expected-result document (Q5), which is authoritative;
  a scenario file never states what it expects to happen.
- **R3.5** **An expected failure must pin *how* it fails**: the oracle that must
  report it and the reason code. A bare `expect: fail` is forbidden — a run that
  fails because an image pull timed out would otherwise count as a success. This
  is the same false-pass hazard the plan guards against elsewhere.
- **R3.6** For an experiment's selected expectation (R1.3b), the baseline's
  expected outcome is always `pass`; its entry must not contain an oracle or
  reason. If it mismatches, the experiment is `inconclusive`, because the
  scenario or its measuring apparatus is invalid.
- **R3.7** Any clean candidate outcome that differs from its declared outcome,
  oracle, or reason code is a `fail`, never a pass. An expected failure that
  passes reports `expectation-not-reproduced`; one that fails through another
  oracle or reason reports `unexpected-failure-mode`; an expected pass that
  fails reports `unexpected-pass-failure`.
- **R3.8** The **pair is the unit of evidence** in the run report. Report the
  claim, the varying keys, both outcomes, and the verdict — not two unrelated
  runs the reader must correlate.

### This subsumes PR gating

Connector pull-request gating is the same mechanism with one axis:

| Use case | `varies` | Baseline | Candidate | Expected |
| --- | --- | --- | --- | --- |
| Connector PR gate | `flink_connector_artifact` | pinned released artifact | PR build artifact | both pass |
| State-backend independence | `flink_state_backend` | `rocksdb` | `hashmap` | baseline passes, candidate fails with a pinned reason |
| Flink version regression | `flink_version` | known-good version | candidate version | both pass |
| Bug reproduction (R1.6) | `flink_connector_artifact` | fixed artifact | affected artifact | baseline passes, candidate fails as pinned |

- **R3.9** There is **one** differential mechanism. Connector PR gating is not a
  separate feature; it is this mechanism with
  `varies: [flink_connector_artifact]`. Implement it once.

### Boundary: claims that are not universal

- **R3.10** Some differences are legitimately by design — not every backend,
  version, or system can substitute for another. The `claim` field exists to
  force the author to state *why* the invariant should hold universally before
  asserting it. A claim that cannot be stated plainly is probably not a real
  invariant, and the scenario is measuring a design decision rather than a bug.

---

## 4. Setup / infrastructure

- **R4.1** Infrastructure is **declared**, not created by steps. Steps act on
  components that already exist in the declaration, so references can be resolved
  and validated before anything starts.
- **R4.2** `setup.kafka.clusters` is a required non-empty map of one or two
  clusters; its key count is the cluster count. Every cluster requires `image`,
  `brokers`, and `topics`. `mode` is optional and defaults to `kraft`; v1 accepts
  only `mode: kraft` when it is declared. `brokers` is an integer of at least
  one. Other topology modes are rejected before provisioning as
  `unsupported-capability`, never coerced to KRaft.
- **R4.2a** The first executable v1 broker capability is **Apache Kafka
  `4.0.x` only**. Every resolved setup image and explicit Kafka restart image
  must be an exact official `apache/kafka:4.0.<patch>` tag, optionally followed
  by `@sha256:<64-lowercase-hex>`; `3.x`, `4.1.x`, later lines, and a reference
  whose broker version cannot be established reject before provisioning with
  `runner.kafka.image-version-unsupported`. Parameter interpolation does
  not defer this check past resolved semantic preflight. Adding another broker
  line requires an explicit capability-registry entry and integration coverage;
  a permissive image string is not a compatibility assertion.
- **R4.2b** The v1 format continues to express the broker/topic counts in R4.2
  and R4.3, including valid multi-broker scenarios. The first narrow executable
  runner is a capability subset: it accepts exactly one broker and therefore
  requires every topic's `replication_factor` to be `1`. It rejects wider valid-v1
  topology before artifact preparation or Docker with
  `runner.kafka.broker-count-unsupported` or
  `runner.kafka.replication-factor-unsupported`; it never rewrites the scenario.
- **R4.3** Topics are declared **under their cluster** — with two clusters, a
  bare `input_topic` is ambiguous. Every topic requires `name`, `partitions`, and
  `replication_factor`; both numeric values are positive integers and replication
  factor cannot exceed the cluster's broker count. Resolved topic names are
  unique within a cluster; interpolation that makes two declarations equal is
  rejected before provisioning. Relevant transaction settings are explicit
  rather than implicit.
- **R4.4** Input data generation is declared on Kafka topics with
  `input_source`, not hardcoded. The field names the harness-owned producer or
  feeder for that topic; a topic named `input` is just a topic name. The runtime
  must derive its input manifest and oracle expectation from the resolved source;
  it must not contain a fixed record count coupled to a particular topic name.

### Input modes

- **R4.5** Four `input_source.mode` values, with v1 support as marked:

  | Mode | Required fields | Description | v1 |
  | --- | --- | --- | --- |
  | `file` | `format`, `path` | User supplies a file (CSV/JSON-lines) fed into the source topic. | yes |
  | `generated` | `format`, `total` | Generate exactly `total` unique records before the workload starts. The generated set becomes the expected output set. | yes |
  | `generated-rate` | `format`, `total`, `rate_per_second` | Generate up to `total` unique records at a bounded rate. Production may continue *during* the chaos phase. | yes |
  | `custom` | `artifact`, `timeout`, optional `config` | User-supplied executable JAR, for proprietary formats or forks. | yes, with a custom validator (R4.10, R7.5) |

  “v1” in this table is the schema contract, not a claim that the first narrow
  execution vertical wires every mode at once. That first vertical accepts only
  preload-bounded `generated` input in `integer-sequence` format, with a positive
  `total` no greater than 1,000,000, so final partition offsets exist before job
  submission and its temporary in-memory manifest remains bounded. A larger
  resolved total is rejected before artifact preparation or Docker with
  `runner.input.total-unsupported`; the broad v1 schema does not impose this
  narrow runner's 1,000,000-record cap. The cap may be lifted only after manifest
  and validator evidence become streaming or spill-backed. `file` and custom
  preload feeding remain later v1 capabilities. `generated-rate` remains bounded
  because `total` is mandatory,
  but its concurrent cutoff delivery is also later; the narrow runner rejects
  every unsupported mode explicitly before provisioning rather than omitting
  R5.6c's stopping offsets.

  v1 allows at most one `input_source` per scenario. A job source topic used
  with a built-in terminal oracle must be the topic that declares it; otherwise
  the runner has no expected-data artifact to compare against. Future
  multi-source scenarios require explicit manifest selectors before more than
  one input manifest can be accepted. When that exists, the run report records
  one input manifest per `(cluster, topic)` pair.

- **R4.5a** Built-in input-source lifecycle is harness-owned in v1, not driven by
  scenario steps. `file` and `generated` are preload modes: the runner finishes
  writing the bounded input and closes the manifest before auto-started workload
  jobs are submitted. That preload snapshot supplies the exact per-partition
  exclusive stopping offsets required by the first executable workload protocol
  (R5.6c). `generated-rate` is a concurrent bounded mode in the v1 schema: the runner
  starts it when phase execution begins, records producer start/end timestamps,
  and waits for it to finish before terminalization. Its execution wiring belongs
  after the first narrow preload vertical because its final per-partition offsets
  are not known at job submission. Scenario authors choose `total` and
  `rate_per_second` large enough for the producer to overlap the phases they
  intend to stress. Any input source may declare
  `connect_via_proxy: <setup.proxies alias>` when the scenario intentionally
  faults the harness-owned producer path; otherwise input producers connect
  directly to the cluster.

- **R4.6** **Continuous production and unbounded input are different things, and
  only the first is needed for v1.** A scenario that produces 100 000 records at
  200/second is *continuous* — records flow throughout the fault window — while
  the total remains known in advance, so the exact-output oracle still works.
  Genuinely unbounded input (no total) is what breaks the oracle.
- **R4.7** **Truly unbounded input is out of scope for v1** and its `mode` value
  is not accepted, per the review's argument that a schema which accepts a mode
  the executor rejects is misleading. It requires an acknowledged-event ledger,
  a start/stop protocol, and a ledger-based oracle. Bounded-but-rate-limited
  input (R4.6) covers the initial EOS and recovery scenarios.

  A future controlled-unbounded extension will not weaken terminal isolation. It
  must stop/pause the producer, establish and record a finite input cutoff, issue
  Flink stop-with-savepoint with drain, wait for the asynchronous operation and
  every submitted job to become terminal, and then apply R7.1c's process fence.
  Forced cancellation is only a fallback: it can establish a write fence, but it
  cannot prove that every event before the intended cutoff was processed, so a
  whole-stream completeness oracle cannot pass from that evidence. External
  sources that cannot be paused require an explicit finite observation-window
  contract. None of these unbounded forms is accepted or executable in v1.
- **R4.8** The built-in input producer is a **harness-owned component, separate
  from the Flink workload under test.** In v1 it is idempotent but
  **non-transactional**: it runs with `enable.idempotence=true`, assigns a
  logical event ID before the first send, and retains that ID across retries. Its
  input manifest records the event ID, payload hash, every producer attempt,
  partition, offset, broker-ack outcome, and terminal disposition. This is the
  expected-data artifact the terminal oracle compares against. Here a producer
  attempt means one harness-issued producer-API send invocation. Kafka-client
  transport retries inside that invocation are opaque: the harness neither
  observes nor invents them. The first narrow runner currently issues exactly one
  such invocation per generated ID and records it as one-based attempt `1`.
- **R4.9** For bounded modes the manifest is verified complete before terminal
  write fencing or the correctness verdict. For `generated-rate`, production may
  still be in flight during chaos; terminalization waits for the producer to
  finish and its manifest to close.
- **R4.10** **Pluggable input implies a pluggable oracle.** If a scenario supplies
  `input_source.mode: custom`, the harness cannot know what correct output looks
  like, so the scenario must also supply a custom validator (R7.5). Rejecting a
  custom producer with built-in oracles is a semantic validation error, not a
  runtime surprise. Custom input is not eligible for `expected: input-manifest`
  and does not inherit R4.8 or R4.14–R4.18's manifest guarantees. Its selected
  expectation may name only its custom terminal validator; the run report records
  `input_contract: custom-unverified`.
- **R4.10a** A harness-owned `file`, `generated`, or `generated-rate` input
  requires a top-level `kafka.id-set` validator. Its target must be the sink of
  exactly one declared job whose source is the sole input-source topic; this is
  the v1 input-manifest lineage. Supplemental custom terminal validators are
  allowed. A custom input instead permits only custom terminal validators.

### Flink and images

- **R4.11** `setup.flink.image` is required. `jobmanagers` and `taskmanagers`
  are optional and default to 1; both fields, when declared, are integers of at
  least one. The run report records the resolved counts. Scenarios whose
  behavior, targets, or recovery properties depend on multiple TaskManagers must
  declare the intended count explicitly.
- **R4.12** JobManager count greater than 1 requires Flink HA services
  (ZooKeeper or Kubernetes HA backend, HA storage, leader election). v1 permits
  only 1, so an omitted `jobmanagers` resolves to 1 and a higher declared value
  is rejected with `unsupported-capability: multiple-jobmanagers` before
  provisioning rather than silently starting a broken cluster.
- **R4.13** Every v1 artifact has a **declared reference** (image reference,
  Maven coordinate where permitted, or local path) *and* a **resolved identity**
  (OCI digest or file SHA-256) recorded in the run report. `git ref` is not a v1
  artifact-reference form; repository/ref support requires an explicit future
  source-resolution contract.
  - Local references resolve against an explicit invocation artifact root, never
    against the scenario file's directory. The CLI defaults that root to its
    working directory and exposes `--artifact-root`. A file input must name one
    exact regular file. A JAR may instead use a build-output glob only in its
    final filename component; zero or multiple regular-file matches reject.
    Relative traversal that escapes the root, absolute paths outside the root,
    and symlinks that escape the root reject. v1 does not expand `~`, environment
    variables, URLs, or shell syntax.
  - The v1 Maven form is
    `maven:<groupId>:<artifactId>:<release-version>`. It is permitted for a
    subject connector and for an entry in any connector's explicit
    `runtime_dependencies`; other artifact roles remain local-only. Classifiers,
    packaging, repository URLs, version ranges, `LATEST`, `RELEASE`, and
    snapshots are rejected. Online preparation checks the local Maven cache and
    then the fixed HTTPS Maven Central repository; offline preparation reports an
    explicit cache miss. It never shells out to Maven or reads Maven
    settings/credentials.
- **R4.13a** A Maven subject connector with no `runtime_dependencies` uses
  **auto mode**: preparation resolves its primary JAR and a locked runtime closure
  from its effective Maven model. In **explicit mode**—the field is present for
  either a Maven or local primary—the primary contributes only its own JAR and
  the declared entries are the dependency roots. Each Maven dependency root then
  contributes its own JAR and locked runtime closure; a local dependency root
  contributes exactly its selected JAR. In either Maven traversal, the closure
  contains selected JAR dependencies reachable through `compile` or `runtime` scope.
  `provided`, `test`, `system`, and `import` artifacts are not runtime entries;
  optional transitive edges and declared exclusions are not followed. Parent and
  imported-BOM POMs may influence the effective model but are descriptor evidence,
  not classpath entries. A selected non-JAR runtime artifact, unresolved property,
  version range, dynamic version, or snapshot anywhere in the selected closure
  rejects before provisioning. Effective-model JDK profile activation uses the
  canonical Java 21 model properties regardless of the host JVM running preparation;
  this both evaluates published Flink parent POMs and prevents a host-JDK upgrade from
  silently changing the locked closure. Maven settings, user properties, and host
  environment variables do not participate.
- **R4.13b** Maven conflict mediation and classpath order are deterministic.
  The conflict key is `groupId:artifactId:extension:classifier` (empty classifier
  is significant). The nearest declaration wins; equal-depth conflicts use the
  first encounter in breadth-first traversal. Siblings retain effective-POM
  declaration order. In explicit mode, the declared `runtime_dependencies`
  order supplies the root order. The subject connector is
  classpath entry zero; selected dependencies follow in first-encounter order.
  An already-selected identical conflict key/version or local SHA-256 is emitted
  once and retains its first position. Every omitted conflict candidate and its
  winning entry is recorded; a result must never depend on filesystem listing or
  download-completion order.
- **R4.13c** Preparation creates immutable **target-specific connector closure
  locks** before provisioning. The resolved connector graph and staged bytes may
  be shared, but one lock is emitted for every `(effective side, declared
  connector alias, distinct target Flink image reference)` in the resolved
  scenario; after R5.6 rejects unused declarations, R5.6a builds the deployment
  union from those job references. The valid target references are obtained by
  applying R6.11a's deterministic component-image state rules to
  `setup.flink.image` and the ordered restart steps. Reusing the same exact
  reference reuses its lock; a valid restart without
  a new image creates no target, while an ambiguous inheritance rejects instead
  of guessing. An upgrade from `flink:2.2.0` to `flink:2.2.1` therefore produces
  two locks for one unchanged connector closure. All such locks and their hashes
  exist before the attempt starts its first container.

  The full lock records the connector alias and declared primary reference;
  exact resolved target Flink image reference; dependency mode (`auto` or
  `explicit`); every runtime entry's classpath index, origin root, canonical
  Maven identity or canonical local source, source and staged paths, Maven
  dependency path, and lowercase SHA-256; every consulted Maven POM's canonical
  identity, source path, and SHA-256; and every conflict decision. An origin root
  is either `primary` or `runtime_dependency` with the original zero-based
  `runtime_dependencies` array index. In auto mode the connector primary and all
  its transitives have origin root `primary`; being a transitive classpath entry
  does not turn that origin into a nonexistent explicit dependency root.
  If identical local or Maven-selected bytes coalesce within one closure and the
  retained prepared artifact has multiple origins matching the same effective
  side, the lock's singular stable origin is the first matching origin in
  R4.13b's deterministic encounter order. Valid coalescing does not reject as
  ambiguous; full prepared and report evidence retains every matching origin.

  Its lowercase `closure_sha256` is SHA-256 over the exact UTF-8 JSON bytes of
  the versioned stable projection
  `flink-stability.connector-closure-lock/v1`. The projection contains:

  - top-level `format: flink-stability.connector-closure-lock/v1`;
  - connector alias, declared primary reference and primary SHA-256;
  - the exact resolved target Flink image reference and dependency mode;
  - ordered classpath indexes, structured canonical Maven identities (including
    `group_id`, `artifact_id`, `extension`, the significant possibly-empty
    `classifier`, and `version`) or local kind, origin-root kind/index, and entry
    SHA-256 values;
  - consulted descriptor identities and SHA-256 values sorted by canonical Maven
    identity; and
  - mediation decisions in deterministic encounter order, including loser and
    winner identities, `nearest` or `first_breadth_first` reason, depths, origin
    roots, and Maven dependency paths.

  Object keys are lexicographically sorted, there is no insignificant whitespace,
  and arrays retain the semantic order stated above. A local entry's stable
  identity is its `local` kind plus its recorded content SHA-256; its canonical
  host path is report evidence, not replay identity. The projection excludes the
  scenario side/scope, JSON declaration pointers, host-specific source and staged
  paths, timestamps, origin declaration references other than the separately
  required primary reference, entry depths/dependency paths outside mediation
  decisions, and the runtime-resolved OCI digest.

  The target image value is the exact post-interpolation declaration string; the
  runner does not normalize equivalent Docker spellings before hashing. If that
  declaration itself contains `@sha256:...`, the digest is part of the string and
  therefore part of the lock hash. A digest resolved later from a tag is not: at
  runtime the report associates each lock with that independently resolved digest.
  Docker-free validation can consequently create and verify the same locks
  without Docker, while the run report still distinguishes two executions where
  a tag resolved to different image bytes. Execution consumes only staged bytes
  named by the lock. Before any prepared connector classpath is accepted, every
  staged JAR is rehashed; a missing, unreadable, or mismatching entry rejects
  before a container starts.
  Re-preparation may reuse cached bytes only after recomputing and matching their
  recorded digest.
- **R4.13d** File inputs, workload JARs, connector primary/dependency JARs, and
  custom-extension JARs resolve and receive SHA-256 identities before
  provisioning. Preparation copies their bytes into a private per-validation
  content-addressed staging tree under the artifact root; execution consumes the
  staged path, not the mutable source path. Reports retain both the canonical
  selected source path and the staged execution path. The prepared scenario/suite
  plan owns this tree until closed. An executing `PreparedScenarioPlan` and its
  private staging tree remain open through host re-verification, copy, and
  pre-start verification for **every** initial and replacement Flink container;
  execution closes them only during attempt teardown, after no later lifecycle
  step can construct another replacement. Closing after initial construction
  would invalidate a later restart's immutable source bytes and is forbidden.
  Validate-only commands close the plan before returning, and failed preparation
  removes its private tree before reporting the failure.
  Workload and custom-extension JARs require a selected source filename ending in
  `.jar` (case-insensitive), a readable JAR whose entries fully decompress and
  pass integrity checks, and `Main-Class`. A workload JAR additionally declares
  `Flink-Stability-Workload-Protocol: v1` in its main manifest and implements
  R5.6c. Immediately before REST upload, the runner copies the staged workload
  into a private single-call upload snapshot without following a final symlink,
  rehashes that snapshot against the prepared identity, synchronously uploads the
  same snapshot, and deletes it. Mutation or replacement of the staged path after
  snapshot creation therefore cannot change the uploaded bytes. The runner
  accepts any JAR satisfying that byte-bound contract, not only the bundled
  generator. A missing, duplicate, blank, or unsupported attribute rejects the
  staged artifact before provisioning. Connector primary/dependency JARs also
  require the `.jar` suffix and complete archive integrity but need not be
  executable. Preparation assumes the artifact root is runner-owned and is not
  adversarially renamed while it is being inspected; concurrent hostile
  filesystem mutation is outside v1's threat model. The selected source is
  nevertheless opened without following a final symlink before its bytes are
  copied.
  A tag like `flink:2.2.0` is not by itself a pinned image. Docker-free validation
  checks the declared file/Maven artifacts but deliberately leaves OCI digest
  resolution to runtime preparation. Protected CI requires the runtime-resolved
  digests. The Kroxylicious image is a harness-pinned capability in v1 because
  `setup.proxies` has no public image field; its configured reference and resolved
  digest are still recorded by the runner.
- **R4.14** A generated-input topic uses `cleanup.policy: delete`, never
  compaction, and retains data for longer than the maximum attempt duration.
  This makes the manifest's reconciliation snapshot observable.
- **R4.15** To close a manifest, the producer flushes and closes, then the runner
  captures the log-end offset of every input partition. It consumes from the
  attempt's recorded starting offsets through those bounds, using v1's fixed
  `read_uncommitted` isolation level because the built-in producer is
  non-transactional (R4.8), and records the bounds, consumer configuration, and
  observed event IDs in the manifest.
- **R4.16** An event is `present` only if its stable ID appears in that bounded
  snapshot. It is `absent` only if the producer recorded a definitive,
  non-retriable rejection and the snapshot does not contain the ID. A successful
  producer acknowledgement whose stable ID is absent from the snapshot is
  `acknowledged-missing`, recorded with reason
  `input-manifest.acknowledged-missing`. Every other unresolved event is
  `indeterminate`.
- **R4.17** The expected output set is exactly the set of `present` events, not
  the requested record count. Any `indeterminate` or `acknowledged-missing` event
  makes the attempt `inconclusive`; the latter's distinct reason preserves the
  evidence of a possible broker or harness defect.
- **R4.18** The manifest records the captured offsets and reconciliation evidence
  as run artifacts so a later reader can distinguish a source-side rejection from
  downstream loss. Its evidence status is `complete` only when every expected ID,
  consumer configuration, and captured bound is reconciled. Once acknowledgements
  and closed bounds exist, acknowledged-missing or incomplete reconciliation
  retains an immutable `partial` manifest with the available per-ID and
  reconciliation evidence. This includes a reconciliation consumer that fails
  while polling, querying positions, or waiting on the stage deadline: the IDs it
  observed so far, the captured bounds, and its configuration are retained,
  unobserved IDs are `indeterminate`, and the consumer failure stays primary
  (`infrastructure.kafka-input-setup-failed`). A failure before closed bounds and
  reconciliation configuration exist carries no partial manifest; the runner must
  not fabricate evidence that was never observed. A consumer or Admin close failure after a
  reconciliation snapshot or manifest exists makes the attempt infrastructure-
  inconclusive but does not erase those facts: the failure retains the manifest
  with its observed `complete` or `partial` evidence status. When a substantive
  input-manifest failure already exists, it remains primary and the close failure
  is supplemental.
- **R4.18a** The first runner gives the whole Kafka input-preparation stage one
  fixed, internal `2m` monotonic deadline. The clock starts immediately before
  opening the Kafka client boundary and does not reset for topic
  creation/verification, generated sends and acknowledgements, producer close,
  end- and beginning-offset capture, reconciliation, consumer/Admin close, or
  final completion. Each operation receives only the remaining budget. This is
  an infrastructure/setup deadline, not a scenario field and not a terminal
  validator deadline. If it expires, the runner returns the structured
  `inconclusive` outcome `infrastructure.kafka-input-setup-failed`; it retains
  every already-observed input manifest as truthful `partial` or `complete`
  evidence. Because this occurs before Flink starts and before any write fence,
  it is distinct from the post-fence, fail-closed `verification.*` outcomes in
  R7.1c and R7.3b.

---

## 5. Workload / job configuration

- **R5.1** The job's options are first-class scenario fields, not opaque args:
  delivery guarantee, Kafka transactional-ID prefix, Kafka transaction ID naming
  strategy, Kafka transaction timeout, parallelism, checkpoint interval and mode,
  checkpoint storage, state backend, state TTL, watermark settings, restart
  strategy. In v1 these live on `workload.jobs[]` and its nested `source`,
  `sink`, and `checkpointing` fields: `parallelism`, `state_backend`,
  `state_ttl`, `watermarks`, `restart_strategy`, `sink.delivery_guarantee`,
  `sink.transactional_id_prefix`, `sink.transaction_id_naming_strategy`,
  `sink.transaction_timeout`, `checkpointing.interval`, `checkpointing.mode`,
  and `checkpointing.storage`.
- **R5.1a** Omission is deterministic. Before execution, the resolver
  materializes these effective values for every job:

  | Omitted field | Resolved value |
  | --- | --- |
  | `state_backend` | `hashmap` |
  | `state_ttl` | `{ enabled: false }` |
  | `watermarks` | `{ strategy: no-watermarks }` |
  | `checkpointing.storage` | `{ type: jobmanager }` |
  | `restart_strategy` | `{ type: flink-default }` |

  `flink-default` is a stable harness marker meaning that the runner supplies no
  job-specific restart override and lets the registered Flink `2.2.x` line
  select its checkpointing-aware default. It does not freeze a version-independent
  copy of Flink's exponential-backoff sub-options. The report records declared
  omissions, every materialized value, the effective Flink line, and the
  runtime-effective restart type/options. Defaults are never merely schema
  documentation that disappear from execution evidence.
- **R5.2** Arguments pass through as a **structured list** end to end. No joining
  into a string and re-splitting.
- **R5.2a** `program_args` remains a structured list for job-specific settings
  outside the harness model. An argument conflicting with a typed canonical
  workload option is a semantic error, never a silent override. v1 reserves
  tokens beginning `--flink-stability.workload.` and the known bootstrap-override token
  `--bootstrapServers` (both its separate-value and `=value` spellings).
  Every other non-empty token is preserved byte-for-byte and in declaration
  order; the runner does not interpret workload-specific arguments such as the
  bundled generator's `--processingDelayMs`.
- **R5.3** Job options are parameterizable per R2, because several of them *are*
  the dimension under test — delivery guarantee is how `selftest-duplicates` is
  built; state backend is the §3 example.
- **R5.4** Kafka transaction ID naming strategy is **mandatory and explicit** for
  exactly-once scenarios. It changes what a correct outcome is (see the plan's
  section on `INCREMENTING` vs `POOLING`), and inheriting the connector default
  silently would let a connector PR change what the suite tests. The canonical
  workload field is `transaction_id_naming_strategy` under the Kafka sink.
  `transactional_id_prefix` and `transaction_id_naming_strategy` are required
  only when the resolved `sink.delivery_guarantee` is `EXACTLY_ONCE`; they are
  invalid for `NONE` and `AT_LEAST_ONCE`, because no Kafka transactions should
  exist in those modes. The optional `transaction_timeout` duration follows the
  same rule: it is valid only for `EXACTLY_ONCE` and defaults to `2h`. It may be
  shorter than the checkpoint interval or an expected outage; that is how a
  scenario expresses the documented loss when a transaction times out before its
  commit, which is itself a negative-control shape.
- **R5.4a** First-class workload option shapes are intentionally small in v1:
  - `checkpointing.storage` is an object, never a scalar:
    `{ type: jobmanager }` or `{ type: filesystem }`. Filesystem storage is
    harness-owned: the runner assigns
    `file:/flink/checkpoints/attempt-<ordinal>-<nonce>/<job-alias>`, where the
    ordinal/nonce are generated for the isolated attempt and the final component
    is the stable job alias,
    mounts the attempt storage into every JobManager and TaskManager, and records
    the effective URI. There is no public `path` field in v1; an author-supplied
    path, `file:` URI, distributed-filesystem URI, or other arbitrary URI is
    rejected rather than trusted. The storage object itself is not parameterized
    as `${storage}` in v1, although its `type` discriminator may be a whole-scalar
    template resolved before provisioning.
  - `state_backend` is `hashmap` or `rocksdb`, or a whole-scalar template that
    resolves to one of those values.
  - `state_ttl` is `{ enabled: false }` or
    `{ enabled: true, ttl: <duration>, cleanup?: none|incremental|rocksdb-compaction-filter }`.
  - `watermarks` is `{ strategy: no-watermarks }`,
    `{ strategy: monotonic-timestamps, idleness?: <duration> }`, or
    `{ strategy: bounded-out-of-orderness, max_out_of_orderness: <duration>, idleness?: <duration> }`.
  - `restart_strategy` is `{ type: none }`,
    `{ type: flink-default }`,
    `{ type: fixed-delay, attempts: <positive-int>, delay: <duration> }`, or
    `{ type: failure-rate, max_failures: <positive-int>, failure_rate_interval: <duration>, delay: <duration> }`.
  If a discriminating field such as `state_ttl.enabled`,
  `watermarks.strategy`, or `restart_strategy.type` is itself parameterized, the
  runner validates the resolved object against the same cases before
  provisioning.
- **R5.4b** The broad v1 format retains both state backends and every restart
  shape in R5.4a. The first narrow executable runner accepts only `hashmap` and
  `{ type: flink-default }`. Because RocksDB compaction-filter TTL cleanup is
  inapplicable to HashMap state, the narrow runner rejects that cleanup mode
  rather than silently executing a no-op policy. It rejects other valid-v1 values before artifact
  preparation or Docker with `runner.workload.state-backend-unsupported` or
  `runner.workload.restart-strategy-unsupported`. This is a runner capability
  boundary, not schema narrowing. Harness-managed filesystem checkpoint storage
  is executable in the first vertical per R5.4a.
- **R5.5** A job has a stable **alias** within the scenario, so steps and
  validators can name it. Job identity must survive stop/restart and version
  upgrade — the alias is scenario-level, not the Flink job ID. Resolved job
  aliases are unique; duplicate aliases are rejected before reference binding.
- **R5.5a** A Kafka source, sink, or input-source topic reference may declare
  `connect_via_proxy: <setup.proxies alias>`. When present, the runner gives that
  connection the proxy's `listen` bootstrap address instead of the
  direct cluster bootstrap. Semantic validation rejects a network-fault scenario
  where the connection intended to be affected by `setup.proxies.<alias>` omits
  that proxy route or names a proxy attached to another cluster. If
  `connect_via_proxy` is omitted, the endpoint connects directly; the runner must
  not silently rewrite direct endpoints through a proxy.
- **R5.6** The artifact under test is declared in top-level `subject`, separate
  from the workload job JAR. For connector scenarios, `subject.connectors` is a
  non-empty map keyed by scenario-local aliases; each connector declares
  `artifact` (canonical `maven:<groupId>:<artifactId>:<release-version>`, exact
  local JAR, or final-filename build output pattern per R4.13).
  - If `runtime_dependencies` is absent, the literal or resolved primary must be
    Maven and preparation uses auto mode: its POM supplies the locked closure
    under R4.13a–R4.13c. An absent list is invalid for a local primary.
  - If `runtime_dependencies` is present, preparation uses explicit mode for
    either a Maven or local primary. Each entry is either a local connector-JAR
    reference under R4.13's path rules or a canonical Maven coordinate. A local
    dependency contributes exactly that JAR and has no inferred transitives; a
    Maven dependency entry contributes its selected JAR plus its locked Maven
    runtime closure under R4.13a. `runtime_dependencies: []` explicitly asserts
    that the primary is self-contained. The runner does not inspect bytecode,
    guess libraries from filenames, or silently fall back to the primary's POM.
    Duplicate declared entries reject structurally, and duplicate/conflicting
    resolved identities follow R4.13b.
  - Explicit mode is also how a differential scenario holds the classpath fixed:
    a Maven baseline and local candidate may declare the same dependency roots,
    so the primary connector bytes are the only varied input.
  - When `artifact` contains a parameter template, raw structural validation
    defers this conditional presence rule. After interpolation, the resolved
    scenario is validated again: an absent list is valid only for Maven auto
    mode; a present list selects explicit mode for either reference form. This
    preserves parameterized artifact references without making the dependency
    mode ambiguous.
  In v1, every connector listed in `subject.connectors` is considered part of the
  subject under test. Jobs that use subject connectors list their aliases in
  `workload.jobs[].connectors`. Every declared connector alias must appear in at
  least one such list; an unreferenced subject connector rejects resolved semantic
  preflight rather than being downloaded, reported, or silently omitted from the
  running cluster. The run report records each referenced connector's declared
  artifact, dependency mode, target-specific closure locks, and resolved checksums.
- **R5.6a** A v1 attempt deploys the connector closure as a cluster-level
  classpath, not as opaque `program_args` and not by shading it into the workload
  JAR. The runner computes the side-specific union of connector aliases
  referenced by workload jobs, removes duplicate references, orders aliases
  lexicographically, and visits each connector in its R4.13b classpath order.
  The merge is computed once per effective side from the image-independent
  staged entries. For each alias, its target-specific locks across all target
  images must describe the same connector entry bytes and order. The first
  encounter supplies a unique entry's zero-based global classpath index.

  Classpath coalescing is content-based: entries with the same SHA-256 share one
  global JAR even when their declarations, connector aliases, or Maven identities
  differ, and full prepared/report provenance retains every contributing alias
  and closure index. Separately, the runner maps every Maven conflict key
  `groupId:artifactId:extension:classifier` to its first digest. Another entry
  with that key and the same bytes coalesces, including when its Maven version
  differs; another entry with distinct bytes rejects with both contributors as
  evidence. A local entry has no inferred Maven key, and the runner does not guess
  one from its filename or contents. The first encountered verified staged path
  supplies the bytes for a coalesced entry.

  Deployment deliberately separates two immutable identities:

  1. Exactly one **byte-only classpath manifest** is emitted per effective side
     and is common to every Flink container in that side, even when active
     components use different valid target image references. Its canonical format
     is `flink-stability-connector-classpath-v1`. Its only top-level fields are
     `format: flink-stability-connector-classpath-v1` and an ordered `entries`
     array. Each entry's only fields are zero-based `index`, JAR basename
     `filename`, and lowercase `sha256`. The canonical whitespace-free,
     lexicographically sorted-key UTF-8 JSON bytes are copied verbatim to
     `/opt/flink/lib/.flink-stability-connector-classpath-v1.json`; their external
     SHA-256 is `classpath_manifest_sha256` and is not recursively embedded in
     the file. The manifest excludes target images, aliases, closure hashes,
     contributors, host paths, side/scope, timestamps, and runtime OCI digests.
  2. One **target-specific bundle binding** is emitted per `(effective side,
     distinct valid target image reference)` and uses canonical format
     `flink-stability.connector-cluster-bundle/v1`. Its only top-level fields are
     `format: flink-stability.connector-cluster-bundle/v1`,
     `target_flink_image_reference`, an alias-sorted `closures` array, and
     `classpath_manifest_sha256`; each closure item's only fields are `alias` and
     its target-specific `closure_sha256`. Its whitespace-free, lexicographically
     sorted-key UTF-8 canonical bytes have external hash
     `target_binding_sha256`, which is not recursively embedded. The binding
     excludes host paths, contributors, side/scope, timestamps, runtime-resolved
     OCI digest, and a redundant entry/path list already committed by the
     classpath-manifest hash. It is planning and report evidence and is not the
     manifest copied verbatim into containers.

  Each unique JAR is copied into every JobManager and TaskManager's existing
  `/opt/flink/lib` directory **before** the Flink process starts. The exact path is
  `/opt/flink/lib/flink-stability-connector-%08d-<full-sha256>.jar`, where `%08d`
  is the entry's zero-based global index padded to exactly eight decimal digits
  and `<full-sha256>` is all 64 lowercase hexadecimal characters. The classpath
  manifest's `filename` is this basename without `/opt/flink/lib/`. Distribution-
  provided JARs are not hidden or replaced. Full report provenance retains every
  contributing alias/closure index and the verified host staged path, but neither
  canonical hash includes that provenance.

  Preparation builds and verifies the byte-only classpath manifest and every
  target-specific lock/binding needed by the attempt before constructing its
  first container. Each binding must contain exactly the referenced aliases,
  every named lock must have the same effective side and exact target image as
  that binding, and re-merging those locks must reproduce the binding's referenced
  `classpath_manifest_sha256`. Every initial or replacement Flink container
  receives identical classpath-manifest bytes and the exact JAR checksums named
  there, independent of its target binding. A stale staged byte, cross-connector
  Maven conflict, missing closure, target-lock/classpath disagreement, or observed
  manifest/checksum mismatch rejects provisioning. A mismatch diagnostic
  identifies the effective side, exact target reference when applicable,
  connector alias and closure/global indexes, expected and observed digests or
  read failure, and both contributors plus the Maven conflict key for a
  cross-connector conflict. A Flink-image upgrade selects the binding whose locks
  name the new image while injecting the same byte-only classpath manifest. The
  executable workload JAR remains separate
  and is submitted through Flink's REST JAR API. Because each attempt owns a fresh
  cluster, this system-classloader placement cannot leak connector versions
  between attempts.
- **R5.6b** The first executable v1 compatibility line is Flink `2.2.x` with the
  Kafka connector `5.0.x` built for that line; the canonical released example is
  `org.apache.flink:flink-connector-kafka:5.0.0-2.2`. Other compatibility lines
  require an explicit capability-registry addition and integration coverage,
  rather than being assumed compatible because a JAR happened to load. Every
  canonical Maven primary must match a coordinate-and-version pattern in that
  registry; v1 fails closed for an unknown Maven group/artifact even when its
  version text resembles a supported release. Only a local connector reference
  is the scenario author's assertion that the build targets this line; its exact
  bytes and dependency lock still determine replay identity. Setup images and
  explicit `flink`, `jobmanager`, or `taskmanager` restart images must all identify
  the registered Flink line.
- **R5.6c** **Any executable workload JAR may participate when its staged bytes
  declare and implement workload protocol v1.** The main JAR manifest contains
  exactly `Flink-Stability-Workload-Protocol: v1` (R4.13d). The harness supplies
  the protocol through the Flink execution-environment configuration before the
  JAR creates `StreamExecutionEnvironment`; it is not a generated file and is not
  flattened into `program_args`. `flink-stability.workload.protocol=v1` is a
  required runtime cross-check. The remaining reserved keys are:

  ```text
  flink-stability.workload.v1.job-alias
  flink-stability.workload.v1.source.bootstrap-servers
  flink-stability.workload.v1.source.topic
  flink-stability.workload.v1.source.group-id
  flink-stability.workload.v1.source.starting-offsets
  flink-stability.workload.v1.source.isolation-level
  flink-stability.workload.v1.source.stopping-offsets
  flink-stability.workload.v1.sink.bootstrap-servers
  flink-stability.workload.v1.sink.topic
  flink-stability.workload.v1.sink.delivery-guarantee
  flink-stability.workload.v1.sink.transactional-id-prefix
  flink-stability.workload.v1.sink.transaction-id-naming-strategy
  flink-stability.workload.v1.sink.transaction-timeout-ms
  flink-stability.workload.v1.state-ttl.enabled
  flink-stability.workload.v1.state-ttl.ttl-ms
  flink-stability.workload.v1.state-ttl.cleanup
  flink-stability.workload.v1.watermarks.strategy
  flink-stability.workload.v1.watermarks.max-out-of-orderness-ms
  flink-stability.workload.v1.watermarks.idleness-ms
  ```

  The runner materializes one immutable map in lexicographic key order so report
  evidence is deterministic. Conditional fields are present only when their
  scenario shape enables them. A conforming workload rejects any unknown key
  under the reserved `flink-stability.workload.` prefix; it does not guess a
  newer protocol's meaning.
  Boolean values are lowercase `true`/`false`; millisecond fields are canonical
  non-negative base-10 integers. For an exactly-once sink, the effective
  transaction timeout is the declared `transaction_timeout` in milliseconds, or
  `7200000` when omitted, and both transaction-ID fields are present. The runner
  configures Kafka `transaction.max.timeout.ms` to a fixed two hours; a declared
  timeout above that maximum is a planning error
  (`runner.workload.transaction-timeout-unsupported`), not a late producer-init
  surprise.
  The v1 built-in source uses `starting-offsets=committed-or-earliest` and
  `isolation-level=read_uncommitted`, matching the non-transactional harness input
  producer.

  The first executable bounded vertical requires `source.stopping-offsets`. Its
  value is a comma-separated list of `partition:exclusiveOffset` pairs, for
  example `0:1000,1:750`. Partition IDs and offsets are non-negative base-10
  integers with no sign or leading zero (except the value `0`); partitions are
  unique and strictly increasing by numeric ID; partition IDs fit a signed
  32-bit integer and offsets fit a signed 64-bit integer. The map contains
  exactly one entry for every declared source-topic partition, including a
  partition whose exclusive offset is `0`. Empty elements,
  whitespace, duplicates, negative values, overflow, and non-canonical ordering
  reject. The runner derives the map from the closed
  preload input snapshot, and the workload applies it with a bounded Kafka source
  (`setBounded(OffsetsInitializer.offsets(map))`). Consequently the source can
  exhaust its exact input range and the Flink job can reach `FINISHED` naturally.
  Omission is reserved for the future controlled-unbounded drain protocol in
  R4.7 and is not executable in v1.

  Flink-owned settings travel through their standard configuration surface:
  `parallelism.default`, `execution.checkpointing.*`, checkpoint-storage options,
  `state.backend.type` (plus selected backend options), and
  `restart-strategy.*`. A conforming workload obtains and respects that
  environment and must not override the resolved values. The reserved workload
  namespace exists only for data-path/operator semantics Flink does not model.
  Artifact preparation checks the manifest declaration and execution supplies
  the complete configuration map unchanged. The bundled workload rejects a
  missing/wrong runtime protocol key and unknown reserved keys. For an arbitrary
  external JAR, the marker is an author conformance assertion: the first runner
  detects observable submission, terminal-state, fence, and Kafka-output
  failures, but cannot prove that a job silently used every operator-level TTL
  or watermark setting. Such a JAR needs protocol-conformance integration tests
  before it is trusted as a CI oracle. A declaration without conforming behavior
  remains a protocol defect, never permission to fall back to legacy arguments.
- **R5.7** Each workload job has optional `start: auto | manual`, defaulting to
  `auto`. Auto-start jobs are submitted after infrastructure is ready and any
  preload input source has completed per R4.5a, before the first phase begins.
  `generated-rate` input sources start with phase execution, also per R4.5a. A
  `start` step is required only for `start: manual` jobs or for jobs
  intentionally started later in a lifecycle test.

---

## 6. Phases and steps

- **R6.1** A scenario is an ordered list of named `phases`, each an ordered list
  of steps. Phase names appear in the report so a failure localises.
- **R6.1a** Phase names are unique within a scenario. Duplicate names would make
  report paths and lifecycle evidence ambiguous and are rejected before
  provisioning.
- **R6.2** Sequential execution only in v1. No parallel step orchestration —
  concurrency makes observable triggers ambiguous.
- **R6.3** Step vocabulary, at minimum: `await`, `wait`, `start`, `stop`,
  `kill`, `restart`, `savepoint`, `checkpoint`, `restore`, `validate`, `loop`.
- **R6.3a** If a job is auto-started by R5.7, the first phase begins only after
  the runner has attempted submission and recorded the resulting Flink job ID.
  The phase may still begin with an `await job-state RUNNING`; that wait observes
  readiness and recovery, not submission itself.
- **R6.4** **`await` takes a typed condition from a closed enum**, never an
  expression string: job state, TaskManager count, checkpoint completed,
  savepoint completed, log marker, Kafka transaction state, record threshold.
  Adding a condition requires code — which it needs anyway, to be implemented and
  tested.
- **R6.5** A plain time `wait: { duration: <duration> }` remains available but is
  never the primary trigger for a race-window scenario.
- **R6.6** Repetition is an explicit `loop` step containing nested steps.
  Phase-level `repeat` is removed: it currently re-runs lifecycle steps such as
  `start`, which is a footgun.
- **R6.7** Every fault step names a **target** as a structured object, not a bare
  string. v1 resolves static names (`taskmanager-2`, `broker-1`); the shape must
  accommodate runtime selectors (`the TaskManager hosting sink subtask 0`)
  without changing committed scenario files.
  Static names are canonical one-based names: `taskmanager-{1..N}`,
  `jobmanager-{1..N}`, and `broker-{1..N}`. A role/name mismatch, zero,
  leading-zero ordinal, or ordinal above the resolved component count is not a
  declared target. Because the process-target shape has no Kafka-cluster field,
  a broker name must resolve in exactly one declared cluster; zero matches is
  missing and multiple matches is ambiguous. `kind: selector` remains reserved
  in the schema but is an unsupported v1 capability and is rejected before any
  container starts.
- **R6.8** Fault steps: process `kill`/`stop`, Kafka cluster faults, and network
  faults via a proxy layer. v1 accepts only Kroxylicious for protocol-aware
  Kafka faults. A future coarse-TCP proxy such as Toxiproxy may use a distinct
  proxy `type`; it is not a v1 scenario choice.
- **R6.8a** A network-fault scenario declares its proxy in `setup.proxies`
  before any step references it; an undeclared proxy reference is a semantic
  error before provisioning. SPEC-004 defines the exact proxy topology and
  network-fault step schema.
- **R6.8b** A network-fault step can affect only Kafka traffic routed through its
  named proxy. Semantic validation rejects a network-fault scenario where no
  matching endpoint declares `connect_via_proxy` for that proxy. Matching is
  deterministic in v1:
  - `network_fault.proxy` names the proxy route.
  - `network_fault.target.cluster` must equal the proxy's declared `cluster`.
    If `proxy.bootstrap` uses `{ cluster: ... }`, that cluster must also equal
    `proxy.cluster`; cross-cluster proxy faults are out of scope for v1.
  - Candidate endpoints are only source, sink, or input-source endpoints whose
    cluster is the target cluster and whose `connect_via_proxy` equals the named
    proxy. For `input_source`, the cluster and topic are inherited from the
    enclosing Kafka topic declaration.
  - `match.topic`, when present, narrows candidates to the endpoint's topic.
    `target.topic` is not accepted in v1; topic selection lives only in `match`.
  - `match.api: produce` matches workload sinks and harness input producers.
  - `match.api: fetch` matches workload sources.
  - `match.api: metadata` matches any routed source, sink, or input producer on
    the target cluster, narrowed by topic if a topic is present.
  - Transaction APIs (`init-producer-id`, `add-partitions-to-txn`,
    `add-offsets-to-txn`, `txn-offset-commit`, `end-txn`) match exactly-once
    sinks. `transactional_id_prefix`, when present, must match the sink's
    configured prefix. `target.transactional_id_prefix` is not accepted in v1;
    transactional-prefix selection lives only in `match`.
  - Topic matching follows fields that actually exist in each request:
    `add-partitions-to-txn` matches the sink topic and `txn-offset-commit`
    matches the paired job source topic on the target cluster.
    `init-producer-id`, `add-offsets-to-txn`, and `end-txn` do not accept a
    topic selector. This is deliberately narrower than treating every
    transaction API as topic-bearing.
  - `target.client` is out of scope for v1 because endpoints do not yet declare
    stable client aliases.
  This prevents a scenario from "passing" because the Flink job or input
  producer used the direct broker listener and bypassed the injected fault.
- **R6.9** v1 distinguishes **instantaneous faults** from **held faults**.
  `kill` is instantaneous: the fault window closes when the targeted process exit
  is confirmed, and recovery is the behavior under test rather than an explicit
  heal step. Held faults (`stop`, network faults, Kafka broker isolation, and any
  future fault that leaves the system in an intentionally degraded state) declare
  a bounded `duration` and a reliable `heal` action. All faults still get a fault
  ID and lifecycle evidence per R6.12 and §12.6.
- **R6.9a** Each first-runner named TaskManager `kill` or `restart` phase action
  has its own fixed, internal `2m` monotonic deadline. The deadline starts when
  that action begins, is independent of every other action and cleanup, and is
  shared without reset across every Docker start, kill, inspect, remove, and
  confirmation call the action needs. A timed-out kill is `inconclusive` with
  `taskmanager.kill.timeout`; a timed-out restart is `inconclusive` with
  `taskmanager.restart.timeout`. Either timeout stops phase execution, skips
  terminal fencing and terminal validation, and does not set the irreversible
  terminal-fence latch. Later resource cleanup remains a separate bounded
  operation under R8.2c; a late Docker completion cannot turn the timed-out
  action into a successful phase or authorize terminal validation.
- **R6.10** **`restore` names the exact checkpoint or savepoint to restart
  from** — the latest, or a specific earlier one, to test rollback.
  `restore.from` accepts `latest-savepoint`, `latest-checkpoint`,
  `{ type: savepoint|checkpoint, name: <prior-step-as> }`, or
  `{ type: savepoint|checkpoint, path: <external-path> }`. `checkpoint` and
  `savepoint` steps may declare `as: <name>` so later restore steps can refer to
  a specific earlier state artifact. Restore mode (claim / no-claim) is a field,
  since it is itself an invariant dimension.
  A named artifact identity is `(job, type, name)`: a named restore must resolve
  to an earlier producer for the same job and type. Duplicate identities are
  rejected. A producer with `as` inside a loop that executes more than once is
  also rejected because v1 has no iteration-qualified artifact name; a named
  restore inside a loop may still reuse an unambiguous artifact produced before
  it. `latest-*` and external `{ type, path }` references are resolved at
  runtime and do not require an explicit earlier producer step.
- **R6.11** A Kafka cluster **upgrade** is expressible. It is a lifecycle
  operation, not a fault; it belongs in its own phase rather than in a chaos
  phase, even though it involves restarts.
  The v1 `restart: { component: kafka }` shape names no cluster, so it is valid
  only when exactly one Kafka cluster is declared; a multi-cluster Kafka restart
  is rejected as ambiguous rather than applied to every cluster implicitly.
- **R6.11a** Flink restart-image inheritance is deterministic and fail-closed.
  Static preflight computes the exact **desired target** reference of every
  logical JobManager and TaskManager, initially `setup.flink.image`, through the
  ordered phase/loop steps. Desired target is intentionally distinct from the
  last successfully provisioned target and its evidence:

  - `restart: { component: jobmanager, image: X }` changes the sole v1
    JobManager's desired target to `X`.
  - `restart: { component: taskmanager, image: X }` is valid only when the
    resolved `taskmanagers` count is exactly one, and then changes that logical
    TaskManager's desired target to `X`. With multiple TaskManagers the shape
    names no slot and rejects as ambiguous; v1 neither chooses one nor silently
    upgrades all of them.
  - `restart: { component: flink, image: X }` assigns desired target `X` to every
    JobManager and TaskManager and restores uniform desired state.
  - A JobManager or TaskManager restart without `image` retains each affected
    logical component's own desired target. A full `component: flink` restart
    without `image` is valid only when all desired Flink component targets have
    one exact uniform reference; it inherits that reference. If their targets
    differ, the step rejects before provisioning rather than choosing the setup
    image, the most recent image, or one component's image.

  Every valid exact desired reference reached by this state machine participates
  in the target-specific locks and bindings of R4.13c/R5.6a. Runtime-resolved OCI
  digests remain associated evidence and do not replace exact declared-reference
  equality in this preflight state.

  At execution time, after an explicit JobManager or sole-TaskManager retarget
  has successfully stopped the old process, `X` becomes that logical component's
  desired target before replacement provisioning starts. If construction,
  connector copy/verification, or process start for `X` fails, the desired target
  remains `X`; a later `start` or restart without `image` retries `X`. The runner
  does not silently roll back to the prior image. Until `X` provisions
  successfully, the component's last-successful provisioning evidence continues
  to name the prior target, while the failed attempt records `X` as both desired
  and attempted target.
- **R6.12** Every fault records **evidence that it actually occurred**. A run
  where the fault cannot be confirmed is `inconclusive`, never `pass`.
- **R6.12a** For the first runner's named TaskManager `kill`, a confirmed
  process exit proves only the injection. The runner also records whether the
  kill affected the job:

  1. Immediately before the kill, it takes one read-only observation of the job
     through the Flink REST API: JobManager time, job state, completed and
     restored checkpoint counts, the latest restored checkpoint, the exception
     history, and each subtask's state and TaskManager.
  2. It takes one more observation when execution ends: after the job reaches a
     terminal state or its completion timeout expires, before the process fence
     (R7.1c), or when phase execution stops with a failure (R6.9a). The last one
     also explains the failure, for example a job in a restart loop.
  3. Each kill is judged against the next observation: the one before the next
     kill, or the final observation of item 2 for the last kill. Only values
     from the JobManager clock are compared with each other.

  Immediately after confirmed process exit, the runner requests a fresh JobManager
  time using a separate job-details call. This post-exit sample is a conservative
  lower bound for accepted recovery evidence, not the timestamp of the kill.
  Failures and restores must occur strictly after that sample; failures already
  present in the pre-kill observation are excluded. Missing or reversed timing
  evidence is unconfirmed. A failure detected between exit and sampling can
  therefore be unconfirmed rather than attributed using an ambiguous time window.
  A restore must also follow a matching host failure (equal millisecond timestamps
  are accepted). Without a completed checkpoint, recovery requires a later
  `FINISHED` observation or an active execution attempt with a greater attempt
  number for a previously observed vertex/subtask; a failure alone proves no restart.

  The effect is confirmed only when Flink recorded a qualifying new failure on a
  TaskManager that hosted a deployed, initializing, or running subtask just before
  the kill, and either Flink restored a checkpoint after that failure
  (`checkpoint-restored`) or no checkpoint had completed before the kill and the
  later observation proves a restart (`restarted-without-checkpoint`). It is
  unconfirmed when the job was already terminal (`job-terminal-before-kill`), when
  no subtask was active (`no-active-subtask-before-kill`), when no such failure or
  no required restore follows (`no-recovery-observed`), or when an observation
  or timing sample failed (`evidence-unavailable`). A job that is already failing
  over for another reason therefore does not confirm a kill that found no active
  subtask. Each observation has one fixed internal `30s` deadline; its failure is recorded as
  evidence and never blocks the kill or the process fence. The post-exit clock
  request has its own fixed internal `30s` deadline; its failure leaves timing
  evidence unavailable and does not skip the subsequent restart.

  An unconfirmed effect never hides a failure: a failing terminal oracle keeps its
  `fail` result. A passing oracle with any unconfirmed kill makes the attempt
  `inconclusive` with `taskmanager.kill.effect-unconfirmed`, because that pass does
  not show recovery. A scenario whose bounded input finishes before its kill, or
  whose TaskManager hosts no active subtask, therefore cannot pass.
- **R6.13** A suite entry is `{ scenario, as?, parameters?, runs? }`. `as`
  defaults to the scenario name and is **required** when the same scenario
  appears more than once in a suite. `runs`, when present, is a positive integer
  that replaces the scenario's `runs` only for that suite invocation; the report
  records both declared and effective values. Submit-time CLI overrides for
  `runs` and `health_retry_limit` are not supported in v1; `health_retry_limit`
  is scenario-owned and cannot be suite-overridden. Every run artifact, report
  key, and CI annotation is keyed on `as`, never on `meta.name` alone.
  Parameter indirection does not bypass this rule: semantic validation rejects
  a suite binding or submit-time parameter whose interpolation provenance
  reaches scenario `runs` or `health_retry_limit`. A suite-specific repetition
  count uses the entry's explicit `runs` field.

---

## 7. Validation

- **R7.1** Two distinct forms, named differently so neither can be mistaken for
  the other:
  - an inline `validate` **step**, runnable at any point, for observations and
    preconditions. Its result is recorded but never gates the run.
  - a required top-level **`terminal_validations`** list. These are the
    gate-authoritative oracles.

  An expected failure (R3.5) may only name a terminal validator.
- **R7.1b** v1 permits at most one top-level terminal validator of each `type`.
  Expected-result `oracle` names that type, so duplicate types would make the
  failure source ambiguous. Inline `validate` steps do not participate in this
  identity rule and never satisfy an expected oracle.
- **R7.1a** Terminal aggregation produces an **attempt result**, not the aggregate
  scenario verdict (R8.7). An attempt passes only if **every** terminal validator
  passes. Any `fail` makes the attempt `fail`. If no validator failed but any
  returned `inconclusive`, the attempt is `inconclusive` — an unevaluated oracle
  is never treated as a satisfied one. Terminalization or verification failures
  in R7.1c, R7.3b, or R7.3d bypass expected-oracle matching and force the scenario or
  experiment to `fail` with their own `verification.*` reason.
- **R7.1c** **Terminal validation runs only behind a confirmed Flink write
  fence.** After every phase has completed and the bounded input manifest is
  closed, the runner executes this order:

  1. Wait for every submitted bounded workload job to reach `FINISHED` naturally.
     The scenario-level `completion_timeout` is optional in raw YAML, defaults to
     `2m`, and is mandatory/materialized in the resolved scenario and report.
     Its non-resetting clock starts when both prerequisites are true: all phases
     have completed and the input manifest is closed.
  2. Establish the irreversible physical process fence in R7.1d. A terminal job
     state alone is not the process fence, and its fixed internal deadline is
     separate from `completion_timeout`. Immediately before the fence, the runner
     takes the read-only job observation of R6.12a, also after a completion
     timeout. It only reads REST state, so it cannot admit writes.
  3. Declare the write fence established and start each terminal validator's one
     absolute deadline **before** its Kafka metadata, topic, and partition
     discovery. Immediately after discovery, a topic-reading validator uses a
     `read_uncommitted` boundary consumer to capture each target partition's
     high-watermark offset exactly once. It then obtains beginning offsets and
     uses a separate `read_committed` consumer to traverse that exact fixed
     range. This prevents a transaction whose `EndTxn` was acknowledged before
     the Flink process fence, but whose commit marker becomes visible afterward,
     from falling beyond a `read_committed` last-stable-offset snapshot. A commit
     exposes its records inside the fixed range, an abort advances past them
     without exposing them, and an unresolved transaction prevents traversal
     from reaching the boundary and therefore fails at the same deadline.
     Discovery, raw-boundary capture, beginning-offset capture, bounded reading,
     decoding, and comparison all share the same non-resetting deadline. A
     transaction validator begins its transaction-state discovery and query
     under the same one-deadline rule.

  The runner never cancels a healthy bounded job merely to make step 1 finish:
  cancellation can abort an otherwise correct in-flight transaction and create
  artificial missing output. If `completion_timeout` expires, the runner
  invokes the same physical process fence, retains its evidence when that fence
  succeeds, records
  `verification.flink.job-completion-timeout`, and fails the experiment. It may
  run fenced snapshot checks for diagnostics, but their output is not an
  authoritative completeness result and cannot satisfy an expected
  data-integrity failure. If the runner cannot confirm the process fence, no
  terminal validator starts and the experiment fails with
  `verification.flink.process-fence-failed`. A non-timeout failure while querying
  or waiting for terminal job state similarly fails with
  `verification.flink.job-terminalization-failed` before any oracle can match.

  After the terminal validators finish, the first runner lists the Kafka
  transactions whose transactional ID starts with the sink's
  `transactional_id_prefix` through the Kafka Admin API (fixed internal `30s` per
  call) and records every one that is not `Empty`, `CompleteCommit`, or
  `CompleteAbort`. Once Flink is fenced, only the broker's transaction timeout can
  resolve such a transaction, so this names what may pin a last stable offset. The
  listing is evidence only: its failure is a diagnostic and never changes the
  attempt result.

  This order prevents a delayed checkpoint notification, TaskManager recovery,
  or producer transaction from committing after a terminal validator has passed.
  Kafka stays running; Flink does not. Inline `validate` steps remain live
  observations and do not receive this terminal guarantee.
- **R7.1d** The process side of the write fence has one fixed, internal `2m`
  monotonic deadline. It is not a scenario field, does not reuse
  `completion_timeout` or a validator timeout, and does not reset for each
  component. Within that shared budget the runner sends `SIGKILL` to every
  running TaskManager first and then every running JobManager, attempts every
  known component even after an earlier failure, and confirms that none remains
  running. Successful evidence contains one deterministic entry per known
  component with its logical name, role, available physical runtime ID, and
  `SIGKILLED` or `ALREADY_STOPPED` outcome, plus the fence-completion timestamp.
  The same evidence is retained when a forced fence succeeds after job-completion
  timeout or another terminalization failure.

  Starting the process fence is irreversible for that attempt: no Flink process
  may subsequently be created or restarted. Later attempt cleanup may remove the
  already-stopped physical containers, but it cannot reopen the write boundary.
  Failure to kill or confirm every process within the shared budget yields
  `verification.flink.process-fence-failed`; no terminal validator starts.
- **R7.2** Every validator returns a structured result: status
  (`pass`/`fail`/`inconclusive`), machine-readable reason code, evidence,
  metrics, message. Never a bare boolean — §3 depends on reason codes.
- **R7.2a** Reason codes are stable dot-separated namespaces defined before an
  expected-result document may reference them. Expected failures reference a
  terminal-validator `fail` code, never an error-message substring or an
  `inconclusive`, await, fault, or infrastructure code.
- **R7.2b** The initial v1 expected-failure registry is deliberately fail-closed:

  | Terminal validator `type` | Allowed expected `fail` reason |
  | --- | --- |
  | `kafka.id-set` | `validator.kafka.id-set.malformed-ids` |
  | `kafka.id-set` | `validator.kafka.id-set.unexpected-ids` |
  | `kafka.id-set` | `validator.kafka.id-set.duplicate-ids` |
  | `kafka.id-set` | `validator.kafka.id-set.missing-ids` |
  | `kafka.no-hanging-transactions` | `validator.kafka.transaction.ongoing-after-timeout` |

  `kafka.record-count` and `flink.log-match` remain valid terminal validators for
  expected-pass scenarios, but cannot be named by an expected failure until
  their stable failure-code contracts are added here and to the implementation.
  Codes such as `await.job-state.timeout`, `fault.network.unconfirmed`,
  `infrastructure.image-pull-failed`, and
  `input-manifest.acknowledged-missing` are intentionally outside this registry.
  So are all `verification.*` reasons: failure to complete the experiment's
  verification is never evidence that an expected connector data-integrity bug
  was reproduced.
- **R7.3** Built-in validators required for v1: Kafka record count, Kafka exact
  ID-set/exactly-once (`kafka.id-set`), Kafka no-hanging-transactions, Flink log
  match. `kafka.id-set` compares output IDs exactly with the manifest's `present`
  set; v1 does not expose a `contiguous` option, because conclusively absent
  source IDs can create legitimate gaps in the requested numeric range. The
  obsolete `kafka.id-sequence` name is rejected. After reading the complete
  fenced snapshot, `kafka.id-set` records evidence for every observed defect and
  selects one stable primary reason with this precedence:
  `malformed-ids` > `unexpected-ids` > `duplicate-ids` > `missing-ids`. Thus the
  result never depends on consumer encounter order. Malformed evidence identifies
  record coordinates and decoding errors; unexpected/duplicate/missing evidence
  includes deterministic sorted IDs and counts, subject to report-size limits. A
  Kafka tombstone (a record with a null value) is preserved with its
  partition/offset coordinates and is a malformed ID, not snapshot unavailability.
- **R7.3a** `kafka.no-hanging-transactions` is the canonical validator name; the
  obsolete `kafka.no-ongoing-transactions` is rejected. Its required
  `stabilization_timeout` is the maximum post-fence settling period allowed for a
  reachable, observable transaction to leave the ongoing state. It is distinct
  from and must not exceed the validator's absolute `timeout`.
- **R7.3b** Every validation invocation has an absolute `timeout`. It is optional
  in raw YAML, defaults to `2m`, and is mandatory/materialized in the resolved
  scenario and report. For a terminal validator the non-resetting clock starts
  **immediately after the R7.1c write fence and before Kafka discovery**.
  Metadata/topic/partition discovery runs first under that deadline. Capturing
  Kafka partition high-watermark offsets with `read_uncommitted` isolation is
  the first range-reading snapshot operation immediately after discovery. A
  separate `read_committed` consumer must reach those immutable raw bounds;
  beginning-offset capture, connection retries, transaction stabilization,
  bounded consumption, decoding, and comparison all consume the same remaining
  budget and never receive fresh clocks. The
  no-hanging-transactions validator's transaction discovery and first state query
  are likewise timed. This makes an unreachable broker during discovery or
  snapshot capture bounded too.

  If Kafka remains unreachable when the deadline expires, the experiment fails
  with `verification.kafka.unreachable-after-timeout`. If Kafka was reachable
  but the validator cannot obtain complete evidence through every captured
  partition bound by the deadline, it fails with
  `verification.kafka.incomplete-after-timeout`. These are verification failures,
  not `inconclusive` validator results, and R8's health or expectation machinery
  must not turn them green. A missing topic, a non-zero beginning offset showing
  that the attempt snapshot was truncated, or another non-timeout Kafka snapshot
  failure likewise fails closed as `verification.kafka.topic-not-found`,
  `verification.kafka.snapshot-truncated`, or
  `verification.kafka.snapshot-unavailable`. A temporary outage that recovers
  before the absolute deadline proceeds to the ordinary validator result. Every
  incomplete or failed snapshot retains bounded partial evidence available at
  the failure point: any captured beginning/end offset maps, the observed-record
  count, deterministic bounded partition/offset/value samples, and an explicit
  incomplete-snapshot marker. Missing, malformed, unexpected, duplicate, and
  distinct-expected totals are authoritative only after the entire fixed range
  is decoded; partial evidence omits those totals instead of publishing a
  provisional prefix or treating every unobserved ID as missing. A complete
  snapshot that proves a duplicate,
  missing, unexpected, malformed, or hanging transaction returns the
  corresponding `validator.*` failure instead.
- **R7.3c** The broad v1 contract continues to permit one or more terminal
  validators of the types in R7.3. The first narrow executable runner accepts
  exactly one `kafka.id-set` validator with `expected: input-manifest`.
  Unsupported validator counts, types, or expected-data sources are rejected
  before artifact preparation or Docker with
  `runner.validation.count-unsupported`,
  `runner.validation.type-unsupported`, or
  `runner.validation.expected-unsupported`, respectively. This is a runner
  capability boundary, not a narrowing of the v1 format.
- **R7.3d** The first narrow in-memory runner observes at most `1,000,000`
  terminal Kafka output records, the same temporary bound used for generated
  input. Encountering the next in-range record stops materialization and fails
  closed with `verification.kafka.output-limit-exceeded`, while retaining the
  fixed partition bounds and bounded record-coordinate evidence already
  observed. Tombstones count toward this observation limit. This verification
  failure cannot satisfy an expected data-integrity oracle. Each retained record
  keeps at most 64 characters of its value: a longer value cannot be a canonical
  ID, so it keeps a 32-character prefix marked with its length and counts as a
  malformed ID. Retained memory is thus bounded by the record cap, not by the
  size of what the job wrote. Lifting the cap
  requires streaming or spill-backed snapshot and validator evidence; it does
  not narrow the broad v1 scenario schema.
- **R7.4** Savepoint self-containment is assertable (no dangling references to
  another checkpoint's files).
- **R7.5** **Custom validator extension point.** A scenario may point at a
  user-supplied executable JAR with a `Main-Class`, executed by the harness in a
  separate JVM, for organisations running
  proprietary forks of Flink or Kafka.
  - **R7.5.1** It returns a **structured result** (JSON on a defined channel),
    not `TRUE`/`FALSE`. A boolean gives a failure with no evidence, which
    violates the plan's evidence-preservation rule.
  - **R7.5.2** It runs under the resolved R7.3b timeout; omission in raw YAML
    resolves to `2m`. A hang is `inconclusive` because a third-party process
    failure supplies no built-in Kafka evidence; the fail-closed Kafka exception
    in R7.3b applies to built-in Kafka terminal verification.
  - **R7.5.3** It receives the resolved scenario context — cluster addresses,
    topics, job alias, artifact paths.
  - **R7.5.4** Its identity (path plus checksum) is recorded in the run report.
    A custom oracle that is not recorded makes a result unreproducible.
  - **R7.5.5** A custom producer or validator runs in a disposable per-attempt
    container, not in the runner process. The artifact and generated JSON context
    are mounted read-only; only an isolated scratch directory is writable. It has
    no host filesystem or Docker-socket mount, inherits no environment or secret
    values, and is attached only to the isolated attempt network.
  - **R7.5.6** The runner sets CPU, memory, process-count, wall-clock,
    context-input-size, and stdout-size limits; captures stderr and exit
    metadata; and destroys the extension container in a `finally` block. A limit
    breach, protocol failure, crash, or timeout is `inconclusive` and preserves
    the partial run report.
  - **R7.5.7** A custom validator that may be named by an expected failure
    declares its stable `fail` codes in optional `failure_reasons`. The field is
    required semantically when an expected failure uses `oracle: custom`; its
    entries use the `validator.custom.*` namespace and are the only custom
    reasons that expectation may reference. A custom validator used only by
    expected-pass scenarios may omit it.

---

## 8. Outcomes and reporting

- **R8.1** Three outcomes, with these CI statuses:

  | Outcome | Meaning | CI status |
  | --- | --- | --- |
  | `pass` | successful evidence | green |
  | `fail` | evidence contradicting expected behaviour, or an R7.1c, R7.3b, or R7.3d fail-closed terminal verification condition occurred | `assertion-failed` |
  | `inconclusive` | no evidence either way | `unvalidated / retry-required` |

  `inconclusive` is **not green and not a regression**. On a required merge gate
  it leaves the pull request **unvalidated** until retried or explicitly waived.
  It must never be silently treated as a pass, nor attributed to the code change.
- **R8.2** The run report records everything needed to replay: resolved
  parameters; declared image references and independently resolved image digests;
  artifact checksums; every target-specific connector closure lock and
  `closure_sha256` (R4.13c); the byte-only classpath manifest's exact canonical
  bytes at `/opt/flink/lib/.flink-stability-connector-classpath-v1.json`,
  `classpath_manifest_sha256`, ordered index/filename/checksum entries, and
  contributor/host-staging provenance; every target-specific bundle binding's
  canonical bytes, exact target reference, sorted alias/`closure_sha256` pairs,
  referenced `classpath_manifest_sha256`, and `target_binding_sha256` (R5.6a);
  seeds; fault evidence with timestamps; per-validator results with reason codes;
  every declared omission and materialized default; the workload-protocol
  declaration and effective configuration; the completion deadline, job terminal
  states, process-fence evidence and timestamp; each validator's absolute deadline
  and captured Kafka partition bounds; any `verification.*` failure; and the
  differential verdict when §3 applies. Filesystem checkpoint storage also records
  its generated attempt/job URI. Each Flink deployment/restart
  attempt names the logical component, desired exact target reference, outcome,
  resolved OCI digest when available, selected target binding and lock hashes,
  and observed classpath-manifest path/hash. Per-component last-successful
  provisioning evidence is recorded separately and changes only after a new
  target provisions successfully. Thus an upgrade report preserves
  distinct connector/image pairings even when every container received identical
  connector bytes.
- **R8.2a** Terms used by this section:

  | Term | Definition |
  | --- | --- |
  | *attempt* | One execution of one resolved side (`baseline` or `candidate`) of an experiment, or one execution of a resolved non-experiment scenario. |
  | *attempt result* | `pass`, `fail`, or `inconclusive` from terminal-validator aggregation (R7.1a), unless an R7.1c, R7.3b, or R7.3d `verification.*` failure forces `fail`, before repetition aggregation. |
  | *expectation match* | Whether a non-inconclusive authoritative oracle result matches the selected expected-result outcome and, for expected failures, its required oracle and reason. A `verification.*` failure is never an expectation match. |
  | *clean attempt* | An attempt whose environment health is clean (R8.4) and whose terminal validators produced a non-inconclusive result. |
  | *scenario verdict* | The final `pass`, `fail`, or `inconclusive` result after the repetition and experiment rules in R8.5–R8.8. |

- **R8.2b** The implemented first-vertical `run` command emits one stable JSON
  result/evidence summary after execution and prepared-artifact cleanup. Its
  top-level `status`, `reason`, and `message` are the scenario verdict of R8.7a,
  and only a `pass` verdict exits `0`. `expectation` names the selected outcome,
  the oracle and reason of an expected failure, and whether the attempt matched.
  It also includes scenario and attempt identity, retained checkpoint root, the
  attempt's own status, reason and message under `attempt`, summarized input and
  phase evidence,
  write/process-fence completion evidence, terminal-validation status/counts/
  completeness, Flink provisioning count, and diagnostics. It also includes the
  last job observation of R6.12a as `evidence.flinkJob` (`observed`,
  `unavailable` with its failure, or `not-run`) and one `evidence.taskManagerKills`
  entry per confirmed kill: step path and loop iterations, target, effect outcome,
  whether it is confirmed, job state, checkpoint and active-subtask counts before
  the kill, `jobManagerTimeAfterKill` when available, the restored checkpoint and
  `restoredAfterKillObservationMs` measured from that post-exit time sample,
  the failures recorded after the kill, and a one-sentence detail. The R7.1c sink
  transaction listing appears as `evidence.sinkTransactions`: `listed` with the
  prefix, the total, and each unresolved transaction, or `not-listed`. Input
  evidence names `complete` or `partial` status and reconciliation completion
  explicitly; a stage that never started remains present as `not-started` or
  `not-run` with `completed: false`, and terminal `snapshotComplete: false`;
  terminal defect totals appear only for a complete snapshot. This is
  useful executable evidence but is **not** the complete replay-grade R8.2 run
  report. Environment-health sampling, independently resolved OCI digests, and
  the full resolved/artifact/configuration/provenance report remain roadmap work;
  the summary must not claim that those absent fields were collected.

- **R8.2c** After the attempt result is decided, the first runner starts physical
  resource cleanup exactly once and waits under one fixed internal `2m` wall
  deadline. Cleanup runs on a daemon boundary so an interrupt-ignoring Docker
  call cannot prevent the command from returning its structured result. Timeout
  or interruption adds an `infrastructure.attempt-cleanup-failed` diagnostic: a
  prior `pass` becomes `inconclusive`, while an existing `fail`—especially a
  `verification.*` failure—remains authoritative. Cancellation of the cleanup
  worker is best effort, the attempt runtime is discarded, and no concurrent
  cleanup retry is allowed. Worker-start handshaking consumes the same deadline;
  if an accepted cleanup task has not started when that deadline expires, the
  runner returns the timeout result without cancelling the queued task so it may
  still start exactly once. Detached physical cleanup owns no prepared artifact
  and must not read staged files, construct replacement components, or reopen the
  irreversible process fence. The CLI therefore remains the sole prepared-plan
  owner and may close its private staging tree after execution returns even if a
  timed-out cleanup worker has not returned.

- **R8.2d** After execution returns, the CLI closes the prepared-plan owner
  synchronously before it emits JSON. If that close fails after an attempt result
  exists, add `infrastructure.prepared-artifact-cleanup-failed` and still emit the
  retained result and evidence: a prior `pass` becomes `inconclusive`, while an
  existing `fail` or `inconclusive` keeps its stable primary reason. Only a cleanup
  failure before any attempt result exists uses the stderr-only infrastructure
  error path; prepared-owner cleanup must never erase an authoritative
  `verification.*` result or its process-fence evidence.

- **R8.3** A scenario declares `runs: K`, a positive integer; the report gives
  N-of-K. Any clean expectation mismatch is a failure — exactly-once is not a
  statistical property.
- **R8.4** Every attempt collects environment-health signals covering its own
  execution window: image-pull failures, Docker daemon reachability sampled
  throughout, host disk and memory pressure, clock jumps, unexpected container
  restarts, and fault-injection confirmation. A dirty-health signal forces the
  attempt `inconclusive` before its oracle result is considered.

  R7.1c, R7.3b, and R7.3d are the deliberate exceptions: failure to finish/fence Flink or to
  complete built-in Kafka terminal verification by its absolute deadline remains
  `fail` with its stable `verification.*` reason even when the same outage also
  appears in health evidence. The report preserves both observations; health
  classification must not erase the verification failure or convert it to an
  expected data-integrity oracle match.

  Process exits and restarts are correlated with the fault lifecycle (12.6): an
  event on the declared target between `trigger-confirmed` and `healed` is
  expected fault evidence; an event on any other component, outside that window,
  or without a fault ID is dirty health. Each `process_event` record includes its
  component identity, timestamp, exit code or signal, observed restart state,
  fault ID when applicable, and classification. Thus an intentional SIGKILL/137
  is not confused with an unexplained OOM kill.
- **R8.5** With an `experiment`, `runs: K` applies to the candidate side. The
  runner executes the baseline once, executes the candidate until it has K clean
  attempts, and runs one confirmatory baseline after the first clean candidate
  expectation mismatch. A terminal-validator `inconclusive` under clean health
  stops the invocation immediately under R8.6a; it does not continue to seek K
  clean attempts. A `verification.*` failure stops and fails the invocation
  immediately. Without an experiment, `runs: K` applies to the whole scenario.
  If a committed experiment parameter resolves the two side documents to
  different `runs` values, the candidate's resolved value is K; the baseline is
  still governed by the fixed initial/confirmatory rules above.
- **R8.6** A dirty attempt is discarded and retried from a **single per-scenario
  invocation** budget, `health_retry_limit`, a non-negative integer that defaults
  to 1; it does not count toward K. The budget covers initial, candidate, and
  confirmatory-baseline attempts together. Exhausting it makes the scenario
  `inconclusive` with an infrastructure reason code.
  Because this is one shared invocation budget, an experiment whose baseline and
  candidate resolve `health_retry_limit` to different values is rejected before
  provisioning.
- **R8.6a** A terminal-validator `inconclusive` under clean health is not a
  clean attempt and is not retried automatically: the runner stops the invocation
  and returns `inconclusive` with that validator's reason code. A dirty
  confirmatory baseline consumes the shared R8.6 retry budget and is retried
  until it is clean or the budget is exhausted. An R7.1c, R7.3b, or R7.3d verification
  deadline already includes its own retries and is not a dirty attempt eligible
  for this budget; it fails the invocation.
- **R8.6b** The first narrow executable runner does not yet classify environment
  health or create fresh isolated retry attempts, so it accepts only a resolved
  `health_retry_limit: 0`. A non-zero resolved value is rejected before artifact
  preparation or Docker with `runner.invocation.health-retries-unsupported`.
  This is a runner capability boundary; the broad v1 default remains `1` under
  R8.6.
- **R8.6c** The first narrow executable runner's remaining topology boundary is
  exact and fail-closed: one plain scenario with `runs: 1`; one Kafka cluster
  containing exactly the distinct input and sink topics; one subject connector;
  one auto-started job at parallelism `1` whose Kafka sink is `EXACTLY_ONCE` or
  `AT_LEAST_ONCE`; one JobManager and one TaskManager; no free-form
  `setup.flink.config`; the bounded input, phase, and validator subset registered
  in R4.5, R6, and R7.3c; and a selected expected outcome of `pass`, or `fail`
  pinned to the `kafka.id-set` oracle and one of its registered reasons (a
  negative control, R8.7a). `AT_LEAST_ONCE` exists for negative controls: a
  recovery is expected to duplicate its output. Valid broad-v1 scenarios outside
  that boundary reject before artifact preparation or Docker with their
  source-aware `runner.*-unsupported` or `runner.*-required` capability issue;
  a `NONE` sink rejects with `runner.workload.delivery-guarantee-unsupported`,
  and an expected failure of another oracle with
  `runner.expectation.outcome-unsupported`. The runner never ignores extra topics,
  connectors, Flink configuration, or a selected expected failure.
- **R8.7** An experiment aggregates attempts as follows:

  | Condition | Verdict |
  | --- | --- |
  | Any side produces `verification.*` | `fail` — comparison/verification failed; no expected oracle is matched |
  | Initial baseline matches and all K candidate attempts match | `pass` |
  | Any clean candidate attempt mismatches and confirmatory baseline matches | `fail` |
  | Any clean candidate attempt mismatches and confirmatory baseline mismatches | `inconclusive` — apparatus invalid |
  | Initial baseline mismatches | `inconclusive` — candidate is not evaluated |
  | Any terminal validator is `inconclusive` under clean health | `inconclusive` |
  | Dirty-health retry budget is exhausted | `inconclusive` |

  Without an experiment, the runner executes until it has K clean attempts: all
  K expectation matches produce `pass`, and the first clean expectation mismatch
  produces `fail`. R8.6 and R8.6a still take precedence for dirty health and
  unevaluable terminal validators, except that R7.1c, R7.3b, or R7.3d verification failures
  take precedence over both.

- **R8.7a** Without an experiment, the first runner turns its single attempt into
  the scenario verdict as follows. An `inconclusive` attempt is an `inconclusive`
  verdict and a `verification.*` failure is a `fail` verdict; neither matches any
  expectation (SPEC-002 E4.2, R7.1a). Otherwise the verdict is `pass` exactly when
  the attempt matches the selected expectation (SPEC-002 E4.3–E4.4): an expected
  `pass` needs a passing attempt, and an expected failure needs a failing attempt
  with the pinned reason and complete experiment evidence: successful phase execution,
  both write fences, a complete terminal snapshot whose failing oracle reason matches
  the attempt, and confirmed effect for every TaskManager kill (R6.12a). Missing phase,
  fence, or oracle evidence makes a matching failure's verdict `inconclusive` with
  `expectation.evidence-unconfirmed`; an ineffective kill uses
  `taskmanager.kill.effect-unconfirmed`. The raw failing attempt and its reason remain
  unchanged. A mismatch is a `fail` verdict. A failing attempt that
  was expected to pass keeps its own reason; an expected failure that did not
  occur, or occurred with another reason, reports `expectation.mismatch`. A
  negative control is therefore green only when it fails exactly as pinned with valid
  experiment evidence.
- **R8.8** N-of-K is reported as evidence strength, never used as a threshold to
  dismiss a clean expectation mismatch. `inconclusive` is reserved for invalid
  evidence, including dirty health, retry exhaustion, an invalid baseline, or an
  unevaluable oracle, except for the explicitly fail-closed terminal verification
  conditions in R7.1c, R7.3b, and R7.3d.

---

## 9. Explicitly out of scope for v1

- **R9.1** Inheritance / `extends` / shared fragments. Duplication across a
  handful of scenarios is not yet a problem, and templating introduced early
  makes both reading and validation much harder. Revisit around fifteen files.
- **R9.2** Automatic matrix *expansion* (one file producing N runs across a
  cartesian product). Parameterization is in (§2); expansion collides with stable
  IDs and expected-result manifests. Expansion belongs to a suite definition
  later.
- **R9.3** Parallel step orchestration (R6.2).
- **R9.4** Multiple JobManagers (R4.12).
- **R9.5** Non-Kafka systems. The envelope must not *prevent* them; nothing in v1
  implements them.

---

## 10. Canonical example

**Consistent with every answer in §11.** This example is illustrative, while the
existing JSON Schema is the authoritative structural contract. The example shows
the shape the schema accepts and the scenario style reviewers should expect.

The canonical scenario is a realistic, non-minimal example. It includes fields
that teach important v1 behavior, but it does not include every optional field.
It describes the broad v1 contract and intentionally exceeds some first-runner
capabilities (for example multi-broker topology and RocksDB); §10.5 is the narrow
first-vertical shape. Unsupported runner capabilities fail before provisioning,
not by narrowing this schema example.
In particular, it omits `meta.tickets` because this example is not tied to a
real upstream issue. A real bug-reproduction or regression guard should list its
tracking issue there, for example `tickets: [FLINK-xxxxx]`; placeholder tickets
are forbidden because they create false traceability.

The canonical example below has three documents: the scenario, its
expected-result sibling, and a suite.

### 10.1 Scenario

`scenarios/kafka/eos-statebackend-independence.yaml`

```yaml
format: v1
kind: scenario

meta:
  # R1.3: scenario name equals the filename, lower-kebab-case, globally unique.
  name: eos-statebackend-independence
  description: >
    Exactly-once delivery must hold regardless of which state backend is
    configured. Suspected record loss with the heap backend under TaskManager
    failure; RocksDB is believed unaffected.

# R2: a parameter declares a name, a type, and a default. It does NOT enumerate
# legal values — whether `forst` is a valid state backend is a fact about the
# harness, not about this scenario (R2.6).
#
# A single run resolves exactly one value per parameter. A parameter takes two
# values in one run only when `experiment.varies` names it, and in that case the
# experiment states both explicitly — see `flink_state_backend`.
parameters:
  flink_state_backend: { type: string,  default: rocksdb }
  kafka_transaction_id_naming_strategy:
    type: string
    default: INCREMENTING
  flink_version:      { type: string,  default: "2.2.0" }
  input_record_count: { type: integer, default: 100000, min: 1 }

# R3: the differential experiment. Expectations live in the sibling document.
experiment:
  claim: >
    Exactly-once holds regardless of state backend. The connector declares EOS
    support; the backend governs how state is stored, not delivery semantics.
  varies: [flink_state_backend]
  baseline:  { flink_state_backend: rocksdb }    # R3.0: not "control"/"treatment"
  candidate: { flink_state_backend: hashmap }

# R8.5: baseline runs once; the candidate must complete five clean attempts.
# Same-attempt health signals, not the baseline, distinguish degraded environments.
runs: 5
health_retry_limit: 1  # R8.6: one shared retry across this scenario invocation
completion_timeout: 10m # R7.1c: bounded jobs must finish before Flink is fenced

setup:
  kafka:
    clusters:
      main:
        # R4.13: declared reference plus resolved digest recorded at run time.
        image: "apache/kafka:4.0.0"
        brokers: 3
        topics:
          - name: input
            partitions: 4
            replication_factor: 3
            # First executable vertical: preload a bounded total, capture exact
            # stopping offsets, then let delayed workload processing span faults.
            input_source:
              mode: generated
              format: integer-sequence
              total: "${input_record_count}"
          - name: output
            partitions: 4
            replication_factor: 3
  flink:
    image: "flink:${flink_version}"
    # R4.11: omitted JobManagers default to 1. This count is explicit because
    # the scenario targets a named TaskManager and tests recovery with a 3-node pool.
    taskmanagers: 3
  # R6.8a: a proxy must be declared before any step may reference it.
  # Omitted here — this scenario injects no network faults.

subject:
  connectors:
    kafka:
      artifact: ./flink-connector-kafka/target/flink-connector-kafka-*.jar
      # This local PR build is thin, so its direct runtime roots are explicit.
      # Maven entries bring their own locked compile/runtime closure (R4.13a).
      runtime_dependencies:
        - maven:org.apache.commons:commons-lang3:3.18.0
        - maven:com.fasterxml.jackson.core:jackson-core:2.18.2
        - maven:com.fasterxml.jackson.core:jackson-databind:2.18.2
        - maven:com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.18.2
        - maven:com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.2
        - maven:org.apache.kafka:kafka-clients:4.2.0

workload:
  jobs:
    - alias: eos-job                       # R5.5: survives restart/upgrade
      # R5.7: omitted `start` defaults to auto before the warmup phase.
      jar: ./flink-job-generator/target/flink-job-generator.jar
      connectors: [kafka]                  # R5.6: subject connector under test
      parallelism: 2
      state_backend: "${flink_state_backend}"
      source: { cluster: main, topic: input }
      sink:
        cluster: main
        topic: output
        delivery_guarantee: EXACTLY_ONCE
        transactional_id_prefix: eos-sb-independence
        # R5.4: mandatory, never implicit.
        transaction_id_naming_strategy: "${kafka_transaction_id_naming_strategy}"
      checkpointing:
        interval: 5s                            # R2.11: duration string
        mode: EXACTLY_ONCE
      # R5.2a: job-specific settings the harness does not model.
      program_args: ["--processingDelayMs", "5"]

phases:
  - name: warmup
    steps:
      # R6.4: typed condition, explicit timeout, explicit on_timeout.
      - await:
          condition: { type: job-state, job: eos-job, state: RUNNING }
          timeout: 2m
          on_timeout: inconclusive
      - await:
          condition: { type: checkpoint-completed, job: eos-job, count: 2 }
          timeout: 2m
          on_timeout: inconclusive

  - name: chaos
    steps:
      # R6.6: loop scopes exactly which steps repeat.
      - loop:
          times: 5
          steps:
            - await:
                condition: { type: log-marker, marker: checkpoint-precommit }
                timeout: 1m
                on_timeout: inconclusive
            # R6.7: discriminated target. v1 accepts `kind: named`; `kind: selector`
            # is reserved and rejected with unsupported-capability.
            - kill:
                target: { kind: named, role: taskmanager, name: taskmanager-2 }
            # These awaits check capacity, not recovery: the job can report RUNNING
            # before the JobManager notices the loss. R6.12a judges the kill's effect
            # from Flink's own restore and failure records.
            - await:
                condition: { type: taskmanager-count, value: 3 }
                timeout: 2m
                # Recovery after a confirmed fault IS the behavior under test.
                on_timeout: fail
            - await:
                condition: { type: job-state, job: eos-job, state: RUNNING }
                timeout: 3m
                on_timeout: fail

terminal_validations:
  - type: kafka.id-set
    cluster: main
    topic: output
    # R4.8: compared against the producer's input manifest, not a literal list.
    expected: input-manifest
    timeout: 2m # includes raw-boundary capture and every Kafka retry/read
  - type: kafka.no-hanging-transactions      # R7.3a: canonical name
    cluster: main
    transactional_id_prefix: eos-sb-independence
    stabilization_timeout: 30s
    timeout: 2m
```

### 10.2 Expected result

`scenarios/kafka/eos-statebackend-independence.expected.yaml`

R1.3b and §11.3: this file holds the expected **outcome** only. The expected
**data** — the actual record IDs — is the run-time input manifest, never a
committed file.

```yaml
format: v1
kind: expected-result

meta:
  name: eos-statebackend-independence.expected
  scenario: eos-statebackend-independence     # CI rejects a missing/mismatched sibling

default:
  baseline:
    outcome: pass
  candidate:
    # R3.5: an expected failure must pin how it fails. A bare `fail` would let
    # an image-pull timeout count as success.
    outcome: fail
    oracle: kafka.id-set
    reason: validator.kafka.id-set.missing-ids # R7.2a: namespaced code

```

If a parameter value changes the expected outcome or failure mode, its case
contains that different complete `baseline`/`candidate` expectation. Redundant
cases are forbidden by document validation: they add no behavior and can drift
silently from `default`.

### 10.3 Suite

`suites/eos-nightly.yaml`

R6.13: membership lives here, and parameter binding at membership is how coverage
is served without N-way experiments (§11.1).

```yaml
format: v1
kind: suite

meta:
  name: eos-nightly

scenarios:
  - scenario: eos-tm-kill-open-transaction
    as: eos-smoke-style
    runs: 1                    # R6.13: suite-specific repetition level

  # `as` is the stable suite-entry identifier. Required whenever a scenario
  # appears more than once, because reports, artifact directories, retries and
  # CI annotations key on it — `meta.name` alone would collide.
  - scenario: eos-statebackend-independence
    as: sb-independence-default
  - scenario: eos-statebackend-independence
    as: sb-independence-pooling
    parameters: { kafka_transaction_id_naming_strategy: POOLING }
    runs: 20                   # nightly evidence level without editing the scenario
  - scenario: eos-statebackend-independence
    as: sb-independence-flink-221
    parameters: { flink_version: "2.2.1" }
```

See R6.13 for the suite-entry contract.

### 10.4 What a version upgrade actually looks like

This is a phase fragment, not a schema-valid standalone scenario: it deliberately
omits the required envelope, setup/workload, expected-result sibling, and
`terminal_validations`. A complete upgrade scenario supplies terminal validators
after its final restore.

An upgrade is **not** a parameter with two values. It needs both versions inside
a single run, in sequence, so it is expressed as phase structure:

```yaml
parameters:
  from_version: { type: string, default: "2.2.0" }
  to_version:   { type: string, default: "2.2.1" }

setup:
  flink:
    image: "flink:${from_version}"

phases:
  - name: upgrade
    steps:
      - savepoint: { job: eos-job, stop: true }
      - await:
          condition: { type: savepoint-completed, job: eos-job }
          timeout: 5m
          on_timeout: fail
      - restart:
          component: flink
          image: "flink:${to_version}"     # the second version enters here
      - restore:
          job: eos-job
          from: latest-savepoint
          mode: no-claim                    # R6.10
```

Both versions resolve to one value each; the *sequence* is what makes it an
upgrade. Contrast with `flink_state_backend` above, where the experiment runs two
configurations independently and compares them.

### 10.5 Minimal scenario shape

This is a compact plain-scenario example showing which common fields may be
omitted. It intentionally has no `meta.tickets`, no `parameters`, no
`experiment`, no `jobmanagers`/`taskmanagers`, no job `start`, and no Kafka
`mode`, `completion_timeout`, per-validator `timeout`, or optional job settings.
It explicitly selects `health_retry_limit: 0` because this example is executable
by the public first-narrow `run` path (R8.6b). Other omissions still resolve
to concrete values in the run report: KRaft Kafka mode, one JobManager, one TaskManager,
auto-started jobs, a two-minute completion deadline, two-minute validator
deadlines, HashMap state, disabled TTL, no watermarks, JobManager checkpoint
storage, and Flink `2.2.x`'s restart default.

```yaml
format: v1
kind: scenario

meta:
  name: eos-minimal
  description: Exactly-once delivery holds for the default local Kafka setup.

runs: 1
health_retry_limit: 0

setup:
  kafka:
    clusters:
      main:
        image: "apache/kafka:4.0.0"
        brokers: 1
        topics:
          - name: input
            partitions: 1
            replication_factor: 1
            input_source:
              mode: generated
              format: integer-sequence
              total: 1000
          - name: output
            partitions: 1
            replication_factor: 1
  flink:
    image: "flink:2.2.0"

subject:
  connectors:
    kafka:
      artifact: "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2"

workload:
  jobs:
    - alias: eos-job
      jar: ./flink-job-generator/target/flink-job-generator.jar
      connectors: [kafka]
      parallelism: 1
      source: { cluster: main, topic: input }
      sink:
        cluster: main
        topic: output
        delivery_guarantee: EXACTLY_ONCE
        transactional_id_prefix: eos-minimal
        transaction_id_naming_strategy: INCREMENTING
      checkpointing:
        interval: 5s
        mode: EXACTLY_ONCE

phases:
  - name: verify-running
    steps:
      - await:
          condition: { type: job-state, job: eos-job, state: RUNNING }
          timeout: 2m
          on_timeout: inconclusive

terminal_validations:
  - type: kafka.id-set
    cluster: main
    topic: output
    expected: input-manifest
```

Its required expected-result sibling is correspondingly small:

```yaml
format: v1
kind: expected-result

meta:
  name: eos-minimal.expected
  scenario: eos-minimal

default:
  outcome: pass
```

### 10.6 Optional fields catalogue

There is deliberately no single "full scenario with every optional field".
Several optional fields are conditional, not additive: `meta.tickets` requires a
real tracking reference; `input_source.mode: custom` cannot use built-in
`expected: input-manifest`; network proxies belong only to network-fault
scenarios; and `expected-result.cases` belongs only when a parameter changes the
expected outcome. A maximal example that includes all of them would teach bad
habits.

Use this catalogue as the checklist for optional v1 fields and patterns:

| Optional field or pattern | Example | Use when |
| --- | --- | --- |
| `meta.tickets` | `tickets: [FLINK-xxxxx]` | A real upstream issue, regression, or coverage task exists. Never use placeholders. |
| Parameter `description` | `description: Flink state backend under test.` | The parameter name is not enough for reviewers. |
| Required parameter | `flink_connector_artifact: { type: string, required: true }` | There is no safe default, such as PR gating a specific connector build. |
| `experiment` | `varies: [flink_state_backend]` | The scenario is a diagnosis pair with a baseline and candidate. |
| Expected-result `cases` | `when: { kafka_transaction_id_naming_strategy: POOLING }` | A parameter value changes the expected outcome or reason code. |
| `health_retry_limit` | `health_retry_limit: 2` | The scenario needs a different dirty-health retry budget from the default. |
| `completion_timeout` | `completion_timeout: 5m` | Bounded jobs legitimately need longer than the default `2m` to reach `FINISHED` before the Flink write fence. |
| Kafka cluster `mode` | `mode: kraft` | Usually omit in v1; declared only when the author wants topology to be explicit. |
| `setup.flink.jobmanagers` | `jobmanagers: 1` | Usually omit; declaring a value above 1 is rejected in v1. |
| `setup.flink.taskmanagers` | `taskmanagers: 3` | The scenario targets or requires a specific TaskManager pool size. |
| Connector `runtime_dependencies` | `runtime_dependencies: []` | Required for a local primary and optional for a Maven primary. Presence selects explicit dependency mode; empty explicitly asserts a shaded/self-contained JAR. Omission selects Maven auto-POM mode and is invalid for local. |
| Job `start` | `start: manual` | A phase intentionally starts the job later with a `start` step. |
| Job `program_args` | `program_args: ["--processingDelayMs", "5"]` | The user job needs arguments outside the harness's typed workload model. |
| Job state/default overrides | `state_backend: rocksdb` | The scenario intentionally differs from the materialized HashMap/TTL-off/no-watermarks/Flink-default-restart contract. |
| Filesystem checkpoint storage | `storage: { type: filesystem }` | State must survive process replacement in the harness-managed attempt/job namespace. No public `path` is accepted. |
| Workload topic `connect_via_proxy` | `sink: { cluster: main, topic: output, connect_via_proxy: kafka-proxy }` | The job edge must route through a declared proxy so network faults affect it. |
| Terminal validator `timeout` | `timeout: 5m` | The fenced Kafka snapshot is large enough to need more than the default `2m`; the absolute clock still includes offset capture and retries. |
| Suite-entry `as` | `as: sb-independence-pooling` | Required when the same scenario appears more than once in a suite. |
| Suite-entry `parameters` | `parameters: { flink_version: "2.2.1" }` | The suite binds a scenario to a specific parameter value. |
| Suite-entry `runs` | `runs: 20` | The suite needs a different evidence level than the scenario default. |

Conditional optional shapes are best shown as fragments:

```yaml
# A parameter-selected expectation. The `when` map cannot name a key from
# `experiment.varies`; it selects common parameters only.
cases:
  - when: { kafka_transaction_id_naming_strategy: POOLING }
    baseline:
      outcome: pass
    candidate:
      outcome: pass
```

```yaml
# A manual-start job. The runner will not submit it before phase execution.
workload:
  jobs:
    - alias: eos-job
      start: manual
      jar: ./flink-job-generator/target/flink-job-generator.jar

phases:
  - name: start-job
    steps:
      - start: { job: eos-job }
```

```yaml
# A held network fault needs a declared proxy plus duration/heal semantics.
# This fragment is illustrative; SPEC-004 defines the exact step schema.
setup:
  kafka:
    clusters:
      main:
        image: "apache/kafka:4.0.0"
        brokers: 1
        topics:
          - name: input
            partitions: 1
            replication_factor: 1
          - name: output
            partitions: 1
            replication_factor: 1
  proxies:
    kafka-proxy:
      type: kroxylicious
      cluster: main
      listen: kafka-proxy:9092
      bootstrap: { cluster: main }

workload:
  jobs:
    - alias: eos-job
      source: { cluster: main, topic: input }
      sink:
        cluster: main
        topic: output
        connect_via_proxy: kafka-proxy

phases:
  - name: network-chaos
    steps:
      - network_fault:
          proxy: kafka-proxy
          target: { cluster: main, broker: broker-1 }
          match: { api: produce, topic: output }
          fault:
            type: delay
            latency: 5s
          duration: 30s
          heal: restore-proxy-rule
```

```yaml
# Custom input is paired with a custom terminal validator, not input-manifest.
input_source:
  mode: custom
  artifact: ./producers/custom-input.jar
  timeout: 2m
  config:
    profile: proprietary-format

terminal_validations:
  - type: custom
    artifact: ./validators/custom-output-validator.jar
    timeout: 2m
    failure_reasons:
      - validator.custom.output.missing-records
```

---
## 11. Design decisions (all resolved)

### Answers settled 2026-08-09

| Q | Answer | Note |
| --- | --- | --- |
| Q1 | (a) selector shape in schema, static names implemented in v1 | Use a **discriminated** target: `kind: named` vs `kind: selector`. v1 rejects `kind: selector` with `unsupported-capability: runtime-target-selector` before any container starts. |
| Q2 | (a) strict pair | Renamed `baseline`/`candidate` (R3.0). N-baselines deferred — see §11.1 below. |
| Q3 | Harness-owned producer, separate from the workload; four input modes | R4.5–R4.10. Writes an input manifest that is the expected-data artifact. |
| Q4 | **Adaptive**, not K full pairs | See §11.2 — neither of the two proposed answers was right. |
| Q5 | Separate `.expected.yaml` sibling | But it holds the expected *outcome* only. Expected *data* is a runtime artifact — see §11.3. |
| Q6 | (b) `kind: suite` exists in v1 | Ordered scenario-ID list with **parameter binding at membership** — the mechanism that serves coverage after Q2 deferred N-way experiments. |
| Q7 | (a) subprocess + versioned JSON protocol | Built-ins stay in-JVM. Crash/malformed/timeout ⇒ `inconclusive`. Same protocol serves the custom input producer (R4.10). |
| Q8 | Required per-`await` `on_timeout`, commonly `inconclusive` | Use explicit `on_timeout: fail` only where reaching the condition is the behavior under test. Infrastructure failures and unconfirmed faults are always `inconclusive`, overriding any `fail` request. |
| Q9 | Duration strings | Grammar: positive integer + `ms`/`s`/`m`/`h`. No decimals, no compounds (`1m30s`), no ISO-8601. |
| Q10 | `loop` confirmed | The change is **scope, not vocabulary** — see §11.4. |
| Q11 | Yes, `meta.name` == filename | Lower-kebab-case, globally unique regardless of directory. |
| Q12 | Bounded-but-continuous in v1; truly unbounded out | R4.6–R4.7. Supersedes both the original question and the review's answer. |

For Q11, “filename” applies to scenario names; expected-result names and paths
are the derived exception defined in R1.3a. For Q7, the subprocess protocol runs
inside the disposable extension container required by R7.5.5.

### Additional implementation decisions settled 2026-08-24

| Decision | Adopted v1 contract | Consequence |
| --- | --- | --- |
| Connector dependency handling | **Dependency-aware closure resolution (option 1A).** A Maven primary with no `runtime_dependencies` uses its effective-POM runtime closure. A present list is the complete explicit root set for either a Maven or local primary; `[]` asserts that the primary is self-contained. | The runner never guesses from filenames or silently combines explicit dependencies with the primary POM. A local primary must declare the field. See R4.13a–R4.13b. |
| First executable compatibility line | **Flink `2.2.x` with the registered Kafka connector coordinate `org.apache.flink:flink-connector-kafka:5.0.<patch>-2.2`**, initially exemplified by `5.0.0-2.2`. | Canonical Maven primaries fail closed unless the coordinate/version pattern is registered. Local connector JARs remain an explicit author compatibility assertion. See R5.6b. |
| Connector/runtime identity during upgrades | **Target-specific locks (option A).** The exact post-interpolation Flink image reference participates in `closure_sha256`; a distinct upgrade target creates another lock even when connector bytes are unchanged. | `flink:2.2.0` and `flink:2.2.1` produce distinct connector/image evidence. A tag's independently resolved OCI digest remains runtime evidence outside the Docker-free lock hash. See R4.13c. |
| Connector deployment | **One deterministic byte-only classpath manifest per effective side, plus a target-specific binding for each distinct Flink image.** The canonical manifest and its JARs are injected under `/opt/flink/lib` before Flink starts; the workload JAR stays separate and must not shade the subject connector. | Every process receives identical verified dependency bytes in stable order even when component image targets differ. `classpath_manifest_sha256` identifies those bytes; `target_binding_sha256` identifies the image/lock pairing. Cross-connector conflicts reject before provisioning. See R5.6a. |
| Flink restart image inheritance | **Track desired image references per logical component and reject ambiguous shorthand.** An image-bearing TaskManager restart requires exactly one TaskManager; a full Flink restart without an image requires one uniform desired target. | v1 never guesses which TaskManager to upgrade or which divergent image a full restart should inherit. After the old process stops, a failed explicit retarget remains desired and is retried without automatic rollback; last-successful provisioning evidence remains separate. See R6.11a. |

### First execution-vertical decisions settled 2026-08-26

| Decision | Adopted v1 contract | Consequence |
| --- | --- | --- |
| Workload JAR compatibility (1A) | **Any workload JAR declaring `Flink-Stability-Workload-Protocol: v1` and implementing the typed configuration contract.** | Artifact validation is byte-bound and not restricted to the bundled generator. The runtime protocol key cross-checks the declaration; legacy opaque arguments are not a fallback. See R4.13d and R5.6c. |
| Omitted workload settings (2A) | **Materialize HashMap state, TTL disabled, no watermarks, JobManager checkpoint storage, and `{ type: flink-default }` restart behavior.** | Raw YAML stays compact while the resolved scenario and report remain explicit. The restart marker preserves Flink `2.2.x` ownership of its default; runtime-effective values are reported. See R5.1a. |
| Filesystem checkpoint storage (3A) | **Harness-managed local `file:/flink/checkpoints/attempt-<ordinal>-<nonce>/<job-alias>` namespaces only.** | Authors select `{ type: filesystem }` but cannot supply a path or arbitrary URI. Attempt/job isolation and the effective generated URI are recorded. See R5.4a. |
| Kafka broker compatibility (4A) | **Apache Kafka `4.0.x` only.** | Setup and Kafka restart images outside or not demonstrably on that line reject before provisioning. See R4.2a. |
| Terminal validation boundary (5A) | **Bounded job completion followed by an irreversible physical Flink write fence.** | v1 preload input yields exclusive stopping offsets and natural `FINISHED`; then one fixed internal `2m` fence budget SIGKILLs TaskManagers before JobManagers and retains per-component/runtime/timestamp evidence before terminal checks. Future controlled-unbounded support requires a finite cutoff plus stop-with-savepoint/drain and is roadmap-only. See R4.7, R5.6c, and R7.1c–R7.1d. |
| Validator deadlines (6A) | **Optional raw `timeout`, default `2m`, mandatory after resolution; fail closed when built-in Kafka verification cannot complete.** | The clock starts immediately after the fence and before Kafka discovery. Discovery, one fixed `read_uncommitted` high-watermark boundary, beginning offsets, `read_committed` traversal, decoding, and comparison share it. Kafka unreachability, unresolved transactions/incomplete traversal, over-limit observation, and bounded-job completion timeout fail with stable `verification.*` reasons that cannot satisfy expected data-integrity failures. See R7.1c, R7.3b, R7.3d, and R8.4. |
| Input-manifest evidence | **Record only observable harness sends and reconciliation facts, with explicit `complete` or `partial` status.** | Kafka-client-internal retries remain opaque rather than being guessed. Failures after acknowledgements and closed bounds preserve partial evidence; earlier failures fabricate none. See R4.8 and R4.14–R4.18. |
| Attempt cleanup boundary | **Wait once under a fixed internal `2m` daemon boundary; never let stuck physical cleanup suppress the structured attempt result.** | Cleanup timeout makes a prior pass inconclusive, but cannot erase an authoritative failure. Prepared artifacts stay caller-owned and detached cleanup may not read them. See R8.2c. |

### §11.1 — Why not N baselines and one candidate

Structurally, "N baselines + 1 candidate" *is* shape (b): a list of
configurations with one designated reference. Agreeing with (a) and wanting N
baselines are therefore in mild tension.

The idea is sound on its merits — knowing that both `rocksdb` and `forst` pass
while `hashmap` fails is stronger evidence than one passing config, because it
argues the culprit is `hashmap` specifically rather than "anything that is not
`rocksdb`". It is deferred purely to keep the pair as an unambiguous unit of
evidence (R3.8) in v1.

**Schema consequence:** shape the experiment block so a list can be added later
without breaking committed files. Prefer a form that generalises over one hard-wired
to exactly two keys.

### §11.2 — Q4 revisited: adaptive baseline execution

Neither original answer survives scrutiny. "K independent pairs" doubles cost for
a baseline that rarely changes. "One baseline for all K" cannot detect an
environment that degrades partway through a suite.

**Adopted:** run the baseline **once**, the candidate **K** times, and re-run the
baseline **on demand** whenever a candidate run fails.

- If the confirmatory baseline passes, the environment was healthy: the candidate
  failure is a real finding.
- If it fails, the environment degraded: the result is `inconclusive`, not a
  finding.

This costs `1 + K` runs on the happy path instead of `2K`.

#### The confirmatory baseline does not close the race

A confirmatory baseline is a proxy measurement taken **at a different time** than
the failure it is meant to explain. If the environment was sick during the
candidate run and recovered before the baseline re-run, the baseline passes and a
transient infrastructure fault is reported as a finding. Running the baseline
sooner narrows the window; nothing closes it.

The baseline was carrying two jobs, and it can only do one:

| Job | Question | Time-sensitive | Baseline suited |
| --- | --- | --- | --- |
| Systematic validity | Is the scenario well-formed, the workload right, the oracle calibrated, the claim's premise sound? | no — a broken scenario fails every time | **yes** |
| Instantaneous health | Was *this run's* environment sound? | yes | **no** |

**Adopted correction.** Environment health is measured directly and
contemporaneously, not inferred from a proxy run. The baseline is retained for
systematic validity only; R8.4–R8.8 define the normative health, retry, and
verdict behavior.

A future option, if the race proves troublesome in practice: run baseline and
candidate **concurrently** on isolated networks so they share environmental
conditions. Deferred because concurrent runs contend for host resources, and that
contention can itself manufacture the failures the design is trying to rule out.

### §11.3 — Expected *outcome* vs expected *data*

Q5 conflated two artifacts with different lifecycles:

| | Expected outcome | Expected data |
| --- | --- | --- |
| Content | pass/fail, which oracle, which reason code | the actual 10 000 record IDs |
| Origin | written by a human | generated at run time |
| Lifecycle | committed, changes only via regression-contract review | run artifact, regenerated every run |
| Location | `<scenario>.expected.yaml` | input manifest (R4.8) |

Auto-generated input cannot have a hand-written expected-data file — nobody
writes out 10 000 IDs. The generated set *is* the expectation, which is exactly
why the producer must be harness-owned and must emit a manifest.

### §11.4 — Q10: the issue is scope, not the word

`loop` is not better than `repeat` as vocabulary; the current `repeat` is a
problem because of **where it sits**. It attaches to a phase, so it re-runs every
step in that phase — including `start` and other lifecycle steps. The README's
own example only works because the repeated phase happens to contain none.

A nested block scopes exactly which steps repeat. Keeping the name `repeat` for
that nested block would be equally fine.

All twelve are now answered. The reasoning for each is kept below, since the
schema and its validation tests should cite it.

Tier 1 changes the architecture. Tier 2 changes the schema's shape. Tier 3 is
convention — quick to answer, but freeze it so it stops being re-litigated.

### Tier 1 — architectural

#### Q1. Targeting model

The §10 example uses a **runtime selector**:

```yaml
kill: { target: { role: taskmanager, hosting: { job: eos-job, vertex: sink, subtask: 0 } } }
```

The harness would resolve which TaskManager hosts sink subtask 0 via the Flink
REST API. The alternative is **static names only** (`taskmanager-2`), which
forces `parallelism: 1` in any scenario needing a specific subtask, so there is
only ever one candidate.

- (a) Selector object in the schema now; v1 implements static names only, the
  resolver lands later.
- (b) Static names only; revisit the schema when it becomes limiting.

*Impact:* (b) means every committed scenario file is rewritten once multi-subtask
scenarios arrive. The selector shape is the expensive part to change, not the
resolver. *Recommendation:* (a).

**Answer:** superseded — see the resolved table at the head of §11.

#### Q2. Experiment shape — pair, or one baseline against many candidates

§3 is currently written as exactly two runs, one baseline and one candidate. But a
dimension like `flink_state_backend` has three real values (`hashmap`, `rocksdb`,
`forst`). Three possible shapes:

**(a) Strict pair.** Exactly one baseline, one candidate. Covering a third value
means a second scenario file.

```yaml
varies: [flink_state_backend]
baseline:  { flink_state_backend: rocksdb, expect: { outcome: pass } }
candidate: { flink_state_backend: hashmap, expect: { outcome: fail, oracle: kafka.id-set, reason: missing-ids } }
```

**(b) One baseline, N candidates.** All compared against the same known-good
baseline.

```yaml
varies: [flink_state_backend]
baseline: { flink_state_backend: rocksdb, expect: { outcome: pass } }
candidates:
  - { flink_state_backend: hashmap, expect: { outcome: fail, oracle: kafka.id-set, reason: missing-ids } }
  - { flink_state_backend: forst,   expect: { outcome: pass } }
```

**(c) No baseline.** A flat list of configurations, each with its own expectation.
Loses the privileged known-good run entirely, and with it a direct check that the
comparison apparatus is valid (R3.6, R8.7).

*The underlying question is which of two different questions the scenario asks:*

| Question | Shape that fits |
| --- | --- |
| *Diagnosis* — "I suspect `hashmap` breaks EOS; prove it by changing only that." | pair |
| *Coverage* — "Does EOS hold across all three backends?" | N candidates |

*Impact:* (b) introduces a semantics problem (a) cannot have. If the baseline
passes, `hashmap` fails as expected, and `forst` fails *unexpectedly* — is the
experiment pass, fail, or partial? What is the reporting unit? (a) keeps the pair
as the atomic unit of evidence per R3.8. *Recommendation:* (a); serve coverage
later through suites running several scenarios rather than one file with N
branches.

**Answer:** superseded — see the resolved table at the head of §11.

#### Q3. Who generates bounded input

The harness pre-loads N records into the topic before the job starts, or a
workload component produces them.

*Impact:* determines whether the acked-history mechanism required for unbounded
input (R4.6) is a second system or an extension of the first.

**Answer:** superseded — see the resolved table at the head of §11.

#### Q4. How `runs: K` and experiments interact

With `runs: 5` and a baseline plus candidate — is that five *pairs* (ten container
runs), or five candidate runs against one baseline run?

*Impact:* the second is roughly half the cost and materially weaker: a baseline
executed once cannot detect an environment that degrades mid-suite.

**Answer:** superseded — see the resolved table at the head of §11.

### Tier 2 — schema shape

#### Q5. Expected results inline or in a separate file

§10 puts `expect:` inline in the experiment block. The plan's repository layout
instead has a sibling `.expected.yaml` per scenario.

*Impact:* inline is simpler to write and read. A separate file makes
re-baselining show up as its own diff in review, which matters given the
"never silently weaken an oracle" rule.

**Answer:** superseded — see the resolved table at the head of §11.

#### Q6. Does `kind: suite` exist in v1

A suite is a named set of scenarios: `selftest`, `eos-smoke`, `eos-nightly`,
`pr-gate`. The question is only *where membership is recorded*.

**(a) A field on the scenario.**

```yaml
meta:
  name: eos-tm-kill-open-transaction
  suites: [eos-smoke, pr-gate]
```

**(b) A separate document.**

```yaml
format: v1
kind: suite
meta:
  name: eos-smoke
scenarios:
  - scenario: eos-tm-kill-open-transaction
  - scenario: eos-tm-kill-precommitted
    parameters: { kafka_transaction_id_naming_strategy: POOLING }   # binding at membership
```

**(c) Both.** Rejected outright — two sources of truth for the same fact.

Three arguments decide it, in increasing order of weight:

1. **Ordering.** A suite may want cheap scenarios first so it fails fast. A field
   cannot express order; a list can.
2. **Visibility of change.** With (a), deleting a scenario file silently shrinks
   the merge gate, and no diff anywhere says "the gate got weaker." With (b), the
   suite file *is* the contract and `git diff` shows it changing. That is exactly
   the property the plan's "never silently weaken an oracle" rule needs.
3. **Parameter binding — the decisive one.** Only (b) lets the same scenario run
   in two suites with different parameters:

   > `eos-smoke` runs `eos-tm-kill-open-transaction` with `INCREMENTING`;
   > `eos-nightly` runs the same scenario with `POOLING`.

   Under (a) this is impossible without suite-conditional parameters inside the
   scenario, which is worse than either option. And this is precisely how
   *coverage* gets served after Q2 deferred N-way experiments (§11.1): not by
   matrix expansion inside one file, but by suites binding parameters at
   membership.

Costs of (b): two files to keep consistent, and a suite can name a scenario that
does not exist — so CI must check referential integrity. Both are cheap. A v1
suite document is roughly ten lines.

**Answer:** **(b)** — `kind: suite` exists in v1, deliberately minimal: an
ordered list of scenario IDs with optional parameter bindings. No inheritance, no
tag queries, no cartesian expansion.

#### Q7. Custom validator transport (R7.5)

**(a) Separate process.** The harness launches the artifact, passes resolved
context as JSON on stdin (or a file path for large context), and requires exactly
one schema-valid JSON result on stdout. `stderr` is captured as diagnostics.

**(b) In-JVM.** The JAR implements a `Validator` interface and is loaded through
a `URLClassLoader` / `ServiceLoader`.

| | (a) subprocess | (b) in-JVM |
| --- | --- | --- |
| Plugin crash / `System.exit` / OOM | Contained; harness records `inconclusive` | **Takes down the runner** |
| Dependency conflicts | None — separate JVM | Plugin's Kafka/Jackson versions collide with the harness's |
| Timeout enforcement | Kill the process; reliable | Interrupting a stuck thread is unreliable |
| Language | Java executable JAR in v1 | Java only |
| Context richness | Serialized: addresses, topics, paths | Live objects, existing helpers |
| Startup cost | ~1s JVM start | None |

The decisive argument is **evidence preservation**, not generic isolation
hygiene. The runner is the process that writes the run report. If a third-party
validator can kill the JVM, a plugin defect destroys the evidence for the entire
run — including evidence about the *real* failure the scenario was investigating.
For a harness whose central purpose is producing trustworthy failure evidence,
that risk is not acceptable to trade for convenience.

The dependency-conflict point is close behind and very concrete: a validator
built against a different Kafka client version than the harness is an ordinary
situation for a proprietary fork, and in-JVM it is a genuine mess.

Startup cost is irrelevant — runs take minutes.

**Answer:** **(a)**, with three qualifications:

- **Built-in validators stay in-JVM.** The subprocess protocol is for third-party
  code only, so the common path pays nothing.
- **Any of** crash, non-zero exit, malformed JSON, oversized output, or timeout
  ⇒ `inconclusive`, never `fail`. A broken plugin is not evidence about the
  system under test.
- **The same protocol serves the custom input producer** (R4.10), in the opposite
  direction. One extension mechanism, two uses.

The serialized protocol is language-neutral, but v1's schema declares only an
`artifact` and no command, interpreter, or image. v1 therefore executes a local
JAR via its `Main-Class`; arbitrary-language executables are deferred until that
launch contract is explicit.

Also required: declared timeout, output-size cap, artifact checksum recorded in
the run report (R7.5.4), and process cleanup on cancellation.

#### Q8. Step failure semantics

An `await` times out waiting for job `RUNNING`. Is that `fail` — the system did
not do what it should — or `inconclusive`, since the precondition was never
reached and the oracle never ran?

*Impact:* the honest answer differs by position: `inconclusive` during setup,
`fail` after the fault has been injected. If so, that distinction has to be
expressible — most likely per phase.

**Answer:** superseded — see the resolved table at the head of §11.

### Tier 3 — convention

#### Q9. Duration format

`10s` / `2m` strings everywhere, or integer fields with a `_ms` suffix?
*Recommendation:* strings, one convention, no mixing.

**Answer:** superseded — see the resolved table at the head of §11.

#### Q10. Loop construct

Confirm the explicit `loop` step (R6.6) replaces phase-level `repeat`.

**Answer:** superseded — see the resolved table at the head of §11.

#### Q11. Must `meta.name` equal the filename

*Recommendation:* yes — one fewer way for identity to drift.

**Answer:** superseded — see the resolved table at the head of §11.

#### Q12. Unbounded input in v1

Is schema-reserved-but-rejected (R4.5, R4.6) acceptable for v1, or is continuous
input needed in the first release?

**Answer:** see the table above and R4.6 — the question contained a false
dichotomy. Continuous *production* is needed in v1; unbounded *totals* are not.

---

## 12. Adopted external-review rationale

These findings informed the numbered requirements above. Where this historical
rationale differs from a numbered requirement, the numbered requirement controls.

- **12.1 CI semantics for `inconclusive`.** The earlier “never gates a PR in
  either direction” wording was too loose — read literally it lets a PR merge
  although a required scenario never ran. The adopted semantics in R8.1 leave the PR
  **unvalidated**, producing a distinct retry/infrastructure status. Not green,
  not attributed to the code change.
- **12.2 Parameter typing and interpolation.** v1 types: `string`, `integer`,
  `boolean`, `duration`. A reference used as a whole scalar preserves its
  type; embedded references are permitted only inside strings
  (`flink:${flink_version}`). References forbidden in map keys, parameter
  definitions, and recursive expansions. Resolve overrides first, validate second,
  record both template and effective sets. **No secrets in parameters.**
- **12.3 Keep `program_args`.** Typed harness options (R5.1) cannot cover a user
  JAR's own settings. Retain a structured `program_args` list. Precedence: typed
  fields configure the harness and canonical workload; a duplicate/conflicting
  program argument for the canonical workload is a semantic error, not a silent
  override.
- **12.4 Reason-code namespaces**, defined before expected results depend on
  them: the four `validator.kafka.id-set.{malformed-ids,unexpected-ids,duplicate-ids,missing-ids}`
  codes, `validator.kafka.transaction.ongoing-after-timeout`,
  `await.job-state.timeout`, `fault.network.unconfirmed`, and
  `infrastructure.image-pull-failed`. Expected failures reference a registered
  validator code, never an error-message substring. The
  `verification.flink.*`/`verification.kafka.*` namespace records fail-closed
  inability to complete verification and is intentionally not expectation-matchable.
- **12.5 Experiment isolation is operational, not just textual.** R3.3 must also
  force separate Docker networks, Kafka cluster IDs, topic names, consumer
  groups, checkpoint/savepoint prefixes, and proxy rule names per run — otherwise
  stale offsets or transactions silently invalidate a comparison. Same seed and
  input manifest within a pair; distinct recorded run ID across pairs.
- **12.6 Fault lifecycle ownership.** Every fault has an ID and an explicit
  lifecycle: `declared → armed → trigger-confirmed → active → healed →
  evidence-checked`. **The runner owns healing in a `finally`**, not a
  best-effort scenario step; the scenario's heal action is the normal path, the
  runner's is the safety net. A fault that never reaches `trigger-confirmed` and
  `evidence-checked` makes the run `inconclusive`.
- **12.7 Savepoint self-containment, defined precisely.** R7.4 means: the
  savepoint metadata and every referenced state file are reachable from shared
  storage *after the original JobManager and all TaskManagers are destroyed*, and
  it is validated by restoring into replacement containers — not by checking that
  a directory exists.
- **12.8 One validator name.** `kafka.no-hanging-transactions` is canonical;
  `kafka.no-ongoing-transactions` is removed. The name describes intent while
  leaving the implementation free to report observed state.
- **12.9 Proxy declaration and routing.** A v1 network-fault scenario must
  declare its Kroxylicious proxy in `setup` before any step references it, and
  affected source/sink endpoints must explicitly opt into that proxy with
  `connect_via_proxy`. A future coarse-TCP proxy may use a separate proxy type.

## 13. Companion artifacts

The following companion artifacts complete the current v1 contract:

1. `SPEC-002-expected-result-schema.md` — expected-result document plus the
   regression-contract-change policy.
2. `SPEC-003-suite-schema.md` — minimal suite membership and parameter binding.
3. `SPEC-004-kroxylicious-fault-model.md` — proxy topology, rule schema, supported
   Kafka APIs, correlation, evidence model.
4. The v1 JSON Schemas under `core/src/main/resources/schema`, plus
   `SPEC-005-semantic-validation-test-cases.md` for the decisions in §11.

---

## 14. Execution-semantics traceability

The execution decisions formerly recorded as review proposals are now normative:

| Concern | Normative requirements |
| --- | --- |
| Expected-result discovery and parameter cases | R1.3a–R1.3c |
| Repetition, health, and verdicts | R8.3–R8.8 |
| Input-source shape and lifecycle | R4.4–R4.10 |
| Input-manifest identity and reconciliation | R4.8 and R4.14–R4.18 |
| Kafka broker compatibility line | R4.2a |
| Parameter precedence and experiment overrides | R2.7–R2.10 |
| Baseline and expected-failure semantics | R3.6–R3.7 |
| Job auto-start semantics | R5.7 and R6.3a |
| Proxy routing for network faults | R5.5a and R6.8b |
| Fault lifecycle, instantaneous vs held faults, and TaskManager-kill effect | R6.9, R6.12–R6.12a, and §12.6 |
| Suite-specific repetition | R6.13 |
| Artifact-under-test declaration, dependency closure, and deployment | R4.13–R4.13d and R5.6–R5.6b |
| Workload defaults, runner capability/storage boundary, and protocol | R5.1a, R5.4a–R5.4b, and R5.6c |
| Terminal completion, write fence, validator deadlines, and bounded observation | R7.1c–R7.1d, R7.3–R7.3d, and R8.4–R8.8 |

The JSON Schema encodes structural constraints; semantic-validation tests encode
the cross-file and cross-field constraints cited above.
