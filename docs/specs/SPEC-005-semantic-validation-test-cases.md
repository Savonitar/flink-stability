# SPEC-005 - Semantic Validation Test Cases v1

**Status:** draft test catalogue derived from SPEC-001 through SPEC-004.
Pre-provisioning cases through artifact/suite/CLI validation, connector
lock/bundle-binding and classpath preparation, and the Testcontainers copy,
pre-start verification, and component-lifecycle infrastructure are implemented
as applicable. The public `run` path now implements the first narrow bounded
plain-scenario execution, physical write fence, `kafka.id-set` terminal
verification, and structured result/evidence cases. Broader scenario-step
orchestration, OCI-digest and complete R8.2 report assembly, environment health,
repetition/experiment verdicts, and complete broad-v1 execution remain roadmap
work.

**Last updated:** 2026-08-26; first execution-vertical decisions 1A–6A and their
implemented narrow-runner cases are captured below. Broader contract cases are
not thereby marked implemented.

JSON Schema validates local structure. These cases define cross-file,
cross-field, and resolved-configuration checks that must run before
provisioning.

When a field contains parameter templates, structural validation checks that the
raw field is in a syntactically supported position. Semantic validation then
validates the resolved value against the same capability and grammar rules as a
literal value.

Each case should become at least one unit test with a minimal fixture and the
expected validation code.

## 1. Document dispatch and siblings

| ID | Case | Expected result |
| --- | --- | --- |
| SV-001 | Missing `format`. | Reject before dispatch. |
| SV-002 | `format: v2` with a v1 runner. | Reject unsupported format. |
| SV-003 | Unknown `kind`. | Reject before provisioning. |
| SV-004 | Scenario has no `X.expected.yaml` sibling. | Reject missing expected result. |
| SV-005 | Expected-result sibling has wrong `meta.scenario`. | Reject mismatch. |
| SV-006 | Expected-result sibling has wrong `meta.name`. | Reject mismatch. |
| SV-007 | Recursive discovery sees `kind: expected-result`. | Do not execute as scenario. |
| SV-008 | Two expected-result files target the same scenario. | Reject duplicate target. |
| SV-009 | Scenario omits required `meta.description`. | Reject structurally. |
| SV-010 | Expected-result omits `meta.description`. | Accept if all required expected-result fields are present. |
| SV-011 | Suite omits `meta.description`. | Accept if all required suite fields are present. |
| SV-012 | Scenario contains an unknown top-level field. | Reject structurally before provisioning. |
| SV-013 | Expected-result contains an unknown field. | Reject structurally before provisioning. |
| SV-014 | Suite entry contains an unsupported field. | Reject structurally before provisioning. |
| SV-015 | `network_fault` step contains an unknown field. | Reject structurally before provisioning. |
| SV-016 | Scenario file path basename does not match `meta.name`. | Reject mismatch. |
| SV-017 | Expected-result file references no discovered scenario. | Reject orphan expected result. |
| SV-018 | Two scenario files declare the same global `meta.name`. | Reject duplicate scenario name. |

## 2. Parameters and interpolation

| ID | Case | Expected result |
| --- | --- | --- |
| SV-020 | Submit-time override names undeclared parameter. | Reject unknown parameter. |
| SV-021 | Suite binding names undeclared parameter. | Reject unknown parameter. |
| SV-022 | Experiment side overrides key not listed in `experiment.varies`. | Reject scenario definition. |
| SV-023 | Submit-time override names key in `experiment.varies`. | Reject override. |
| SV-024 | Resolved baseline/candidate configs differ outside `experiment.varies`. | Reject implementation/configuration error. |
| SV-025 | Parameter reference used in map key. | Reject unsupported interpolation. |
| SV-026 | Recursive parameter expansion. | Reject recursive expansion. |
| SV-027 | Whole-scalar integer parameter resolves into integer field. | Preserve type. |
| SV-028 | Embedded integer parameter resolves inside string. | Produce string. |
| SV-029 | Duration parameter resolves to `1m30s`. | Reject duration grammar. |
| SV-030 | Capability field resolves to unsupported state backend. | Reject unsupported capability. |
| SV-031 | Whole-scalar integer parameter is used for `brokers`, topic partitions, or job parallelism. | Preserve integer type and validate bounds after resolution. |
| SV-032 | Parameter reference is used for a scenario-local alias or map key. | Reject unsupported interpolation. |
| SV-033 | Embedded topic template `input-${suffix}` resolves to a valid Kafka topic name. | Accept after resolved-value validation. |
| SV-034 | Embedded topic template resolves to an invalid Kafka topic name. | Reject before provisioning. |
| SV-035 | Scenario body references an undeclared parameter, or a declared required parameter with no effective value. | Reject unresolved reference before provisioning. |
| SV-036 | `experiment.varies` names an undeclared parameter. | Reject scenario definition. |
| SV-037 | Required parameter has no effective value after all applicable bindings. | Reject the affected side before interpolation. |
| SV-038 | Integer parameter declares `min` greater than `max`. | Reject incoherent bounds. |
| SV-039 | Integer default, suite binding, submit override, or side override falls outside the declared bounds. | Reject the offending value even if a higher-precedence value would replace it. |
| SV-039a | Experiment claim interpolates a pair-common parameter. | Resolve it once and record the effective pair-level claim. |
| SV-039b | Experiment claim interpolates a key in `experiment.varies`. | Reject because the pair has no single effective value for that key. |
| SV-039c | Experiment claim resolves to empty or whitespace-only text. | Reject with a source-aware claim diagnostic. |

## 3. Expected-result cases

| ID | Case | Expected result |
| --- | --- | --- |
| SV-040 | Expected failure omits `oracle`. | Reject. |
| SV-041 | Expected failure omits `reason`. | Reject. |
| SV-042 | Expected pass declares `oracle`. | Reject. |
| SV-043 | Experiment baseline declares `outcome: fail`. | Reject. |
| SV-044 | Expected failure names inline `validate` step instead of terminal validator. | Reject. |
| SV-045 | Expected failure names unknown reason code. | Reject. |
| SV-046 | Case `when` references undeclared parameter. | Reject. |
| SV-047 | Case `when` names a key in `experiment.varies`. | Reject. |
| SV-048 | Two cases overlap. | Reject overlap. |
| SV-049 | Case replacement equals `default`. | Reject redundant case. |
| SV-050 | Suite-entry `as` differs but parameters are equal. | Select same expected case. |
| SV-051 | Plain scenario uses an experiment expectation, or experiment uses a plain expectation. | Reject every mismatched default or case shape. |
| SV-052 | An unselected case has an invalid typed value, unsupported capability, or produces a structurally invalid resolved scenario. | Reject the expected-result contract before selection or provisioning. |
| SV-053 | Two top-level terminal validators declare the same `type`. | Reject ambiguous terminal oracle identity. |
| SV-054 | Expected failure uses `oracle: custom`, but its custom terminal validator omits the expected code from `failure_reasons`. | Reject unknown oracle/reason pair. |
| SV-055 | Expected failure uses `oracle: custom` with a reason declared by its sole custom terminal validator. | Accept oracle/reason preflight. |
| SV-056 | Expected failure names `kafka.id-set` and one of `malformed-ids`, `unexpected-ids`, `duplicate-ids`, or `missing-ids` in the full `validator.kafka.id-set.*` namespace. | Accept the registered oracle/reason pair. |
| SV-057 | Expected failure names any `verification.*` reason, including Kafka unreachability/incomplete observation or Flink terminalization. | Reject before provisioning; inability to verify never satisfies an expected data-integrity failure. |
| SV-058 | Expected failure names an unregistered `kafka.id-set` reason despite matching the namespace grammar. | Reject fail-closed oracle/reason preflight. |

## 4. Suite validation

| ID | Case | Expected result |
| --- | --- | --- |
| SV-060 | Suite entry names missing scenario. | Reject. |
| SV-061 | Same scenario appears twice without explicit `as`. | Reject. |
| SV-062 | Two entries have same explicit `as`. | Reject. |
| SV-062a | A default entry ID collides with another entry's explicit `as`. | Reject both ambiguous identities. |
| SV-063 | Suite entry overrides `health_retry_limit`. | Reject unknown/unsupported field. |
| SV-064 | Suite entry `runs: 0`. | Reject. |
| SV-065 | Suite binds parameter listed in `experiment.varies`. | Reject. |
| SV-065a | Suite binding or global submit override controls scenario `runs` through parameter interpolation. | Reject; use suite-entry `runs`. |
| SV-065b | Suite binding or global submit override controls `health_retry_limit` through parameter interpolation. | Reject; the budget is scenario-owned. |
| SV-065c | Experiment baseline and candidate resolve different `health_retry_limit` values. | Reject because the invocation has one shared retry budget. |
| SV-066 | Suite binding changes expected-result case. | Record selected case in report. |
| SV-066a | Global submit override is declared by one suite scenario but not another. | Reject the whole suite and identify the incompatible entry. |
| SV-066b | A later suite entry has invalid parameters or topology. | Reject planning before the first entry provisions; return no partial plan. |
| SV-066c | A suite has large `runs` or an experiment with `runs: K`. | Preserve K without materializing K invocations; logical slots are lazy and experiment slots are candidate-only. |

## 5. Setup and workload references

| ID | Case | Expected result |
| --- | --- | --- |
| SV-080 | Topic replication factor exceeds broker count. | Reject. |
| SV-081 | Job source references missing Kafka cluster. | Reject. |
| SV-082 | Job source references missing topic. | Reject. |
| SV-083 | Job sink references missing Kafka cluster. | Reject. |
| SV-084 | Job sink references missing topic. | Reject. |
| SV-085 | Job references missing subject connector alias. | Reject. |
| SV-085a | A `subject.connectors` alias is referenced by no `workload.jobs[].connectors` list. | Reject resolved semantic preflight before artifact download/preparation. Every declared v1 subject connector must enter the cluster under test; do not report or silently omit an unused declaration. |
| SV-086 | Exactly-once sink omits `transaction_id_naming_strategy`. | Reject. |
| SV-086a | Exactly-once sink omits `transactional_id_prefix`. | Reject. |
| SV-087 | Non-exactly-once sink declares `transactional_id_prefix`, `transaction_id_naming_strategy`, or `transaction_timeout`. | Reject conflict. |
| SV-088 | `jobmanagers: 2`. | Reject unsupported multiple JobManagers. |
| SV-089 | Omitted Kafka `mode`. | Resolve to `kraft`. |
| SV-090 | Kafka `mode: zookeeper`. | Reject unsupported capability. |
| SV-090a | Setup and every explicit Kafka restart image use an exact official Apache Kafka `4.0.x` tag such as `apache/kafka:4.0.0` or `apache/kafka:4.0.1`, optionally pinned as `apache/kafka:4.0.1@sha256:<64-lowercase-hex>`. | Accept the initial v1 broker capability and record each declared reference/resolved digest. |
| SV-090b | A setup or explicit Kafka restart image resolves to Kafka `3.x`, `4.1.x`, another line, or a broker version that cannot be established. | Reject before provisioning with `runner.kafka.image-version-unsupported`; an opaque/local-looking image name is not an author compatibility assertion. |
| SV-090c | A structurally valid v1 scenario declares more than one Kafka broker or a topic `replication_factor` above `1` for the first narrow executable runner. | Reject before artifact preparation/Docker with `runner.kafka.broker-count-unsupported` and/or `runner.kafka.replication-factor-unsupported`; do not imply the broader v1 format forbids that topology. |
| SV-091 | Scenario omits required `subject`. | Reject structurally. |
| SV-092 | `subject.connectors` is empty. | Reject structurally. |
| SV-093 | A subject connector omits `artifact`. | Reject structurally. |
| SV-094 | Declared connector artifact cannot be resolved or checksummed. | Reject unresolved artifact before execution. |
| SV-094a | Exact local connector or workload JAR resolves under the explicit artifact root. | Copy it into private content-addressed staging; record declared reference, staged real path, and lowercase SHA-256 before provisioning. Mutating the source afterward does not mutate the prepared bytes. |
| SV-094b | Final-filename build glob matches exactly one regular JAR. | Resolve that JAR deterministically. |
| SV-094c | Build glob matches zero or more than one regular JAR. | Reject not-found or ambiguous artifact; never pick one. |
| SV-094d | Glob metacharacter occurs in a directory component, or a file input uses a glob. | Reject unsupported local-reference shape. |
| SV-094e | A JAR-role source lacks a case-insensitive `.jar` suffix, has any entry that cannot fully decompress/pass integrity checks, or an executable workload/custom extension lacks `Main-Class`. | Reject before provisioning. Connector primary/runtime-dependency JARs require complete archive integrity but not `Main-Class`. |
| SV-094f | Subject connector uses immutable `maven:<groupId>:<artifactId>:<version>` and omits `runtime_dependencies`. | Select auto mode; resolve and stage its primary JAR plus locked Maven runtime closure from the local cache or fixed Maven Central. |
| SV-094g | Maven coordinate has a classifier/type/range/dynamic/snapshot version, or appears outside a subject connector's `artifact`/`runtime_dependencies`. | Reject unsupported v1 reference. |
| SV-094h | Offline Maven resolution misses any required JAR, parent/imported POM, or other effective-model descriptor in the local cache. | Reject with a distinct offline-cache-miss diagnostic identifying the missing coordinate; do not contact a repository. |
| SV-094i | File/custom input, terminal custom validator, or deeply nested inline custom validator declares an artifact. | Resolve and checksum every applicable field at its exact JSON pointer. |
| SV-094j | Experiment sides use the same artifact reference, or vary it. | Store one common identity for the former; preserve baseline/candidate identities for the latter. |
| SV-094k | A suite has artifact failures in multiple entries. | Aggregate every entry-aware issue and return no partial prepared suite. |
| SV-094l | Local reference escapes with `..`, names an absolute outside-root path, or uses an in-root symlink that resolves outside the artifact root. | Reject before reading or checksumming the target. An internal normalization such as `sub/../job.jar` remains allowed. |
| SV-094m | A glob occurs in a path component later removed by normalization, such as `build-*/../job.jar`. | Reject; normalization cannot hide unsupported pattern placement. |
| SV-094n | Existing content-addressed staged file does not hash to the digest in its filename, or the private staging path escapes through a symlink. | Reject the corrupt or redirected staging cache. |
| SV-094o | Prepared scenario/suite is closed, validate-only preparation completes, or preparation fails after making some copies. | Remove that invocation's private staging tree without following links; retain no unbounded validate-only copies. |
| SV-094p | A literal local connector omits `runtime_dependencies`, or a parameterized connector resolves local with the field absent. | Reject the connector definition before provisioning. Raw validation may defer only while `artifact` still contains a template. |
| SV-094q | Either a Maven or local primary declares `runtime_dependencies: []`. | Select explicit mode, stage only the primary, record the self-contained assertion, and do not inspect bytecode or fall back to the primary's POM. |
| SV-094r | Either primary declares a non-empty explicit list mixing local JAR references and canonical Maven coordinates. | Preserve declared root order. Each local root contributes exactly its selected JAR; each Maven root contributes its selected JAR and locked compile/runtime closure. Do not derive dependencies from the primary's POM. |
| SV-094s | A traversed Maven graph contains `compile`, `runtime`, `provided`, `test`, `system`, `import`, optional, and excluded edges. | Include only selected JARs reachable through compile/runtime non-optional, non-excluded edges. Use parent/BOM POMs only as checksummed descriptor evidence. |
| SV-094t | A selected transitive uses a range/dynamic/snapshot version, unresolved property, or non-JAR runtime type. | Reject the closure before provisioning with coordinate and dependency path. |
| SV-094u | Two graph paths select the same Maven conflict key with different versions at different/equal depths. | Use nearest-wins, then first breadth-first encounter; explicit-root order and effective-POM sibling order break ties. Connector is classpath index zero and selected dependencies retain first-encounter order independent of cache/filesystem/download order. Record every loser/winner decision. |
| SV-094v | Preparation resolves a connector closure for one effective side and target Flink image. | Create an immutable target-specific lock containing mode, exact post-interpolation target reference, ordered artifact identities/origins/checksums, checksummed Maven descriptors, and conflict decisions. Hash the canonical versioned UTF-8 projection. Execution consumes only staged bytes named by that lock. |
| SV-094w | An experiment uses a Maven baseline and local candidate with one shared explicit `runtime_dependencies` list. | Resolve the same ordered dependency identities/checksums on both sides and preserve only the primary connector identity as varied evidence; reject a hidden side-specific fallback to the Maven primary's POM. |
| SV-094x | Multiple jobs reference multiple connector aliases. | Form one side-specific classpath merge by lexicographic alias order and each connector's locked order; coalesce identical entries and reject distinct bytes for one Maven conflict key before container start. For each alias, locks across target images must agree on that alias's entry bytes/order and reproduce the same side-wide merged classpath. |
| SV-094y | A prepared connector classpath is provisioned. | Copy the exact canonical byte-only manifest and every listed JAR into the existing `/opt/flink/lib` of every JobManager and TaskManager before Flink starts, without hiding distribution JARs; reject any container checksum/manifest mismatch. Keep the executable workload JAR separate for REST submission. |
| SV-094z | Effective setup and any explicit Flink/JobManager/TaskManager restart images use Flink `2.2.x` with Kafka connector `5.0.x` built for the `2.2` line, including `5.0.0-2.2`. | Accept the initial v1 compatibility capability. Any canonical Maven primary not matching the registered coordinate/version pattern rejects before provisioning; only a local primary is recorded as the author's compatibility assertion. |
| SV-094aa | Two otherwise identical closure locks use different host source/staging paths, timestamps, side/scope values, or independently resolved OCI digests. | Produce identical `closure_sha256` values. The full report retains applicable paths, side, timestamps, and runtime digest, but the stable projection excludes them. |
| SV-094ab | A closure's alias, primary declared reference/SHA-256, exact target image reference, dependency mode, ordered classpath identity/root/checksum, descriptor identity/checksum, or mediation decision changes. | Produce a different `closure_sha256`. Serialize format `flink-stability.connector-closure-lock/v1` as UTF-8 JSON with lexicographically sorted object keys, no insignificant whitespace, and arrays in semantic order. |
| SV-094ac | Auto mode selects a transitive entry, or explicit mode selects a dependency rooted at `runtime_dependencies[2]`. | Record the auto transitive as a non-primary classpath entry with origin root `primary`; record the explicit entry with origin root `runtime_dependency` and original index `2`. Never reinterpret an auto transitive as an explicit root. |
| SV-094ad | A local entry resolves at different canonical absolute paths but has the same bytes and logical declaration. | Use local kind plus content SHA-256 as its stable projection identity. Preserve the canonical local source and staged paths only in full evidence. |
| SV-094ae | Target declarations are `flink:2.2.1`, `docker.io/library/flink:2.2.1`, and `flink:2.2.1@sha256:<digest>`. | Treat the exact resolved declaration strings as distinct hash inputs; do not Docker-normalize them. A digest explicitly present after `@sha256:` participates in the lock hash, while a digest later resolved from a tag does not. |
| SV-094af | Setup uses `flink:2.2.0`, an upgrade restart explicitly uses `flink:2.2.1`, and later restarts reuse either reference. | Before the first container, create one lock per connector and one target binding for each of the two distinct target references, while both bindings reference the same side-wide byte-manifest hash. Reuse the corresponding lock/binding for repeated references or restarts without a new image; do not collapse the upgrade into one image-independent lock. |
| SV-094ag | Jobs list connector aliases repeatedly and in different orders; their locks contain overlapping entries. | Form the side-specific union, order aliases lexicographically, traverse each connector in closure order, and assign unique global indexes by first encounter. Job/list declaration order does not change the byte-only classpath manifest or target bindings. |
| SV-094ah | Maven/Maven, Maven/local, or local/local entries from different aliases have the same SHA-256, including two versions for one Maven conflict key with identical bytes. | Coalesce the bytes into one global entry, keep its first-encounter position/staged source, and retain every alias/closure-index contributor. |
| SV-094ai | Two merged classpath entries have one Maven conflict key but distinct SHA-256 values. | Reject before constructing or starting any container. Diagnose the conflict key and both aliases, closure indexes, Maven identities, declaration paths, and digests. Do not choose a winner by alias order. |
| SV-094aj | A staged connector JAR is missing, unreadable, or changes after its lock was created. | Rehash immediately before prepared-classpath acceptance and reject the stale entry before any container starts, reporting alias, closure index, staged path, expected digest, and actual digest or read failure. |
| SV-094ak | A verified side-specific classpath contains several unique entries. | Name each JAR `/opt/flink/lib/flink-stability-connector-%08d-<full-sha256>.jar`, using its contiguous zero-based index padded to exactly eight digits and all 64 lowercase digest characters. Write canonical JSON whose only top-level fields are `format: flink-stability-connector-classpath-v1` and ordered `entries` to `/opt/flink/lib/.flink-stability-connector-classpath-v1.json`; each entry contains only `index`, basename `filename`, and `sha256`. Record the external `classpath_manifest_sha256`. |
| SV-094al | Initial, replacement, and upgraded Flink containers for one effective side use different valid target-image bindings. | Supply every container with identical byte-only classpath-manifest bytes and listed JAR checksums before its Flink process starts. Keep each target-specific binding as planning/report evidence rather than copying it as the supposedly uniform container manifest. Reject the first observed manifest/checksum mismatch without hiding distribution JARs. |
| SV-094am | Two byte-only classpath manifests differ only in target image, aliases/lock hashes, contributors/host paths, side/scope, timestamps, or runtime OCI digest; alternatively, entry index, filename, order, or SHA-256 changes. | Produce the same `classpath_manifest_sha256` for excluded-evidence changes and a different hash for any canonical entry change. Apply sorted-key, semantic-array-order, whitespace-free UTF-8 JSON rules to format `flink-stability-connector-classpath-v1`. |
| SV-094an | Two target-specific bindings reference the same byte-only classpath manifest but differ in target image or alias/`closure_sha256` pair; alternatively, only host paths, contributors, side/scope, timestamps, runtime digest, or a redundant entry list changes. | Canonical JSON has only `format: flink-stability.connector-cluster-bundle/v1`, exact `target_flink_image_reference`, alias-sorted `closures` items containing only `alias` and `closure_sha256`, and `classpath_manifest_sha256`. Produce a different external `target_binding_sha256` for a canonical target/lock/manifest-hash change and the same hash for excluded evidence. Do not embed the external hash or expose a second binding-hash name. |
| SV-094ao | A target binding omits/adds a referenced alias, names a lock from another side or another exact target, or its locks re-merge to a different byte-manifest hash. | Reject Docker-free preparation before constructing any container. Diagnose the effective side, exact target, missing/unexpected alias or mismatching lock, closure/global indexes when applicable, and expected versus recomputed `classpath_manifest_sha256`. |
| SV-094ap | Identical local or Maven-selected bytes coalesce within one connector closure and the retained prepared artifact has multiple origins matching the same effective side. | Accept. Use the first matching origin in deterministic R4.13b encounter order as the lock projection's singular origin; retain every matching origin in full prepared/report evidence. Do not reject valid coalescing as origin-ambiguous. |
| SV-094aq | An attempt constructs initial Flink containers and may later construct replacements that need the prepared connector classpath. | Keep the executing `PreparedScenarioPlan` and its private staging tree open through host verification, copy, and pre-start verification for every initial/replacement container. Close them only during attempt teardown after no later replacement can be constructed; validate-only and failed-preparation paths still close/clean promptly. |
| SV-094ar | Any staged workload JAR, including one not bundled with the harness, has `Main-Class` and exactly `Flink-Stability-Workload-Protocol: v1` in its main manifest. | Accept its workload-protocol capability after archive validation/checksum; do not whitelist a filename or implementation class. |
| SV-094as | A workload JAR omits, duplicates, blanks, or supplies an unsupported value for `Flink-Stability-Workload-Protocol`. | Reject the staged artifact before provisioning with a workload-protocol diagnostic; do not fall back to legacy `program_args`. |
| SV-094at | The runner constructs the workload v1 configuration. | Supply an immutable lexicographically key-ordered map containing `flink-stability.workload.protocol=v1`, the exact R5.6c data-path/operator keys, and standard Flink settings through their native keys; use exactly-once transaction timeout `7200000` ms and Kafka `transaction.max.timeout.ms=7200000`. Do not serialize typed fields as program arguments or let a workload override resolved values. |
| SV-094au | An 11-partition input topic's closed preload snapshot has valid exclusive `long` offsets, including `0` for an empty partition. | Emit exactly one pair for each partition `0..10` in canonical numeric order (so partition `2` precedes `10`), parse it losslessly, and configure the Kafka source with `setBounded(OffsetsInitializer.offsets(map))`. |
| SV-094av | `source.stopping-offsets` is empty or contains whitespace, an empty pair, a sign, a leading zero, a missing/extra colon, duplicate or non-increasing numeric partitions, a missing/extra declared partition, a negative offset, or integer/long overflow. | Reject the protocol value before job submission. Lexicographic order (`10` before `2`) does not satisfy numeric ordering. |
| SV-094aw | The first executable bounded vertical cannot derive a closed preload stopping-offset map, or the runtime protocol omits the required key. | Fail before authoritative execution/validation; omission is reserved for a future controlled-unbounded cutoff-and-drain capability and is not executable in v1. |
| SV-094ax | The JAR declares protocol v1 but observably rejects the runtime cross-check, fails submission/completion after receiving bounded stopping offsets, or produces output inconsistent with the supplied contract. | Record the resulting workload/terminal failure and do not claim a conforming result. A marker on an arbitrary external JAR is an author assertion, not proof that silent operator-level behavior honors every field; require separate protocol-conformance integration coverage before trusting it as a CI oracle. |
| SV-094ay | A Maven effective model contains JDK-activated profiles, including the published Flink connector parent profiles. | Evaluate profile activation with canonical Java 21 model properties independent of the host JVM; include Java-21-selected compile/runtime dependencies, exclude later-JDK profiles, and reject any unresolved selected model. Maven settings, user properties, and environment variables remain outside v1 closure resolution. |
| SV-094az | A prepared workload path is mutated or replaced after its recorded digest is checked but before the HTTP transport consumes the upload body. | Copy into a private single-call snapshot, verify that snapshot against the prepared digest, upload that same snapshot synchronously, and delete it. The transport receives the verified bytes; a mismatch rejects before REST upload. |
| SV-095 | Source or sink `connect_via_proxy` names a missing proxy. | Reject. |
| SV-096 | Source or sink `connect_via_proxy` names a proxy attached to another Kafka cluster. | Reject. |
| SV-097 | Proxy `bootstrap` uses `{ cluster: main }`. | Resolve to that declared cluster's bootstrap. |
| SV-098 | Proxy `bootstrap` uses `{ address: broker:9092 }`. | Use the explicit listener address. |
| SV-099 | Proxy `bootstrap` declares both `cluster` and `address`. | Reject structurally. |
| SV-099a | Proxy declares `cluster: main` and `bootstrap: { cluster: other }`. | Reject cross-cluster proxy bootstrap. |
| SV-099b | `program_args` contains a token beginning `--flink-stability.workload.`, exact `--bootstrapServers`, or `--bootstrapServers=<value>`. | Reject with `runner.workload.program-args-conflict`; preserve every other non-empty argument exactly and in order without interpreting workload-specific syntax. |
| SV-099c | `state_ttl: { enabled: true }` omits `ttl`. | Reject structurally or after resolved-value validation. |
| SV-099d | `watermarks: { strategy: bounded-out-of-orderness }` omits `max_out_of_orderness`. | Reject structurally or after resolved-value validation. |
| SV-099e | `restart_strategy: { type: fixed-delay }` omits `attempts` or `delay`. | Reject structurally or after resolved-value validation. |
| SV-099f | `restart_strategy: { type: failure-rate }` omits `max_failures`, `failure_rate_interval`, or `delay`. | Reject structurally or after resolved-value validation. |
| SV-099g | `checkpointing.storage: "${storage}"`. | Reject; storage is an object in v1. |
| SV-099h | Proxy `listen` omits `host:port` address shape. | Reject structurally before provisioning. |
| SV-099i | Proxy `bootstrap: { address: ... }` omits `host:port` address shape. | Reject structurally before provisioning. |
| SV-099j | Proxy address uses a port outside `1..65535`. | Reject structurally before provisioning. |
| SV-099k | A job omits `state_backend`, `state_ttl`, `watermarks`, `checkpointing.storage`, and `restart_strategy`. | Materialize `hashmap`, `{ enabled: false }`, `{ strategy: no-watermarks }`, `{ type: jobmanager }`, and `{ type: flink-default }` respectively before execution; record each declared omission and effective value. For restart, also report the Flink `2.2.x` line and runtime-effective strategy/options. |
| SV-099l | `checkpointing.storage` is `{ type: filesystem }`. | Accept and generate `file:/flink/checkpoints/attempt-<ordinal>-<nonce>/<job-alias>`; mount the isolated attempt storage into all Flink processes and report the effective URI. |
| SV-099m | Filesystem checkpoint storage declares `path`, a `file:` URI, an object-store/distributed-filesystem URI, or any other author-selected location. | Reject structurally; v1 exposes no public path field and never accepts an arbitrary storage URI. |
| SV-099n | Two jobs or two attempts select filesystem checkpoint storage. | Generate distinct job/attempt namespaces so neither can read or overwrite the other's state; preserve each mapping in the report. |
| SV-099o | A job explicitly declares `restart_strategy: { type: flink-default }`. | Accept, supply no job-specific restart override, and report the effective Flink `2.2.x` runtime strategy/options. |
| SV-099p | A structurally valid v1 job selects `state_backend: rocksdb` for the first narrow executable runner. | Reject before artifact preparation/Docker with `runner.workload.state-backend-unsupported`; do not imply the broader v1 schema removed RocksDB. |
| SV-099q | A structurally valid v1 job selects `none`, `fixed-delay`, or `failure-rate` restart behavior for the first narrow executable runner. | Reject before artifact preparation/Docker with `runner.workload.restart-strategy-unsupported`; do not rewrite it to `flink-default` or narrow the broad schema. |
| SV-099r | A first-runner job combines its required `state_backend: hashmap` with `state_ttl.cleanup: rocksdb-compaction-filter`. | Reject before artifact preparation/Docker with `runner.workload.state-ttl-cleanup-unsupported`; do not silently accept a RocksDB-only cleanup policy that cannot apply. |

## 6. Input source and terminal validators

| ID | Case | Expected result |
| --- | --- | --- |
| SV-100 | Built-in `kafka.id-set` validator with no `input_source` on source topic. | Reject no expected-data artifact. |
| SV-101 | `input_source.mode: custom` with `expected: input-manifest`. | Reject. |
| SV-102 | `input_source.mode: custom` without custom terminal validator. | Reject. |
| SV-103 | Built-in generated input with custom-only expected result. | Reject validator mismatch. |
| SV-104 | Generated topic uses compaction. | Reject. |
| SV-105 | Generated-rate input has no `total`. | Reject structurally. |
| SV-106 | Manifest contains `acknowledged-missing` event. | Attempt inconclusive with manifest reason. |
| SV-107 | Manifest contains indeterminate event. | Attempt inconclusive. |
| SV-108 | More than one `input_source` appears in a v1 scenario. | Reject ambiguous input-manifest selection. |
| SV-109 | Preload `file` or `generated` input with an auto-start job. | Verify input producer completes and manifest closes before job submission. |
| SV-110 | `generated-rate` input has a finite `total` and phase execution. | Accept the bounded v1 shape. Once its advertised execution capability is enabled, start the producer with phase execution and wait for the manifest/cutoff to close before terminalization. |
| SV-110a | The initial narrow execution vertical is asked to execute `file`, `generated-rate`, or `custom` input instead of preload `generated`. | Reject before artifact preparation/Docker with `runner.input.mode-unsupported`; do not omit bounded stopping offsets or pretend the job can finish naturally. The broader v1 schema remains unchanged. |
| SV-110b | Initial-runner `generated` input uses a format other than `integer-sequence`. | Reject before artifact preparation/Docker with `runner.input.format-unsupported`; the broad v1 input-format contract is not narrowed. |
| SV-110c | Initial-runner `generated` input resolves `total` above 1,000,000. | Reject before artifact preparation/Docker with `runner.input.total-unsupported`; do not impose the narrow runner's 1,000,000-record cap on the broad v1 schema, and require streaming or spill-backed manifest/validator evidence before lifting the execution cap. |
| SV-110d | The first runner successfully closes generated input. | Emit an immutable `complete` manifest containing ordered IDs and payload SHA-256 values, one or more one-based harness-issued producer-send attempts per ID, acknowledged partition/offset/outcome, terminal disposition, exact beginning/end bounds, normalized reconciliation-consumer configuration, and observed IDs. Do not invent Kafka-client-internal transport retries; the current runner records its one send invocation as attempt `1`. |
| SV-110e | Generated records were acknowledged and closed bounds exist, but reconciliation reports an acknowledged-missing ID or does not reach every bound. | Return the stable input-manifest failure with an immutable `partial` manifest containing every available acknowledgement, disposition, bound, configuration, and observed ID. A failure before closed bounds/configuration exist carries no fabricated partial manifest. |
| SV-110f | Reconciliation-consumer or Admin close fails after a reconciliation snapshot or manifest has already been built. | Return infrastructure-inconclusive while retaining the observed manifest and its truthful `complete` or `partial` status. Preserve any substantive input-manifest failure as primary and attach the close failure as supplemental evidence; do not regress to “bounds were never evidenced.” |
| SV-110g | Opening the Kafka client boundary, topic creation, production, offset capture, reconciliation, and close each consume part of input preparation's internal `2m` budget. | Start one monotonic deadline immediately before open and pass only its remaining duration to every operation through final completion. Do not give any stage a fresh two-minute timeout. |
| SV-110h | A generated-record send or acknowledgement blocks until the shared input-preparation deadline expires. | Stop preparation with structured `inconclusive` `infrastructure.kafka-input-setup-failed`; do not start Flink, a process fence, or terminal validation. Retain only factual input evidence already available, if any. |
| SV-110i | Input bounds/reconciliation have already yielded a `partial` or `complete` manifest, but consumer/Admin close or final completion exhausts the same deadline. | Return structured `inconclusive` `infrastructure.kafka-input-setup-failed` and preserve that manifest unchanged. This setup failure is not a post-fence `verification.*` failure and cannot be treated as a terminal-validator result. |
| SV-111 | Manual-start job with preload input. | Verify the runner prepares input but does not submit the job before the explicit `start` step. |
| SV-112 | Input source declares `connect_via_proxy` for a generated producer. | Route the harness input producer through the named proxy. |
| SV-113 | Bounded preload input and all phases complete normally. | Wait for every submitted job to reach `FINISHED`, establish the irreversible physical process fence, then start terminal-validator clocks. No Kafka terminal validator runs while Flink can still write or commit. |
| SV-113a | The process fence begins after natural terminalization. | Start one fixed internal `2m` monotonic deadline; SIGKILL every running TaskManager before every running JobManager, attempt all known components under that shared non-resetting budget, confirm none is running, and retain logical name/role/runtime ID/outcome entries plus the fence-completion timestamp. Disable all later Flink process creation or restart for the attempt. |
| SV-114 | A submitted bounded job has not reached `FINISHED` when the resolved `completion_timeout` expires. | Invoke the same physical process fence and fail the experiment with `verification.flink.job-completion-timeout`. Snapshot checks may run for diagnostics only and cannot satisfy an expected data-integrity oracle. |
| SV-114a | Querying or waiting for terminal job state fails for a non-timeout reason. | Attempt the safety fence and fail with `verification.flink.job-terminalization-failed`; no expected oracle can match it. |
| SV-114b | Job completion times out or terminal-state querying fails, but the forced process fence succeeds. | Retain the complete process-fence component/runtime/outcome/timestamp evidence alongside `verification.flink.job-completion-timeout` or `verification.flink.job-terminalization-failed`; do not discard it because the terminal write fence as a whole failed. |
| SV-115 | Job is terminal but one Flink process cannot be confirmed stopped/fenced. | Do not start terminal validation; fail with `verification.flink.process-fence-failed`. The process-fence reason takes precedence when terminalization and fencing both fail. |
| SV-115a | One process kill or liveness confirmation consumes the process fence's internal `2m` deadline. | Do not reset the budget for another component. Attempt the remaining known components where possible, fail with `verification.flink.process-fence-failed`, and never reopen process creation for the attempt. |
| SV-115b | The Flink job-control boundary returns null or a state other than `FINISHED` from bounded completion/drain instead of throwing a typed failure. | Treat it as `verification.flink.job-terminalization-failed`, force the irreversible process fence exactly once, retain successful process-fence evidence, and skip every terminal validator. Do not let an unchecked evidence-constructor failure discard the fence proof. |
| SV-116 | `completion_timeout` and built-in/custom terminal-validator `timeout` are omitted in raw YAML. | Materialize `2m` for each before execution and record every effective deadline. |
| SV-117 | A topic-reading terminal validator starts after the fence while Kafka is unreachable. | Start its absolute clock before metadata/topic/partition discovery, retry discovery within that same deadline, then fail the experiment with `verification.kafka.unreachable-after-timeout` if still unreachable. Do not return `inconclusive` or match an expected oracle. |
| SV-117a | Kafka discovery succeeds for a topic-reading terminal validator. | Use `read_uncommitted` isolation to capture one immutable high-watermark boundary per partition as the first range-reading snapshot operation immediately after discovery. Then capture beginning offsets and use a separate `read_committed` consumer to traverse only that fixed raw range. Discovery, raw-boundary/beginning capture, reading, decoding, and comparison all share the original non-resetting deadline. |
| SV-117b | Flink has returned `FINISHED` and is physically fenced, but Kafka has acknowledged `EndTxn` while its commit marker has not yet advanced the `read_committed` last stable offset. | The fixed `read_uncommitted` boundary already includes the transaction's flushed records. Keep polling with the `read_committed` consumer until it reaches that raw boundary, then include committed records in the oracle; if the transaction never resolves before the shared deadline, fail with `verification.kafka.incomplete-after-timeout`. Never capture the last stable offset as the supposedly final boundary or allow an exact visible prefix to pass. |
| SV-118 | Kafka is reachable and raw high-watermark offsets are captured, but the `read_committed` validator cannot traverse every partition to its captured bound before the same deadline. | Fail with `verification.kafka.incomplete-after-timeout`; retain any captured beginning/raw-end offsets, observed count, deterministic bounded record coordinates/values, and an incomplete-snapshot marker. Omit authoritative distinct/malformed/unexpected/duplicate/missing totals because the range is incomplete. Partial evidence cannot match an expected data-integrity failure. |
| SV-118a | The terminal snapshot topic is missing, its beginning offset is non-zero because output was truncated, or another non-timeout Kafka snapshot operation fails. | Fail closed with `verification.kafka.topic-not-found`, `verification.kafka.snapshot-truncated`, or `verification.kafka.snapshot-unavailable`; none can satisfy an expected oracle. |
| SV-118b | The fixed Kafka range was read, but ID decoding, evidence construction, or comparison exhausts the original validator deadline. | Fail with `verification.kafka.incomplete-after-timeout`, retain bounded offsets/observed-count/record-coordinate evidence with an incomplete marker, omit provisional defect totals, and do not grant a fresh comparison deadline. |
| SV-118c | The first narrow in-memory validator encounters an output record after already observing 1,000,000 in-range records. | Stop materializing output, retain fixed offsets and bounded partial record-coordinate evidence, and fail with `verification.kafka.output-limit-exceeded`. The broad schema remains unchanged; streaming or spill-backed evidence is required to lift the cap. |
| SV-119 | Kafka is temporarily unavailable, recovers before the absolute deadline, and complete evidence is obtained. | Continue to the ordinary validator comparison without resetting the clock; return its definitive `pass` or `validator.*` failure. |
| SV-119a | `kafka.no-hanging-transactions` has `stabilization_timeout` greater than its resolved absolute `timeout`. | Reject before provisioning; stabilization must fit within the one non-resetting validator deadline. |
| SV-119b | A reachable, fully observed transaction remains ongoing beyond `stabilization_timeout` while the validator is still within its absolute deadline. | Return definitive `fail` with `validator.kafka.transaction.ongoing-after-timeout`, distinct from Kafka verification unavailability. |
| SV-119c | A complete `kafka.id-set` snapshot contains multiple defect classes. | Record evidence for all classes and choose the stable primary reason in order: `malformed-ids`, `unexpected-ids`, `duplicate-ids`, `missing-ids`; consumer encounter order cannot change the reason. |
| SV-119d | Expected result names `verification.kafka.unreachable-after-timeout`, `verification.kafka.incomplete-after-timeout`, `verification.kafka.output-limit-exceeded`, or a Flink terminalization reason as its oracle reason. | Reject the expectation before provisioning; `verification.*` is outside the expected data-integrity registry. |
| SV-119e | Flink is fenced, the raw Kafka boundary is captured, and a delayed commit marker makes a previously hidden transaction visible. | Because all committable records were flushed below the fixed raw boundary before `EndTxn`, require `read_committed` traversal to reach that boundary and validate the newly visible records before returning. Treat any actual post-boundary Flink write as a fence breach and fail; never let an earlier validator pass stand. |
| SV-119f | The initial narrow runner receives zero or multiple terminal validators, a first validator other than `kafka.id-set`, or an `id-set` expected source other than `input-manifest`. | Reject before artifact preparation/Docker with `runner.validation.count-unsupported`, `runner.validation.type-unsupported`, or `runner.validation.expected-unsupported`, respectively; do not narrow the broad v1 validator schema. |
| SV-119g | A complete `kafka.id-set` snapshot contains a Kafka tombstone with a null value. | Preserve its partition/offset coordinates, count it as an observed record, and return definitive `validator.kafka.id-set.malformed-ids`; do not convert it to `verification.kafka.snapshot-unavailable`. |

## 7. Phase and fault semantics

| ID | Case | Expected result |
| --- | --- | --- |
| SV-120 | Step references missing job alias. | Reject. |
| SV-121 | `kind: selector` target in v1. | Reject unsupported capability. |
| SV-122 | `kill` target cannot be resolved statically. | Reject before execution. |
| SV-123 | Held network fault omits `duration`. | Reject structurally. |
| SV-124 | Held network fault omits `heal`. | Reject structurally. |
| SV-125 | Network fault references undeclared proxy. | Reject. |
| SV-126 | Network fault proxy points to missing cluster. | Reject. |
| SV-127 | Job affected by network fault omits matching `connect_via_proxy`. | Reject. |
| SV-128 | Network fault uses unsupported Kafka API alias. | Reject. |
| SV-129 | Fault never reaches `trigger-confirmed`. | Attempt inconclusive. |
| SV-130 | Fault heal fails. | Attempt inconclusive. |
| SV-131 | Network fault names a proxy but no source, sink, or input-source endpoint routes through that proxy. | Reject. |
| SV-132 | `match.api: produce` with `match.topic` equal to a proxied sink topic. | Match the sink endpoint. |
| SV-133 | `match.api: fetch` with `match.topic` equal to a proxied source topic. | Match the source endpoint. |
| SV-134 | `match.api: produce` with `match.topic` equal to a proxied input-source topic. | Match the harness input producer. |
| SV-135 | Transaction API fault names a `transactional_id_prefix` not used by any proxied exactly-once sink. | Reject no matching endpoint. |
| SV-136 | Topic-less `metadata` fault has at least one endpoint routed through the proxy on the target cluster. | Accept as matching that routed cluster traffic. |
| SV-137 | Plain `wait: { duration: 5s }` appears outside a race-window trigger. | Execute bounded wait and record it as timing-only. |
| SV-138 | `restore.from` names `{ type: savepoint, name: before-upgrade }` produced by a prior savepoint step. | Restore from that named state artifact. |
| SV-139 | `restore.from` names an unknown checkpoint/savepoint artifact. | Reject before execution. |
| SV-139a | Network target declares `topic`; topic belongs under `match`. | Reject structurally. |
| SV-139b | Network target declares `transactional_id_prefix`; prefix belongs under `match`. | Reject structurally. |
| SV-139c | Network target declares `client`. | Reject unsupported v1 selector. |
| SV-139d | `error-response` selects an API/error pair that cannot be represented safely. | Reject before provisioning. |
| SV-139e | Two phases use the same name. | Reject ambiguous phase identity. |
| SV-139f | A top-level or loop-nested step references an undeclared job alias. | Reject at the exact nested `job` path. |
| SV-139g | `kill` or `stop` uses `target.kind: selector`. | Reject unsupported runtime-target capability without derivative selector errors. |
| SV-139h | A named process target has a role/name mismatch, non-canonical ordinal, or ordinal above the resolved component count. | Reject unresolved static target. |
| SV-139i | `broker-N` resolves in more than one declared Kafka cluster. | Reject ambiguous static broker target. |
| SV-139j | `restart: { component: kafka }` appears with two Kafka clusters. | Reject ambiguous Kafka restart. |
| SV-139k | `await: taskmanager-count` exceeds the resolved TaskManager count. | Reject unreachable condition. |
| SV-139l | Kafka transaction await names no matching exactly-once sink prefix, or record-threshold names a missing cluster/topic. | Reject the unresolved await dependency. |
| SV-139m | Named restore refers to a prior artifact with the same job, type, and name. | Accept static state lineage. |
| SV-139n | Named restore refers only to a later producer. | Reject non-prior state artifact. |
| SV-139o | Named restore has an unknown name, wrong type, or producer from another job. | Reject with the specific state-lineage mismatch. |
| SV-139p | Two producers declare the same `(job, type, as)` identity. | Reject duplicate state-artifact identity. |
| SV-139q | A checkpoint/savepoint with `as` is inside a loop whose resolved `times` is greater than one. | Reject iteration-ambiguous state-artifact identity. |
| SV-139r | Two proxies resolve to the same `listen` address, including DNS-equivalent host spelling such as case or a trailing root dot. | Reject duplicate proxy listener. |
| SV-139s | Explicit proxy bootstrap address equals that proxy's own `listen` under DNS-identity comparison. | Reject immediate proxy route cycle. |
| SV-139t | Network target broker is malformed or above the target cluster's broker count. | Reject unresolved network broker. |
| SV-139u | Non-transaction Kafka API declares `transactional_id_prefix`. | Reject incompatible selector. |
| SV-139v | `init-producer-id`, `add-offsets-to-txn`, or `end-txn` declares `match.topic`. | Reject topic selector unsupported by that request profile. |
| SV-139w | `add-partitions-to-txn` topic matches the routed exactly-once sink topic, or `txn-offset-commit` topic matches its paired source topic on the target cluster. | Accept the exact request-field match. |
| SV-139x | An API/error pair appears in SPEC-004 K5.4a. | Accept static error-response compatibility. |
| SV-139y | A logical matching endpoint is direct or routed through another proxy. | Reject that the selected fault proxy is bypassed. |
| SV-139z | The named proxy has routed cluster traffic but none for the selected API/topic/prefix. | Reject no matching endpoint. |
| SV-139aa | Resolved `taskmanagers: 2` and a step declares `restart: { component: taskmanager, image: flink:2.2.1 }`. | Reject resolved semantic preflight before provisioning: the shape identifies no logical TaskManager slot, and v1 neither chooses one nor upgrades all of them. |
| SV-139ab | Resolved `taskmanagers: 1` and a TaskManager restart declares a new image. | Accept; update that sole logical TaskManager's desired exact target, select its precomputed target-specific lock/binding, and inject the same byte-only classpath manifest as every other Flink container. |
| SV-139ac | A component-specific image restart makes desired JobManager and TaskManager target references differ, followed by `restart: { component: flink }` without `image`. | Reject resolved semantic preflight before provisioning; do not infer the setup image, most recent image, or either component's image. |
| SV-139ad | All desired Flink component targets have one exact reference and a full Flink restart omits `image`. | Accept and inherit that uniform exact reference for every component. The step creates no new target lock or binding. |
| SV-139ae | Desired Flink component target references differ and a full Flink restart supplies explicit `image: X`. | Accept; set every logical JobManager and TaskManager desired target to exact reference `X`, restoring uniform desired state and selecting the precomputed binding for `X`. |
| SV-139af | A JobManager or sole-TaskManager restart omits `image` after that component previously changed image. | Retain the affected logical component's own desired exact reference; do not reset it to `setup.flink.image` or another component's target. |
| SV-139ag | An explicit JobManager or sole-TaskManager retarget stops the old process, commits desired target `X`, and then replacement construction, connector verification, or process start fails. | Keep `X` as the component's desired target and retain the prior target only as last-successful provisioning evidence. Record the failed attempt against `X`; do not automatically roll back or relabel the old target as desired. |
| SV-139ah | After an explicit retarget to `X` fails with the old process stopped, a later `start` or component restart without `image` addresses that logical component. | Retry desired target `X` with its lock/binding and byte manifest. Update last-successful provisioning evidence to `X` only if the retry succeeds; another failure still leaves `X` desired. |
| SV-139ai | One named TaskManager kill or restart uses several Docker operations (start/kill/inspect/remove/confirmation), and earlier calls consume most of its internal `2m` action budget. | Share one monotonic deadline for that action only; every later Docker call receives its remaining time and no call restarts the budget. A subsequent TaskManager action receives a new independent two-minute deadline. |
| SV-139aj | A named TaskManager kill or restart exhausts its action deadline. | Report `inconclusive` with `taskmanager.kill.timeout` or `taskmanager.restart.timeout` respectively, stop phase execution, and skip terminal process fencing and Kafka validation. Do not set the irreversible terminal-fence latch; cleanup remains separately bounded. |
| SV-139ak | A TaskManager restart times out, then its Docker start/inspect call completes late while bounded attempt cleanup is running or has begun. | Keep the already-decided `inconclusive` restart-timeout result and never reinterpret the late completion as a successful phase. Cleanup may remove resources under its separate deadline, but neither it nor the late restart may set the terminal-fence latch or permit terminal validation. |

## 8. Repetition, health, and verdicts

| ID | Case | Expected result |
| --- | --- | --- |
| SV-140 | Plain scenario `runs: 3`, all three clean attempts match. | Scenario pass. |
| SV-141 | Plain scenario first clean attempt mismatches. | Scenario fail immediately. |
| SV-142 | Dirty attempt then clean pass with retry budget available. | Dirty attempt discarded; scenario may pass. |
| SV-143 | Dirty-health retry budget exhausted. | Scenario inconclusive. |
| SV-144 | Experiment initial baseline mismatches. | Scenario inconclusive; candidate not evaluated. |
| SV-145 | Experiment candidate mismatch and confirmatory baseline matches. | Scenario fail. |
| SV-146 | Experiment candidate mismatch and confirmatory baseline mismatches. | Scenario inconclusive. |
| SV-147 | A custom or other non-Kafka terminal validator is inconclusive under clean health. | Scenario inconclusive; no automatic retry. |
| SV-147a | Built-in Kafka terminal verification reaches its absolute deadline without complete evidence, and health evidence also marks Kafka dirty. | Scenario/experiment fail with the stable `verification.kafka.*` reason. The verification rule takes precedence over dirty-health inconclusive and its own retry loop is not rerun through `health_retry_limit`. |
| SV-147b | Initial or confirmatory baseline encounters a `verification.*` failure. | Fail the whole experiment and do not interpret candidate comparison or an expected-failure oracle as successful. Preserve which side failed verification. |
| SV-147c | The first narrow runner receives a scenario whose resolved `health_retry_limit` is non-zero, including the broad v1 default of `1`. | Reject before artifact preparation/Docker with `runner.invocation.health-retries-unsupported`; do not claim retry support until fresh-attempt health classification and cleanup exist. The broad schema/default remain unchanged. |
| SV-148 | Expected failure passes. | Candidate fail with `expectation-not-reproduced`. |
| SV-149 | Expected failure fails via different reason. | Candidate fail with `unexpected-failure-mode`. |

## 9. Report requirements

| ID | Case | Expected result |
| --- | --- | --- |
| SV-160 | Scenario uses defaults for Kafka mode, JobManagers, TaskManagers, health retry, completion timeout, validator timeout, state backend, state TTL, watermarks, checkpoint storage, restart strategy, and job start. | Report records every declared omission and materialized value: `kraft`, `1`, `1`, `1`, `2m`, `2m`, `hashmap`, TTL disabled, no watermarks, JobManager storage, `flink-default`, and `auto`; restart evidence also includes the Flink line and runtime-effective settings. |
| SV-160a | A bounded terminal attempt uses workload protocol v1. | The complete R8.2 report records the JAR manifest declaration/checksum, effective standard/reserved configuration (redacting no values because the protocol accepts no secrets), canonical stopping-offset map, input cutoff, completion deadline, observed job terminal states, per-component process-fence evidence/timestamp, validator deadlines, Kafka snapshot bounds, completeness, and bounded partial coordinates when applicable. |
| SV-160b | A job selects filesystem checkpoint storage. | Report the generated attempt/job namespace and effective `file:/flink/checkpoints/attempt-<ordinal>-<nonce>/<job-alias>` URI; never report an author-selected arbitrary URI because none is accepted. |
| SV-160c | The implemented narrow `run` command completes an attempt. | Emit a stable JSON result/evidence summary with scenario/attempt/checkpoint identity, status/reason/message, summarized input/phases, write/process-fence completion, terminal-validation counts/completeness, Flink provisioning count, and diagnostics. Do not label this summary the complete R8.2 replay report or imply that roadmap health/OCI/full-provenance fields were collected. |
| SV-161 | Suite overrides runs. | Report records declared and effective runs. |
| SV-162 | Scenario uses local JAR or file-input path. | Report records declared reference, canonical selected source path, prepared execution path, and SHA-256. |
| SV-163 | Scenario uses image tag. | Report records tag and resolved digest when resolution succeeds. |
| SV-164 | Expected-result case selected. | Report records selected case or default. |
| SV-165 | Network fault executes. | Report records full proxy/fault evidence. |
| SV-166 | Scenario uses a subject connector. | For every effective side and distinct target Flink reference, report the full lock: auto/explicit mode, primary identity/reference, ordered runtime entries with origin-root kind/index, source/staged paths, Maven paths, SHA-256 and classpath indexes, consulted POM identities/source paths/SHA-256, mediation decisions, exact target reference, runtime-resolved digest when deployed, and stable `closure_sha256`. |
| SV-166a | Several jobs/connectors form a side-specific connector classpath. | Report the byte-only manifest's exact canonical bytes, format, fixed container path, `classpath_manifest_sha256`, and ordered index/filename/SHA-256 entries; retain every coalesced-entry contributor and verified host staging path as non-canonical provenance. Also report each target-specific binding's exact target reference, sorted alias/`closure_sha256` pairs, referenced `classpath_manifest_sha256`, canonical bytes, and `target_binding_sha256`, plus any pre-provisioning mismatch diagnostic. |
| SV-166b | A Flink upgrade changes the target image while connector bytes remain identical. | Report distinct target-specific `closure_sha256` and `target_binding_sha256` values but one identical `classpath_manifest_sha256` and canonical byte manifest. Associate each deployment/restart event with its logical component, exact declared reference, independently resolved OCI digest, selected binding/locks, and observed classpath-manifest hash. |
| SV-166c | A connector classpath is installed into initial, replacement, or upgraded Flink containers. | For each container, report successful pre-start verification of `/opt/flink/lib/.flink-stability-connector-classpath-v1.json`, its `classpath_manifest_sha256`, and every listed JAR filename/SHA-256. Do not claim that the target-specific binding itself was copied into the container. |
| SV-166d | An explicit component retarget to `X` fails after the old target `A` was stopped, then a later retry succeeds or fails again. | Report every attempt with desired/attempted target and outcome. Keep last-successful provisioning evidence at `A` across failures and move it to `X` only after successful provisioning; never report an automatic rollback that did not occur. |

## 10. Docker-free validation command

| ID | Case | Expected result |
| --- | --- | --- |
| SV-170 | `validate` selects one existing scenario or suite by `meta.name`. | Load the complete catalog, resolve/preflight the selected target, prepare every artifact without constructing/provisioning Docker, close prepared copies, and exit 0. |
| SV-171 | Structural, catalog, parameter, expectation, semantic, artifact, or suite-entry validation fails. | Print every stable source/scope/code/pointer diagnostic to stderr, return no partial plan, and exit 1. |
| SV-172 | CLI syntax is invalid, both/neither target selectors are supplied, a parameter assignment is malformed/duplicated, or the selected name is absent. | Print a usage-oriented error (including sorted available names for an absent target) and exit 2. |
| SV-173 | `-p NAME=VALUE` uses `true`/`false`, an integer, unquoted text, or a quoted JSON string. | Preserve boolean/integer/string type; a quoted numeric-looking value remains a string and trailing tokens reject. |
| SV-174 | `--offline` is used with an ordinary Maven-Central artifact already present in the local Maven cache. | Reuse the conventional `central` repository identity without network access; distinguish a healthy cache miss from an unusable local repository. |

## 11. First bounded `run` command

| ID | Case | Expected result |
| --- | --- | --- |
| SV-175 | `run` selects one compiler-supported bounded plain scenario. | Resolve and capability-check before artifacts, prepare and bind immutable artifacts before Docker, allocate one collision-safe attempt namespace, execute exactly once, close attempt and artifact owners, then emit the structured result/evidence summary. A pass exits 0. |
| SV-176 | The executed `run` attempt returns `fail` or `inconclusive`. | Preserve its stable reason and available partial/fence evidence in the execution result; always emit input, phase, write-fence, process-fence, and terminal stage objects, using `not-started`/`not-run`, `completed: false`, and terminal `snapshotComplete: false` when a stage never ran. Emit defect totals only for a complete terminal snapshot and exit non-zero, unless a failing attempt matches its pinned expected failure (SV-183). Never turn a `verification.*` failure into `inconclusive` or an expected-oracle match, including when prepared-owner cleanup later fails. |
| SV-177 | `run` preparation fails structural, semantic, capability, expectation, or artifact validation. | Emit source-aware diagnostics and exit non-zero before attempt-context allocation or Docker. Unknown scenario or unsupported CLI syntax uses the usage path; no partial attempt result is invented. |
| SV-178 | `run` is given `--suite`. | Reject `--suite` as a usage error because the first execution vertical accepts exactly one scenario; do not route through an obsolete command, loader, or schema adapter. |
| SV-179 | An otherwise supported first-runner scenario declares fewer or more than exactly the distinct input and sink Kafka topics. | Reject before artifact preparation/Docker with `runner.kafka.topic-count-unsupported`; do not ignore an extra declared topic or narrow the broad v1 topology schema. |
| SV-180 | An otherwise supported first-runner scenario declares zero or multiple subject connectors. | Reject before artifact preparation/Docker with `runner.connector.count-unsupported`; the one executable workload must use exactly one verified connector closure. |
| SV-181 | The first-runner workload job resolves `parallelism` to a value other than `1`. | Reject before artifact preparation/Docker with `runner.workload.parallelism-unsupported`; broader v1 parallelism remains valid for a later execution capability. |
| SV-182 | `setup.flink.config` is non-empty for the first runner. | Reject before artifact preparation/Docker with `runner.flink.config-unsupported`; do not pass through configuration keys the typed plan has not registered. |
| SV-183 | The selected plain expected outcome is `fail`. | Accept when it names the `kafka.id-set` oracle: the verdict is `pass` only when the attempt fails with the pinned reason and complete, confirmed experiment evidence, `expectation.mismatch` when it passes or fails with another reason, and never a match for an `inconclusive` or `verification.*` attempt (SPEC-001 R8.7a). Reject an expected failure of any other oracle before artifact preparation/Docker with `runner.expectation.outcome-unsupported`. |
| SV-183a | A first-runner job sink declares `delivery_guarantee: NONE`. | Reject before artifact preparation/Docker with `runner.workload.delivery-guarantee-unsupported`. `EXACTLY_ONCE` and `AT_LEAST_ONCE` sinks are executable; an `AT_LEAST_ONCE` sink carries no transaction settings and has no sink transactions to list. |
| SV-183b | A first-runner exactly-once sink declares `transaction_timeout` above the broker's fixed `2h` maximum. | Reject before artifact preparation/Docker with `runner.workload.transaction-timeout-unsupported`. A shorter timeout, even one below the checkpoint interval, is valid and reaches the workload as `sink.transaction-timeout-ms`. |
| SV-183c | The subject connector's primary artifact lacks a protocol-v1 entry class, another connector-bundle entry (such as a runtime dependency holding the released connector) contains one, or the workload JAR contains one. | Reject at plan binding, before Docker, with `runner.subject.entry-class-missing` or `runner.subject.entry-class-conflict` naming the artifact and the offending bundle entry or its declaration path. A primary that alone supplies both entry classes binds. |
| SV-183d | After the process fence, a Flink class-load log shows an entry class loaded from anywhere but the subject primary's `lib` path, no TaskManager log shows the entry classes, or the logs cannot be read. | A passing oracle becomes `inconclusive` with `subject.connector.origin-mismatch` or `subject.connector.origin-unconfirmed`; a failing oracle keeps `fail`. A PASS result cannot be constructed without confirmed origins. |
| SV-183e | A pinned expected failure occurs with an ineffective TaskManager kill, failed phases, missing write/process-fence evidence, or an absent, incomplete, or differently failing terminal oracle. | Retain the attempt's data failure; the scenario verdict is inconclusive and the expectation does not match. Every kill must have confirmed effect, and the complete failing oracle must agree with the attempt's pinned reason (SPEC-001 R8.7a). |
| SV-183f | A multi-release subject primary, dependency, or workload contains versioned KafkaSource/KafkaSink entries. | Reject before Docker with `runner.subject.entry-class-versioned-unsupported`; a dependency with unrelated versioned classes remains valid. |
| SV-183h | An unrelated failure/restore occurs during pre-kill observation, the post-exit clock sample is missing/reversed, or the selected restore precedes the matching failure. | The kill effect is unconfirmed. Recovery events must follow the post-exit clock sample, exclude already-observed failures, and preserve failure-before-restore order. |
| SV-184 | Attempt resource cleanup blocks beyond the first runner's fixed internal `2m` cleanup deadline, its worker does not start before that deadline, the waiting thread is interrupted, or its interrupt flag is already set when cleanup begins. | Return a structured result without retrying cleanup concurrently. Add `infrastructure.attempt-cleanup-failed`; convert a prior pass to inconclusive but preserve an existing failure and its verification/process-fence evidence. Preserve the caller's interrupt flag. If an accepted task misses the start deadline, leave it eligible to start exactly once rather than cancelling it before invocation. Detached cleanup must not own or dereference prepared artifacts. |
| SV-185 | Prepared-artifact-owner close fails after `run` has produced an attempt result. | Add `infrastructure.prepared-artifact-cleanup-failed` and still emit the structured attempt evidence. Convert a prior pass to inconclusive; preserve an existing fail or inconclusive primary reason, especially `verification.*`, and retain process-fence and terminal evidence. Use the stderr-only cleanup path only when no attempt result exists. |
