# Testing custom runtime subjects

A copied catalog can select a privately built Flink distribution, Kafka broker,
connector and protocol-v1 workload while retaining the canonical scenario's
faults, timing, input and expected result. Explicit pins identify the selected
bytes; they do not certify a vendor distribution or infer compatibility from an
image name. Keep source revisions, build commands, artifact hashes, resulting
catalogs, JSON results and raw logs together in a new evidence directory.

The canonical `scenarios/` files and the pool-reuse calibration contract remain
unchanged. A successful custom run covers that build combination and that
attempt. In particular, selecting `connector-default` explicitly delegates naming
to the subject: it does not prove that the subject uses POOLING or satisfies the
pool-reuse calibration's separate transaction-reuse checks.

## Fields, rejection rules and evidence

| Input | Meaning and rejection rules | Retained evidence |
| --- | --- | --- |
| `setup.flink.line: "major.minor"` | Declares the runtime line for an opaque tag. Requires `image_id` and `runtime_jar`. A parseable tag naming another line rejects with `capability.flink-line.tag-mismatch`; missing pins reject with `capability.flink-line.pin-required`. Without this field, the existing 2.2 and pinned experimental 2.4 policy applies. | `evidence.flinkRuntime.declaredLine` and `tag`; observed `imageId` / `runtimeJarSha256` only after unanimous provisioning receipts, plus per-process verification and runtime class origins. Declared pins remain under `expectedImageId` and `runtimeJar.expected`. |
| `setup.flink.image_id` | Full local Docker `sha256:` image ID. Every created Flink container, including replacements, must match before process start. | Declared ID and every observed container image ID. |
| `setup.flink.runtime_jar: {container_path: /opt/flink/lib/flink-dist-<version>.jar, sha256: ...}` | Pins the distribution JAR inside every Flink container. Both `flink-dist-<version>.jar` and `flink-dist_<scala>-<version>.jar` are accepted directly under `/opt/flink/lib`; each label starts with a letter or digit and uses only letters, digits, dots, underscores, pluses and hyphens. The SHA-256 remains mandatory. JAR bytes and runtime class origins must agree. Missing or mismatched identity prevents PASS. | Per-incarnation JAR hashes and JobManager `ResourceManager` / TaskManager `TaskExecutor` origins. |
| `subject.connectors.kafka.artifact: image:/opt/flink/lib/subject.jar` with `sha256` | Selects an existing image JAR; installs nothing. The path must be a normalized absolute JAR path and the pin must contain 64 lowercase hex characters. Dependencies must be absent or empty. Connector classes in another `/opt/flink/lib` JAR, staged dependency or workload are rejected, as are observed concrete connector classes loaded from elsewhere. | `evidence.connectorPrimaries` with `origin: image`, declared and observed hashes; per-container verification and class origins from every TaskManager incarnation. |
| `setup.kafka.clusters.main.image_id` | Pins the broker image and permits a custom image reference. Without the pin the Apache Kafka 4.0 tag policy remains. A mismatched created-container image stops startup. | `evidence.kafka` retains the requested and observed image identities. |
| `setup.kafka.clusters.main.launch: {type: generic-kraft, layout: apache}` | Explicit one- or three-node KRaft launcher described below. Generic layout defaults to `apache`; `confluent-platform` selects Confluent tool paths. Unknown layouts reject with `runner.kafka.layout-unsupported`. Layout is not accepted with `apache-kafka` (`runner.kafka.layout-not-applicable`), which preserves the Testcontainers Apache launcher. | Resolved plan and `evidence.kafka` retain the layout, format/start command, readiness command, environment, properties/log directory and actual container identities. |
| `setup.kafka.clusters.main.broker_config` | Literal broker property overrides. Harness-owned listeners, node/broker/cluster IDs, process roles, log directories, controller quorum and automatic topic creation cannot be overridden (`runner.kafka.config-reserved-key`). Invalid property names or multiline values reject. | Resolved overrides and actual broker launch configuration under `evidence.kafka`. |
| `setup.kafka.clusters.main.transaction_version: broker-default` | Calls `describeFeatures`, records the observed finalized `transaction.version`, and never calls `updateFeatures`. Failed observation prevents PASS with inconclusive `kafka.feature.observation-unavailable`. Explicit `1`/`2` retain strict feature selection. | `evidence.kafkaTransactionVersion` reports `requested: broker-default`, observations, and `observed` or `unavailable` status with the error. |
| `setup.flink.config` | Supplies scalar values to every process's `FLINK_PROPERTIES` and the REST job submission's `flinkConfiguration`. Reserved keys reject with `runner.flink.config-reserved-key`; invalid values reject with `runner.flink.config-invalid`. | The resolved map, `evidence.flinkRuntime.config`, and each successfully started TaskManager incarnation's configuration. |
| `setup.flink.log_markers` | Named Java regular expressions with explicit `taskmanager` or `jobmanager` scope and optional `required: true`. A required marker must match every incarnation in its scope; absence prevents PASS with `subject.flink.log-marker-missing`. | `evidence.flinkRuntime.logMarkers` retains the first matching line or the reason no match was established, bound to each process incarnation. A data failure keeps its original status and reason. |
| `workload.jobs[].jar` | A workload built for the selected runtime and connector, retained inside the artifact root. It must remain thin and declare `Flink-Stability-Workload-Protocol: v1`. | `evidence.customRuntimeSubject.workload` retains `artifact` and `sha256`; preparation also binds the staged bytes. |
| `workload.jobs[].sink.transaction_id_naming_strategy: connector-default` | Valid only for `EXACTLY_ONCE`. The workload does not inspect or invoke the optional naming setter for this choice. `INCREMENTING` and `POOLING` use reflection and fail clearly if the connector cannot implement the request. | `evidence.customRuntimeSubject.transactionIdNamingStrategy` and `jobFlinkConfiguration` retain the choice and compiled typed/pass-through job settings. |

`evidence.customRuntimeSubject` is emitted only for custom runtime inputs; ordinary
canonical results keep their existing shape. Declared image/JAR pins do not count
as observations before containers have been verified. The job settings map precedes
attempt-specific protocol, endpoint and checkpoint-path materialization.
Explicit Flink lines and pinned Kafka images are labeled
`compatibilityBasis: author-assertion` in runtime evidence and in the custom
subject's `compatibilityAssertions` map in plans/results. A successful hash check
establishes identity, not compatibility with an unrun version.

Reserved Flink keys include `jobmanager.rpc.address`,
`taskmanager.numberOfTaskSlots`, `taskmanager.resource-id`, checkpoint storage and
HA settings, `env.java.opts` and `env.java.opts.*`, and the
`flink-stability.workload.*` namespace. Classloading controls (`classloader.*`,
`pipeline.jars`, `pipeline.classpaths`) are reserved so pass-through cannot add
another connector source or bypass class-origin checks. First-class workload settings also retain
ownership of `parallelism.default`, state backend, checkpoint interval and mode;
checkpoint/savepoint location aliases (including `execution.checkpointing.savepoint-dir`,
`state.backend.fs.checkpointdir`, and local-backup directories) and legacy
`recovery.mode` / `recovery.zookeeper.*` HA options are reserved too;
pass-through cannot silently replace those scenario inputs. Use typed catalog
fields for those settings. Ordinary keys such as
`execution.checkpointing.unaligned.enabled` and `pipeline.name` are passed through
as literal strings. An unknown Flink option can still be rejected or ignored by
the selected Flink runtime; the harness records what it supplied and does not
claim that every vendor option changed behavior.

Process-level confirmation proves delivery to the process; the submission
evidence separately records the REST job configuration. Neither proves that the
job graph or an operator consumed an option. Flink builds the job's
`ExecutionConfig` from selected configuration options during graph generation,
so an option delivered to the process can still be omitted at the job or operator
level. Use a component's own log marker to establish that it observed the value,
and interpret that marker according to what the component actually reports.

Nonempty Flink configuration uses an explicit launcher requiring `/bin/bash`,
`/opt/flink/bin/jobmanager.sh`, `/opt/flink/bin/taskmanager.sh` and standard YAML
at `/opt/flink/conf/config.yaml`. It merges the image configuration with harness
settings and the literal custom values before starting the Flink shell script.
The merge uses the YAML 1.2 CoreSchema used by Flink 2.2; nested image settings
retain their scalar types. Ambiguous duplicate keys and aliases reject.
The image entrypoint is bypassed: the official 2.2.0 entrypoint strips whitespace
and expands environment references in `FLINK_PROPERTIES`, which cannot safely
carry arbitrary literal strings through that parser. The environment still
records the declaration; it is not treated as proof of the effective values.
After readiness, the harness checks the configuration file and each requested
value in that incarnation's configuration-loading logs. Missing or altered
observations reject provisioning. An image with a different layout or logging
contract needs an explicit adapter. With no custom configuration, the existing
entrypoint remains in use.
Each component's `effectiveConfiguration` records the path, hash of the merged
file copied before startup (`sourceSha256`), post-start hash (`observedSha256`),
launcher and observed values. The container ID and `classLoadProcess` bind that
receipt to one incarnation. Missing receipts are
`subject.flink.config-unconfirmed`; mismatched bytes or values are
`subject.flink.config-mismatch`, and neither can produce PASS.

For component-level observations, declare markers on a copied catalog:

```yaml
setup:
  flink:
    log_markers:
      - name: taskmanager-started
        regex: 'Starting TaskManager with ResourceID:'
        scope: taskmanager
        required: true
      - name: resource-manager-started
        regex: 'Starting the resource manager\.'
        scope: jobmanager
        required: false
```

Markers use Java regular-expression syntax and search individual component log
lines after the process fence. Select a precise message from the component whose
behavior matters. A startup marker such as the example above proves only that
the startup message was observed, not that an arbitrary configuration option
took effect. Each initial and replacement process needs its own observation;
a replacement cannot satisfy its predecessor's required marker. Optional missing
markers remain evidence without changing the verdict. Required missing markers
make a passing result inconclusive and cannot turn an observed data failure into
a different outcome.

Maven subjects and local-JAR subjects retain their previous artifact handling.
A local primary needs explicit `runtime_dependencies`; the gate's `--runtime-dir`
selects those JARs. Maven SNAPSHOT catalog coordinates remain forbidden. For an
unregistered Flink line, use an image-supplied or local connector build, rather
than treating a version suffix as a compatibility declaration.

## Kafka launcher contracts

For `apache-kafka`, broker property names must round-trip through the Apache
image's environment encoding. Ambiguous names such as `custom..flag` reject
with `runner.kafka.config-invalid`. Wrapper/JVM controls such as `opts`,
`heap.opts`, `log4j.loggers` and `version` reject with
`runner.kafka.config-reserved-key`; they are not broker properties in that
launcher. Both launchers escape property values so leading whitespace and
backslashes remain literal. The generic launcher writes property names directly
and does not apply Apache's environment-name restrictions.

`generic-kraft` requires `/bin/sh` and an explicit tool layout. Omitting `layout`
selects `apache`; it never infers the layout from the image name.

| `launch.layout` | Format tool | Start tool | Readiness tool |
| --- | --- | --- | --- |
| `apache` (default) | `/opt/kafka/bin/kafka-storage.sh` | `/opt/kafka/bin/kafka-server-start.sh` | `/opt/kafka/bin/kafka-broker-api-versions.sh` |
| `confluent-platform` | `/usr/bin/kafka-storage` | `/usr/bin/kafka-server-start` | `/usr/bin/kafka-broker-api-versions` |

Both layouts override the image entrypoint and write an explicit properties file
at `/tmp/flink-stability-kafka.properties`, format KRaft storage and start the
broker. Properties arrive through
`FLINK_STABILITY_KAFKA_PROPERTIES`; `CLUSTER_ID` is also explicit. The resolved
plan contains the full command and properties, with host-port placeholders that
are replaced and retained in actual container evidence.

Each node has broker and controller roles, an internal listener on 19092, an
external mapped listener on 9092 and a controller listener on 9094. The harness
owns the voter set and node identities. Readiness executes the selected layout's
API-version tool against the internal listener. A vendor image with
another filesystem layout or incompatible tools must be adapted explicitly;
choosing an image ID does not make that layout compatible.

The declared physical log directory is `/tmp/kafka-logs`. Log/archive capture
must use the launcher's declared directory; an unsupported capture path is
reported as `kafka.log-capture.unsupported`, never interpreted as an empty healthy
capture. Retain capture errors and partial logs alongside the original verdict.

For `broker-default`, a single finalized feature entry is recorded with
`levelSource: finalized-feature`. Kafka represents level zero by omitting its
finalized entry. When the response supports zero and has a nonnegative metadata
epoch, the harness reports `observedLevel: 0`, `levelSource: implicit-zero`, and
leaves the raw finalized entry absent. Missing supporting metadata remains
`kafka.feature.observation-unavailable`; explicit level 1/2 selection is unchanged.

## Build the workload against the subject

First build or select the runtime image. The
[distribution image builder](FLINK-RUNTIME-TESTING.md#select-the-image) packages a
complete local `flink-dist` output with one connector and its explicit closure
on a public Java 17 or 21 Flink base:

```sh
tools/build_runtime_image.sh jobs/runtime/dist jobs/runtime/connector.jar \
  jobs/runtime/closure local/flink:2.2.0-java21-candidate \
  --java 21 --output jobs/runtime/image-build
```

Retain its Dockerfile, manifest and output. Use the returned image ID and the
observed in-image distribution/connector hashes as catalog pins. The script
rejects duplicate connector class sources in `/opt/flink/lib`, separate runtime JARs, incompatible
class-file versions, input links and an existing output tag. A private version
can explicitly choose a public `--base-image flink:<release>-java<N>` with the
same selected Java. The chosen base must supply a compatible official
entrypoint; building an image does not establish workload compatibility.
Omitting `--output` retains the build context and manifest under
`jobs/runtime-images/build-*`.

Use JDK 21 and an isolated Maven cache for the custom build. For artifacts already
installed in that cache, an offline build avoids contacting a repository:

```sh
mvn -B -o -pl flink-job-generator clean package \
  -Dmaven.repo.local="$PWD/jobs/custom-runtime/maven" \
  -Dflink.version=<exact-runtime-version> \
  -Dkafka.connector.version=<exact-connector-version> \
  -Dkafka.connector.groupId=<connector-group-id>
```

Use `clean package` whenever the Flink, connector or compiler-release inputs
change. The module records `target/flink.version` and its build settings, and
fails before compilation when an existing output belongs to another or
unrecorded configuration. Clean only this module when retaining an experiment:
the root project's `clean` also removes checkpoints and other run directories.

`-Dworkload.compiler.release=17` (or `21`) selects the workload's emitted
bytecode level; the default remains 11. For example, add
`-Dworkload.compiler.release=17` to the clean build above for a Java 17 workload.
The compiler JDK and the target release are different settings. A recent
compiler can read Java 17 dependencies while emitting Java 11 bytecode; a
"class file has wrong version 61.0, should be 55.0" diagnostic instead requires
checking which compiler/toolchain Maven actually launched. Raising the target
release does not upgrade an older compiler. Keep the harness build on JDK 21
and choose a workload release supported by the selected runtime image.

The group ID defaults to `org.apache.flink`. The older
`-Dflink.kafka.connector.version=...` remains a compatible default when
`kafka.connector.version` is not supplied. A locally installed development
version can be used for this compilation; select its retained JAR or image bytes
in the catalog rather than a Maven SNAPSHOT reference.

The module produces `flink-job-generator/target/flink-job-generator.jar`. Copy it
to a retained location under the artifact root before another build replaces it,
then pass that path with `--workload-jar`. Flink and connector dependencies remain
`provided`, so they are not shaded into the workload. The manifest still declares
`Flink-Stability-Workload-Protocol: v1`.

`AsyncSnapshotDelay` directly extends `StreamMap` and overrides its
`snapshotState(long, long, CheckpointOptions, CheckpointStreamFactory)` method. It
uses `OperatorSnapshotFutures.getOperatorStateRawFuture()` and
`setOperatorStateRawFuture(...)`. Compilation against the selected
`flink.version` checks those exact APIs; no alternate timer or synchronous sleep
is substituted when an API is absent. Its tests exercise cancellation and
preservation of the original snapshot result. Jobs with zero async delay keep
the ordinary map operator.

The maintained compilation baseline for this extension is Flink 2.2.0 and Kafka
connector 5.0.0-2.2. No broader version range is claimed by accepting an explicit
line. Other versions need their own successful build and retained live runs;
reflection removes one optional connector API dependency, not all Flink and
connector compatibility requirements.

## Generate both arms without editing canonical catalogs

All runtime options apply identically to both arms. Only the connector subject
and an explicit repeatable `--candidate-flink-config key=value` may differ. The
candidate overlay can replace a common configuration value or add a new one.
Omitting either naming or transaction-version option preserves the catalog's
original choice; selecting `connector-default` or `broker-default` is explicit.

For an image-supplied connector with a configuration flag as the dimension under
test, both arms can select the same pinned image subject:

```sh
python3 tools/pr_gate.py \
  --scenario pool-reuse-inflight-control-v1 \
  --scenario pool-reuse-inflight-kill-v1 \
  --flink-image local/flink:private \
  --flink-image-id sha256:<flink-image-id> \
  --flink-line <major.minor> \
  --runtime-jar /opt/flink/lib/flink-dist-private.jar=<runtime-jar-sha256> \
  --kafka-image local/kafka:private \
  --kafka-image-id sha256:<kafka-image-id> \
  --kafka-launch generic-kraft \
  --broker-config transaction.two.phase.commit.enable=true \
  --transaction-version broker-default \
  --flink-config execution.checkpointing.unaligned.enabled=false \
  --flink-config pipeline.name=custom-runtime-subject \
  --candidate-flink-config execution.checkpointing.unaligned.enabled=true \
  --workload-jar jobs/custom-runtime/workload.jar \
  --transaction-id-naming-strategy connector-default \
  --subject image:/opt/flink/lib/subject.jar=<connector-sha256> \
  --baseline-subject image:/opt/flink/lib/subject.jar=<connector-sha256> \
  --output jobs/custom-runtime/comparison --prepare-only
```

Replace placeholders with retained identities. The image's `/opt/flink/lib` inventory must use regular JAR files; linked or
special JAR entries reject because their contents cannot be established from the
library archive alone. The image must contain a single connector primary: placing two connector versions at separate image paths is
still a duplicate-class error. The command intentionally cannot vary images
between arms. For two local connector artifacts, use `--connector-jar` with
`--runtime-dir` and the paired `--baseline-connector-jar` /
`--baseline-runtime-dir`; both explicit dependency directories must contain the
same dependency bytes (including multiplicity). Custom runtime flags, image
subjects and candidate configuration require an explicit baseline and matching
source modes: image/image or local/local. Mixed source modes and different hashes
for the same image path are rejected before output. Runtime identity is shared
between arms, so the same path cannot supply different bytes in one pinned image.

The image connector inventory covers its declared primary and `/opt/flink/lib`,
plus staged dependencies and the workload. Class-load evidence checks all
observed artifact-defined classes in the current and legacy Kafka connector
namespaces, including implementation helpers, and requires both entry classes
from every TaskManager incarnation. This does not inventory dormant files
elsewhere in the image or attest generated/transformed bytecode. Runtime
provenance assumes a trusted image and immutable JARs after startup checks.

The legacy gate invocation without custom runtime options may still omit baseline
options to select the released Maven baseline. This includes the existing
`--flink-image docker.io/library/flink:2.2.0` spelling when used without new
runtime inputs. That auto-resolved closure is
verified during artifact preparation, but this legacy mode does not prove that
its dependencies equal the local candidate's explicit closure. Use the strict
custom-runtime mode for a comparison in which only the connector artifact and
explicit candidate configuration may vary.

`--prepare-only` creates both catalog directories and `manifest.json` without
Maven or Docker. `--dry-run` prints the plan without writing catalogs. Each
manifest records all runtime substitutions, workload hash, connector pins,
common and candidate configuration, and SHA-256 for every resulting scenario
and copied expected-result document. The canonical files are never rewritten.
Configuration values are evidence, so do not put passwords or other secrets in
these options.

To prepare a runtime comparison as independent arms, invoke the same command
once per image with `--single-arm --prepare-only`, a fresh `--output` directory,
and that image's runtime and connector pins. Omit `--baseline-*` and
`--candidate-flink-config`; use `--flink-config` for the one selected arm. Each
invocation writes `subject-catalog/` and a manifest with
`mode: single-arm-prepare`, the subject identity, every runtime substitution and
the resulting catalog hashes. This mode prepares evidence inputs only; it does
not execute or qualify a comparison. Keep both manifests and compare the
resolved plans before running the two catalog directories.

The gate also accepts repeatable `--log-marker 'name=Java regex'` and
`--required-log-marker 'name=Java regex'`. A bare
`--required-log-marker name` makes an already declared marker required. Markers
default to TaskManagers; `--log-marker-scope name=jobmanager` selects JobManagers.
In the two-arm mode the same marker declarations apply to both arms. Regex text
is preserved for Java preflight validation, including Java-specific syntax.

For a Confluent Platform-style tool layout, supply both `--kafka-launch generic-kraft`
and `--kafka-layout confluent-platform`, along with `--kafka-image` and its
`--kafka-image-id` pin. The layout is shared by both arms and recorded in the
manifest. `--runtime-jar /opt/flink/lib/flink-dist_2.12-<version>.jar=<sha256>`
selects a Scala-suffixed runtime filename with the same mandatory pin.

Validate the prepared copy before starting containers:

```sh
mvn -q -o exec:java -pl cli \
  -Dexec.args="validate --offline --show-plan --catalog-root jobs/custom-runtime/comparison/candidate-catalog --scenario pool-reuse-inflight-kill-v1 --artifact-root ."
```

The validation JSON includes the resolved execution plan, exposing each runtime
substitution before a run. To use the gate for live runs, choose a fresh output
directory and omit `--prepare-only`. It retains stdout JSON, stderr logs, broker
log captures, exit codes and a comparison summary for every arm and repetition.
Image subject provenance requires both matching runtime hash evidence and
confirmed class origins; the declared hash alone cannot satisfy the gate.

## Verification scope

Docker-free tests check custom-line pins and tag disagreement, connector source
identity, reserved configuration keys, broker launch plans, feature observation,
optional naming APIs and catalog-copy invariants. A validation fixture accepting
an opaque custom line proves planning and artifact checks only. It does not show
that that image exists, that it contains the declared JAR, or that it runs.

On 2026-10-04, the public-image smoke used ARM64 Flink 2.2.0 with its local
image ID and runtime-JAR pin, declared line `2.2`, Apache Kafka 4.0.0 through
`generic-kraft`, the released Kafka connector 5.0.0-2.2 and unchanged POOLING
naming. Both `pool-reuse-inflight-control-v1` and
`pool-reuse-inflight-kill-v1` passed with exactly 14,000 records and no missing,
duplicate, malformed or unexpected IDs. The kill targeted checkpoint 2 with
`restWindowConfirmed: true`; Flink restored checkpoint 1. Runtime image/JAR
identities and connector class origins were confirmed, and physical Kafka logs
were collected for both attempts.

Both runs supplied `jobmanager.memory.process.size=1600m`, a pipeline name with
spaces and a harmless custom value containing literal `$HOME`, spaces, quotes,
a colon and a hash. The JobManager and every initial/replacement TaskManager
confirmed those exact strings, with equal pre/post-start configuration hashes.
The runs also retained a supplied broker property
`transaction.two.phase.commit.enable=true`; recording that input alone does not
prove that a particular broker recognizes it or changes behavior.
A separate probe using the same generic launcher set `log.retention.ms=3600000`;
Kafka's `describeConfigs` reported that value with source `STATIC_BROKER_CONFIG`.
The observed container command and broker properties matched the launch receipt.

The explicit `broker-default` override observed finalized transaction level **2**
in both runs, despite the catalogs' `-v1` names; these were not forced level-1
runs. A separate live Kafka 4.0 probe formatted storage at transaction level zero
and confirmed the omitted finalized entry was interpreted as `implicit-zero`
without a feature update. That probe covers observation, not an end-to-end
pool-reuse run at level zero.

Also on 2026-10-04, `pool-reuse-inflight-control-v1` passed with the public ARM64
`confluentinc/cp-kafka:8.3.2` image, pinned by local image ID, through
`generic-kraft` with `layout: confluent-platform`. It used the same pinned public
Flink 2.2.0 runtime and released connector 5.0.0-2.2 with unchanged POOLING naming,
input, timing and expected result. The oracle observed exactly 14,000 records
with no missing, duplicate, malformed or unexpected IDs. `broker-default`
observed finalized transaction level **2** without requesting a feature update.
The result retained the verified image identity, selected layout, actual format/
start command and readiness command. Broker output and four physical segment
archives were retained; all four archives parsed and decoded successfully.
This attempt did not run the kill scenario against Confluent Kafka.

Two additional public ARM64 controls on 2026-10-04 passed using
`flink:2.2.0-java21` and that image's distribution repackaged by
`tools/build_runtime_image.sh`. Both used the pinned public
`confluentinc/cp-kafka:8.3.2` generic launcher and released connector 5.0.0-2.2;
the official image used a local connector with an explicit closure, while the
repackaged image supplied the connector from `/opt/flink/lib`. Each observed
exactly 14,000 IDs without defects, confirmed runtime and connector origins, and
decoded four retained Kafka archives. The image-supplied run attributed 61
connector hidden lambdas in its TaskManager using the JDK 21 name form.
Both required JobManager/TaskManager startup markers matched, and an optional
absent marker did not change PASS. The post-fence copies and file hashes were
verified; these completed jobs had already disposed checkpoint state, leaving
four process logs in each copied attempt root. HA-state copying is covered by
Docker-free tests, not these controls. These runs preserve the canonical
control's input, timing, naming strategy and expected outcome; `broker-default`
observed transaction level **2**.

The local live results cover the stated public, single-broker combinations. Private
images, `confluentinc/cp-server`, Scala-suffixed Flink distributions, other
Flink/connector versions, three-node generic clusters and the pinned
`apache-kafka` launcher have not been exercised locally.
The full calibration matrix has not been rerun with the rebuilt workload.
Docker-free tests and offline fixtures cover the remaining configuration and
verification paths without claiming live compatibility.
