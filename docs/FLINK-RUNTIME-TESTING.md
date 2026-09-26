# Testing a Flink runtime build

The engine can run its existing Kafka scenarios on a custom Flink **2.2** image using
the official image's entrypoint and filesystem layout. `setup.flink.image_id` pins the
expected local Docker image ID. The engine checks the created container before starting
each Flink process, including replacement TaskManagers. A tag alone is not a build
identity: rebuilding or retagging an image must not silently change the tested runtime.

## Select the image

Build the desired Flink distribution and compatible image separately, in a local checkout
of the project under test. Keep the source revision and build commands with your run
evidence. The engine does not patch Flink, inject tests into its source tree, or infer a
source commit from a Docker tag.

For an already-built image, obtain its local image ID:

```sh
docker image inspect --format '{{.Id}}' local/flink:2.2.0-candidate
```

Copy the complete `sha256:...` value into a copy of the selected scenario under an ignored
catalog such as `jobs/runtime-candidate/catalog/`, along with its expected-result sibling:

```yaml
setup:
  flink:
    image: local/flink:2.2.0-candidate
    image_id: sha256:<64-lowercase-hex-characters-from-docker-inspect>
    jobmanagers: 1
    taskmanagers: 1
```

This is a fragment; retain the scenario's other fields, faults, timings, load, and
expectations. Image IDs support ordinary string parameter interpolation. Validate and
run the copied catalog through the normal engine after `mvn clean install`:

```sh
mvn -q exec:java -pl cli -Dexec.args="validate --catalog-root jobs/runtime-candidate/catalog --scenario bounded-eos --artifact-root . --offline"
mvn -q exec:java -pl cli -Dexec.args="run --catalog-root jobs/runtime-candidate/catalog --scenario bounded-eos --artifact-root . --offline"
```

Run the release baseline with its own image ID using the same scenario contract. Keep
both results, including failures. A green candidate only covers the exercised scenario.

## Pin the runtime JAR

To check a particular distribution artifact already present in the image, add its
container path and the SHA-256 from your retained build artifact:

```yaml
setup:
  flink:
    image: flink:2.2.0
    runtime_jar:
      container_path: /opt/flink/lib/flink-dist-2.2.0.jar
      sha256: "${runtime_jar_sha256}"
```

Declare `runtime_jar_sha256` as a string parameter containing the 64 lowercase hex
characters. The engine hashes that file before Flink starts and after startup readiness,
for each initial and replacement container. For this optional check, TaskManager readiness
requires the standard `Starting TaskManager with ResourceID:` log message. Custom logging
that removes the marker cannot satisfy this startup check.

After the process fence, every JobManager must show `ResourceManager` loaded from the
verified path, and every TaskManager must show `TaskExecutor` loaded from it. Each log
has an explicit association with its physical container; replacement evidence cannot
cover a missing predecessor. The JAR check is `not-requested` when the descriptor is absent.

## Check runtime evidence

Every successful Flink process start records the logical component, role, physical
container ID, declared image reference, observed Docker image ID, and connector bundle
identities. The JSON result retains every incarnation, including replaced TaskManagers.
Each component's `connectorArtifactsRef` indexes the shared `connectorArtifactSets`
array. Only identical ordered JAR entries are shared; each process retains its own
observed hashes. The expected logical components and roles come from the executable plan.
The runtime identity check requires one consistent image across the attempt and a match
with `image_id` when supplied. Missing or inconsistent evidence cannot produce a passing
scenario, including an expected-failure control; an observed data failure stays recorded.

When `image_id` is omitted, the first created Flink container pins the image for the
attempt. Later containers must match before starting. This protects one attempt against
tag changes but does not select the intended candidate across separate runs. Use an
explicit ID for a PR comparison.

Docker's local image ID identifies the image configuration and its root filesystem
layers. It is distinct from a registry manifest digest (`RepoDigests`), and works for a
locally built image that has never been pushed. Neither identity proves a source commit
or that a particular runtime class or changed code path executed. Connector class-load
evidence remains a separate check. The optional runtime JAR check adds binary provenance
under trusted-image and immutable-JAR assumptions: it does not establish a Git revision,
reproducible compilation, absence of bytecode transformation, or method execution.
Concurrent JAR mutation after startup verification is unsupported.

`evidence.flinkRuntime.runtimeJar` reports the expected artifact and observed class sources.
Each component's `runtimeJar.classLoadProcess` joins these sources to its container ID;
the component also retains the observed path and hash. Both positive results and
expected-failure controls require confirmation when the check is requested.

## Supported scope

The compatibility registry still requires Flink 2.2 and a compatible Kafka connector and
workload. An image ID does not override this check or establish compatibility with another
version. The executable topology and fault types remain those listed in the
[README](../README.md#current-executable-subset).

## Verification

On 2026-09-27, the runtime JAR change passed the JDK 21
`mvn clean install` suite with 701 tests and no failures, errors or skips;
the Python tooling suite passed 16 tests.

Docker checks cover the initial image-identity implementation at `589f3ba`, before
the subsequent inventory, report, and runtime JAR changes. A real `bounded-eos` run on the released `flink:2.2.0` image with
its explicit image ID passed: all 3,000
records were present exactly once, the TaskManager kill/recovery effect was confirmed,
and the JobManager plus both TaskManager incarnations had matching image IDs. A second
run with a deliberately wrong image ID stopped before Flink process start, returned
`inconclusive / infrastructure.flink-start-failed`, and retained both IDs in diagnostics.
These checks validate the engine's image verification, not a Flink PR or token behavior.
Runtime JAR checks were verified separately on `8ac0c69`, also with released Flink 2.2.0.
The explicitly pinned JAR passed `bounded-eos` with 3,000 exact records, confirmed
checkpoint recovery, matching hashes in all three containers, and the required runtime
class sources in all three retained JVM logs. The wrong-hash control stopped before
Flink startup with `inconclusive / infrastructure.flink-start-failed`, no accepted Flink
components, and both expected and actual hashes retained. The canonical scenario's faults,
timing, load, and expected outcome were unchanged. This still does not exercise a Flink PR.
