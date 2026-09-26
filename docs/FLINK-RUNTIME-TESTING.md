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
evidence remains a separate check. Runtime JAR checksums and runtime class-load evidence
remain work to complete before claiming source-level PR provenance.

## Supported scope

The compatibility registry still requires Flink 2.2 and a compatible Kafka connector and
workload. An image ID does not override this check or establish compatibility with another
version. The executable topology and fault types remain those listed in the
[README](../README.md#current-executable-subset).

## Verification

On 2026-09-27, after the inventory and report review fixes, the JDK 21
`mvn clean install` suite passed 680 tests with no failures, errors or skips;
the Python tooling suite passed 16 tests.

Docker checks cover the initial image-identity implementation at `589f3ba`, before
those review fixes. A real `bounded-eos` run on the released `flink:2.2.0` image with
its explicit image ID passed: all 3,000
records were present exactly once, the TaskManager kill/recovery effect was confirmed,
and the JobManager plus both TaskManager incarnations had matching image IDs. A second
run with a deliberately wrong image ID stopped before Flink process start, returned
`inconclusive / infrastructure.flink-start-failed`, and retained both IDs in diagnostics.
These checks validate the engine's image verification, not a Flink PR or token behavior.
Docker checks were not repeated for the inventory and report review fixes.
