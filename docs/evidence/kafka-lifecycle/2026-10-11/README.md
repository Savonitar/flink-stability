# Kafka lifecycle probe receipts — 2026-10-11

These are unmodified `completed.json` outputs from `tools/kafka_lifecycle_probe.py`
at clean source commit `51637486f056e042b02338816eb70ee7ccd264f5`. The published
supervisor was invoked once per case, serially, with `--surefire-report`, the
verified JDK 21 launcher build and an explicit Docker Desktop socket. No retries
or image downloads occurred. All three selected image identities were cached.

| Case | Image / platform | Result | Elapsed | Receipt |
|---|---|---|---|---|
| default-apache | apache/kafka:4.0.0 / linux/arm64 | Accepted | 7.04s | [completed.json](default-apache/completed.json) |
| custom-apache | apache/kafka:4.0.0 / linux/arm64 | Accepted | 6.15s | [completed.json](custom-apache/completed.json) |
| generic-confluent | confluentinc/cp-kafka:8.3.2 / linux/arm64/v8 | Accepted | 5.87s | [completed.json](generic-confluent/completed.json) |

[Provenance](provenance.json) records exact source/tool hashes, image configuration
IDs, repository digests, receipt hashes and the CI revision. The retained build's
420 source inputs and 264 runtime-file hashes matched before launch. Each
invocation also verified its exact classpath through the real Java serialization
preflight. Source and runtime identities stayed unchanged throughout execution.

Each accepted receipt records Kafka shutdown observation before cleanup, zero
preflight/lifecycle exits, healthy continuous-log evidence and successful owned
cleanup without forced container removal. Its `evidenceHashes` bind the full
locally retained inspection, transfer, process, configuration, log and cleanup
artifacts; the artifact contents are not included here. The pre-attempt bridge
checks were stable; final checks retained all ten historical stopped containers
and all five pre-existing networks. No resources remain from these invocations.

[CI for PR head 5163748](https://github.com/Savonitar/flink-stability/actions/runs/38133829723)
ran the GitHub merge revision recorded in provenance: **1,386 Java tests selected
(1,385 passed, one opt-in live test skipped)** and **222 Python tests passed**.
These live receipts are separate from CI. Generic Apache was not rerun with this
tool in this sequence. Historical manually supervised lifecycle and rolling
results remain separate evidence; no failed historical attempt is reclassified.

The [probe contract](../../../KAFKA-LIFECYCLE-PROBE.md) describes reproduction,
acceptance and recovery. These results cover only the selected configurations;
they do not qualify a connector, arbitrary custom images or live `--reconcile`.
Launcher SLF4J diagnostics currently lack a provider (the live JVM emitted the
NOP-provider warning); the separately captured Kafka logs passed their checks.
Other retained follow-ups include recovery with unrelated running containers,
Darwin PID permission/reuse handling, the default socket on Docker Desktop,
and the custom receipt `confirms()` Apache command check.
