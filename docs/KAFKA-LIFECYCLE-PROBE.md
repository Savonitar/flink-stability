# Kafka lifecycle probe

`tools/kafka_lifecycle_probe.py` supervises **one explicitly selected** real-Docker
lifecycle case. It does not run rolling scenarios or a chaos batch and never
retries a case. A passing selected image is evidence for that configuration;
it does not qualify arbitrary custom images or a connector.

## Build the exact launcher

Use Python 3.10 or newer, a POSIX host, JDK 21 and Maven, and a local Docker daemon
with the selected platform available. Build a fresh checkout of the reviewed
source. Keep the build inside the selected checkout; choose output according to
the path policy below. Never run `mvn clean` over retained logs, checkpoints or
historical evidence. Do not rebuild or edit
source while the probe runs.

From the checkout root, select the JDK explicitly and run the fixture's offline
checks through the reactor. `JAVA_HOME` must already name the intended JDK 21:

```sh
export PATH="$JAVA_HOME/bin:$PATH"
mkdir -p jobs
mkdir jobs/kafka-probe-build
"$JAVA_HOME/bin/java" -version > jobs/kafka-probe-build/java-version.txt 2>&1

git rev-parse HEAD > jobs/kafka-probe-build/source-head.txt
git diff HEAD --binary > jobs/kafka-probe-build/source.patch
mvn -B -ntp -pl cli -am \
  -Dtest=KafkaGracefulStopDockerTest,KafkaLifecycleInspectionTest,KafkaLaunchContractTest,CliShutdownLoggingTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dflink.kafka.lifecycle=false \
  verify > jobs/kafka-probe-build/verify.log 2>&1
```

Stop if Maven fails. This builds reactor dependencies from the same checkout;
do not substitute an installed, unrelated `0.1.0-SNAPSHOT`. Launcher classes must
come from this checkout's `testcontainers/target` outputs or a matching source
snapshot built beneath its ignored `jobs/` tree. A separate checkout's launcher
is rejected. Retain the command,
JDK version, source revision/patch, Maven log and Surefire reports. The source
identity and compiled artifact hashes are separate observations; hashes alone do
not prove when or from which sources an existing class directory was compiled.

Pass the successful fixture report directly with `--surefire-report`. The tool
checks the exact suite, zero failures/errors and an executed
`evidenceSerializationWorksWithoutDocker` test; the opt-in live test may be
skipped. It retains the report identity, raw Surefire classpath and ordered
explicit entries after dropping empty separators. Duplicate classpath properties,
invalid paths, a launcher from another checkout, duplicate Jackson providers or
incompatible Jackson versions reject before live execution. Core, databind and
annotations must each have one 2.18.4 provider.

`--classpath` remains available and is mutually exclusive with
`--surefire-report`. It receives the same path, launcher and Jackson checks, but
requires explicit nonempty entries; an empty entry must never add the current
working directory implicitly. Both modes bind actual runtime bytes and preserve
source/build change detection. A successful report and matching source files do
not prove that existing classes were compiled freshly: retain the fresh-build
command and its successful reports. Do not substitute an older build merely
because its serialization check passes.

The supervisor also runs `KafkaGracefulStopDockerTest --verify-evidence` with the
exact supplied Java executable and classpath **before creating live resources**.
This exercises the shared timestamped, atomically published JSON writer on that
launcher. Its failure prevents the live launch. Keep the Java preflight output.

## Select an image and run one case

| Case | Launch contract |
|---|---|
| `default-apache` | Default Apache target and ordinary broker settings |
| `custom-apache` | Explicit image-ID pin, Apache launcher, custom retention property |
| `generic-apache` | Explicit image-ID pin, generic KRaft Apache paths and custom retention |
| `generic-confluent` | Explicit image-ID pin, generic KRaft Confluent paths |

Resolve and review the intended public image **before** running the probe. If
needed, download that image with `docker pull --platform <platform> <reference>`.
Inspect it with `docker image inspect <reference>` and select its configuration
ID, OS/architecture (including the variant whenever inspection reports one),
and the full matching repository
digest. A tag alone is not reproducible evidence. If inspection has multiple
repository digests, choose the intended one explicitly; do not take element zero.
Do not replace a requested pin after an unexpected mismatch.

Set `KAFKA_PROBE_IMAGE`, `KAFKA_PROBE_IMAGE_ID` (`sha256:<64 hex>`),
`KAFKA_PROBE_DIGEST` (`repository@sha256:<64 hex>`) and `KAFKA_PROBE_PLATFORM`
from that reviewed selection. The following runs only the selected case; use a
new output path for every separately requested run:

```sh
: "${JAVA_HOME:?Select JDK 21}"
: "${KAFKA_PROBE_IMAGE:?Select the image reference}"
: "${KAFKA_PROBE_IMAGE_ID:?Select the configuration ID}"
: "${KAFKA_PROBE_DIGEST:?Select the full repository digest}"
: "${KAFKA_PROBE_PLATFORM:?Select OS/architecture[/variant]}"
python3 tools/kafka_lifecycle_probe.py \
  --case custom-apache \
  --java "$JAVA_HOME/bin/java" \
  --surefire-report "$PWD/testcontainers/target/surefire-reports/TEST-org.savonitar.flink.stability.testcontainers.KafkaGracefulStopDockerTest.xml" \
  --image "$KAFKA_PROBE_IMAGE" --image-id "$KAFKA_PROBE_IMAGE_ID" \
  --digest "$KAFKA_PROBE_DIGEST" --platform "$KAFKA_PROBE_PLATFORM" \
  --source-root "$PWD" --output "$PWD/jobs/kafka-lifecycle-custom-001"
```

Use `--docker-host unix:///path/to/docker.sock` if the daemon uses a different
local socket, and `--docker /path/to/docker` to select its executable. The same
daemon is passed to the Java process. The supervisor uses an empty Docker config;
it does not log in, read credential stores, or upload anything. Its outputs record
resolved image inputs, Java/classpath identities, source state and commands.
Image availability and the requested pins must be satisfied before launch.

## Output path policy

The output must be fresh and must not overlap any classpath/build input or the
selected source tree itself. Inside `--source-root`, choose an ignored directory
such as `$PWD/jobs/kafka-lifecycle-custom-001`; tracked or unignored output is
rejected. An external output directory is also supported when it is distinct from
all source/build inputs and within the user's permitted filesystem scope. Its
parent must already exist. Symlink ambiguity is rejected.

The tool validates this policy before creating output, launching Java or invoking
Docker. Ignoring run output prevents its changing command log from becoming part
of source identity; genuine tracked/untracked source and build mutations still
invalidate the run. Do not add ignore rules during an invocation or weaken the
source check to make a run pass.

## Supervision and evidence contract

The external supervisor owns a unique labelled network and session. It supplies
all eight `flink.kafka.lifecycle.*` inputs: `case`, `image`, `imageId`, `platform`,
`digest`, `session`, `networkId`, and `output`, plus the `flink.kafka.lifecycle=true`
opt-in. Its reserved output directory and the Java child evidence directory are
distinct: the latter must not exist because the fixture creates it. Stale output,
pre-existing cleanup authorization and accidental overwrites reject.

The phases use independent monotonic deadlines within an overall **360 seconds**:

| Phase | Maximum | Boundary |
|---|---|---|
| Preflight | 60 seconds | Input validation, source/build hashing, exact-classpath serialization, Docker inventory and image verification |
| Live startup | 120 seconds | Before owned network creation through the fixture's pre-TERM inspections and `term-submitted.json` |
| Shutdown observation | 120 seconds | TERM submission through accepted or failed `observation-ended.json` |
| Cleanup | 60 seconds | Observation boundary or earlier failure through reaping, owned cleanup and final checks |

Unused time is not permission to extend a phase. Startup starts after preflight;
slow hashing or serialization cannot consume its advertised allowance. Cleanup
retains a reserved opportunity even when startup or observation expires. Docker
commands and serialization subprocess waits are bounded within their phase.
Preflight and lifecycle processes have separate exit fields and launch states.
An execution exit is populated only when that JVM reached its fixture evidence;
otherwise it is null and the raw wrapper exit remains separate. In particular,
`exec-submitted-unconfirmed` can mean an early JVM failure or interruption: it
does not prove that no JVM launched. No phase timeout starts another case.

After `observation-ended.json` arrives, the supervisor durably records that
boundary before writing `cleanup-authorized`. A failed observation still needs
bounded cleanup. The token is never created in advance. It must be a regular, non-symlink file
containing exactly the session plus a newline; an incomplete matching prefix
remains pending within the same deadline. The Java fixture allows at most
10 seconds for authorization inside its fixed 45-second cleanup budget;
owned runtime cleanup reserves the final 10 seconds for callback cleanup. The
supervisor permits at most 50 seconds for Java to finish and retains the remaining
outer cleanup time for process reaping, network removal and reconciliation.

A missing token is a failed invocation, even when the Java safety cleanup
succeeds. Authorization waiting and safety cleanup are separate actions: Java
still attempts ownership verification, runtime cleanup and callback cleanup,
retaining the original failure and a separate safety-cleanup receipt. Identity
mismatch forbids destructive cleanup. Safety-induced death cannot satisfy STOP,
replace pre-cleanup observations or emit normal success postconditions.

Ryuk is disabled for this probe; the supervisor owns the labelled broker/network
cleanup, including fallback after interruption or a missing handshake. SIGINT,
SIGTERM and SIGHUP enter bounded cleanup. **SIGKILL and OOM cannot be intercepted.**
Java fallback helps only if Java survives and reaches cleanup. An interrupted
machine or surviving resource may require the separate reconciliation command
below. Normal cleanup and reconciliation never prune globally or remove by a
broad name pattern. Forced removal is cleanup and leaves the attempt failed.

The Java fixture verifies the selected image ID/digest/platform, actual command,
relevant configuration and custom launch receipt; it retains inspection/transfer
outcomes before assertions. Apache entrypoint preservation compares against the
validated selected image, including a compatible custom entrypoint. Null and
empty entrypoints remain distinct and are allowed only when the corresponding
container value matches exactly. Entrypoint equality alone proves neither PID 1
nor signal delivery. The independent process/namespace/start-time proof, Kafka
TERM/shutdown logs and container exit **before cleanup** remain required. Record
the actual exit code; do not require 143.

`kafka-continuous.log` contains **normalized textual output** from Testcontainers
1.21.4: its frame adapter decodes/re-encodes UTF-8 and strips ANSI color sequences.
The direct sink-byte test preserves bytes delivered to the sink; it does not
claim raw Docker transport capture. END delivery cannot substitute for natural
completion. Transport/sink failures remain visible through waits and final close;
a successful independent final log fetch cannot rescue a failed continuous stream.
Late errors observed before final teardown also invalidate capture.

`result.json` records accepted shutdown observation before cleanup. It is not an
overall verdict. The final `log-evidence.json` booleans are success postconditions
emitted only after healthy natural completion and successful close; they are not
snapshots of private callback flags. In particular, starting natural completion
or starting close is not proof that cleanup completed. The inherited synchronized
`awaitCompletion` can wait beyond its nominal timeout while close holds its
monitor; the fixture's evidence deadline and external supervisor provide the live
bounds.

Overall success additionally requires a zero child exit, the final healthy
natural-completion receipt, successful cleanup evidence, and independent absence
of the exact owned containers and network. Missing/malformed receipts, observer
failure, ownership mismatch or incomplete cleanup fail the invocation. Keep the
first failure and secondary cleanup diagnostics. Preserve all failed attempts;
there is no automatic retry. Run Docker workloads serially and retain historical
resources, catalogs, faults, timing, load and data oracles unchanged.

## Reconcile an interrupted run

Only after the original run is inactive, use its existing supervisor directory:

```sh
python3 tools/kafka_lifecycle_probe.py \
  --reconcile "$PWD/jobs/kafka-lifecycle-custom-001"
```

This performs idempotent cleanup within one **60-second** deadline; it never
relaunches the fixture. It uses
the recorded session, case, local Docker endpoint, image and resource identities,
checks exact IDs, ownership labels and network membership, and searches ambiguous
creation only through the exact session labels. Foreign resources are refused.
The original Java process must be unable to create more resources before deletion
starts. An active run, an unverified process owner or incomplete launch identity
is a refusal, not permission to guess. A saved PID or process-group ID alone
cannot establish ownership because IDs can be reused. Recovery uses Linux
process-start ticks plus boot identity, or Darwin kernel process-start time,
and refuses unavailable identity or a surviving process-group member.

Each reconciliation writes separate receipts and preserves all original evidence
and the original verdict. Repeated cleanup of already absent owned resources may
succeed; it cannot retroactively pass the test. Older receipts that lack sufficient
process-start/ownership information may require manual investigation. Do not edit
those historical receipts to make them acceptable to recovery.

## Offline regression checks and coverage limits

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/tests \
  -p 'test_kafka_lifecycle*.py' -v
```

These fake-process/clock tests cover supervision, ownership and partial-success
rejection without Docker. Java fixture and launcher tests cover entrypoints,
transport failure, normalization and starter preservation. Live integration of a
changed supervisor requires a separately requested fresh run; old manually
supervised lifecycle receipts do not verify this new tool. A future Testcontainers
upgrade must rerun `KafkaLaunchContractTest` and the selected live probe before
claiming its starter contract is supported. Structural starter mismatch diagnostics
must not include raw scripts, listeners or environment values.
