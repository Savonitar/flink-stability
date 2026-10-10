# Kafka lifecycle probe

`tools/kafka_lifecycle_probe.py` supervises **one explicitly selected** real-Docker
lifecycle case. It does not run rolling scenarios or a chaos batch and never
retries a case. A passing selected image is evidence for that configuration;
it does not qualify arbitrary custom images or a connector.

## Build the exact launcher

Use Python 3.10 or newer, a POSIX host, JDK 21 and Maven, and a local Docker daemon
with the selected platform available. Build a fresh checkout of the reviewed
source. Keep the build and output inside the repository; never run `mvn clean`
over retained logs, checkpoints or historical evidence. Do not rebuild or edit
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
do not substitute an installed, unrelated `0.1.0-SNAPSHOT`. Retain the command,
JDK version, source revision/patch, Maven log and Surefire reports. The source
identity and compiled artifact hashes are separate observations; hashes alone do
not prove when or from which sources an existing class directory was compiled.

Extract the resolved entries from the successful fixture test, including its
fresh `target/test-classes` and `target/classes` and reactor dependencies. Preserve
the raw Surefire value, then omit empty separator entries so the launcher never
implicitly searches its working directory:

```sh
python3 - <<'PYTHON'
from pathlib import Path
import os
import xml.etree.ElementTree as ET
report = Path('testcontainers/target/surefire-reports/'
              'TEST-org.savonitar.flink.stability.testcontainers.KafkaGracefulStopDockerTest.xml')
tree = ET.parse(report)
assert int(tree.getroot().get('failures', '0')) == 0
assert int(tree.getroot().get('errors', '0')) == 0
classpath = tree.find(".//property[@name='surefire.test.class.path']").get('value')
Path('jobs/kafka-probe-build/classpath.raw.txt').write_text(classpath)
entries = [p for p in classpath.split(os.pathsep) if p]
assert entries and all(Path(p).is_absolute() and Path(p).exists() for p in entries)
Path('jobs/kafka-probe-build/classpath.txt').write_text(os.pathsep.join(entries))
PYTHON
```

Verify the Jackson family on **this** classpath. The module BOM aligns Jackson
at 2.18.4; inspect the actual providers and reject duplicates instead of relying
only on a dependency-tree summary:

```sh
python3 - <<'PYTHON'
from pathlib import Path
import os, zipfile
classpath = Path('jobs/kafka-probe-build/classpath.txt').read_text().split(os.pathsep)
classes = {'com/fasterxml/jackson/databind/ObjectMapper.class': 'jackson-databind',
           'com/fasterxml/jackson/core/JsonFactory.class': 'jackson-core',
           'com/fasterxml/jackson/annotation/JsonCreator.class': 'jackson-annotations'}
for name, artifact in classes.items():
    providers = []
    for entry in map(Path, classpath):
        if entry.is_dir():
            if (entry / name).exists(): providers.append(entry)
        elif zipfile.is_zipfile(entry):
            with zipfile.ZipFile(entry) as jar:
                if name in jar.namelist(): providers.append(entry)
    assert len(providers) == 1, (name, providers)
    assert providers[0].name == artifact + '-2.18.4.jar', providers
    print(name, providers[0])
PYTHON
```

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
  --classpath "$(cat jobs/kafka-probe-build/classpath.txt)" \
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

## Supervision and evidence contract

The external supervisor owns a unique labelled network and session. It supplies
all eight `flink.kafka.lifecycle.*` inputs: `case`, `image`, `imageId`, `platform`,
`digest`, `session`, `networkId`, and `output`, plus the `flink.kafka.lifecycle=true`
opt-in. Its reserved output directory and the Java child evidence directory are
distinct: the latter must not exist because the fixture creates it. Stale output,
pre-existing cleanup authorization and accidental overwrites reject.

Startup, observation and cleanup are independently bounded at **120, 120 and 60
seconds**, with an **overall 360-second limit**. Docker/preflight calls are also
bounded. Timeouts use a monotonic clock. The supervisor reserves cleanup time even
when observation exhausts its deadline, drains child output to files, and reaps
only the child process/group it created. No phase timeout starts another case.

After `observation-ended.json` arrives, the supervisor durably records that
boundary before writing `cleanup-authorized`. A failed observation still needs
bounded cleanup. The token is never created in advance. Ryuk is disabled for this
probe; the supervisor owns the labelled broker/network cleanup, including fallback
cleanup after interruption or a missing handshake. It reconciles an ambiguous
creation only by this invocation's unique labels, validates IDs, labels and
network membership before removal, and independently verifies absence. It never
prunes globally or removes by a broad name pattern. Cleanup-induced death cannot
satisfy the TERM-only STOP observation; forced removal is recorded as cleanup and
leaves the invocation failed.

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

## Offline regression checks and coverage limits

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/tests \
  -p test_kafka_lifecycle_probe.py -v
```

These fake-process/clock tests cover supervision, ownership and partial-success
rejection without Docker. Java fixture and launcher tests cover entrypoints,
transport failure, normalization and starter preservation. Live integration of a
changed supervisor requires a separately requested fresh run; old manually
supervised lifecycle receipts do not verify this new tool. A future Testcontainers
upgrade must rerun `KafkaLaunchContractTest` and the selected live probe before
claiming its starter contract is supported. Structural starter mismatch diagnostics
must not include raw scripts, listeners or environment values.
