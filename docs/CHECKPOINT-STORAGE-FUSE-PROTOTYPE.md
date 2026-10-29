# Checkpoint-storage FUSE prototype

Historical evidence links open [sanitized copies and compact summaries](evidence/README.md).
Printed receipt/result hashes identify the **raw originals**, retained unchanged
under local `jobs/`. The [publication manifest](evidence/checkpoint-storage-publication-manifest.json)
records each copy's separate SHA-256 and omissions. Full journals, binaries and
command transcripts are not included; the copies cannot rerun the raw audit.

The review rebuild on 29 October 2026 is a new artifact:
[build 02 receipt](evidence/checkpoint-storage-helper-build-20261029-02/build-evidence.json),
original manifest SHA-256
`50721e4f8f3bd515ca9f445f7a96aee0d3f04da21448a744088bdd4045454e2d`.
It contains aggregate IO counters, read forwarding after journal exhaustion,
and control-connection handling that aborts only after a lost accepted mutation
acknowledgement. It passed strict Linux compilation, 100,357 native selftest
checks and the unprivileged Java probe smoke in the pristine Flink image;
both owned containers were removed. All 51 locked package receipts were checked
before installation, with network disconnection verified before installation
and before source copy. Its observed store was classic `overlay2`.

| New artifact | SHA-256 |
| --- | --- |
| `checkpoint-fuse` | `aa2f74a87e73551c6ddb9c6e9285f33e1d14fbd476212fec18a024a9a33f6aef` |
| `checkpoint-fuse-selftest` | `297b4070df3dfcfc6e82849f1024b55c4b4fd64f5cf61be997b46661876cc83a` |
| `probe.jar` | `2fd880248a7515b08ee419e1ff875cc1edff8e0f784878aa55a0aaa0a1780983` |

[Build 01](evidence/checkpoint-storage-helper-build-20261029-01/build-evidence.json)
retains its selftest compilation failure and successful cleanup. The corrected
build used a new directory. These build/smoke checks do not mount FUSE or qualify
an integrated Flink campaign; the 27 October results below retain their original
artifact identities and scope.

The PR 3 prototype implements a bounded filesystem write-failure experiment
using two FUSE views of one fresh Docker named volume and a standalone Java I/O
probe. It does not yet provision a Flink job or inject a failure into a real
Flink checkpoint. That integration is [PR 4 in the implementation plan](STATE-RECOVERY-STORAGE-FAULT-PLAN.md#pr-4--typed-storage-control-and-checkpoint-failure-orchestration).

**Qualified on Docker Desktop `linux/arm64`, 2026-10-27.** The
[final retained run](evidence/checkpoint-storage-probe-20261027-02/result.summary.json)
returned PASS and exit 0 for all seven cases. Both primary instances produced
matching native EIO receipts for Java create/write failures; healing restored
cross-mount reads of the exact 23 bytes `before-fault-after-heal`. The same JVM
survived daemon death and received `ENOTCONN`. Cancellation and the Java watchdog
returned 143 and 124; terminal ABORTED/EXPIRED rules rejected healing.

All four daemon journals were complete (264, 273, 15 and 15 receipts), captured
after daemon termination. Four containers and the shared volume were removed.
Independent read-only auditing confirmed the outcomes, raw journals, command
receipts, and frozen helper/harness hashes. Earlier failures remain retained
below; none was relabeled as a successful attempt.

## Execution boundary

The [native helper](../testcontainers/src/main/native/checkpoint-fuse/checkpoint_fuse.c)
mounts `/view` independently inside each container. Both views expose only the
generated `job_<id>` namespace from the same attempt-owned volume at `/backing`.
The volume root and raw namespace are owned by root with mode `0700`. The Java
process uses the image's normal Flink UID/GID `9999:9999`, with no effective
capabilities. Explicit raw-read and raw-write probes must fail.

Daemon control sockets and journals live under `/work/control`; Java evidence
lives under `/work/java-evidence`. These paths are outside both the FUSE view and
the raw storage. An inaccessible underlying mount directory prevents an
unmounted view from becoming a writable fallback. Each daemon uses an
`openat`/`O_NOFOLLOW` path jail, rejects traversal and unsupported object types,
and refuses symlinks and multiply linked regular files.

Runtime containers have networking disabled, all capabilities dropped except
`SYS_ADMIN`, and only `/dev/fuse` passed as a device. They use a named volume,
not a host bind mount. No privileged container, host namespace, host block
device, Docker socket mount, or security-profile bypass is used. Two primary
containers exercise the shared-storage cases; separate short-lived instances
exercise cancellation and watchdog expiry.

Docker documents the need for both the FUSE device and mount capability, and
the equivalence of capability names with or without the `CAP_` prefix. The
checker still requires exactly the single permitted capability.
See [Docker's runtime privilege and capability documentation](https://docs.docker.com/engine/containers/run/#runtime-privilege-and-linux-capabilities).

The helper sets `direct_io=1`, disables writeback, kernel and automatic data
caching, and sets entry, attribute and negative-lookup timeouts to zero. These
settings make write failures and changes through the other mount observable;
they are not a performance benchmark configuration. The corresponding cache
semantics are described in the [libfuse configuration reference](https://libfuse.github.io/doxygen/structfuse__config.html).

## Build and run

Prerequisites are Python 3.10+, JDK 21, a Docker CLI, and an explicitly selected
local Unix Docker endpoint. The checked-in dependency lock and helper build are
currently for `linux/arm64`. A native Linux host and Docker Desktop require
separate live qualification; this document does not establish `linux/amd64`
support. If the selected environment cannot provide `/dev/fuse` or the required
mount capability, the probe records an unsupported result.

The [builder](../testcontainers/checkpoint_storage_build.py) uses the existing
Flink distribution image. It installs the exact 51-package closure from
[dependencies.json](../testcontainers/src/main/native/checkpoint-fuse/dependencies.json)
in a temporary build container, verifies package SHA-256 values against the
lock, and retains the downloaded packages. The lock was derived from Ubuntu's
signed archive indexes. The container is disconnected from its network before
repository sources enter it. Sources, commands, compiler output and hashes are
retained in the new build directory.

The resulting helper links libfuse 3.14.0 statically; its only dynamic dependencies
are the original image's libc and loader. The Java probe is compiled with JDK 21
and `--release 17`. A second pristine image container runs native self-tests and
a Java hello/quit exchange as UID/GID `9999:9999`, with all capabilities dropped.
The build produces a separate bundle; it does not rebuild Flink JARs or create
a derived runtime image.

Both tools use a fresh empty Docker client configuration and a minimal process
environment. Missing images may be downloaded by the pinned public digest;
cached images are not refreshed. Identity and platform are checked before use.
If a locked Ubuntu package becomes unavailable or differs from its recorded
hash, the build fails rather than silently selecting another version.

Run from the repository root. Replace the two executable paths below and choose
new output directory names for every build and probe. The endpoint shown is the
explicit local default; substitute another local `unix:///...` socket only when
that is the intended daemon.

```sh
CHECKPOINT_JDK21=/path/to/jdk-21
CHECKPOINT_DOCKER=/path/to/docker

python3 testcontainers/checkpoint_storage_build.py \
  --java-home "$CHECKPOINT_JDK21" \
  --docker "$CHECKPOINT_DOCKER" \
  --docker-host unix:///var/run/docker.sock \
  --output jobs/checkpoint-storage-helper-local-01

python3 tools/checkpoint_storage_probe.py \
  --bundle jobs/checkpoint-storage-helper-local-01 \
  --output jobs/checkpoint-storage-probe-local-01 \
  --image-id sha256:2e93ded2cb7bf1a50f71b663d5e3ea01420bcc4e549cbf666d05120f939713c7 \
  --platform linux/arm64 \
  --docker-host unix:///var/run/docker.sock
```

`--bundle` names the build output containing `build-evidence.json` and `bundle/`.
The runtime adapter verifies the complete bundle and copied helper hashes before
use. The probe writes `result.json`, `observations.json`, Java transcripts,
daemon journals, Docker command receipts and cleanup evidence. Exit zero means
the standalone probe qualified; a non-pass result exits one. This entry point
does not change the native scenario runner's executable contract. Normal Maven
and Python unit tests do not require FUSE or Docker.

## Evidence and bounds

The first rule injects `EIO` into file creation and writes, including truncation,
inside the exact generated namespace. Path validation precedes fault matching.
Reads of existing data, metadata operations and cleanup remain available.
Healthy `fsync`/`fdatasync` are forwarded to the backing filesystem. Application
failure is observed at create/write, including an already-open Java
`FileChannel`; it is not inferred from a close callback.

Each control command binds attempt, storage and daemon instance identity,
revision, and the selected fault. Mutations require the next revision. Leases
are bounded to 1–120,000 milliseconds. Expiry and abort retain terminal
`EXPIRED`/`ABORTED` evidence and reject healing; a timed-out or cancelled attempt
cannot become successful through automatic healing. The ordinary write-failure
case uses a 30-second lease; the expiry case uses a separate short lease.

The native journal records relative paths, operation, result/errno, byte counts,
timestamps, identity, revision, lease and sequence. Successful acknowledgements
follow durable receipts. Default journal bounds are 8 MiB and 10,000 records;
hard maxima are 64 MiB and 100,000 records. A reserved terminal receipt reports
budget exhaustion. Incomplete, malformed or lost journal evidence invalidates
qualification.

Native paths are bounded to 1,024 bytes and 32 components, with generated ASCII
component names; the Java probe imposes a stricter 256-character relative-path
limit. The Java protocol permits at most 256 commands, 8 open handles, 4 KiB
write payloads and 16 KiB reads. Its process watchdog is at most 120 seconds.
The supervisor has a 300-second execution budget, per-operation limits and a
separate 30-second cleanup budget. Cleanup checks exact container/volume
ownership and retains failures in the result. The build has its own deadline
and removes only its uniquely identified temporary containers.

## Seven qualification cases

| Case | Required evidence |
| --- | --- |
| Cross-container visibility | Both normal-UID JVMs read bytes written through the other FUSE view. |
| Create and held-descriptor EIO | New-file creation and a write through a previously opened channel raise real Java I/O exceptions, correlated with matching native `errno=5` receipts while the rule is active. |
| Read preservation and healing | Stored bytes remain unchanged and readable during the fault; failed new files remain absent; writes and creates succeed after explicit healing. |
| Healthy filesystem operations | Both force variants, rename, cross-mount stat/list and deletion succeed with matching data and operation evidence. |
| Daemon death | The targeted daemon is killed, the existing JVM remains alive, and view access fails without falling back to raw storage. |
| Cancellation, deadline and lease | Separate cancelled-JVM, watchdog-exit and expired-rule cases retain aborted/expired state, reject healing and finish through bounded cleanup. |
| Namespace, identity and evidence isolation | Traversal, a foreign namespace, unsafe symlink entries and stale/foreign control commands are rejected; raw storage access fails and evidence writes remain possible during the fault. |

The independent qualification function checks these observations together with
container capabilities, mounts, process identity and contiguous journals. Bundle
and copied-helper hashes are verified before startup; frozen helper/harness
hashes are retained for subsequent auditing. Running all commands without those checks is insufficient for PASS.

## Offline verification

JDK 21 `mvn -o -B -ntp verify` passed across the reactor: 1,505 Java tests,
zero failures, one opt-in Kafka lifecycle Docker test skipped because its
activation property was unset. This includes 17 focused Java probe tests.
`python3 -m unittest discover -s tools/tests -v` passed all 314 tests, including
41 probe/qualifier tests and 15 builder tests. Native build checks and the live
Docker observations are reported separately below.

[Retained commands and test results](evidence/checkpoint-storage-checks-20261027-01/checks.json)
link to the complete Maven/Python output. Documentation links and
`git diff --check` passed. No normal CI check starts FUSE or requires Docker.

## Retained artifacts and observed environment

The final helper is [build 03](evidence/checkpoint-storage-helper-build-20261027-03/build-evidence.json).
It passed 243 native core/jail checks in the pristine image, plus the Java 17
probe exchange under UID/GID `9999:9999` with effective capabilities zero.
These build checks do not themselves mount FUSE. The dependency-lock SHA-256 is
`1458d129527d65c94d60fee9226cd557049f4e89b541b6f1aa93f7a3d516ba47`.

| Artifact | SHA-256 |
| --- | --- |
| `checkpoint-fuse` | `49d01b4f62b6a7bd8256779777cf1e6d193d2e1d7365796c115f4ffb66c85f66` |
| `checkpoint-fuse-selftest` | `bbdd6a07da940c9d9db135958ec7f1caf8502f03767203f349aa65b5c7a3b9d5` |
| `probe.jar` | `3c7e87c4a5e924116dd3ac47cc5f4cb6c2b1e4d1849fe278ee7a50d7fcc454b7` |

The image digest is
`flink@sha256:7ecfab9d5e8ec6987a248b32f6605b5fe9bcb7347aea0d7b1bc3976588408681`.
The 27 October 2026 qualification used the classic graphdriver image store.
Its arm64 image config digest is
`sha256:2e93ded2cb7bf1a50f71b663d5e3ea01420bcc4e549cbf666d05120f939713c7`;
the containerd image store reports index digest
`sha256:7ecfab9d5e8ec6987a248b32f6605b5fe9bcb7347aea0d7b1bc3976588408681`.
The integrated adapter now verifies author-declared `image_id` against both
container ID and image `RepoDigests`, recording the matched form and daemon
store backend. Optional `image_digest` is the content-addressable repository
pin; `image_id` depends on the backend. These identity checks preserve the
historical qualification scope.

The observed environment is Docker Desktop 4.56.0, Docker Engine 29.1.3,
`linux/arm64`, kernel `6.12.54-linuxkit`, and Temurin Java 17.0.19. The helper
uses GCC 13.3.0 and static libfuse 3.14.0. A native Linux qualification has not
been performed.

Earlier artifacts remain immutable:

- [Build 01](evidence/checkpoint-storage-helper-build-20261027-01/build-evidence.json)
  passed its original checks before journal reservation hardening.
- [Build 02](evidence/checkpoint-storage-helper-build-20261027-02/build-evidence.json)
  passed after that hardening. Build 03 additionally contains namespace-before-fault
  matching and the builder's pinned-image download fallback.
- [Pilot 01](evidence/checkpoint-storage-probe-pilot-20261027-01/result.summary.json)
  failed when runtime `chmod` of copied artifacts was denied under reduced
  capabilities. The adapter now verifies prepared file modes without that chmod.
- [Pilot 02](evidence/checkpoint-storage-probe-pilot-20261027-02/result.summary.json)
  failed when cancellation attempted to signal the JVM across UIDs. Cancellation
  now sends the signal as the JVM's own UID.
- [Qualification attempt 01](evidence/checkpoint-storage-probe-20261027-01/result.summary.json)
  completed the seven cases, then failed the capability spelling check described
  above. Read-only reevaluation with the exact alias fix recognizes the seven
  cases; the stored failed result is preserved and is not relabeled as PASS.

PR 4 still needs typed storage control, actual Flink checkpoint-file
materialization evidence, correlation with one failed checkpoint, TaskManager
replacement and state-oracle verification. This prototype establishes neither
that integrated recovery result nor permanent disk loss, bit corruption,
ENOSPC, power-loss durability, RocksDB, rescaling or multi-worker coverage.
