# Checkpoint-storage N1–N5 verification — 2026-10-30

N1–N5 are implemented. The final JDK 21 reactor installation ran **1,699 tests,
zero failures/errors, one existing opt-in Docker test skipped**. All **497 Python
tests** passed. `checkpoint-storage-fault --show-plan` accepted the freshly built
helper; the frozen live catalog also validated successfully.
`git diff --check` is clean, and all 310 checked local documentation links resolve.

The [verification receipt](evidence/checkpoint-review-n1-n5-20261030-01/verification.json)
contains selected results and the SHA-256 of every original build, validation,
test and live receipt. This sanitized summary has its own SHA-256:
`4c56b1e647889cedd2b3dfd94ce93bb1be4c87f64aa0d17b1d379d9532db2b4b`.
Original paths in that receipt are workspace-relative provenance labels, not
claims that the gitignored raw files are included in the repository. Raw logs,
journals, response bodies and frozen artifacts remain unchanged under the new
`jobs/checkpoint-review-n1-n5-20261030-01/` directory. The compact summary alone
cannot rerun the source-response audit.

## Offline regressions

- A logical 600-second test performs 6,000 polling iterations / 18,000 fake REST
  calls without creating a capture file. Only a selected proof group consumes
  the persistent budget. The 2 MiB response limit also applies to unretained
  polling; an oversized response does not poison the evidence store.
- Empty bodies produce valid zero-byte captures and empty-body SHA-256 values.
  Empty HTTP error retries remain auditable, and subsequent requests work.
- Missing required response groups, inconsistent utilisation and incomplete
  captures still reject confirmation. An expected data FAIL cannot qualify as
  scenario PASS with incomplete raw evidence or skipped/unconfirmed phase steps.
- Nonzero ID-less baseline failures pass the complete simulated fault sequence.
  A retained or latest identified FAILED checkpoint rejects before the inspector
  runs. Post-ARM counter-delta checks remain strict.
- Two real compilations with the same JDK produce byte-identical executable
  `probe.jar` files. The builder records the selected Java toolchain; archive
  normalization does not promise identical class bytes across JDK releases.

Final commands were `mvn -B -ntp install` with JDK 21 and explicit public-only
user/global Maven settings, and `python3 -B -m unittest discover -s tools/tests -v`.
Both full suites passed after correcting test fixtures for the new JDK identity
checks and the FUSE-only configuration fields. Earlier check logs are retained.

## Fresh helper

The helper was built with Amazon Corretto 21.0.7, Docker Engine 29.1.3 and the
classic `overlay2` image store on Docker Desktop `linux/arm64`. Native selftests,
the pristine-runtime Java smoke and builder cleanup passed. Its manifest SHA-256
is `67d958d62e1d1e3a35ad162f8b72c95483593912a084ca0f139fcfb4db0cb8d7`.

| Artifact | SHA-256 |
| --- | --- |
| `checkpoint-fuse` | `c45845195feb47442d993b3d4757bac74cf118bb88a50628ebadd7f3093830fe` |
| `checkpoint-fuse-selftest` | `f1799cbf544d85dbcc2fd0cb0b52736bb883ae57c4f608b9a95afa3a6be7f584` |
| `probe.jar` | `131f0713101a4e98532d4e3a0915b0d13ff953ee372d03bb0a3b6406498e4043` |

The native hashes match the historical build 03. This newly normalized Java
archive and its build receipt are separate artifacts; no historical helper was
replaced. Explicit Docker Desktop CLI/context instructions and the probe's
`--docker` option are described in the
[prototype guide](CHECKPOINT-STORAGE-FUSE-PROTOTYPE.md).

## Two live attempts

Both attempts used one frozen framework and correct-arm workload, separate
attempt directories, a 600-second outer timeout and no automatic retries. Both
returned `validator.kafka.state-prefix.match`: all 30,000 IDs appeared exactly
once across eight keys, with zero missing, unexpected, malformed, duplicate or
state-mismatched records. The independent checker confirmed all six causal fault
stages, state restoration, required raw responses and final storage evidence.

| Attempt | Result | REST captures | REST bytes | `Failed to trigger checkpoint` lines | Cleanup |
| --- | --- | ---: | ---: | --- | --- |
| 1 | PASS | 66 | 368,971 | 0 | Confirmed |
| 2 | PASS | 66 | 370,450 | 1, after deliberate kill while replacement tasks were unavailable | Confirmed |

Both stores report `complete: true`, against limits of 4,096 captures and
67,108,864 bytes. Each source-response audit checks 59 unique references across
21 groups, including Docker and inspector sources; that count is distinct from
the complete REST store inventory. Natural-completion polling did not increase
the mid-run REST inventory of 61 captures; the final observation added five.

The initial `baselineIdlessTriggerFailures` was **zero in both attempts**.
Attempt 2's retained JobManager line says that the source task was not executing;
it occurred after the recorded kill boundary, not before ARM. The nonzero initial
baseline case is covered by unit regressions and was not observed in these two
live attempts. The log scan covers complete hashed retained JobManager prefixes,
not an attestation of the complete process lifetime.

Each attempt retained two producer-fenced component events and zero REST errors.
These events remain visible despite the data PASS. Cleanup receipts confirm the
absence of each attempt's four exact container IDs and its storage volume.
Intervals between recorded UTC start/end timestamps were 243.419 and 260.113
seconds. These are the summary's `durationSeconds`, not the separate monotonic
execution clock. Stdout JSON sizes were 2,780,593
and 2,781,954 bytes; these are observations, not maximum size guarantees.

This is a two-attempt regression check. It does not replace the retained
12-attempt qualification matrix or add a new mutant qualification. Source hashes
prove consistency of retained bytes, not responding-daemon authenticity.
