# Checkpoint and state-recovery evidence

| Date | Final report | Observed outcome |
| --- | --- | --- |
| October 26 | [State-recovery matrix](state-recovery-qualification-20261026-01/final-report.summary.json) | 12 attempts, nine raw PASS and three reset-on-restore mutant FAIL results with every input ID present. |
| October 27 | [Standalone FUSE probe](checkpoint-storage-probe-20261027-02/result.summary.json) | Seven qualified probe cases on Docker Desktop `linux/arm64`, including a failed write through an open descriptor. |
| October 27 | [Checkpoint-storage matrix](checkpoint-storage-qualification-20261027-01/reports/final/qualification.md) · [JSON](checkpoint-storage-qualification-20261027-01/reports/final/qualification.summary.json) | 12 attempts, nine raw PASS and three state-mismatch FAIL results. All 30,000 IDs were present per attempt. All 120 native EIOs observed in this matrix occurred on create. |
| October 29 | [Final verification](checkpoint-review-remediation-20261029-01/final-verification.json) | Correct workload recovered in the final strict-journal FUSE attempt. The report distinguishes this attempt from earlier regression runs. |
| October 30 | [N1–N5 verification](checkpoint-review-n1-n5-20261030-01/verification.json) | Two correct-arm EIO/replacement regression attempts passed with confirmed restoration and cleanup. |

Both matrices describe the frozen workloads, tools and verifier used for those runs.
The October 29 and 30 checks cover their respective helper and framework revisions.
The [publication manifest](checkpoint-storage-publication-manifest.json) records
original and published SHA-256 values and byte counts. The October 30 summary
combines 15 source receipts and records a hash for each. The
[Kafka lifecycle receipts](kafka-lifecycle/2026-10-11/README.md) cover a separate experiment.

## Historical review archive

[Download the sanitized evidence archive](https://github.com/Savonitar/flink-stability/releases/download/checkpoint-storage-review-archive-20261030/checkpoint-storage-review-evidence-20261030.tar.gz)
or [browse its files at the release tag](https://github.com/Savonitar/flink-stability/tree/checkpoint-storage-review-archive-20261030/docs/evidence).
The archive contains the original published copies of all 74 receipts and
summaries, their directory indexes and the full publication manifest. These
include pilot failures, build and cleanup receipts, and subsequent re-audits.
Each tagged file has the same repository-relative path inside the archive.
The five pre-existing Kafka lifecycle files are available separately above.

| Archive | Integrity |
| --- | --- |
| `checkpoint-storage-review-evidence-20261030.tar.gz` | 320,057 bytes, SHA-256 `e831b0454b26f4003300a640a6809bb544b1b3c9c4fa45f0e385fdd893d9ab22` |
| Release tag `checkpoint-storage-review-archive-20261030` | Commit `c92aa34ff403fdc17bd75dbdf3afd89984ec7cfc` |
| Member inventory | `ARCHIVE-INVENTORY.json` lists the path, byte size and SHA-256 of all 103 historical files. |
| Original publication manifest | `docs/evidence/checkpoint-storage-publication-manifest.json` records the published hash, original receipt identity and fields omitted during publication. |

After downloading the archive, verify its digest and inspect any receipt by member path:

```sh
shasum -a 256 checkpoint-storage-review-evidence-20261030.tar.gz
tar -xOzf checkpoint-storage-review-evidence-20261030.tar.gz ARCHIVE-INVENTORY.json
tar -xOzf checkpoint-storage-review-evidence-20261030.tar.gz docs/evidence/checkpoint-storage-review-fixes-20261027-01/re-audit-final.md
```

The detailed summaries retain verdicts, state mismatch counts, errors and component
events. Publication replaced host paths with placeholders and bounded mismatch
examples and long failure text. Full raw journals, response bodies, transcripts,
binaries and original logs stay in local `jobs/`. Replaying the source-response
audit requires those raw files.

For documentation changes, run `python3 -B tools/check_doc_links.py` after staging
the intended paths. The checker verifies tracked relative links locally.
