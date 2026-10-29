# Checkpoint-storage review verification — 2026-10-29

The required A1/A2 and B1–B9 review changes are implemented. The final JDK 21
reactor installation ran **1,681 tests, zero failures/errors, one skipped**;
the final Python suite passed **487 tests**. The skipped test is the existing
opt-in Docker test. Commands, original log hashes and result identities are in
the [final verification receipt](evidence/checkpoint-review-remediation-20261029-01/final-verification.json).
The first full installation exposed one stale image-identity test fixture; its
mismatch expectation was preserved and the corrected full installation passed.
`git diff --check` is clean, and all 307 checked local documentation links resolve.

All five [CLI validations](evidence/checkpoint-review-remediation-20261029-01/d-cli-final-validations.json)
returned the expected results: baseline and FUSE with either image identity
form exit 0; missing parameters exit 1 with `parameter.required-value-missing`;
`helper_bundle=../x` exits 1 with `runner.checkpoint-storage.configuration-invalid`.
The final helper was the separately retained October 29 build 03, not a replacement
of any October 27 artifact. Build 01's failed strict selftest compilation remains
retained; [build 03](evidence/checkpoint-storage-helper-build-20261029-03/README.md)
completed compilation, native selftests and cleanup.

The [actual packaged JAR comparison](evidence/checkpoint-review-remediation-20261029-01/d-final-packaged-pair.json)
found 18 identical class entries and CRCs. Only `MANIFEST.MF` and
`META-INF/flink-stability/keyed-prefix-build.properties` differ. The correct and
profile-built mutant outputs occupy separate trees. Both actual property-only
mutant builds were [rejected by the guard](evidence/checkpoint-review-remediation-20261029-01/d-property-only-results.json),
including a fresh tree; the installed correct JAR remained unchanged. Operator
harness tests snapshot and restore both arms into fresh operators.

## Fresh live observations

The first three single attempts ran from a [frozen framework and input manifest](evidence/checkpoint-review-live-20261029-01/manifest.json),
each preceded by validation, without retries. Their [finished report](evidence/checkpoint-review-live-20261029-01/finished.json)
confirms the state oracle, complete traversal and cleanup for every attempt.
The final review then removed other successful non-fault operations from individual
native receipts, retained checkpoint-bound reads and added aggregate sync counters. A separate
[final frozen FUSE attempt](evidence/checkpoint-review-final-fuse-20261029-01/manifest.json)
tested that change with helper 03; its [finished report](evidence/checkpoint-review-final-fuse-20261029-01/finished.json)
also confirms all gates. Neither batch was retried or rewritten.

| Attempt | Records | Result | Wall time | stdout JSON | Component events |
| --- | ---: | --- | ---: | ---: | ---: |
| State-prefix baseline | 3,000 | PASS | 41.32 s | 81,386 bytes | 0 |
| Composed TaskManager restart | 6,000 | PASS | 114.05 s | 80,252 bytes | 2 |
| FUSE EIO, replacement and heal — helper 02 | 30,000 | PASS | 279.73 s | 2,749,144 bytes | 2 |
| Final strict-journal FUSE EIO — helper 03 | 30,000 | PASS | 281.70 s | 2,762,931 bytes | 2 |

Every attempt has `stateRecovery.confirmed=true`, zero missing, duplicate,
unexpected or malformed IDs, and zero state mismatches. The restart attempts
retain their producer-fenced component observations; none had a REST error.
Cleanup independently found all exact owned containers absent and the FUSE
backing volume absent. The observed store was classic `overlay2`, with the
config-digest pin matching `CONTAINER_IMAGE_ID` and the index digest recorded in
`RepoDigests`. Containerd identity handling has unit coverage, not a new live
containerd qualification.

In the final FUSE attempt the baseline was checkpoint 1, checkpoint 2 failed, and the replacement
TaskManager restored checkpoint 1. All six stages and final capture were
confirmed. Twenty native EIO receipts were `create` failures; this run does not
extend live coverage to `write` or `fsync` failure. The checker compared 59 source
captures with the typed evidence. The stdout size above includes those source
references and the validator-bound journal subset; raw journals remain separate.
An additional [full-journal check](evidence/checkpoint-review-remediation-20261029-01/d-final-journal-filter-check.json)
found zero successful records outside the required set. Every final instance
used the eight-counter schema. The sync counts in that check receipt describe
the last captured STATUS, not lifetime totals from subsequent SUMMARY records.

The final current checker also [re-audited the final retained attempt](evidence/checkpoint-review-remediation-20261029-01/d-final-current-audit-seed1.json)
without modifying it. Runs with `PYTHONHASHSEED=1` and `137` produced byte-identical
reports, original SHA-256
`88ef39bf50bdc2e52c04d7fa8e576ba231a93ad7d219bbf7d317500abd458c14`.
The frozen runner's CLI projections are explicitly classified as
derived, with `independentProof=false`; mandatory raw proof is checked separately.
The final runtime, helper and recipes remained frozen throughout that attempt.
The current checker also accepted the separately retained helper 02 evidence
under its explicit legacy six-counter representation.

## Scope and reproduction

These are four fresh regression attempts across two frozen batches, **not a new
twelve-run qualification matrix**. The new helper has not had a new live mutant campaign.
The original October 27 matrix and all retained originals remain immutable.
Remaining optional C work is recorded in the
[local follow-up ledger](CHECKPOINT-STORAGE-REVIEW-FOLLOWUPS.md).

Run from the repository root with JDK 21 and explicit public-only Maven settings
when the default mirror is unavailable:

```sh
mvn -B -ntp install
python3 -B -m unittest discover -s tools/tests -v
git diff --check
python3 -B tools/check_doc_links.py
```

Build new helper/workload artifacts and freeze a new directory using the
[qualification recipe](../calibration/checkpoint-storage/README.md) before any
new live execution. The published files are sanitized receipts, with separate
copy and original hashes in the
[publication manifest](evidence/checkpoint-storage-publication-manifest.json).
They omit the raw logs, response bodies, journals and binaries needed to replay
the full audit; source consistency is not an attestation of daemon authenticity
or an independent replay of raw Kafka records.
