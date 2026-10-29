# Published checkpoint and state-recovery evidence

The linked copies make selected historical receipts available in a fresh clone.
The original `jobs/` directories remain local and immutable. Each published receipt
is a sanitized copy or a compact projection, not the original raw proof.

[The publication manifest](checkpoint-storage-publication-manifest.json) maps every
copy to its original source name, original SHA-256, sanitized-copy SHA-256, byte sizes
and exact omissions. Hashes printed in the implementation and qualification guides
continue to identify the original receipts, journals and binaries. They must not be
used to verify the sanitized copies.

Host workspace, home and temporary paths use `<workspace>`, `<host-home>` and
`<host-temporary>` placeholders; the macOS Docker executable is
`<host-docker-cli>`. Container paths are retained. Compact results keep
verdicts, failure/diagnostic details and component events, but omit raw native
operation arrays and repetitive command/state-marker transcripts. Verification logs
are represented by numbered summary lines with their source filename and hash.
These files support inspection of the reported observations; they are insufficient
for independently rerunning the full raw-evidence checker.

The publication contains 56 selected receipts from 21 batches. All 50 previously
linked local targets were available (49 files and one directory); no missing
receipt was substituted or presented as observed. Original and published hashes
were checked for every copy, and 90 result/report objects were checked for unchanged
verdicts and reported error/event counts. Detailed mismatch examples are bounded
to the first three where the original contains more; total mismatch counts remain.
Long failure text is explicitly excerpted with its original length and text hash.
Framework source/class inventory maps are summarized; the original manifest hash
still identifies the complete inventory. These checks are not a secret-scanner run.

After staging the intended files, check local documentation links with:

```sh
python3 -B tools/check_doc_links.py
python3 -B -m unittest discover -s tools/tests -p 'test_check_doc_links.py' -v
```

The checker makes no network requests. It checks tracked Markdown under `docs/`,
including inline/reference/image/HTML links and Markdown heading fragments.
Targets must exist inside the repository and be tracked; an ignored local
`jobs/` file cannot satisfy a link. Symbolic-link targets are rejected.

- [checkpoint-storage-checks-20261027-01](checkpoint-storage-checks-20261027-01/README.md)
- [checkpoint-storage-direct-java-pilot-20261027-01](checkpoint-storage-direct-java-pilot-20261027-01/README.md)
- [checkpoint-storage-direct-java-pilot-20261027-02](checkpoint-storage-direct-java-pilot-20261027-02/README.md)
- [checkpoint-storage-helper-build-20261027-01](checkpoint-storage-helper-build-20261027-01/README.md)
- [checkpoint-storage-helper-build-20261027-02](checkpoint-storage-helper-build-20261027-02/README.md)
- [checkpoint-storage-helper-build-20261027-03](checkpoint-storage-helper-build-20261027-03/README.md)
- [checkpoint-storage-helper-build-20261027-04](checkpoint-storage-helper-build-20261027-04/README.md)
- [checkpoint-storage-integration-checks-20261027-01](checkpoint-storage-integration-checks-20261027-01/README.md)
- [checkpoint-storage-integration-pilots-20261027-01](checkpoint-storage-integration-pilots-20261027-01/README.md)
- [checkpoint-storage-integration-pilots-20261027-02](checkpoint-storage-integration-pilots-20261027-02/README.md)
- [checkpoint-storage-probe-20261027-01](checkpoint-storage-probe-20261027-01/README.md)
- [checkpoint-storage-probe-20261027-02](checkpoint-storage-probe-20261027-02/README.md)
- [checkpoint-storage-probe-pilot-20261027-01](checkpoint-storage-probe-pilot-20261027-01/README.md)
- [checkpoint-storage-probe-pilot-20261027-02](checkpoint-storage-probe-pilot-20261027-02/README.md)
- [checkpoint-storage-qualification-20261027-01](checkpoint-storage-qualification-20261027-01/README.md)
- [checkpoint-storage-qualification-pilots-20261027-01](checkpoint-storage-qualification-pilots-20261027-01/README.md)
- [checkpoint-storage-qualification-preparation-20261027-03](checkpoint-storage-qualification-preparation-20261027-03/README.md)
- [checkpoint-storage-qualification-review-20261027-01](checkpoint-storage-qualification-review-20261027-01/README.md)
- [checkpoint-storage-review-fixes-20261027-01](checkpoint-storage-review-fixes-20261027-01/README.md)
- [state-recovery-build-20261026-02](state-recovery-build-20261026-02/README.md)
- [state-recovery-qualification-20261026-01](state-recovery-qualification-20261026-01/README.md)

The previously published [Kafka lifecycle receipts](kafka-lifecycle/2026-10-11/README.md) have their own provenance and are unchanged.
