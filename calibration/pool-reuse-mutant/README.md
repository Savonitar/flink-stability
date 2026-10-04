# Same-ID producer reuse calibration

This recipe deliberately skips both `setTransactionId` and `initTransactions` when
the real pool returns a recycled producer already carrying the requested non-null
transactional ID. The writer's unchanged `beginTransaction` therefore uses the
existing session. Different IDs, new producers, checkpoint bookkeeping and recycle
failure handling retain the release behavior.

Run `build.py` with `--java-home`, `--maven`, `--maven-repo` and `--maven-settings`.
JDK 21 and an explicitly selected existing Maven cache/settings are required. The
build is offline, rejects symlink cache/settings paths, isolates Java home/temp,
and refuses to overwrite `target`. Cache and settings paths may be explicitly
supplied from inside or outside the checkout.
Default user settings are never read implicitly. It verifies the release source SHA-256
`c05dcdc7d7256575f10f784c728f983b517058daee517ae258021edd1e87ecfe`
and binary SHA-256
`6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da`.

Only `ProducerPoolImpl.class` is replaced. The compiler also emits the unchanged
nested `ProducerEntry` implementation; the overlay retains the original nested
class byte-for-byte, as it does every other archive entry, LICENSE and NOTICE.
`target/build-evidence.json` records artifact, source, recipe and dependency hashes.
`DecisionCheck.java` invokes the actual pool with a recording producer allocated
without a Kafka constructor. Both release and mutant are checked without a broker.
This establishes the mutation, not live calibration.

Live qualification requires release PASS on both transaction-version fault cells
and their no-kill controls, and passing mutant no-kill controls. Run each fault
cell at least three times with no health retries. Keep the harness default
transaction timeout of 7,200,000 ms (two hours) for both release and mutant; it
overrides the released connector's one-hour default. Require the TV1 mutant to
fail with duplicate IDs in one contiguous block,
confirmed kill-window evidence and exactly one implicated transactional ID. Retain
and classify TV2 fencing and unresolved transactions separately from the data
verdict. No expected outcome is changed to make this negative calibration pass.

## Calibration contract

The checker applies the following recipe-specific qualification rules.
`qualified: true` and a zero exit code mean that the supplied evidence satisfies
these rules. They do not establish acceptance against an external specification
or independent validation of the contract. The 3,000 ms margin and 200–900-record
range are calibration parameters chosen for this workload, not general Flink or
Kafka correctness limits. The retained batch does not independently validate
those threshold choices.

1. Verify the pinned release/source, the one-class mutation, identical runtime
   dependency closures and actual connector class origins on every TaskManager.
2. Require passing release/mutant no-kill controls for both transaction versions
   and at least three confirmed repetitions per subject/version fault cell.
   Retain every original attempt and any documented timing supplement.
3. Require k-1 completed and its transaction committed before k triggered, sink
   snapshot acknowledgement before kill, at least 3,000 ms of asynchronous work
   remaining, 200–900 contiguous reused records, restore of k-1 before transaction
   expiry, and the unchanged 7,200,000 ms harness transaction timeout. Window misses
   do not count as confirmed repetitions, even if their data oracle passes.
4. Release faults must pass and abort the reused record block. TV1 mutant faults
   must fail specifically with duplicate IDs, no other data defects, and exactly
   one contiguous duplicate block equal to the reused block on exactly one ID.
5. That ID must retain the same producer ID and epoch across its earlier
   `CompleteCommit`, new `Ongoing` transaction after k, stale resume after the kill,
   and second `CompleteCommit` closing that reused block with a COMMIT marker.
6. TV2 mutant faults must demonstrate the fenced stale resume; record data outcome
   and any unresolved/hanging transaction separately. Component errors remain findings
   even when data passes. Missing required evidence or a failed control cannot
   produce a successful calibration.

The retained evidence has three confirmed fault repetitions per subject
and transaction version, plus all four passing controls. TV1 mutant failures have
contiguous duplicate blocks of 608, 688 and 438 IDs. TV2 stale resumes were fenced;
no unresolved transactions were observed. Two TV2 attempts missed the required
three-second margin and remain inconclusive; an additional pair used a shorter
post-trigger wait. See [the calibration report](../../docs/POOL-REUSE-CALIBRATION.md)
for the result matrix, timings and limitations. Raw evidence is retained locally
and is not distributed with this repository.

Audit the retained runs without Docker:

```bash
calibration_evidence=jobs/validation/my-pool-reuse-batch
calibration_audit=jobs/validation/my-pool-reuse-audit
mkdir -p "$calibration_audit"
python3 calibration/pool-reuse-mutant/check.py \
  --controls "$calibration_evidence/controls" \
  --faults "$calibration_evidence/faults" \
  --supplement "$calibration_evidence/tv2-retuned" \
  --build-evidence "$calibration_evidence/build-evidence.json" \
  --output "$calibration_audit/calibration-report.json"
```

Set `calibration_evidence` to an existing batch directory in the checkout and
`calibration_audit` to a separate output directory. Omit `--supplement` if there
is no supplemental TV2 pair. The checker audits existing evidence without
launching Docker and refuses to overwrite an existing output file. Preserve the
original reports, including missed windows and failed attempts. Use a separate
Maven cache for subsequent harness builds so recorded SNAPSHOT artifacts remain
available for reproduction.
