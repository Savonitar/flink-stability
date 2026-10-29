# Native checkpoint filesystem helper

The daemon keeps every mutation, fault-family operation (`create`, `write`,
`truncate`, `open-trunc`), error and control/lifecycle receipt. Successful reads
of files below a canonical `chk-<positive integer>/` component also remain
individual receipts, including the metadata inspector and restored state reads.

Successful `read`, `getattr`, `readdir`, `access`, `flush` and `release` calls
increment cumulative counters. Each counter has `count` and `bytes`; only the
read counter has nonzero bytes. Checkpoint reads count once even though they also
have individual receipts. STATUS replies and control/lifecycle records include
the counters. SUMMARY records emit after 1,000 counted operations or one second
of pending activity; idle time and STATUS polling produce no journal entries.

Journal writes use `fdatasync` for the create/write fault family and control or
lifecycle acknowledgements. Other successful filesystem callbacks and SUMMARY
records append without a journal sync on every callback. Filesystem `fsync` and
`fdatasync` requests still perform the requested operation on the backing file.

The adapter sets a per-instance limit of 16 MiB and 25,000 records. Standalone
daemon defaults are 8 MiB and 10,000 records; accepted limits are 64 KiB–64 MiB
and 10–100,000 records. The final slot and receipt-sized byte reserve record
budget exhaustion as FATAL with `journalComplete=false`. Reads and metadata
queries can continue; mutations fail with EIO. Such evidence cannot qualify a
run. STATUS remains available without consuming the exhausted record budget.
These are evidence-volume bounds, not a guaranteed experiment duration. An
armed fault lease is independently limited to 120 seconds.

A failed socket response aborts the daemon only after an accepted ARM, HEAL,
ABORT or STOP changed its state. A disconnected STATUS client or an invalid,
incomplete or rejected command cannot abort the mount. Rejections still have an
individual journal receipt. STOP wakes the FUSE main loop; that main thread owns
unmount and destruction. Partial startup failures close owned descriptors and
remove only a control socket successfully bound by this process.

Overlong paths return ENAMETOOLONG and record the bounded prefix with
`pathTruncated=true`; this path error does not mark the whole journal incomplete.
The parser preserves counters and the truncation flag, rejects malformed or
regressing counters, and continues to read historical receipts without counters.

`selftest.c` exercises the state machine and path jail without a FUSE mount.
Its fixed-clock aggregation case performs 100,000 successful metadata callbacks
with 100 SUMMARY entries plus the two startup entries. Live mount behavior and
the daemon binary are checked separately in a fresh pinned-image helper build.

The builder's `signedIndexSha256` records the downloaded index as provenance;
it is not a signature-verification claim. The locked package hashes and sizes
are verified before network disconnection and installation.
