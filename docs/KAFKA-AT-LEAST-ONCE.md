# At-least-once chaos coverage

The `at-least-once-*` catalog exercises partition-leader kill and pause, fixed-order
rolling restart, lost Produce responses, timeout errors after successful append,
and a TaskManager restart. Direct and proxy controls retain the corresponding sink
routes. All use 30,000 IDs, three brokers/partitions, replication factor three, and
an explicit `mode: at-least-once` terminal oracle. The producer's connector defaults
are preserved; the protocol filter supports idempotent and non-idempotent batches.

A complete fenced output passes only with no missing, unexpected or malformed IDs.
Duplicates are permitted and retained as counts and samples. This does not claim
that every fault must produce duplicates. A fault still needs its own physical or
protocol confirmation, independent of the data oracle. These new workloads have
been validated offline and need live calibration; no existing negative control or
expectation has changed.
