# Parallel Kafka chaos coverage

The `parallel-*` catalog adds parallelism 4 on two TaskManagers, with four input and
four output partitions. Both transaction versions are covered. POOLING recovery is
exercised with delay, error-response and lost-response faults on DescribeProducers
and ListTransactions, both with all brokers present and with broker-1 down, and with a lost EndTxn commit response. The remaining variants
cover partition-leader kill, coordinator kill during commit, and coordinator-first
rolling restart. Separate POOLING and rolling controls use the same topology.

Input counts are four times their single-subtask parents, retaining approximately
the same per-subtask processing window. Recovery targets `taskmanager-1` explicitly;
the surviving TaskManager can retain other subtasks. The exact-ID oracle reads all
output partitions. Coordinator selection deterministically chooses one currently
open sink transaction and retains its identity in evidence; it does not claim every
subtask's coordinator was faulted.

These are new, uncalibrated scenarios in `chaos-full`; no existing scenario,
expectation or `chaos-quick` member is changed. Live runs must still confirm physical
fault effects and recovery before these variants establish coverage.
