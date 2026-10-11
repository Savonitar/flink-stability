package org.savonitar.flink.stability.flinkjob;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;

/** Job-specific arguments which are intentionally outside the typed workload protocol. */
final class FlinkKafkaEosJobArguments {

    private static final String PROCESSING_DELAY_OPTION = "--processingDelayMs";
    private static final String PRODUCER_MAX_BLOCK_OPTION = "--producerMaxBlockMs";

    private static final String PRODUCER_REQUEST_TIMEOUT_OPTION = "--producerRequestTimeoutMs";
    private static final String SNAPSHOT_ASYNC_DELAY_OPTION = "--snapshotAsyncDelayMs";
    private static final String FAIL_ASYNC_SNAPSHOTS_OPTION = "--failAsyncSnapshotForCheckpoints";
    private static final String FAIL_AFTER_SNAPSHOT_OPTION = "--failAfterSnapshotOfCheckpoint";
    private static final String FAIL_AFTER_SNAPSHOT_DELAY_OPTION = "--failAfterSnapshotDelayMs";

    private final int processingDelayMs;
    private final Integer producerMaxBlockMs;
    private final Integer producerRequestTimeoutMs;
    private final int snapshotAsyncDelayMs;
    private final Set<Long> failAsyncSnapshotForCheckpoints;
    private final Long failAfterSnapshotOfCheckpoint;
    private final int failAfterSnapshotDelayMs;
    private final boolean snapshotHooksEnabled;

    private FlinkKafkaEosJobArguments(int processingDelayMs, Integer producerMaxBlockMs,
            Integer producerRequestTimeoutMs, int snapshotAsyncDelayMs,
            Set<Long> failAsyncSnapshotForCheckpoints, Long failAfterSnapshotOfCheckpoint,
            int failAfterSnapshotDelayMs, boolean snapshotHooksEnabled) {
        this.processingDelayMs = processingDelayMs;
        this.producerMaxBlockMs = producerMaxBlockMs;
        this.producerRequestTimeoutMs = producerRequestTimeoutMs;
        this.snapshotAsyncDelayMs = snapshotAsyncDelayMs;
        this.failAsyncSnapshotForCheckpoints = Set.copyOf(failAsyncSnapshotForCheckpoints);
        this.failAfterSnapshotOfCheckpoint = failAfterSnapshotOfCheckpoint;
        this.failAfterSnapshotDelayMs = failAfterSnapshotDelayMs;
        this.snapshotHooksEnabled = snapshotHooksEnabled;
    }

    static FlinkKafkaEosJobArguments from(String[] args) {
        Objects.requireNonNull(args, "args");
        Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            String option = Objects.requireNonNull(args[index], "Program arguments must not be null");
            if (!PROCESSING_DELAY_OPTION.equals(option) && !PRODUCER_MAX_BLOCK_OPTION.equals(option)
                    && !PRODUCER_REQUEST_TIMEOUT_OPTION.equals(option) && !SNAPSHOT_ASYNC_DELAY_OPTION.equals(option)
                    && !FAIL_ASYNC_SNAPSHOTS_OPTION.equals(option) && !FAIL_AFTER_SNAPSHOT_OPTION.equals(option)
                    && !FAIL_AFTER_SNAPSHOT_DELAY_OPTION.equals(option)) {
                throw new IllegalArgumentException("Unknown program argument: " + option);
            }
            if (values.containsKey(option)) {
                throw new IllegalArgumentException("Duplicate program argument: " + option);
            }
            if (++index >= args.length) {
                throw new IllegalArgumentException("Missing value for program argument: " + option);
            }
            String raw = Objects.requireNonNull(args[index], "Program argument values must not be null");
            values.put(option, raw);
        }
        Set<Long> failedSnapshots = new HashSet<>();
        if (values.containsKey(FAIL_ASYNC_SNAPSHOTS_OPTION)) {
            for (String id : values.get(FAIL_ASYNC_SNAPSHOTS_OPTION).split(",", -1)) {
                long checkpointId = checkpointId(FAIL_ASYNC_SNAPSHOTS_OPTION, id);
                if (!failedSnapshots.add(checkpointId)) {
                    throw new IllegalArgumentException(FAIL_ASYNC_SNAPSHOTS_OPTION + " contains duplicate checkpoint id: " + id);
                }
            }
        }
        int asyncDelay = integer(SNAPSHOT_ASYNC_DELAY_OPTION, values.getOrDefault(SNAPSHOT_ASYNC_DELAY_OPTION, "0"), false);
        return new FlinkKafkaEosJobArguments(
                integer(PROCESSING_DELAY_OPTION, values.getOrDefault(PROCESSING_DELAY_OPTION, "0"), false),
                integer(PRODUCER_MAX_BLOCK_OPTION, values.get(PRODUCER_MAX_BLOCK_OPTION), true),
                integer(PRODUCER_REQUEST_TIMEOUT_OPTION, values.get(PRODUCER_REQUEST_TIMEOUT_OPTION), true),
                asyncDelay, failedSnapshots,
                values.containsKey(FAIL_AFTER_SNAPSHOT_OPTION)
                        ? checkpointId(FAIL_AFTER_SNAPSHOT_OPTION, values.get(FAIL_AFTER_SNAPSHOT_OPTION)) : null,
                integer(FAIL_AFTER_SNAPSHOT_DELAY_OPTION, values.getOrDefault(FAIL_AFTER_SNAPSHOT_DELAY_OPTION, "2000"), false),
                asyncDelay > 0 || values.containsKey(FAIL_ASYNC_SNAPSHOTS_OPTION)
                        || values.containsKey(FAIL_AFTER_SNAPSHOT_OPTION) || values.containsKey(FAIL_AFTER_SNAPSHOT_DELAY_OPTION));
    }

    private static Integer integer(String option, String raw, boolean positive) {
        if (raw == null) return null;
        if (!raw.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException(option + " must be a nonnegative base-10 integer");
        }
        try {
            int value = Integer.parseInt(raw);
            if (positive && value == 0) throw new IllegalArgumentException(option + " must be positive");
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + " exceeds the supported integer range", e);
        }
    }

    private static long checkpointId(String option, String raw) {
        if (!raw.matches("[1-9][0-9]*")) {
            throw new IllegalArgumentException(option + " requires positive base-10 checkpoint ids");
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + " exceeds the supported checkpoint id range", e);
        }
    }

    int processingDelayMs() {
        return processingDelayMs;
    }

    int snapshotAsyncDelayMs() { return snapshotAsyncDelayMs; }

    Set<Long> failAsyncSnapshotForCheckpoints() { return failAsyncSnapshotForCheckpoints; }

    Long failAfterSnapshotOfCheckpoint() { return failAfterSnapshotOfCheckpoint; }

    int failAfterSnapshotDelayMs() { return failAfterSnapshotDelayMs; }

    boolean snapshotHooksEnabled() { return snapshotHooksEnabled; }

    Integer producerRequestTimeoutMs() { return producerRequestTimeoutMs; }

    Integer producerMaxBlockMs() {
        return producerMaxBlockMs;
    }
}
