package org.savonitar.flink.stability.flinkjob;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Job-specific arguments which are intentionally outside the typed workload protocol. */
final class FlinkKafkaEosJobArguments {

    private static final String PROCESSING_DELAY_OPTION = "--processingDelayMs";
    private static final String PRODUCER_MAX_BLOCK_OPTION = "--producerMaxBlockMs";

    private static final String PRODUCER_REQUEST_TIMEOUT_OPTION = "--producerRequestTimeoutMs";
    private static final String SNAPSHOT_ASYNC_DELAY_OPTION = "--snapshotAsyncDelayMs";

    private final int processingDelayMs;
    private final Integer producerMaxBlockMs;
    private final Integer producerRequestTimeoutMs;
    private final int snapshotAsyncDelayMs;

    private FlinkKafkaEosJobArguments(int processingDelayMs, Integer producerMaxBlockMs, Integer producerRequestTimeoutMs, int snapshotAsyncDelayMs) {
        this.processingDelayMs = processingDelayMs;
        this.producerMaxBlockMs = producerMaxBlockMs;
        this.producerRequestTimeoutMs = producerRequestTimeoutMs;
        this.snapshotAsyncDelayMs = snapshotAsyncDelayMs;
    }

    static FlinkKafkaEosJobArguments from(String[] args) {
        Objects.requireNonNull(args, "args");
        Map<String, Integer> values = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            String option = Objects.requireNonNull(args[index], "Program arguments must not be null");
            if (!PROCESSING_DELAY_OPTION.equals(option) && !PRODUCER_MAX_BLOCK_OPTION.equals(option) && !PRODUCER_REQUEST_TIMEOUT_OPTION.equals(option) && !SNAPSHOT_ASYNC_DELAY_OPTION.equals(option)) {
                throw new IllegalArgumentException("Unknown program argument: " + option);
            }
            if (values.containsKey(option)) {
                throw new IllegalArgumentException("Duplicate program argument: " + option);
            }
            if (++index >= args.length) {
                throw new IllegalArgumentException("Missing value for program argument: " + option);
            }
            String raw = Objects.requireNonNull(args[index], "Program argument values must not be null");
            if (!raw.matches("0|[1-9][0-9]*")) {
                throw new IllegalArgumentException(option + " must be a nonnegative base-10 integer");
            }
            try {
                int value = Integer.parseInt(raw);
                if (!PROCESSING_DELAY_OPTION.equals(option) && !SNAPSHOT_ASYNC_DELAY_OPTION.equals(option) && value == 0) {
                    throw new IllegalArgumentException(option + " must be positive");
                }
                values.put(option, value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(option + " exceeds the supported integer range", e);
            }
        }
        return new FlinkKafkaEosJobArguments(values.getOrDefault(PROCESSING_DELAY_OPTION, 0),
                values.get(PRODUCER_MAX_BLOCK_OPTION), values.get(PRODUCER_REQUEST_TIMEOUT_OPTION),
                values.getOrDefault(SNAPSHOT_ASYNC_DELAY_OPTION, 0));
    }

    int processingDelayMs() {
        return processingDelayMs;
    }

    int snapshotAsyncDelayMs() { return snapshotAsyncDelayMs; }

    Integer producerRequestTimeoutMs() { return producerRequestTimeoutMs; }

    Integer producerMaxBlockMs() {
        return producerMaxBlockMs;
    }
}
