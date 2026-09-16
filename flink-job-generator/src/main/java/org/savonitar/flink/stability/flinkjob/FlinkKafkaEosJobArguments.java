package org.savonitar.flink.stability.flinkjob;

import java.util.Objects;

/** Job-specific arguments which are intentionally outside the typed workload protocol. */
final class FlinkKafkaEosJobArguments {

    private static final String PROCESSING_DELAY_OPTION = "--processingDelayMs";

    private final int processingDelayMs;

    private FlinkKafkaEosJobArguments(int processingDelayMs) {
        this.processingDelayMs = processingDelayMs;
    }

    static FlinkKafkaEosJobArguments from(String[] args) {
        Objects.requireNonNull(args, "args");

        Integer processingDelayMs = null;
        for (int index = 0; index < args.length; index++) {
            String option = Objects.requireNonNull(args[index], "Program arguments must not be null");
            if (!PROCESSING_DELAY_OPTION.equals(option)) {
                throw new IllegalArgumentException("Unknown program argument: " + option);
            }
            if (processingDelayMs != null) {
                throw new IllegalArgumentException(
                        "Duplicate program argument: " + PROCESSING_DELAY_OPTION);
            }
            if (++index >= args.length) {
                throw new IllegalArgumentException(
                        "Missing value for program argument: " + PROCESSING_DELAY_OPTION);
            }

            String rawValue = Objects.requireNonNull(
                    args[index], "Program argument values must not be null");
            if (!rawValue.matches("0|[1-9][0-9]*")) {
                throw new IllegalArgumentException(
                        PROCESSING_DELAY_OPTION + " must be a nonnegative base-10 integer");
            }
            try {
                processingDelayMs = Integer.parseInt(rawValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        PROCESSING_DELAY_OPTION + " exceeds the supported integer range", e);
            }
        }

        return new FlinkKafkaEosJobArguments(processingDelayMs == null ? 0 : processingDelayMs);
    }

    int processingDelayMs() {
        return processingDelayMs;
    }
}
