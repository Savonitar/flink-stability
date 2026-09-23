package org.savonitar.flink.stability.core.validation.kafka;

import java.util.List;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/** Structured terminal result; inability to verify is a distinct experiment failure reason. */
public record KafkaIdSetValidationResult(
        Status status,
        String reason,
        String message,
        Evidence evidence) {

    public KafkaIdSetValidationResult {
        if (status == null || reason == null || reason.isBlank()
                || message == null || evidence == null) {
            throw new IllegalArgumentException("Validation result fields must be present");
        }
    }

    public enum Status {
        PASS,
        FAIL
    }

    public record Evidence(
            long expectedCount,
            long observedCount,
            Optional<DefectTotals> defectTotals,
            List<RecordSample> observedSamples,
            List<RecordSample> malformedSamples,
            List<RecordSample> unexpectedSamples,
            List<RecordSample> duplicateSamples,
            List<Long> missingSamples,
            Map<Integer, Long> beginningOffsets,
            Map<Integer, Long> endOffsets,
            boolean snapshotComplete) {
        public Evidence {
            if (expectedCount < 1 || observedCount < 0) {
                throw new IllegalArgumentException(
                        "Expected count must be positive and observed count non-negative");
            }
            defectTotals = Objects.requireNonNull(defectTotals, "defectTotals");
            observedSamples = List.copyOf(observedSamples);
            malformedSamples = List.copyOf(malformedSamples);
            unexpectedSamples = List.copyOf(unexpectedSamples);
            duplicateSamples = List.copyOf(duplicateSamples);
            missingSamples = List.copyOf(missingSamples);
            beginningOffsets = Collections.unmodifiableMap(
                    new LinkedHashMap<>(new TreeMap<>(beginningOffsets)));
            endOffsets = Collections.unmodifiableMap(
                    new LinkedHashMap<>(new TreeMap<>(endOffsets)));
            if (snapshotComplete != defectTotals.isPresent()) {
                throw new IllegalArgumentException(
                        "Exact defect totals exist if and only if the snapshot is complete");
            }
            if (!snapshotComplete && !missingSamples.isEmpty()) {
                throw new IllegalArgumentException(
                        "An incomplete snapshot cannot prove missing-ID samples");
            }
            defectTotals.ifPresent(totals -> {
                long classifiedExpected = Math.addExact(
                        totals.distinctExpectedCount(), totals.missingCount());
                if (classifiedExpected != expectedCount) {
                    throw new IllegalArgumentException(
                            "Distinct and missing totals must cover the expected ID set");
                }
                long classifiedObserved = Math.addExact(
                        Math.addExact(
                                totals.distinctExpectedCount(), totals.malformedCount()),
                        Math.addExact(totals.unexpectedCount(), totals.duplicateCount()));
                if (classifiedObserved != observedCount) {
                    throw new IllegalArgumentException(
                            "Exact defect totals must classify every observed record");
                }
            });
        }

        public static Evidence unavailable(long expectedCount) {
            return new Evidence(
                    expectedCount, 0, Optional.empty(),
                    List.of(), List.of(), List.of(), List.of(), List.of(),
                    Map.of(), Map.of(), false);
        }
    }

    /** Exact totals available only after the complete fixed Kafka range was decoded. */
    public record DefectTotals(
            long distinctExpectedCount,
            long malformedCount,
            long unexpectedCount,
            long duplicateCount,
            long missingCount) {
        public DefectTotals {
            if (distinctExpectedCount < 0
                    || malformedCount < 0
                    || unexpectedCount < 0
                    || duplicateCount < 0
                    || missingCount < 0) {
                throw new IllegalArgumentException("Defect totals must not be negative");
            }
        }
    }

    /** Deterministic anomaly evidence retaining the Kafka record coordinate and raw value. */
    public record RecordSample(int partition, long offset, String rawValue) {
        public RecordSample {
            if (partition < 0 || offset < 0) {
                throw new IllegalArgumentException(
                        "Record sample partition and offset must not be negative");
            }
        }
    }
}
