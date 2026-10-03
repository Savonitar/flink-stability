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
        Evidence evidence,
        Mode mode) {

    public KafkaIdSetValidationResult {
        if (status == null || reason == null || reason.isBlank()
                || message == null || evidence == null || mode == null) {
            throw new IllegalArgumentException("Validation result fields must be present");
        }
    }

    public KafkaIdSetValidationResult(Status status, String reason, String message, Evidence evidence) {
        this(status, reason, message, evidence, Mode.EXACTLY_ONCE);
    }

    public enum Mode { EXACTLY_ONCE, AT_LEAST_ONCE }

    /** Interpret only a completed ID-set comparison; verification failures remain failures. */
    public KafkaIdSetValidationResult forMode(Mode requested) {
        Objects.requireNonNull(requested, "mode");
        if (requested == mode) return this;
        if (!evidence.snapshotComplete() || !reason.startsWith("validator.kafka.id-set."))
            return new KafkaIdSetValidationResult(status, reason, message, evidence, requested);
        var totals = evidence.defectTotals().orElseThrow();
        String defect = totals.malformedCount() > 0 ? "malformed" : totals.unexpectedCount() > 0 ? "unexpected"
                : requested == Mode.EXACTLY_ONCE && totals.duplicateCount() > 0 ? "duplicate"
                : totals.missingCount() > 0 ? "missing" : null;
        if (defect != null) return new KafkaIdSetValidationResult(Status.FAIL,
                "validator.kafka.id-set." + defect + "-ids", "Kafka output contains " + defect + " IDs", evidence, requested);
        return new KafkaIdSetValidationResult(Status.PASS,
                requested == Mode.AT_LEAST_ONCE ? "validator.kafka.id-set.at-least-once-match" : "validator.kafka.id-set.match",
                requested == Mode.AT_LEAST_ONCE ? "All expected IDs are present; duplicates are permitted and counted"
                        : "Kafka output exactly matches the input manifest", evidence, requested);
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
