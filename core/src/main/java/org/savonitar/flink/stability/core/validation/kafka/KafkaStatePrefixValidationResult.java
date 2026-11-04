package org.savonitar.flink.stability.core.validation.kafka;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** ID integrity and independently calculated keyed-state values from the same Kafka snapshot. */
public record KafkaStatePrefixValidationResult(
        KafkaIdSetValidationResult.Status status,
        String reason,
        String message,
        KafkaIdSetValidationResult.Evidence evidence,
        StateEvidence stateEvidence) implements KafkaTerminalValidationResult {
    public KafkaStatePrefixValidationResult {
        Objects.requireNonNull(status, "status");
        if (reason == null || reason.isBlank()) throw new IllegalArgumentException("reason is required");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(stateEvidence, "stateEvidence");
        if (evidence.snapshotComplete() != stateEvidence.mismatchCount().isPresent())
            throw new IllegalArgumentException("Exact state totals require complete verification");
        if (status == KafkaIdSetValidationResult.Status.PASS) {
            if (!evidence.snapshotComplete() || stateEvidence.mismatchCount().orElseThrow() != 0)
                throw new IllegalArgumentException("PASS requires complete verification without state mismatches");
            var totals = evidence.defectTotals().orElseThrow();
            if (totals.malformedCount() != 0 || totals.unexpectedCount() != 0
                    || totals.duplicateCount() != 0 || totals.missingCount() != 0)
                throw new IllegalArgumentException("PASS requires exact ID integrity");
        }
    }

    @Override public KafkaIdSetValidationResult.Mode mode() { return KafkaIdSetValidationResult.Mode.EXACTLY_ONCE; }
    @Override public String validatorType() { return "kafka.state-prefix"; }

    /** Exact totals are absent on incomplete traversal or comparison; samples remain observations. */
    public record StateEvidence(int keyCount, OptionalLong mismatchCount, List<StateMismatchSample> mismatchSamples) {
        public StateEvidence {
            if (keyCount < 1 || keyCount > 1024) throw new IllegalArgumentException("keyCount must be in 1..1024");
            Objects.requireNonNull(mismatchCount, "mismatchCount");
            mismatchSamples = List.copyOf(mismatchSamples);
            if (mismatchSamples.size() > 100 || mismatchCount.isPresent()
                    && (mismatchCount.getAsLong() < 0 || mismatchCount.getAsLong() < mismatchSamples.size()))
                throw new IllegalArgumentException("Invalid bounded state evidence");
        }
    }

    public record StateMismatchSample(int partition, long offset, long id,
            long expectedKey, long actualKey, long expectedCount, long actualCount,
            long expectedSum, long actualSum) {
        public StateMismatchSample {
            if (partition < 0 || offset < 0 || id < 0) throw new IllegalArgumentException("Invalid record coordinate");
        }
    }
}
