package org.savonitar.flink.stability.core.validation.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Typed inability to obtain a complete, fixed Kafka snapshot before the deadline. */
public final class KafkaSnapshotException extends Exception {
    private final String reason;
    private final PartialEvidence partialEvidence;

    public KafkaSnapshotException(String reason, String message) {
        this(reason, message, null, PartialEvidence.empty());
    }

    public KafkaSnapshotException(String reason, String message, Throwable cause) {
        this(reason, message, cause, PartialEvidence.empty());
    }

    public KafkaSnapshotException(
            String reason,
            String message,
            Throwable cause,
            PartialEvidence partialEvidence) {
        super(message, cause);
        this.reason = requireReason(reason);
        this.partialEvidence = java.util.Objects.requireNonNull(
                partialEvidence, "partialEvidence");
    }

    public String reason() {
        return reason;
    }

    public PartialEvidence partialEvidence() {
        return partialEvidence;
    }

    /** Best-effort immutable progress captured before a terminal snapshot failure. */
    public record PartialEvidence(
            Map<Integer, Long> beginningOffsets,
            Map<Integer, Long> endOffsets,
            long observedCount,
            List<KafkaTopicSnapshot.ObservedRecord> observedSamples) {
        public PartialEvidence {
            beginningOffsets = immutableOffsets(beginningOffsets);
            endOffsets = immutableOffsets(endOffsets);
            if (observedCount < 0) {
                throw new IllegalArgumentException("observedCount must not be negative");
            }
            observedSamples = List.copyOf(observedSamples);
            if (observedSamples.size() > observedCount) {
                throw new IllegalArgumentException(
                        "observedSamples must not exceed observedCount");
            }
        }

        public static PartialEvidence empty() {
            return new PartialEvidence(Map.of(), Map.of(), 0, List.of());
        }

        private static Map<Integer, Long> immutableOffsets(Map<Integer, Long> source) {
            TreeMap<Integer, Long> sorted = new TreeMap<>();
            source.forEach((partition, offset) -> {
                if (partition == null || partition < 0 || offset == null || offset < 0) {
                    throw new IllegalArgumentException("Partial offsets must not be negative");
                }
                sorted.put(partition, offset);
            });
            return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
        }
    }

    private static String requireReason(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason must not be blank");
        }
        return reason;
    }
}
