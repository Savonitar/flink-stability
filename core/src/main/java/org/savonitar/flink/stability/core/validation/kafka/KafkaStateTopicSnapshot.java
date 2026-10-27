package org.savonitar.flink.stability.core.validation.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Raw committed values from a completely traversed fixed Kafka boundary. */
public record KafkaStateTopicSnapshot(String topic, Map<Integer, Long> beginningOffsets,
        Map<Integer, Long> endOffsets, List<ObservedRecord> records) {
    public static final int MAX_RECORD_BYTES = 1024;

    public KafkaStateTopicSnapshot {
        // Share the established offset/bounds contract without decoding any record bytes.
        var bounds = new KafkaTopicSnapshot(topic, beginningOffsets, endOffsets, List.of());
        beginningOffsets = bounds.beginningOffsets();
        endOffsets = bounds.endOffsets();
        records = List.copyOf(records);
        for (var record : records) {
            var begin = beginningOffsets.get(record.partition());
            var end = endOffsets.get(record.partition());
            if (begin == null || record.offset() < begin || record.offset() >= end)
                throw new IllegalArgumentException("Record lies outside the fixed snapshot bounds");
        }
    }

    /** Oversized values retain a diagnostic prefix and explicit original size, never valid JSON. */
    public record ObservedRecord(int partition, long offset, byte[] bytes, int originalSize) {
        public ObservedRecord(int partition, long offset, byte[] bytes) {
            this(partition, offset, bytes == null ? null
                    : Arrays.copyOf(bytes, Math.min(bytes.length, MAX_RECORD_BYTES)),
                    bytes == null ? -1 : bytes.length);
        }

        public ObservedRecord {
            if (partition < 0 || offset < 0 || originalSize < -1
                    || (bytes == null) != (originalSize == -1)
                    || bytes != null && (bytes.length > MAX_RECORD_BYTES || bytes.length != Math.min(originalSize, MAX_RECORD_BYTES)))
                throw new IllegalArgumentException("Invalid bounded record bytes");
            bytes = bytes == null ? null : bytes.clone();
        }

        @Override public byte[] bytes() { return bytes == null ? null : bytes.clone(); }
        public int retainedBytes() { return bytes == null ? 0 : bytes.length; }

        /** Diagnostic text only: strict decoding and original-size checks are performed by the oracle. */
        public String diagnosticValue() {
            if (bytes == null) return null;
            return new String(bytes, StandardCharsets.UTF_8)
                    + (originalSize > MAX_RECORD_BYTES ? "…[" + originalSize + " bytes]" : "");
        }
    }
}
