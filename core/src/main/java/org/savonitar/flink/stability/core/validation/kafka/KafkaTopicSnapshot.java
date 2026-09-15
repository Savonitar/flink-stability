package org.savonitar.flink.stability.core.validation.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Complete read-committed records bounded by captured raw exclusive partition end offsets. */
public record KafkaTopicSnapshot(
        String topic,
        Map<Integer, Long> beginningOffsets,
        Map<Integer, Long> endOffsets,
        List<ObservedRecord> records) {

    public KafkaTopicSnapshot {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        beginningOffsets = immutableOffsets(beginningOffsets, "beginningOffsets");
        endOffsets = immutableOffsets(endOffsets, "endOffsets");
        if (!beginningOffsets.keySet().equals(endOffsets.keySet())) {
            throw new IllegalArgumentException(
                    "Beginning and end offsets must cover identical partitions");
        }
        for (int partition : beginningOffsets.keySet()) {
            if (beginningOffsets.get(partition) > endOffsets.get(partition)) {
                throw new IllegalArgumentException(
                        "Beginning offset exceeds end offset for partition " + partition);
            }
        }
        records = List.copyOf(records);
        for (ObservedRecord record : records) {
            if (!beginningOffsets.containsKey(record.partition())) {
                throw new IllegalArgumentException(
                        "Record references an undeclared snapshot partition");
            }
            if (record.offset() < beginningOffsets.get(record.partition())
                    || record.offset() >= endOffsets.get(record.partition())) {
                throw new IllegalArgumentException(
                        "Record offset is outside the fixed snapshot bounds");
            }
        }
    }

    /** One read-committed record; a null value is a tombstone and remains evidence. */
    public record ObservedRecord(int partition, long offset, String value) {
        public ObservedRecord {
            if (partition < 0 || offset < 0) {
                throw new IllegalArgumentException(
                        "Record partition and offset must not be negative");
            }
        }
    }

    private static Map<Integer, Long> immutableOffsets(
            Map<Integer, Long> offsets,
            String label) {
        if (offsets == null || offsets.isEmpty()) {
            throw new IllegalArgumentException(label + " must not be empty");
        }
        TreeMap<Integer, Long> sorted = new TreeMap<>();
        offsets.forEach((partition, offset) -> {
            if (partition == null || partition < 0 || offset == null || offset < 0) {
                throw new IllegalArgumentException(label + " contains an invalid offset");
            }
            sorted.put(partition, offset);
        });
        return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
    }
}
