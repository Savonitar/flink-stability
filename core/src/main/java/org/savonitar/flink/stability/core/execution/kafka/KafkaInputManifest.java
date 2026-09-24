package org.savonitar.flink.stability.core.execution.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Immutable evidence for generated records and the source topic's closed input bounds. */
public record KafkaInputManifest(
        String clusterAlias,
        String topic,
        long totalRecords,
        List<RecordAcknowledgement> records,
        Map<Integer, Long> beginningOffsets,
        Map<Integer, Long> exclusiveEndOffsets,
        EvidenceStatus evidenceStatus,
        ReconciliationEvidence reconciliation) {

    public KafkaInputManifest {
        clusterAlias = requireNonBlank(clusterAlias, "clusterAlias");
        topic = requireNonBlank(topic, "topic");
        if (totalRecords < 1) {
            throw new IllegalArgumentException("totalRecords must be positive");
        }
        records = List.copyOf(Objects.requireNonNull(records, "records"));
        beginningOffsets = immutableSortedOffsets(beginningOffsets, "beginningOffsets");
        exclusiveEndOffsets = immutableSortedOffsets(
                exclusiveEndOffsets, "exclusiveEndOffsets");
        Objects.requireNonNull(evidenceStatus, "evidenceStatus");
        Objects.requireNonNull(reconciliation, "reconciliation");
        if (records.size() != totalRecords) {
            throw new IllegalArgumentException(
                    "The acknowledgement count must equal totalRecords");
        }
        if (!beginningOffsets.keySet().equals(exclusiveEndOffsets.keySet())) {
            throw new IllegalArgumentException(
                    "Beginning and end offsets must cover identical partitions");
        }
        if (beginningOffsets.isEmpty()) {
            throw new IllegalArgumentException("Input bounds must cover at least one partition");
        }
        long boundedRecords = 0;
        for (Map.Entry<Integer, Long> beginning : beginningOffsets.entrySet()) {
            long end = exclusiveEndOffsets.get(beginning.getKey());
            if (beginning.getValue() != 0 || end < beginning.getValue()) {
                throw new IllegalArgumentException(
                        "Generated input bounds must start at zero and not move backwards");
            }
            boundedRecords = Math.addExact(boundedRecords, end - beginning.getValue());
        }
        if (boundedRecords != totalRecords) {
            throw new IllegalArgumentException(
                    "Closed input bounds must contain exactly totalRecords");
        }
        Set<Long> observedIds = Set.copyOf(reconciliation.observedIds());
        for (int index = 0; index < records.size(); index++) {
            RecordAcknowledgement record = records.get(index);
            if (record.id() != index) {
                throw new IllegalArgumentException(
                        "Record evidence must be ordered by contiguous generated id");
            }
            Long partitionEnd = exclusiveEndOffsets.get(record.partition());
            if (partitionEnd == null || record.offset() >= partitionEnd) {
                throw new IllegalArgumentException(
                        "Record acknowledgement must be inside the closed input bounds");
            }
            if (record.producerAttempts().stream().anyMatch(
                    attempt -> attempt.targetPartition() != record.partition())) {
                throw new IllegalArgumentException(
                        "Producer attempts must target the acknowledged record partition");
            }
            boolean observed = observedIds.contains(record.id());
            if (observed != (record.terminalDisposition() == TerminalDisposition.PRESENT)) {
                throw new IllegalArgumentException(
                        "Observed ids and terminal record dispositions must agree");
            }
        }
        if (reconciliation.observedIds().stream().anyMatch(id -> id >= totalRecords)) {
            throw new IllegalArgumentException(
                    "Observed ids must identify generated records in this manifest");
        }
        if (evidenceStatus == EvidenceStatus.COMPLETE
                && (reconciliation.consumerConfiguration().isEmpty()
                        || !reconciliation.reachedEveryExclusiveEnd()
                        || reconciliation.observedIds().size() != totalRecords
                        || records.stream().anyMatch(record ->
                                record.terminalDisposition() != TerminalDisposition.PRESENT))) {
            throw new IllegalArgumentException(
                    "Complete evidence requires consumer settings, every record, and bounds");
        }
    }

    /**
     * The generated IDs observed in the closed input, sorted ascending: the exact set that
     * the terminal kafka.id-set oracle expects (SPEC-001 R7.3).
     */
    public long[] presentIds() {
        return records.stream()
                .filter(record -> record.terminalDisposition() == TerminalDisposition.PRESENT)
                .mapToLong(RecordAcknowledgement::id)
                .toArray();
    }

    public enum EvidenceStatus {
        COMPLETE,
        PARTIAL
    }

    public enum AcknowledgementOutcome {
        ACKNOWLEDGED
    }

    /** One invocation of the harness producer API; Kafka-internal retries are not observable. */
    public record ProducerAttempt(
            int ordinal,
            int targetPartition,
            int acknowledgedPartition,
            long acknowledgedOffset,
            AcknowledgementOutcome acknowledgementOutcome) {

        public ProducerAttempt {
            if (ordinal < 1
                    || targetPartition < 0
                    || acknowledgedPartition < 0
                    || acknowledgedOffset < 0) {
                throw new IllegalArgumentException(
                        "Producer attempt values must identify a non-negative Kafka position");
            }
            Objects.requireNonNull(acknowledgementOutcome, "acknowledgementOutcome");
        }
    }

    public record RecordAcknowledgement(
            long id,
            String payloadSha256,
            List<ProducerAttempt> producerAttempts,
            int partition,
            long offset,
            AcknowledgementOutcome acknowledgementOutcome,
            TerminalDisposition terminalDisposition) {

        private static final Pattern SHA_256 = Pattern.compile("^[0-9a-f]{64}$");

        public RecordAcknowledgement {
            if (id < 0 || partition < 0 || offset < 0) {
                throw new IllegalArgumentException(
                        "Record id, partition, and offset must not be negative");
            }
            Objects.requireNonNull(payloadSha256, "payloadSha256");
            producerAttempts = List.copyOf(Objects.requireNonNull(
                    producerAttempts, "producerAttempts"));
            Objects.requireNonNull(acknowledgementOutcome, "acknowledgementOutcome");
            Objects.requireNonNull(terminalDisposition, "terminalDisposition");
            if (!SHA_256.matcher(payloadSha256).matches()) {
                throw new IllegalArgumentException(
                        "payloadSha256 must be a lowercase SHA-256 value");
            }
            if (producerAttempts.isEmpty()) {
                throw new IllegalArgumentException(
                        "Every acknowledged record must retain its producer attempts");
            }
            for (int index = 0; index < producerAttempts.size(); index++) {
                if (producerAttempts.get(index).ordinal() != index + 1) {
                    throw new IllegalArgumentException(
                            "Producer attempt ordinals must be contiguous and one-based");
                }
            }
            ProducerAttempt terminalAttempt = producerAttempts.getLast();
            if (terminalAttempt.acknowledgedPartition() != partition
                    || terminalAttempt.acknowledgedOffset() != offset
                    || terminalAttempt.acknowledgementOutcome()
                            != acknowledgementOutcome) {
                throw new IllegalArgumentException(
                        "Terminal producer attempt must match the record acknowledgement");
            }
        }
    }

    public enum TerminalDisposition {
        PRESENT,
        ACKNOWLEDGED_MISSING,
        INDETERMINATE
    }

    /** Evidence emitted by the bounded reconciliation consumer. */
    public record ReconciliationEvidence(
            Map<String, String> consumerConfiguration,
            List<Long> observedIds,
            boolean reachedEveryExclusiveEnd) {

        public ReconciliationEvidence {
            consumerConfiguration = immutableSortedStrings(
                    consumerConfiguration, "consumerConfiguration");
            Objects.requireNonNull(observedIds, "observedIds");
            List<Long> sortedIds = new ArrayList<>(observedIds.size());
            for (Long id : observedIds) {
                if (id == null || id < 0) {
                    throw new IllegalArgumentException(
                            "Observed ids must not contain negative or null values");
                }
                sortedIds.add(id);
            }
            sortedIds.sort(Long::compareTo);
            for (int index = 1; index < sortedIds.size(); index++) {
                if (sortedIds.get(index).equals(sortedIds.get(index - 1))) {
                    throw new IllegalArgumentException("Observed ids must be unique");
                }
            }
            observedIds = List.copyOf(sortedIds);
        }
    }

    private static Map<Integer, Long> immutableSortedOffsets(
            Map<Integer, Long> offsets, String name) {
        Objects.requireNonNull(offsets, name);
        TreeMap<Integer, Long> sorted = new TreeMap<>();
        offsets.forEach((partition, offset) -> {
            Objects.requireNonNull(partition, name + " partition");
            Objects.requireNonNull(offset, name + " offset");
            if (partition < 0 || offset < 0) {
                throw new IllegalArgumentException(name + " values must not be negative");
            }
            if (sorted.putIfAbsent(partition, offset) != null) {
                throw new IllegalArgumentException(name + " contains a duplicate partition");
            }
        });
        return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
    }

    private static Map<String, String> immutableSortedStrings(
            Map<String, String> values, String name) {
        Objects.requireNonNull(values, name);
        TreeMap<String, String> sorted = new TreeMap<>();
        values.forEach((key, value) -> {
            String checkedKey = requireNonBlank(key, name + " key");
            String checkedValue = requireNonBlank(value, name + " value");
            if (sorted.putIfAbsent(checkedKey, checkedValue) != null) {
                throw new IllegalArgumentException(name + " contains a duplicate key");
            }
        });
        return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
