package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaClientSnapshotReaderTest {
    private static final String TOPIC = "output";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    @Test
    void capturesFixedBoundsAndRetainsCoordinatesIncludingTombstones() throws Exception {
        MockConsumer<String, String> consumer = configured(0, 2);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, null, "0"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, null, null));
        });

        KafkaTopicSnapshot snapshot = reader(consumer).read(
                "kafka:9092", TOPIC, Duration.ofSeconds(1), 10);

        assertEquals(Map.of(0, 0L), snapshot.beginningOffsets());
        assertEquals(Map.of(0, 2L), snapshot.endOffsets());
        assertEquals(List.of(
                        new KafkaTopicSnapshot.ObservedRecord(0, 0, "0"),
                        new KafkaTopicSnapshot.ObservedRecord(0, 1, null)),
                snapshot.records());
        assertTrue(consumer.closed());
    }

    @Test
    void retainsOnlyABoundedSampleOfManyOversizedValues() throws Exception {
        // Review finding F11: large malformed values must not exhaust the heap below the cap.
        int records = 1_000;
        String oversized = "x".repeat(100_000);
        MockConsumer<String, String> consumer = configured(0, records);
        consumer.schedulePollTask(() -> {
            for (int offset = 0; offset < records; offset++) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, null, oversized));
            }
        });

        KafkaTopicSnapshot snapshot = reader(consumer).read(
                "kafka:9092", TOPIC, Duration.ofSeconds(5), records);

        assertEquals(records, snapshot.records().size());
        assertTrue(snapshot.records().stream().allMatch(record -> record.value().length()
                <= KafkaClientSnapshotReader.MAX_RETAINED_VALUE_CHARS));
        assertEquals("x".repeat(32) + "…[100000 chars]",
                snapshot.records().getFirst().value());

        KafkaIdSetValidationResult result = new KafkaIdSetValidator(
                (bootstrap, topic, timeout, maximumRecords) -> snapshot)
                .validate("kafka:9092", TOPIC, new long[] {0}, Duration.ofSeconds(5));
        assertEquals("validator.kafka.id-set.malformed-ids", result.reason());
        assertEquals(records, result.evidence().defectTotals().orElseThrow().malformedCount());
        assertTrue(result.evidence().malformedSamples().size() <= 100);
    }

    @Test
    void keepsShortValuesAndTombstonesExactly() {
        String longestCanonicalId = Long.toString(Long.MIN_VALUE);

        assertEquals(longestCanonicalId,
                KafkaClientSnapshotReader.retainedValue(longestCanonicalId));
        assertEquals("y".repeat(64), KafkaClientSnapshotReader.retainedValue("y".repeat(64)));
        assertEquals(null, KafkaClientSnapshotReader.retainedValue(null));
    }

    @Test
    void capturesRawHighWatermarkAndRejectsADuplicateExposedByADelayedCommitMarker()
            throws Exception {
        List<String> events = new ArrayList<>();
        List<String> isolationLevels = new ArrayList<>();
        MockConsumer<String, String> boundary = new MockConsumer<>("earliest") {
            @Override
            public synchronized Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
                events.add("metadata");
                return super.listTopics(timeout);
            }

            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(
                    Collection<TopicPartition> partitions,
                    Duration timeout) {
                events.add("raw-end");
                return super.endOffsets(partitions, timeout);
            }
        };
        configure(boundary, 0, 3);
        MockConsumer<String, String> committed = new MockConsumer<>("earliest") {
            @Override
            public synchronized Map<TopicPartition, Long> beginningOffsets(
                    Collection<TopicPartition> partitions,
                    Duration timeout) {
                events.add("committed-beginning");
                return super.beginningOffsets(partitions, timeout);
            }

            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(
                    Collection<TopicPartition> partitions,
                    Duration timeout) {
                throw new AssertionError(
                        "The committed last stable offset must not define the snapshot boundary");
            }

            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                events.add("committed-poll");
                return super.poll(timeout);
            }
        };
        configure(committed, 0, 2);
        committed.schedulePollTask(() -> {
            committed.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, null, "0"));
            committed.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, null, "1"));
        });
        committed.schedulePollTask(() -> committed.addRecord(
                new ConsumerRecord<>(TOPIC, 0, 2, null, "1")));

        KafkaClientSnapshotReader reader = new KafkaClientSnapshotReader(properties -> {
            String isolation = properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
            isolationLevels.add(isolation);
            events.add("create:" + isolation);
            return "read_uncommitted".equals(isolation) ? boundary : committed;
        });
        KafkaIdSetValidationResult result = new KafkaIdSetValidator(reader)
                .validate("kafka:9092", TOPIC, new long[] {0, 1}, Duration.ofSeconds(1));

        assertEquals(List.of("read_uncommitted", "read_committed"), isolationLevels);
        assertEquals(List.of(
                        "create:read_uncommitted",
                        "metadata",
                        "raw-end",
                        "create:read_committed",
                        "committed-beginning",
                        "committed-poll",
                        "committed-poll"),
                events);
        assertEquals(KafkaIdSetValidationResult.Status.FAIL, result.status());
        assertEquals("validator.kafka.id-set.duplicate-ids", result.reason());
        assertEquals(Map.of(0, 3L), result.evidence().endOffsets());
        assertEquals(3, result.evidence().observedCount());
        assertEquals(List.of(
                        new KafkaIdSetValidationResult.RecordSample(0, 2, "1")),
                result.evidence().duplicateSamples());
        assertTrue(boundary.closed());
        assertTrue(committed.closed());
    }

    @Test
    void ignoresRecordsBeyondTheOneCapturedRawBoundary() throws Exception {
        MockConsumer<String, String> boundary = configured(0, 2);
        MockConsumer<String, String> committed = configured(0, 3);
        committed.schedulePollTask(() -> {
            committed.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, null, "0"));
            committed.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, null, "1"));
            committed.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, null, "later"));
        });

        KafkaTopicSnapshot snapshot = reader(boundary, committed).read(
                "kafka:9092", TOPIC, Duration.ofSeconds(1), 10);

        assertEquals(Map.of(0, 2L), snapshot.endOffsets());
        assertEquals(List.of(
                        new KafkaTopicSnapshot.ObservedRecord(0, 0, "0"),
                        new KafkaTopicSnapshot.ObservedRecord(0, 1, "1")),
                snapshot.records());
    }

    @Test
    void unresolvedTransactionTimesOutWithTheRawBoundaryAndCommittedPrefixEvidence() {
        AtomicLong clock = new AtomicLong();
        MockConsumer<String, String> boundary = configured(0, 3);
        AtomicInteger polls = new AtomicInteger();
        MockConsumer<String, String> committed = new MockConsumer<>("earliest") {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                if (polls.incrementAndGet() == 2) {
                    clock.set(2);
                }
                return super.poll(timeout);
            }
        };
        configure(committed, 0, 2);
        committed.schedulePollTask(() -> {
            committed.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, null, "0"));
            committed.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, null, "1"));
        });

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(boundary, committed, clock).read(
                        "kafka:9092", TOPIC, Duration.ofNanos(1), 10));

        assertEquals("verification.kafka.incomplete-after-timeout", failure.reason());
        assertEquals(Map.of(0, 3L), failure.partialEvidence().endOffsets());
        assertEquals(2, failure.partialEvidence().observedCount());
        assertEquals(List.of(
                        new KafkaTopicSnapshot.ObservedRecord(0, 0, "0"),
                        new KafkaTopicSnapshot.ObservedRecord(0, 1, "1")),
                failure.partialEvidence().observedSamples());
        assertTrue(boundary.closed());
        assertTrue(committed.closed());
    }

    @Test
    void committedReaderCanAdvanceAcrossAnAbortedGapWithoutMaterializingIt()
            throws Exception {
        MockConsumer<String, String> boundary = configured(0, 3);
        MockConsumer<String, String> committed = new MockConsumer<>("earliest") {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                ConsumerRecords<String, String> records = super.poll(timeout);
                // A real read_committed consumer advances over aborted data and control records
                // without returning them. Model that position movement explicitly here.
                seek(PARTITION, 3L);
                return records;
            }
        };
        configure(committed, 0, 3);
        committed.schedulePollTask(() -> committed.addRecord(
                new ConsumerRecord<>(TOPIC, 0, 0, null, "0")));

        KafkaTopicSnapshot snapshot = reader(boundary, committed).read(
                "kafka:9092", TOPIC, Duration.ofSeconds(1), 10);

        assertEquals(Map.of(0, 3L), snapshot.endOffsets());
        assertEquals(List.of(
                new KafkaTopicSnapshot.ObservedRecord(0, 0, "0")), snapshot.records());
        assertTrue(boundary.closed());
        assertTrue(committed.closed());
    }

    @Test
    void reachableBrokerWithMissingTopicHasAStableMissingTopicFailure() {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        consumer.updatePartitions("another-topic", List.of(partition("another-topic")));

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(consumer).read(
                        "kafka:9092", TOPIC, Duration.ofSeconds(1), 10));

        assertEquals("verification.kafka.topic-not-found", failure.reason());
    }

    @Test
    void outputLimitFailsClosedAndCountsTheRecordThatProvesTheLimitWasExceeded() {
        MockConsumer<String, String> consumer = configured(0, 2);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, null, "0"));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, null, "0"));
        });

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(consumer).read(
                        "kafka:9092", TOPIC, Duration.ofSeconds(1), 1));

        assertEquals("verification.kafka.output-limit-exceeded", failure.reason());
        assertEquals(Map.of(0, 0L), failure.partialEvidence().beginningOffsets());
        assertEquals(Map.of(0, 2L), failure.partialEvidence().endOffsets());
        assertEquals(2, failure.partialEvidence().observedCount());
        assertEquals(
                List.of(
                        new KafkaTopicSnapshot.ObservedRecord(0, 0, "0"),
                        new KafkaTopicSnapshot.ObservedRecord(0, 1, "0")),
                failure.partialEvidence().observedSamples());
    }

    @Test
    void failureEvidenceRetainsOnlyTheCoordinateOrderedTopOneHundredSamples() {
        TopicPartition partitionOne = new TopicPartition(TOPIC, 1);
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        consumer.updatePartitions(
                TOPIC,
                List.of(partition(TOPIC, 0), partition(TOPIC, 1)));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L, partitionOne, 0L));
        consumer.updateEndOffsets(Map.of(PARTITION, 76L, partitionOne, 76L));
        consumer.schedulePollTask(() -> {
            for (int offset = 0; offset < 76; offset++) {
                consumer.addRecord(new ConsumerRecord<>(
                        TOPIC, 1, offset, null, Integer.toString(offset)));
            }
            for (int offset = 0; offset < 76; offset++) {
                consumer.addRecord(new ConsumerRecord<>(
                        TOPIC, 0, offset, null, Integer.toString(offset)));
            }
        });

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(consumer).read(
                        "kafka:9092", TOPIC, Duration.ofSeconds(1), 151));

        assertEquals("verification.kafka.output-limit-exceeded", failure.reason());
        assertEquals(152, failure.partialEvidence().observedCount());
        List<KafkaTopicSnapshot.ObservedRecord> expected = new ArrayList<>();
        for (int offset = 0; offset < 76; offset++) {
            expected.add(new KafkaTopicSnapshot.ObservedRecord(
                    0, offset, Integer.toString(offset)));
        }
        for (int offset = 0; offset < 24; offset++) {
            expected.add(new KafkaTopicSnapshot.ObservedRecord(
                    1, offset, Integer.toString(offset)));
        }
        assertEquals(expected, failure.partialEvidence().observedSamples());
    }

    @Test
    void timeoutAfterPollDoesNotTraverseTheReturnedBatch() {
        AtomicLong clock = new AtomicLong();
        MockConsumer<String, String> consumer = configured(0, 500);
        consumer.schedulePollTask(() -> {
            for (int offset = 0; offset < 500; offset++) {
                consumer.addRecord(new ConsumerRecord<>(
                        TOPIC, 0, offset, null, Integer.toString(offset)));
            }
            clock.set(2);
        });

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(consumer, clock).read(
                        "kafka:9092", TOPIC, Duration.ofNanos(1), 1_000));

        assertEquals("verification.kafka.incomplete-after-timeout", failure.reason());
        assertEquals(0, failure.partialEvidence().observedCount());
        assertEquals(List.of(), failure.partialEvidence().observedSamples());
    }

    @Test
    void nonZeroBeginningOffsetFailsAsTruncatedWithCapturedBounds() {
        MockConsumer<String, String> consumer = configured(1, 2);

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(consumer).read(
                        "kafka:9092", TOPIC, Duration.ofSeconds(1), 10));

        assertEquals("verification.kafka.snapshot-truncated", failure.reason());
        assertEquals(Map.of(0, 1L), failure.partialEvidence().beginningOffsets());
        assertEquals(Map.of(0, 2L), failure.partialEvidence().endOffsets());
    }

    @Test
    void metadataTimeoutBeforeReachabilityIsUnreachable() {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest") {
            @Override
            public synchronized Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
                throw new TimeoutException("metadata unavailable");
            }
        };

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(consumer).read(
                        "kafka:9092", TOPIC, Duration.ofSeconds(1), 10));

        assertEquals("verification.kafka.unreachable-after-timeout", failure.reason());
        assertEquals(Map.of(), failure.partialEvidence().endOffsets());
    }

    @Test
    void timeoutAfterSuccessfulMetadataDiscoveryIsIncompleteRatherThanUnreachable() {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest") {
            @Override
            public synchronized Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
                Map<String, List<PartitionInfo>> topics = super.listTopics(timeout);
                injectTimeoutException(1);
                return topics;
            }
        };
        configure(consumer, 0, 1);

        KafkaSnapshotException failure = assertThrows(
                KafkaSnapshotException.class,
                () -> reader(consumer).read(
                        "kafka:9092", TOPIC, Duration.ofSeconds(1), 10));

        assertEquals("verification.kafka.incomplete-after-timeout", failure.reason());
    }

    @Test
    void negativeNanoTimeOriginDoesNotExpireAHealthySnapshot() throws Exception {
        AtomicLong clock = new AtomicLong(-1_000_000L);
        MockConsumer<String, String> consumer = configured(0, 1);
        consumer.schedulePollTask(() -> consumer.addRecord(
                new ConsumerRecord<>(TOPIC, 0, 0, null, "0")));

        KafkaTopicSnapshot snapshot = reader(consumer, clock).read(
                "kafka:9092", TOPIC, Duration.ofSeconds(1), 10);

        assertEquals(List.of(
                new KafkaTopicSnapshot.ObservedRecord(0, 0, "0")), snapshot.records());
    }

    private static KafkaClientSnapshotReader reader(MockConsumer<String, String> consumer) {
        return new KafkaClientSnapshotReader(properties -> consumer);
    }

    private static KafkaClientSnapshotReader reader(
            MockConsumer<String, String> consumer,
            AtomicLong clock) {
        return new KafkaClientSnapshotReader(properties -> consumer, clock::get);
    }

    private static KafkaClientSnapshotReader reader(
            MockConsumer<String, String> boundary,
            MockConsumer<String, String> committed) {
        return new KafkaClientSnapshotReader(properties ->
                consumerForIsolation(properties, boundary, committed));
    }

    private static KafkaClientSnapshotReader reader(
            MockConsumer<String, String> boundary,
            MockConsumer<String, String> committed,
            AtomicLong clock) {
        return new KafkaClientSnapshotReader(
                properties -> consumerForIsolation(properties, boundary, committed),
                clock::get);
    }

    private static MockConsumer<String, String> consumerForIsolation(
            Properties properties,
            MockConsumer<String, String> boundary,
            MockConsumer<String, String> committed) {
        return switch (properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG)) {
            case "read_uncommitted" -> boundary;
            case "read_committed" -> committed;
            default -> throw new AssertionError("Unexpected isolation level: "
                    + properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        };
    }

    private static MockConsumer<String, String> configured(long beginning, long end) {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        configure(consumer, beginning, end);
        return consumer;
    }

    private static void configure(
            MockConsumer<String, String> consumer,
            long beginning,
            long end) {
        consumer.updatePartitions(TOPIC, List.of(partition(TOPIC)));
        consumer.updateBeginningOffsets(Map.of(PARTITION, beginning));
        consumer.updateEndOffsets(Map.of(PARTITION, end));
    }

    private static PartitionInfo partition(String topic) {
        return partition(topic, 0);
    }

    private static PartitionInfo partition(String topic, int partition) {
        return new PartitionInfo(topic, partition, null, new Node[0], new Node[0]);
    }
}
