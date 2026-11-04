package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.LongSupplier;

/**
 * Kafka-client implementation that captures raw high watermarks once and reads only that final
 * range with {@code read_committed} isolation.
 */
public final class KafkaClientStateSnapshotReader implements KafkaStateSnapshotReader {
    private static final Duration MAX_POLL = Duration.ofSeconds(1);
    private static final int MAX_FAILURE_EVIDENCE_SAMPLES = 100;
    private static final int DEADLINE_CHECK_INTERVAL = 256;
    private static final Comparator<KafkaStateTopicSnapshot.ObservedRecord> COORDINATE_ORDER =
            Comparator.comparingInt(KafkaStateTopicSnapshot.ObservedRecord::partition)
                    .thenComparingLong(KafkaStateTopicSnapshot.ObservedRecord::offset);

    private final ConsumerFactory consumerFactory;
    private final LongSupplier nanoTime;

    public KafkaClientStateSnapshotReader() {
        this(properties -> new KafkaConsumer<>(properties), System::nanoTime);
    }

    KafkaClientStateSnapshotReader(ConsumerFactory consumerFactory) {
        this(consumerFactory, System::nanoTime);
    }

    KafkaClientStateSnapshotReader(ConsumerFactory consumerFactory, LongSupplier nanoTime) {
        this.consumerFactory = java.util.Objects.requireNonNull(
                consumerFactory, "consumerFactory");
        this.nanoTime = java.util.Objects.requireNonNull(nanoTime, "nanoTime");
    }

    @Override
    public KafkaStateTopicSnapshot read(
            String bootstrapServers,
            String topic,
            Duration timeout,
            long maximumRecords,
            long maximumRetainedBytes) throws KafkaSnapshotException {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (maximumRecords < 1) {
            throw new IllegalArgumentException("maximumRecords must be positive");
        }
        if (maximumRetainedBytes < 1) throw new IllegalArgumentException("maximumRetainedBytes must be positive");
        long retainedBytes = 0;
        MonotonicDeadline deadline = MonotonicDeadline.start(timeout, nanoTime);
        Consumer<byte[], byte[]> boundaryConsumer = null;
        Consumer<byte[], byte[]> consumer = null;
        boolean brokerReached = false;
        boolean offsetsCaptured = false;
        Map<TopicPartition, Long> beginning = Map.of();
        Map<TopicPartition, Long> end = Map.of();
        List<KafkaStateTopicSnapshot.ObservedRecord> records = new ArrayList<>();
        FailureProgress failureProgress = new FailureProgress();
        try {
            boundaryConsumer = consumerFactory.create(
                    properties(bootstrapServers, timeout, "read_uncommitted"));
            Map<String, List<PartitionInfo>> topics =
                    boundaryConsumer.listTopics(remaining(deadline));
            brokerReached = true;
            List<PartitionInfo> partitionInfo = topics.get(topic);
            if (partitionInfo == null || partitionInfo.isEmpty()) {
                throw new KafkaSnapshotException(
                        "verification.kafka.topic-not-found",
                        "Kafka topic does not exist or has no partitions: " + topic);
            }
            List<TopicPartition> partitions = partitionInfo.stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .sorted(Comparator.comparingInt(TopicPartition::partition))
                    .toList();
            // A read_committed endOffsets call returns the current last stable offsets, which can
            // advance after an already-accepted EndTxn finishes writing its commit markers. Raw
            // high watermarks already include every acknowledged transactional record, so they
            // form the fixed boundary that the committed reader must traverse before succeeding.
            end = boundaryConsumer.endOffsets(partitions, remaining(deadline));
            offsetsCaptured = true;

            consumer = consumerFactory.create(
                    properties(bootstrapServers, timeout, "read_committed"));
            consumer.assign(partitions);
            beginning = consumer.beginningOffsets(partitions, remaining(deadline));
            List<TopicPartition> truncated = beginning.entrySet().stream()
                    .filter(entry -> entry.getValue() != 0L)
                    .map(Map.Entry::getKey)
                    .toList();
            if (!truncated.isEmpty()) {
                throw new KafkaSnapshotException(
                        "verification.kafka.snapshot-truncated",
                        "Kafka output no longer begins at offset zero for partitions "
                                + truncated,
                        null,
                        partial(beginning, end, failureProgress));
            }
            beginning.forEach(consumer::seek);

            while (!complete(consumer, end, deadline)) {
                ConsumerRecords<byte[], byte[]> polled =
                        consumer.poll(min(MAX_POLL, remaining(deadline)));
                int examined = 0;
                for (ConsumerRecord<byte[], byte[]> record : polled) {
                    if ((examined++ % DEADLINE_CHECK_INTERVAL) == 0) {
                        remaining(deadline);
                    }
                    TopicPartition partition =
                            new TopicPartition(record.topic(), record.partition());
                    long exclusiveEnd = end.get(partition);
                    if (record.offset() < exclusiveEnd) {
                        KafkaStateTopicSnapshot.ObservedRecord observed =
                                new KafkaStateTopicSnapshot.ObservedRecord(
                                        record.partition(),
                                        record.offset(),
                                        record.value());
                        failureProgress.observe(observed);
                        if (failureProgress.observedCount() > maximumRecords) {
                            throw new KafkaSnapshotException(
                                    "verification.kafka.output-limit-exceeded",
                                    "Kafka output exceeded the in-memory verification limit of "
                                            + maximumRecords + " records",
                                    null,
                                    partial(beginning, end, failureProgress));
                        }
                        retainedBytes = Math.addExact(retainedBytes, observed.retainedBytes());
                        if (retainedBytes > maximumRetainedBytes) {
                            throw new KafkaSnapshotException(
                                    "verification.kafka.output-byte-limit-exceeded",
                                    "Kafka retained record bytes exceeded the verification limit of "
                                            + maximumRetainedBytes + " bytes", null,
                                    partial(beginning, end, failureProgress));
                        }
                        records.add(observed);
                    }
                }
            }

            return new KafkaStateTopicSnapshot(
                    topic,
                    integerOffsets(beginning),
                    integerOffsets(end),
                    records);
        } catch (KafkaSnapshotException failure) {
            throw failure;
        } catch (TimeoutException failure) {
            throw new KafkaSnapshotException(
                    brokerReached
                            ? "verification.kafka.incomplete-after-timeout"
                            : "verification.kafka.unreachable-after-timeout",
                    brokerReached
                            ? (offsetsCaptured
                                    ? "Kafka snapshot reading did not complete before the timeout"
                                    : "Kafka high-watermark capture did not complete before the timeout")
                            : "Kafka remained unreachable until the verification timeout",
                    failure,
                    partial(beginning, end, failureProgress));
        } catch (RuntimeException failure) {
            throw new KafkaSnapshotException(
                    "verification.kafka.snapshot-unavailable",
                    "Kafka snapshot failed: " + failure.getClass().getSimpleName(),
                    failure,
                    partial(beginning, end, failureProgress));
        } finally {
            closeQuietly(consumer);
            if (boundaryConsumer != consumer) {
                closeQuietly(boundaryConsumer);
            }
        }
    }

    private static void closeQuietly(Consumer<byte[], byte[]> consumer) {
        if (consumer == null) {
            return;
        }
        try {
            consumer.close(Duration.ZERO);
        } catch (RuntimeException ignored) {
            // Snapshot outcome already owns the result; close cannot make it complete.
        }
    }

    private static boolean complete(
            Consumer<byte[], byte[]> consumer,
            Map<TopicPartition, Long> end,
            MonotonicDeadline deadline) {
        for (Map.Entry<TopicPartition, Long> entry : end.entrySet()) {
            if (consumer.position(entry.getKey(), remaining(deadline)) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    private static Map<Integer, Long> integerOffsets(Map<TopicPartition, Long> offsets) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        offsets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparingInt(TopicPartition::partition)))
                .forEach(entry -> result.put(entry.getKey().partition(), entry.getValue()));
        return result;
    }

    private static KafkaSnapshotException.PartialEvidence partial(
            Map<TopicPartition, Long> beginning,
            Map<TopicPartition, Long> end,
            FailureProgress failureProgress) {
        return new KafkaSnapshotException.PartialEvidence(
                integerOffsets(beginning),
                integerOffsets(end),
                failureProgress.observedCount(),
                failureProgress.observedSamples().stream().map(record ->
                        new KafkaTopicSnapshot.ObservedRecord(record.partition(), record.offset(),
                                record.diagnosticValue())).toList());
    }

    private static Properties properties(
            String bootstrapServers,
            Duration timeout,
            String isolationLevel) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "flink-stability-validator-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 256);
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, boundedMillis(timeout));
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, boundedMillis(timeout));
        return properties;
    }

    private static int boundedMillis(Duration duration) {
        long millis;
        try {
            millis = duration.toMillis();
        } catch (ArithmeticException overflow) {
            millis = Integer.MAX_VALUE;
        }
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, millis));
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static final class FailureProgress {
        private final TreeSet<KafkaStateTopicSnapshot.ObservedRecord> observedSamples =
                new TreeSet<>(COORDINATE_ORDER);
        private long observedCount;

        private void observe(KafkaStateTopicSnapshot.ObservedRecord record) {
            observedCount++;
            observedSamples.add(record);
            if (observedSamples.size() > MAX_FAILURE_EVIDENCE_SAMPLES) {
                observedSamples.pollLast();
            }
        }

        private long observedCount() {
            return observedCount;
        }

        private List<KafkaStateTopicSnapshot.ObservedRecord> observedSamples() {
            return List.copyOf(observedSamples);
        }
    }

    @FunctionalInterface
    interface ConsumerFactory {
        Consumer<byte[], byte[]> create(Properties properties);
    }

    private static Duration remaining(MonotonicDeadline deadline) {
        return deadline.remainingOrThrow(
                () -> new TimeoutException("Kafka snapshot deadline expired"));
    }
}
