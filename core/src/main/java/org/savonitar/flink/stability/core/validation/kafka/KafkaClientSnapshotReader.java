package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
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
public final class KafkaClientSnapshotReader implements KafkaSnapshotReader {
    private static final Duration MAX_POLL = Duration.ofSeconds(1);
    private static final int MAX_FAILURE_EVIDENCE_SAMPLES = 100;
    /** Far above the 20 characters of any canonical long ID (review finding F11). */
    static final int MAX_RETAINED_VALUE_CHARS = 64;
    private static final int OVERSIZED_VALUE_SAMPLE_CHARS = 32;
    private static final int DEADLINE_CHECK_INTERVAL = 256;
    private static final Comparator<KafkaTopicSnapshot.ObservedRecord> COORDINATE_ORDER =
            Comparator.comparingInt(KafkaTopicSnapshot.ObservedRecord::partition)
                    .thenComparingLong(KafkaTopicSnapshot.ObservedRecord::offset);

    private final ConsumerFactory consumerFactory;
    private final LongSupplier nanoTime;

    public KafkaClientSnapshotReader() {
        this(properties -> new KafkaConsumer<>(properties), System::nanoTime);
    }

    KafkaClientSnapshotReader(ConsumerFactory consumerFactory) {
        this(consumerFactory, System::nanoTime);
    }

    KafkaClientSnapshotReader(ConsumerFactory consumerFactory, LongSupplier nanoTime) {
        this.consumerFactory = java.util.Objects.requireNonNull(
                consumerFactory, "consumerFactory");
        this.nanoTime = java.util.Objects.requireNonNull(nanoTime, "nanoTime");
    }

    @Override
    public KafkaTopicSnapshot read(
            String bootstrapServers,
            String topic,
            Duration timeout,
            long maximumRecords) throws KafkaSnapshotException {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (maximumRecords < 1) {
            throw new IllegalArgumentException("maximumRecords must be positive");
        }
        MonotonicDeadline deadline = MonotonicDeadline.start(timeout, nanoTime);
        Consumer<String, String> boundaryConsumer = null;
        Consumer<String, String> consumer = null;
        boolean brokerReached = false;
        boolean offsetsCaptured = false;
        Map<TopicPartition, Long> beginning = Map.of();
        Map<TopicPartition, Long> end = Map.of();
        List<KafkaTopicSnapshot.ObservedRecord> records = new ArrayList<>();
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
                ConsumerRecords<String, String> polled =
                        consumer.poll(min(MAX_POLL, remaining(deadline)));
                int examined = 0;
                for (ConsumerRecord<String, String> record : polled) {
                    if ((examined++ % DEADLINE_CHECK_INTERVAL) == 0) {
                        remaining(deadline);
                    }
                    TopicPartition partition =
                            new TopicPartition(record.topic(), record.partition());
                    long exclusiveEnd = end.get(partition);
                    if (record.offset() < exclusiveEnd) {
                        KafkaTopicSnapshot.ObservedRecord observed =
                                new KafkaTopicSnapshot.ObservedRecord(
                                        record.partition(),
                                        record.offset(),
                                        retainedValue(record.value()));
                        failureProgress.observe(observed);
                        if (failureProgress.observedCount() > maximumRecords) {
                            throw new KafkaSnapshotException(
                                    "verification.kafka.output-limit-exceeded",
                                    "Kafka output exceeded the in-memory verification limit of "
                                            + maximumRecords + " records",
                                    null,
                                    partial(beginning, end, failureProgress));
                        }
                        records.add(observed);
                    }
                }
            }

            return new KafkaTopicSnapshot(
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

    /**
     * Bounds what a record keeps in memory until validation. A value too long to be a canonical
     * ID keeps a short prefix plus its length; the ID-set oracle reports it as malformed.
     */
    static String retainedValue(String value) {
        if (value == null || value.length() <= MAX_RETAINED_VALUE_CHARS) {
            return value;
        }
        return value.substring(0, OVERSIZED_VALUE_SAMPLE_CHARS)
                + "…[" + value.length() + " chars]";
    }

    private static void closeQuietly(Consumer<String, String> consumer) {
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
            Consumer<String, String> consumer,
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
                failureProgress.observedSamples());
    }

    private static Properties properties(
            String bootstrapServers,
            Duration timeout,
            String isolationLevel) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "flink-stability-validator-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
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
        private final TreeSet<KafkaTopicSnapshot.ObservedRecord> observedSamples =
                new TreeSet<>(COORDINATE_ORDER);
        private long observedCount;

        private void observe(KafkaTopicSnapshot.ObservedRecord record) {
            observedCount++;
            observedSamples.add(record);
            if (observedSamples.size() > MAX_FAILURE_EVIDENCE_SAMPLES) {
                observedSamples.pollLast();
            }
        }

        private long observedCount() {
            return observedCount;
        }

        private List<KafkaTopicSnapshot.ObservedRecord> observedSamples() {
            return List.copyOf(observedSamples);
        }
    }

    @FunctionalInterface
    interface ConsumerFactory {
        Consumer<String, String> create(Properties properties);
    }

    private static Duration remaining(MonotonicDeadline deadline) {
        return deadline.remainingOrThrow(
                () -> new TimeoutException("Kafka snapshot deadline expired"));
    }
}
