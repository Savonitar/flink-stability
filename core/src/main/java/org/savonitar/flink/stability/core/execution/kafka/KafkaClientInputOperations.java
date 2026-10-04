package org.savonitar.flink.stability.core.execution.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Kafka 4.0 client implementation for explicit topic creation and bounded input preparation. */
final class KafkaClientInputOperations implements KafkaInputOperations {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientInputOperations.class);

    private final String bootstrapServers;
    private final Admin admin;
    private final ProducerFactory producerFactory;
    private final ConsumerFactory consumerFactory;
    private final KafkaBrokerTopicReadiness.Observer topicObserver;
    private final RetryPause retryPause;

    KafkaClientInputOperations(String bootstrapServers, Duration timeout) {
        this(
                bootstrapServers,
                timeout,
                Admin.create(adminProperties(bootstrapServers, timeout)),
                KafkaClientInputOperations::createProducer,
                properties -> new KafkaConsumer<>(properties));
    }

    KafkaClientInputOperations(
            String bootstrapServers,
            Duration timeout,
            Admin admin,
            ConsumerFactory consumerFactory) {
        this(
                bootstrapServers,
                timeout,
                admin,
                KafkaClientInputOperations::createProducer,
                consumerFactory);
    }

    KafkaClientInputOperations(
            String bootstrapServers,
            Duration timeout,
            Admin admin,
            ProducerFactory producerFactory,
            ConsumerFactory consumerFactory) {
        this(bootstrapServers, timeout, admin, producerFactory, consumerFactory,
                KafkaBrokerTopicReadiness::observe, Thread::sleep);
    }

    KafkaClientInputOperations(
            String bootstrapServers, Duration timeout, Admin admin,
            ProducerFactory producerFactory, ConsumerFactory consumerFactory,
            KafkaBrokerTopicReadiness.Observer topicObserver, RetryPause retryPause) {
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(timeout, "timeout");
        this.admin = Objects.requireNonNull(admin, "admin");
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.topicObserver = Objects.requireNonNull(topicObserver, "topicObserver");
        this.retryPause = Objects.requireNonNull(retryPause, "retryPause");
    }

    @Override
    public void createTopics(
            List<TopicDefinition> topics,
            KafkaInputPreparationDeadline deadline) throws Exception {
        List<NewTopic> requested = topics.stream()
                .map(topic -> new NewTopic(
                                topic.name(), topic.partitions(), topic.replicationFactor())
                        .configs(topicConfiguration(topic)))
                .toList();
        List<String> topicNames = topics.stream().map(TopicDefinition::name).toList();
        try (var calls = new KafkaInputCallBoundary("kafka-input-topic-readiness")) {
            // Creation is a single mutation. Only observations are retried after it succeeds.
            invoke("creating Kafka topics", deadline, calls, () -> admin.createTopics(requested)
                    .all().get(deadline.requireRemaining("creating Kafka topics").toNanos(),
                            TimeUnit.NANOSECONDS));
            for (String endpoint : bootstrapServers.split(",")) {
                String broker = endpoint.trim();
                observeWithRetry("observing created Kafka topics " + topicNames + " at broker " + broker,
                        deadline, calls, () -> {
                            topicObserver.observe(broker, topics, deadline);
                            return null;
                        });
            }
            observeWithRetry("describing created Kafka topics", deadline, calls, () -> {
                verifyTopics(topics, deadline);
                return null;
            });
            observeWithRetry("describing Kafka topic configuration", deadline, calls, () -> {
                verifyTopicConfiguration(topics, deadline);
                return null;
            });
        }
    }

    private void verifyTopics(List<TopicDefinition> topics, KafkaInputPreparationDeadline deadline)
            throws Exception {
        Map<String, TopicDescription> actual = admin.describeTopics(
                        topics.stream().map(TopicDefinition::name).toList())
                .allTopicNames()
                .get(deadline.requireRemaining("describing created Kafka topics").toNanos(),
                        TimeUnit.NANOSECONDS);
        if (actual.size() != topics.size()) {
            throw new UnknownTopicOrPartitionException("Kafka has not observed every declared topic");
        }
        for (TopicDefinition expected : topics) {
            TopicDescription description = actual.get(expected.name());
            if (description == null) {
                throw new UnknownTopicOrPartitionException("Kafka has not observed topic " + expected.name());
            }
            if (description.isInternal()
                    || description.partitions().size() != expected.partitions()
                    || description.partitions().stream().anyMatch(
                            partition -> partition.replicas().size()
                                    != expected.replicationFactor())) {
                throw new IllegalStateException(
                        "Kafka topic does not match its declaration: " + expected.name());
            }
        }
    }

    private void verifyTopicConfiguration(List<TopicDefinition> topics,
            KafkaInputPreparationDeadline deadline) throws Exception {
        List<ConfigResource> resources = topics.stream()
                .map(topic -> new ConfigResource(
                        ConfigResource.Type.TOPIC, topic.name()))
                .toList();
        Map<ConfigResource, Config> configurations = admin.describeConfigs(resources)
                .all()
                .get(deadline.requireRemaining("describing Kafka topic configuration").toNanos(),
                        TimeUnit.NANOSECONDS);
        for (TopicDefinition expected : topics) {
            ConfigResource resource = new ConfigResource(
                    ConfigResource.Type.TOPIC, expected.name());
            Config actualConfiguration = configurations.get(resource);
            Map<String, String> expectedConfiguration = topicConfiguration(expected);
            if (actualConfiguration == null
                    || expectedConfiguration.entrySet().stream().anyMatch(entry ->
                            actualConfiguration.get(entry.getKey()) == null
                                    || !entry.getValue().equals(
                                            actualConfiguration.get(entry.getKey()).value()))) {
                throw new IllegalStateException(
                        "Kafka topic configuration does not match its declaration: "
                                + expected.name());
            }
        }
    }

    private <T> T observeWithRetry(String operation, KafkaInputPreparationDeadline deadline,
            KafkaInputCallBoundary calls, Callable<T> observation) throws Exception {
        Throwable lastTransient = null;
        int attempts = 0;
        while (true) {
            try {
                attempts++;
                T result = calls.call(operation, deadline, observation);
                if (lastTransient != null) {
                    LOG.info("Kafka input observation succeeded while {} after {} attempts", operation, attempts);
                }
                return result;
            } catch (InterruptedException failure) {
                throw interrupted(operation, failure);
            } catch (Exception failure) {
                Throwable cause = underlying(failure);
                boolean deadlineFailure = cause instanceof KafkaInputPreparationDeadline
                        .KafkaInputPreparationDeadlineExceededException
                        || (cause instanceof java.util.concurrent.TimeoutException
                                && deadline.remaining().isZero());
                if (lastTransient != null && deadlineFailure) {
                    var exhausted = deadline.exceeded(operation, lastTransient);
                    exhausted.addSuppressed(failure);
                    throw exhausted;
                }
                if (!isTransientObservationFailure(cause)) throw contextual(operation, cause);
                if (lastTransient == null) {
                    LOG.info("Kafka input observation temporarily unavailable while {}; retrying within "
                                    + "the preparation deadline: {}: {}",
                            operation, cause.getClass().getSimpleName(), cause.getMessage());
                }
                lastTransient = cause;
                Duration remaining = deadline.remaining();
                if (remaining.isZero()) throw deadline.exceeded(operation, cause);
                try {
                    retryPause.pause(remaining.compareTo(Duration.ofMillis(50)) < 0
                            ? remaining : Duration.ofMillis(50));
                } catch (InterruptedException interrupted) {
                    throw interrupted(operation, interrupted);
                }
                if (deadline.remaining().isZero()) throw deadline.exceeded(operation, cause);
            }
        }
    }

    private static <T> T invoke(String operation, KafkaInputPreparationDeadline deadline,
            KafkaInputCallBoundary calls, Callable<T> invocation) throws Exception {
        try {
            return calls.call(operation, deadline, invocation);
        } catch (InterruptedException failure) {
            throw interrupted(operation, failure);
        } catch (Exception failure) {
            throw contextual(operation, underlying(failure));
        }
    }

    private static Throwable underlying(Throwable failure) {
        while (failure instanceof ExecutionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static boolean isTransientObservationFailure(Throwable failure) {
        return failure instanceof RetriableException
                || failure instanceof SocketTimeoutException || failure instanceof SocketException
                || failure instanceof EOFException;
    }

    private static IllegalStateException contextual(String operation, Throwable failure) {
        return new IllegalStateException("Kafka input preparation failed while " + operation, failure);
    }

    private static InterruptedException interrupted(String operation, InterruptedException failure) {
        InterruptedException contextual = new InterruptedException("Interrupted while " + operation);
        contextual.initCause(failure);
        return contextual;
    }

    @Override
    public List<ProducedRecord> produceAndAwait(
            String topic,
            int partitions,
            long totalRecords,
            KafkaInputPreparationDeadline deadline) throws Exception {
        InputProducer producer = producerFactory.create(
                producerProperties(
                        bootstrapServers,
                        deadline.requireRemaining("creating the Kafka input producer")));
        Exception operationFailure = null;
        try (KafkaInputCallBoundary sendCalls = new KafkaInputCallBoundary(
                "kafka-input-send")) {
            List<PendingRecord> pending = new ArrayList<>(Math.toIntExact(totalRecords));
            for (long id = 0; id < totalRecords; id++) {
                int partition = (int) (id % partitions);
                byte[] value = Long.toString(id).getBytes(StandardCharsets.UTF_8);
                ProducerRecord<byte[], byte[]> requested = new ProducerRecord<>(
                        topic, partition, null, value);
                Future<RecordMetadata> acknowledgement = sendCalls.call(
                        "sending generated Kafka input record " + id,
                        deadline,
                        () -> producer.send(requested));
                pending.add(new PendingRecord(id, partition, acknowledgement));
            }

            List<ProducedRecord> produced = new ArrayList<>(pending.size());
            for (PendingRecord record : pending) {
                RecordMetadata metadata = record.acknowledgement()
                        .get(deadline.requireRemaining(
                                "awaiting generated Kafka input acknowledgements").toNanos(),
                                TimeUnit.NANOSECONDS);
                if (!topic.equals(metadata.topic()) || record.partition() != metadata.partition()) {
                    throw new IllegalStateException(
                            "Kafka acknowledged a generated record on an unexpected topic partition");
                }
                produced.add(new ProducedRecord(
                        record.id(),
                        metadata.partition(),
                        metadata.offset(),
                        List.of(new HarnessProducerAttempt(
                                1,
                                record.partition(),
                                metadata.partition(),
                                metadata.offset()))));
            }
            return List.copyOf(produced);
        } catch (Exception failure) {
            operationFailure = failure;
            throw failure;
        } finally {
            // Offset capture occurs only after this bounded close completes.
            try {
                closeProducer(producer, deadline);
            } catch (Exception closeFailure) {
                if (operationFailure != null) {
                    operationFailure.addSuppressed(closeFailure);
                } else {
                    throw closeFailure;
                }
            }
        }
    }

    private static void closeProducer(
            InputProducer producer,
            KafkaInputPreparationDeadline deadline) throws Exception {
        try (KafkaInputCallBoundary closeCall = new KafkaInputCallBoundary(
                "kafka-input-producer-close")) {
            closeCall.call(
                    "closing the Kafka input producer",
                    deadline,
                    () -> {
                        producer.close(deadline.remaining());
                        return null;
                    });
        }
    }

    @Override
    public OffsetSnapshot readOffsets(
            String topic,
            int partitions,
            KafkaInputPreparationDeadline deadline) throws Exception {
        Map<TopicPartition, OffsetSpec> beginnings = new LinkedHashMap<>();
        Map<TopicPartition, OffsetSpec> ends = new LinkedHashMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            beginnings.put(topicPartition, OffsetSpec.earliest());
            ends.put(topicPartition, OffsetSpec.latest());
        }
        // Freeze the bounded source cut before checking its origin.
        Map<TopicPartition, Long> endValues = readOffsetValues(ends, "latest", deadline);
        Map<TopicPartition, Long> beginningValues = readOffsetValues(beginnings, "earliest", deadline);

        Map<Integer, Long> beginningOffsets = new TreeMap<>();
        Map<Integer, Long> exclusiveEndOffsets = new TreeMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            beginningOffsets.put(partition, beginningValues.get(topicPartition));
            exclusiveEndOffsets.put(partition, endValues.get(topicPartition));
        }
        return new OffsetSnapshot(
                immutableCopy(beginningOffsets), immutableCopy(exclusiveEndOffsets));
    }

    @Override
    public ReconciliationSnapshot reconcileFromZeroThrough(
            String topic,
            Map<Integer, Long> exclusiveEndOffsets,
            KafkaInputPreparationDeadline deadline) throws Exception {
        Properties configuration = consumerProperties(
                bootstrapServers,
                deadline.requireRemaining("creating the Kafka reconciliation consumer"));
        Consumer<byte[], byte[]> consumer = consumerFactory.create(configuration);
        List<ObservedRecord> observed = new ArrayList<>();
        ReconciliationSnapshot completed = null;
        Exception operationFailure = null;
        try {
            List<TopicPartition> assigned = exclusiveEndOffsets.keySet().stream()
                    .sorted()
                    .map(partition -> new TopicPartition(topic, partition))
                    .toList();
            consumer.assign(assigned);
            assigned.forEach(partition -> consumer.seek(partition, 0));

            boolean reachedEveryEnd = exclusiveEndOffsets.values().stream()
                    .allMatch(offset -> offset == 0);
            while (!reachedEveryEnd) {
                Duration remaining = deadline.requireRemaining(
                        "reconciling generated Kafka input");
                ConsumerRecords<byte[], byte[]> records = consumer.poll(
                        remaining.compareTo(Duration.ofMillis(250)) < 0
                                ? remaining
                                : Duration.ofMillis(250));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    Long end = exclusiveEndOffsets.get(record.partition());
                    if (end == null || record.offset() < end) {
                        observed.add(new ObservedRecord(
                                record.value(), record.partition(), record.offset()));
                    }
                }

                reachedEveryEnd = true;
                for (TopicPartition partition : assigned) {
                    Duration positionTimeout = deadline.requireRemaining(
                            "confirming Kafka reconciliation positions");
                    if (consumer.position(partition, positionTimeout)
                                    < exclusiveEndOffsets.get(partition.partition())) {
                        reachedEveryEnd = false;
                        break;
                    }
                }
            }
            completed = new ReconciliationSnapshot(
                    observed,
                    reachedEveryEnd,
                    configurationEvidence(configuration));
        } catch (Exception failure) {
            // Keep what was observed as partial evidence; never fabricate completeness.
            operationFailure = new ReconciliationSnapshotException(
                    "Kafka input reconciliation failed after observing " + observed.size()
                            + " records",
                    failure,
                    new ReconciliationSnapshot(
                            observed, false, configurationEvidence(configuration)));
            throw operationFailure;
        } finally {
            try {
                consumer.close(deadline.remaining());
            } catch (RuntimeException closeFailure) {
                if (operationFailure != null) {
                    operationFailure.addSuppressed(closeFailure);
                } else if (completed != null) {
                    throw new ReconciliationSnapshotException(
                            "Kafka reconciliation consumer failed to close after its snapshot "
                                    + "was captured",
                            closeFailure,
                            completed);
                } else {
                    throw closeFailure;
                }
            }
        }
        return completed;
    }

    @Override
    public void close(Duration timeout) {
        admin.close(timeout);
    }

    static Properties adminProperties(String bootstrapServers, Duration timeout) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "flink-stability-input-admin");
        properties.put(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                Long.toString(timeout.toMillis()));
        properties.put(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                Long.toString(timeout.toMillis()));
        return properties;
    }

    static Properties producerProperties(String bootstrapServers, Duration timeout) {
        Properties properties = new Properties();
        // Kafka 4.0 exposes no producer allow.auto.create.topics option. The v1 broker disables
        // auto-creation globally, and this client only writes after Admin created exact topics.
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "flink-stability-input-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(timeout.toMillis()));
        properties.put(
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                Long.toString(timeout.toMillis()));
        properties.put(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                Long.toString(requestTimeoutMillis(timeout)));
        return properties;
    }

    static Properties consumerProperties(String bootstrapServers, Duration timeout) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "flink-stability-input-reconciliation");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        properties.put(
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                Long.toString(timeout.toMillis()));
        properties.put(
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                Long.toString(requestTimeoutMillis(timeout)));
        return properties;
    }

    static Map<String, String> topicConfiguration(TopicDefinition topic) {
        return Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG,
                topic.cleanupPolicy().configurationValue(),
                TopicConfig.RETENTION_MS_CONFIG,
                Long.toString(topic.retention().toMillis()));
    }

    static Map<String, String> configurationEvidence(Properties properties) {
        TreeMap<String, String> evidence = new TreeMap<>();
        properties.forEach((key, value) -> evidence.put(
                String.valueOf(key),
                value instanceof Class<?> type ? type.getName() : String.valueOf(value)));
        return Collections.unmodifiableMap(new LinkedHashMap<>(evidence));
    }

    private Map<TopicPartition, Long> readOffsetValues(
            Map<TopicPartition, OffsetSpec> requested,
            String bound,
            KafkaInputPreparationDeadline deadline) throws Exception {
        String operation = "capturing Kafka source " + bound + " offsets";
        try (var calls = new KafkaInputCallBoundary("kafka-input-offsets")) {
            return observeWithRetry(operation, deadline, calls, () -> {
                Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo>
                        values = admin.listOffsets(requested)
                                .all()
                                .get(deadline.requireRemaining(operation).toNanos(),
                                        TimeUnit.NANOSECONDS);
                Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
                values.forEach((partition, result) -> offsets.put(partition, result.offset()));
                return offsets;
            });
        }
    }

    private static Map<Integer, Long> immutableCopy(Map<Integer, Long> offsets) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(offsets));
    }

    private static long requestTimeoutMillis(Duration timeout) {
        return Math.max(1, timeout.toMillis() / 2);
    }

    private record PendingRecord(
            long id, int partition, Future<RecordMetadata> acknowledgement) {}

    @FunctionalInterface
    interface RetryPause {
        void pause(Duration duration) throws InterruptedException;
    }

    private static InputProducer createProducer(Properties properties) {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);
        return new InputProducer() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
                return producer.send(record);
            }

            @Override
            public void close(Duration timeout) {
                producer.close(timeout);
            }
        };
    }

    interface InputProducer {
        Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record);

        void close(Duration timeout);
    }

    @FunctionalInterface
    interface ProducerFactory {
        InputProducer create(Properties configuration);
    }

    @FunctionalInterface
    interface ConsumerFactory {
        Consumer<byte[], byte[]> create(Properties configuration);
    }
}
