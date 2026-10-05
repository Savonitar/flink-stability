package org.savonitar.flink.stability.core.execution.kafka;

import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Direct observations of each owned PLAINTEXT endpoint, without Admin's broker selection. */
public final class KafkaBrokerTopicReadiness {
    public static final int MAX_REQUEST_MILLIS = 1_000;
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(KafkaBrokerTopicReadiness.class);
    // Metadata v4 supports disabling automatic creation and is supported by Kafka 4.0.
    static final short METADATA_VERSION = 4;
    private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

    private KafkaBrokerTopicReadiness() {}

    static void observe(String endpoint, List<KafkaInputOperations.TopicDefinition> topics,
            KafkaInputPreparationDeadline deadline) throws Exception {
        String operation = "observing created Kafka topics "
                + topics.stream().map(KafkaInputOperations.TopicDefinition::name).toList()
                + " at broker " + endpoint;
        validate(request(endpoint, topics.stream().map(KafkaInputOperations.TopicDefinition::name).toList(),
                org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(
                        deadline.requireRemaining(operation), System::nanoTime)), topics);
    }

    /** A stalled endpoint gets at most one second and never the whole remaining observation budget. */
    public static java.time.Duration callTimeout(org.savonitar.flink.stability.runtime.api.MonotonicDeadline deadline)
            throws java.util.concurrent.TimeoutException {
        var remaining = deadline.remainingOrThrow(() -> new java.util.concurrent.TimeoutException("Kafka observation deadline expired"));
        return java.time.Duration.ofNanos(Math.max(1, Math.min(
                java.time.Duration.ofMillis(MAX_REQUEST_MILLIS).toNanos(), remaining.toNanos() / 2)));
    }

    /** Any healthy owned endpoint can supply cluster metadata; never route through Admin's selected node. */
    public static Map<String, org.apache.kafka.clients.admin.TopicDescription> describeTopics(String bootstrap,
            List<String> names, org.savonitar.flink.stability.runtime.api.MonotonicDeadline deadline) throws Exception {
        Exception last = null;
        do {
            for (String endpoint : bootstrap.split(",")) {
                if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Kafka metadata observation interrupted");
                try {
                    var response = request(endpoint.trim(), names, deadline);
                    if (response.topLevelError() != Errors.NONE) throw response.topLevelError().exception();
                    var nodes = response.brokers().stream().collect(Collectors.toMap(
                            org.apache.kafka.common.Node::id, Function.identity()));
                    Function<Integer, org.apache.kafka.common.Node> node = id -> nodes.getOrDefault(
                            id, new org.apache.kafka.common.Node(id, "", -1));
                    var result = new java.util.LinkedHashMap<String, org.apache.kafka.clients.admin.TopicDescription>();
                    for (var topic : response.topicMetadata()) {
                        if (topic.error() != Errors.NONE) throw topic.error().exception();
                        var partitions = new java.util.ArrayList<org.apache.kafka.common.TopicPartitionInfo>();
                        for (var partition : topic.partitionMetadata()) {
                            if (partition.error != Errors.NONE) throw partition.error.exception();
                            partitions.add(new org.apache.kafka.common.TopicPartitionInfo(partition.partition(),
                                    partition.leaderId.map(node).orElse(null),
                                    partition.replicaIds.stream().map(node).toList(), partition.inSyncReplicaIds.stream().map(node).toList()));
                        }
                        if (result.put(topic.topic(), new org.apache.kafka.clients.admin.TopicDescription(
                                topic.topic(), topic.isInternal(), partitions)) != null)
                            throw new IllegalStateException("Duplicate Kafka topic metadata");
                    }
                    if (!result.keySet().equals(new HashSet<>(names)))
                        throw new UnknownTopicOrPartitionException("Incomplete Kafka topic metadata: " + names);
                    deadline.remainingOrThrow(() -> new java.util.concurrent.TimeoutException("Late Kafka metadata response"));
                    return Map.copyOf(result);
                } catch (java.io.IOException | org.apache.kafka.common.errors.RetriableException unavailable) {
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Kafka metadata observation interrupted");
                    LOG.warn("Kafka metadata unavailable target={} topics={} at={}", endpoint, names, java.time.Instant.now(), unavailable);
                    last = unavailable;
                }
            }
            if (!deadline.remaining().isZero()) Thread.sleep(Math.min(100, Math.max(1, deadline.remaining().toMillis())));
        } while (!deadline.remaining().isZero());
        throw new java.util.concurrent.TimeoutException("No Kafka endpoint answered within observation budget: " + last);
    }

    private static MetadataResponse request(String endpoint, List<String> names,
            org.savonitar.flink.stability.runtime.api.MonotonicDeadline outer) throws Exception {
        var start = java.time.Instant.now(); long nanos = System.nanoTime();
        LOG.info("Kafka observation start operation=Metadata target={} topics={} start={}", endpoint, names, start);
        try {
            var deadline = org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(callTimeout(outer), System::nanoTime);
            String host = Utils.getHost(endpoint); Integer port = Utils.getPort(endpoint);
            if (host == null || port == null || port < 1 || port > 65535)
                throw new IllegalArgumentException("Invalid owned Kafka endpoint: " + endpoint);
            var request = new MetadataRequest.Builder(names, false).build(METADATA_VERSION);
            var header = new RequestHeader(ApiKeys.METADATA, METADATA_VERSION, "flink-stability-observation", 1);
            ByteBuffer serialized = request.serializeWithHeader(header);
            // SocketChannel makes blocking I/O interruptible; one deadline covers the entire response, including trickling bytes.
            try (var channel = SocketChannel.open()) {
                Socket socket = channel.socket();
                socket.connect(new InetSocketAddress(host, port), timeoutMillis(deadline));
                var output = new DataOutputStream(socket.getOutputStream());
                output.writeInt(serialized.remaining()); output.write(Utils.toArray(serialized)); output.flush();
                InputStream input = socket.getInputStream();
                int length = ByteBuffer.wrap(readFully(input, socket, 4, deadline)).getInt();
                if (length < 4 || length > MAX_RESPONSE_BYTES) throw new IllegalStateException("Invalid Kafka metadata response length: " + length);
                ByteBuffer body = ByteBuffer.wrap(readFully(input, socket, length, deadline));
                MetadataResponse response = (MetadataResponse) AbstractResponse.parseResponse(body, header);
                deadline.remainingOrThrow(() -> new java.net.SocketTimeoutException("Late Kafka metadata response"));
                LOG.info("Kafka observation end operation=Metadata target={} topics={} start={} end={} elapsedNanos={} outcome=success",
                        endpoint, names, start, java.time.Instant.now(), System.nanoTime() - nanos);
                return response;
            }
        } catch (Exception failure) {
            LOG.warn("Kafka observation end operation=Metadata target={} topics={} start={} end={} elapsedNanos={} outcome=failure",
                    endpoint, names, start, java.time.Instant.now(), System.nanoTime() - nanos, failure);
            throw failure;
        }
    }

    private static byte[] readFully(InputStream input, Socket socket, int length,
            org.savonitar.flink.stability.runtime.api.MonotonicDeadline deadline) throws Exception {
        byte[] result = new byte[length]; int offset = 0;
        while (offset < length) {
            socket.setSoTimeout(timeoutMillis(deadline));
            int read = input.read(result, offset, length - offset);
            if (read < 0) throw new EOFException("Kafka closed its metadata response early");
            offset += read;
        }
        return result;
    }

    private static int timeoutMillis(org.savonitar.flink.stability.runtime.api.MonotonicDeadline deadline)
            throws java.net.SocketTimeoutException {
        return (int) Math.max(1, deadline.remainingOrThrow(
                () -> new java.net.SocketTimeoutException("Kafka metadata request deadline expired")).toMillis());
    }

    static void validate(MetadataResponse response, List<KafkaInputOperations.TopicDefinition> topics) {
        if (response.topLevelError() != Errors.NONE) throw response.topLevelError().exception();
        Map<String, MetadataResponse.TopicMetadata> actual = response.topicMetadata().stream()
                .collect(Collectors.toMap(MetadataResponse.TopicMetadata::topic, Function.identity()));
        for (var expected : topics) {
            var topic = actual.get(expected.name());
            if (topic == null) {
                throw new UnknownTopicOrPartitionException("Broker has not observed topic " + expected.name());
            }
            if (topic.error() != Errors.NONE) throw topic.error().exception();
            if (topic.isInternal() || topic.partitionMetadata().size() != expected.partitions()) {
                throw new IllegalStateException("Kafka topic does not match its declaration: " + expected.name());
            }
            var partitions = new HashSet<Integer>();
            for (var partition : topic.partitionMetadata()) {
                if (partition.error != Errors.NONE) throw partition.error.exception();
                if (!partitions.add(partition.partition()) || partition.partition() < 0
                        || partition.partition() >= expected.partitions()
                        || partition.replicaIds.size() != expected.replicationFactor()) {
                    throw new IllegalStateException("Kafka topic does not match its declaration: " + expected.name());
                }
                if (partition.leaderId.isEmpty() || partition.leaderId.get() < 0) {
                    throw new LeaderNotAvailableException("Broker has no leader for " + partition.topicPartition);
                }
            }
        }
        if (actual.size() != topics.size()) {
            throw new IllegalStateException("Kafka metadata contains unexpected topics");
        }
    }

    @FunctionalInterface
    interface Observer {
        void observe(String endpoint, List<KafkaInputOperations.TopicDefinition> topics,
                KafkaInputPreparationDeadline deadline) throws Exception;
    }
}
