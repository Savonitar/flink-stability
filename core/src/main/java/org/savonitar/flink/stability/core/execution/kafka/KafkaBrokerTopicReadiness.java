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
final class KafkaBrokerTopicReadiness {
    // Metadata v4 supports disabling automatic creation and is supported by Kafka 4.0.
    static final short METADATA_VERSION = 4;
    private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

    private KafkaBrokerTopicReadiness() {}

    static void observe(String endpoint, List<KafkaInputOperations.TopicDefinition> topics,
            KafkaInputPreparationDeadline deadline) throws Exception {
        String operation = "observing created Kafka topics "
                + topics.stream().map(KafkaInputOperations.TopicDefinition::name).toList()
                + " at broker " + endpoint;
        String host = Utils.getHost(endpoint);
        Integer port = Utils.getPort(endpoint);
        if (host == null || port == null || port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid owned Kafka endpoint: " + endpoint);
        }
        var request = new MetadataRequest.Builder(
                topics.stream().map(KafkaInputOperations.TopicDefinition::name).toList(), false)
                .build(METADATA_VERSION);
        var header = new RequestHeader(ApiKeys.METADATA, METADATA_VERSION,
                "flink-stability-input-readiness", 1);
        ByteBuffer serialized = request.serializeWithHeader(header);
        // SocketChannel makes blocking socket I/O interruptible when the call boundary expires.
        try (var channel = SocketChannel.open()) {
            Socket socket = channel.socket();
            socket.connect(new InetSocketAddress(host, port), timeoutMillis(deadline, operation));
            var output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(serialized.remaining());
            output.write(Utils.toArray(serialized));
            output.flush();
            InputStream input = socket.getInputStream();
            int length = ByteBuffer.wrap(readFully(input, socket, 4, deadline, operation)).getInt();
            if (length < 4 || length > MAX_RESPONSE_BYTES) {
                throw new IllegalStateException("Invalid Kafka metadata response length: " + length);
            }
            ByteBuffer body = ByteBuffer.wrap(readFully(input, socket, length, deadline, operation));
            MetadataResponse response = (MetadataResponse) AbstractResponse.parseResponse(body, header);
            validate(response, topics);
        }
    }

    private static byte[] readFully(InputStream input, Socket socket, int length,
            KafkaInputPreparationDeadline deadline, String operation) throws Exception {
        byte[] result = new byte[length];
        int offset = 0;
        while (offset < length) {
            socket.setSoTimeout(timeoutMillis(deadline, operation));
            int read = input.read(result, offset, length - offset);
            if (read < 0) throw new EOFException("Kafka closed its metadata response early");
            offset += read;
        }
        return result;
    }

    private static int timeoutMillis(KafkaInputPreparationDeadline deadline, String operation)
            throws KafkaInputPreparationDeadline.KafkaInputPreparationDeadlineExceededException {
        return (int) Math.max(1, Math.min(1_000, deadline.requireRemaining(operation).toMillis()));
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
