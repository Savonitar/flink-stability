package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;
import java.util.regex.Pattern;

/** Exact image, logical identity, and broker policy for one v1 Kafka runtime. */
public record KafkaRuntimeTarget(
        String clusterAlias,
        String imageReference,
        KafkaBrokerPolicy brokerPolicy, int brokers) {

    public static final int INTERNAL_LISTENER_PORT = 19_092;

    private static final Pattern CLUSTER_ALIAS =
            Pattern.compile("^[a-z0-9]+(?:-[a-z0-9]+)*$");
    private static final Pattern OFFICIAL_KAFKA_4_0_IMAGE =
            Pattern.compile("^apache/kafka:4\\.0\\.(?:0|[1-9][0-9]*)"
                    + "(?:@sha256:[0-9a-f]{64})?$");

    public KafkaRuntimeTarget(String clusterAlias, String imageReference, KafkaBrokerPolicy policy) {
        this(clusterAlias, imageReference, policy, 1);
    }

    public String brokerAlias(int ordinal) {
        if (ordinal < 1 || ordinal > brokers) throw new IllegalArgumentException("Unknown broker ordinal");
        return brokers == 1 ? networkAlias() : networkAlias() + "-broker-" + ordinal;
    }

    public KafkaRuntimeTarget {
        Objects.requireNonNull(clusterAlias, "clusterAlias");
        Objects.requireNonNull(imageReference, "imageReference");
        Objects.requireNonNull(brokerPolicy, "brokerPolicy");
        if ((brokers != 1 && brokers != 3) || brokerPolicy.transactionStateLogReplicationFactor() != brokers)
            throw new IllegalArgumentException("Kafka topology and replication policy must agree (one or three brokers)");
        if (!CLUSTER_ALIAS.matcher(clusterAlias).matches()) {
            throw new IllegalArgumentException(
                    "Kafka cluster alias must be lower-kebab-case: " + clusterAlias);
        }
        if (!OFFICIAL_KAFKA_4_0_IMAGE.matcher(imageReference).matches()) {
            throw new IllegalArgumentException(
                    "The v1 runtime requires an exact apache/kafka:4.0.<patch> image: "
                            + imageReference);
        }
    }

    public String networkAlias() {
        return "kafka-" + clusterAlias;
    }

    public String internalBootstrapServers() {
        return java.util.stream.IntStream.rangeClosed(1, brokers).mapToObj(i -> brokerAlias(i) + ":" + INTERNAL_LISTENER_PORT)
                .collect(java.util.stream.Collectors.joining(","));
    }
}
