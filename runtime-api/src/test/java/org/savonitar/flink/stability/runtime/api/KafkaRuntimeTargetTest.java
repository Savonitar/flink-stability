package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaRuntimeTargetTest {

    @Test
    void derivesDeterministicInternalEndpointAndExactBrokerConfiguration() {
        KafkaRuntimeTarget target = target();

        assertEquals("kafka-main", target.networkAlias());
        assertEquals("kafka-main:19092", target.internalBootstrapServers());
        assertEquals(
                Map.of(
                        "group.initial.rebalance.delay.ms", "0",
                        "offsets.topic.replication.factor", "1",
                        "transaction.max.timeout.ms", "7200000",
                        "transaction.state.log.min.isr", "1",
                        "transaction.state.log.replication.factor", "1"),
                target.brokerPolicy().kafkaConfiguration());
        assertThrows(
                UnsupportedOperationException.class,
                () -> target.brokerPolicy().kafkaConfiguration().put("x", "y"));
    }

    @Test
    void rejectsNonOfficialOrNonPatchKafkaImagesAndInvalidAliases() {
        KafkaBrokerPolicy policy = policy();

        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaRuntimeTarget("main", "confluentinc/cp-kafka:4.0.1", policy));
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaRuntimeTarget("main", "apache/kafka:4.1.0", policy));
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaRuntimeTarget("main", "apache/kafka:4.0", policy));
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaRuntimeTarget("Main", "apache/kafka:4.0.0", policy));
    }

    @Test
    void acceptsAnExactKafka40TagPinnedBySha256Digest() {
        String image = "apache/kafka:4.0.17@sha256:" + "a".repeat(64);

        assertEquals(image, new KafkaRuntimeTarget("main", image, policy()).imageReference());
    }

    @Test
    void rejectsAnyRelaxationOfTheV1SingleBrokerPolicy() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaBrokerPolicy(Duration.ofHours(1), 1, 1, 1, Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaBrokerPolicy(Duration.ofHours(2), 2, 1, 1, Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaBrokerPolicy(
                        Duration.ofHours(2), 1, 1, 1, Duration.ofMillis(1)));
    }

    private static KafkaRuntimeTarget target() {
        return new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", policy());
    }

    private static KafkaBrokerPolicy policy() {
        return KafkaBrokerPolicy.v1SingleBroker();
    }
}
