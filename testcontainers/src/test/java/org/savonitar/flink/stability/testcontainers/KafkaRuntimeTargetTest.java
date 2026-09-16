package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.containers.Network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaRuntimeTargetTest {

    @Test
    void configuresApacheContainerWithoutContactingDocker() {
        ApacheKafkaRuntime runtime = new ApacheKafkaRuntime(Network.SHARED, target());

        assertEquals("apache/kafka:4.0.0", runtime.configuredImageReference());
        assertEquals("kafka-main", runtime.configuredNetworkAlias());
        assertEquals("kafka-main:19092", runtime.configuredInternalListener());
        assertEquals(
                "false",
                runtime.configuredEnvironment()
                        .get("KAFKA_AUTO_CREATE_TOPICS_ENABLE"));
        assertEquals(
                "7200000",
                runtime.configuredEnvironment()
                        .get("KAFKA_TRANSACTION_MAX_TIMEOUT_MS"));
        assertEquals(
                "1",
                runtime.configuredEnvironment()
                        .get("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"));
        assertEquals(
                "1",
                runtime.configuredEnvironment()
                        .get("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"));
        assertEquals(
                "1",
                runtime.configuredEnvironment()
                        .get("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"));
        assertEquals(
                "0",
                runtime.configuredEnvironment()
                        .get("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> runtime.configuredEnvironment().put("x", "y"));
    }

    private static KafkaRuntimeTarget target() {
        return new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", policy());
    }

    private static KafkaBrokerPolicy policy() {
        return KafkaBrokerPolicy.v1SingleBroker();
    }
}
