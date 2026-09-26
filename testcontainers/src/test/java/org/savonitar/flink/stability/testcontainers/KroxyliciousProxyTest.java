package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.KafkaProxyTarget;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KroxyliciousProxyTest {

    @Test
    void routesEveryConnectionThroughTheFaultFilterToTheClusterListener() {
        String configuration = KroxyliciousProxy.configuration(
                new KafkaProxyTarget("kafka-proxy", "kafka-proxy", 9092, "kafka-main:19092"));

        assertTrue(configuration.contains("bootstrapServers: kafka-main:19092"), configuration);
        assertTrue(configuration.contains("bootstrapAddress: kafka-proxy:9092"), configuration);
        // Brokers are advertised on the ports right after the bootstrap port.
        assertTrue(configuration.contains("nodeStartPort: 9093"), configuration);
        assertTrue(configuration.contains(
                "type: org.savonitar.flink.stability.faultproxy.FaultInjection"), configuration);
        assertTrue(configuration.contains(
                "controlDirectory: " + KroxyliciousProxy.CONTROL_PATH), configuration);
        assertTrue(configuration.contains("defaultFilters:\n  - flink-stability-faults"),
                configuration);
    }

    @Test
    void embedsTheFaultFilterAndPinsTheImageByDigest() {
        assertNotNull(KroxyliciousProxy.class.getClassLoader()
                .getResource(KroxyliciousProxy.FILTER_RESOURCE), "embedded filter JAR");
        assertTrue(KroxyliciousProxy.IMAGE.contains("@sha256:"), KroxyliciousProxy.IMAGE);
    }
}
