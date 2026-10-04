package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CustomKafkaRuntimeTest {
    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);

    @Test void verifiesFullImageIdentityAndRejectsMissingOrMismatchedPinsBeforeStarting() {
        var target = target("generic-kraft");
        assertDoesNotThrow(() -> KafkaImageIdentity.verify(target, IMAGE_ID));
        assertThrows(IllegalStateException.class, () -> KafkaImageIdentity.verify(target, null));
        assertThrows(IllegalStateException.class, () -> KafkaImageIdentity.verify(target, "sha256:abc"));
        assertThrows(IllegalStateException.class, () -> KafkaImageIdentity.verify(target, "sha256:" + "b".repeat(64)));
    }

    @Test void apacheLauncherRetainsExplicitOverridesAndPinCompatibleCustomImageWithoutDocker() {
        var runtime = new ApacheKafkaRuntime(Network.SHARED, target("apache-kafka"));
        assertEquals("vendor/kafka:private", runtime.configuredImageReference());
        assertEquals("false", runtime.configuredEnvironment().get("KAFKA_TRANSACTION_TWO_PHASE_COMMIT_ENABLE"));
        assertEquals("KAFKA_CUSTOM__KEY_WITH___DASH", ApacheKafkaRuntime.environmentKey("custom_key.with-dash"));
    }

    @Test void genericContainerHasExplicitCommandAndUnobservedEvidenceCannotBeFabricated() {
        var container = new GenericKraftKafkaContainer(Network.SHARED, target("generic-kraft"), 1, 29092);
        assertArrayEquals(new String[] {org.savonitar.flink.stability.runtime.api.KafkaRuntimeLaunch.COMMAND.getLast()},
                container.getCommandParts(), "The complete shell script must remain one -ec argument");
        assertThrows(IllegalStateException.class, container::evidence);
        assertEquals("29092:9092/tcp", container.getPortBindings().getFirst());
    }

    @Test void genericLogLayoutFailureIsExplicitAndRetainsItsOriginalReceipt() {
        var inventory = new org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Inventory(
                new org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Partition("output", 0),
                "/tmp/kafka-logs/output-0", "IO_FAILURE", java.util.List.of(), true,
                java.nio.file.Path.of("inventory.bin"), "a".repeat(64), "container", IMAGE_ID, "network", 1);
        var capture = new org.savonitar.flink.stability.runtime.api.KafkaLogCapture(
                java.util.List.of(inventory), java.util.List.of(), 7, 3, java.util.List.of("find failed"));
        var result = GenericKraftKafkaRuntime.logCaptureCapability(capture);
        assertEquals(capture.inventories(), result.inventories());
        assertEquals(7, result.retainedBytes());
        assertEquals("find failed", result.diagnostics().getFirst());
        assertTrue(result.diagnostics().getLast().startsWith("kafka.log-capture.unsupported:"));
    }

    @Test void emptyGenericCaptureAlwaysExplainsUnsupportedPhysicalEvidence() {
        for (var inventories : java.util.List.of(java.util.List.<org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Inventory>of(),
                java.util.List.of(new org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Inventory(
                        new org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Partition("output", 0),
                        "/tmp/kafka-logs/output-0", "OBSERVED", java.util.List.of(), true,
                        java.nio.file.Path.of("inventory.bin"), "a".repeat(64), "container", IMAGE_ID, "network", 1)))) {
            var capture = new org.savonitar.flink.stability.runtime.api.KafkaLogCapture(inventories,
                    java.util.List.of(), 0, 1, java.util.List.of());
            var result = GenericKraftKafkaRuntime.logCaptureCapability(capture);
            assertTrue(result.diagnostics().getLast().startsWith("kafka.log-capture.unsupported:"));
            assertEquals(inventories, result.inventories());
        }
    }

    @Test void failedPrestartPinCheckRetainsExactPartialReceiptWithoutClaimingReadiness() {
        var target = target("generic-kraft");
        var container = new GenericKraftKafkaContainer(Network.SHARED, target, 1, 29092);
        container.configureLaunch("localhost");
        String actual = "sha256:" + "b".repeat(64);
        assertThrows(IllegalStateException.class, () -> container.verifyCreated("1".repeat(64), actual));
        var runtime = new GenericKraftKafkaRuntime(Network.SHARED, target, java.util.List.of(container));
        var receipt = runtime.runtimeEvidence().orElseThrow().containers().getFirst();
        assertEquals("1".repeat(64), receipt.containerId());
        assertEquals(actual, receipt.imageId());
        assertFalse(receipt.imageVerified());
        assertFalse(receipt.ready());
        assertFalse(runtime.runtimeEvidence().orElseThrow().confirms(target));
        assertTrue(receipt.environment().get("FLINK_STABILITY_KAFKA_PROPERTIES").contains("EXTERNAL://localhost:29092"));
        assertThrows(IllegalArgumentException.class, container::markReady);
        container.verifyCreated("1".repeat(64), IMAGE_ID);
        assertTrue(container.evidence().imageVerified());
        assertFalse(container.evidence().ready());
        container.markReady();
        assertTrue(container.evidence().ready());
        assertTrue(runtime.runtimeEvidence().orElseThrow().confirms(target));
    }

    @Test void apacheWrapperPropertiesKeepLiteralWhitespaceBackslashesAndSeparators() throws Exception {
        String value = "\f \tleading \\t \\u0020 quoted' shell$(literal)=value:ending\\";
        var target = new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.v1SingleBroker(), 1,
                Optional.empty(), "apache-kafka", Map.of("custom_key.with-dash", value));
        var environment = ApacheKafkaRuntime.brokerEnvironment(target);
        // Apache's wrapper appends decodedKey + '=' + envValue directly to server.properties.
        String key = ApacheKafkaRuntime.environmentKey("custom_key.with-dash").substring("KAFKA_".length())
                .toLowerCase(java.util.Locale.ROOT).replace('_', '.').replace("...", "-").replace("..", "_");
        var properties = new java.util.Properties();
        properties.load(new java.io.StringReader(key + "=" + environment.get(ApacheKafkaRuntime.environmentKey("custom_key.with-dash"))));
        assertEquals("custom_key.with-dash", key);
        assertEquals(value, properties.getProperty(key));
        assertEquals("7200000", environment.get("KAFKA_TRANSACTION_MAX_TIMEOUT_MS"));
    }

    private static KafkaRuntimeTarget target(String launch) {
        return new KafkaRuntimeTarget("main", "vendor/kafka:private", KafkaBrokerPolicy.v1SingleBroker(), 1,
                Optional.of(IMAGE_ID), launch, Map.of("transaction.two.phase.commit.enable", "false"));
    }
}
