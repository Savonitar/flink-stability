package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.io.StringReader;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void aFullLocalImagePinPermitsAnExplicitNonApacheImage() {
        var target = new KafkaRuntimeTarget("main", "vendor/private:build", policy(), 1,
                Optional.of("sha256:" + "a".repeat(64)), "generic-kraft", Map.of());
        assertEquals("vendor/private:build", target.imageReference());
        assertEquals(1, target.resolvedLaunches().size());
        assertEquals("apache", target.layout());
        assertThrows(IllegalArgumentException.class, () -> new KafkaRuntimeTarget("main", "vendor/private:build",
                policy(), 1, Optional.of("sha256:abc"), "generic-kraft", Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new KafkaRuntimeTarget("main", "vendor/private:build",
                policy(), 1, Optional.empty(), "generic-kraft", Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new KafkaRuntimeTarget("main", "apache/kafka:4.0.0",
                policy(), 1, Optional.empty(), "vendor", Map.of()));
    }

    @Test
    void layoutsResolveAllCommandsAgainstIndependentFixtures() throws Exception {
        for (String layout : List.of("apache", "confluent-platform")) {
            var target = new KafkaRuntimeTarget("main", "vendor/kafka:private", policy(), 1,
                    Optional.of("sha256:" + "a".repeat(64)), "generic-kraft", Map.of("log.retention.ms", "3600000"), layout);
            var fixture = new Properties();
            try (var input = getClass().getResourceAsStream("/kafka-runtime-launch/" + layout + ".properties")) {
                fixture.load(java.util.Objects.requireNonNull(input));
            }
            var launch = target.resolvedLaunches().getFirst();
            assertEquals(fixture.getProperty("layout"), launch.layout());
            assertEquals(List.of("/bin/sh", "-ec", fixture.getProperty("command")), launch.command());
            assertEquals(fixture.getProperty("readiness"), launch.readinessCommand());
            assertEquals("/tmp/kafka-logs", launch.logDirectory());
            assertEquals("3600000", launch.brokerProperties().get("log.retention.ms"));
            assertEquals("INTERNAL://0.0.0.0:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
                    launch.brokerProperties().get("listeners"));
        }
    }

    @Test
    void layoutCannotBeUnknownOrSelectDifferentToolsForTheApacheWrapper() {
        for (String layout : List.of("unknown", "", "Confluent-platform", "/usr/bin")) {
            assertThrows(IllegalArgumentException.class, () -> new KafkaRuntimeTarget("main", "apache/kafka:4.0.0",
                    policy(), 1, Optional.empty(), "generic-kraft", Map.of(), layout));
            assertThrows(IllegalArgumentException.class, () -> KafkaRuntimeLaunch.command(layout));
            assertThrows(IllegalArgumentException.class, () -> KafkaRuntimeLaunch.readinessCommand(layout));
        }
        assertThrows(IllegalArgumentException.class, () -> new KafkaRuntimeTarget("main", "apache/kafka:4.0.0",
                policy(), 1, Optional.empty(), "apache-kafka", Map.of(), "confluent-platform"));
    }

    @Test
    void rejectsEveryOwnedBrokerSettingAndPropertyInjection() {
        for (String key : List.of("listeners", "advertised.listeners", "listener.security.protocol.map",
                "listener.name.internal.ssl.keystore.location", "inter.broker.listener.name", "security.inter.broker.protocol",
                "controller.listener.names", "controller.quorum.voters", "controller.quorum.bootstrap.servers",
                "node.id", "broker.id", "cluster.id", "process.roles", "log.dir", "log.dirs", "metadata.log.dir",
                "auto.create.topics.enable", "controlled.shutdown.enable")) {
            var failure = assertThrows(IllegalArgumentException.class,
                    () -> KafkaRuntimeTarget.validateBrokerConfig(Map.of(key, "x")), key);
            assertTrue(failure.getMessage().contains("runner.kafka.config-reserved-key"), key);
        }
        for (String value : List.of("one\ntwo", "one\rtwo", "one\0two", "one\u0001two", "one\u001btwo", "one\u007ftwo"))
            assertThrows(IllegalArgumentException.class,
                    () -> KafkaRuntimeTarget.validateBrokerConfig(Map.of("message.max.bytes", value)));
        for (String key : List.of("", "TwoWords", "x=y", "x\ny"))
            assertThrows(IllegalArgumentException.class,
                    () -> KafkaRuntimeTarget.validateBrokerConfig(Map.of(key, "x")));
    }

    @Test
    void genericPlansDeclareOneOrThreeNodesAndPreserveOverrideValuesWithoutShellInterpretation() throws Exception {
        for (int nodes : List.of(1, 3)) {
            String value = "\f  value\\literal $(not-a-command) ' quoted=\t";
            var target = new KafkaRuntimeTarget("main", "apache/kafka:4.0.0",
                    nodes == 1 ? policy() : KafkaBrokerPolicy.threeBrokers(), nodes,
                    Optional.empty(), "generic-kraft", Map.of("transaction.two.phase.commit.enable", "false", "custom.property", value));
            assertEquals(nodes, target.resolvedLaunches().size());
            for (int node = 1; node <= nodes; node++) {
                var launch = target.resolvedLaunches().get(node - 1);
                assertEquals(Integer.toString(node), launch.brokerProperties().get("node.id"));
                assertEquals("false", launch.brokerProperties().get("transaction.two.phase.commit.enable"));
                assertEquals("/tmp/kafka-logs", launch.logDirectory());
                assertEquals(nodes, launch.brokerProperties().get("controller.quorum.voters").split(",").length);
                assertEquals(KafkaRuntimeLaunch.COMMAND, launch.command());
                assertTrue(launch.readinessCommand().contains("kafka-broker-api-versions.sh"));
                var properties = new Properties();
                properties.load(new StringReader(launch.environment().get("FLINK_STABILITY_KAFKA_PROPERTIES")));
                assertEquals(value, properties.getProperty("custom.property"));
                assertEquals(launch.brokerProperties(), properties);
                assertThrows(UnsupportedOperationException.class, () -> launch.environment().put("x", "y"));
            }
        }
    }

    @Test
    void apacheEnvironmentKeysMustRoundTripAndCannotSelectWrapperControls() {
        for (String key : List.of("custom..flag", "custom._flag", "custom_-flag", "custom.__flag")) {
            var failure = assertThrows(IllegalArgumentException.class,
                    () -> KafkaRuntimeTarget.validateBrokerConfig(Map.of(key, "value"), "apache-kafka"));
            assertTrue(failure.getMessage().contains("cannot preserve"));
            assertEquals(Map.of(key, "value"), KafkaRuntimeTarget.validateBrokerConfig(Map.of(key, "value"), "generic-kraft"));
        }
        for (String key : List.of("version", "heap.opts", "log4j.opts", "opts", "jmx.opts", "jvm.performance.opts",
                "gc.log.opts", "log4j.root.loglevel", "log4j.loggers", "tools.log4j.loglevel", "jmx.hostname")) {
            assertTrue(KafkaRuntimeTarget.isReservedBrokerConfigKey(key, "apache-kafka"));
            var failure = assertThrows(IllegalArgumentException.class,
                    () -> KafkaRuntimeTarget.validateBrokerConfig(Map.of(key, "value"), "apache-kafka"));
            assertTrue(failure.getMessage().contains("runner.kafka.config-reserved-key"));
            assertEquals(Map.of(key, "value"), KafkaRuntimeTarget.validateBrokerConfig(Map.of(key, "value"), "generic-kraft"));
        }
        var values = KafkaRuntimeTarget.validateBrokerConfig(Map.of("custom_key.with-dash", "literal"), "apache-kafka");
        assertEquals(Map.of("custom_key.with-dash", "literal"), values);
        assertEquals("KAFKA_CUSTOM__KEY_WITH___DASH", KafkaRuntimeTarget.apacheEnvironmentKey("custom_key.with-dash"));
        assertThrows(UnsupportedOperationException.class, () -> values.put("another.key", "value"));
    }

    private static KafkaRuntimeTarget target() {
        return new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", policy());
    }

    private static KafkaBrokerPolicy policy() {
        return KafkaBrokerPolicy.v1SingleBroker();
    }
}
