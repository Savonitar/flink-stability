package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerConfig;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class KafkaLaunchContractTest {
    private static final String STARTER = "/tmp/testcontainers_start.sh";
    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);

    @Test void pinnedTestcontainersStarterKeepsListenerSetupAndModeWithTwoExplicitExecBoundaries() {
        Render pinned = new Render();
        pinned.render();
        for (boolean custom : List.of(false, true)) {
            var target = target(custom, 1);
            var container = new VerifiedKafkaContainer(target);
            assertArrayEquals(new String[]{"sh", "-c", "while [ ! -f /tmp/testcontainers_start.sh ]; do sleep 0.1; done; exec /tmp/testcontainers_start.sh"}, container.getCommandParts());
            var prepared = container.prepareTransfer(pinned.script, STARTER);
            String original = text(pinned.script), actual = text(prepared);
            assertTrue(original.contains("export KAFKA_ADVERTISED_LISTENERS="));
            assertTrue(original.contains("PLAINTEXT://localhost:39092"));
            assertTrue(original.contains("BROKER://broker-1:9093"));
            assertEquals(original.replace("\n/etc/kafka/docker/run \n", "\nexec /etc/kafka/docker/run \n"), actual);
            assertEquals(pinned.script.getFileMode(), prepared.getFileMode());
            assertTrue(actual.endsWith("\nexec /etc/kafka/docker/run \n"));
        }
    }

    @Test void customReceiptContainsTransferredBytesAndOtherFilesAreUnchanged() throws Exception {
        var target = target(true, 1);
        var container = new VerifiedKafkaContainer(target);
        Field receipt = VerifiedKafkaContainer.class.getDeclaredField("receipt");
        receipt.setAccessible(true);
        receipt.set(container, new KafkaRuntimeEvidence.Container(target.brokerAlias(1), "owned", IMAGE_ID,
                Map.of(), List.of(container.getCommandParts()), KafkaRuntimeTarget.LOG_DIRECTORY, true, false));
        var source = Transferable.of("#!/bin/bash\nexport KAFKA_ADVERTISED_LISTENERS=one,two\n/etc/kafka/docker/run \n", 0750);
        var prepared = container.prepareTransfer(source, STARTER);
        assertEquals(0750, prepared.getFileMode());
        assertEquals(text(prepared), container.retainedEvidence().orElseThrow().startupScript());
        var unrelated = Transferable.of("unchanged", 0640);
        assertSame(unrelated, container.prepareTransfer(unrelated, "/tmp/other"));
        assertThrows(IllegalStateException.class, () -> container.prepareTransfer(Transferable.of("unexpected", 0755), STARTER));
    }

    @Test void productionSingleAndThreeBrokerFactoriesShareTheCorrectedApacheLauncher() throws Exception {
        for (boolean custom : List.of(false, true)) {
            var target = target(custom, 1);
            var runtime = new ApacheKafkaRuntime(Network.SHARED, target);
            Field field = ApacheKafkaRuntime.class.getDeclaredField("container");field.setAccessible(true);
            assertCorrected((KafkaContainer) field.get(runtime));
            for (var broker : ThreeBrokerKafkaRuntime.containers(Network.SHARED, target(custom, 3))) assertCorrected(broker);
        }
    }
    private static void assertCorrected(KafkaContainer broker) {
        assertInstanceOf(VerifiedKafkaContainer.class, broker);
        assertTrue(broker.getCommandParts()[2].endsWith("; exec " + STARTER));
    }
    private static KafkaRuntimeTarget target(boolean custom, int brokers) {
        return new KafkaRuntimeTarget("main", "apache/kafka:4.0.0",
                brokers == 1 ? KafkaBrokerPolicy.v1SingleBroker() : KafkaBrokerPolicy.threeBrokers(), brokers,
                custom ? Optional.of(IMAGE_ID) : Optional.empty(), "apache-kafka",
                custom ? Map.of("log.retention.ms", "3600000") : Map.of());
    }
    private static String text(Transferable value) { return new String(value.getBytes(), StandardCharsets.UTF_8); }
    private static final class Render extends KafkaContainer {
        Transferable script;
        Render() { super("apache/kafka:4.0.0");withListener("kafka-main:19092"); }
        @Override public String getHost() { return "localhost"; }
        @Override public Integer getMappedPort(int port) { return 39092; }
        @Override public void copyFileToContainer(Transferable value, String destination) { assertEquals(STARTER, destination);script = value; }
        void render() {
            configure();
            containerIsStarting(new InspectContainerResponse() {
                @Override public ContainerConfig getConfig() { return new ContainerConfig().withHostName("broker-1"); }
            });
        }
    }
}
