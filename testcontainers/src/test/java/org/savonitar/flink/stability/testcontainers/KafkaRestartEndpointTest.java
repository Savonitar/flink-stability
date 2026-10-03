package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class KafkaRestartEndpointTest {
    @Test void dockerRestartDoesNotRegenerateTestcontainersAdvertisedListener() {
        var node = new ScriptProbe();
        node.initialStart();
        assertTrue(node.script.contains("PLAINTEXT://localhost:39001"));
        // Docker start re-executes the persisted script; it does not call the Java lifecycle hook.
        node.port = 39002;
        assertEquals("localhost:39002", node.getBootstrapServers());
        assertTrue(node.script.contains("PLAINTEXT://localhost:39001"));
        assertFalse(node.script.contains("39002"));
    }

    @Test void everyRestartableBrokerHasAnExplicitDistinctHostPort() {
        var target = new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.threeBrokers(), 3);
        var nodes = ThreeBrokerKafkaRuntime.containers(Network.SHARED, target);
        var bindings = nodes.stream().map(KafkaContainer::getPortBindings).toList();
        assertTrue(bindings.stream().allMatch(value -> value.size() == 1), bindings.toString());
        assertEquals(3, bindings.stream().flatMap(List::stream).distinct().count());
        assertTrue(bindings.stream().flatMap(List::stream).allMatch(value -> value.matches("[1-9][0-9]*:9092(/tcp)?")), bindings.toString());
    }

    private static final class ScriptProbe extends KafkaContainer {
        int port = 39001; String script;
        ScriptProbe() { super("apache/kafka:4.0.0"); }
        @Override public String getHost() { return "localhost"; }
        @Override public Integer getMappedPort(int ignored) { return port; }
        @Override public void copyFileToContainer(Transferable contents, String path) {
            script = new String(contents.getBytes(), StandardCharsets.UTF_8);
        }
        void initialStart() {
            var info = new InspectContainerResponse() {
                @Override public com.github.dockerjava.api.model.ContainerConfig getConfig() {
                    return new com.github.dockerjava.api.model.ContainerConfig().withHostName("broker-1");
                }
            };
            super.containerIsStarting(info);
        }
    }
}
