package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.NetworkSettings;
import com.github.dockerjava.api.model.Ports;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerControl;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.testcontainers.kafka.KafkaContainer;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaPausedEndpointTest {
    private static final int PORT = 39001;
    private static final ExposedPort KAFKA_PORT = ExposedPort.tcp(9092);

    @Test void pauseMayOmitRuntimePortsBetweenMatchingLiveObservations() {
        var fixture = new Fixture();
        assertFalse(fixture.inspect().paused());

        // Docker's paused response omits NetworkSettings.Ports while retaining HostConfig.PortBindings.
        fixture.paused = true;
        fixture.runtimePorts = new Ports();
        var paused = fixture.inspect();
        assertTrue(paused.running());
        assertTrue(paused.paused());

        fixture.paused = false;
        fixture.runtimePorts = runtimePorts(PORT);
        assertFalse(fixture.inspect().paused());
    }

    @Test void pausedOmissionWithoutPriorLiveObservationCannotConfirm() {
        var fixture = new Fixture();
        fixture.paused = true;
        fixture.runtimePorts = new Ports();
        assertMissing(fixture);
    }

    @Test void configuredPortMustHaveBeenObservedWithTheLiveMapping() {
        var fixture = new Fixture();
        fixture.configuredPorts = null;
        assertFalse(fixture.inspect().paused());
        fixture.paused = true;
        fixture.runtimePorts = new Ports();
        fixture.configuredPorts = configuredPorts(PORT);
        assertMissing(fixture);
    }

    @Test void configuredPortMustRemainPresentAndUnchangedDuringPause() {
        for (int variant = 0; variant < 4; variant++) {
            var fixture = new Fixture();
            fixture.inspect();
            fixture.paused = true;
            fixture.runtimePorts = new Ports();
            fixture.configuredPorts = switch (variant) {
                case 0 -> null;
                case 1 -> new Ports();
                case 2 -> configuredPorts(PORT + 1);
                default -> new Ports(KAFKA_PORT, Ports.Binding.bindIpAndPort("127.0.0.1", PORT));
            };
            assertMissing(fixture);
        }
    }

    @Test void contradictoryRuntimePortIsNeverHiddenByMatchingConfiguration() {
        for (boolean paused : List.of(false, true)) {
            var fixture = new Fixture();
            fixture.inspect();
            fixture.paused = paused;
            fixture.runtimePorts = runtimePorts(PORT + 1);
            var failure = assertThrows(IllegalStateException.class, fixture::inspect);
            assertTrue(failure.getMessage().contains("port mismatch"));
            assertTrue(failure.getMessage().contains(Integer.toString(PORT)));
            assertTrue(failure.getMessage().contains(Integer.toString(PORT + 1)));
        }
    }

    @Test void oneMatchingAddressCannotHideAnotherContradictoryBinding() {
        var fixture = new Fixture();
        fixture.inspect();
        fixture.paused = true;
        fixture.runtimePorts = runtimePorts(PORT);
        fixture.runtimePorts.bind(KAFKA_PORT, Ports.Binding.bindIpAndPort("127.0.0.1", PORT + 1));
        assertTrue(assertThrows(IllegalStateException.class, fixture::inspect).getMessage().contains("port mismatch"));
    }

    @Test void missingRuntimePortAfterResumeCannotUseThePausedFallback() {
        var fixture = new Fixture();
        fixture.inspect();
        fixture.paused = true;
        fixture.runtimePorts = new Ports();
        assertTrue(fixture.inspect().paused());
        fixture.paused = false;
        assertMissing(fixture);

        // A failed live observation also invalidates the earlier continuity witness.
        fixture.paused = true;
        assertMissing(fixture);
    }

    @Test void nullAndEmptyPortObservationsRemainFailClosedWhenUnpaused() {
        for (int variant = 0; variant < 3; variant++) {
            var fixture = new Fixture();
            fixture.runtimePorts = variant == 0 ? null : new Ports();
            if (variant == 2) fixture.runtimePorts.getBindings().put(KAFKA_PORT, new Ports.Binding[0]);
            assertMissing(fixture);
        }
    }

    @Test void pausedFallbackCannotBypassContainerImageOrNetworkOwnership() {
        for (int variant = 0; variant < 4; variant++) {
            var fixture = new Fixture();
            fixture.inspect();
            fixture.paused = true;
            fixture.runtimePorts = new Ports();
            switch (variant) {
                case 0 -> fixture.containerId = "replacement";
                case 1 -> fixture.imageId = "different-image";
                case 2 -> fixture.networkId = "different-network";
                default -> fixture.alias = "different-alias";
            }
            assertThrows(IllegalStateException.class, fixture::inspect);
        }
    }

    @Test void processRestartInvalidatesTheEarlierPortObservation() {
        var fixture = new Fixture();
        fixture.inspect();
        fixture.driver.mutate(KafkaBrokerControl.Action.RESTART, deadline());
        fixture.paused = true;
        fixture.runtimePorts = new Ports();
        assertMissing(fixture);
    }

    private static void assertMissing(Fixture fixture) {
        var failure = assertThrows(IllegalStateException.class, fixture::inspect);
        assertTrue(failure.getMessage().contains("port observation missing"), failure.getMessage());
        assertTrue(failure.getMessage().contains("expected 9092/tcp->" + PORT), failure.getMessage());
    }

    private static MonotonicDeadline deadline() {
        return MonotonicDeadline.start(Duration.ofSeconds(2), System::nanoTime);
    }

    private static Ports configuredPorts(int port) {
        return new Ports(KAFKA_PORT, Ports.Binding.bindIpAndPort("", port));
    }

    private static Ports runtimePorts(int port) {
        var ports = new Ports(KAFKA_PORT, Ports.Binding.bindIpAndPort("0.0.0.0", port));
        ports.bind(KAFKA_PORT, Ports.Binding.bindIpAndPort("::", port));
        return ports;
    }

    private static final class Fixture {
        boolean paused;
        String containerId = "owned-container", imageId = "owned-image", networkId = "owned-network";
        String alias = "kafka-main-broker-1";
        Ports runtimePorts = runtimePorts(PORT), configuredPorts = configuredPorts(PORT);
        final DockerClient docker = (DockerClient) Proxy.newProxyInstance(DockerClient.class.getClassLoader(),
                new Class<?>[]{DockerClient.class}, (proxy, method, args) -> {
                    if (!method.getName().equals("inspectContainerCmd") && !method.getName().equals("startContainerCmd"))
                        throw new AssertionError("Unexpected Docker call: " + method.getName());
                    assertEquals("owned-container", args[0]);
                    return Proxy.newProxyInstance(method.getReturnType().getClassLoader(),
                            new Class<?>[]{method.getReturnType()}, (command, commandMethod, commandArgs) -> {
                                if (commandMethod.getName().equals("close")) return null;
                                if (commandMethod.getName().equals("exec"))
                                    return method.getName().equals("inspectContainerCmd") ? response() : null;
                                throw new AssertionError("Unexpected Docker command: " + commandMethod.getName());
                            });
                });
        final KafkaContainer container = new KafkaContainer("apache/kafka:4.0.0") {
            @Override public String getContainerId() { return "owned-container"; }
            @Override public Integer getMappedPort(int port) { assertEquals(9092, port); return PORT; }
            @Override public DockerClient getDockerClient() { return docker; }
        };
        final DockerKafkaBrokerDriver driver = new DockerKafkaBrokerDriver(container,
                new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.threeBrokers(), 3),
                1, "owned-network", () -> true, null, new HashMap<>());

        KafkaBrokerControl.Snapshot inspect() { return driver.inspect(deadline()); }

        InspectContainerResponse response() {
            var response = new InspectContainerResponse();
            var state = response.new ContainerState() {
                @Override public Boolean getRunning() { return true; }
                @Override public Boolean getPaused() { return paused; }
            };
            return new InspectContainerResponse() {
                @Override public String getId() { return containerId; }
                @Override public String getImageId() { return imageId; }
                @Override public ContainerState getState() { return state; }
                @Override public HostConfig getHostConfig() { return new HostConfig().withPortBindings(configuredPorts); }
                @Override public NetworkSettings getNetworkSettings() {
                    return new NetworkSettings() {
                        @Override public Ports getPorts() { return runtimePorts; }
                        @Override public Map<String, ContainerNetwork> getNetworks() {
                            return Map.of("owned", new ContainerNetwork().withNetworkID(networkId).withAliases(alias));
                        }
                    };
                }
            };
        }
    }
}
