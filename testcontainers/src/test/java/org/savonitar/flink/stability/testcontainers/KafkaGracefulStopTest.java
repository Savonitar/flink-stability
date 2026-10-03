package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.KillContainerCmd;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.kafka.KafkaContainer;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaGracefulStopTest {
    @Test void productionDriverSendsTermToExactOwnedContainerAndEnablesControlledShutdown() {
        List<String> calls = new ArrayList<>();
        var command = (KillContainerCmd) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{KillContainerCmd.class}, (proxy, method, args) -> {
            if (method.getName().equals("withSignal")) { calls.add("signal:" + args[0]); return proxy; }
            if (method.getName().equals("exec")) { calls.add("exec"); return null; }
            if (method.getName().equals("close")) return null;
            throw new AssertionError(method.getName());
        });
        var docker = (DockerClient) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DockerClient.class}, (proxy, method, args) -> {
            assertEquals("killContainerCmd", method.getName()); calls.add("target:" + args[0]); return command;
        });
        var container = new KafkaContainer("apache/kafka:4.0.0") {
            @Override public String getContainerId() { return "owned-container"; }
            @Override public Integer getMappedPort(int port) { return 39001; }
            @Override public DockerClient getDockerClient() { return docker; }
        };
        var target = new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.threeBrokers(), 3);
        var driver = new DockerKafkaBrokerDriver(container, target, 1, "owned-network", () -> true, null, new HashMap<>());
        driver.mutate(KafkaBrokerControl.Action.STOP, MonotonicDeadline.start(Duration.ofSeconds(2), System::nanoTime));
        assertEquals(List.of("target:owned-container", "signal:TERM", "exec"), calls);
        assertEquals("true", ThreeBrokerKafkaRuntime.environment(target, 1, "cluster").get("KAFKA_CONTROLLED_SHUTDOWN_ENABLE"));
    }
    @Test void legacyPhysicalDriverCannotTurnGracefulStopIntoKill() {
        var driver = new KafkaBrokerFault.Driver() {
            public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline d) { throw new AssertionError(); }
            public void mutate(boolean restart, MonotonicDeadline d) { throw new AssertionError("Must not fall back to kill"); }
            public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> p, MonotonicDeadline d) { return List.of(); }
        };
        assertThrows(UnsupportedOperationException.class, () -> driver.mutate(KafkaBrokerControl.Action.STOP,
                MonotonicDeadline.start(Duration.ofSeconds(1), System::nanoTime)));
    }
}
