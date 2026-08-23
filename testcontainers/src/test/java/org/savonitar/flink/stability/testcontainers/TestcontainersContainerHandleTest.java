package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.KillContainerCmd;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestcontainersContainerHandleTest {

    @Test
    void killUsesDockerKillThenRemovesTheExitedContainerWithoutStartingAReplacement() {
        RecordingGenericContainer container = new RecordingGenericContainer();
        TestcontainersContainerHandle handle = new TestcontainersContainerHandle(container);

        handle.kill();

        assertEquals(List.of("kill:physical-1", "stop"), container.events);
        assertFalse(container.running);
    }

    @Test
    void explicitStartAndGracefulStopRemainSeparateOperations() {
        RecordingGenericContainer container = new RecordingGenericContainer();
        container.running = false;
        TestcontainersContainerHandle handle = new TestcontainersContainerHandle(container);

        handle.start();
        assertTrue(container.running);
        handle.stop();

        assertEquals(List.of("start", "stop"), container.events);
        assertFalse(container.running);
    }

    private static final class RecordingGenericContainer
            extends GenericContainer<RecordingGenericContainer> {
        private final List<String> events = new ArrayList<>();
        private final DockerClient dockerClient;
        private boolean running = true;

        private RecordingGenericContainer() {
            super(DockerImageName.parse("alpine:3.20"));
            KillContainerCmd killCommand = (KillContainerCmd) Proxy.newProxyInstance(
                    KillContainerCmd.class.getClassLoader(),
                    new Class<?>[]{KillContainerCmd.class},
                    (proxy, method, args) -> {
                        if ("exec".equals(method.getName())) {
                            events.add("kill:physical-1");
                            running = false;
                            return null;
                        }
                        if (method.getReturnType().isInstance(proxy)) {
                            return proxy;
                        }
                        return defaultValue(method.getReturnType());
                    });
            dockerClient = (DockerClient) Proxy.newProxyInstance(
                    DockerClient.class.getClassLoader(),
                    new Class<?>[]{DockerClient.class},
                    (proxy, method, args) -> {
                        if ("killContainerCmd".equals(method.getName())) {
                            assertEquals("physical-1", args[0]);
                            return killCommand;
                        }
                        return defaultValue(method.getReturnType());
                    });
        }

        @Override
        public void start() {
            events.add("start");
            running = true;
        }

        @Override
        public void stop() {
            events.add("stop");
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public String getContainerId() {
            return "physical-1";
        }

        @Override
        public DockerClient getDockerClient() {
            return dockerClient;
        }

        private static Object defaultValue(Class<?> type) {
            if (!type.isPrimitive()) {
                return null;
            }
            if (type == boolean.class) {
                return false;
            }
            if (type == char.class) {
                return '\0';
            }
            if (type == byte.class) {
                return (byte) 0;
            }
            if (type == short.class) {
                return (short) 0;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            if (type == float.class) {
                return 0F;
            }
            return 0D;
        }
    }
}
