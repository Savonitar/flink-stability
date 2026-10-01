package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.HostConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.utility.ResourceReaper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Runs the dependency's real creation path with a strict in-memory Docker API. */
class TestcontainersLifecycleOrderTest {
    @TempDir Path directory;

    @Test
    void configuredCopiesPrecedeTheCreatedHookAndItsFailurePreventsStart() throws Exception {
        // Replace only the reaper singleton, preventing its usual Docker discovery/initialization.
        // The base implementation adds labels only; restore the prior instance even on failure.
        synchronized (ResourceReaper.class) {
            var field = ResourceReaper.class.getDeclaredField("instance");
            field.setAccessible(true);
            Object previous = field.get(null);
            var constructor = ResourceReaper.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            field.set(null, constructor.newInstance());
            try {
                Path file = Files.writeString(directory.resolve("prepared.jar"), "prepared-fixture");
                for (boolean reject : List.of(false, true)) {
                    List<String> calls = new ArrayList<>();
                    var container = new FakeContainer(calls, reject);
                    container.withCopyFileToContainer(MountableFile.forHostPath(file), "/fixture/file.jar");
                    container.withCopyToContainer(Transferable.of("manifest"), "/fixture/manifest.json");
                    if (reject) {
                        assertThrows(ContainerLaunchException.class, container::runCreation);
                        assertEquals(List.of("create", "copy:/fixture/file.jar", "copy:/fixture/manifest.json",
                                "created", "cleanup"), calls);
                        assertFalse(calls.contains("start"));
                    } else {
                        assertThrows(ReachedStart.class, container::runCreation);
                        assertEquals(List.of("create", "copy:/fixture/file.jar", "copy:/fixture/manifest.json",
                                "created", "start"), calls);
                    }
                }
            } finally {
                field.set(null, previous);
            }
        }
    }

    private static final class FakeContainer extends GenericContainer<FakeContainer> {
        private final List<String> calls;
        private final boolean reject;

        FakeContainer(List<String> calls, boolean reject) {
            super("fixture.invalid/lifecycle:1");
            this.calls = calls;
            this.reject = reject;
            var response = new CreateContainerResponse();
            response.setId("fixture-container");
            var labels = new HashMap<String, String>();
            var host = new HostConfig();
            CreateContainerCmd create = proxy(CreateContainerCmd.class, (self, method, args) -> {
                if (method.getName().equals("exec")) { calls.add("create"); return response; }
                if (method.getName().equals("getLabels")) return labels;
                if (method.getName().equals("getHostConfig")) return host;
                if (method.getName().equals("getNetworkMode")) return "none";
                if (method.getName().startsWith("with")) return self;
                throw new AssertionError("Unexpected create command method: " + method.getName());
            });
            StartContainerCmd start = proxy(StartContainerCmd.class, (self, method, args) -> {
                if (method.getName().equals("exec")) {
                    calls.add("start");
                    // Stop precisely at the boundary under test; never reach wait/inspect or a real client.
                    throw new ReachedStart();
                }
                throw new AssertionError("Unexpected start command method: " + method.getName());
            });
            this.dockerClient = proxy(DockerClient.class, (self, method, args) -> switch (method.getName()) {
                case "createContainerCmd" -> create;
                case "startContainerCmd" -> start;
                default -> throw new AssertionError("Unexpected Docker client method: " + method.getName());
            });
        }

        void runCreation() { super.doStart(); }
        @Override public String getDockerImageName() { return "fixture.invalid/lifecycle:1"; }
        @Override public void copyFileToContainer(MountableFile source, String target) { calls.add("copy:" + target); }
        @Override public void copyFileToContainer(Transferable source, String target) { calls.add("copy:" + target); }
        @Override protected void containerIsCreated(String id) {
            assertEquals("fixture-container", id);
            assertEquals(List.of("create", "copy:/fixture/file.jar", "copy:/fixture/manifest.json"), calls);
            calls.add("created");
            if (reject) throw new IllegalStateException("fixture pre-start verification failed");
        }
        @Override public String getLogs() { return ""; }
        @Override public void stop() { calls.add("cleanup"); }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static final class ReachedStart extends Error {}
}
