package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.exception.NotFoundException;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Objects;

final class TestcontainersContainerHandle implements ContainerHandle {
    private static final Duration KILL_CONFIRMATION_TIMEOUT = Duration.ofSeconds(10);
    private static final long KILL_CONFIRMATION_POLL_MS = 100;

    private final GenericContainer<?> container;

    TestcontainersContainerHandle(GenericContainer<?> container) {
        this.container = Objects.requireNonNull(container, "container");
    }

    @Override
    public void start() {
        container.start();
    }

    @Override
    public void stop() {
        container.stop();
    }

    @Override
    public void kill() {
        String containerId = runtimeId();
        container.getDockerClient().killContainerCmd(containerId).exec();

        long deadline = System.nanoTime() + KILL_CONFIRMATION_TIMEOUT.toNanos();
        while (isRunning() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(KILL_CONFIRMATION_POLL_MS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while confirming container termination " + containerId,
                        exception);
            }
        }
        if (isRunning()) {
            throw new IllegalStateException(
                    "Container remained running after SIGKILL: " + containerId);
        }

        // Remove the terminated physical container. A later logical start creates a fresh one.
        container.stop();
    }

    @Override
    public boolean isRunning() {
        try {
            return container.isRunning();
        } catch (NotFoundException ignored) {
            return false;
        }
    }

    @Override
    public int mappedPort(int containerPort) {
        return container.getMappedPort(containerPort);
    }

    @Override
    public String runtimeId() {
        String id = container.getContainerId();
        if (id == null || id.isBlank()) {
            throw new IllegalStateException("Container has no runtime ID");
        }
        return id;
    }
}
