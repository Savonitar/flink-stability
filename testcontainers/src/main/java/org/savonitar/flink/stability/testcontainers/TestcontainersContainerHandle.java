package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Objects;

final class TestcontainersContainerHandle implements ContainerHandle {
    private final ContainerLifecycle lifecycle;
    private final String logicalName;
    private final FlinkComponentRole role;
    private final FlinkRuntimeTarget runtimeTarget;
    private final VerifiedFlinkContainer verifiedContainer;

    TestcontainersContainerHandle(
            VerifiedFlinkContainer container,
            String logicalName,
            FlinkComponentRole role,
            FlinkRuntimeTarget runtimeTarget) {
        this.verifiedContainer = Objects.requireNonNull(container, "container");
        this.lifecycle = new ContainerLifecycle(container);
        this.logicalName = Objects.requireNonNull(logicalName, "logicalName");
        this.role = Objects.requireNonNull(role, "role");
        this.runtimeTarget = Objects.requireNonNull(runtimeTarget, "runtimeTarget");
    }

    @Override
    public void start() {
        lifecycle.start();
    }

    @Override
    public void startWithin(ContainerOperationDeadline deadline) {
        lifecycle.startWithin(deadline);
    }

    @Override
    public void stop() {
        lifecycle.stop();
    }

    @Override
    public void killAndRemoveWithin(ContainerOperationDeadline deadline) {
        lifecycle.killAndRemoveWithin(deadline);
    }

    @Override
    public void killProcessForWriteFence(ContainerOperationDeadline deadline) {
        lifecycle.killProcessForWriteFence(deadline);
    }

    @Override
    public boolean isRunning() {
        return lifecycle.isRunning();
    }

    @Override
    public boolean isRunningWithin(ContainerOperationDeadline deadline) {
        return lifecycle.isRunningWithin(deadline);
    }

    @Override
    public int mappedPort(int containerPort) {
        return lifecycle.mappedPort(containerPort);
    }

    @Override
    public String runtimeId() {
        return lifecycle.runtimeId();
    }

    @Override
    public FlinkComponentProvisioningEvidence provisioningEvidence() {
        String runtimeId = runtimeId();
        FlinkConnectorBundleInstallation installation = runtimeTarget.connectorBundle();

        VerifiedFlinkContainer.ConnectorBundleVerification verification =
                verifiedContainer.connectorBundleVerification();
        if (verification == null) {
            throw new ConnectorBundleProvisioningException(
                    "Connector bundle has no pre-process verification evidence for "
                            + logicalName);
        }
        return FlinkComponentProvisioningEvidence.verified(
                logicalName,
                role,
                runtimeId,
                runtimeTarget.imageReference(),
                installation.targetBindingSha256(),
                verification.classpathManifestSha256(),
                verification.connectorArtifacts());
    }

    /** Docker lifecycle seam kept independent from Flink provisioning evidence. */
    static final class ContainerLifecycle {
        private static final long KILL_CONFIRMATION_POLL_MS = 100;

        private final GenericContainer<?> container;
        private final Object driverCallLock = new Object();

        ContainerLifecycle(GenericContainer<?> container) {
            this.container = Objects.requireNonNull(container, "container");
        }

        void start() {
            container.start();
        }

        void startWithin(ContainerOperationDeadline deadline) {
            ContainerDriverCallBoundary.run(
                    Objects.requireNonNull(deadline, "deadline"),
                    "starting container",
                    () -> {
                        // Cleanup uses this same lock. If start ignores cancellation and returns
                        // after its caller timed out, cleanup cannot stop the handle before the
                        // late start has completed.
                        synchronized (driverCallLock) {
                            container.start();
                        }
                    });
        }

        void stop() {
            // A timed-out driver call may ignore interruption and continue in its daemon
            // worker. Never issue a concurrent stop against the same GenericContainer; the outer
            // attempt-cleanup deadline bounds this wait and can abandon the physical resource.
            synchronized (driverCallLock) {
                container.stop();
            }
        }

        void killAndRemoveWithin(ContainerOperationDeadline deadline) {
            ContainerOperationDeadline required = Objects.requireNonNull(deadline, "deadline");
            killAndConfirmProcessTerminationWithin(required);
            ContainerDriverCallBoundary.run(
                    required,
                    "removing killed container " + runtimeId(),
                    () -> {
                        synchronized (driverCallLock) {
                            container.stop();
                        }
                    });
        }

        void killProcessForWriteFence(ContainerOperationDeadline deadline) {
            killAndConfirmProcessTerminationWithin(
                    Objects.requireNonNull(deadline, "deadline"));
        }

        boolean isRunning() {
            try {
                return container.isRunning();
            } catch (NotFoundException ignored) {
                return false;
            }
        }

        boolean isRunningWithin(ContainerOperationDeadline deadline) {
            String containerId = runtimeId();
            try {
                InspectContainerResponse inspection = ContainerDriverCallBoundary.call(
                        Objects.requireNonNull(deadline, "deadline"),
                        "inspecting liveness for " + containerId,
                        () -> {
                            synchronized (driverCallLock) {
                                return container.getDockerClient()
                                        .inspectContainerCmd(containerId)
                                        .exec();
                            }
                        });
                if (inspection == null
                        || inspection.getState() == null
                        || inspection.getState().getRunning() == null) {
                    throw new IllegalStateException(
                            "Docker returned incomplete liveness state for " + containerId);
                }
                return inspection.getState().getRunning();
            } catch (NotFoundException ignored) {
                return false;
            }
        }

        int mappedPort(int containerPort) {
            return container.getMappedPort(containerPort);
        }

        String runtimeId() {
            String id = container.getContainerId();
            if (id == null || id.isBlank()) {
                throw new IllegalStateException("Container has no runtime ID");
            }
            return id;
        }

        private void killAndConfirmProcessTerminationWithin(
                ContainerOperationDeadline deadline) {
            String containerId = runtimeId();
            ContainerDriverCallBoundary.run(
                    deadline,
                    "sending SIGKILL to " + containerId,
                    () -> {
                        synchronized (driverCallLock) {
                            container.getDockerClient().killContainerCmd(containerId).exec();
                        }
                    });

            while (isRunningWithin(deadline)) {
                Duration remaining = deadline.remaining(
                        "confirming process termination for " + containerId);
                long sleepNanos = Math.min(
                        Duration.ofMillis(KILL_CONFIRMATION_POLL_MS).toNanos(),
                        remaining.toNanos());
                try {
                    Thread.sleep(
                            Duration.ofNanos(sleepNanos).toMillis(),
                            Duration.ofNanos(sleepNanos).toNanosPart() % 1_000_000);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while confirming container termination " + containerId,
                            exception);
                }
            }
        }
    }
}
