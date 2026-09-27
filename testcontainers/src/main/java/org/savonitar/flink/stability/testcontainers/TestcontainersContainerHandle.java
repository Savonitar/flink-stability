package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

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
    public String advertisedAlias() {
        return verifiedContainer.advertisedAlias();
    }

    @Override
    public FlinkHaControl.ProcessState processState(ContainerOperationDeadline deadline) {
        return lifecycle.processState(deadline);
    }

    @Override
    public void pauseWithin(ContainerOperationDeadline deadline) {
        lifecycle.pauseWithin(deadline);
    }

    @Override
    public void resumeWithin(ContainerOperationDeadline deadline) {
        lifecycle.resumeWithin(deadline);
    }

    @Override
    public Optional<TaskManagerControl.Identity> taskManagerIdentity() {
        if (role != FlinkComponentRole.TASK_MANAGER || !isRunning()) {
            return Optional.empty();
        }
        return Optional.of(new TaskManagerControl.Identity(
                logicalName, runtimeId(), verifiedContainer.resourceId()));
    }

    @Override
    public FlinkComponentProvisioningEvidence provisioningEvidence() {
        String runtimeId = runtimeId();
        verifiedContainer.verifyTokenPluginAfterStart(runtimeId);
        FlinkConnectorBundleInstallation installation = runtimeTarget.connectorBundle();

        VerifiedFlinkContainer.ConnectorBundleVerification verification =
                verifiedContainer.connectorBundleVerification();
        if (verification == null) {
            throw new ConnectorBundleProvisioningException(
                    "Connector bundle has no pre-process verification evidence for "
                            + logicalName);
        }
        FlinkComponentProvisioningEvidence evidence = FlinkComponentProvisioningEvidence.verified(
                logicalName,
                role,
                runtimeId,
                runtimeTarget.imageReference(),
                verifiedContainer.verifiedImageId(),
                installation.targetBindingSha256(),
                verification.classpathManifestSha256(),
                verification.connectorArtifacts());
        return verifiedContainer.runtimeJarEvidence(runtimeId)
                .map(jar -> evidence.withRuntimeJarEvidence(jar.jar(), jar.classLoadProcess()))
                .orElse(evidence);
    }

    /** Docker lifecycle seam kept independent from Flink provisioning evidence. */
    static final class ContainerLifecycle {
        private static final long KILL_CONFIRMATION_POLL_MS = 100;

        private final GenericContainer<?> container;
        private final Object driverCallLock = new Object();
        private volatile boolean pauseAttempted;

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
                if (pauseAttempted && container.isRunning()) {
                    InspectContainerResponse inspection = container.getDockerClient()
                            .inspectContainerCmd(runtimeId()).exec();
                    if (Boolean.TRUE.equals(inspection.getState().getPaused())) {
                        container.getDockerClient().unpauseContainerCmd(runtimeId()).exec();
                    }
                    pauseAttempted = false;
                }
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

        FlinkHaControl.ProcessState processState(ContainerOperationDeadline deadline) {
            String id = runtimeId();
            InspectContainerResponse inspection = ContainerDriverCallBoundary.call(
                    deadline, "inspecting process state for " + id, () -> {
                        synchronized (driverCallLock) {
                            return container.getDockerClient().inspectContainerCmd(id).exec();
                        }
                    });
            if (inspection == null || inspection.getState() == null
                    || inspection.getState().getRunning() == null
                    || inspection.getState().getPaused() == null) {
                throw new IllegalStateException("Docker returned incomplete process state for " + id);
            }
            return new FlinkHaControl.ProcessState(id, inspection.getState().getRunning(),
                    inspection.getState().getPaused());
        }

        void pauseWithin(ContainerOperationDeadline deadline) {
            String id = runtimeId();
            pauseAttempted = true;
            ContainerDriverCallBoundary.run(deadline, "pausing " + id, () -> {
                synchronized (driverCallLock) {
                    container.getDockerClient().pauseContainerCmd(id).exec();
                }
            });
            FlinkHaControl.ProcessState state = processState(deadline);
            if (!state.running() || !state.paused()) {
                throw new IllegalStateException("Docker did not confirm a paused live process: " + id);
            }
        }

        void resumeWithin(ContainerOperationDeadline deadline) {
            if (!processState(deadline).paused()) {
                pauseAttempted = false;
                return;
            }
            String id = runtimeId();
            ContainerDriverCallBoundary.run(deadline, "resuming " + id, () -> {
                synchronized (driverCallLock) {
                    container.getDockerClient().unpauseContainerCmd(id).exec();
                }
            });
            FlinkHaControl.ProcessState state = processState(deadline);
            if (!state.running() || state.paused()) {
                throw new IllegalStateException("Docker did not confirm a resumed live process: " + id);
            }
            pauseAttempted = false;
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
            if (pauseAttempted) {
                resumeWithin(deadline);
            }
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
