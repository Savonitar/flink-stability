package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;

import java.util.Optional;

/** Minimal lifecycle surface used by the cluster registry. */
interface ContainerHandle {
    void start();

    /** Starts within one enclosing component-action deadline. */
    void startWithin(ContainerOperationDeadline deadline);

    void stop();

    /** SIGKILLs, confirms termination, and removes the container within one deadline. */
    void killAndRemoveWithin(ContainerOperationDeadline deadline);

    /**
     * SIGKILLs and confirms process termination within the shared fence deadline while retaining
     * the physical handle for later cleanup.
     */
    void killProcessForWriteFence(ContainerOperationDeadline deadline);

    /** Reports process liveness within the enclosing operation deadline. */
    boolean isRunningWithin(ContainerOperationDeadline deadline);

    boolean isRunning();

    int mappedPort(int containerPort);

    String runtimeId();

    FlinkComponentProvisioningEvidence provisioningEvidence();

    default Optional<TaskManagerControl.Identity> taskManagerIdentity() {
        return Optional.empty();
    }

    default String advertisedAlias() {
        throw new UnsupportedOperationException("Container has no advertised Flink identity");
    }

    default FlinkHaControl.ProcessState processState(ContainerOperationDeadline deadline) {
        throw new UnsupportedOperationException("Detailed process state is unsupported");
    }

    default void pauseWithin(ContainerOperationDeadline deadline) {
        throw new UnsupportedOperationException("Process pause is unsupported");
    }

    default void resumeWithin(ContainerOperationDeadline deadline) {
        throw new UnsupportedOperationException("Process resume is unsupported");
    }
}
