package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;

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
}
