package org.savonitar.flink.stability.testcontainers;

import java.util.Optional;

/** Minimal lifecycle surface used by the cluster registry. */
interface ContainerHandle {
    void start();

    void stop();

    void kill();

    boolean isRunning();

    int mappedPort(int containerPort);

    String runtimeId();

    default Optional<FlinkComponentProvisioningEvidence> provisioningEvidence() {
        return Optional.empty();
    }
}
