package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Keeps the Apache entrypoint contract and verifies opt-in image pins before process start. */
final class VerifiedKafkaContainer extends KafkaContainer {
    private final KafkaRuntimeTarget target;
    private final int node;
    private volatile org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence.Container receipt;

    VerifiedKafkaContainer(KafkaRuntimeTarget target) { this(target, 1); }

    VerifiedKafkaContainer(KafkaRuntimeTarget target, int node) {
        super(DockerImageName.parse(target.imageReference()).asCompatibleSubstituteFor("apache/kafka"));
        this.target = target;
        this.node = node;
    }

    @Override protected void containerIsCreated(String containerId) {
        super.containerIsCreated(containerId);
        if (target.customConfiguration()) {
            retainCreated(containerId, null, false);
            try (var inspect = getDockerClient().inspectContainerCmd(containerId)) {
                String actual = inspect.exec().getImageId();
                retainCreated(containerId, actual, false);
                KafkaImageIdentity.verify(target, actual);
                retainCreated(containerId, actual, true);
            }
        }
    }

    private void retainCreated(String containerId, String imageId, boolean verified) {
        receipt = new org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence.Container(
                target.brokerAlias(node), containerId, imageId, getEnvMap(), java.util.List.of(getCommandParts()),
                KafkaRuntimeTarget.LOG_DIRECTORY, verified, false);
    }

    @Override public void copyFileToContainer(org.testcontainers.images.builder.Transferable transferable, String path) {
        if (target.customConfiguration() && path.equals("/tmp/testcontainers_start.sh") && receipt != null)
            receipt = receipt.withStartupScript(new String(transferable.getBytes(), java.nio.charset.StandardCharsets.UTF_8));
        super.copyFileToContainer(transferable, path);
    }

    void markReady() { if (receipt != null) receipt = receipt.started(); }

    java.util.Optional<org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence.Container> retainedEvidence() {
        return java.util.Optional.ofNullable(receipt);
    }
}
