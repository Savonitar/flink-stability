package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Keeps the Apache entrypoint contract and verifies opt-in image pins before process start. */
final class VerifiedKafkaContainer extends KafkaContainer {
    // KafkaLaunchContractTest checks this starter contract against Testcontainers 1.21.4.
    private static final String STARTER_PATH = "/tmp/testcontainers_start.sh";
    private static final String VENDOR_EXECUTABLE = "/etc/kafka/docker/run";
    private static final String VENDOR_START_SUFFIX = "\n" + VENDOR_EXECUTABLE + " \n";

    private final KafkaRuntimeTarget target;
    private final int node;
    private volatile org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence.Container receipt;

    VerifiedKafkaContainer(KafkaRuntimeTarget target) { this(target, 1); }

    VerifiedKafkaContainer(KafkaRuntimeTarget target, int node) {
        super(DockerImageName.parse(target.imageReference()).asCompatibleSubstituteFor("apache/kafka"));
        this.target = target;
        this.node = node;
        // Preserve the image entrypoint and replace the waiting shell when Kafka is ready to launch.
        withCommand("sh", "-c", "while [ ! -f " + STARTER_PATH + " ]; do sleep 0.1; done; exec " + STARTER_PATH);
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
        super.copyFileToContainer(prepareTransfer(transferable, path), path);
    }

    /** Preserve Testcontainers' listener setup and file mode, then replace its final shell boundary. */
    org.testcontainers.images.builder.Transferable prepareTransfer(
            org.testcontainers.images.builder.Transferable transferable, String path) {
        if (!path.equals(STARTER_PATH)) return transferable;
        byte[] originalBytes = transferable.getBytes();
        String starter = new String(originalBytes, java.nio.charset.StandardCharsets.UTF_8);
        boolean expectedSuffix = starter.endsWith(VENDOR_START_SUFFIX);
        int occurrences = 0;
        for (int match = starter.indexOf(VENDOR_START_SUFFIX); match >= 0;
                match = starter.indexOf(VENDOR_START_SUFFIX, match + 1)) occurrences++;
        if (!expectedSuffix || occurrences != 1)
            throw new IllegalStateException("Unexpected pinned Testcontainers Kafka starter script: byteLength="
                    + originalBytes.length + ", expectedSuffix=" + expectedSuffix + ", vendorStartOccurrences=" + occurrences);
        String corrected = starter.substring(0, starter.length() - VENDOR_START_SUFFIX.length())
                + "\nexec " + VENDOR_EXECUTABLE + " \n";
        var prepared = org.testcontainers.images.builder.Transferable.of(
                corrected.getBytes(java.nio.charset.StandardCharsets.UTF_8), transferable.getFileMode());
        if (target.customConfiguration() && receipt != null)
            receipt = receipt.withStartupScript(corrected);
        return prepared;
    }

    void markReady() { if (receipt != null) receipt = receipt.started(); }

    java.util.Optional<org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence.Container> retainedEvidence() {
        return java.util.Optional.ofNullable(receipt);
    }
}
