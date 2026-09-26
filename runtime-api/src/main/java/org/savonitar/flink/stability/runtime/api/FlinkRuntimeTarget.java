package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.runtime.api.Checks.requireDockerImageId;
import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;
import static org.savonitar.flink.stability.runtime.api.Checks.requireSha256;
/** One effective Flink image and its mandatory verified connector-bundle installation. */
public final class FlinkRuntimeTarget {
    private final String imageReference;
    private final FlinkConnectorBundleInstallation connectorBundle;
    private final Optional<String> expectedImageId;
    private final Optional<RuntimeJar> expectedRuntimeJar;

    private FlinkRuntimeTarget(
            String imageReference,
            FlinkConnectorBundleInstallation connectorBundle,
            Optional<String> expectedImageId,
            Optional<RuntimeJar> expectedRuntimeJar) {
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.connectorBundle = Objects.requireNonNull(connectorBundle, "connectorBundle");
        this.expectedImageId = Objects.requireNonNull(expectedImageId, "expectedImageId");
        this.expectedRuntimeJar = Objects.requireNonNull(expectedRuntimeJar, "expectedRuntimeJar");
    }

    /**
     * v1 execution path, including a valid manifest that may contain zero entries. The supplied
     * image must exactly equal the image committed by the structured target binding.
     */
    public static FlinkRuntimeTarget withConnectorBundle(
            String imageReference,
            FlinkConnectorBundleInstallation connectorBundle) {
        imageReference = requireNonBlank(imageReference, "imageReference");
        connectorBundle = Objects.requireNonNull(connectorBundle, "connectorBundle");
        if (!imageReference.equals(connectorBundle.targetFlinkImageReference())) {
            throw new IllegalArgumentException(
                    "Runtime image must exactly match connector bundle target image: "
                            + imageReference + " versus "
                            + connectorBundle.targetFlinkImageReference());
        }
        return new FlinkRuntimeTarget(imageReference, connectorBundle, Optional.empty(), Optional.empty());
    }

    /** Requires every physical Flink process to use this local Docker image identity. */
    public FlinkRuntimeTarget withExpectedImageId(String imageId) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle,
                Optional.of(requireDockerImageId(imageId, "expectedImageId")), expectedRuntimeJar);
    }

    /** Requires the selected runtime JAR bytes in every physical Flink process. */
    public FlinkRuntimeTarget withExpectedRuntimeJar(RuntimeJar runtimeJar) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId,
                Optional.of(Objects.requireNonNull(runtimeJar, "runtimeJar")));
    }

    public String imageReference() {
        return imageReference;
    }

    public FlinkConnectorBundleInstallation connectorBundle() {
        return connectorBundle;
    }

    public Optional<String> expectedImageId() {
        return expectedImageId;
    }

    public Optional<RuntimeJar> expectedRuntimeJar() {
        return expectedRuntimeJar;
    }

    /** One directly addressed distribution JAR, with no host path or shell interpretation. */
    public record RuntimeJar(String containerPath, String sha256) {
        private static final Pattern PATH = Pattern.compile(
                "/opt/flink/lib/flink-dist-[A-Za-z0-9][A-Za-z0-9._+-]*\\.jar");

        public RuntimeJar {
            containerPath = requireNonBlank(containerPath, "containerPath");
            sha256 = requireSha256(sha256, "Runtime JAR sha256");
            if (!PATH.matcher(containerPath).matches()) {
                throw new IllegalArgumentException(
                        "Runtime JAR path must directly name /opt/flink/lib/flink-dist-<version>.jar"
                                + " using only letters, digits, dots, underscores, pluses, and hyphens");
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof FlinkRuntimeTarget target
                && imageReference.equals(target.imageReference)
                && Objects.equals(connectorBundle, target.connectorBundle)
                && expectedImageId.equals(target.expectedImageId)
                && expectedRuntimeJar.equals(target.expectedRuntimeJar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(imageReference, connectorBundle, expectedImageId, expectedRuntimeJar);
    }

    @Override
    public String toString() {
        return imageReference + " (binding " + connectorBundle.targetBindingSha256()
                + expectedImageId.map(value -> ", expected image " + value).orElse("")
                + expectedRuntimeJar.map(value -> ", runtime JAR " + value).orElse("") + ")";
    }
}
