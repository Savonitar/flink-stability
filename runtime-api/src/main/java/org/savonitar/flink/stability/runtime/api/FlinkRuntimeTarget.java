package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;
import java.util.Optional;

import static org.savonitar.flink.stability.runtime.api.Checks.requireDockerImageId;
import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;
/** One effective Flink image and its mandatory verified connector-bundle installation. */
public final class FlinkRuntimeTarget {
    private final String imageReference;
    private final FlinkConnectorBundleInstallation connectorBundle;
    private final Optional<String> expectedImageId;

    private FlinkRuntimeTarget(
            String imageReference,
            FlinkConnectorBundleInstallation connectorBundle,
            Optional<String> expectedImageId) {
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.connectorBundle = Objects.requireNonNull(connectorBundle, "connectorBundle");
        this.expectedImageId = Objects.requireNonNull(expectedImageId, "expectedImageId");
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
        return new FlinkRuntimeTarget(imageReference, connectorBundle, Optional.empty());
    }

    /** Requires every physical Flink process to use this local Docker image identity. */
    public FlinkRuntimeTarget withExpectedImageId(String imageId) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle,
                Optional.of(requireDockerImageId(imageId, "expectedImageId")));
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

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof FlinkRuntimeTarget target
                && imageReference.equals(target.imageReference)
                && Objects.equals(connectorBundle, target.connectorBundle)
                && expectedImageId.equals(target.expectedImageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(imageReference, connectorBundle, expectedImageId);
    }

    @Override
    public String toString() {
        return imageReference + " (binding " + connectorBundle.targetBindingSha256()
                + expectedImageId.map(value -> ", expected image " + value).orElse("") + ")";
    }
}
