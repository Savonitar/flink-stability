package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;
/** One effective Flink image and its mandatory verified connector-bundle installation. */
public final class FlinkRuntimeTarget {
    private final String imageReference;
    private final FlinkConnectorBundleInstallation connectorBundle;

    private FlinkRuntimeTarget(
            String imageReference,
            FlinkConnectorBundleInstallation connectorBundle) {
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.connectorBundle = Objects.requireNonNull(connectorBundle, "connectorBundle");
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
        return new FlinkRuntimeTarget(imageReference, connectorBundle);
    }

    public String imageReference() {
        return imageReference;
    }

    public FlinkConnectorBundleInstallation connectorBundle() {
        return connectorBundle;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof FlinkRuntimeTarget target
                && imageReference.equals(target.imageReference)
                && Objects.equals(connectorBundle, target.connectorBundle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(imageReference, connectorBundle);
    }

    @Override
    public String toString() {
        return imageReference + " (binding " + connectorBundle.targetBindingSha256() + ")";
    }
}
