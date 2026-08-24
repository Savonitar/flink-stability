package org.savonitar.flink.stability.testcontainers;

import java.util.Objects;
import java.util.Optional;

/** One effective Flink image and its optional v1 connector-bundle installation. */
public final class FlinkRuntimeTarget {
    private final String imageReference;
    private final FlinkConnectorBundleInstallation connectorBundle;

    private FlinkRuntimeTarget(
            String imageReference,
            FlinkConnectorBundleInstallation connectorBundle) {
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.connectorBundle = connectorBundle;
    }

    /** Legacy execution path: no manifest or connector JAR is injected. */
    public static FlinkRuntimeTarget legacy(String imageReference) {
        return new FlinkRuntimeTarget(imageReference, null);
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

    public Optional<FlinkConnectorBundleInstallation> connectorBundle() {
        return Optional.ofNullable(connectorBundle);
    }

    public boolean connectorBundleEnabled() {
        return connectorBundle != null;
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
        return connectorBundle == null
                ? imageReference + " (legacy)"
                : imageReference + " (binding " + connectorBundle.targetBindingSha256() + ")";
    }

    private static String requireNonBlank(String value, String name) {
        value = Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
