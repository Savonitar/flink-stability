package org.savonitar.flink.stability.testcontainers;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Immutable evidence for one successfully started physical Flink container. */
public final class FlinkComponentProvisioningEvidence {
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");
    private final String logicalName;
    private final FlinkComponentRole role;
    private final String runtimeId;
    private final String imageReference;
    private final String targetBindingSha256;
    private final String classpathManifestSha256;
    private final List<ProvisionedConnectorArtifact> connectorArtifacts;
    private final boolean verifiedBeforeProcessStart;

    private FlinkComponentProvisioningEvidence(
            String logicalName,
            FlinkComponentRole role,
            String runtimeId,
            String imageReference,
            String targetBindingSha256,
            String classpathManifestSha256,
            List<ProvisionedConnectorArtifact> connectorArtifacts,
            boolean verifiedBeforeProcessStart) {
        this.logicalName = requireNonBlank(logicalName, "logicalName");
        this.role = Objects.requireNonNull(role, "role");
        this.runtimeId = requireNonBlank(runtimeId, "runtimeId");
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.targetBindingSha256 = targetBindingSha256;
        this.classpathManifestSha256 = classpathManifestSha256;
        this.connectorArtifacts = List.copyOf(Objects.requireNonNull(
                connectorArtifacts, "connectorArtifacts"));
        this.verifiedBeforeProcessStart = verifiedBeforeProcessStart;
        if ((targetBindingSha256 == null) != (classpathManifestSha256 == null)) {
            throw new IllegalArgumentException(
                    "Target binding and classpath manifest identities must be present together");
        }
        if (targetBindingSha256 != null
                && (!SHA_256.matcher(targetBindingSha256).matches()
                || !SHA_256.matcher(classpathManifestSha256).matches())) {
            throw new IllegalArgumentException(
                    "Provisioning identities must be 64 lowercase hexadecimal characters");
        }
        if (targetBindingSha256 == null
                && (!this.connectorArtifacts.isEmpty() || verifiedBeforeProcessStart)) {
            throw new IllegalArgumentException(
                    "Legacy evidence cannot contain connector provisioning observations");
        }
        if (targetBindingSha256 != null && !verifiedBeforeProcessStart) {
            throw new IllegalArgumentException(
                    "Connector provisioning evidence must be verified before process start");
        }
    }

    public static FlinkComponentProvisioningEvidence legacy(
            String logicalName,
            FlinkComponentRole role,
            String runtimeId,
            String imageReference) {
        return new FlinkComponentProvisioningEvidence(
                logicalName, role, runtimeId, imageReference,
                null, null, List.of(), false);
    }

    public static FlinkComponentProvisioningEvidence verified(
            String logicalName,
            FlinkComponentRole role,
            String runtimeId,
            String imageReference,
            String targetBindingSha256,
            String classpathManifestSha256,
            List<ProvisionedConnectorArtifact> connectorArtifacts) {
        return new FlinkComponentProvisioningEvidence(
                logicalName, role, runtimeId, imageReference,
                requireNonBlank(targetBindingSha256, "targetBindingSha256"),
                requireNonBlank(classpathManifestSha256, "classpathManifestSha256"),
                connectorArtifacts,
                true);
    }

    public String logicalName() {
        return logicalName;
    }

    public FlinkComponentRole role() {
        return role;
    }

    public String runtimeId() {
        return runtimeId;
    }

    public String imageReference() {
        return imageReference;
    }

    public Optional<String> targetBindingSha256() {
        return Optional.ofNullable(targetBindingSha256);
    }

    public Optional<String> classpathManifestSha256() {
        return Optional.ofNullable(classpathManifestSha256);
    }

    public List<ProvisionedConnectorArtifact> connectorArtifacts() {
        return connectorArtifacts;
    }

    public boolean verifiedBeforeProcessStart() {
        return verifiedBeforeProcessStart;
    }

    private static String requireNonBlank(String value, String name) {
        value = Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
