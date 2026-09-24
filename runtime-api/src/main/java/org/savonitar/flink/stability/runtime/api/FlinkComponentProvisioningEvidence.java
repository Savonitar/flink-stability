package org.savonitar.flink.stability.runtime.api;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

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

    private FlinkComponentProvisioningEvidence(
            String logicalName,
            FlinkComponentRole role,
            String runtimeId,
            String imageReference,
            String targetBindingSha256,
            String classpathManifestSha256,
            List<ProvisionedConnectorArtifact> connectorArtifacts) {
        this.logicalName = requireNonBlank(logicalName, "logicalName");
        this.role = Objects.requireNonNull(role, "role");
        this.runtimeId = requireNonBlank(runtimeId, "runtimeId");
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.targetBindingSha256 = Objects.requireNonNull(
                targetBindingSha256, "targetBindingSha256");
        this.classpathManifestSha256 = Objects.requireNonNull(
                classpathManifestSha256, "classpathManifestSha256");
        this.connectorArtifacts = List.copyOf(Objects.requireNonNull(
                connectorArtifacts, "connectorArtifacts"));
        if (!SHA_256.matcher(targetBindingSha256).matches()
                || !SHA_256.matcher(classpathManifestSha256).matches()) {
            throw new IllegalArgumentException(
                    "Provisioning identities must be 64 lowercase hexadecimal characters");
        }
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
                connectorArtifacts);
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

    public String targetBindingSha256() {
        return targetBindingSha256;
    }

    public String classpathManifestSha256() {
        return classpathManifestSha256;
    }

    public List<ProvisionedConnectorArtifact> connectorArtifacts() {
        return connectorArtifacts;
    }
}
