package org.savonitar.flink.stability.runtime.api;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.savonitar.flink.stability.runtime.api.Checks.requireDockerImageId;
import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;
import static org.savonitar.flink.stability.runtime.api.Checks.requireSha256;

/** Immutable evidence for one successfully started physical Flink container. */
public final class FlinkComponentProvisioningEvidence {
    private final String logicalName;
    private final FlinkComponentRole role;
    private final String runtimeId;
    private final String imageReference;
    private final String imageId;
    private final String targetBindingSha256;
    private final String classpathManifestSha256;
    private final List<ProvisionedConnectorArtifact> connectorArtifacts;
    private final Optional<RuntimeJarEvidence> runtimeJarEvidence;

    private FlinkComponentProvisioningEvidence(
            String logicalName,
            FlinkComponentRole role,
            String runtimeId,
            String imageReference,
            String imageId,
            String targetBindingSha256,
            String classpathManifestSha256,
            List<ProvisionedConnectorArtifact> connectorArtifacts,
            Optional<RuntimeJarEvidence> runtimeJarEvidence) {
        this.logicalName = requireNonBlank(logicalName, "logicalName");
        this.role = Objects.requireNonNull(role, "role");
        this.runtimeId = requireNonBlank(runtimeId, "runtimeId");
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.imageId = requireDockerImageId(imageId, "imageId");
        this.targetBindingSha256 = requireSha256(
                targetBindingSha256, "targetBindingSha256");
        this.classpathManifestSha256 = requireSha256(
                classpathManifestSha256, "classpathManifestSha256");
        this.connectorArtifacts = List.copyOf(Objects.requireNonNull(
                connectorArtifacts, "connectorArtifacts"));
        this.runtimeJarEvidence = Objects.requireNonNull(runtimeJarEvidence, "runtimeJarEvidence");
    }

    public static FlinkComponentProvisioningEvidence verified(
            String logicalName,
            FlinkComponentRole role,
            String runtimeId,
            String imageReference,
            String imageId,
            String targetBindingSha256,
            String classpathManifestSha256,
            List<ProvisionedConnectorArtifact> connectorArtifacts) {
        return new FlinkComponentProvisioningEvidence(
                logicalName, role, runtimeId, imageReference, imageId,
                requireNonBlank(targetBindingSha256, "targetBindingSha256"),
                requireNonBlank(classpathManifestSha256, "classpathManifestSha256"),
                connectorArtifacts, Optional.empty());
    }

    /** Attaches bytes read from this container and its explicitly registered JVM log key. */
    public FlinkComponentProvisioningEvidence withRuntimeJarEvidence(
            FlinkRuntimeTarget.RuntimeJar jar, String classLoadProcess) {
        return new FlinkComponentProvisioningEvidence(logicalName, role, runtimeId, imageReference,
                imageId, targetBindingSha256, classpathManifestSha256, connectorArtifacts,
                Optional.of(new RuntimeJarEvidence(jar, classLoadProcess)));
    }

    public Optional<RuntimeJarEvidence> runtimeJarEvidence() {
        return runtimeJarEvidence;
    }

    public record RuntimeJarEvidence(FlinkRuntimeTarget.RuntimeJar jar, String classLoadProcess) {
        public RuntimeJarEvidence {
            Objects.requireNonNull(jar, "jar");
            classLoadProcess = requireNonBlank(classLoadProcess, "classLoadProcess");
        }
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

    public String imageId() {
        return imageId;
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
