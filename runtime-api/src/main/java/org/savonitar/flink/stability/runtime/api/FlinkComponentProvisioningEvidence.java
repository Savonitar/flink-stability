package org.savonitar.flink.stability.runtime.api;

import java.util.List;
import java.util.Map;
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
    private final List<ImageConnectorArtifact> imageConnectorArtifacts;
    private final Map<String, String> flinkConfig;
    private final Optional<String> classLoadProcess;
    private final Optional<EffectiveConfigurationEvidence> effectiveConfiguration;

    private FlinkComponentProvisioningEvidence(
            String logicalName,
            FlinkComponentRole role,
            String runtimeId,
            String imageReference,
            String imageId,
            String targetBindingSha256,
            String classpathManifestSha256,
            List<ProvisionedConnectorArtifact> connectorArtifacts,
            Optional<RuntimeJarEvidence> runtimeJarEvidence,
            List<ImageConnectorArtifact> imageConnectorArtifacts, Map<String, String> flinkConfig,
            Optional<String> classLoadProcess, Optional<EffectiveConfigurationEvidence> effectiveConfiguration) {
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
        this.imageConnectorArtifacts = List.copyOf(imageConnectorArtifacts);
        this.flinkConfig = FlinkConfiguration.validate(flinkConfig);
        this.classLoadProcess = Objects.requireNonNull(classLoadProcess, "classLoadProcess");
        this.effectiveConfiguration = Objects.requireNonNull(effectiveConfiguration, "effectiveConfiguration");
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
                connectorArtifacts, Optional.empty(), List.of(), Map.of(), Optional.empty(), Optional.empty());
    }

    /** Attaches bytes read from this container and its explicitly registered JVM log key. */
    public FlinkComponentProvisioningEvidence withRuntimeJarEvidence(
            FlinkRuntimeTarget.RuntimeJar jar, String classLoadProcess) {
        return new FlinkComponentProvisioningEvidence(logicalName, role, runtimeId, imageReference,
                imageId, targetBindingSha256, classpathManifestSha256, connectorArtifacts,
                Optional.of(new RuntimeJarEvidence(jar, classLoadProcess)), imageConnectorArtifacts, flinkConfig,
                this.classLoadProcess, effectiveConfiguration);
    }

    /** Actual parsed startup configuration and registered log key for this physical incarnation. */
    public FlinkComponentProvisioningEvidence withProcessConfiguration(
            Map<String, String> config, String process) {
        return new FlinkComponentProvisioningEvidence(logicalName, role, runtimeId, imageReference,
                imageId, targetBindingSha256, classpathManifestSha256, connectorArtifacts,
                runtimeJarEvidence, imageConnectorArtifacts, config, Optional.of(requireNonBlank(process, "process")),
                effectiveConfiguration);
    }

    public FlinkComponentProvisioningEvidence withImageConnectorArtifacts(List<ImageConnectorArtifact> artifacts) {
        return new FlinkComponentProvisioningEvidence(logicalName, role, runtimeId, imageReference,
                imageId, targetBindingSha256, classpathManifestSha256, connectorArtifacts,
                runtimeJarEvidence, artifacts, flinkConfig, classLoadProcess, effectiveConfiguration);
    }

    public FlinkComponentProvisioningEvidence withEffectiveConfiguration(EffectiveConfigurationEvidence receipt) {
        return new FlinkComponentProvisioningEvidence(logicalName, role, runtimeId, imageReference,
                imageId, targetBindingSha256, classpathManifestSha256, connectorArtifacts,
                runtimeJarEvidence, imageConnectorArtifacts, receipt.observedValues(), classLoadProcess,
                Optional.of(receipt));
    }

    public Optional<EffectiveConfigurationEvidence> effectiveConfiguration() { return effectiveConfiguration; }

    /** sourceSha256 hashes the merged file verified before start; observedSha256 hashes it after startup. */
    public record EffectiveConfigurationEvidence(String path, String sourceSha256, String observedSha256,
                                                  List<String> launcher, Map<String, String> observedValues) {
        public EffectiveConfigurationEvidence {
            path = requireNonBlank(path, "path");
            sourceSha256 = requireSha256(sourceSha256, "sourceSha256");
            observedSha256 = requireSha256(observedSha256, "observedSha256");
            launcher = List.copyOf(launcher);
            if (launcher.isEmpty()) throw new IllegalArgumentException("Effective configuration requires a launcher");
            launcher.forEach(value -> requireNonBlank(value, "launcher argument"));
            observedValues = FlinkConfiguration.validate(observedValues);
        }
    }

    public List<ImageConnectorArtifact> imageConnectorArtifacts() { return imageConnectorArtifacts; }
    public Map<String, String> flinkConfig() { return flinkConfig; }
    public Optional<String> classLoadProcess() { return classLoadProcess; }

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
