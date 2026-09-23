package org.savonitar.flink.stability.core.artifact;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/** One target-specific closure entry and the declaration selected for that scenario side. */
public final class ConnectorClosureLockEntry {
    private final PreparedConnectorArtifact artifact;
    private final PreparedConnectorOrigin effectiveOrigin;

    ConnectorClosureLockEntry(
            PreparedConnectorArtifact artifact,
            PreparedConnectorOrigin effectiveOrigin) {
        this.artifact = Objects.requireNonNull(artifact, "artifact");
        this.effectiveOrigin = Objects.requireNonNull(effectiveOrigin, "effectiveOrigin");
        if (!artifact.origins().contains(effectiveOrigin)) {
            throw new IllegalArgumentException(
                    "The effective origin must belong to the prepared classpath entry");
        }
        if (artifact.primary() != effectiveOrigin.primaryRoot()) {
            if (artifact.primary()) {
                throw new IllegalArgumentException(
                        "A primary classpath entry requires a primary declaration origin");
            }
            // AUTO dependencies intentionally originate from the connector primary.
            if (artifact.mavenIdentity().isEmpty()
                    || effectiveOrigin.rootKind() != PreparedConnectorOrigin.RootKind.PRIMARY) {
                throw new IllegalArgumentException(
                        "Only an AUTO Maven dependency may use a primary-root origin");
            }
        }
    }

    public PreparedConnectorArtifact artifact() {
        return artifact;
    }

    public PreparedConnectorOrigin effectiveOrigin() {
        return effectiveOrigin;
    }

    public int classpathIndex() {
        return artifact.classpathIndex();
    }

    public boolean primary() {
        return artifact.primary();
    }

    public Path stagedPath() {
        return artifact.preparedPath();
    }

    public String sha256() {
        return artifact.sha256();
    }

    public Optional<MavenArtifactIdentity> mavenIdentity() {
        return artifact.mavenIdentity();
    }
}
