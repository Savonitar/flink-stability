package org.savonitar.flink.stability.core.spec;

import java.nio.file.Path;
import java.util.Objects;

/** Filesystem and network policy for deterministic artifact preparation. */
public record ArtifactResolutionOptions(Path artifactRoot, boolean offline) {
    public ArtifactResolutionOptions {
        artifactRoot = Objects.requireNonNull(artifactRoot, "artifactRoot")
                .toAbsolutePath().normalize();
    }

    public static ArtifactResolutionOptions online(Path artifactRoot) {
        return new ArtifactResolutionOptions(artifactRoot, false);
    }
}
