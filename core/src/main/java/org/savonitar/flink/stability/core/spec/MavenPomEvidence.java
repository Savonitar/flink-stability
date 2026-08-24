package org.savonitar.flink.stability.core.spec;

import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

/** Hash evidence for one POM that Maven Resolver consulted while building a closure. */
public record MavenPomEvidence(
        MavenArtifactIdentity identity,
        Path sourcePath,
        String sha256) {
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    public MavenPomEvidence {
        Objects.requireNonNull(identity, "identity");
        if (!"pom".equals(identity.extension())) {
            throw new IllegalArgumentException("POM evidence identity must use extension pom");
        }
        sourcePath = Objects.requireNonNull(sourcePath, "sourcePath")
                .toAbsolutePath().normalize();
        sha256 = Objects.requireNonNull(sha256, "sha256");
        if (!SHA_256.matcher(sha256).matches()) {
            throw new IllegalArgumentException(
                    "sha256 must be 64 lowercase hexadecimal characters");
        }
    }
}
