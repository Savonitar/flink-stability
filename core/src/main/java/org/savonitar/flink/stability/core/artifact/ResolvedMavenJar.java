package org.savonitar.flink.stability.core.artifact;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** One selected runtime JAR in deterministic breadth-first classpath order. */
record ResolvedMavenJar(
        MavenArtifactIdentity identity,
        int depth,
        int originRootIndex,
        String effectiveScope,
        List<MavenArtifactIdentity> dependencyPath,
        Path sourcePath,
        String sha256) {
    private static final Set<String> RUNTIME_SCOPES = Set.of("compile", "runtime");
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    ResolvedMavenJar {
        Objects.requireNonNull(identity, "identity");
        if (!"jar".equals(identity.extension())) {
            throw new IllegalArgumentException("A resolved runtime artifact must be a JAR");
        }
        if (depth < 0) {
            throw new IllegalArgumentException("depth must be non-negative");
        }
        if (originRootIndex < 0) {
            throw new IllegalArgumentException("originRootIndex must be non-negative");
        }
        effectiveScope = Objects.requireNonNull(effectiveScope, "effectiveScope");
        if (!RUNTIME_SCOPES.contains(effectiveScope)) {
            throw new IllegalArgumentException(
                    "effectiveScope must be compile or runtime, not " + effectiveScope);
        }
        dependencyPath = List.copyOf(Objects.requireNonNull(
                dependencyPath, "dependencyPath"));
        if (dependencyPath.isEmpty()
                || !identity.equals(dependencyPath.get(dependencyPath.size() - 1))) {
            throw new IllegalArgumentException(
                    "dependencyPath must be non-empty and end at identity");
        }
        if (dependencyPath.size() != depth + 1) {
            throw new IllegalArgumentException(
                    "dependencyPath length must equal depth + 1");
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
