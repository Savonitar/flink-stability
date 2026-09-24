package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

/** A declared artifact bound to one immutable local file identity. */
public final class ResolvedArtifact {
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    private final ResolutionScope scope;
    private final ArtifactRole role;
    private final String path;
    private final String declaredReference;
    private final Path sourcePath;
    private final Path preparedPath;
    private final String sha256;

    ResolvedArtifact(
            ResolutionScope scope,
            ArtifactRole role,
            String path,
            String declaredReference,
            Path sourcePath,
            Path preparedPath,
            String sha256) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.role = Objects.requireNonNull(role, "role");
        this.path = Objects.requireNonNull(path, "path");
        this.declaredReference = Objects.requireNonNull(declaredReference, "declaredReference");
        this.sourcePath = Objects.requireNonNull(sourcePath, "sourcePath")
                .toAbsolutePath().normalize();
        this.preparedPath = Objects.requireNonNull(preparedPath, "preparedPath")
                .toAbsolutePath().normalize();
        this.sha256 = Objects.requireNonNull(sha256, "sha256");
        if (!path.startsWith("$/")) {
            throw new IllegalArgumentException("path must be a root-relative JSON pointer");
        }
        if (declaredReference.isBlank()) {
            throw new IllegalArgumentException("declaredReference must not be blank");
        }
        if (!SHA_256.matcher(sha256).matches()) {
            throw new IllegalArgumentException("sha256 must be 64 lowercase hexadecimal characters");
        }
    }

    public ResolutionScope scope() {
        return scope;
    }

    public ArtifactRole role() {
        return role;
    }

    public String path() {
        return path;
    }

    public String declaredReference() {
        return declaredReference;
    }

    /** Canonical local/Maven file selected by the declared reference, for reporting. */
    public Path sourcePath() {
        return sourcePath;
    }

    /** Private snapshot that execution must consume instead of the mutable source. */
    public Path preparedPath() {
        return preparedPath;
    }

    public String sha256() {
        return sha256;
    }
}
