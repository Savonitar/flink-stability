package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Pattern;

/** One immutable connector classpath entry prepared for execution. */
public final class PreparedConnectorArtifact {
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    private final ResolutionScope scope;
    private final int classpathIndex;
    private final boolean primary;
    private final int originRootIndex;
    private final int dependencyDepth;
    private final String declarationPath;
    private final String sourceReference;
    private final List<PreparedConnectorOrigin> origins;
    private final MavenArtifactIdentity mavenIdentity;
    private final List<MavenArtifactIdentity> mavenDependencyPath;
    private final Path sourcePath;
    private final Path preparedPath;
    private final String sha256;

    PreparedConnectorArtifact(
            ResolutionScope scope,
            int classpathIndex,
            boolean primary,
            int originRootIndex,
            int dependencyDepth,
            String declarationPath,
            String sourceReference,
            MavenArtifactIdentity mavenIdentity,
            List<MavenArtifactIdentity> mavenDependencyPath,
            Path sourcePath,
            Path preparedPath,
            String sha256) {
        this(
                scope,
                classpathIndex,
                primary,
                originRootIndex,
                dependencyDepth,
                declarationPath,
                sourceReference,
                List.of(new PreparedConnectorOrigin(
                        scope,
                        primary
                                ? PreparedConnectorOrigin.RootKind.PRIMARY
                                : PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                        primary ? -1 : originRootIndex,
                        declarationPath,
                        sourceReference)),
                mavenIdentity,
                mavenDependencyPath,
                sourcePath,
                preparedPath,
                sha256);
    }

    PreparedConnectorArtifact(
            ResolutionScope scope,
            int classpathIndex,
            boolean primary,
            int originRootIndex,
            int dependencyDepth,
            String declarationPath,
            String sourceReference,
            List<PreparedConnectorOrigin> origins,
            MavenArtifactIdentity mavenIdentity,
            List<MavenArtifactIdentity> mavenDependencyPath,
            Path sourcePath,
            Path preparedPath,
            String sha256) {
        this.scope = Objects.requireNonNull(scope, "scope");
        if (classpathIndex < 0) {
            throw new IllegalArgumentException("classpathIndex must be non-negative");
        }
        this.classpathIndex = classpathIndex;
        this.primary = primary;
        if (primary) {
            if (classpathIndex != 0 || originRootIndex != -1 || dependencyDepth != -1) {
                throw new IllegalArgumentException(
                        "The primary must be classpath index zero without a dependency origin");
            }
        } else if (classpathIndex == 0 || originRootIndex < 0 || dependencyDepth < 0) {
            throw new IllegalArgumentException(
                    "A dependency needs a positive classpath index and non-negative origin/depth");
        }
        this.originRootIndex = originRootIndex;
        this.dependencyDepth = dependencyDepth;
        this.declarationPath = requirePointer(declarationPath, "declarationPath");
        this.sourceReference = requireNonBlank(sourceReference, "sourceReference");
        this.origins = List.copyOf(Objects.requireNonNull(origins, "origins"));
        if (this.origins.isEmpty()) {
            throw new IllegalArgumentException("A connector classpath entry needs an origin");
        }
        for (PreparedConnectorOrigin origin : this.origins) {
            if (primary && !origin.primaryRoot()) {
                throw new IllegalArgumentException(
                        "A connector primary must originate from its primary declaration");
            }
            if (!primary
                    && origin == this.origins.getFirst()
                    && !origin.primaryRoot()
                    && origin.declarationIndex() != originRootIndex) {
                throw new IllegalArgumentException(
                        "The first origin must name the retained dependency root index");
            }
            if (scope != ResolutionScope.COMMON && origin.scope() != scope) {
                throw new IllegalArgumentException(
                        "A side-scoped classpath entry must have origins from that side");
            }
            if (scope == ResolutionScope.SINGLE && origin.scope() != ResolutionScope.SINGLE) {
                throw new IllegalArgumentException(
                        "A single-scenario classpath entry needs a single-scenario origin");
            }
        }
        if (scope == ResolutionScope.COMMON) {
            boolean hasCommon = this.origins.stream()
                    .anyMatch(origin -> origin.scope() == ResolutionScope.COMMON);
            boolean hasSingle = this.origins.stream()
                    .anyMatch(origin -> origin.scope() == ResolutionScope.SINGLE);
            if (hasSingle || (hasCommon && this.origins.stream()
                    .anyMatch(origin -> origin.scope() != ResolutionScope.COMMON))) {
                throw new IllegalArgumentException(
                        "Common entries use either common origins or experiment-side origins");
            }
            if (!hasCommon
                    && (this.origins.stream().noneMatch(
                            origin -> origin.scope() == ResolutionScope.BASELINE)
                    || this.origins.stream().noneMatch(
                            origin -> origin.scope() == ResolutionScope.CANDIDATE))) {
                throw new IllegalArgumentException(
                        "Shared experiment entries need baseline and candidate origins");
            }
        }
        this.mavenIdentity = mavenIdentity;
        if (mavenIdentity != null && !"jar".equals(mavenIdentity.extension())) {
            throw new IllegalArgumentException(
                    "Connector classpath Maven identities must use extension jar");
        }
        this.mavenDependencyPath = List.copyOf(Objects.requireNonNull(
                mavenDependencyPath, "mavenDependencyPath"));
        if (mavenIdentity == null && !this.mavenDependencyPath.isEmpty()) {
            throw new IllegalArgumentException(
                    "Only Maven entries may carry a Maven dependency path");
        }
        if (mavenIdentity != null && primary
                && !this.mavenDependencyPath.equals(List.of(mavenIdentity))) {
            throw new IllegalArgumentException(
                    "A Maven primary path must contain exactly its own identity");
        }
        if (mavenIdentity != null && !primary) {
            if (this.mavenDependencyPath.size() != dependencyDepth + 1
                    || !mavenIdentity.equals(this.mavenDependencyPath.getLast())) {
                throw new IllegalArgumentException(
                        "Maven dependency path must match the dependency depth and identity");
            }
        }
        this.sourcePath = Objects.requireNonNull(sourcePath, "sourcePath")
                .toAbsolutePath().normalize();
        this.preparedPath = Objects.requireNonNull(preparedPath, "preparedPath")
                .toAbsolutePath().normalize();
        this.sha256 = Objects.requireNonNull(sha256, "sha256");
        if (!SHA_256.matcher(sha256).matches()) {
            throw new IllegalArgumentException(
                    "sha256 must be 64 lowercase hexadecimal characters");
        }
    }

    public ResolutionScope scope() {
        return scope;
    }

    public int classpathIndex() {
        return classpathIndex;
    }

    public boolean primary() {
        return primary;
    }

    public OptionalInt originRootIndex() {
        return primary ? OptionalInt.empty() : OptionalInt.of(originRootIndex);
    }

    public OptionalInt dependencyDepth() {
        return primary ? OptionalInt.empty() : OptionalInt.of(dependencyDepth);
    }

    /** JSON pointer of the primary or dependency root that introduced this entry. */
    public String declarationPath() {
        return declarationPath;
    }

    /** Declared reference for roots; canonical Maven identity for transitives. */
    public String sourceReference() {
        return sourceReference;
    }

    /** All declarations represented by this entry, including both sides of shared work. */
    public List<PreparedConnectorOrigin> origins() {
        return origins;
    }

    public Optional<MavenArtifactIdentity> mavenIdentity() {
        return Optional.ofNullable(mavenIdentity);
    }

    public List<MavenArtifactIdentity> mavenDependencyPath() {
        return mavenDependencyPath;
    }

    public Path sourcePath() {
        return sourcePath;
    }

    public Path preparedPath() {
        return preparedPath;
    }

    public String sha256() {
        return sha256;
    }

    private static String requirePointer(String value, String name) {
        value = requireNonBlank(value, name);
        if (!value.startsWith("$/")) {
            throw new IllegalArgumentException(name + " must be a root-relative JSON pointer");
        }
        return value;
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
