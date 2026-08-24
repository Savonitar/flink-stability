package org.savonitar.flink.stability.core.spec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** One connector's immutable primary and ordered runtime classpath evidence. */
public final class PreparedConnectorClosure {
    private final ResolutionScope scope;
    private final String alias;
    private final String connectorPath;
    private final ConnectorDependencyMode dependencyMode;
    private final PreparedConnectorArtifact primary;
    private final List<PreparedConnectorArtifact> dependencies;
    private final List<PreparedConnectorArtifact> classpath;
    private final List<MavenPomEvidence> consultedPoms;
    private final List<MavenConflictDecision> conflicts;

    PreparedConnectorClosure(
            ResolutionScope scope,
            String alias,
            String connectorPath,
            ConnectorDependencyMode dependencyMode,
            PreparedConnectorArtifact primary,
            List<PreparedConnectorArtifact> dependencies,
            List<MavenPomEvidence> consultedPoms,
            List<MavenConflictDecision> conflicts) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.alias = requireNonBlank(alias, "alias");
        this.connectorPath = requirePointer(connectorPath, "connectorPath");
        this.dependencyMode = Objects.requireNonNull(dependencyMode, "dependencyMode");
        this.primary = Objects.requireNonNull(primary, "primary");
        this.dependencies = List.copyOf(Objects.requireNonNull(
                dependencies, "dependencies"));
        this.consultedPoms = List.copyOf(Objects.requireNonNull(
                consultedPoms, "consultedPoms"));
        this.conflicts = List.copyOf(Objects.requireNonNull(conflicts, "conflicts"));

        if (!primary.primary() || primary.classpathIndex() != 0) {
            throw new IllegalArgumentException("Connector closure primary is invalid");
        }
        validateScope(primary);
        Set<ClasspathIdentity> selected = new HashSet<>();
        selected.add(classpathIdentity(primary));
        for (int index = 0; index < this.dependencies.size(); index++) {
            PreparedConnectorArtifact dependency = this.dependencies.get(index);
            if (dependency.primary() || dependency.classpathIndex() != index + 1) {
                throw new IllegalArgumentException(
                        "Connector dependencies must use contiguous classpath indexes");
            }
            validateScope(dependency);
            if (!selected.add(classpathIdentity(dependency))) {
                throw new IllegalArgumentException(
                        "Connector closure contains a duplicate logical classpath entry");
            }
        }
        List<PreparedConnectorArtifact> ordered = new ArrayList<>(
                this.dependencies.size() + 1);
        ordered.add(primary);
        ordered.addAll(this.dependencies);
        this.classpath = List.copyOf(ordered);
    }

    public ResolutionScope scope() {
        return scope;
    }

    public String alias() {
        return alias;
    }

    public String connectorPath() {
        return connectorPath;
    }

    public ConnectorDependencyMode dependencyMode() {
        return dependencyMode;
    }

    public PreparedConnectorArtifact primary() {
        return primary;
    }

    public List<PreparedConnectorArtifact> dependencies() {
        return dependencies;
    }

    /** Primary at index zero followed by deterministic dependency order. */
    public List<PreparedConnectorArtifact> classpath() {
        return classpath;
    }

    public List<MavenPomEvidence> consultedPoms() {
        return consultedPoms;
    }

    public List<MavenConflictDecision> conflicts() {
        return conflicts;
    }

    private void validateScope(PreparedConnectorArtifact artifact) {
        if (artifact.scope() != scope && artifact.scope() != ResolutionScope.COMMON) {
            throw new IllegalArgumentException(
                    "Connector classpath entries must be closure-scoped or common");
        }
        if (scope == ResolutionScope.COMMON
                && artifact.scope() != ResolutionScope.COMMON) {
            throw new IllegalArgumentException(
                    "A common connector closure may contain only common entries");
        }
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

    private static ClasspathIdentity classpathIdentity(
            PreparedConnectorArtifact artifact) {
        return artifact.mavenIdentity()
                .map(identity -> new ClasspathIdentity("maven", identity))
                .orElseGet(() -> new ClasspathIdentity("local-sha256", artifact.sha256()));
    }

    private record ClasspathIdentity(String kind, Object identity) {}
}
