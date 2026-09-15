package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable target-specific connector closure lock and its canonical hash projection. */
public final class ConnectorClosureLock {
    public static final String FORMAT = "flink-stability.connector-closure-lock/v1";
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    private final ScenarioSide side;
    private final String alias;
    private final String connectorPath;
    private final String declaredPrimaryReference;
    private final String targetFlinkImageReference;
    private final ConnectorDependencyMode dependencyMode;
    private final List<ConnectorClosureLockEntry> classpath;
    private final List<MavenPomEvidence> descriptors;
    private final List<MavenConflictDecision> mediationDecisions;
    private final String canonicalProjectionJson;
    private final String closureSha256;

    ConnectorClosureLock(
            ScenarioSide side,
            String alias,
            String connectorPath,
            String declaredPrimaryReference,
            String targetFlinkImageReference,
            ConnectorDependencyMode dependencyMode,
            List<ConnectorClosureLockEntry> classpath,
            List<MavenPomEvidence> descriptors,
            List<MavenConflictDecision> mediationDecisions,
            String canonicalProjectionJson,
            String closureSha256) {
        this.side = Objects.requireNonNull(side, "side");
        this.alias = requireNonBlank(alias, "alias");
        this.connectorPath = requirePointer(connectorPath, "connectorPath");
        this.declaredPrimaryReference = requireNonBlank(
                declaredPrimaryReference, "declaredPrimaryReference");
        this.targetFlinkImageReference = requireNonBlank(
                targetFlinkImageReference, "targetFlinkImageReference");
        this.dependencyMode = Objects.requireNonNull(dependencyMode, "dependencyMode");
        this.classpath = List.copyOf(Objects.requireNonNull(classpath, "classpath"));
        this.descriptors = List.copyOf(Objects.requireNonNull(descriptors, "descriptors"));
        this.mediationDecisions = List.copyOf(Objects.requireNonNull(
                mediationDecisions, "mediationDecisions"));
        this.canonicalProjectionJson = requireNonBlank(
                canonicalProjectionJson, "canonicalProjectionJson");
        this.closureSha256 = Objects.requireNonNull(closureSha256, "closureSha256");
        if (!SHA_256.matcher(closureSha256).matches()) {
            throw new IllegalArgumentException(
                    "closureSha256 must be 64 lowercase hexadecimal characters");
        }
        if (this.classpath.isEmpty() || !this.classpath.getFirst().primary()) {
            throw new IllegalArgumentException("A connector lock must begin with its primary");
        }
        for (int index = 0; index < this.classpath.size(); index++) {
            if (this.classpath.get(index).classpathIndex() != index) {
                throw new IllegalArgumentException(
                        "Connector lock classpath indexes must be contiguous");
            }
        }
    }

    public ScenarioSide side() {
        return side;
    }

    public String alias() {
        return alias;
    }

    public String connectorPath() {
        return connectorPath;
    }

    public String declaredPrimaryReference() {
        return declaredPrimaryReference;
    }

    public String targetFlinkImageReference() {
        return targetFlinkImageReference;
    }

    public ConnectorDependencyMode dependencyMode() {
        return dependencyMode;
    }

    public List<ConnectorClosureLockEntry> classpath() {
        return classpath;
    }

    public List<MavenPomEvidence> descriptors() {
        return descriptors;
    }

    public List<MavenConflictDecision> mediationDecisions() {
        return mediationDecisions;
    }

    public String canonicalProjectionJson() {
        return canonicalProjectionJson;
    }

    public String closureSha256() {
        return closureSha256;
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
