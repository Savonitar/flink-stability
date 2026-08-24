package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Creates deterministic target-specific locks from prepared connector closures. */
public final class ConnectorClosureLockFactory {

    /** Creates one lock for a prepared connector and exact resolved Flink image reference. */
    public ConnectorClosureLock create(
            PreparedConnectorClosure prepared,
            ScenarioSide side,
            String targetFlinkImageReference) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(side, "side");
        requireNonBlank(targetFlinkImageReference, "targetFlinkImageReference");
        validateScope(prepared, side);
        if (prepared.dependencyMode() == ConnectorDependencyMode.AUTO
                && prepared.primary().mavenIdentity().isEmpty()) {
            throw new IllegalArgumentException(
                    "AUTO connector closure locks require a Maven primary");
        }

        List<ConnectorClosureLockEntry> classpath = new ArrayList<>(
                prepared.classpath().size());
        for (PreparedConnectorArtifact artifact : prepared.classpath()) {
            PreparedConnectorOrigin origin = effectiveOrigin(artifact, side);
            artifact.origins().stream()
                    .filter(candidate -> candidate.scope() == origin.scope())
                    .forEach(candidate -> validateOriginMode(
                            prepared.dependencyMode(), artifact, candidate));
            classpath.add(new ConnectorClosureLockEntry(artifact, origin));
        }

        ConnectorClosureLockEntry primary = classpath.getFirst();
        String declaredPrimaryReference = primary.effectiveOrigin().declaredReference();
        List<MavenPomEvidence> descriptors = sortedDescriptors(prepared.consultedPoms());
        List<MavenConflictDecision> mediationDecisions = List.copyOf(prepared.conflicts());
        String projection = canonicalProjection(
                prepared.alias(),
                declaredPrimaryReference,
                targetFlinkImageReference,
                prepared.dependencyMode(),
                classpath,
                descriptors,
                mediationDecisions);
        return new ConnectorClosureLock(
                side,
                prepared.alias(),
                prepared.connectorPath(),
                declaredPrimaryReference,
                targetFlinkImageReference,
                prepared.dependencyMode(),
                classpath,
                descriptors,
                mediationDecisions,
                projection,
                CanonicalJson.sha256(projection));
    }

    /**
     * Returns distinct exact target image references in first-execution encounter order.
     * Setup is first, followed by explicit Flink, JobManager, and TaskManager restart images.
     */
    public List<String> targetFlinkImageReferences(
            PreparedScenarioPlan plan,
            ScenarioSide side) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(side, "side");
        ObjectNode document = plan.scenarioPlan().scenario().side(side).document();
        Set<String> targets = new LinkedHashSet<>();
        JsonNode setupImage = document.at("/setup/flink/image");
        if (setupImage.isTextual() && !setupImage.textValue().isBlank()) {
            targets.add(setupImage.textValue());
        }
        collectRestartTargets(document.get("phases"), targets);
        if (targets.isEmpty()) {
            throw new IllegalArgumentException(
                    "A resolved scenario side must declare setup.flink.image");
        }
        return List.copyOf(targets);
    }

    /** Creates every declared alias/target lock required before provisioning one side. */
    public List<ConnectorClosureLock> createAll(
            PreparedScenarioPlan plan,
            ScenarioSide side) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(side, "side");
        ObjectNode document = plan.scenarioPlan().scenario().side(side).document();
        JsonNode connectors = document.at("/subject/connectors");
        if (!(connectors instanceof ObjectNode connectorObject)) {
            throw new IllegalArgumentException(
                    "A resolved scenario side must declare subject.connectors");
        }
        List<String> aliases = new ArrayList<>();
        connectorObject.fieldNames().forEachRemaining(aliases::add);
        aliases.sort(Comparator.naturalOrder());
        List<String> targets = targetFlinkImageReferences(plan, side);
        List<ConnectorClosureLock> locks = new ArrayList<>(aliases.size() * targets.size());
        for (String alias : aliases) {
            PreparedConnectorClosure closure = plan.connectorClosure(side, alias)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "No prepared connector closure for alias " + alias));
            for (String target : targets) {
                locks.add(create(closure, side, target));
            }
        }
        return List.copyOf(locks);
    }

    static String canonicalProjection(ConnectorClosureLock lock) {
        Objects.requireNonNull(lock, "lock");
        return canonicalProjection(
                lock.alias(),
                lock.declaredPrimaryReference(),
                lock.targetFlinkImageReference(),
                lock.dependencyMode(),
                lock.classpath(),
                lock.descriptors(),
                lock.mediationDecisions());
    }

    private static String canonicalProjection(
            String alias,
            String declaredPrimaryReference,
            String targetFlinkImageReference,
            ConnectorDependencyMode mode,
            List<ConnectorClosureLockEntry> classpath,
            List<MavenPomEvidence> descriptors,
            List<MavenConflictDecision> mediationDecisions) {
        Map<String, Object> projection = new TreeMap<>();
        projection.put("classpath", classpath.stream()
                .map(ConnectorClosureLockFactory::classpathProjection)
                .toList());
        projection.put("connector_alias", alias);
        projection.put("declared_primary_reference", declaredPrimaryReference);
        projection.put("dependency_mode", mode.name().toLowerCase(Locale.ROOT));
        projection.put("descriptors", descriptors.stream()
                .map(ConnectorClosureLockFactory::descriptorProjection)
                .toList());
        projection.put("format", ConnectorClosureLock.FORMAT);
        projection.put("mediation_decisions", mediationDecisions.stream()
                .map(decision -> mediationProjection(decision, mode))
                .toList());
        projection.put("primary_sha256", classpath.getFirst().sha256());
        projection.put("target_flink_image_reference", targetFlinkImageReference);
        return CanonicalJson.write(projection);
    }

    private static Map<String, Object> classpathProjection(
            ConnectorClosureLockEntry entry) {
        Map<String, Object> projection = new TreeMap<>();
        projection.put("classpath_index", entry.classpathIndex());
        Map<String, Object> identity = entry.mavenIdentity()
                .map(ConnectorClosureLockFactory::classpathMavenIdentityProjection)
                .orElseGet(() -> {
                    Map<String, Object> local = new TreeMap<>();
                    local.put("kind", "local");
                    return local;
                });
        projection.put("identity", identity);
        Map<String, Object> origin = new TreeMap<>();
        origin.put("index", entry.effectiveOrigin().declarationIndex());
        origin.put("kind", rootKind(entry.effectiveOrigin().rootKind()));
        projection.put("origin_root", origin);
        projection.put("sha256", entry.sha256());
        return projection;
    }

    private static Map<String, Object> classpathMavenIdentityProjection(
            MavenArtifactIdentity identity) {
        Map<String, Object> projection = mavenIdentityProjection(identity);
        projection.put("kind", "maven");
        return projection;
    }

    private static Map<String, Object> descriptorProjection(MavenPomEvidence descriptor) {
        Map<String, Object> projection = new TreeMap<>();
        projection.put("identity", mavenIdentityProjection(descriptor.identity()));
        projection.put("sha256", descriptor.sha256());
        return projection;
    }

    private static Map<String, Object> mediationProjection(
            MavenConflictDecision decision,
            ConnectorDependencyMode mode) {
        Map<String, Object> projection = new TreeMap<>();
        projection.put("omitted_depth", decision.omittedDepth());
        projection.put("omitted_identity", mavenIdentityProjection(decision.omitted()));
        projection.put("omitted_origin_root", mediationOriginProjection(
                mode, decision.omittedOriginRootIndex()));
        projection.put("omitted_path", decision.omittedPath().stream()
                .map(ConnectorClosureLockFactory::mavenIdentityProjection)
                .toList());
        projection.put("reason", decision.reason().name().toLowerCase(Locale.ROOT));
        projection.put("winner_depth", decision.winnerDepth());
        projection.put("winner_identity", mavenIdentityProjection(decision.winner()));
        projection.put("winner_origin_root", mediationOriginProjection(
                mode, decision.winnerOriginRootIndex()));
        projection.put("winner_path", decision.winnerPath().stream()
                .map(ConnectorClosureLockFactory::mavenIdentityProjection)
                .toList());
        return projection;
    }

    private static Map<String, Object> mediationOriginProjection(
            ConnectorDependencyMode mode,
            int resolverRootIndex) {
        Map<String, Object> origin = new TreeMap<>();
        if (mode == ConnectorDependencyMode.AUTO) {
            origin.put("index", -1);
            origin.put("kind", "primary");
        } else {
            origin.put("index", resolverRootIndex);
            origin.put("kind", "runtime_dependency");
        }
        return origin;
    }

    private static Map<String, Object> mavenIdentityProjection(
            MavenArtifactIdentity identity) {
        Map<String, Object> projection = new TreeMap<>();
        projection.put("artifact_id", identity.artifactId());
        projection.put("classifier", identity.classifier());
        projection.put("extension", identity.extension());
        projection.put("group_id", identity.groupId());
        projection.put("version", identity.version());
        return projection;
    }

    private static PreparedConnectorOrigin effectiveOrigin(
            PreparedConnectorArtifact artifact,
            ScenarioSide side) {
        ResolutionScope desired = scope(side);
        List<PreparedConnectorOrigin> exact = artifact.origins().stream()
                .filter(origin -> origin.scope() == desired)
                .toList();
        if (!exact.isEmpty()) {
            // Content coalescing may retain several declarations from one side. The
            // prepared order already records R4.13b's first encounter; the stable lock
            // projects that retained root while full artifact evidence keeps every origin.
            return exact.getFirst();
        }
        List<PreparedConnectorOrigin> common = artifact.origins().stream()
                .filter(origin -> origin.scope() == ResolutionScope.COMMON)
                .toList();
        if (!common.isEmpty()) {
            return common.getFirst();
        }
        throw new IllegalArgumentException(
                "No origin for classpath index " + artifact.classpathIndex()
                        + " and side " + side);
    }

    private static void validateScope(
            PreparedConnectorClosure prepared,
            ScenarioSide side) {
        ResolutionScope desired = scope(side);
        if (prepared.scope() != desired && prepared.scope() != ResolutionScope.COMMON) {
            throw new IllegalArgumentException(
                    "Connector closure scope " + prepared.scope()
                            + " cannot create a lock for side " + side);
        }
        if (side == ScenarioSide.SINGLE && prepared.scope() == ResolutionScope.COMMON) {
            throw new IllegalArgumentException(
                    "A plain scenario cannot use an experiment-common connector closure");
        }
    }

    private static void validateOriginMode(
            ConnectorDependencyMode mode,
            PreparedConnectorArtifact artifact,
            PreparedConnectorOrigin origin) {
        if (artifact.primary()) {
            if (!origin.primaryRoot()) {
                throw new IllegalArgumentException(
                        "A connector primary must use its primary declaration origin");
            }
            return;
        }
        if (mode == ConnectorDependencyMode.AUTO && !origin.primaryRoot()) {
            throw new IllegalArgumentException(
                    "AUTO dependencies must originate from the connector primary");
        }
        if (mode == ConnectorDependencyMode.EXPLICIT && origin.primaryRoot()) {
            throw new IllegalArgumentException(
                    "EXPLICIT dependencies must originate from runtime_dependencies");
        }
    }

    static List<MavenPomEvidence> sortedDescriptors(
            List<MavenPomEvidence> descriptors) {
        Map<MavenArtifactIdentity, MavenPomEvidence> unique = new LinkedHashMap<>();
        for (MavenPomEvidence descriptor : descriptors) {
            MavenPomEvidence previous = unique.putIfAbsent(descriptor.identity(), descriptor);
            if (previous != null && !previous.sha256().equals(descriptor.sha256())) {
                throw new IllegalArgumentException(
                        "One Maven descriptor identity has multiple SHA-256 values: "
                                + descriptor.identity().coordinate());
            }
        }
        return unique.values().stream()
                .sorted(Comparator.comparing(MavenPomEvidence::identity))
                .toList();
    }

    private static void collectRestartTargets(JsonNode node, Set<String> targets) {
        if (node instanceof ObjectNode object) {
            JsonNode restart = object.get("restart");
            if (restart instanceof ObjectNode restartObject
                    && isFlinkProcess(restartObject.path("component").textValue())) {
                JsonNode image = restartObject.get("image");
                if (image != null && image.isTextual() && !image.textValue().isBlank()) {
                    targets.add(image.textValue());
                }
            }
            object.fields().forEachRemaining(entry ->
                    collectRestartTargets(entry.getValue(), targets));
        } else if (node instanceof ArrayNode array) {
            for (JsonNode element : array) {
                collectRestartTargets(element, targets);
            }
        }
    }

    private static boolean isFlinkProcess(String component) {
        return "flink".equals(component)
                || "jobmanager".equals(component)
                || "taskmanager".equals(component);
    }

    private static ResolutionScope scope(ScenarioSide side) {
        return switch (side) {
            case SINGLE -> ResolutionScope.SINGLE;
            case BASELINE -> ResolutionScope.BASELINE;
            case CANDIDATE -> ResolutionScope.CANDIDATE;
        };
    }

    private static String rootKind(PreparedConnectorOrigin.RootKind rootKind) {
        return switch (rootKind) {
            case PRIMARY -> "primary";
            case RUNTIME_DEPENDENCY -> "runtime_dependency";
        };
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
