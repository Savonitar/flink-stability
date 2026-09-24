package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Builds and verifies one deterministic multi-connector cluster bundle. */
public final class ConnectorClusterBundleBuilder {
    private final ConnectorClosureLockFactory lockFactory;

    public ConnectorClusterBundleBuilder() {
        this(new ConnectorClosureLockFactory());
    }

    ConnectorClusterBundleBuilder(ConnectorClosureLockFactory lockFactory) {
        this.lockFactory = Objects.requireNonNull(lockFactory, "lockFactory");
    }

    /** Builds with freshly derived locks for the job-referenced connectors. */
    public PreparedConnectorBundle build(
            PreparedScenarioPlan plan,
            ScenarioSide side,
            String targetFlinkImageReference) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(side, "side");
        requireNonBlank(targetFlinkImageReference, "targetFlinkImageReference");
        Map<String, String> referencedAliases = referencedAliases(plan, side);
        List<Diagnostic> issues = new ArrayList<>();
        validateTarget(plan, side, targetFlinkImageReference, issues);
        List<ConnectorClosureLock> selectedLocks = new ArrayList<>(referencedAliases.size());
        for (Map.Entry<String, String> reference : referencedAliases.entrySet()) {
            PreparedConnectorClosure closure = plan.connectorClosure(side, reference.getKey())
                    .orElse(null);
            if (closure == null) {
                issues.add(issue(
                        plan,
                        side,
                        "artifact.connector.bundle-missing-closure",
                        reference.getValue(),
                        "No prepared connector closure exists for referenced alias '"
                                + reference.getKey() + "'"));
            } else {
                selectedLocks.add(lockFactory.create(
                        closure, side, targetFlinkImageReference));
            }
        }
        return assemble(
                plan,
                side,
                targetFlinkImageReference,
                referencedAliases,
                selectedLocks,
                issues);
    }

    private PreparedConnectorBundle assemble(
            PreparedScenarioPlan plan,
            ScenarioSide side,
            String targetFlinkImageReference,
            Map<String, String> referencedAliases,
            List<ConnectorClosureLock> selectedLocks,
            List<Diagnostic> initialIssues) {
        List<Diagnostic> issues = new ArrayList<>(initialIssues);
        Map<String, MutableBundleEntry> entriesByDigest = new LinkedHashMap<>();
        Map<String, MavenEncounter> mavenByConflictKey = new HashMap<>();

        for (ConnectorClosureLock lock : selectedLocks) {
            verifyLock(plan, side, lock, issues);
            for (ConnectorClosureLockEntry entry : lock.classpath()) {
                verifyStagedBytes(plan, side, lock, entry, issues);
                ConnectorBundleContribution contribution = new ConnectorBundleContribution(
                        lock.alias(), entry.classpathIndex(), entry);
                entry.mavenIdentity().ifPresent(identity -> {
                    MavenEncounter first = mavenByConflictKey.putIfAbsent(
                            identity.conflictKey(),
                            new MavenEncounter(identity, entry.sha256(), contribution));
                    if (first != null && !first.sha256().equals(entry.sha256())) {
                        issues.add(issue(
                                plan,
                                side,
                                "artifact.connector.bundle-maven-conflict",
                                entry.effectiveOrigin().declarationPath(),
                                conflictMessage(identity.conflictKey(), first, contribution)));
                    }
                });
                entriesByDigest.computeIfAbsent(
                                entry.sha256(),
                                ignored -> new MutableBundleEntry(entry.stagedPath()))
                        .contributions()
                        .add(contribution);
            }
        }

        if (!issues.isEmpty()) {
            throw new SpecificationException(Stage.ARTIFACT, issues);
        }

        List<PreparedConnectorBundleEntry> entries = new ArrayList<>(entriesByDigest.size());
        int globalIndex = 0;
        for (Map.Entry<String, MutableBundleEntry> selected : entriesByDigest.entrySet()) {
            String fileName = String.format(
                    Locale.ROOT,
                    "flink-stability-connector-%08d-%s.jar",
                    globalIndex,
                    selected.getKey());
            entries.add(new PreparedConnectorBundleEntry(
                    globalIndex,
                    fileName,
                    "/opt/flink/lib/" + fileName,
                    selected.getValue().stagedPath(),
                    selected.getKey(),
                    selected.getValue().contributions()));
            globalIndex++;
        }
        List<String> aliases = List.copyOf(referencedAliases.keySet());
        String classpathManifest = canonicalClasspathManifest(entries);
        String classpathManifestSha256 = CanonicalJson.sha256(classpathManifest);
        String targetBinding = canonicalTargetBinding(
                targetFlinkImageReference, selectedLocks, classpathManifestSha256);
        return new PreparedConnectorBundle(
                side,
                targetFlinkImageReference,
                aliases,
                selectedLocks,
                entries,
                targetBinding,
                CanonicalJson.sha256(targetBinding),
                classpathManifest,
                classpathManifestSha256);
    }

    private void verifyLock(
            PreparedScenarioPlan plan,
            ScenarioSide side,
            ConnectorClosureLock lock,
            List<Diagnostic> issues) {
        String projection = ConnectorClosureLockFactory.canonicalProjection(lock);
        String actualHash = CanonicalJson.sha256(projection);
        if (!projection.equals(lock.canonicalProjectionJson())
                || !actualHash.equals(lock.closureSha256())) {
            issues.add(issue(
                    plan,
                    side,
                    "artifact.connector.lock-hash-mismatch",
                    lock.connectorPath() + "/artifact",
                    "Connector lock '" + lock.alias()
                            + "' does not match its canonical projection: recorded "
                            + lock.closureSha256() + ", recomputed " + actualHash));
        }
    }

    private static void verifyStagedBytes(
            PreparedScenarioPlan plan,
            ScenarioSide side,
            ConnectorClosureLock lock,
            ConnectorClosureLockEntry entry,
            List<Diagnostic> issues) {
        Path path = entry.stagedPath();
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || !Files.isReadable(path)) {
            issues.add(issue(
                    plan,
                    side,
                    "artifact.connector.staged-unreadable",
                    entry.effectiveOrigin().declarationPath(),
                    "Connector '" + lock.alias() + "' classpath index "
                            + entry.classpathIndex() + " staged JAR is unavailable: " + path
                            + "; expected SHA-256 " + entry.sha256()));
            return;
        }
        String actual;
        try {
            actual = sha256(path);
        } catch (IOException exception) {
            issues.add(issue(
                    plan,
                    side,
                    "artifact.connector.staged-unreadable",
                    entry.effectiveOrigin().declarationPath(),
                    "Could not read connector '" + lock.alias() + "' classpath index "
                            + entry.classpathIndex() + " staged JAR " + path + ": "
                            + safeMessage(exception) + "; expected SHA-256 "
                            + entry.sha256()));
            return;
        }
        if (!entry.sha256().equals(actual)) {
            issues.add(issue(
                    plan,
                    side,
                    "artifact.connector.staged-digest-mismatch",
                    entry.effectiveOrigin().declarationPath(),
                    "Connector '" + lock.alias() + "' classpath index "
                            + entry.classpathIndex() + " staged JAR " + path
                            + " has SHA-256 " + actual + ", expected " + entry.sha256()));
        }
    }

    private void validateTarget(
            PreparedScenarioPlan plan,
            ScenarioSide side,
            String targetFlinkImageReference,
            List<Diagnostic> issues) {
        List<String> declared = lockFactory.targetFlinkImageReferences(plan, side);
        if (!declared.contains(targetFlinkImageReference)) {
            issues.add(issue(
                    plan,
                    side,
                    "artifact.connector.bundle-target-image-mismatch",
                    "$/setup/flink/image",
                    "Target Flink image '" + targetFlinkImageReference
                            + "' is not one of this side's exact resolved target references "
                            + declared));
        }
    }

    private static Map<String, String> referencedAliases(
            PreparedScenarioPlan plan,
            ScenarioSide side) {
        ObjectNode document = plan.scenarioPlan().scenario().side(side).document();
        Map<String, String> aliases = new TreeMap<>();
        JsonNode jobs = document.at("/workload/jobs");
        if (!(jobs instanceof ArrayNode jobArray)) {
            return aliases;
        }
        for (int jobIndex = 0; jobIndex < jobArray.size(); jobIndex++) {
            JsonNode connectors = jobArray.get(jobIndex).get("connectors");
            if (!(connectors instanceof ArrayNode connectorArray)) {
                continue;
            }
            for (int connectorIndex = 0; connectorIndex < connectorArray.size(); connectorIndex++) {
                JsonNode alias = connectorArray.get(connectorIndex);
                if (alias.isTextual()) {
                    aliases.putIfAbsent(
                            alias.textValue(),
                            "$/workload/jobs/" + jobIndex + "/connectors/" + connectorIndex);
                }
            }
        }
        return aliases;
    }

    static String canonicalTargetBinding(
            String targetFlinkImageReference,
            List<ConnectorClosureLock> locks,
            String classpathManifestSha256) {
        Map<String, Object> projection = new TreeMap<>();
        projection.put("classpath_manifest_sha256", classpathManifestSha256);
        projection.put("closures", locks.stream().map(lock -> {
            Map<String, Object> item = new TreeMap<>();
            item.put("alias", lock.alias());
            item.put("closure_sha256", lock.closureSha256());
            return item;
        }).toList());
        projection.put("format", PreparedConnectorBundle.TARGET_BINDING_FORMAT);
        projection.put("target_flink_image_reference", targetFlinkImageReference);
        return CanonicalJson.write(projection);
    }

    static String canonicalClasspathManifest(
            List<PreparedConnectorBundleEntry> entries) {
        Map<String, Object> projection = new TreeMap<>();
        projection.put("entries", entries.stream().map(entry -> {
            Map<String, Object> item = new TreeMap<>();
            item.put("filename", entry.fileName());
            item.put("index", entry.globalClasspathIndex());
            item.put("sha256", entry.sha256());
            return item;
        }).toList());
        projection.put("format", PreparedConnectorBundle.CLASSPATH_MANIFEST_FORMAT);
        return CanonicalJson.write(projection);
    }

    private static String conflictMessage(
            String conflictKey,
            MavenEncounter first,
            ConnectorBundleContribution second) {
        MavenArtifactIdentity secondIdentity = second.closureEntry()
                .mavenIdentity().orElseThrow();
        return "Maven conflict key '" + conflictKey + "' has distinct bytes; first: alias='"
                + first.contribution().alias() + "', closure_index="
                + first.contribution().closureClasspathIndex() + ", identity='"
                + first.identity().coordinate() + "', declaration='"
                + first.contribution().closureEntry().effectiveOrigin().declarationPath()
                + "', sha256=" + first.sha256() + "; second: alias='"
                + second.alias() + "', closure_index=" + second.closureClasspathIndex()
                + ", identity='" + secondIdentity.coordinate() + "', declaration='"
                + second.closureEntry().effectiveOrigin().declarationPath()
                + "', sha256=" + second.closureEntry().sha256();
    }

    private static Diagnostic issue(
            PreparedScenarioPlan plan,
            ScenarioSide side,
            String code,
            String path,
            String message) {
        return new Diagnostic(
                plan.scenarioPlan().scenario().template().source(),
                scope(side),
                code,
                path,
                message);
    }

    private static ResolutionScope scope(ScenarioSide side) {
        return switch (side) {
            case SINGLE -> ResolutionScope.SINGLE;
            case BASELINE -> ResolutionScope.BASELINE;
            case CANDIDATE -> ResolutionScope.CANDIDATE;
        };
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                for (int read; (read = input.read(buffer)) >= 0;) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java must provide SHA-256", impossible);
        }
    }

    private static String safeMessage(Throwable throwable) {
        return throwable.getMessage() == null
                ? throwable.getClass().getSimpleName()
                : throwable.getMessage();
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private record MavenEncounter(
            MavenArtifactIdentity identity,
            String sha256,
            ConnectorBundleContribution contribution) {}

    private record MutableBundleEntry(
            Path stagedPath,
            List<ConnectorBundleContribution> contributions) {
        private MutableBundleEntry(Path stagedPath) {
            this(stagedPath, new ArrayList<>());
        }
    }
}
