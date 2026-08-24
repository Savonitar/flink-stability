package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.testcontainers.ConnectorClasspathManifest;
import org.savonitar.flink.stability.testcontainers.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.testcontainers.FlinkRuntimeTarget;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorClusterBundleBuilderTest {
    private final SpecificationLoader loader = new SpecificationLoader();
    private final ConnectorClosureLockFactory lockFactory = new ConnectorClosureLockFactory();
    private final ConnectorClusterBundleBuilder builder = new ConnectorClusterBundleBuilder();

    @TempDir
    Path temporary;

    @Test
    void aliasOrderAndDigestCoalescingProduceDeterministicTargetAndClasspathManifests()
            throws IOException {
        Path alpha = write("alpha.jar", "identical connector bytes");
        Path beta = write("beta.jar", "identical connector bytes");
        String hash = sha256(alpha);
        ObjectNode document = scenarioDocument(
                "coalesced-bundle", List.of("beta", "alpha"), false, true);
        PreparedScenarioPlan plan = directPlan(
                resolve(document),
                List.of(
                        localClosure("alpha", alpha, hash, List.of()),
                        localClosure("beta", beta, hash, List.of())));

        List<PreparedConnectorBundle> allTargets = builder.buildAll(
                plan, ScenarioSide.SINGLE);
        assertEquals(2, allTargets.size());
        PreparedConnectorBundle initial = allTargets.get(0);
        PreparedConnectorBundle upgraded = allTargets.get(1);

        assertEquals(List.of("alpha", "beta"), initial.aliases());
        assertEquals(List.of("alpha", "beta"), initial.closureLocks().stream()
                .map(ConnectorClosureLock::alias).toList());
        assertEquals(1, initial.entries().size());
        PreparedConnectorBundleEntry entry = initial.entries().getFirst();
        assertEquals(0, entry.globalClasspathIndex());
        assertEquals("flink-stability-connector-00000000-" + hash + ".jar",
                entry.fileName());
        assertEquals("/opt/flink/lib/" + entry.fileName(), entry.containerPath());
        assertEquals(alpha.toAbsolutePath().normalize(), entry.stagedPath());
        assertEquals(List.of("alpha", "beta"), entry.contributions().stream()
                .map(ConnectorBundleContribution::alias).toList());
        assertNotEquals(initial.targetBindingSha256(), upgraded.targetBindingSha256());
        assertEquals(initial.classpathManifestJson(), upgraded.classpathManifestJson());
        assertEquals(initial.classpathManifestSha256(), upgraded.classpathManifestSha256());
        assertFalse(initial.classpathManifestJson().contains("flink:2.2"));
        assertFalse(initial.classpathManifestJson().contains("alpha"));
        assertFalse(initial.classpathManifestJson().contains(temporary.toString()));
        assertEquals(
                "{\"entries\":[{\"filename\":\"" + entry.fileName()
                        + "\",\"index\":0,\"sha256\":\"" + hash + "\"}],\"format\":\""
                        + PreparedConnectorBundle.CLASSPATH_MANIFEST_FORMAT + "\"}",
                initial.classpathManifestJson());
        String expectedBinding = "{\"classpath_manifest_sha256\":\""
                + initial.classpathManifestSha256() + "\",\"closures\":[{\"alias\":\"alpha\","
                + "\"closure_sha256\":\""
                + initial.closureLocks().get(0).closureSha256()
                + "\"},{\"alias\":\"beta\",\"closure_sha256\":\""
                + initial.closureLocks().get(1).closureSha256()
                + "\"}],\"format\":\"" + PreparedConnectorBundle.TARGET_BINDING_FORMAT
                + "\",\"target_flink_image_reference\":\"flink:2.2.0\"}";
        assertEquals(expectedBinding, initial.canonicalTargetBindingJson());
        assertEquals(CanonicalJson.sha256(expectedBinding), initial.targetBindingSha256());
        assertFalse(initial.canonicalTargetBindingJson().contains("\"aliases\""));
        assertFalse(initial.canonicalTargetBindingJson().contains("\"entries\""));
        assertThrows(UnsupportedOperationException.class, () -> initial.aliases().clear());
        assertThrows(UnsupportedOperationException.class, () -> initial.entries().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> entry.contributions().clear());
    }

    @Test
    void firstAliasAndClosureEncounterControlsGlobalOrderAndRetainsAllContributors()
            throws IOException {
        Path alpha = write("alpha.jar", "alpha");
        Path beta = write("beta.jar", "beta");
        Path sharedAlpha = write("shared-alpha.jar", "shared");
        Path sharedBeta = write("shared-beta.jar", "shared");
        ObjectNode document = scenarioDocument(
                "ordered-bundle", List.of("beta", "alpha"), true, false);
        PreparedScenarioPlan plan = directPlan(
                resolve(document),
                List.of(
                        localClosure(
                                "alpha",
                                alpha,
                                sha256(alpha),
                                List.of(localDependency(
                                        "alpha", 0, sharedAlpha, sha256(sharedAlpha)))),
                        localClosure(
                                "beta",
                                beta,
                                sha256(beta),
                                List.of(localDependency(
                                        "beta", 0, sharedBeta, sha256(sharedBeta))))));

        PreparedConnectorBundle bundle = builder.build(
                plan, ScenarioSide.SINGLE, "flink:2.2.0");

        assertEquals(List.of(sha256(alpha), sha256(sharedAlpha), sha256(beta)),
                bundle.entries().stream().map(PreparedConnectorBundleEntry::sha256).toList());
        assertEquals(List.of(0, 1, 2), bundle.entries().stream()
                .map(PreparedConnectorBundleEntry::globalClasspathIndex).toList());
        assertEquals(List.of("alpha", "beta"), bundle.entries().get(1).contributions().stream()
                .map(ConnectorBundleContribution::alias).toList());
        assertEquals(List.of(1, 1), bundle.entries().get(1).contributions().stream()
                .map(ConnectorBundleContribution::closureClasspathIndex).toList());
        assertEquals(sharedAlpha.toAbsolutePath().normalize(),
                bundle.entries().get(1).stagedPath());
        assertTrue(bundle.entries().stream().allMatch(entry ->
                entry.fileName().startsWith(String.format(
                        "flink-stability-connector-%08d-",
                        entry.globalClasspathIndex()))));

        ObjectNode reorderedDocument = scenarioDocument(
                "ordered-bundle-other-declaration", List.of("alpha", "beta"), true, false);
        PreparedScenarioPlan reordered = directPlan(
                resolve(reorderedDocument), plan.connectorClosures());
        PreparedConnectorBundle same = builder.build(
                reordered, ScenarioSide.SINGLE, "flink:2.2.0");
        assertEquals(bundle.targetBindingSha256(), same.targetBindingSha256());
        assertEquals(bundle.classpathManifestSha256(), same.classpathManifestSha256());
    }

    @Test
    void sameMavenConflictKeyAndBytesCoalescesAcrossVersionsButDistinctBytesReject()
            throws IOException {
        Path alpha = write("alpha.jar", "alpha-primary");
        Path beta = write("beta.jar", "beta-primary");
        Path sharedOne = write("shared-one.jar", "same-maven-bytes");
        Path sharedTwo = write("shared-two.jar", "same-maven-bytes");
        ObjectNode document = scenarioDocument(
                "maven-coalescing", List.of("beta", "alpha"), true, false);
        PreparedScenarioPlan coalescingPlan = directPlan(
                resolve(document),
                List.of(
                        localClosure(
                                "alpha",
                                alpha,
                                sha256(alpha),
                                List.of(mavenDependency(
                                        "alpha", "1.0", sharedOne, sha256(sharedOne)))),
                        localClosure(
                                "beta",
                                beta,
                                sha256(beta),
                                List.of(mavenDependency(
                                        "beta", "2.0", sharedTwo, sha256(sharedTwo))))));

        PreparedConnectorBundle coalesced = builder.build(
                coalescingPlan, ScenarioSide.SINGLE, "flink:2.2.0");
        PreparedConnectorBundleEntry maven = coalesced.entries().stream()
                .filter(entry -> entry.sha256().equals(uncheckedSha256(sharedOne)))
                .findFirst()
                .orElseThrow();
        assertEquals(List.of("alpha", "beta"), maven.contributions().stream()
                .map(ConnectorBundleContribution::alias).toList());
        assertEquals(List.of("1.0", "2.0"), maven.contributions().stream()
                .map(contribution -> contribution.closureEntry().mavenIdentity()
                        .orElseThrow().version())
                .toList());

        Path conflicting = write("shared-conflicting.jar", "different-maven-bytes");
        PreparedScenarioPlan conflictingPlan = directPlan(
                resolve(scenarioDocument(
                        "maven-conflict", List.of("alpha", "beta"), true, false)),
                List.of(
                        localClosure(
                                "alpha",
                                alpha,
                                sha256(alpha),
                                List.of(mavenDependency(
                                        "alpha", "1.0", sharedOne, sha256(sharedOne)))),
                        localClosure(
                                "beta",
                                beta,
                                sha256(beta),
                                List.of(mavenDependency(
                                        "beta", "2.0", conflicting, sha256(conflicting))))));

        ArtifactResolutionException failure = assertThrows(
                ArtifactResolutionException.class,
                () -> builder.build(
                        conflictingPlan, ScenarioSide.SINGLE, "flink:2.2.0"));
        assertEquals(List.of("artifact.connector.bundle-maven-conflict"),
                failure.issues().stream().map(ArtifactIssue::code).toList());
        String message = failure.issues().getFirst().message();
        assertTrue(message.contains("org.example:shared:jar:1.0"));
        assertTrue(message.contains("org.example:shared:jar:2.0"));
        assertTrue(message.contains("alias='alpha'"));
        assertTrue(message.contains("alias='beta'"));
        assertTrue(message.contains(sha256(sharedOne)));
        assertTrue(message.contains(sha256(conflicting)));
    }

    @Test
    void staleBytesMissingLocksTargetDisagreementAndCorruptLockHashesAreTyped()
            throws IOException {
        Path alpha = write("alpha.jar", "prepared-alpha");
        String recorded = sha256(alpha);
        PreparedConnectorClosure closure = localClosure("alpha", alpha, recorded, List.of());
        PreparedScenarioPlan plan = directPlan(
                resolve(scenarioDocument(
                        "bundle-failures", List.of("alpha"), false, false)),
                List.of(closure));
        ConnectorClosureLock valid = lockFactory.create(
                closure, ScenarioSide.SINGLE, "flink:2.2.0");

        ArtifactResolutionException missingLock = assertThrows(
                ArtifactResolutionException.class,
                () -> builder.build(
                        plan, ScenarioSide.SINGLE, "flink:2.2.0", List.of()));
        assertSingleCode(missingLock, "artifact.connector.bundle-missing-closure");

        ArtifactResolutionException targetMismatch = assertThrows(
                ArtifactResolutionException.class,
                () -> builder.build(plan, ScenarioSide.SINGLE, "flink:2.2.9"));
        assertSingleCode(targetMismatch, "artifact.connector.bundle-target-image-mismatch");

        ConnectorClosureLock corrupt = new ConnectorClosureLock(
                valid.side(),
                valid.alias(),
                valid.connectorPath(),
                valid.declaredPrimaryReference(),
                valid.targetFlinkImageReference(),
                valid.dependencyMode(),
                valid.classpath(),
                valid.descriptors(),
                valid.mediationDecisions(),
                "{}",
                "0".repeat(64));
        ArtifactResolutionException badHash = assertThrows(
                ArtifactResolutionException.class,
                () -> builder.build(
                        plan, ScenarioSide.SINGLE, "flink:2.2.0", List.of(corrupt)));
        assertSingleCode(badHash, "artifact.connector.lock-hash-mismatch");

        Files.writeString(alpha, "mutated", StandardCharsets.UTF_8);
        ArtifactResolutionException digestMismatch = assertThrows(
                ArtifactResolutionException.class,
                () -> builder.build(
                        plan, ScenarioSide.SINGLE, "flink:2.2.0", List.of(valid)));
        assertSingleCode(digestMismatch, "artifact.connector.staged-digest-mismatch");
        assertTrue(digestMismatch.issues().getFirst().message().contains(recorded));

        Files.delete(alpha);
        ArtifactResolutionException unreadable = assertThrows(
                ArtifactResolutionException.class,
                () -> builder.build(
                        plan, ScenarioSide.SINGLE, "flink:2.2.0", List.of(valid)));
        assertSingleCode(unreadable, "artifact.connector.staged-unreadable");
        assertTrue(unreadable.issues().getFirst().message().contains(recorded));
    }

    @Test
    void trustedRuntimeAdapterKeepsImageBindingAndBytesFromOnePreparedBundle()
            throws IOException {
        Path alpha = write("runtime-alpha.jar", "runtime-alpha");
        PreparedConnectorClosure closure = localClosure(
                "alpha", alpha, sha256(alpha), List.of());
        PreparedScenarioPlan plan = directPlan(
                resolve(scenarioDocument(
                        "runtime-adapter", List.of("alpha"), false, false)),
                List.of(closure));
        PreparedConnectorBundle bundle = builder.build(
                plan, ScenarioSide.SINGLE, "flink:2.2.0");

        FlinkRuntimeTarget target = new PreparedConnectorRuntimeTargetFactory().create(bundle);

        assertEquals(bundle.targetFlinkImageReference(), target.imageReference());
        FlinkConnectorBundleInstallation installation = target.connectorBundle().orElseThrow();
        assertEquals(bundle.targetFlinkImageReference(),
                installation.targetFlinkImageReference());
        assertEquals(
                bundle.closureLocks().stream()
                        .map(lock -> new FlinkConnectorBundleInstallation.ClosureLockHash(
                                lock.alias(), lock.closureSha256()))
                        .toList(),
                installation.closureLocks());
        assertArrayEquals(bundle.canonicalTargetBindingBytes(),
                installation.canonicalTargetBindingBytes());
        assertEquals(bundle.targetBindingSha256(), installation.targetBindingSha256());
        ConnectorClasspathManifest manifest = installation.classpathManifest();
        assertEquals(bundle.classpathManifestSha256(), manifest.manifestSha256());
        assertEquals(bundle.classpathManifestJson(),
                new String(manifest.canonicalBytes(), StandardCharsets.UTF_8));
        assertEquals(bundle.entries().getFirst().fileName(),
                manifest.entries().getFirst().containerFilename());
        assertEquals(bundle.entries().getFirst().containerPath(),
                manifest.entries().getFirst().containerPath());
        assertEquals(bundle.entries().getFirst().stagedPath(),
                manifest.entries().getFirst().preparedPath());

        PreparedConnectorBundle corruptManifest = new PreparedConnectorBundle(
                bundle.side(),
                bundle.targetFlinkImageReference(),
                bundle.aliases(),
                bundle.closureLocks(),
                bundle.entries(),
                bundle.canonicalTargetBindingJson(),
                bundle.targetBindingSha256(),
                "{}",
                "0".repeat(64));
        assertThrows(
                IllegalStateException.class,
                () -> new PreparedConnectorRuntimeTargetFactory().create(corruptManifest));

        Files.writeString(alpha, "mutated-after-bundle", StandardCharsets.UTF_8);
        IllegalStateException mutation = assertThrows(
                IllegalStateException.class,
                () -> new PreparedConnectorRuntimeTargetFactory().create(bundle));
        assertTrue(mutation.getMessage().contains("expected SHA-256"));
        assertTrue(mutation.getMessage().contains(bundle.entries().getFirst().sha256()));
        assertTrue(mutation.getMessage().contains("actual"));
    }

    private ObjectNode scenarioDocument(
            String name,
            List<String> jobAliases,
            boolean dependencies,
            boolean upgrade) {
        ObjectNode document = loader.loadScenario(resource("minimal.yaml")).document();
        document.withObject("meta").put("name", name);
        ObjectNode connectors = document.withObject("subject").withObject("connectors");
        connectors.removeAll();
        List<String> declarations = jobAliases.isEmpty()
                ? List.of("alpha")
                : jobAliases.stream().distinct().sorted().toList();
        for (String alias : declarations) {
            ObjectNode connector = connectors.putObject(alias);
            connector.put("artifact", alias + ".jar");
            ArrayNode runtime = connector.putArray("runtime_dependencies");
            if (dependencies) {
                runtime.add("maven:org.example:shared:1.0");
            }
        }
        ObjectNode job = (ObjectNode) document.at("/workload/jobs/0");
        job.put("jar", "job.jar");
        if (jobAliases.isEmpty()) {
            job.remove("connectors");
        } else {
            ArrayNode aliases = job.putArray("connectors");
            jobAliases.forEach(aliases::add);
        }
        if (upgrade) {
            ArrayNode steps = ((ObjectNode) document.at("/phases/0")).putArray("steps");
            steps.addObject().putObject("restart")
                    .put("component", "flink")
                    .put("image", "flink:2.2.1");
        }
        return document;
    }

    private PreparedConnectorClosure localClosure(
            String alias,
            Path primaryPath,
            String primaryHash,
            List<PreparedConnectorArtifact> dependencies) {
        String declarationPath = "$/subject/connectors/" + alias + "/artifact";
        String reference = alias + ".jar";
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.PRIMARY,
                -1,
                declarationPath,
                reference);
        PreparedConnectorArtifact primary = new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                0,
                true,
                -1,
                -1,
                declarationPath,
                reference,
                List.of(origin),
                null,
                List.of(),
                primaryPath,
                primaryPath,
                primaryHash);
        return new PreparedConnectorClosure(
                ResolutionScope.SINGLE,
                alias,
                "$/subject/connectors/" + alias,
                ConnectorDependencyMode.EXPLICIT,
                primary,
                dependencies,
                List.of(),
                List.of());
    }

    private PreparedConnectorArtifact localDependency(
            String alias,
            int dependencyIndex,
            Path path,
            String hash) {
        String declarationPath = "$/subject/connectors/" + alias
                + "/runtime_dependencies/" + dependencyIndex;
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                dependencyIndex,
                declarationPath,
                path.getFileName().toString());
        return new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                1,
                false,
                dependencyIndex,
                0,
                declarationPath,
                origin.declaredReference(),
                List.of(origin),
                null,
                List.of(),
                path,
                path,
                hash);
    }

    private PreparedConnectorArtifact mavenDependency(
            String alias,
            String version,
            Path path,
            String hash) {
        MavenArtifactIdentity identity = new MavenArtifactIdentity(
                "org.example", "shared", "jar", "", version);
        String declarationPath = "$/subject/connectors/" + alias
                + "/runtime_dependencies/0";
        String reference = "maven:org.example:shared:" + version;
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                0,
                declarationPath,
                reference);
        return new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                1,
                false,
                0,
                0,
                declarationPath,
                reference,
                List.of(origin),
                identity,
                List.of(identity),
                path,
                path,
                hash);
    }

    private PreparedScenarioPlan directPlan(
            ResolvedScenarioPlan scenarioPlan,
            List<PreparedConnectorClosure> closures) {
        List<ResolvedArtifact> artifacts = new ArrayList<>();
        for (PreparedConnectorClosure closure : closures) {
            artifacts.add(new ResolvedArtifact(
                    closure.primary().scope(),
                    ArtifactRole.SUBJECT_CONNECTOR,
                    closure.connectorPath() + "/artifact",
                    closure.primary().sourceReference(),
                    closure.primary().sourcePath(),
                    closure.primary().preparedPath(),
                    closure.primary().sha256()));
        }
        return new PreparedScenarioPlan(
                scenarioPlan,
                new ArtifactWorkspace(temporary, temporary.resolve(
                        "plan-" + scenarioPlan.scenario().template().name())),
                artifacts,
                closures,
                false);
    }

    private ResolvedScenarioPlan resolve(ObjectNode document) {
        String name = document.at("/meta/name").textValue();
        Path source = resource("minimal.yaml").resolveSibling(name + ".yaml");
        ScenarioSpecification scenario = loader.validateScenarioDocument(source, document);
        ObjectNode expectedDocument = loader.loadExpectedResult(
                resource("minimal.expected.yaml")).document();
        expectedDocument.withObject("meta")
                .put("name", name + ".expected")
                .put("scenario", name);
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                source.resolveSibling(name + ".expected.yaml"), expectedDocument);
        return new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected), ResolutionRequest.none());
    }

    private Path write(String name, String value) throws IOException {
        return Files.writeString(temporary.resolve(name), value, StandardCharsets.UTF_8);
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return java.util.HexFormat.of().formatHex(digest.digest(Files.readAllBytes(path)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("Java must provide SHA-256", impossible);
        }
    }

    private static String uncheckedSha256(Path path) {
        try {
            return sha256(path);
        } catch (IOException exception) {
            throw new AssertionError(exception);
        }
    }

    private static void assertSingleCode(
            ArtifactResolutionException exception,
            String code) {
        assertEquals(1, exception.issues().size(), () -> exception.issues().toString());
        assertEquals(code, exception.issues().getFirst().code());
        assertFalse(exception.issues().getFirst().message().isBlank());
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
