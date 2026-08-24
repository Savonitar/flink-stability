package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArtifactPlanResolverConnectorClosureTest {
    private static final String KAFKA_COORDINATE =
            "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2";
    private static final String CONNECTOR_PATH = "$/subject/connectors/kafka/artifact";
    private static final String DEPENDENCIES_PATH =
            "$/subject/connectors/kafka/runtime_dependencies/";

    private final SpecificationLoader loader = new SpecificationLoader();

    @TempDir
    Path artifactRoot;

    @Test
    void autoModeStagesTheFullMavenClosureAndPreservesImmutableEvidence()
            throws IOException {
        Path primaryJar = createJar(artifactRoot.resolve("maven/primary.jar"), false, "same");
        Path childJar = copy(primaryJar, artifactRoot.resolve("maven/child.jar"));
        Path grandchildJar = copy(primaryJar, artifactRoot.resolve("maven/grandchild.jar"));
        createJar(artifactRoot.resolve("job.jar"), true, "job");

        MavenCoordinate root = MavenCoordinate.parse(KAFKA_COORDINATE);
        MavenArtifactIdentity primary = identity(root);
        MavenArtifactIdentity child = identity("org.example", "child", "1.0");
        MavenArtifactIdentity grandchild = identity("org.example", "grandchild", "1.0");
        MavenArtifactIdentity omittedChild = identity("org.example", "child", "0.9");
        Path pom = Files.writeString(
                artifactRoot.resolve("maven/primary.pom"), "<project/>", StandardCharsets.UTF_8);
        MavenPomEvidence pomEvidence = new MavenPomEvidence(
                new MavenArtifactIdentity(
                        primary.groupId(), primary.artifactId(), "pom", "", primary.version()),
                pom,
                sha256(pom));
        MavenConflictDecision conflict = new MavenConflictDecision(
                omittedChild,
                child,
                MavenConflictDecision.Reason.FIRST_BREADTH_FIRST,
                1,
                1,
                0,
                0,
                List.of(primary, omittedChild),
                List.of(primary, child));
        MavenRuntimeClosure graph = new MavenRuntimeClosure(
                List.of(
                        jar(primary, 0, 0, List.of(primary), primaryJar),
                        jar(child, 1, 0, List.of(primary, child), childJar),
                        jar(grandchild, 2, 0,
                                List.of(primary, child, grandchild), grandchildJar)),
                List.of(conflict),
                List.of(pomEvidence));
        RecordingLookup lookup = new RecordingLookup(Map.of(), (roots, offline) -> graph);

        PreparedScenarioPlan prepared = new ArtifactPlanResolver(lookup).resolve(
                plainPlan("auto-closure", document -> setCoreArtifacts(
                        document, KAFKA_COORDINATE, "job.jar")),
                ArtifactResolutionOptions.online(artifactRoot));

        assertTrue(lookup.primaryCalls.isEmpty());
        assertEquals(List.of(List.of(new MavenRuntimeRoot(0, root))), lookup.closureCalls);
        PreparedConnectorClosure closure = prepared.connectorClosure(
                ScenarioSide.SINGLE, "kafka").orElseThrow();
        assertEquals(ConnectorDependencyMode.AUTO, closure.dependencyMode());
        assertEquals(List.of(primary, child, grandchild), closure.classpath().stream()
                .map(entry -> entry.mavenIdentity().orElseThrow())
                .toList());
        assertEquals(List.of(0, 1, 2), closure.classpath().stream()
                .map(PreparedConnectorArtifact::classpathIndex)
                .toList());
        assertEquals(1, closure.classpath().stream()
                .map(PreparedConnectorArtifact::preparedPath)
                .distinct()
                .count(), "physical staging may share bytes without merging Maven identities");
        assertSame(conflict, closure.conflicts().getFirst());
        assertSame(pomEvidence, closure.consultedPoms().getFirst());
        assertEquals(KAFKA_COORDINATE,
                closure.dependencies().getFirst().origins().getFirst().declaredReference());
        assertTrue(closure.dependencies().getFirst().origins().getFirst().primaryRoot());
        assertTrue(closure.dependencies().getFirst()
                .origins().getFirst().runtimeDependencyIndex().isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> closure.classpath().clear());
        assertThrows(UnsupportedOperationException.class, () -> closure.conflicts().clear());
        assertThrows(UnsupportedOperationException.class, () -> closure.consultedPoms().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> closure.dependencies().getFirst().origins().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> prepared.connectorClosures().clear());
        assertTrue(closure.classpath().stream().allMatch(entry ->
                Files.isRegularFile(entry.preparedPath())
                        && entry.sha256().equals(uncheckedSha256(entry.preparedPath()))));
    }

    @Test
    void explicitEmptyResolvesOnlyThePrimaryThroughTheCompatibilityLookup()
            throws IOException {
        Path primaryJar = createJar(artifactRoot.resolve("maven/primary.jar"), false, "primary");
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        MavenCoordinate coordinate = MavenCoordinate.parse(KAFKA_COORDINATE);
        RecordingLookup lookup = new RecordingLookup(
                Map.of(coordinate, primaryJar),
                (roots, offline) -> {
                    throw new AssertionError("An explicit empty list must not resolve a graph");
                });

        PreparedScenarioPlan prepared = new ArtifactPlanResolver(lookup).resolve(
                plainPlan("explicit-empty", document -> {
                    setCoreArtifacts(document, KAFKA_COORDINATE, "job.jar");
                    connector(document).putArray("runtime_dependencies");
                }),
                new ArtifactResolutionOptions(artifactRoot, true));

        assertEquals(List.of(new PrimaryCall(coordinate, true)), lookup.primaryCalls);
        assertTrue(lookup.closureCalls.isEmpty());
        PreparedConnectorClosure closure = prepared.connectorClosure(
                ScenarioSide.SINGLE, "kafka").orElseThrow();
        assertEquals(ConnectorDependencyMode.EXPLICIT, closure.dependencyMode());
        assertEquals(1, closure.classpath().size());
        assertSame(closure.primary(), closure.classpath().getFirst());
        assertTrue(closure.dependencies().isEmpty());
        assertEquals(List.of(identity(coordinate)), closure.primary().mavenDependencyPath());
    }

    @Test
    void explicitMixedRootsUseDeclaredIndexesAndGlobalBreadthFirstOrder()
            throws IOException {
        Path primaryJar = createJar(artifactRoot.resolve("connector.jar"), false, "primary");
        Path localA = createJar(artifactRoot.resolve("local-a.jar"), false, "local-a");
        Path localC = createJar(artifactRoot.resolve("local-c.jar"), false, "local-c");
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        Path sharedMavenBytes = createJar(
                artifactRoot.resolve("maven/shared.jar"), false, "maven-shared");
        Path rootBJar = copy(sharedMavenBytes, artifactRoot.resolve("maven/root-b.jar"));
        Path rootDJar = copy(sharedMavenBytes, artifactRoot.resolve("maven/root-d.jar"));
        Path childBJar = copy(sharedMavenBytes, artifactRoot.resolve("maven/child-b.jar"));
        Path childDJar = copy(sharedMavenBytes, artifactRoot.resolve("maven/child-d.jar"));

        MavenCoordinate rootB = MavenCoordinate.parse("maven:org.example:root-b:1.0");
        MavenCoordinate rootD = MavenCoordinate.parse("maven:org.example:root-d:1.0");
        MavenArtifactIdentity b = identity(rootB);
        MavenArtifactIdentity d = identity(rootD);
        MavenArtifactIdentity childB = identity("org.example", "child-b", "1.0");
        MavenArtifactIdentity childD = identity("org.example", "child-d", "1.0");
        MavenRuntimeClosure graph = new MavenRuntimeClosure(
                List.of(
                        jar(b, 0, 1, List.of(b), rootBJar),
                        jar(d, 0, 3, List.of(d), rootDJar),
                        jar(childB, 1, 1, List.of(b, childB), childBJar),
                        jar(childD, 1, 3, List.of(d, childD), childDJar)),
                List.of(),
                List.of());
        RecordingLookup lookup = new RecordingLookup(Map.of(), (roots, offline) -> graph);

        PreparedScenarioPlan prepared = new ArtifactPlanResolver(lookup).resolve(
                plainPlan("explicit-mixed", document -> {
                    setCoreArtifacts(document, "connector.jar", "job.jar");
                    connector(document).putArray("runtime_dependencies")
                            .add("local-a.jar")
                            .add(rootB.declaredReference())
                            .add("local-c.jar")
                            .add(rootD.declaredReference());
                }),
                ArtifactResolutionOptions.online(artifactRoot));

        assertEquals(List.of(List.of(
                new MavenRuntimeRoot(1, rootB),
                new MavenRuntimeRoot(3, rootD))), lookup.closureCalls);
        PreparedConnectorClosure closure = prepared.connectorClosure(
                ScenarioSide.SINGLE, "kafka").orElseThrow();
        assertEquals(List.of(
                        primaryJar.toRealPath(),
                        localA.toRealPath(),
                        rootBJar.toRealPath(),
                        localC.toRealPath(),
                        rootDJar.toRealPath(),
                        childBJar.toRealPath(),
                        childDJar.toRealPath()),
                closure.classpath().stream().map(PreparedConnectorArtifact::sourcePath).toList());
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6), closure.classpath().stream()
                .map(PreparedConnectorArtifact::classpathIndex)
                .toList());
        assertEquals(List.of(0, 1, 2, 3, 1, 3), closure.dependencies().stream()
                .map(entry -> entry.originRootIndex().orElseThrow())
                .toList());
        assertEquals(List.of(0, 0, 0, 0, 1, 1), closure.dependencies().stream()
                .map(entry -> entry.dependencyDepth().orElseThrow())
                .toList());
        assertEquals(4, closure.dependencies().stream()
                .filter(entry -> entry.mavenIdentity().isPresent())
                .count(), "identical Maven bytes must retain four logical identities");
    }

    @Test
    void duplicateCanonicalRootsRejectButDistinctLocalSourcesWithOneHashEmitOnce()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, "primary");
        Path leaf = createJar(artifactRoot.resolve("leaf.jar"), false, "leaf");
        createJar(artifactRoot.resolve("job.jar"), true, "job");

        ArtifactResolutionException duplicate = assertThrows(
                ArtifactResolutionException.class,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("duplicate-local-root", document -> {
                            setCoreArtifacts(document, "connector.jar", "job.jar");
                            connector(document).putArray("runtime_dependencies")
                                    .add("leaf.jar")
                                    .add("nested/../leaf.jar");
                        }),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(
                duplicate,
                "artifact.connector.runtime-dependency-duplicate",
                DEPENDENCIES_PATH + "1");
        assertPreparedWorkspacesEmpty();

        Path sameBytes = copy(leaf, artifactRoot.resolve("same-bytes.jar"));
        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                plainPlan("same-local-hash", document -> {
                    setCoreArtifacts(document, "connector.jar", "job.jar");
                    connector(document).putArray("runtime_dependencies")
                            .add("leaf.jar")
                            .add("same-bytes.jar");
                }),
                ArtifactResolutionOptions.online(artifactRoot));
        PreparedConnectorArtifact emitted = prepared.connectorClosure(
                        ScenarioSide.SINGLE, "kafka").orElseThrow()
                .dependencies().getFirst();
        assertEquals(leaf.toRealPath(), emitted.sourcePath());
        assertEquals(sha256(sameBytes), emitted.sha256());
        assertEquals(List.of(0, 1), emitted.origins().stream()
                .map(origin -> origin.runtimeDependencyIndex().orElseThrow())
                .toList());
        assertEquals(List.of("leaf.jar", "same-bytes.jar"), emitted.origins().stream()
                .map(PreparedConnectorOrigin::declaredReference)
                .toList());
    }

    @Test
    void experimentSharesCanonicalDependenciesAndTheirTwoOriginsAcrossVariedPrimaries()
            throws IOException {
        Path mavenPrimary = createJar(
                artifactRoot.resolve("maven/primary.jar"), false, "maven-primary");
        createJar(artifactRoot.resolve("connector.jar"), false, "local-primary");
        Path shared = createJar(artifactRoot.resolve("shared-1.jar"), false, "shared");
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        MavenCoordinate coordinate = MavenCoordinate.parse(KAFKA_COORDINATE);
        RecordingLookup lookup = new RecordingLookup(
                Map.of(coordinate, mavenPrimary),
                (roots, offline) -> {
                    throw new AssertionError("Explicit local dependency roots need no graph");
                });

        PreparedScenarioPlan prepared = new ArtifactPlanResolver(lookup).resolve(
                experimentPlan("shared-canonical-dependencies", document -> {
                    setCoreArtifacts(document, "${connector_artifact}", "job.jar");
                    connector(document).putArray("runtime_dependencies")
                            .add("${dependency_artifact}");
                    parameter(document, "connector_artifact", KAFKA_COORDINATE);
                    parameter(document, "dependency_artifact", "nested/../shared-*.jar");
                    ObjectNode experiment = document.putObject("experiment");
                    experiment.put("claim", "Only the connector source and spelling vary.");
                    experiment.putArray("varies")
                            .add("connector_artifact")
                            .add("dependency_artifact");
                    experiment.putObject("baseline")
                            .put("connector_artifact", KAFKA_COORDINATE)
                            .put("dependency_artifact", "nested/../shared-*.jar");
                    experiment.putObject("candidate")
                            .put("connector_artifact", "connector.jar")
                            .put("dependency_artifact", "shared-1.jar");
                }),
                ArtifactResolutionOptions.online(artifactRoot));

        PreparedConnectorClosure baseline = prepared.connectorClosure(
                ScenarioSide.BASELINE, "kafka").orElseThrow();
        PreparedConnectorClosure candidate = prepared.connectorClosure(
                ScenarioSide.CANDIDATE, "kafka").orElseThrow();
        assertNotSame(baseline, candidate);
        assertEquals(ResolutionScope.BASELINE, baseline.primary().scope());
        assertEquals(ResolutionScope.CANDIDATE, candidate.primary().scope());
        PreparedConnectorArtifact sharedDependency = baseline.dependencies().getFirst();
        assertSame(sharedDependency, candidate.dependencies().getFirst());
        assertEquals(ResolutionScope.COMMON, sharedDependency.scope());
        assertEquals(shared.toRealPath(), sharedDependency.sourcePath());
        assertEquals(List.of(ResolutionScope.BASELINE, ResolutionScope.CANDIDATE),
                sharedDependency.origins().stream()
                        .map(PreparedConnectorOrigin::scope)
                        .toList());
        assertEquals(List.of("nested/../shared-*.jar", "shared-1.jar"),
                sharedDependency.origins().stream()
                        .map(PreparedConnectorOrigin::declaredReference)
                        .toList());
        assertEquals(List.of(new PrimaryCall(coordinate, false)), lookup.primaryCalls);
        assertTrue(lookup.closureCalls.isEmpty());
    }

    @Test
    void reorderedCanonicalRootSetsRemainSideSpecific() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, "primary");
        createJar(artifactRoot.resolve("a.jar"), false, "a");
        createJar(artifactRoot.resolve("b.jar"), false, "b");
        createJar(artifactRoot.resolve("job.jar"), true, "job");

        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                experimentPlan("reordered-dependencies", document -> {
                    setCoreArtifacts(document, "connector.jar", "job.jar");
                    connector(document).putArray("runtime_dependencies")
                            .add("${first_dependency}")
                            .add("${second_dependency}");
                    parameter(document, "first_dependency", "a.jar");
                    parameter(document, "second_dependency", "b.jar");
                    ObjectNode experiment = document.putObject("experiment");
                    experiment.put("claim", "Only dependency order varies.");
                    experiment.putArray("varies")
                            .add("first_dependency")
                            .add("second_dependency");
                    experiment.putObject("baseline")
                            .put("first_dependency", "a.jar")
                            .put("second_dependency", "b.jar");
                    experiment.putObject("candidate")
                            .put("first_dependency", "b.jar")
                            .put("second_dependency", "a.jar");
                }),
                ArtifactResolutionOptions.online(artifactRoot));

        PreparedConnectorClosure baseline = prepared.connectorClosure(
                ScenarioSide.BASELINE, "kafka").orElseThrow();
        PreparedConnectorClosure candidate = prepared.connectorClosure(
                ScenarioSide.CANDIDATE, "kafka").orElseThrow();
        assertEquals(List.of(ResolutionScope.BASELINE, ResolutionScope.BASELINE),
                baseline.dependencies().stream().map(PreparedConnectorArtifact::scope).toList());
        assertEquals(List.of(ResolutionScope.CANDIDATE, ResolutionScope.CANDIDATE),
                candidate.dependencies().stream().map(PreparedConnectorArtifact::scope).toList());
        assertEquals(List.of("a.jar", "b.jar"), baseline.dependencies().stream()
                .map(PreparedConnectorArtifact::sourceReference).toList());
        assertEquals(List.of("b.jar", "a.jar"), candidate.dependencies().stream()
                .map(PreparedConnectorArtifact::sourceReference).toList());
    }

    @Test
    void suiteCachesMavenClosureWorkButKeepsEntryPlansIndependent() throws IOException {
        Path primaryJar = createJar(artifactRoot.resolve("maven/primary.jar"), false, "primary");
        Path childJar = createJar(artifactRoot.resolve("maven/child.jar"), false, "child");
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        MavenCoordinate coordinate = MavenCoordinate.parse(KAFKA_COORDINATE);
        MavenArtifactIdentity primary = identity(coordinate);
        MavenArtifactIdentity child = identity("org.example", "child", "1.0");
        MavenRuntimeClosure graph = new MavenRuntimeClosure(
                List.of(
                        jar(primary, 0, 0, List.of(primary), primaryJar),
                        jar(child, 1, 0, List.of(primary, child), childJar)),
                List.of(),
                List.of());
        RecordingLookup lookup = new RecordingLookup(Map.of(), (roots, offline) -> graph);
        ScenarioBundle alpha = bundle("closure-alpha", document -> setCoreArtifacts(
                document, KAFKA_COORDINATE, "job.jar"), false);
        ScenarioBundle beta = bundle("closure-beta", document -> setCoreArtifacts(
                document, KAFKA_COORDINATE, "job.jar"), false);
        SuiteSpecification suite = suite("closure-suite", entries -> {
            entry(entries, "closure-alpha");
            entry(entries, "closure-beta");
        });
        ResolvedSuitePlan suitePlan = new SuitePlanResolver().resolve(
                catalog(suite, alpha, beta), suite.name());

        PreparedSuitePlan prepared = new ArtifactPlanResolver(lookup).resolve(
                suitePlan, ArtifactResolutionOptions.online(artifactRoot));

        assertEquals(1, lookup.closureCalls.size());
        PreparedConnectorClosure first = prepared.entries().getFirst().scenario()
                .connectorClosure(ScenarioSide.SINGLE, "kafka").orElseThrow();
        PreparedConnectorClosure second = prepared.entries().get(1).scenario()
                .connectorClosure(ScenarioSide.SINGLE, "kafka").orElseThrow();
        assertNotSame(first, second);
        assertEquals(first.classpath().stream()
                        .map(PreparedConnectorArtifact::preparedPath).toList(),
                second.classpath().stream()
                        .map(PreparedConnectorArtifact::preparedPath).toList());
        Path sharedWorkspace = prepared.preparationRoot();
        prepared.entries().getFirst().scenario().close();
        assertTrue(Files.isDirectory(sharedWorkspace));
        prepared.close();
        assertFalse(Files.exists(sharedWorkspace));
    }

    @Test
    void suiteRejectsDifferentBytesForOneCoordinateAcrossAutoAndExplicitModes()
            throws IOException {
        Path primaryJar = createJar(artifactRoot.resolve("maven/primary.jar"), false, "first");
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        MavenCoordinate coordinate = MavenCoordinate.parse(KAFKA_COORDINATE);
        MavenArtifactIdentity primary = identity(coordinate);
        MavenRuntimeClosure graph = new MavenRuntimeClosure(
                List.of(jar(primary, 0, 0, List.of(primary), primaryJar)),
                List.of(),
                List.of());
        RecordingLookup lookup = new RecordingLookup(
                Map.of(coordinate, primaryJar),
                (roots, offline) -> graph);
        lookup.beforePrimaryResolve = ignored -> createJar(
                primaryJar, false, "changed-before-explicit-resolution");
        ScenarioBundle auto = bundle("cache-auto", document -> setCoreArtifacts(
                document, KAFKA_COORDINATE, "job.jar"), false);
        ScenarioBundle explicit = bundle("cache-explicit", document -> {
            setCoreArtifacts(document, KAFKA_COORDINATE, "job.jar");
            connector(document).putArray("runtime_dependencies");
        }, false);
        SuiteSpecification suite = suite("cross-mode-cache-suite", entries -> {
            entry(entries, "cache-auto");
            entry(entries, "cache-explicit");
        });
        ResolvedSuitePlan suitePlan = new SuitePlanResolver().resolve(
                catalog(suite, auto, explicit), suite.name());

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> new ArtifactPlanResolver(lookup).resolve(
                        suitePlan, ArtifactResolutionOptions.online(artifactRoot)));

        assertEquals(1, exception.issues().size());
        assertEquals("cache-explicit", exception.issues().getFirst().entry().entryId());
        assertEquals("artifact.maven.invalid-closure", exception.issues().getFirst().code());
        assertEquals(CONNECTOR_PATH, exception.issues().getFirst().path());
        assertPreparedWorkspacesEmpty();
    }

    @Test
    void suiteRehashesConsultedPomsBeforeReusingACachedClosure() throws IOException {
        Path primaryA = createJar(artifactRoot.resolve("maven/a.jar"), false, "a");
        Path primaryB = createJar(artifactRoot.resolve("maven/b.jar"), false, "b");
        Path pom = Files.writeString(
                artifactRoot.resolve("maven/a.pom"), "<project/>", StandardCharsets.UTF_8);
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        MavenCoordinate coordinateA = MavenCoordinate.parse(KAFKA_COORDINATE);
        MavenCoordinate coordinateB = MavenCoordinate.parse(
                "maven:org.apache.flink:flink-connector-kafka:5.0.1-2.2");
        MavenArtifactIdentity primary = identity(coordinateA);
        MavenPomEvidence pomEvidence = new MavenPomEvidence(
                new MavenArtifactIdentity(
                        primary.groupId(), primary.artifactId(), "pom", "", primary.version()),
                pom,
                sha256(pom));
        MavenRuntimeClosure graph = new MavenRuntimeClosure(
                List.of(jar(primary, 0, 0, List.of(primary), primaryA)),
                List.of(),
                List.of(pomEvidence));
        RecordingLookup lookup = new RecordingLookup(
                Map.of(coordinateB, primaryB),
                (roots, offline) -> graph);
        lookup.beforePrimaryResolve = coordinate -> {
            if (coordinate.equals(coordinateB)) {
                Files.writeString(pom, "<changed/>", StandardCharsets.UTF_8);
            }
        };
        ScenarioBundle first = bundle("pom-cache-first", document -> setCoreArtifacts(
                document, KAFKA_COORDINATE, "job.jar"), false);
        ScenarioBundle second = bundle("pom-cache-second", document -> {
            setCoreArtifacts(document, KAFKA_COORDINATE, "job.jar");
            ObjectNode connectors = document.withObject("subject").withObject("connectors");
            explicitConnector(
                    connectors,
                    "aaa",
                    "maven:org.apache.flink:flink-connector-kafka:5.0.1-2.2");
            ArrayNode aliases = ((ObjectNode) document.at("/workload/jobs/0"))
                    .putArray("connectors");
            aliases.add("aaa").add("kafka");
        }, false);
        SuiteSpecification suite = suite("pom-cache-suite", entries -> {
            entry(entries, "pom-cache-first");
            entry(entries, "pom-cache-second");
        });
        ResolvedSuitePlan suitePlan = new SuitePlanResolver().resolve(
                catalog(suite, first, second), suite.name());

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> new ArtifactPlanResolver(lookup).resolve(
                        suitePlan, ArtifactResolutionOptions.online(artifactRoot)));

        assertEquals(1, exception.issues().size());
        assertEquals("pom-cache-second", exception.issues().getFirst().entry().entryId());
        assertEquals("artifact.maven.invalid-closure", exception.issues().getFirst().code());
        assertEquals(CONNECTOR_PATH, exception.issues().getFirst().path());
        assertEquals(1, lookup.closureCalls.size(), "the second entry must hit the closure cache");
        assertPreparedWorkspacesEmpty();
    }

    @Test
    void closureLookupFailuresAndChangedSelectedBytesAreTypedAndCleanedUp()
            throws IOException {
        Path primaryJar = createJar(artifactRoot.resolve("maven/primary.jar"), false, "primary");
        Path childJar = createJar(artifactRoot.resolve("maven/child.jar"), false, "child");
        createJar(artifactRoot.resolve("connector.jar"), false, "local-primary");
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        MavenCoordinate coordinate = MavenCoordinate.parse(KAFKA_COORDINATE);
        RecordingLookup failedLookup = new RecordingLookup(Map.of(), (roots, offline) -> {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                    "invalid graph");
        });

        ArtifactResolutionException invalidClosure = assertThrows(
                ArtifactResolutionException.class,
                () -> new ArtifactPlanResolver(failedLookup).resolve(
                        plainPlan("invalid-closure", document -> setCoreArtifacts(
                                document, KAFKA_COORDINATE, "job.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(invalidClosure, "artifact.maven.invalid-closure", CONNECTOR_PATH);
        assertPreparedWorkspacesEmpty();

        ArtifactResolutionException nullPrimary = assertThrows(
                ArtifactResolutionException.class,
                () -> new ArtifactPlanResolver((ignored, offline) -> null).resolve(
                        plainPlan("null-primary", document -> {
                            setCoreArtifacts(document, KAFKA_COORDINATE, "job.jar");
                            connector(document).putArray("runtime_dependencies");
                        }),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(nullPrimary, "artifact.maven.invalid-closure", CONNECTOR_PATH);
        assertPreparedWorkspacesEmpty();

        RecordingLookup jointFailureLookup = new RecordingLookup(Map.of(), (roots, offline) -> {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.NOT_FOUND,
                    "one jointly resolved root is missing");
        });
        ArtifactResolutionException jointFailure = assertThrows(
                ArtifactResolutionException.class,
                () -> new ArtifactPlanResolver(jointFailureLookup).resolve(
                        plainPlan("joint-root-failure", document -> {
                            setCoreArtifacts(document, "connector.jar", "job.jar");
                            connector(document).putArray("runtime_dependencies")
                                    .add("maven:org.example:healthy:1.0")
                                    .add("maven:org.example:missing:1.0");
                        }),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(
                jointFailure,
                "artifact.maven.not-found",
                "$/subject/connectors/kafka/runtime_dependencies");
        assertPreparedWorkspacesEmpty();

        MavenArtifactIdentity primary = identity(coordinate);
        MavenArtifactIdentity child = identity("org.example", "child", "1.0");
        MavenRuntimeClosure changedGraph = new MavenRuntimeClosure(
                List.of(
                        jar(primary, 0, 0, List.of(primary), primaryJar),
                        new ResolvedMavenJar(
                                child,
                                1,
                                0,
                                "runtime",
                                List.of(primary, child),
                                childJar,
                                "0".repeat(64))),
                List.of(),
                List.of());
        RecordingLookup changedLookup = new RecordingLookup(
                Map.of(), (roots, offline) -> changedGraph);
        ArtifactResolutionException changed = assertThrows(
                ArtifactResolutionException.class,
                () -> new ArtifactPlanResolver(changedLookup).resolve(
                        plainPlan("changed-closure-bytes", document -> setCoreArtifacts(
                                document, KAFKA_COORDINATE, "job.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(changed, "artifact.staging.source-changed", CONNECTOR_PATH);
        assertPreparedWorkspacesEmpty();

        Path invalidJar = Files.writeString(
                artifactRoot.resolve("maven/invalid.jar"),
                "not a jar",
                StandardCharsets.UTF_8);
        MavenRuntimeClosure corruptGraph = new MavenRuntimeClosure(
                List.of(
                        jar(primary, 0, 0, List.of(primary), primaryJar),
                        jar(child, 1, 0, List.of(primary, child), invalidJar)),
                List.of(),
                List.of());
        RecordingLookup corruptLookup = new RecordingLookup(
                Map.of(), (roots, offline) -> corruptGraph);
        ArtifactResolutionException corrupt = assertThrows(
                ArtifactResolutionException.class,
                () -> new ArtifactPlanResolver(corruptLookup).resolve(
                        plainPlan("corrupt-closure-jar", document -> setCoreArtifacts(
                                document, KAFKA_COORDINATE, "job.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(corrupt, "artifact.jar.invalid", CONNECTOR_PATH);
        assertPreparedWorkspacesEmpty();
    }

    @Test
    void cachedExplicitMavenPrimaryRejectsMutationWithinOnePreparation() throws IOException {
        Path primaryA = createJar(artifactRoot.resolve("maven/a.jar"), false, "first-a");
        Path primaryB = createJar(artifactRoot.resolve("maven/b.jar"), false, "b");
        createJar(artifactRoot.resolve("job.jar"), true, "job");
        MavenCoordinate coordinateA = MavenCoordinate.parse(KAFKA_COORDINATE);
        MavenCoordinate coordinateB = MavenCoordinate.parse(
                "maven:org.apache.flink:flink-connector-kafka:5.0.1-2.2");
        RecordingLookup lookup = new RecordingLookup(Map.of(
                coordinateA, primaryA,
                coordinateB, primaryB), (roots, offline) -> {
                    throw new AssertionError("All three connector declarations are explicit-empty");
                });
        lookup.beforePrimaryResolve = coordinate -> {
            if (coordinate.equals(coordinateB)) {
                createJar(primaryA, false, "mutated-a");
            }
        };

        ArtifactResolutionException exception = assertThrows(
                ArtifactResolutionException.class,
                () -> new ArtifactPlanResolver(lookup).resolve(
                        plainPlan("mutated-primary-cache", document -> {
                            setCoreArtifacts(document, "connector.jar", "job.jar");
                            ObjectNode connectors = document.withObject("subject")
                                    .withObject("connectors");
                            connectors.removeAll();
                            explicitConnector(connectors, "aaa", KAFKA_COORDINATE);
                            explicitConnector(
                                    connectors,
                                    "bbb",
                                    "maven:org.apache.flink:flink-connector-kafka:5.0.1-2.2");
                            explicitConnector(connectors, "ccc", KAFKA_COORDINATE);
                            ArrayNode aliases = ((ObjectNode) document.at("/workload/jobs/0"))
                                    .putArray("connectors");
                            aliases.add("aaa").add("bbb").add("ccc");
                        }),
                        ArtifactResolutionOptions.online(artifactRoot)));

        assertSingleIssue(
                exception,
                "artifact.staging.source-changed",
                "$/subject/connectors/ccc/artifact");
        assertEquals(List.of(
                new PrimaryCall(coordinateA, false),
                new PrimaryCall(coordinateB, false)), lookup.primaryCalls);
        assertPreparedWorkspacesEmpty();
    }

    @Test
    void preparedPrimaryModelRejectsAContradictoryMavenIdentityPath() throws IOException {
        Path jar = createJar(artifactRoot.resolve("model.jar"), false, "model");
        MavenArtifactIdentity primary = identity("org.example", "primary", "1.0");
        MavenArtifactIdentity other = identity("org.example", "other", "1.0");

        assertThrows(IllegalArgumentException.class, () -> new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                0,
                true,
                -1,
                -1,
                CONNECTOR_PATH,
                "maven:org.example:primary:1.0",
                primary,
                List.of(other),
                jar,
                jar,
                sha256(jar)));
        assertThrows(IllegalArgumentException.class, () -> new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.PRIMARY,
                0,
                CONNECTOR_PATH,
                "maven:org.example:primary:1.0"));
    }

    private ResolvedScenarioPlan plainPlan(String name, Consumer<ObjectNode> changes) {
        return new ScenarioPlanResolver().resolve(
                bundle(name, changes, false), ResolutionRequest.none());
    }

    private ResolvedScenarioPlan experimentPlan(String name, Consumer<ObjectNode> changes) {
        return new ScenarioPlanResolver().resolve(
                bundle(name, changes, true), ResolutionRequest.none());
    }

    private ScenarioBundle bundle(
            String name,
            Consumer<ObjectNode> changes,
            boolean experimentExpectation) {
        Path scenarioSource = resource("minimal.yaml").resolveSibling(name + ".yaml");
        ObjectNode scenarioDocument = loader.loadScenario(resource("minimal.yaml")).document();
        scenarioDocument.withObject("meta").put("name", name);
        changes.accept(scenarioDocument);
        ScenarioSpecification scenario = loader.validateScenarioDocument(
                scenarioSource, scenarioDocument);

        Path expectedSource = scenarioSource.resolveSibling(name + ".expected.yaml");
        ObjectNode expectedDocument = loader.loadExpectedResult(
                resource("minimal.expected.yaml")).document();
        expectedDocument.withObject("meta")
                .put("name", name + ".expected")
                .put("scenario", name);
        if (experimentExpectation) {
            ObjectNode expectation = expectedDocument.withObject("default");
            expectation.removeAll();
            expectation.putObject("baseline").put("outcome", "pass");
            expectation.putObject("candidate").put("outcome", "pass");
        }
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                expectedSource, expectedDocument);
        return new ScenarioBundle(scenario, expected);
    }

    private SuiteSpecification suite(String name, Consumer<ArrayNode> entries) {
        ObjectNode document = loader.loadSuite(resource("smoke-suite.yaml")).document();
        document.withObject("meta").put("name", name);
        ArrayNode scenarios = document.withArray("scenarios");
        scenarios.removeAll();
        entries.accept(scenarios);
        return new SuiteSpecification(
                resource("smoke-suite.yaml").resolveSibling(name + ".yaml"),
                document);
    }

    private static ObjectNode entry(ArrayNode scenarios, String scenarioName) {
        return scenarios.addObject().put("scenario", scenarioName);
    }

    private static SpecificationCatalog catalog(
            SuiteSpecification suite,
            ScenarioBundle... bundles) {
        Map<String, ScenarioBundle> scenarios = new LinkedHashMap<>();
        Arrays.stream(bundles).forEach(bundle ->
                scenarios.put(bundle.scenario().name(), bundle));
        return new SpecificationCatalog(scenarios, Map.of(suite.name(), suite));
    }

    private static void setCoreArtifacts(
            ObjectNode document,
            String connectorReference,
            String jobReference) {
        ObjectNode connector = connector(document);
        connector.put("artifact", connectorReference);
        connector.remove("runtime_dependencies");
        if (!connectorReference.startsWith("maven:")
                && !connectorReference.contains("${")) {
            connector.putArray("runtime_dependencies");
        }
        ((ObjectNode) document.at("/workload/jobs/0")).put("jar", jobReference);
    }

    private static ObjectNode connector(ObjectNode document) {
        return (ObjectNode) document.at("/subject/connectors/kafka");
    }

    private static void parameter(ObjectNode document, String name, String defaultValue) {
        document.withObject("parameters")
                .putObject(name)
                .put("type", "string")
                .put("default", defaultValue);
    }

    private static void explicitConnector(
            ObjectNode connectors,
            String alias,
            String artifact) {
        ObjectNode connector = connectors.putObject(alias);
        connector.put("artifact", artifact);
        connector.putArray("runtime_dependencies");
    }

    private static MavenArtifactIdentity identity(MavenCoordinate coordinate) {
        return identity(coordinate.groupId(), coordinate.artifactId(), coordinate.version());
    }

    private static MavenArtifactIdentity identity(
            String groupId,
            String artifactId,
            String version) {
        return new MavenArtifactIdentity(groupId, artifactId, "jar", "", version);
    }

    private static ResolvedMavenJar jar(
            MavenArtifactIdentity identity,
            int depth,
            int originRootIndex,
            List<MavenArtifactIdentity> path,
            Path source) throws IOException {
        return new ResolvedMavenJar(
                identity,
                depth,
                originRootIndex,
                depth == 0 ? "compile" : "runtime",
                path,
                source,
                sha256(source));
    }

    private static Path createJar(
            Path path,
            boolean mainClass,
            String payload) throws IOException {
        Files.createDirectories(path.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (mainClass) {
            manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
        }
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            output.putNextEntry(new JarEntry("payload.txt"));
            output.write(payload.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return path;
    }

    private static Path copy(Path source, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        return Files.copy(source, target);
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return java.util.HexFormat.of().formatHex(digest.digest(Files.readAllBytes(path)));
        } catch (NoSuchAlgorithmException exception) {
            throw new AssertionError("Java must provide SHA-256", exception);
        }
    }

    private static String uncheckedSha256(Path path) {
        try {
            return sha256(path);
        } catch (IOException exception) {
            throw new AssertionError(exception);
        }
    }

    private void assertPreparedWorkspacesEmpty() throws IOException {
        Path preparationParent = artifactRoot.resolve(".flink-stability/artifacts/prepared");
        if (!Files.exists(preparationParent)) {
            return;
        }
        try (var descendants = Files.walk(preparationParent)) {
            assertTrue(descendants
                    .filter(path -> !path.equals(preparationParent))
                    .findAny()
                    .isEmpty());
        }
    }

    private static void assertSingleIssue(
            ArtifactResolutionException exception,
            String code,
            String path) {
        assertEquals(1, exception.issues().size(), () -> exception.issues().toString());
        assertEquals(code, exception.issues().getFirst().code());
        assertEquals(path, exception.issues().getFirst().path());
        assertFalse(exception.issues().getFirst().message().isBlank());
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }

    @FunctionalInterface
    private interface ClosureResolver {
        MavenRuntimeClosure resolve(
                List<MavenRuntimeRoot> roots,
                boolean offline) throws MavenArtifactLookupException;
    }

    @FunctionalInterface
    private interface PrimaryHook {
        void beforeResolve(MavenCoordinate coordinate) throws IOException;
    }

    private static final class RecordingLookup implements MavenArtifactLookup {
        private final Map<MavenCoordinate, Path> primaryResults;
        private final ClosureResolver closureResolver;
        private final List<PrimaryCall> primaryCalls = new ArrayList<>();
        private final List<List<MavenRuntimeRoot>> closureCalls = new ArrayList<>();
        private PrimaryHook beforePrimaryResolve = coordinate -> {};

        private RecordingLookup(
                Map<MavenCoordinate, Path> primaryResults,
                ClosureResolver closureResolver) {
            this.primaryResults = Map.copyOf(primaryResults);
            this.closureResolver = closureResolver;
        }

        @Override
        public Path resolve(
                MavenCoordinate coordinate,
                boolean offline) throws MavenArtifactLookupException {
            primaryCalls.add(new PrimaryCall(coordinate, offline));
            try {
                beforePrimaryResolve.beforeResolve(coordinate);
            } catch (IOException exception) {
                throw new MavenArtifactLookupException(
                        MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                        "Test primary hook failed",
                        exception);
            }
            Path result = primaryResults.get(coordinate);
            if (result == null) {
                throw new AssertionError("Unexpected primary resolution for " + coordinate);
            }
            return result;
        }

        @Override
        public MavenRuntimeClosure resolveRuntimeClosure(
                List<MavenRuntimeRoot> roots,
                boolean offline) throws MavenArtifactLookupException {
            closureCalls.add(List.copyOf(roots));
            return closureResolver.resolve(roots, offline);
        }
    }

    private record PrimaryCall(MavenCoordinate coordinate, boolean offline) {}
}
