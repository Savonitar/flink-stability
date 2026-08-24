package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorClosureLockFactoryTest {
    private static final String A = "a".repeat(64);
    private static final String B = "b".repeat(64);
    private static final String C = "c".repeat(64);
    private static final String D = "d".repeat(64);
    private static final String E = "e".repeat(64);
    private static final String PRIMARY_REF =
            "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2";

    private final SpecificationLoader loader = new SpecificationLoader();
    private final ConnectorClosureLockFactory factory = new ConnectorClosureLockFactory();

    @TempDir
    Path temporary;

    @Test
    void canonicalProjectionHasAGoldenVersionedShapeAndExcludesHostEvidence() {
        PreparedConnectorClosure closure = autoClosure(
                ResolutionScope.SINGLE,
                "kafka",
                "$/subject/connectors/kafka",
                "$/subject/connectors/kafka/artifact",
                temporary.resolve("source-a.jar"),
                temporary.resolve("staged-a.jar"));

        ConnectorClosureLock lock = factory.create(
                closure, ScenarioSide.SINGLE, "flink:2.2.0");

        assertEquals(goldenProjection(), lock.canonicalProjectionJson());
        assertEquals("aad5aebc6f6d26fb63bff7ae542af06add4cd613bfa1c46dbf2bb4465a21173d",
                lock.closureSha256());
        assertEquals(List.of("org.example:a", "org.example:z"), lock.descriptors().stream()
                .map(evidence -> evidence.identity().groupId() + ":"
                        + evidence.identity().artifactId())
                .toList());

        PreparedConnectorClosure hostVariant = fullEvidenceVariant(autoClosure(
                ResolutionScope.SINGLE,
                "kafka",
                "$/subject/connectors/a-different-json-pointer",
                "$/subject/connectors/a-different-json-pointer/artifact",
                temporary.resolve("another-host/source.jar"),
                temporary.resolve("another-host/staged.jar")));
        ConnectorClosureLock equivalent = factory.create(
                hostVariant, ScenarioSide.SINGLE, "flink:2.2.0");
        assertEquals(lock.closureSha256(), equivalent.closureSha256());
        assertEquals(lock.canonicalProjectionJson(), equivalent.canonicalProjectionJson());

        assertNotEquals(lock.closureSha256(), factory.create(
                closure, ScenarioSide.SINGLE, "flink:2.2.1").closureSha256());
        assertNotEquals(lock.closureSha256(), factory.create(
                closure,
                ScenarioSide.SINGLE,
                "flink:2.2.0@sha256:" + D).closureSha256());
        assertFalse(lock.canonicalProjectionJson().contains(temporary.toString()));
        assertFalse(lock.canonicalProjectionJson().contains("$/"));
        assertFalse(lock.canonicalProjectionJson().contains("timestamp"));
        assertFalse(lock.canonicalProjectionJson().contains("runtime_resolved_oci_digest"));
        assertFalse(lock.canonicalProjectionJson().contains("\"side\""));
        assertFalse(lock.canonicalProjectionJson().contains("\"scope\""));
        assertThrows(UnsupportedOperationException.class, () -> lock.classpath().clear());
        assertThrows(UnsupportedOperationException.class, () -> lock.descriptors().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> lock.mediationDecisions().clear());
    }

    @Test
    void runtimeOciDigestAndTimestampsAreHonestlyAbsentFromCoreLockInputs() {
        for (Class<?> type : List.of(
                PreparedConnectorClosure.class,
                PreparedConnectorArtifact.class,
                PreparedConnectorOrigin.class,
                ConnectorClosureLock.class,
                ConnectorClosureLockEntry.class)) {
            List<String> fieldNames = Arrays.stream(type.getDeclaredFields())
                    .map(field -> field.getName().toLowerCase())
                    .toList();
            assertTrue(fieldNames.stream().noneMatch(name -> name.contains("timestamp")),
                    type.getSimpleName());
            assertTrue(fieldNames.stream().noneMatch(name -> name.contains("oci")),
                    type.getSimpleName());
            assertTrue(fieldNames.stream().noneMatch(name -> name.contains("runtimedigest")),
                    type.getSimpleName());
        }
    }

    @Test
    void localStableIdentityUsesContentAndNotHostPaths() {
        PreparedConnectorClosure first = localClosureAtPaths(
                "kafka",
                A,
                temporary.resolve("machine-one/cache/kafka.jar"),
                temporary.resolve("machine-one/staged/kafka.jar"));
        PreparedConnectorClosure second = localClosureAtPaths(
                "kafka",
                A,
                temporary.resolve("machine-two/cache/renamed.jar"),
                temporary.resolve("machine-two/staged/renamed.jar"));

        ConnectorClosureLock firstLock = factory.create(
                first, ScenarioSide.SINGLE, "flink:2.2.1");
        ConnectorClosureLock secondLock = factory.create(
                second, ScenarioSide.SINGLE, "flink:2.2.1");

        assertEquals(firstLock.canonicalProjectionJson(), secondLock.canonicalProjectionJson());
        assertEquals(firstLock.closureSha256(), secondLock.closureSha256());
        assertTrue(firstLock.canonicalProjectionJson().contains(
                "\"identity\":{\"kind\":\"local\"}"));
        assertFalse(firstLock.canonicalProjectionJson().contains("machine-one"));
    }

    @Test
    void exactTargetSpellingAndExplicitDeclaredDigestRemainHashSignificant() {
        PreparedConnectorClosure closure = localClosure("kafka", A);

        ConnectorClosureLock shortTag = factory.create(
                closure, ScenarioSide.SINGLE, "flink:2.2.1");
        ConnectorClosureLock qualifiedTag = factory.create(
                closure, ScenarioSide.SINGLE, "docker.io/library/flink:2.2.1");
        ConnectorClosureLock declaredDigest = factory.create(
                closure,
                ScenarioSide.SINGLE,
                "flink:2.2.1@sha256:" + D);

        assertNotEquals(shortTag.closureSha256(), qualifiedTag.closureSha256());
        assertNotEquals(shortTag.closureSha256(), declaredDigest.closureSha256());
        assertNotEquals(qualifiedTag.closureSha256(), declaredDigest.closureSha256());
        assertTrue(declaredDigest.canonicalProjectionJson().contains("@sha256:" + D));
    }

    @ParameterizedTest(name = "canonical lock field mutation: {0}")
    @EnumSource(CanonicalFieldMutation.class)
    void everyMutableCanonicalLockFieldChangesClosureSha256(
            CanonicalFieldMutation mutation) {
        ConnectorClosureLock baseline = factory.create(
                explicitCanonicalClosure(), ScenarioSide.SINGLE, "flink:2.2.0");
        ConnectorClosureLock changed = mutateCanonicalField(baseline, mutation);

        assertNotEquals(
                baseline.closureSha256(),
                changed.closureSha256(),
                mutation.name());
    }

    @Test
    void fixedProjectionFieldsAreEnforcedByThePreparedModel() {
        ConnectorClosureLock lock = factory.create(
                explicitCanonicalClosure(), ScenarioSide.SINGLE, "flink:2.2.0");

        assertTrue(lock.canonicalProjectionJson().contains(
                "\"format\":\"" + ConnectorClosureLock.FORMAT + "\""));
        assertEquals(List.of(0, 1), lock.classpath().stream()
                .map(ConnectorClosureLockEntry::classpathIndex)
                .toList());
        assertTrue(lock.classpath().stream()
                .flatMap(entry -> entry.mavenIdentity().stream())
                .allMatch(identity -> identity.extension().equals("jar")));
        assertTrue(lock.descriptors().stream()
                .allMatch(descriptor -> descriptor.identity().extension().equals("pom")));

        PreparedConnectorArtifact dependency =
                explicitCanonicalClosure().dependencies().getFirst();
        MavenArtifactIdentity invalidExtension = new MavenArtifactIdentity(
                "org.example", "dependency", "zip", "", "1.0");
        assertThrows(IllegalArgumentException.class, () -> copyArtifactWithIdentity(
                dependency, invalidExtension));
        assertThrows(IllegalArgumentException.class, () -> new MavenPomEvidence(
                jarIdentity("org.example", "descriptor", "1.0"),
                temporary.resolve("descriptor.jar"),
                A));
    }

    @Test
    void selectsSideOrCommonOriginsWithoutHashingScopeAndKeepsRootKindsDistinct() {
        MavenArtifactIdentity identity = jarIdentity(
                "org.apache.flink", "flink-connector-kafka", "5.0.0-2.2");
        List<PreparedConnectorOrigin> paired = List.of(
                new PreparedConnectorOrigin(
                        ResolutionScope.BASELINE,
                        PreparedConnectorOrigin.RootKind.PRIMARY,
                        -1,
                        "$/subject/connectors/kafka/artifact",
                        PRIMARY_REF),
                new PreparedConnectorOrigin(
                        ResolutionScope.CANDIDATE,
                        PreparedConnectorOrigin.RootKind.PRIMARY,
                        -1,
                        "$/experiment/candidate/subject/connectors/kafka/artifact",
                        PRIMARY_REF));
        PreparedConnectorArtifact primary = new PreparedConnectorArtifact(
                ResolutionScope.COMMON,
                0,
                true,
                -1,
                -1,
                paired.getFirst().declarationPath(),
                PRIMARY_REF,
                paired,
                identity,
                List.of(identity),
                temporary.resolve("primary-source.jar"),
                temporary.resolve("primary-staged.jar"),
                A);
        PreparedConnectorClosure shared = new PreparedConnectorClosure(
                ResolutionScope.COMMON,
                "kafka",
                "$/subject/connectors/kafka",
                ConnectorDependencyMode.EXPLICIT,
                primary,
                List.of(),
                List.of(),
                List.of());

        ConnectorClosureLock baseline = factory.create(
                shared, ScenarioSide.BASELINE, "flink:2.2.0");
        ConnectorClosureLock candidate = factory.create(
                shared, ScenarioSide.CANDIDATE, "flink:2.2.0");
        assertEquals(ResolutionScope.BASELINE,
                baseline.classpath().getFirst().effectiveOrigin().scope());
        assertEquals(ResolutionScope.CANDIDATE,
                candidate.classpath().getFirst().effectiveOrigin().scope());
        assertEquals(baseline.closureSha256(), candidate.closureSha256());

        PreparedConnectorOrigin commonOrigin = new PreparedConnectorOrigin(
                ResolutionScope.COMMON,
                PreparedConnectorOrigin.RootKind.PRIMARY,
                -1,
                "$/subject/connectors/kafka/artifact",
                PRIMARY_REF);
        PreparedConnectorArtifact commonPrimary = new PreparedConnectorArtifact(
                ResolutionScope.COMMON,
                0,
                true,
                -1,
                -1,
                commonOrigin.declarationPath(),
                PRIMARY_REF,
                List.of(commonOrigin),
                identity,
                List.of(identity),
                temporary.resolve("common-source.jar"),
                temporary.resolve("common-staged.jar"),
                A);
        PreparedConnectorClosure commonFallback = new PreparedConnectorClosure(
                ResolutionScope.COMMON,
                "kafka",
                "$/subject/connectors/kafka",
                ConnectorDependencyMode.EXPLICIT,
                commonPrimary,
                List.of(),
                List.of(),
                List.of());
        assertEquals(
                ResolutionScope.COMMON,
                factory.create(commonFallback, ScenarioSide.BASELINE, "flink:2.2.0")
                        .classpath().getFirst().effectiveOrigin().scope());

        PreparedConnectorOrigin commonDependencyOrigin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                2,
                "$/subject/connectors/kafka/runtime_dependencies/2",
                "dependency.jar");
        PreparedConnectorOrigin coalescedDependencyOrigin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                3,
                "$/subject/connectors/kafka/runtime_dependencies/3",
                "same-dependency-bytes.jar");
        PreparedConnectorArtifact localDependency = new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                1,
                false,
                2,
                0,
                commonDependencyOrigin.declarationPath(),
                commonDependencyOrigin.declaredReference(),
                List.of(commonDependencyOrigin, coalescedDependencyOrigin),
                null,
                List.of(),
                temporary.resolve("dependency-source.jar"),
                temporary.resolve("dependency-staged.jar"),
                B);
        MavenArtifactIdentity explicitWinner = jarIdentity(
                "org.example", "explicit-dependency", "1.0");
        MavenArtifactIdentity explicitOmitted = jarIdentity(
                "org.example", "explicit-dependency", "0.9");
        PreparedConnectorOrigin mavenDependencyOrigin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                4,
                "$/subject/connectors/kafka/runtime_dependencies/4",
                "maven:org.example:explicit-dependency:1.0");
        PreparedConnectorArtifact mavenDependency = new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                2,
                false,
                4,
                0,
                mavenDependencyOrigin.declarationPath(),
                mavenDependencyOrigin.declaredReference(),
                List.of(mavenDependencyOrigin),
                explicitWinner,
                List.of(explicitWinner),
                temporary.resolve("explicit-dependency-source.jar"),
                temporary.resolve("explicit-dependency-staged.jar"),
                C);
        MavenConflictDecision explicitConflict = new MavenConflictDecision(
                explicitOmitted,
                explicitWinner,
                MavenConflictDecision.Reason.FIRST_BREADTH_FIRST,
                0,
                0,
                4,
                4,
                List.of(explicitOmitted),
                List.of(explicitWinner));
        PreparedConnectorArtifact singlePrimary = singleLocalPrimary("kafka", A);
        PreparedConnectorClosure explicit = new PreparedConnectorClosure(
                ResolutionScope.SINGLE,
                "kafka",
                "$/subject/connectors/kafka",
                ConnectorDependencyMode.EXPLICIT,
                singlePrimary,
                List.of(localDependency, mavenDependency),
                List.of(),
                List.of(explicitConflict));
        ConnectorClosureLock explicitLock = factory.create(
                explicit, ScenarioSide.SINGLE, "flink:2.2.0");
        assertEquals(2, explicitLock.classpath().get(1)
                .effectiveOrigin().declarationIndex());
        assertEquals(2, explicitLock.classpath().get(1).artifact().origins().size());
        assertTrue(explicitLock.canonicalProjectionJson().contains(
                "\"origin_root\":{\"index\":2,\"kind\":\"runtime_dependency\"}"));
        assertTrue(explicitLock.canonicalProjectionJson().contains(
                "\"omitted_origin_root\":{\"index\":4,"
                        + "\"kind\":\"runtime_dependency\"}"));

        PreparedConnectorClosure invalidAuto = new PreparedConnectorClosure(
                ResolutionScope.SINGLE,
                "kafka",
                "$/subject/connectors/kafka",
                ConnectorDependencyMode.AUTO,
                mavenPrimary("kafka", A),
                List.of(localDependency),
                List.of(),
                List.of());
        assertThrows(IllegalArgumentException.class, () -> factory.create(
                invalidAuto, ScenarioSide.SINGLE, "flink:2.2.0"));
    }

    @Test
    void discoversExactSetupAndFlinkProcessRestartTargetsAndCreatesEveryLock() {
        ObjectNode document = baseDocument("all-target-locks");
        ObjectNode connectors = document.withObject("subject").withObject("connectors");
        connectors.removeAll();
        localConnector(connectors, "beta");
        localConnector(connectors, "alpha");
        ArrayNode aliases = ((ObjectNode) document.at("/workload/jobs/0"))
                .putArray("connectors");
        aliases.add("beta").add("alpha");
        ArrayNode steps = ((ObjectNode) document.at("/phases/0")).putArray("steps");
        restart(steps, "flink", "flink:2.2.1");
        restart(steps, "jobmanager", "flink:2.2.1");
        restart(steps, "taskmanager", "docker.io/library/flink:2.2.1");
        restart(steps, "kafka", "apache/kafka:4.0.1");
        restart(steps, "flink", "flink:2.2.1");
        restart(steps, "flink", null);

        ResolvedScenarioPlan scenarioPlan = resolve(document);
        PreparedScenarioPlan prepared = directPlan(
                scenarioPlan,
                List.of(localClosure("alpha", A), localClosure("beta", B)));

        assertEquals(
                List.of(
                        "flink:2.2.0",
                        "flink:2.2.1",
                        "docker.io/library/flink:2.2.1"),
                factory.targetFlinkImageReferences(prepared, ScenarioSide.SINGLE));
        List<ConnectorClosureLock> locks = factory.createAll(
                prepared, ScenarioSide.SINGLE);
        assertEquals(6, locks.size());
        assertEquals(List.of("alpha", "alpha", "alpha", "beta", "beta", "beta"),
                locks.stream().map(ConnectorClosureLock::alias).toList());
        assertEquals(3, locks.stream().map(ConnectorClosureLock::targetFlinkImageReference)
                .distinct().count());
    }

    private PreparedConnectorClosure autoClosure(
            ResolutionScope scope,
            String alias,
            String connectorPath,
            String declarationPath,
            Path sourcePath,
            Path stagedPath) {
        MavenArtifactIdentity primaryIdentity = jarIdentity(
                "org.apache.flink", "flink-connector-kafka", "5.0.0-2.2");
        MavenArtifactIdentity child = jarIdentity("org.example", "dependency", "1.0");
        MavenArtifactIdentity omitted = jarIdentity("org.example", "dependency", "0.9");
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                scope,
                PreparedConnectorOrigin.RootKind.PRIMARY,
                -1,
                declarationPath,
                PRIMARY_REF);
        PreparedConnectorArtifact primary = new PreparedConnectorArtifact(
                scope,
                0,
                true,
                -1,
                -1,
                declarationPath,
                PRIMARY_REF,
                List.of(origin),
                primaryIdentity,
                List.of(primaryIdentity),
                sourcePath,
                stagedPath,
                A);
        PreparedConnectorArtifact dependency = new PreparedConnectorArtifact(
                scope,
                1,
                false,
                0,
                1,
                declarationPath,
                PRIMARY_REF,
                List.of(origin),
                child,
                List.of(primaryIdentity, child),
                sourcePath.resolveSibling("dependency-source.jar"),
                stagedPath.resolveSibling("dependency-staged.jar"),
                B);
        MavenPomEvidence z = new MavenPomEvidence(
                pomIdentity("org.example", "z", "1.0"),
                temporary.resolve("z.pom"),
                D);
        MavenPomEvidence a = new MavenPomEvidence(
                pomIdentity("org.example", "a", "1.0"),
                temporary.resolve("a.pom"),
                C);
        MavenConflictDecision conflict = new MavenConflictDecision(
                omitted,
                child,
                MavenConflictDecision.Reason.FIRST_BREADTH_FIRST,
                1,
                1,
                0,
                0,
                List.of(primaryIdentity, omitted),
                List.of(primaryIdentity, child));
        return new PreparedConnectorClosure(
                scope,
                alias,
                connectorPath,
                ConnectorDependencyMode.AUTO,
                primary,
                List.of(dependency),
                List.of(z, a),
                List.of(conflict));
    }

    private PreparedConnectorClosure fullEvidenceVariant(
            PreparedConnectorClosure closure) {
        PreparedConnectorArtifact primary = closure.primary();
        PreparedConnectorArtifact dependency = closure.dependencies().getFirst();
        MavenArtifactIdentity dependencyIdentity = dependency.mavenIdentity().orElseThrow();
        MavenArtifactIdentity unprojectedAncestor = jarIdentity(
                "org.example.host-only", "unprojected-ancestor", "7.0");
        PreparedConnectorOrigin retainedOrigin = dependency.origins().getFirst();
        PreparedConnectorOrigin coalescedOrigin = new PreparedConnectorOrigin(
                retainedOrigin.scope(),
                retainedOrigin.rootKind(),
                retainedOrigin.declarationIndex(),
                "$/subject/connectors/coalesced-host-only/artifact",
                "maven:org.example.host-only:coalesced:7.0");
        PreparedConnectorArtifact evidenceHeavyDependency = new PreparedConnectorArtifact(
                dependency.scope(),
                dependency.classpathIndex(),
                false,
                7,
                2,
                "$/subject/connectors/host-only/runtime_dependencies/7",
                "maven:org.example.host-only:unprojected:7.0",
                List.of(retainedOrigin, coalescedOrigin),
                dependencyIdentity,
                List.of(
                        unprojectedAncestor,
                        primary.mavenIdentity().orElseThrow(),
                        dependencyIdentity),
                temporary.resolve("host-only/cache/dependency.jar"),
                temporary.resolve("host-only/staged/dependency.jar"),
                dependency.sha256());
        List<MavenPomEvidence> relocatedDescriptors = closure.consultedPoms().stream()
                .map(descriptor -> new MavenPomEvidence(
                        descriptor.identity(),
                        temporary.resolve("host-only/poms/")
                                .resolve(descriptor.identity().artifactId() + ".pom"),
                        descriptor.sha256()))
                .toList()
                .reversed();
        return new PreparedConnectorClosure(
                closure.scope(),
                closure.alias(),
                "$/subject/connectors/host-only-connector-path",
                closure.dependencyMode(),
                new PreparedConnectorArtifact(
                        primary.scope(),
                        primary.classpathIndex(),
                        true,
                        -1,
                        -1,
                        "$/subject/connectors/host-only/artifact",
                        primary.sourceReference(),
                        primary.origins(),
                        primary.mavenIdentity().orElse(null),
                        primary.mavenDependencyPath(),
                        temporary.resolve("host-only/cache/primary.jar"),
                        temporary.resolve("host-only/staged/primary.jar"),
                        primary.sha256()),
                List.of(evidenceHeavyDependency),
                relocatedDescriptors,
                closure.conflicts());
    }

    private PreparedConnectorClosure explicitCanonicalClosure() {
        MavenArtifactIdentity primaryIdentity = jarIdentity(
                "org.apache.flink", "flink-connector-kafka", "5.0.0-2.2");
        MavenArtifactIdentity dependencyIdentity = jarIdentity(
                "org.example", "dependency", "1.0");
        MavenArtifactIdentity omittedIdentity = jarIdentity(
                "org.example", "dependency", "0.9");
        PreparedConnectorOrigin primaryOrigin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.PRIMARY,
                -1,
                "$/subject/connectors/kafka/artifact",
                PRIMARY_REF);
        PreparedConnectorOrigin dependencyOrigin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                0,
                "$/subject/connectors/kafka/runtime_dependencies/0",
                "maven:org.example:dependency:1.0");
        PreparedConnectorArtifact primary = new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                0,
                true,
                -1,
                -1,
                primaryOrigin.declarationPath(),
                primaryOrigin.declaredReference(),
                List.of(primaryOrigin),
                primaryIdentity,
                List.of(primaryIdentity),
                temporary.resolve("canonical/primary-source.jar"),
                temporary.resolve("canonical/primary-staged.jar"),
                A);
        PreparedConnectorArtifact dependency = new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                1,
                false,
                0,
                0,
                dependencyOrigin.declarationPath(),
                dependencyOrigin.declaredReference(),
                List.of(dependencyOrigin),
                dependencyIdentity,
                List.of(dependencyIdentity),
                temporary.resolve("canonical/dependency-source.jar"),
                temporary.resolve("canonical/dependency-staged.jar"),
                B);
        MavenConflictDecision conflict = new MavenConflictDecision(
                omittedIdentity,
                dependencyIdentity,
                MavenConflictDecision.Reason.FIRST_BREADTH_FIRST,
                0,
                0,
                0,
                0,
                List.of(omittedIdentity),
                List.of(dependencyIdentity));
        return new PreparedConnectorClosure(
                ResolutionScope.SINGLE,
                "kafka",
                "$/subject/connectors/kafka",
                ConnectorDependencyMode.EXPLICIT,
                primary,
                List.of(dependency),
                List.of(
                        new MavenPomEvidence(
                                pomIdentity("org.example", "a", "1.0"),
                                temporary.resolve("canonical/a.pom"),
                                C),
                        new MavenPomEvidence(
                                pomIdentity("org.example", "z", "1.0"),
                                temporary.resolve("canonical/z.pom"),
                                D)),
                List.of(conflict));
    }

    private ConnectorClosureLock mutateCanonicalField(
            ConnectorClosureLock baseline,
            CanonicalFieldMutation mutation) {
        return switch (mutation) {
            case CONNECTOR_ALIAS -> canonicalizedCopy(
                    baseline, "kafka-mutated", null, null, null, null, null, null);
            case DECLARED_PRIMARY_REFERENCE -> withPrimaryDeclaredReference(
                    baseline, PRIMARY_REF + "-mutated");
            case TARGET_FLINK_IMAGE_REFERENCE -> canonicalizedCopy(
                    baseline, null, null, "flink:2.2.9", null, null, null, null);
            case DEPENDENCY_MODE_AND_ORIGIN_KIND -> factory.create(
                    autoClosure(
                            ResolutionScope.SINGLE,
                            "kafka",
                            "$/subject/connectors/kafka",
                            "$/subject/connectors/kafka/artifact",
                            temporary.resolve("canonical/primary-source.jar"),
                            temporary.resolve("canonical/primary-staged.jar")),
                    ScenarioSide.SINGLE,
                    baseline.targetFlinkImageReference());
            case PRIMARY_SHA256 -> withArtifactSha(baseline, 0, E);
            case CLASSPATH_ENTRY_LIST_AND_INDEX -> withAdditionalLocalDependency(baseline);
            case CLASSPATH_IDENTITY_KIND -> withLocalDependencyIdentity(baseline);
            case CLASSPATH_MAVEN_GROUP_ID -> withDependencyIdentity(
                    baseline, identityWith(
                            dependencyIdentity(baseline), "org.changed", null, null, null));
            case CLASSPATH_MAVEN_ARTIFACT_ID -> withDependencyIdentity(
                    baseline, identityWith(
                            dependencyIdentity(baseline), null, "changed-dependency", null, null));
            case CLASSPATH_MAVEN_CLASSIFIER -> withDependencyIdentity(
                    baseline, identityWith(
                            dependencyIdentity(baseline), null, null, "tests", null));
            case CLASSPATH_MAVEN_VERSION -> withDependencyIdentity(
                    baseline, identityWith(
                            dependencyIdentity(baseline), null, null, null, "1.1"));
            case CLASSPATH_ORIGIN_ROOT_INDEX -> withDependencyOriginIndex(baseline, 3);
            case CLASSPATH_ENTRY_SHA256 -> withArtifactSha(baseline, 1, E);
            case DESCRIPTOR_GROUP_ID -> withDescriptorIdentity(
                    baseline, identityWith(
                            descriptorIdentity(baseline), "org.changed", null, null, null));
            case DESCRIPTOR_ARTIFACT_ID -> withDescriptorIdentity(
                    baseline, identityWith(
                            descriptorIdentity(baseline), null, "changed-descriptor", null, null));
            case DESCRIPTOR_CLASSIFIER -> withDescriptorIdentity(
                    baseline, identityWith(
                            descriptorIdentity(baseline), null, null, "sources", null));
            case DESCRIPTOR_VERSION -> withDescriptorIdentity(
                    baseline, identityWith(
                            descriptorIdentity(baseline), null, null, null, "1.1"));
            case DESCRIPTOR_SHA256 -> withDescriptorSha(baseline, E);
            case MEDIATION_OMITTED_IDENTITY -> withOmittedVersion(baseline, "0.8");
            case MEDIATION_WINNER_IDENTITY -> withDependencyIdentity(
                    baseline, identityWith(
                            dependencyIdentity(baseline), null, null, null, "1.2"));
            case MEDIATION_REASON_AND_DEPTH -> withNearestDecision(baseline);
            case MEDIATION_OMITTED_ORIGIN_ROOT -> withConflictOrigin(
                    baseline, true, 4);
            case MEDIATION_WINNER_ORIGIN_ROOT -> withConflictOrigin(
                    baseline, false, 5);
            case MEDIATION_OMITTED_PATH -> withConflictPathAncestor(
                    baseline, true);
            case MEDIATION_WINNER_PATH -> withConflictPathAncestor(
                    baseline, false);
            case MEDIATION_DECISION_ORDER -> withReorderedMediationDecisions(baseline);
        };
    }

    private static ConnectorClosureLock canonicalizedCopy(
            ConnectorClosureLock baseline,
            String alias,
            String declaredPrimaryReference,
            String targetFlinkImageReference,
            ConnectorDependencyMode dependencyMode,
            List<ConnectorClosureLockEntry> classpath,
            List<MavenPomEvidence> descriptors,
            List<MavenConflictDecision> mediationDecisions) {
        ConnectorClosureLock draft = new ConnectorClosureLock(
                baseline.side(),
                alias == null ? baseline.alias() : alias,
                baseline.connectorPath(),
                declaredPrimaryReference == null
                        ? baseline.declaredPrimaryReference()
                        : declaredPrimaryReference,
                targetFlinkImageReference == null
                        ? baseline.targetFlinkImageReference()
                        : targetFlinkImageReference,
                dependencyMode == null ? baseline.dependencyMode() : dependencyMode,
                classpath == null ? baseline.classpath() : classpath,
                descriptors == null ? baseline.descriptors() : descriptors,
                mediationDecisions == null
                        ? baseline.mediationDecisions()
                        : mediationDecisions,
                baseline.canonicalProjectionJson(),
                baseline.closureSha256());
        String projection = ConnectorClosureLockFactory.canonicalProjection(draft);
        return new ConnectorClosureLock(
                draft.side(),
                draft.alias(),
                draft.connectorPath(),
                draft.declaredPrimaryReference(),
                draft.targetFlinkImageReference(),
                draft.dependencyMode(),
                draft.classpath(),
                draft.descriptors(),
                draft.mediationDecisions(),
                projection,
                CanonicalJson.sha256(projection));
    }

    private static ConnectorClosureLock withPrimaryDeclaredReference(
            ConnectorClosureLock baseline,
            String reference) {
        ConnectorClosureLockEntry oldEntry = baseline.classpath().getFirst();
        PreparedConnectorArtifact oldArtifact = oldEntry.artifact();
        PreparedConnectorOrigin oldOrigin = oldEntry.effectiveOrigin();
        PreparedConnectorOrigin newOrigin = new PreparedConnectorOrigin(
                oldOrigin.scope(),
                oldOrigin.rootKind(),
                oldOrigin.declarationIndex(),
                oldOrigin.declarationPath(),
                reference);
        PreparedConnectorArtifact newArtifact = copyArtifact(
                oldArtifact,
                oldArtifact.originRootIndex().orElse(-1),
                oldArtifact.dependencyDepth().orElse(-1),
                reference,
                List.of(newOrigin),
                oldArtifact.mavenIdentity().orElse(null),
                oldArtifact.mavenDependencyPath(),
                oldArtifact.sourcePath(),
                oldArtifact.preparedPath(),
                oldArtifact.sha256());
        List<ConnectorClosureLockEntry> classpath = new ArrayList<>(baseline.classpath());
        classpath.set(0, new ConnectorClosureLockEntry(newArtifact, newOrigin));
        return canonicalizedCopy(
                baseline, null, reference, null, null, classpath, null, null);
    }

    private static ConnectorClosureLock withArtifactSha(
            ConnectorClosureLock baseline,
            int index,
            String sha256) {
        ConnectorClosureLockEntry oldEntry = baseline.classpath().get(index);
        PreparedConnectorArtifact newArtifact = copyArtifact(
                oldEntry.artifact(),
                oldEntry.artifact().originRootIndex().orElse(-1),
                oldEntry.artifact().dependencyDepth().orElse(-1),
                oldEntry.artifact().sourceReference(),
                oldEntry.artifact().origins(),
                oldEntry.mavenIdentity().orElse(null),
                oldEntry.artifact().mavenDependencyPath(),
                oldEntry.artifact().sourcePath(),
                oldEntry.artifact().preparedPath(),
                sha256);
        List<ConnectorClosureLockEntry> classpath = new ArrayList<>(baseline.classpath());
        classpath.set(index, new ConnectorClosureLockEntry(
                newArtifact, oldEntry.effectiveOrigin()));
        return canonicalizedCopy(
                baseline, null, null, null, null, classpath, null, null);
    }

    private static ConnectorClosureLock withAdditionalLocalDependency(
            ConnectorClosureLock baseline) {
        ConnectorClosureLockEntry primary = baseline.classpath().getFirst();
        int index = baseline.classpath().size();
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                primary.effectiveOrigin().scope(),
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                1,
                "$/subject/connectors/kafka/runtime_dependencies/1",
                "extra-local.jar");
        PreparedConnectorArtifact artifact = new PreparedConnectorArtifact(
                primary.artifact().scope(),
                index,
                false,
                1,
                0,
                origin.declarationPath(),
                origin.declaredReference(),
                List.of(origin),
                null,
                List.of(),
                primary.artifact().sourcePath().resolveSibling("extra-local.jar"),
                primary.artifact().preparedPath().resolveSibling("extra-local.jar"),
                E);
        List<ConnectorClosureLockEntry> classpath = new ArrayList<>(baseline.classpath());
        classpath.add(new ConnectorClosureLockEntry(artifact, origin));
        return canonicalizedCopy(
                baseline, null, null, null, null, classpath, null, null);
    }

    private static ConnectorClosureLock withLocalDependencyIdentity(
            ConnectorClosureLock baseline) {
        ConnectorClosureLockEntry oldEntry = baseline.classpath().get(1);
        PreparedConnectorArtifact oldArtifact = oldEntry.artifact();
        PreparedConnectorArtifact local = copyArtifact(
                oldArtifact,
                oldArtifact.originRootIndex().orElseThrow(),
                oldArtifact.dependencyDepth().orElseThrow(),
                "dependency-local.jar",
                oldArtifact.origins(),
                null,
                List.of(),
                oldArtifact.sourcePath(),
                oldArtifact.preparedPath(),
                oldArtifact.sha256());
        List<ConnectorClosureLockEntry> classpath = new ArrayList<>(baseline.classpath());
        classpath.set(1, new ConnectorClosureLockEntry(local, oldEntry.effectiveOrigin()));
        return canonicalizedCopy(
                baseline, null, null, null, null, classpath, null, List.of());
    }

    private static ConnectorClosureLock withDependencyIdentity(
            ConnectorClosureLock baseline,
            MavenArtifactIdentity identity) {
        ConnectorClosureLockEntry oldEntry = baseline.classpath().get(1);
        PreparedConnectorArtifact artifact = copyArtifactWithIdentity(
                oldEntry.artifact(), identity);
        List<ConnectorClosureLockEntry> classpath = new ArrayList<>(baseline.classpath());
        classpath.set(1, new ConnectorClosureLockEntry(
                artifact, oldEntry.effectiveOrigin()));
        List<MavenConflictDecision> decisions = baseline.mediationDecisions().stream()
                .map(decision -> {
                    MavenArtifactIdentity omitted = new MavenArtifactIdentity(
                            identity.groupId(),
                            identity.artifactId(),
                            identity.extension(),
                            identity.classifier(),
                            decision.omitted().version());
                    return new MavenConflictDecision(
                            omitted,
                            identity,
                            decision.reason(),
                            decision.omittedDepth(),
                            decision.winnerDepth(),
                            decision.omittedOriginRootIndex(),
                            decision.winnerOriginRootIndex(),
                            withLast(decision.omittedPath(), omitted),
                            withLast(decision.winnerPath(), identity));
                })
                .toList();
        return canonicalizedCopy(
                baseline, null, null, null, null, classpath, null, decisions);
    }

    private static ConnectorClosureLock withDependencyOriginIndex(
            ConnectorClosureLock baseline,
            int originIndex) {
        ConnectorClosureLockEntry oldEntry = baseline.classpath().get(1);
        PreparedConnectorArtifact oldArtifact = oldEntry.artifact();
        PreparedConnectorOrigin oldOrigin = oldEntry.effectiveOrigin();
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                oldOrigin.scope(),
                PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                originIndex,
                oldOrigin.declarationPath(),
                oldOrigin.declaredReference());
        PreparedConnectorArtifact artifact = copyArtifact(
                oldArtifact,
                originIndex,
                oldArtifact.dependencyDepth().orElseThrow(),
                oldArtifact.sourceReference(),
                List.of(origin),
                oldEntry.mavenIdentity().orElse(null),
                oldArtifact.mavenDependencyPath(),
                oldArtifact.sourcePath(),
                oldArtifact.preparedPath(),
                oldArtifact.sha256());
        List<ConnectorClosureLockEntry> classpath = new ArrayList<>(baseline.classpath());
        classpath.set(1, new ConnectorClosureLockEntry(artifact, origin));
        List<MavenConflictDecision> decisions = baseline.mediationDecisions().stream()
                .map(decision -> new MavenConflictDecision(
                        decision.omitted(),
                        decision.winner(),
                        decision.reason(),
                        decision.omittedDepth(),
                        decision.winnerDepth(),
                        originIndex,
                        originIndex,
                        decision.omittedPath(),
                        decision.winnerPath()))
                .toList();
        return canonicalizedCopy(
                baseline, null, null, null, null, classpath, null, decisions);
    }

    private static ConnectorClosureLock withDescriptorIdentity(
            ConnectorClosureLock baseline,
            MavenArtifactIdentity identity) {
        List<MavenPomEvidence> descriptors = new ArrayList<>(baseline.descriptors());
        MavenPomEvidence old = descriptors.getFirst();
        descriptors.set(0, new MavenPomEvidence(identity, old.sourcePath(), old.sha256()));
        return canonicalizedCopy(
                baseline, null, null, null, null, null, descriptors, null);
    }

    private static ConnectorClosureLock withDescriptorSha(
            ConnectorClosureLock baseline,
            String sha256) {
        List<MavenPomEvidence> descriptors = new ArrayList<>(baseline.descriptors());
        MavenPomEvidence old = descriptors.getFirst();
        descriptors.set(0, new MavenPomEvidence(old.identity(), old.sourcePath(), sha256));
        return canonicalizedCopy(
                baseline, null, null, null, null, null, descriptors, null);
    }

    private static ConnectorClosureLock withOmittedVersion(
            ConnectorClosureLock baseline,
            String version) {
        MavenConflictDecision decision = baseline.mediationDecisions().getFirst();
        MavenArtifactIdentity omitted = identityWith(
                decision.omitted(), null, null, null, version);
        MavenConflictDecision changed = new MavenConflictDecision(
                omitted,
                decision.winner(),
                decision.reason(),
                decision.omittedDepth(),
                decision.winnerDepth(),
                decision.omittedOriginRootIndex(),
                decision.winnerOriginRootIndex(),
                withLast(decision.omittedPath(), omitted),
                decision.winnerPath());
        return canonicalizedCopy(
                baseline, null, null, null, null, null, null, List.of(changed));
    }

    private static ConnectorClosureLock withNearestDecision(
            ConnectorClosureLock baseline) {
        MavenConflictDecision decision = baseline.mediationDecisions().getFirst();
        MavenArtifactIdentity ancestor = jarIdentity(
                "org.example", "omitted-parent", "1.0");
        MavenConflictDecision changed = new MavenConflictDecision(
                decision.omitted(),
                decision.winner(),
                MavenConflictDecision.Reason.NEAREST,
                1,
                0,
                decision.omittedOriginRootIndex(),
                decision.winnerOriginRootIndex(),
                List.of(ancestor, decision.omitted()),
                decision.winnerPath());
        return canonicalizedCopy(
                baseline, null, null, null, null, null, null, List.of(changed));
    }

    private static ConnectorClosureLock withConflictOrigin(
            ConnectorClosureLock baseline,
            boolean omitted,
            int originIndex) {
        MavenConflictDecision decision = baseline.mediationDecisions().getFirst();
        MavenConflictDecision changed = new MavenConflictDecision(
                decision.omitted(),
                decision.winner(),
                decision.reason(),
                decision.omittedDepth(),
                decision.winnerDepth(),
                omitted ? originIndex : decision.omittedOriginRootIndex(),
                omitted ? decision.winnerOriginRootIndex() : originIndex,
                decision.omittedPath(),
                decision.winnerPath());
        return canonicalizedCopy(
                baseline, null, null, null, null, null, null, List.of(changed));
    }

    private static ConnectorClosureLock withConflictPathAncestor(
            ConnectorClosureLock baseline,
            boolean omitted) {
        MavenConflictDecision decision = baseline.mediationDecisions().getFirst();
        MavenArtifactIdentity originalAncestor = jarIdentity(
                "org.example", "original-parent", "1.0");
        MavenArtifactIdentity changedAncestor = jarIdentity(
                "org.example", "changed-parent", "1.0");
        MavenConflictDecision changed = new MavenConflictDecision(
                decision.omitted(),
                decision.winner(),
                MavenConflictDecision.Reason.FIRST_BREADTH_FIRST,
                1,
                1,
                decision.omittedOriginRootIndex(),
                decision.winnerOriginRootIndex(),
                List.of(
                        omitted ? changedAncestor : originalAncestor,
                        decision.omitted()),
                List.of(
                        omitted ? originalAncestor : changedAncestor,
                        decision.winner()));
        return canonicalizedCopy(
                baseline, null, null, null, null, null, null, List.of(changed));
    }

    private static ConnectorClosureLock withReorderedMediationDecisions(
            ConnectorClosureLock baseline) {
        MavenArtifactIdentity omitted = jarIdentity(
                "org.example", "another-conflict", "0.9");
        MavenArtifactIdentity winner = jarIdentity(
                "org.example", "another-conflict", "1.0");
        MavenConflictDecision another = new MavenConflictDecision(
                omitted,
                winner,
                MavenConflictDecision.Reason.FIRST_BREADTH_FIRST,
                0,
                0,
                1,
                1,
                List.of(omitted),
                List.of(winner));
        return canonicalizedCopy(
                baseline,
                null,
                null,
                null,
                null,
                null,
                null,
                List.of(another, baseline.mediationDecisions().getFirst()));
    }

    private static PreparedConnectorArtifact copyArtifactWithIdentity(
            PreparedConnectorArtifact artifact,
            MavenArtifactIdentity identity) {
        return copyArtifact(
                artifact,
                artifact.originRootIndex().orElse(-1),
                artifact.dependencyDepth().orElse(-1),
                artifact.sourceReference(),
                artifact.origins(),
                identity,
                withLast(artifact.mavenDependencyPath(), identity),
                artifact.sourcePath(),
                artifact.preparedPath(),
                artifact.sha256());
    }

    private static PreparedConnectorArtifact copyArtifact(
            PreparedConnectorArtifact artifact,
            int originRootIndex,
            int dependencyDepth,
            String sourceReference,
            List<PreparedConnectorOrigin> origins,
            MavenArtifactIdentity identity,
            List<MavenArtifactIdentity> dependencyPath,
            Path sourcePath,
            Path preparedPath,
            String sha256) {
        return new PreparedConnectorArtifact(
                artifact.scope(),
                artifact.classpathIndex(),
                artifact.primary(),
                originRootIndex,
                dependencyDepth,
                artifact.declarationPath(),
                sourceReference,
                origins,
                identity,
                dependencyPath,
                sourcePath,
                preparedPath,
                sha256);
    }

    private static MavenArtifactIdentity dependencyIdentity(
            ConnectorClosureLock lock) {
        return lock.classpath().get(1).mavenIdentity().orElseThrow();
    }

    private static MavenArtifactIdentity descriptorIdentity(
            ConnectorClosureLock lock) {
        return lock.descriptors().getFirst().identity();
    }

    private static MavenArtifactIdentity identityWith(
            MavenArtifactIdentity identity,
            String groupId,
            String artifactId,
            String classifier,
            String version) {
        return new MavenArtifactIdentity(
                groupId == null ? identity.groupId() : groupId,
                artifactId == null ? identity.artifactId() : artifactId,
                identity.extension(),
                classifier == null ? identity.classifier() : classifier,
                version == null ? identity.version() : version);
    }

    private static List<MavenArtifactIdentity> withLast(
            List<MavenArtifactIdentity> path,
            MavenArtifactIdentity last) {
        List<MavenArtifactIdentity> changed = new ArrayList<>(path);
        changed.set(changed.size() - 1, last);
        return List.copyOf(changed);
    }

    private PreparedConnectorClosure localClosure(String alias, String hash) {
        return localClosureAtPaths(
                alias,
                hash,
                temporary.resolve(alias + ".jar"),
                temporary.resolve("staged-" + alias + ".jar"));
    }

    private PreparedConnectorClosure localClosureAtPaths(
            String alias,
            String hash,
            Path sourcePath,
            Path preparedPath) {
        return new PreparedConnectorClosure(
                ResolutionScope.SINGLE,
                alias,
                "$/subject/connectors/" + alias,
                ConnectorDependencyMode.EXPLICIT,
                singleLocalPrimary(alias, hash, sourcePath, preparedPath),
                List.of(),
                List.of(),
                List.of());
    }

    private PreparedConnectorArtifact singleLocalPrimary(String alias, String hash) {
        return singleLocalPrimary(
                alias,
                hash,
                temporary.resolve(alias + ".jar"),
                temporary.resolve("staged-" + alias + ".jar"));
    }

    private PreparedConnectorArtifact singleLocalPrimary(
            String alias,
            String hash,
            Path sourcePath,
            Path preparedPath) {
        String path = "$/subject/connectors/" + alias + "/artifact";
        String reference = alias + ".jar";
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.PRIMARY,
                -1,
                path,
                reference);
        return new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                0,
                true,
                -1,
                -1,
                path,
                reference,
                List.of(origin),
                null,
                List.of(),
                sourcePath,
                preparedPath,
                hash);
    }

    private PreparedConnectorArtifact mavenPrimary(String alias, String hash) {
        String path = "$/subject/connectors/" + alias + "/artifact";
        MavenArtifactIdentity identity = jarIdentity(
                "org.apache.flink", "flink-connector-kafka", "5.0.0-2.2");
        PreparedConnectorOrigin origin = new PreparedConnectorOrigin(
                ResolutionScope.SINGLE,
                PreparedConnectorOrigin.RootKind.PRIMARY,
                -1,
                path,
                PRIMARY_REF);
        return new PreparedConnectorArtifact(
                ResolutionScope.SINGLE,
                0,
                true,
                -1,
                -1,
                path,
                PRIMARY_REF,
                List.of(origin),
                identity,
                List.of(identity),
                temporary.resolve(alias + ".jar"),
                temporary.resolve("staged-" + alias + ".jar"),
                hash);
    }

    private PreparedScenarioPlan directPlan(
            ResolvedScenarioPlan scenarioPlan,
            List<PreparedConnectorClosure> closures) {
        List<ResolvedArtifact> artifacts = closures.stream().map(closure ->
                new ResolvedArtifact(
                        closure.primary().scope(),
                        ArtifactRole.SUBJECT_CONNECTOR,
                        closure.connectorPath() + "/artifact",
                        closure.primary().sourceReference(),
                        closure.primary().sourcePath(),
                        closure.primary().preparedPath(),
                        closure.primary().sha256()))
                .toList();
        return new PreparedScenarioPlan(
                scenarioPlan,
                new ArtifactWorkspace(temporary, temporary.resolve("plan-direct")),
                artifacts,
                closures,
                false);
    }

    private ResolvedScenarioPlan resolve(ObjectNode document) {
        Path source = resource("minimal.yaml").resolveSibling(
                document.at("/meta/name").textValue() + ".yaml");
        ScenarioSpecification scenario = loader.validateScenarioDocument(source, document);
        ObjectNode expectedDocument = loader.loadExpectedResult(
                resource("minimal.expected.yaml")).document();
        expectedDocument.withObject("meta")
                .put("name", scenario.name() + ".expected")
                .put("scenario", scenario.name());
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                source.resolveSibling(scenario.name() + ".expected.yaml"), expectedDocument);
        return new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected), ResolutionRequest.none());
    }

    private ObjectNode baseDocument(String name) {
        ObjectNode document = loader.loadScenario(resource("minimal.yaml")).document();
        document.withObject("meta").put("name", name);
        ((ObjectNode) document.at("/workload/jobs/0")).put("jar", "job.jar");
        return document;
    }

    private static void localConnector(ObjectNode connectors, String alias) {
        connectors.putObject(alias)
                .put("artifact", alias + ".jar")
                .putArray("runtime_dependencies");
    }

    private static void restart(ArrayNode steps, String component, String image) {
        ObjectNode restart = steps.addObject().putObject("restart");
        restart.put("component", component);
        if (image != null) {
            restart.put("image", image);
        }
    }

    private static MavenArtifactIdentity jarIdentity(
            String groupId,
            String artifactId,
            String version) {
        return new MavenArtifactIdentity(groupId, artifactId, "jar", "", version);
    }

    private static MavenArtifactIdentity pomIdentity(
            String groupId,
            String artifactId,
            String version) {
        return new MavenArtifactIdentity(groupId, artifactId, "pom", "", version);
    }

    private String goldenProjection() {
        return "{\"classpath\":[{\"classpath_index\":0,\"identity\":{"
                + "\"artifact_id\":\"flink-connector-kafka\",\"classifier\":\"\","
                + "\"extension\":\"jar\",\"group_id\":\"org.apache.flink\","
                + "\"kind\":\"maven\",\"version\":\"5.0.0-2.2\"},"
                + "\"origin_root\":{\"index\":-1,\"kind\":\"primary\"},"
                + "\"sha256\":\"" + A + "\"},{\"classpath_index\":1,"
                + "\"identity\":{\"artifact_id\":\"dependency\",\"classifier\":\"\","
                + "\"extension\":\"jar\",\"group_id\":\"org.example\","
                + "\"kind\":\"maven\",\"version\":\"1.0\"},"
                + "\"origin_root\":{\"index\":-1,\"kind\":\"primary\"},"
                + "\"sha256\":\"" + B + "\"}],\"connector_alias\":\"kafka\","
                + "\"declared_primary_reference\":\"" + PRIMARY_REF + "\","
                + "\"dependency_mode\":\"auto\",\"descriptors\":[{\"identity\":{"
                + "\"artifact_id\":\"a\",\"classifier\":\"\",\"extension\":\"pom\","
                + "\"group_id\":\"org.example\",\"version\":\"1.0\"},"
                + "\"sha256\":\"" + C + "\"},{\"identity\":{\"artifact_id\":\"z\","
                + "\"classifier\":\"\",\"extension\":\"pom\","
                + "\"group_id\":\"org.example\",\"version\":\"1.0\"},"
                + "\"sha256\":\"" + D + "\"}],\"format\":\""
                + ConnectorClosureLock.FORMAT + "\",\"mediation_decisions\":[{"
                + "\"omitted_depth\":1,\"omitted_identity\":{\"artifact_id\":"
                + "\"dependency\",\"classifier\":\"\",\"extension\":\"jar\","
                + "\"group_id\":\"org.example\",\"version\":\"0.9\"},"
                + "\"omitted_origin_root\":{\"index\":-1,\"kind\":\"primary\"},"
                + "\"omitted_path\":[{"
                + "\"artifact_id\":\"flink-connector-kafka\",\"classifier\":\"\","
                + "\"extension\":\"jar\",\"group_id\":\"org.apache.flink\","
                + "\"version\":\"5.0.0-2.2\"},{\"artifact_id\":\"dependency\","
                + "\"classifier\":\"\",\"extension\":\"jar\","
                + "\"group_id\":\"org.example\",\"version\":\"0.9\"}],"
                + "\"reason\":\"first_breadth_first\",\"winner_depth\":1,"
                + "\"winner_identity\":{\"artifact_id\":\"dependency\","
                + "\"classifier\":\"\",\"extension\":\"jar\","
                + "\"group_id\":\"org.example\",\"version\":\"1.0\"},"
                + "\"winner_origin_root\":{\"index\":-1,\"kind\":\"primary\"},"
                + "\"winner_path\":[{\"artifact_id\":"
                + "\"flink-connector-kafka\",\"classifier\":\"\","
                + "\"extension\":\"jar\",\"group_id\":\"org.apache.flink\","
                + "\"version\":\"5.0.0-2.2\"},{\"artifact_id\":\"dependency\","
                + "\"classifier\":\"\",\"extension\":\"jar\","
                + "\"group_id\":\"org.example\",\"version\":\"1.0\"}]}],"
                + "\"primary_sha256\":\"" + A + "\","
                + "\"target_flink_image_reference\":\"flink:2.2.0\"}";
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private enum CanonicalFieldMutation {
        CONNECTOR_ALIAS,
        DECLARED_PRIMARY_REFERENCE,
        TARGET_FLINK_IMAGE_REFERENCE,
        DEPENDENCY_MODE_AND_ORIGIN_KIND,
        PRIMARY_SHA256,
        CLASSPATH_ENTRY_LIST_AND_INDEX,
        CLASSPATH_IDENTITY_KIND,
        CLASSPATH_MAVEN_GROUP_ID,
        CLASSPATH_MAVEN_ARTIFACT_ID,
        CLASSPATH_MAVEN_CLASSIFIER,
        CLASSPATH_MAVEN_VERSION,
        CLASSPATH_ORIGIN_ROOT_INDEX,
        CLASSPATH_ENTRY_SHA256,
        DESCRIPTOR_GROUP_ID,
        DESCRIPTOR_ARTIFACT_ID,
        DESCRIPTOR_CLASSIFIER,
        DESCRIPTOR_VERSION,
        DESCRIPTOR_SHA256,
        MEDIATION_OMITTED_IDENTITY,
        MEDIATION_WINNER_IDENTITY,
        MEDIATION_REASON_AND_DEPTH,
        MEDIATION_OMITTED_ORIGIN_ROOT,
        MEDIATION_WINNER_ORIGIN_ROOT,
        MEDIATION_OMITTED_PATH,
        MEDIATION_WINNER_PATH,
        MEDIATION_DECISION_ORDER
    }
}
