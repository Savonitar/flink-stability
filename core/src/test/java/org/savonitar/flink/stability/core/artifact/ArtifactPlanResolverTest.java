package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalog;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalogTestFactory;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.spec.document.SuiteSpecification;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedSuitePlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;
import org.savonitar.flink.stability.core.spec.resolution.SuitePlanResolver;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
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
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.savonitar.flink.stability.core.spec.document.SpecificationAssertions.assertFailsAt;

class ArtifactPlanResolverTest {
    private static final String JOB_PATH = "$/workload/jobs/0/jar";
    private static final String CONNECTOR_PATH = "$/subject/connectors/kafka/artifact";

    private final SpecificationLoader loader = new SpecificationLoader();

    @TempDir
    Path artifactRoot;

    @Test
    void resolvesRelativeLocalFilesAgainstTheExactArtifactRootAndComputesSha256()
            throws IOException {
        Files.createDirectories(artifactRoot.resolve("inputs"));
        Path input = artifactRoot.resolve("inputs/records.txt");
        Files.writeString(input, "hello");
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);

        ResolvedScenarioPlan plan = plainPlan("local-root", document -> {
            setCoreArtifacts(document, "connector.jar", "job.jar");
            ObjectNode inputSource = (ObjectNode) document.at(
                    "/setup/kafka/clusters/main/topics/0/input_source");
            inputSource.removeAll();
            inputSource.put("mode", "file");
            inputSource.put("format", "integer-sequence");
            inputSource.put("path", "inputs/records.txt");
        });

        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                plan, ArtifactResolutionOptions.online(artifactRoot.resolve(".")));

        ResolvedArtifact resolved = prepared.artifact(
                ScenarioSide.SINGLE,
                "$/setup/kafka/clusters/main/topics/0/input_source/path").orElseThrow();
        assertEquals(artifactRoot.toRealPath(), prepared.artifactRoot());
        assertStagedCopy(resolved, input);
        assertEquals("inputs/records.txt", resolved.declaredReference());
        assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                resolved.sha256());
        assertEquals(ArtifactRole.INPUT_FILE, resolved.role());
        assertEquals(ResolutionScope.SINGLE, resolved.scope());

        Files.writeString(input, "mutated source");
        assertEquals("hello", Files.readString(resolved.preparedPath()));
        assertEquals(resolved.sha256(), sha256(resolved.preparedPath()));
        assertNotEquals(sha256(input), resolved.sha256());
    }

    @Test
    void rejectsAnArtifactRootThatIsNotAnExistingDirectory() {
        Path missingRoot = artifactRoot.resolve("missing");

        SpecificationException exception = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("bad-root", document -> {}),
                        ArtifactResolutionOptions.online(missingRoot)));

        assertEquals(1, exception.diagnostics().size());
        Diagnostic issue = exception.diagnostics().getFirst();
        assertEquals("artifact.root.not-directory", issue.code());
        assertEquals(ResolutionScope.COMMON, issue.scope());
        assertEquals("$", issue.path());
    }

    @Test
    void rejectsAPreexistingStagingSymlinkBeforeCreatingAnythingOutsideTheRoot()
            throws IOException {
        Path trustedRoot = Files.createDirectories(artifactRoot.resolve("trusted"));
        createJar(trustedRoot.resolve("connector.jar"), false);
        createJar(trustedRoot.resolve("job.jar"), true);
        Path outside = Files.createDirectories(artifactRoot.resolve("outside"));
        Files.createSymbolicLink(trustedRoot.resolve(".flink-stability"), outside);

        SpecificationException exception = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("staging-symlink", document -> setCoreArtifacts(
                                document, "connector.jar", "job.jar")),
                        ArtifactResolutionOptions.online(trustedRoot)));

        assertSingleIssue(exception, "artifact.staging.failed", "$");
        try (var outsideEntries = Files.list(outside)) {
            assertEquals(0, outsideEntries.count());
        }
    }

    @Test
    void rejectsRelativeFileAndAbsoluteJarReferencesOutsideTheArtifactRoot()
            throws IOException {
        Path trustedRoot = Files.createDirectories(artifactRoot.resolve("trusted"));
        createJar(trustedRoot.resolve("connector.jar"), false);
        createJar(trustedRoot.resolve("job.jar"), true);
        Path outside = Files.createDirectories(artifactRoot.resolve("outside"));
        Path outsideInput = Files.writeString(outside.resolve("records.txt"), "outside");
        Path outsideJob = createJar(outside.resolve("outside-job.jar"), true);
        String relativeEscape = trustedRoot.relativize(outsideInput).toString();

        SpecificationException escapedFile = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("relative-escape", document -> {
                            setCoreArtifacts(document, "connector.jar", "job.jar");
                            ObjectNode input = (ObjectNode) document.at(
                                    "/setup/kafka/clusters/main/topics/0/input_source");
                            input.removeAll();
                            input.put("mode", "file");
                            input.put("format", "json-lines");
                            input.put("path", relativeEscape);
                        }),
                        ArtifactResolutionOptions.online(trustedRoot)));
        assertSingleIssue(escapedFile, "artifact.local.outside-root",
                "$/setup/kafka/clusters/main/topics/0/input_source/path");

        SpecificationException absoluteJar = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("absolute-escape", document -> setCoreArtifacts(
                                document, "connector.jar", outsideJob.toString())),
                        ArtifactResolutionOptions.online(trustedRoot)));
        assertSingleIssue(absoluteJar, "artifact.local.outside-root", JOB_PATH);
    }

    @Test
    void rejectsInRootSymlinksAndGlobMatchesThatResolveOutsideTheArtifactRoot()
            throws IOException {
        Path trustedRoot = Files.createDirectories(artifactRoot.resolve("trusted"));
        createJar(trustedRoot.resolve("connector.jar"), false);
        Path outside = Files.createDirectories(artifactRoot.resolve("outside"));
        Path outsideJob = createJar(outside.resolve("outside-job.jar"), true);
        Path directLink = trustedRoot.resolve("linked-job.jar");
        Files.createSymbolicLink(directLink, outsideJob);

        SpecificationException linkedJar = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("symlink-escape", document -> setCoreArtifacts(
                                document, "connector.jar", "linked-job.jar")),
                        ArtifactResolutionOptions.online(trustedRoot)));
        assertSingleIssue(linkedJar, "artifact.local.outside-root", JOB_PATH);

        Path build = Files.createDirectories(trustedRoot.resolve("build"));
        Files.createSymbolicLink(build.resolve("job-linked.jar"), outsideJob);
        SpecificationException linkedGlob = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("glob-symlink-escape", document -> setCoreArtifacts(
                                document, "connector.jar", "build/job-*.jar")),
                        ArtifactResolutionOptions.online(trustedRoot)));
        assertSingleIssue(linkedGlob, "artifact.local.outside-root", JOB_PATH);
    }

    @Test
    void rejectsAGlobSegmentEvenWhenPathNormalizationWouldRemoveIt() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);

        SpecificationException exception = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("normalized-glob", document -> setCoreArtifacts(
                                document, "connector.jar", "build-*/../job.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));

        assertSingleIssue(exception, "artifact.reference.invalid", JOB_PATH);
    }

    @Test
    void acceptsInternalParentTraversalThatNormalizesWithinTheArtifactRoot()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        Path job = createJar(artifactRoot.resolve("job.jar"), true);

        ResolvedArtifact resolved = new ArtifactPlanResolver().resolve(
                        plainPlan("internal-normalization", document -> setCoreArtifacts(
                                document, "connector.jar", "sub/../job.jar")),
                        ArtifactResolutionOptions.online(artifactRoot))
                .artifact(ScenarioSide.SINGLE, JOB_PATH).orElseThrow();

        assertEquals("sub/../job.jar", resolved.declaredReference());
        assertStagedCopy(resolved, job);
    }

    @Test
    void resolvesExactlyOneFinalFilenameGlobMatch() throws IOException {
        Files.createDirectories(artifactRoot.resolve("build"));
        Path job = createJar(artifactRoot.resolve("build/job-1.0.jar"), true);
        createJar(artifactRoot.resolve("connector.jar"), false);
        ResolvedScenarioPlan plan = plainPlan("one-glob", document ->
                setCoreArtifacts(document, "connector.jar", "build/job-*.jar"));

        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                plan, ArtifactResolutionOptions.online(artifactRoot));
        ResolvedArtifact resolved = prepared.artifact(
                ScenarioSide.SINGLE, JOB_PATH).orElseThrow();

        assertStagedCopy(resolved, job);
        assertEquals(job.toRealPath(), resolved.sourcePath());
        assertEquals(prepared.preparationRoot(), resolved.preparedPath().getParent());
        assertEquals("build/job-*.jar", resolved.declaredReference());
    }

    @Test
    void distinguishesZeroAndMultipleGlobMatches() throws IOException {
        Files.createDirectories(artifactRoot.resolve("build"));
        createJar(artifactRoot.resolve("connector.jar"), false);
        ArtifactPlanResolver resolver = new ArtifactPlanResolver();

        SpecificationException zero = assertFailsAt(
                Stage.ARTIFACT,
                () -> resolver.resolve(
                        plainPlan("zero-glob", document -> setCoreArtifacts(
                                document, "connector.jar", "build/job-*.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(zero, "artifact.local.not-found", JOB_PATH);

        createJar(artifactRoot.resolve("build/job-a.jar"), true);
        createJar(artifactRoot.resolve("build/job-b.jar"), true);
        SpecificationException multiple = assertFailsAt(
                Stage.ARTIFACT,
                () -> resolver.resolve(
                        plainPlan("multi-glob", document -> setCoreArtifacts(
                                document, "connector.jar", "build/job-*.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(multiple, "artifact.local.ambiguous", JOB_PATH);
    }

    @Test
    void restrictsGlobsToJarRolesAndTheFinalFilenameSegment() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        Files.createDirectories(artifactRoot.resolve("inputs"));

        SpecificationException filePattern = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("file-pattern", document -> {
                            setCoreArtifacts(document, "connector.jar", "job.jar");
                            ObjectNode input = (ObjectNode) document.at(
                                    "/setup/kafka/clusters/main/topics/0/input_source");
                            input.removeAll();
                            input.put("mode", "file");
                            input.put("format", "json-lines");
                            input.put("path", "inputs/*.json");
                        }),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(filePattern, "artifact.reference.invalid",
                "$/setup/kafka/clusters/main/topics/0/input_source/path");

        SpecificationException directoryPattern = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("directory-pattern", document -> setCoreArtifacts(
                                document, "connector.jar", "build-*/job.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(directoryPattern, "artifact.reference.invalid", JOB_PATH);
    }

    @Test
    void rejectsAValidExecutableJarStoredWithANonJarFilename() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.bin"), true);

        SpecificationException extension = assertFailsAt(
                Stage.ARTIFACT,
                () -> resolvePlain("wrong-extension", "connector.jar", "job.bin"));

        assertSingleIssue(extension, "artifact.jar.invalid", JOB_PATH);
    }

    @Test
    void validatesJarArchiveAndRequiredMainClassByRole() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);

        Files.writeString(artifactRoot.resolve("broken.jar"), "not a jar");
        SpecificationException archive = assertFailsAt(
                Stage.ARTIFACT,
                () -> resolvePlain("broken-archive", "connector.jar", "broken.jar"));
        assertSingleIssue(archive, "artifact.jar.invalid", JOB_PATH);

        createJar(artifactRoot.resolve("no-main.jar"), false);
        SpecificationException entrypoint = assertFailsAt(
                Stage.ARTIFACT,
                () -> resolvePlain("no-entrypoint", "connector.jar", "no-main.jar"));
        assertSingleIssue(entrypoint, "artifact.jar.entrypoint-missing", JOB_PATH);

        createJar(artifactRoot.resolve("job.jar"), true);
        PreparedScenarioPlan prepared = resolvePlain(
                "connector-without-main", "connector.jar", "job.jar");
        assertEquals(ArtifactRole.SUBJECT_CONNECTOR,
                prepared.artifact(ScenarioSide.SINGLE, CONNECTOR_PATH).orElseThrow().role());
    }

    @Test
    void rejectsCorruptPayloadEntriesForConnectorAndExecutableJars() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        createJarWithCorruptStoredEntry(
                artifactRoot.resolve("corrupt-connector.jar"), false);
        createJarWithCorruptStoredEntry(
                artifactRoot.resolve("corrupt-job.jar"), true);

        SpecificationException connector = assertFailsAt(
                Stage.ARTIFACT,
                () -> resolvePlain(
                        "corrupt-connector", "corrupt-connector.jar", "job.jar"));
        SpecificationException job = assertFailsAt(
                Stage.ARTIFACT,
                () -> resolvePlain("corrupt-job", "connector.jar", "corrupt-job.jar"));

        assertSingleIssue(connector, "artifact.jar.invalid", CONNECTOR_PATH);
        assertSingleIssue(job, "artifact.jar.invalid", JOB_PATH);
    }

    @Test
    void requiresMainClassForCustomInputAndCustomValidatorArtifacts() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        createJar(artifactRoot.resolve("no-main.jar"), false);
        ResolvedScenarioPlan plan = plainPlan("custom-entrypoints", document -> {
            setCoreArtifacts(document, "connector.jar", "job.jar");
            ObjectNode input = (ObjectNode) document.at(
                    "/setup/kafka/clusters/main/topics/0/input_source");
            input.removeAll();
            input.put("mode", "custom");
            input.put("artifact", "no-main.jar");
            input.put("timeout", "30s");
            ArrayNode terminal = document.withArray("terminal_validations");
            terminal.removeAll();
            addCustomValidator(terminal.addObject(), "no-main.jar");
        });

        SpecificationException exception = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plan, ArtifactResolutionOptions.online(artifactRoot)));

        assertEquals(List.of(
                        "artifact.jar.entrypoint-missing@"
                                + "$/setup/kafka/clusters/main/topics/0/input_source/artifact",
                        "artifact.jar.entrypoint-missing@$/terminal_validations/0/artifact"),
                exception.diagnostics().stream()
                        .map(issue -> issue.code() + "@" + issue.path())
                        .toList());
    }

    @Test
    void inventoriesEveryStaticArtifactFieldIncludingDeepInlineValidators() throws IOException {
        Files.createDirectories(artifactRoot.resolve("input"));
        Files.writeString(artifactRoot.resolve("input/data.csv"), "1\n2\n");
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        createJar(artifactRoot.resolve("terminal.jar"), true);
        createJar(artifactRoot.resolve("deep.jar"), true);

        ResolvedScenarioPlan plan = plainPlan("inventory", document -> {
            setCoreArtifacts(document, "connector.jar", "job.jar");
            ObjectNode fileInput = (ObjectNode) document.at(
                    "/setup/kafka/clusters/main/topics/0/input_source");
            fileInput.removeAll();
            fileInput.put("mode", "file");
            fileInput.put("format", "csv");
            fileInput.put("path", "input/data.csv");

            ArrayNode terminal = document.withArray("terminal_validations");
            addCustomValidator(terminal.addObject(), "terminal.jar");

            ArrayNode topSteps = ((ObjectNode) document.at("/phases/0")).withArray("steps");
            topSteps.removeAll();
            ObjectNode firstLoop = topSteps.addObject().putObject("loop");
            firstLoop.put("times", 2);
            ObjectNode secondLoop = firstLoop.putArray("steps")
                    .addObject().putObject("loop");
            secondLoop.put("times", 3);
            ObjectNode validate = secondLoop.putArray("steps")
                    .addObject().putObject("validate");
            addCustomValidator(validate, "deep.jar");
        });

        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                plan, ArtifactResolutionOptions.online(artifactRoot));

        assertEquals(List.of(
                        "CUSTOM_VALIDATOR@$/phases/0/steps/0/loop/steps/0/loop/steps/0/validate/artifact",
                        "INPUT_FILE@$/setup/kafka/clusters/main/topics/0/input_source/path",
                        "SUBJECT_CONNECTOR@$/subject/connectors/kafka/artifact",
                        "CUSTOM_VALIDATOR@$/terminal_validations/1/artifact",
                        "WORKLOAD_JOB@$/workload/jobs/0/jar"),
                prepared.artifacts().stream()
                        .map(artifact -> artifact.role() + "@" + artifact.path())
                        .toList());
        assertTrue(prepared.artifacts().stream()
                .allMatch(artifact -> artifact.scope() == ResolutionScope.SINGLE));

        createJar(artifactRoot.resolve("custom-input.jar"), true);
        PreparedScenarioPlan customInputPrepared = new ArtifactPlanResolver().resolve(
                plainPlan("custom-input-inventory", document -> {
                    setCoreArtifacts(document, "connector.jar", "job.jar");
                    ObjectNode input = (ObjectNode) document.at(
                            "/setup/kafka/clusters/main/topics/0/input_source");
                    input.removeAll();
                    input.put("mode", "custom");
                    input.put("artifact", "custom-input.jar");
                    input.put("timeout", "30s");
                    ArrayNode terminal = document.withArray("terminal_validations");
                    terminal.removeAll();
                    addCustomValidator(terminal.addObject(), "custom-input.jar");
                }),
                ArtifactResolutionOptions.online(artifactRoot));
        ResolvedArtifact customInput = customInputPrepared.artifact(
                ScenarioSide.SINGLE,
                "$/setup/kafka/clusters/main/topics/0/input_source/artifact").orElseThrow();
        assertEquals(ArtifactRole.CUSTOM_INPUT, customInput.role());
    }

    @Test
    void validatesMavenCoordinatesAndRestrictsThemToSubjectConnectors() throws IOException {
        createJar(artifactRoot.resolve("job.jar"), true);

        SpecificationException invalidCoordinate = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver((coordinate, offline) -> {
                    throw new AssertionError("Invalid coordinates must not reach the lookup");
                }).resolve(
                        plainPlan("snapshot-coordinate", document -> setCoreArtifacts(
                                document,
                                "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2-SNAPSHOT",
                                "job.jar")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(invalidCoordinate, "artifact.maven.invalid-coordinate", CONNECTOR_PATH);

        createJar(artifactRoot.resolve("connector.jar"), false);
        SpecificationException forbiddenRole = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plainPlan("maven-job", document -> setCoreArtifacts(
                                document,
                                "connector.jar",
                                "maven:org.example:job:1.0")),
                        ArtifactResolutionOptions.online(artifactRoot)));
        assertSingleIssue(forbiddenRole, "artifact.reference.invalid", JOB_PATH);
    }

    @Test
    void passesCanonicalCoordinateAndOfflinePolicyToInjectedLookup() throws IOException {
        Path connector = createJar(artifactRoot.resolve("resolved-connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        List<String> calls = new ArrayList<>();
        MavenArtifactLookup lookup = (coordinate, offline) -> {
            calls.add(coordinate.declaredReference() + "|" + coordinate.resolverCoordinate()
                    + "|offline=" + offline);
            return connector;
        };

        PreparedScenarioPlan prepared = new ArtifactPlanResolver(lookup).resolve(
                plainPlan("offline-forwarded", document -> {
                    setCoreArtifacts(
                            document,
                            "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2",
                            "job.jar");
                    ((ObjectNode) document.at("/subject/connectors/kafka"))
                            .putArray("runtime_dependencies");
                }),
                new ArtifactResolutionOptions(artifactRoot, true));

        assertEquals(List.of(
                "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2"
                        + "|org.apache.flink:flink-connector-kafka:jar:5.0.0-2.2"
                        + "|offline=true"),
                calls);
        ResolvedArtifact artifact = prepared.artifact(
                ScenarioSide.SINGLE, CONNECTOR_PATH).orElseThrow();
        assertEquals("maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2",
                artifact.declaredReference());
        assertStagedCopy(artifact, connector);
    }

    @Test
    void mapsEveryMavenLookupFailureKindToAStableIssueCode() throws IOException {
        createJar(artifactRoot.resolve("job.jar"), true);
        Map<MavenArtifactLookupException.Kind, String> expectations = Map.of(
                MavenArtifactLookupException.Kind.NOT_FOUND, "artifact.maven.not-found",
                MavenArtifactLookupException.Kind.REPOSITORY_UNAVAILABLE,
                "artifact.maven.repository-unavailable",
                MavenArtifactLookupException.Kind.OFFLINE_MISS, "artifact.maven.offline-miss",
                MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                "artifact.maven.invalid-closure");

        for (Map.Entry<MavenArtifactLookupException.Kind, String> expectation
                : expectations.entrySet()) {
            MavenArtifactLookup lookup = (coordinate, offline) -> {
                throw new MavenArtifactLookupException(
                        expectation.getKey(), "lookup failed", new IOException("cause"));
            };
            String suffix = expectation.getKey().name().toLowerCase().replace('_', '-');
            SpecificationException exception = assertFailsAt(
                    Stage.ARTIFACT,
                    () -> new ArtifactPlanResolver(lookup).resolve(
                            plainPlan("maven-failure-" + suffix, document -> {
                                setCoreArtifacts(
                                        document,
                                        "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2",
                                        "job.jar");
                                ((ObjectNode) document.at("/subject/connectors/kafka"))
                                        .putArray("runtime_dependencies");
                            }),
                            ArtifactResolutionOptions.online(artifactRoot)));
            assertSingleIssue(exception, expectation.getValue(), CONNECTOR_PATH);
        }
    }

    @Test
    void mergesIdenticalExperimentArtifactsAsCommonAndKeepsChangedArtifactsSideScoped()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("baseline-job.jar"), true);
        createJar(artifactRoot.resolve("candidate-job.jar"), true);
        ResolvedScenarioPlan plan = experimentPlan("experiment-scopes", document -> {
            setCoreArtifacts(document, "connector.jar", "${job_artifact}");
            ObjectNode parameter = document.putObject("parameters")
                    .putObject("job_artifact");
            parameter.put("type", "string");
            parameter.put("default", "baseline-job.jar");
            ObjectNode experiment = document.putObject("experiment");
            experiment.put("claim", "The job artifact is the only variable.");
            experiment.putArray("varies").add("job_artifact");
            experiment.putObject("baseline").put("job_artifact", "baseline-job.jar");
            experiment.putObject("candidate").put("job_artifact", "candidate-job.jar");
        });

        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                plan, ArtifactResolutionOptions.online(artifactRoot));

        assertEquals(List.of(
                        ResolutionScope.COMMON + "@" + CONNECTOR_PATH,
                        ResolutionScope.BASELINE + "@" + JOB_PATH,
                        ResolutionScope.CANDIDATE + "@" + JOB_PATH),
                prepared.artifacts().stream()
                        .map(artifact -> artifact.scope() + "@" + artifact.path())
                        .toList());
        assertStagedCopy(
                prepared.artifact(ScenarioSide.BASELINE, JOB_PATH).orElseThrow(),
                artifactRoot.resolve("baseline-job.jar"));
        assertStagedCopy(
                prepared.artifact(ScenarioSide.CANDIDATE, JOB_PATH).orElseThrow(),
                artifactRoot.resolve("candidate-job.jar"));
        assertSame(
                prepared.artifact(ScenarioSide.BASELINE, CONNECTOR_PATH).orElseThrow(),
                prepared.artifact(ScenarioSide.CANDIDATE, CONNECTOR_PATH).orElseThrow());
    }

    @Test
    void suiteResolutionAggregatesEveryEntryFailureAndNeverReturnsAPartialPlan()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("valid.jar"), true);
        ScenarioBundle valid = bundle("valid-artifact", document -> setCoreArtifacts(
                document, "connector.jar", "valid.jar"), false);
        ScenarioBundle firstMissing = bundle("first-missing", document -> setCoreArtifacts(
                document, "connector.jar", "missing-one.jar"), false);
        ScenarioBundle secondMissing = bundle("second-missing", document -> setCoreArtifacts(
                document, "connector.jar", "missing-two.jar"), false);
        SuiteSpecification suite = suite("artifact-suite", scenarios -> {
            entry(scenarios, "valid-artifact");
            entry(scenarios, "first-missing");
            entry(scenarios, "second-missing");
        });
        ResolvedSuitePlan plan = new SuitePlanResolver().resolve(
                catalog(suite, valid, firstMissing, secondMissing), suite.name());

        SpecificationException exception = assertFailsAt(
                Stage.SUITE_PLANNING,
                () -> new ArtifactPlanResolver().resolve(
                        plan, ArtifactResolutionOptions.online(artifactRoot)));

        assertEquals(List.of("first-missing", "second-missing"), exception.diagnostics().stream()
                .map(issue -> issue.entry().orElseThrow().entryId())
                .toList());
        assertTrue(exception.diagnostics().stream().allMatch(issue ->
                issue.code().equals("artifact.local.not-found")
                        && issue.path().equals(JOB_PATH)));
        assertEquals(firstMissing.scenario().source(), exception.diagnostics().getFirst().source());
        assertEquals(secondMissing.scenario().source(), exception.diagnostics().get(1).source());
        assertPreparedWorkspacesEmpty(artifactRoot);
        assertTrue(Files.isRegularFile(artifactRoot.resolve("connector.jar")));
        assertTrue(Files.isRegularFile(artifactRoot.resolve("valid.jar")));
    }

    @Test
    void scenarioArtifactFailureDeletesItsFreshWorkspaceAndPartialCopies()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        ResolvedScenarioPlan plan = plainPlan("failed-scenario-cleanup", document ->
                setCoreArtifacts(document, "connector.jar", "missing-job.jar"));

        SpecificationException exception = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        plan, ArtifactResolutionOptions.online(artifactRoot)));

        assertTrue(exception.diagnostics().stream().anyMatch(issue ->
                issue.code().equals("artifact.local.not-found")
                        && issue.path().equals(JOB_PATH)));
        assertPreparedWorkspacesEmpty(artifactRoot);
        assertTrue(Files.isRegularFile(artifactRoot.resolve("connector.jar")));
    }

    @Test
    void standaloneCloseRemovesOnlyItsWorkspaceAndIsIdempotent() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        ResolvedScenarioPlan plan = plainPlan("standalone-lifecycle", document ->
                setCoreArtifacts(document, "connector.jar", "job.jar"));
        ArtifactPlanResolver resolver = new ArtifactPlanResolver();
        PreparedScenarioPlan first = resolver.resolve(
                plan, ArtifactResolutionOptions.online(artifactRoot));
        PreparedScenarioPlan second = resolver.resolve(
                plan, ArtifactResolutionOptions.online(artifactRoot));
        Path firstRoot = first.preparationRoot();
        Path secondRoot = second.preparationRoot();

        assertNotEquals(firstRoot, secondRoot);
        assertTrue(Files.isDirectory(firstRoot));
        assertTrue(Files.isDirectory(secondRoot));
        assertTrue(first.artifacts().stream()
                .allMatch(artifact -> artifact.preparedPath().startsWith(firstRoot)));

        first.close();
        assertFalse(Files.exists(firstRoot));
        assertTrue(Files.isDirectory(secondRoot));
        assertTrue(second.artifacts().stream()
                .allMatch(artifact -> Files.isRegularFile(artifact.preparedPath())));
        assertTrue(Files.isRegularFile(artifactRoot.resolve("connector.jar")));
        assertTrue(Files.isRegularFile(artifactRoot.resolve("job.jar")));

        first.close();
        assertFalse(Files.exists(firstRoot));
        assertTrue(Files.isDirectory(secondRoot));

        second.close();
        assertFalse(Files.exists(secondRoot));
    }

    @Test
    void suiteChildrenAreNonOwningAndSuiteCloseRemovesTheSharedWorkspace()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        ScenarioBundle alpha = bundle("lifecycle-alpha", document -> setCoreArtifacts(
                document, "connector.jar", "job.jar"), false);
        ScenarioBundle beta = bundle("lifecycle-beta", document -> setCoreArtifacts(
                document, "connector.jar", "job.jar"), false);
        SuiteSpecification suite = suite("lifecycle-suite", scenarios -> {
            entry(scenarios, "lifecycle-alpha");
            entry(scenarios, "lifecycle-beta");
        });
        ResolvedSuitePlan suitePlan = new SuitePlanResolver().resolve(
                catalog(suite, alpha, beta), suite.name());
        PreparedSuitePlan prepared = new ArtifactPlanResolver().resolve(
                suitePlan, ArtifactResolutionOptions.online(artifactRoot));
        Path sharedRoot = prepared.preparationRoot();

        assertTrue(Files.isDirectory(sharedRoot));
        assertTrue(prepared.entries().stream().allMatch(entry ->
                entry.scenario().preparationRoot().equals(sharedRoot)
                        && entry.scenario().artifacts().stream().allMatch(artifact ->
                                artifact.preparedPath().startsWith(sharedRoot))));

        prepared.entries().getFirst().scenario().close();
        prepared.entries().get(1).scenario().close();
        assertTrue(Files.isDirectory(sharedRoot));
        assertTrue(prepared.entries().stream()
                .flatMap(entry -> entry.scenario().artifacts().stream())
                .allMatch(artifact -> Files.isRegularFile(artifact.preparedPath())));

        prepared.close();
        assertFalse(Files.exists(sharedRoot));
        prepared.close();
        assertFalse(Files.exists(sharedRoot));
    }

    @Test
    void preparedScenarioAndSuiteViewsAreImmutableAndReproducible() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        ScenarioBundle bundle = bundle("immutable-artifacts", document -> setCoreArtifacts(
                document, "connector.jar", "job.jar"), false);
        ResolvedScenarioPlan scenarioPlan = new ScenarioPlanResolver().resolve(
                bundle, ResolutionRequest.none());
        ArtifactPlanResolver resolver = new ArtifactPlanResolver();

        PreparedScenarioPlan first = resolver.resolve(
                scenarioPlan, ArtifactResolutionOptions.online(artifactRoot));
        PreparedScenarioPlan second = resolver.resolve(
                scenarioPlan, ArtifactResolutionOptions.online(artifactRoot));
        assertNotSame(first, second);
        assertEquals(artifactSignature(first), artifactSignature(second));
        assertThrows(UnsupportedOperationException.class, () -> first.artifacts().clear());

        SuiteSpecification suite = suite(
                "immutable-suite", scenarios -> entry(scenarios, "immutable-artifacts"));
        ResolvedSuitePlan suitePlan = new SuitePlanResolver().resolve(
                catalog(suite, bundle), suite.name());
        PreparedSuitePlan preparedSuite = resolver.resolve(
                suitePlan, ArtifactResolutionOptions.online(artifactRoot));
        assertSame(suitePlan, preparedSuite.suitePlan());
        assertSame(suitePlan.entries().getFirst(),
                preparedSuite.entries().getFirst().suiteEntry());
        assertSame(preparedSuite.entries().getFirst(),
                preparedSuite.entry("immutable-artifacts").orElseThrow());
        assertTrue(preparedSuite.entry("missing").isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> preparedSuite.entries().clear());
    }

    private PreparedScenarioPlan resolvePlain(String name, String connector, String job) {
        return new ArtifactPlanResolver().resolve(
                plainPlan(name, document -> setCoreArtifacts(document, connector, job)),
                ArtifactResolutionOptions.online(artifactRoot));
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
            String name, Consumer<ObjectNode> changes, boolean experimentExpectation) {
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
        Path source = resource("smoke-suite.yaml").resolveSibling(name + ".yaml");
        return loader.validateSuiteDocument(source, document);
    }

    private static ObjectNode entry(ArrayNode scenarios, String scenarioName) {
        return scenarios.addObject().put("scenario", scenarioName);
    }

    private static SpecificationCatalog catalog(
            SuiteSpecification suite, ScenarioBundle... bundles) {
        return SpecificationCatalogTestFactory.catalog(suite, bundles);
    }

    private static void setCoreArtifacts(
            ObjectNode document, String connectorReference, String jobReference) {
        ObjectNode connector = (ObjectNode) document.at("/subject/connectors/kafka");
        connector.put("artifact", connectorReference);
        connector.remove("runtime_dependencies");
        if (!connectorReference.startsWith("maven:")
                && !connectorReference.contains("${")) {
            connector.putArray("runtime_dependencies");
        }
        ((ObjectNode) document.at("/workload/jobs/0")).put("jar", jobReference);
    }

    private static void addCustomValidator(ObjectNode validator, String artifact) {
        validator.put("type", "custom");
        validator.put("artifact", artifact);
        validator.put("timeout", "30s");
    }

    private static Path createJar(Path path, boolean mainClass) throws IOException {
        Files.createDirectories(path.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (mainClass) {
            manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
            manifest.getMainAttributes().putValue(
                    WorkloadProtocolArtifactValidator.ATTRIBUTE,
                    WorkloadProtocolArtifactValidator.VERSION);
        }
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            output.putNextEntry(new JarEntry("payload.txt"));
            output.write("payload".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return path;
    }

    private static Path createJarWithCorruptStoredEntry(Path path, boolean mainClass)
            throws IOException {
        Files.createDirectories(path.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (mainClass) {
            manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
            manifest.getMainAttributes().putValue(
                    WorkloadProtocolArtifactValidator.ATTRIBUTE,
                    WorkloadProtocolArtifactValidator.VERSION);
        }
        byte[] payload = "unique-stored-payload-for-crc-check"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CRC32 checksum = new CRC32();
        checksum.update(payload);
        JarEntry entry = new JarEntry("payload.bin");
        entry.setMethod(ZipEntry.STORED);
        entry.setSize(payload.length);
        entry.setCompressedSize(payload.length);
        entry.setCrc(checksum.getValue());
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            output.putNextEntry(entry);
            output.write(payload);
            output.closeEntry();
        }

        byte[] bytes = Files.readAllBytes(path);
        int offset = indexOf(bytes, payload);
        if (offset < 0) {
            throw new AssertionError("Stored test payload was not found in the JAR bytes");
        }
        bytes[offset] ^= 0x01;
        Files.write(path, bytes);
        return path;
    }

    private static int indexOf(byte[] bytes, byte[] pattern) {
        outer:
        for (int index = 0; index <= bytes.length - pattern.length; index++) {
            for (int offset = 0; offset < pattern.length; offset++) {
                if (bytes[index + offset] != pattern[offset]) {
                    continue outer;
                }
            }
            return index;
        }
        return -1;
    }

    private static void assertSingleIssue(
            SpecificationException exception, String code, String path) {
        assertEquals(1, exception.diagnostics().size());
        Diagnostic issue = exception.diagnostics().getFirst();
        assertEquals(code, issue.code());
        assertEquals(path, issue.path());
        assertFalse(issue.message().isBlank());
    }

    private static void assertPreparedWorkspacesEmpty(Path root) throws IOException {
        Path preparationParent = root.resolve(
                ".flink-stability/artifacts/prepared");
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

    private static void assertStagedCopy(ResolvedArtifact artifact, Path source)
            throws IOException {
        Path original = source.toRealPath();
        Path staged = artifact.preparedPath().toRealPath();
        assertEquals(original, artifact.sourcePath());
        assertFalse(Files.isSameFile(original, staged));
        assertTrue(Files.isRegularFile(staged));
        assertTrue(staged.getFileName().toString().startsWith(artifact.sha256()));
        assertArrayEquals(Files.readAllBytes(original), Files.readAllBytes(staged));
        assertEquals(artifact.sha256(), sha256(staged));
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return java.util.HexFormat.of().formatHex(digest.digest(Files.readAllBytes(path)));
        } catch (NoSuchAlgorithmException exception) {
            throw new AssertionError("Java must provide SHA-256", exception);
        }
    }

    private static List<String> artifactSignature(PreparedScenarioPlan plan) {
        return plan.artifacts().stream()
                .map(artifact -> artifact.scope() + "|" + artifact.role() + "|"
                        + artifact.path() + "|" + artifact.declaredReference() + "|"
                        + artifact.sourcePath() + "|"
                        + artifact.preparedPath().getFileName() + "|" + artifact.sha256())
                .toList();
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
