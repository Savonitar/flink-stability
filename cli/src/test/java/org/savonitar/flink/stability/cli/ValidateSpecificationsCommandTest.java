package org.savonitar.flink.stability.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidateSpecificationsCommandTest {
    private static final String JOB_POINTER = "$/workload/jobs/0/jar";

    @TempDir
    Path temporaryDirectory;

    @Test
    void noSubcommandPrintsUsageAndReturnsUsageExit() {
        Invocation result = execute();

        assertEquals(CommandLine.ExitCode.USAGE, result.exitCode());
        assertTrue(result.stdout().contains("Usage: chaos-kit"));
        assertEquals("", result.stderr());
    }

    @Test
    void validateHelpReturnsSuccessWithoutRequiringASelection() {
        Invocation result = execute("validate", "--help");

        assertEquals(CommandLine.ExitCode.OK, result.exitCode());
        assertTrue(result.stdout().contains("--catalog-root"));
        assertTrue(result.stdout().contains("--scenario"));
        assertTrue(result.stdout().contains("--suite"));
        assertEquals("", result.stderr());
    }

    @Test
    void validateRequiresExactlyOneScenarioOrSuiteTarget() {
        Invocation missing = execute(
                "validate", "--catalog-root", temporaryDirectory.toString());
        Invocation both = execute(
                "validate", "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "one", "--suite", "all");

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.USAGE, missing.exitCode()),
                () -> assertTrue(missing.stderr().contains("--scenario")),
                () -> assertTrue(missing.stderr().contains("--suite")),
                () -> assertEquals(CommandLine.ExitCode.USAGE, both.exitCode()),
                () -> assertTrue(both.stderr().contains("--scenario")),
                () -> assertTrue(both.stderr().contains("--suite")));
    }

    @Test
    void malformedAndDuplicateParameterAssignmentsReturnUsageExit() {
        List<String> invalidAssignments = List.of(
                "missing-separator",
                "=value",
                "bad-name=value",
                "empty=",
                "quoted=\"unterminated",
                "trailing=\"value\"junk",
                "second=\"value\" \"extra\"");

        for (String assignment : invalidAssignments) {
            Invocation result = execute(
                    "validate", "--catalog-root", temporaryDirectory.toString(),
                    "--scenario", "unused", "-p", assignment);
            assertEquals(CommandLine.ExitCode.USAGE, result.exitCode(), assignment);
            assertFalse(result.stderr().isBlank(), assignment);
        }

        Invocation duplicate = execute(
                "validate", "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "unused", "-p", "count=1", "-p", "count=2");
        assertEquals(CommandLine.ExitCode.USAGE, duplicate.exitCode());
        assertTrue(duplicate.stderr().contains("assigned more than once"));
    }

    @Test
    void validatesParameterizedScenarioWithExplicitArtifactRootAndCleansWorkspace()
            throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        Path artifactRoot = Files.createDirectories(temporaryDirectory.resolve("artifacts"));
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("jobs/job.jar"), true);
        writePair(catalogRoot, "typed-scenario", "connector.jar", "jobs/job.jar",
                """
                parameters:
                  enabled: { type: boolean, required: true }
                  count: { type: integer, required: true }
                  label: { type: string, required: true }
                  code: { type: string, required: true }

                """);

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "typed-scenario",
                "--artifact-root", artifactRoot.toString(), "--offline",
                "-p", "enabled=true",
                "-p", "count=12",
                "-p", "label=plain",
                "-p", "code=\"42\"");

        assertEquals(CommandLine.ExitCode.OK, result.exitCode());
        assertEquals("valid: scenario 'typed-scenario' (2 artifacts)\n", result.stdout());
        assertEquals("", result.stderr());
        assertNoPreparedWorkspace(artifactRoot);
    }

    @Test
    void eagerlyPreparesEveryEntryInAValidSuite() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        Path artifactRoot = Files.createDirectories(temporaryDirectory.resolve("artifacts"));
        writePair(catalogRoot, "alpha", "alpha-connector.jar", "alpha-job.jar", "");
        writePair(catalogRoot, "beta", "beta-connector.jar", "beta-job.jar", "");
        createJar(artifactRoot.resolve("alpha-connector.jar"), false);
        createJar(artifactRoot.resolve("alpha-job.jar"), true);
        createJar(artifactRoot.resolve("beta-connector.jar"), false);
        createJar(artifactRoot.resolve("beta-job.jar"), true);
        writeSuite(catalogRoot, "all-valid", List.of("alpha", "beta"));

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--suite", "all-valid",
                "--artifact-root", artifactRoot.toString(), "--offline");

        assertEquals(CommandLine.ExitCode.OK, result.exitCode());
        assertEquals("valid: suite 'all-valid' (2 entries, 4 artifacts)\n", result.stdout());
        assertEquals("", result.stderr());
        assertNoPreparedWorkspace(artifactRoot);
    }

    @Test
    void unknownTargetsReturnUsageExitAndSortedAvailableNames() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "zeta", "unused.jar", "unused.jar", "");
        writePair(catalogRoot, "alpha", "unused.jar", "unused.jar", "");
        writeSuite(catalogRoot, "zeta-suite", List.of("zeta"));
        writeSuite(catalogRoot, "alpha-suite", List.of("alpha"));

        Invocation scenario = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "missing");
        Invocation suite = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--suite", "missing-suite");

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.USAGE, scenario.exitCode()),
                () -> assertTrue(scenario.stderr().contains(
                        "available scenarios: [alpha, zeta]")),
                () -> assertEquals(CommandLine.ExitCode.USAGE, suite.exitCode()),
                () -> assertTrue(suite.stderr().contains(
                        "available suites: [alpha-suite, zeta-suite]")));
    }

    @Test
    void schemaFailureRendersRelativeSourceScopeCodeAndPointer() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "schema-bad", "unused.jar", "unused.jar", "");
        Path scenario = catalogRoot.resolve("schema-bad.yaml");
        Files.writeString(scenario, Files.readString(scenario) + "unexpected: true\n");

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "schema-bad");

        assertValidationFailure(result,
                "schema-bad.yaml [common] schema.additional-properties at $");
    }

    @Test
    void catalogFailureRendersRelativeSourceScopeCodeAndPointer() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writeScenario(catalogRoot, "orphan", "unused.jar", "unused.jar", "");

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "orphan");

        assertValidationFailure(result,
                "orphan.yaml [common] catalog.expected-result-missing at $");
    }

    @Test
    void parameterFailureRendersRelativeSourceScopeCodeAndPointer() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "parameter-bad", "unused.jar", "unused.jar",
                """
                parameters:
                  count: { type: integer, required: true }

                """);

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "parameter-bad", "-p", "count=text");

        assertValidationFailure(result,
                "parameter-bad.yaml [common] parameter.type-mismatch "
                        + "at $/submit-overrides/count");
    }

    @Test
    void preflightFailureRendersRelativeSourceScopeCodeAndPointer() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "preflight-bad", "unused.jar", "unused.jar", "");
        Path scenario = catalogRoot.resolve("preflight-bad.yaml");
        Files.writeString(scenario, Files.readString(scenario).replace(
                "            replication_factor: 1\n            input_source:",
                "            replication_factor: 2\n            input_source:"));

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "preflight-bad");

        assertValidationFailure(result,
                "preflight-bad.yaml [single] "
                        + "preflight.kafka.replication-exceeds-brokers "
                        + "at $/setup/kafka/clusters/main/topics/0/replication_factor");
    }

    @Test
    void artifactFailureRendersRelativeSourceScopeCodeAndPointer() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        Path artifactRoot = Files.createDirectories(temporaryDirectory.resolve("artifacts"));
        writePair(catalogRoot, "artifact-bad", "connector.jar", "missing-job.jar", "");
        createJar(artifactRoot.resolve("connector.jar"), false);

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "artifact-bad",
                "--artifact-root", artifactRoot.toString(), "--offline");

        assertValidationFailure(result,
                "artifact-bad.yaml [single] artifact.local.not-found at " + JOB_POINTER);
        assertNoPreparedWorkspace(artifactRoot);
    }

    @Test
    void suiteFailureNamesTheLaterEntryAndLeavesNoPartialWorkspace() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        Path artifactRoot = Files.createDirectories(temporaryDirectory.resolve("artifacts"));
        writePair(catalogRoot, "good", "good-connector.jar", "good-job.jar", "");
        writePair(catalogRoot, "bad", "bad-connector.jar", "missing-job.jar", "");
        createJar(artifactRoot.resolve("good-connector.jar"), false);
        createJar(artifactRoot.resolve("good-job.jar"), true);
        createJar(artifactRoot.resolve("bad-connector.jar"), false);
        writeSuite(catalogRoot, "mixed", List.of("good", "bad"));

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--suite", "mixed",
                "--artifact-root", artifactRoot.toString(), "--offline");

        assertValidationFailure(result,
                "bad.yaml [entry 1 'bad', single] artifact.local.not-found at " + JOB_POINTER);
        assertNoPreparedWorkspace(artifactRoot);
    }

    @Test
    void mainExecuteReturnsTheCommandExitCode() throws IOException {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        try (ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
             ByteArrayOutputStream capturedErr = new ByteArrayOutputStream();
             PrintStream replacementOut = new PrintStream(capturedOut, true, StandardCharsets.UTF_8);
             PrintStream replacementErr = new PrintStream(capturedErr, true, StandardCharsets.UTF_8)) {
            System.setOut(replacementOut);
            System.setErr(replacementErr);

            assertEquals(CommandLine.ExitCode.USAGE, Main.execute("validate"));
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    private static Invocation execute(String... arguments) {
        StringWriter stdout = new StringWriter();
        StringWriter stderr = new StringWriter();
        CommandLine commandLine = new CommandLine(new ChaosKitCommand())
                .setOut(new PrintWriter(stdout, true))
                .setErr(new PrintWriter(stderr, true));
        int exitCode = commandLine.execute(arguments);
        return new Invocation(exitCode, stdout.toString(), stderr.toString());
    }

    private static void assertValidationFailure(Invocation result, String diagnostic) {
        assertEquals(CommandLine.ExitCode.SOFTWARE, result.exitCode(), result.stderr());
        assertEquals("", result.stdout());
        assertTrue(result.stderr().startsWith("validation failed:\n"), result.stderr());
        assertTrue(result.stderr().contains(diagnostic), result.stderr());
    }

    private static void writePair(
            Path root,
            String name,
            String connectorReference,
            String jobReference,
            String parameterBlock) throws IOException {
        writeScenario(root, name, connectorReference, jobReference, parameterBlock);
        Files.writeString(root.resolve(name + ".expected.yaml"), """
                format: v1
                kind: expected-result

                meta:
                  name: %s.expected
                  scenario: %s

                default:
                  outcome: pass
                """.formatted(name, name));
    }

    private static void writeScenario(
            Path root,
            String name,
            String connectorReference,
            String jobReference,
            String parameterBlock) throws IOException {
        String connectorDependencies = connectorReference.startsWith("maven:")
                ? ""
                : "      runtime_dependencies: []\n";
        Files.writeString(root.resolve(name + ".yaml"), """
                format: v1
                kind: scenario

                meta:
                  name: %s
                  description: CLI validation fixture.

                %sruns: 1

                setup:
                  kafka:
                    clusters:
                      main:
                        image: apache/kafka:4.0.0
                        brokers: 1
                        topics:
                          - name: input
                            partitions: 1
                            replication_factor: 1
                            input_source:
                              mode: generated
                              format: integer-sequence
                              total: 10
                          - name: output
                            partitions: 1
                            replication_factor: 1
                  flink:
                    image: flink:2.2.0

                subject:
                  connectors:
                    kafka:
                      artifact: %s
                %s

                workload:
                  jobs:
                    - alias: eos-job
                      jar: %s
                      connectors: [kafka]
                      parallelism: 1
                      source: { cluster: main, topic: input }
                      sink:
                        cluster: main
                        topic: output
                        delivery_guarantee: EXACTLY_ONCE
                        transactional_id_prefix: cli-test
                        transaction_id_naming_strategy: INCREMENTING
                      checkpointing:
                        interval: 5s
                        mode: EXACTLY_ONCE

                phases:
                  - name: verify-running
                    steps:
                      - await:
                          condition: { type: job-state, job: eos-job, state: RUNNING }
                          timeout: 2m
                          on_timeout: inconclusive

                terminal_validations:
                  - type: kafka.id-set
                    cluster: main
                    topic: output
                    expected: input-manifest
                """.formatted(
                        name,
                        parameterBlock,
                        connectorReference,
                        connectorDependencies,
                        jobReference));
    }

    private static void writeSuite(Path root, String name, List<String> scenarios)
            throws IOException {
        StringBuilder entries = new StringBuilder();
        scenarios.forEach(scenario -> entries.append("  - scenario: ").append(scenario).append('\n'));
        Files.writeString(root.resolve(name + ".yaml"), """
                format: v1
                kind: suite

                meta:
                  name: %s

                scenarios:
                %s""".formatted(name, entries));
    }

    private static Path createJar(Path path, boolean executable) throws IOException {
        Files.createDirectories(path.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (executable) {
            manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
        }
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            output.putNextEntry(new JarEntry("payload.txt"));
            output.write("payload".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return path;
    }

    private static void assertNoPreparedWorkspace(Path artifactRoot) throws IOException {
        Path preparedParent = artifactRoot.resolve(".flink-stability/artifacts/prepared");
        if (!Files.exists(preparedParent)) {
            return;
        }
        try (Stream<Path> children = Files.list(preparedParent)) {
            assertTrue(children.noneMatch(path ->
                    path.getFileName().toString().startsWith("plan-")));
        }
    }

    private record Invocation(int exitCode, String stdout, String stderr) {}
}
