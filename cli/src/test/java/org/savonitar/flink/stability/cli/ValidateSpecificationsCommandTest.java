package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidateSpecificationsCommandTest {
    private static final YAMLMapper YAML = new YAMLMapper();
    private static final String JOB_POINTER = "$/workload/jobs/0/jar";

    @TempDir
    Path temporaryDirectory;

    @Test
    void showsPinnedCustomPlanWithoutDockerAndIncludesWorkloadIdentity() throws Exception {
        Path catalog = Files.createDirectories(temporaryDirectory.resolve("custom-catalog"));
        Path artifacts = Files.createDirectories(temporaryDirectory.resolve("artifacts"));
        createJar(artifacts.resolve("job.jar"), true);
        writePair(catalog, "custom", "connector.jar", "job.jar", "");
        ObjectNode document = (ObjectNode) YAML.readTree(catalog.resolve("custom.yaml").toFile());
        document.put("health_retry_limit", 0);
        ObjectNode flink = (ObjectNode) document.at("/setup/flink");
        flink.put("image", "internal/flink:vendor").put("line", "2.3")
                .put("image_id", "sha256:" + "a".repeat(64));
        flink.putObject("runtime_jar").put("container_path", "/opt/flink/lib/flink-dist-vendor.jar")
                .put("sha256", "b".repeat(64));
        flink.putObject("config").put("pipeline.name", "custom-test");
        ObjectNode broker = (ObjectNode) document.at("/setup/kafka/clusters/main");
        broker.put("image", "internal/kafka:vendor").put("image_id", "sha256:" + "c".repeat(64))
                .put("transaction_version", "broker-default");
        broker.putObject("launch").put("type", "generic-kraft").put("layout", "confluent-platform");
        broker.putObject("broker_config").put("transaction.two.phase.commit.enable", true);
        ObjectNode connector = (ObjectNode) document.at("/subject/connectors/kafka");
        connector.remove("runtime_dependencies");
        connector.put("artifact", "image:/opt/flink/lib/subject.jar").put("sha256", "d".repeat(64));
        ((ObjectNode) document.at("/workload/jobs/0/sink"))
                .put("transaction_id_naming_strategy", "connector-default");
        Files.writeString(catalog.resolve("custom.yaml"), YAML.writeValueAsString(document));

        Invocation result = execute("validate", "--catalog-root", catalog.toString(), "--scenario", "custom",
                "--artifact-root", artifacts.toString(), "--offline", "--show-plan");
        assertEquals(0, result.exitCode(), result.stderr());
        var plan = new com.fasterxml.jackson.databind.ObjectMapper().readTree(result.stdout());
        assertEquals("2.3", plan.at("/resolved/setup/flink/line").asText());
        assertEquals("generic-kraft", plan.at("/kafkaRuntime/launchType").asText());
        assertEquals("confluent-platform", plan.at("/kafkaRuntime/layout").asText());
        assertEquals(1, plan.at("/kafkaRuntime/launches").size());
        assertEquals("confluent-platform", plan.at("/kafkaRuntime/launches/0/layout").asText());
        assertEquals("/usr/bin/kafka-broker-api-versions --bootstrap-server localhost:19092",
                plan.at("/kafkaRuntime/launches/0/readinessCommand").asText());
        assertTrue(plan.at("/kafkaRuntime/launches/0/command/2").asText().contains("/usr/bin/kafka-storage format"));
        assertEquals("custom-test", plan.at("/flinkConfig/pipeline.name").asText());
        assertEquals("connector-default", plan.at("/customRuntimeSubject/transactionIdNamingStrategy").asText());
        assertEquals("job.jar", plan.at("/customRuntimeSubject/workload/artifact").asText());
        assertEquals("author-assertion", plan.at("/customRuntimeSubject/compatibilityAssertions/flink").asText());
        assertEquals("author-assertion", plan.at("/customRuntimeSubject/compatibilityAssertions/kafka").asText());
        assertTrue(plan.at("/customRuntimeSubject/workload/sha256").asText().matches("[0-9a-f]{64}"));
        assertNoPreparedWorkspace(artifacts);
    }

    @Test
    void showPlanRejectsSuiteSelection() {
        Invocation result = execute("validate", "--catalog-root", temporaryDirectory.toString(),
                "--suite", "unused", "--show-plan");
        assertEquals(CommandLine.ExitCode.USAGE, result.exitCode());
        assertTrue(result.stderr().contains("--show-plan requires --scenario"));
    }

    @Test
    void rejectsUnsupportedAndIgnoredKafkaLayoutsWithoutShowPlanOrArtifactAccess() throws IOException {
        for (var example : List.of(
                List.of("generic-kraft", "unknown", "runner.kafka.layout-unsupported"),
                List.of("apache-kafka", "apache", "runner.kafka.layout-not-applicable"),
                List.of("apache-kafka", "confluent-platform", "runner.kafka.layout-not-applicable"))) {
            Path catalog = Files.createDirectories(temporaryDirectory.resolve(example.get(0) + "-" + example.get(1)));
            writePair(catalog, "bad-layout", "missing-connector.jar", "missing-job.jar", "");
            updateDocument(catalog.resolve("bad-layout.yaml"), document ->
                    ((ObjectNode)document.at("/setup/kafka/clusters/main")).putObject("launch")
                            .put("type", example.get(0)).put("layout", example.get(1)));
            Invocation result = execute("validate", "--catalog-root", catalog.toString(),
                    "--scenario", "bad-layout", "--artifact-root", temporaryDirectory.toString(), "--offline");
            assertValidationFailure(result, "bad-layout.yaml [single] " + example.get(2)
                    + " at $/setup/kafka/clusters/main/launch/layout");
        }
    }

    @Test
    void capabilityVerdictDoesNotDependOnRenderingOfflineOrSuiteSelection() throws IOException {
        Path catalog = Files.createDirectories(temporaryDirectory.resolve("capabilities"));
        writePair(catalog, "proxy", "missing-connector.jar", "missing-job.jar", "");
        writePair(catalog, "slots", "missing-connector.jar", "missing-job.jar", "");
        updateDocument(catalog.resolve("proxy.yaml"), document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
            document.at("/setup/kafka/clusters/main/topics").forEach(topic ->
                    ((ObjectNode) topic).put("replication_factor", 3));
            ObjectNode proxy = ((ObjectNode) document.at("/setup")).putObject("proxies")
                    .putObject("kafka-proxy");
            proxy.put("type", "kroxylicious").put("cluster", "main")
                    .put("listen", "kafka-proxy:9092");
            proxy.putObject("bootstrap").put("cluster", "main");
            ((ObjectNode) document.at("/workload/jobs/0/sink"))
                    .put("connect_via_proxy", "kafka-proxy");
        });
        updateDocument(catalog.resolve("slots.yaml"), document ->
                ((ObjectNode) document.at("/workload/jobs/0")).put("parallelism", 3));
        writeSuite(catalog, "unsupported", List.of("proxy", "slots"));
        for (boolean offline : List.of(false, true)) {
            for (boolean showPlan : List.of(false, true)) {
                var args = new java.util.ArrayList<>(List.of("validate", "--catalog-root",
                        catalog.toString(), "--scenario", "proxy", "--artifact-root",
                        temporaryDirectory.toString()));
                if (offline) args.add("--offline");
                if (showPlan) args.add("--show-plan");
                Invocation result = execute(args.toArray(String[]::new));
                assertValidationFailure(result, "runner.kafka.multi-broker-proxy-unsupported");
                assertFalse(result.stderr().contains("artifact.local.not-found"));
            }
        }
        Invocation suite = execute("validate", "--catalog-root", catalog.toString(),
                "--suite", "unsupported", "--artifact-root", temporaryDirectory.toString());
        assertValidationFailure(suite, "proxy.yaml [entry 0 'proxy', single] "
                + "runner.kafka.multi-broker-proxy-unsupported at $/setup/proxies");
        assertTrue(suite.stderr().contains("slots.yaml [entry 1 'slots', single] "
                + "runner.workload.insufficient-task-slots"), suite.stderr());
        Invocation slots = execute("validate", "--catalog-root", catalog.toString(),
                "--scenario", "slots", "--artifact-root", temporaryDirectory.toString());
        assertValidationFailure(slots, "runner.workload.insufficient-task-slots");
        assertNoPreparedWorkspace(temporaryDirectory);
    }

    @Test
    void laterSuiteCapabilityFailurePrecedesEarlierMissingArtifacts() throws IOException {
        Path catalog = Files.createDirectories(temporaryDirectory.resolve("suite-order"));
        writePair(catalog, "first", "missing.jar", "missing-job.jar", "");
        writePair(catalog, "later", "missing.jar", "missing-job.jar", "");
        updateDocument(catalog.resolve("later.yaml"), document ->
                ((ObjectNode) document.at("/workload/jobs/0")).put("parallelism", 3));
        writeSuite(catalog, "ordered", List.of("first", "later"));
        Invocation result = execute("validate", "--catalog-root", catalog.toString(),
                "--suite", "ordered", "--artifact-root", temporaryDirectory.toString(), "--offline");
        assertValidationFailure(result, "later.yaml [entry 1 'later', single] "
                + "runner.workload.insufficient-task-slots");
        assertFalse(result.stderr().contains("artifact.local.not-found"));
        assertNoPreparedWorkspace(temporaryDirectory);
    }

    @Test
    void bindingFailureClosesScenarioAndEntireSuiteWorkspace() throws IOException {
        Path catalog = Files.createDirectories(temporaryDirectory.resolve("binding"));
        Path artifacts = Files.createDirectories(temporaryDirectory.resolve("binding-artifacts"));
        writePair(catalog, "good", "connector.jar", "job.jar", "");
        writePair(catalog, "bad", "unrelated.jar", "job.jar", "");
        createJar(artifacts.resolve("connector.jar"), false);
        createJar(artifacts.resolve("job.jar"), true);
        createRawWorkloadJar(artifacts.resolve("unrelated.jar"), List.of());
        writeSuite(catalog, "binding-suite", List.of("good", "bad"));
        for (String selector : List.of("--scenario", "--suite")) {
            Invocation result = execute("validate", "--catalog-root", catalog.toString(),
                    selector, selector.equals("--scenario") ? "bad" : "binding-suite",
                    "--artifact-root", artifacts.toString(), "--offline");
            assertValidationFailure(result, "runner.subject.entry-class-missing");
            if (selector.equals("--suite")) {
                assertTrue(result.stderr().contains("[entry 1 'bad', single]"), result.stderr());
            }
            assertNoPreparedWorkspace(artifacts);
        }
    }

    @Test
    void noSubcommandPrintsUsageAndReturnsUsageExit() {
        Invocation result = execute();

        assertEquals(CommandLine.ExitCode.USAGE, result.exitCode());
        assertTrue(result.stdout().contains("Usage: flink-stability"));
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
    void validDockerFreeCommandDoesNotInitializeTheDockerRuntimeProvider()
            throws IOException {
        Path catalogRoot = Files.createDirectories(
                temporaryDirectory.resolve("docker-free-catalog"));
        Path artifactRoot = Files.createDirectories(
                temporaryDirectory.resolve("docker-free-artifacts"));
        createJar(artifactRoot.resolve("connector.jar"), false);
        createJar(artifactRoot.resolve("job.jar"), true);
        writePair(catalogRoot, "docker-free", "connector.jar", "job.jar", "");

        String previousDockerHost = System.getProperty("docker.host");
        System.setProperty("docker.host", "tcp://127.0.0.1:1");
        try {
            Invocation result = execute(
                    "validate",
                    "--catalog-root", catalogRoot.toString(),
                    "--scenario", "docker-free",
                    "--artifact-root", artifactRoot.toString(),
                    "--offline");

            assertEquals(CommandLine.ExitCode.OK, result.exitCode(), result.stderr());
            assertEquals("valid: scenario 'docker-free' (2 artifacts)\n", result.stdout());
            assertEquals("", result.stderr());
            assertNoPreparedWorkspace(artifactRoot);
        } finally {
            if (previousDockerHost == null) {
                System.clearProperty("docker.host");
            } else {
                System.setProperty("docker.host", previousDockerHost);
            }
        }
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
        updateDocument(scenario, document -> document.put("unexpected", true));

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
        updateDocument(scenario, document ->
                ((ObjectNode) document.requiredAt("/setup/kafka/clusters/main/topics/0"))
                        .put("replication_factor", 2));

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "preflight-bad");

        assertValidationFailure(result,
                "preflight-bad.yaml [single] "
                        + "preflight.kafka.replication-exceeds-brokers "
                        + "at $/setup/kafka/clusters/main/topics/0/replication_factor");
    }

    @Test
    void expectationFailureRendersRelativeSourceScopeCodeAndPointer() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "expectation-bad", "unused.jar", "unused.jar", "");
        updateDocument(catalogRoot.resolve("expectation-bad.expected.yaml"), document -> {
            ObjectNode fallback = document.putObject("default");
            fallback.putObject("baseline").put("outcome", "pass");
            fallback.putObject("candidate").put("outcome", "pass");
        });

        Invocation result = execute("validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "expectation-bad");

        assertValidationFailure(result,
                "expectation-bad.expected.yaml [common] expectation.shape-mismatch at $/default");
    }

    @Test
    void suitePlanningFailureRendersEntryIdentity() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "preflight-bad", "unused.jar", "unused.jar", "");
        updateDocument(catalogRoot.resolve("preflight-bad.yaml"), document ->
                ((ObjectNode) document.requiredAt("/setup/kafka/clusters/main/topics/0"))
                        .put("replication_factor", 2));
        writeSuite(catalogRoot, "suite-bad", List.of("preflight-bad"));

        Invocation result = execute("validate", "--catalog-root", catalogRoot.toString(),
                "--suite", "suite-bad");

        assertValidationFailure(result,
                "preflight-bad.yaml [entry 0 'preflight-bad', single] "
                        + "preflight.kafka.replication-exceeds-brokers "
                        + "at $/setup/kafka/clusters/main/topics/0/replication_factor");
    }

    @Test
    void runnerCapabilityFailureRejectsBeforeArtifactsOrDocker() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "runner-bad", "unused.jar", "unused.jar", "");
        updateDocument(catalogRoot.resolve("runner-bad.yaml"), document -> document.put("runs", 2));

        Invocation result = execute("run", "--catalog-root", catalogRoot.toString(),
                "--scenario", "runner-bad", "--offline");

        assertValidationFailure(result,
                "runner-bad.yaml [single] runner.invocation.runs-unsupported at $/runs");
    }

    @Test
    void rejectsAnUnsupportedKafkaLineBeforeArtifactPreparation() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "kafka-line", "unused.jar", "unused.jar", "");
        Path scenario = catalogRoot.resolve("kafka-line.yaml");
        updateDocument(scenario, document ->
                ((ObjectNode) document.requiredAt("/setup/kafka/clusters/main"))
                        .put("image", "apache/kafka:4.1.0"));

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--scenario", "kafka-line");

        assertValidationFailure(
                result,
                "kafka-line.yaml [single] runner.kafka.image-version-unsupported "
                        + "at $/setup/kafka/clusters/main/image");
    }

    @Test
    void suiteResolutionAttributesAnUnsupportedKafkaLineToItsEntry() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        writePair(catalogRoot, "valid-line", "unused.jar", "unused.jar", "");
        writePair(catalogRoot, "invalid-line", "unused.jar", "unused.jar", "");
        Path invalid = catalogRoot.resolve("invalid-line.yaml");
        updateDocument(invalid, document ->
                ((ObjectNode) document.requiredAt("/setup/kafka/clusters/main"))
                        .put("image", "custom/kafka:4.0.0"));
        writeSuite(catalogRoot, "kafka-lines", List.of("valid-line", "invalid-line"));

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--suite", "kafka-lines");

        assertValidationFailure(
                result,
                "invalid-line.yaml [entry 1 'invalid-line', single] "
                        + "runner.kafka.image-version-unsupported "
                        + "at $/setup/kafka/clusters/main/image");
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
    void rejectsMissingBlankUnsupportedAndDuplicateWorkloadProtocolMarkers()
            throws IOException {
        List<ProtocolCase> cases = List.of(
                new ProtocolCase("missing", List.of(), "missing"),
                new ProtocolCase("blank", List.of(
                        "Flink-Stability-Workload-Protocol: "), "missing"),
                new ProtocolCase("unsupported", List.of(
                        "Flink-Stability-Workload-Protocol: v2"), "unsupported"),
                new ProtocolCase("duplicate", List.of(
                        "Flink-Stability-Workload-Protocol: v1",
                        "flink-stability-workload-protocol: v1"), "duplicate"));

        for (ProtocolCase protocolCase : cases) {
            Path root = Files.createDirectories(
                    temporaryDirectory.resolve("protocol-" + protocolCase.name()));
            Path catalogRoot = Files.createDirectories(root.resolve("catalog"));
            Path artifactRoot = Files.createDirectories(root.resolve("artifacts"));
            String scenarioName = "protocol-" + protocolCase.name();
            writePair(catalogRoot, scenarioName, "connector.jar", "job.jar", "");
            createJar(artifactRoot.resolve("connector.jar"), false);
            createRawWorkloadJar(artifactRoot.resolve("job.jar"), protocolCase.markerLines());

            Invocation result = execute(
                    "validate", "--catalog-root", catalogRoot.toString(),
                    "--scenario", scenarioName,
                    "--artifact-root", artifactRoot.toString(), "--offline");

            assertValidationFailure(
                    result,
                    scenarioName + ".yaml [single] artifact.workload.protocol-"
                            + protocolCase.diagnosticSuffix() + " at " + JOB_POINTER);
            assertNoPreparedWorkspace(artifactRoot);
        }
    }

    @Test
    void suitePreparationRejectsAProtocolInvalidWorkloadInAnyEntry() throws IOException {
        Path catalogRoot = Files.createDirectories(temporaryDirectory.resolve("catalog"));
        Path artifactRoot = Files.createDirectories(temporaryDirectory.resolve("artifacts"));
        writePair(catalogRoot, "valid", "valid-connector.jar", "valid-job.jar", "");
        writePair(catalogRoot, "invalid", "invalid-connector.jar", "invalid-job.jar", "");
        createJar(artifactRoot.resolve("valid-connector.jar"), false);
        createJar(artifactRoot.resolve("valid-job.jar"), true);
        createJar(artifactRoot.resolve("invalid-connector.jar"), false);
        createRawWorkloadJar(artifactRoot.resolve("invalid-job.jar"), List.of());
        writeSuite(catalogRoot, "protocol-suite", List.of("valid", "invalid"));

        Invocation result = execute(
                "validate", "--catalog-root", catalogRoot.toString(),
                "--suite", "protocol-suite",
                "--artifact-root", artifactRoot.toString(), "--offline");

        assertValidationFailure(
                result,
                "invalid.yaml [entry 1 'invalid', single] "
                        + "artifact.workload.protocol-missing at " + JOB_POINTER);
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
        CommandLine commandLine = new CommandLine(new FlinkStabilityCommand())
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

    static void writePair(
            Path root,
            String name,
            String connectorReference,
            String jobReference,
            String parameterBlock) throws IOException {
        writeScenario(root, name, connectorReference, jobReference, parameterBlock);
        ObjectNode expected = readFixture("scenario-v1.expected.yaml");
        ObjectNode metadata = (ObjectNode) expected.required("meta");
        metadata.put("name", name + ".expected");
        metadata.put("scenario", name);
        Files.writeString(root.resolve(name + ".expected.yaml"), YAML.writeValueAsString(expected));
    }

    private static void writeScenario(
            Path root,
            String name,
            String connectorReference,
            String jobReference,
            String parameterBlock) throws IOException {
        ObjectNode scenario = readFixture("scenario-v1.yaml");
        scenario.put("health_retry_limit", 0);
        ((ObjectNode) scenario.required("meta")).put("name", name);
        ObjectNode connector = (ObjectNode) scenario.requiredAt("/subject/connectors/kafka");
        connector.put("artifact", connectorReference);
        if (connectorReference.startsWith("maven:")) {
            connector.remove("runtime_dependencies");
        }
        ((ObjectNode) scenario.requiredAt("/workload/jobs/0")).put("jar", jobReference);
        if (!parameterBlock.isBlank()) {
            scenario.set("parameters", YAML.readTree(parameterBlock).required("parameters"));
        }
        Files.writeString(root.resolve(name + ".yaml"), YAML.writeValueAsString(scenario));
    }

    private static void writeSuite(Path root, String name, List<String> scenarios)
            throws IOException {
        ObjectNode suite = readFixture("suite-v1.yaml");
        ((ObjectNode) suite.required("meta")).put("name", name);
        ArrayNode entries = ((ArrayNode) suite.required("scenarios")).removeAll();
        scenarios.forEach(scenario -> entries.addObject().put("scenario", scenario));
        Files.writeString(root.resolve(name + ".yaml"), YAML.writeValueAsString(suite));
    }

    private static ObjectNode readFixture(String name) throws IOException {
        String resource = "/spec/valid/" + name;
        try (InputStream input = ValidateSpecificationsCommandTest.class.getResourceAsStream(resource)) {
            assertNotNull(input, "Missing YAML fixture: " + resource);
            return YAML.readValue(input, ObjectNode.class);
        }
    }

    private static void updateDocument(Path path, Consumer<ObjectNode> change) throws IOException {
        ObjectNode document = YAML.readValue(path.toFile(), ObjectNode.class);
        change.accept(document);
        Files.writeString(path, YAML.writeValueAsString(document));
    }

    static Path createJar(Path path, boolean executable) throws IOException {
        Files.createDirectories(path.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (executable) {
            manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
            manifest.getMainAttributes().putValue(
                    "Flink-Stability-Workload-Protocol", "v1");
        }
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            if (!executable) {
                for (String entryClass : org.savonitar.flink.stability.core.execution.plan
                        .ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES) {
                    output.putNextEntry(new JarEntry(entryClass.replace('.', '/') + ".class"));
                    output.write(new byte[] {0});
                    output.closeEntry();
                }
            }
            output.putNextEntry(new JarEntry("payload.txt"));
            output.write("payload".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return path;
    }

    private static Path createRawWorkloadJar(Path path, List<String> markerLines)
            throws IOException {
        Files.createDirectories(path.getParent());
        List<String> manifestLines = new java.util.ArrayList<>();
        manifestLines.add("Manifest-Version: 1.0");
        manifestLines.add("Main-Class: example.Main");
        manifestLines.addAll(markerLines);
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(path))) {
            output.putNextEntry(new JarEntry("META-INF/MANIFEST.MF"));
            output.write((String.join("\r\n", manifestLines) + "\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
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

    private record ProtocolCase(
            String name,
            List<String> markerLines,
            String diagnosticSuffix) {}
}
