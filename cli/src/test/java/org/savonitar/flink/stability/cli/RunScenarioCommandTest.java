package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.execution.kafka.KafkaTransactionVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.execution.FlinkRuntimeIdentity;
import org.savonitar.flink.stability.core.execution.FlinkTerminalWriteFence;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.core.execution.SubjectClassOrigins;
import org.savonitar.flink.stability.core.execution.V1AttemptContext;
import org.savonitar.flink.stability.core.execution.V1ScenarioExecutionResult;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.validation.kafka.KafkaIdSetValidationResult;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunScenarioCommandTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path temporaryDirectory;

    private ExecutableScenarioPlan.ExpectedOutcome expectation =
            ExecutableScenarioPlan.ExpectedOutcome.pass();

    @Test
    void registeredCommandHelpRequiresOnlyOneScenarioTarget() {
        Invocation result = executeRoot("run", "--help");
        Invocation removedAlias = executeRoot("run-v1");
        Invocation suite = executeRoot(
                "run",
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "one",
                "--suite", "not-supported");

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.OK, result.exitCode()),
                () -> assertTrue(result.stdout().contains("--catalog-root")),
                () -> assertTrue(result.stdout().contains("--scenario")),
                () -> assertFalse(result.stdout().contains("--suite")),
                () -> assertEquals("", result.stderr()),
                () -> assertEquals(CommandLine.ExitCode.USAGE, removedAlias.exitCode()),
                () -> assertTrue(removedAlias.stderr().contains("run-v1")),
                () -> assertEquals(CommandLine.ExitCode.USAGE, suite.exitCode()),
                () -> assertTrue(suite.stderr().contains("--suite")));
    }

    @Test
    void operationalLoggingCannotContaminateMachineReadableStdout() throws IOException {
        Properties logging = new Properties();
        try (InputStream input = getClass().getResourceAsStream("/log4j2.properties")) {
            assertTrue(input != null, "CLI log4j2.properties must be packaged");
            logging.load(input);
        }

        assertEquals("SYSTEM_ERR", logging.getProperty("appender.console.target"));
    }

    @Test
    void preparesThenExecutesExactlyOnceClosesAndRendersPassEvidence() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicInteger executions = new AtomicInteger();
        AtomicReference<Path> catalog = new AtomicReference<>();
        AtomicReference<String> selectedScenario = new AtomicReference<>();
        AtomicReference<Map<String, ? extends JsonNode>> parameters = new AtomicReference<>();
        AtomicReference<Path> artifactRoot = new AtomicReference<>();
        AtomicReference<Boolean> offline = new AtomicReference<>();
        V1AttemptContext context = new V1AttemptContext(
                1,
                "abc123de",
                temporaryDirectory.resolve("checkpoints/attempt-1-abc123de"));

        RunScenarioCommand command = new RunScenarioCommand(
                (root, name, overrides, options) -> {
                    events.add("prepare");
                    catalog.set(root);
                    selectedScenario.set(name);
                    parameters.set(overrides);
                    artifactRoot.set(options.artifactRoot());
                    offline.set(options.offline());
                    return prepared(events, executions, passResult());
                },
                () -> {
                    events.add("context");
                    return context;
                },
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());

        Path catalogRoot = temporaryDirectory.resolve("catalog");
        Path artifacts = temporaryDirectory.resolve("artifacts");
        Invocation invocation = execute(command,
                "--catalog-root", catalogRoot.toString(),
                "--scenario", "bounded-eos",
                "--artifact-root", artifacts.toString(),
                "--offline",
                "-p", "enabled=true",
                "-p", "count=12",
                "-p", "label=plain");

        JsonNode output = JSON.readTree(invocation.stdout());
        assertAll(
                () -> assertEquals(CommandLine.ExitCode.OK, invocation.exitCode()),
                () -> assertEquals("", invocation.stderr()),
                () -> assertEquals(List.of("prepare", "context", "execute", "close"), events),
                () -> assertEquals(1, executions.get()),
                () -> assertEquals(catalogRoot, catalog.get()),
                () -> assertEquals("bounded-eos", selectedScenario.get()),
                () -> assertTrue(parameters.get().get("enabled").booleanValue()),
                () -> assertEquals(12, parameters.get().get("count").intValue()),
                () -> assertEquals("plain", parameters.get().get("label").textValue()),
                () -> assertEquals(artifacts.toAbsolutePath().normalize(), artifactRoot.get()),
                () -> assertTrue(offline.get()),
                () -> assertEquals("bounded-eos", output.path("scenario").textValue()),
                () -> assertEquals("pass", output.path("status").textValue()),
                () -> assertEquals(1, output.path("attempt").path("ordinal").intValue()),
                () -> assertEquals("abc123de",
                        output.path("attempt").path("nonce").textValue()),
                () -> assertEquals(context.checkpointStorageRoot().toString(),
                        output.path("attempt").path("checkpointRoot").textValue()),
                () -> assertEquals("pass",
                        output.path("evidence").path("terminalValidation")
                                .path("status").textValue()),
                () -> assertEquals(2,
                        output.path("evidence").path("processFence")
                                .path("processes").intValue()),
                () -> assertEquals(0, output.path("diagnostics").size()));
    }

    @Test
    void failAndInconclusiveOutcomesReturnSoftwareExitAndRemainStructured()
            throws Exception {
        V1AttemptContext context = context("1234abcd");
        for (V1ScenarioExecutionResult result : List.of(
                result(V1ScenarioExecutionResult.Status.FAIL,
                        "verification.kafka.missing-records"),
                result(V1ScenarioExecutionResult.Status.INCONCLUSIVE,
                        "infrastructure.kafka-start-failed"))) {
            AtomicInteger executions = new AtomicInteger();
            List<String> events = new ArrayList<>();
            RunScenarioCommand command = new RunScenarioCommand(
                    (root, name, overrides, options) ->
                            prepared(events, executions, result),
                    () -> context,
                    new V1ExecutionResultRenderer(),
                    new ValidationDiagnosticRenderer());

            Invocation invocation = execute(command,
                    "--catalog-root", temporaryDirectory.toString(),
                    "--scenario", "bounded-eos");
            JsonNode output = JSON.readTree(invocation.stdout());

            assertAll(
                    () -> assertEquals(CommandLine.ExitCode.SOFTWARE,
                            invocation.exitCode()),
                    () -> assertEquals("", invocation.stderr()),
                    () -> assertEquals(result.status().name().toLowerCase(Locale.ROOT),
                            output.path("status").textValue()),
                    () -> assertEquals(result.reason(),
                            output.path("reason").textValue()),
                    () -> assertEquals("not-started",
                            output.at("/evidence/input/status").textValue()),
                    () -> assertFalse(
                            output.at("/evidence/input/complete").booleanValue()),
                    () -> assertEquals("not-run",
                            output.at("/evidence/writeFence/status").textValue()),
                    () -> assertFalse(
                            output.at("/evidence/writeFence/completed").booleanValue()),
                    () -> assertEquals("not-run",
                            output.at("/evidence/processFence/status").textValue()),
                    () -> assertFalse(
                            output.at("/evidence/processFence/completed").booleanValue()),
                    () -> assertEquals("not-run",
                            output.at("/evidence/terminalValidation/status").textValue()),
                    () -> assertFalse(output.at(
                            "/evidence/terminalValidation/snapshotComplete").booleanValue()),
                    () -> assertEquals(1, executions.get()),
                    () -> assertEquals(List.of("execute", "close"), events));
        }
    }

    @Test
    void rendersObservedLocalImageIdsAndTheDeclaredExpectationSeparately() throws Exception {
        JsonNode evidence = JSON.readTree(new V1ExecutionResultRenderer().render(
                "bounded-eos", context("1234abcd"), expectation, passResult())).path("evidence");
        JsonNode runtime = evidence.path("flinkRuntime");

        assertAll(
                () -> assertEquals(2, evidence.path("flinkComponents").intValue()),
                () -> assertEquals("confirmed", runtime.path("status").textValue()),
                () -> assertEquals("docker-image-id", runtime.path("identityKind").textValue()),
                () -> assertEquals(IMAGE_ID, runtime.path("expectedImageId").textValue()),
                () -> assertEquals(2, runtime.path("components").size()),
                () -> assertEquals("jm", runtime.at("/components/0/runtimeId").textValue()),
                () -> assertEquals("job_manager", runtime.at("/components/0/role").textValue()),
                () -> assertEquals("flink:2.2.0",
                        runtime.at("/components/0/imageReference").textValue()),
                () -> assertEquals(IMAGE_ID, runtime.at("/components/0/imageId").textValue()),
                () -> assertEquals("taskmanager-1",
                        runtime.at("/components/1/logicalName").textValue()),
                () -> assertEquals("tm", runtime.at("/components/1/runtimeId").textValue()),
                () -> assertEquals(IMAGE_ID, runtime.at("/components/1/imageId").textValue()),
                () -> assertEquals("0".repeat(64),
                        runtime.at("/components/1/targetBindingSha256").textValue()),
                () -> assertEquals(1, runtime.path("connectorArtifactSets").size()),
                () -> assertTrue(runtime.at("/connectorArtifactSets/0").isEmpty()),
                () -> assertEquals(0, runtime.at("/components/0/connectorArtifactsRef").intValue()),
                () -> assertEquals(0, runtime.at("/components/1/connectorArtifactsRef").intValue()),
                () -> assertEquals("not-requested", runtime.at("/runtimeJar/status").textValue()),
                () -> assertFalse(runtime.has("registryDigest")));
    }

    @Test
    void reportsRuntimeJarBytesAndClassLoadsWithTheirPhysicalContainerAssociation() throws Exception {
        var jar = new FlinkRuntimeTarget.RuntimeJar(
                "/opt/flink/lib/flink-dist-2.2.0.jar", "c".repeat(64));
        SubjectClassOrigins origins = new SubjectClassOrigins(jar.containerPath(), List.of(
                new SubjectClassOrigins.ProcessOrigin("jobmanager-1#1", Map.of(
                        "org.apache.flink.runtime.resourcemanager.ResourceManager", List.of(jar.containerPath()))),
                new SubjectClassOrigins.ProcessOrigin("taskmanager-1#1", Map.of(
                        "org.apache.flink.runtime.taskexecutor.TaskExecutor", List.of(jar.containerPath())))),
                Optional.empty());
        V1ScenarioExecutionResult base = passResult();
        for (boolean observed : List.of(true, false)) {
            V1ScenarioExecutionResult result = new V1ScenarioExecutionResult(
                    observed ? V1ScenarioExecutionResult.Status.PASS : V1ScenarioExecutionResult.Status.INCONCLUSIVE,
                    observed ? base.reason() : "subject.flink.runtime-jar-unconfirmed", base.message(),
                    base.inputManifest(), base.phaseEvidence(), base.writeFenceEvidence(),
                    base.processFenceEvidence(), base.finalJobObservation(), base.terminalValidation(),
                    base.sinkTransactions(), base.subjectClassOrigins(),
                    base.flinkProvisioningEvidence().stream().map(component ->
                            component.withRuntimeJarEvidence(jar, component.logicalName() + "#1")).toList(),
                    new FlinkRuntimeIdentity.ExpectedTarget(EXPECTED_RUNTIME.imageId(),
                            EXPECTED_RUNTIME.components(), Optional.of(jar)),
                    observed ? Optional.of(origins) : Optional.empty(),
                    KafkaTransactionVersion.Selection.notRequested(), List.of());
            JsonNode runtime = JSON.readTree(new V1ExecutionResultRenderer().render(
                    "bounded-eos", context("1234abcd"), expectation, result)).at("/evidence/flinkRuntime");
            assertEquals(observed ? "confirmed" : "unconfirmed", runtime.at("/runtimeJar/status").textValue());
            assertEquals(jar.sha256(), runtime.at("/runtimeJar/expected/sha256").textValue());
            assertEquals(jar.containerPath(), runtime.at("/runtimeJar/expected/containerPath").textValue());
            assertEquals("jm", runtime.at("/components/0/runtimeId").textValue());
            assertEquals("jobmanager-1#1", runtime.at("/components/0/runtimeJar/classLoadProcess").textValue());
            assertEquals(jar.sha256(), runtime.at("/components/0/runtimeJar/sha256").textValue());
            assertEquals("taskmanager-1#1", runtime.at("/components/1/runtimeJar/classLoadProcess").textValue());
            if (observed) {
                assertEquals(2, runtime.at("/runtimeJar/classes").size());
                assertEquals(jar.containerPath(), runtime.at("/runtimeJar/classes/1/sources")
                        .path("org.apache.flink.runtime.taskexecutor.TaskExecutor").get(0).textValue());
            } else {
                assertFalse(runtime.path("runtimeJar").has("classes"));
            }
        }
    }

    @Test
    void sharesIdenticalArtifactListsWithoutHidingDifferentObservedEntries() throws Exception {
        List<ProvisionedConnectorArtifact> artifacts = List.of(new ProvisionedConnectorArtifact(
                0, "/opt/flink/lib/flink-stability-connector-00000000-" + "a".repeat(64) + ".jar", "a".repeat(64)));
        List<ProvisionedConnectorArtifact> changed = List.of(new ProvisionedConnectorArtifact(
                0, "/opt/flink/lib/flink-stability-connector-00000000-" + "b".repeat(64) + ".jar", "b".repeat(64)));
        List<FlinkComponentProvisioningEvidence> components = new ArrayList<>();
        for (int incarnation = 0; incarnation < 4; incarnation++) {
            components.add(FlinkComponentProvisioningEvidence.verified(
                    incarnation == 0 ? "jobmanager-1" : "taskmanager-1",
                    incarnation == 0 ? FlinkComponentRole.JOB_MANAGER : FlinkComponentRole.TASK_MANAGER,
                    "container-" + incarnation, "flink:2.2.0", IMAGE_ID,
                    String.valueOf(incarnation).repeat(64), "1".repeat(64),
                    incarnation == 3 ? changed : artifacts));
        }
        JsonNode runtime = JSON.readTree(new V1ExecutionResultRenderer().render(
                "bounded-eos", context("1234abcd"), expectation,
                result(V1ScenarioExecutionResult.Status.INCONCLUSIVE, "test.partial", components)))
                .at("/evidence/flinkRuntime");

        assertEquals(2, runtime.path("connectorArtifactSets").size());
        for (int incarnation = 0; incarnation < 4; incarnation++) {
            JsonNode rendered = runtime.path("components").get(incarnation);
            assertEquals("container-" + incarnation, rendered.path("runtimeId").textValue());
            assertEquals(String.valueOf(incarnation).repeat(64),
                    rendered.path("targetBindingSha256").textValue());
            assertEquals("1".repeat(64), rendered.path("classpathManifestSha256").textValue());
            assertEquals(incarnation == 3 ? 1 : 0, rendered.path("connectorArtifactsRef").intValue());
            assertFalse(rendered.has("connectorArtifacts"));
            JsonNode entries = runtime.path("connectorArtifactSets")
                    .get(rendered.path("connectorArtifactsRef").intValue());
            assertEquals(1, entries.size());
            assertEquals(0, entries.get(0).path("index").intValue());
            ProvisionedConnectorArtifact observed = components.get(incarnation).connectorArtifacts().getFirst();
            assertEquals(observed.containerPath(), entries.get(0).path("containerPath").textValue());
            assertEquals(observed.sha256(), entries.get(0).path("sha256").textValue());
        }
    }

    @Test
    void featureSelectionKeepsRequestedAndObservedLevelsAndOriginalError() throws Exception {
        var observation = new KafkaTransactionVersion.Observation(
                Optional.of(new KafkaTransactionVersion.Range((short) 2, (short) 2)),
                Optional.of(new KafkaTransactionVersion.Range((short) 0, (short) 2)),
                java.util.OptionalLong.of(5));
        var selection = new KafkaTransactionVersion.Selection(Optional.of(1), List.of(observation),
                Optional.of("UnsupportedVersionException: safe downgrade rejected"));
        var result = new V1ScenarioExecutionResult(V1ScenarioExecutionResult.Status.INCONCLUSIVE,
                KafkaTransactionVersion.UNCONFIRMED, "feature selection failed",
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), List.of(), EXPECTED_RUNTIME,
                Optional.empty(), selection, List.of());
        JsonNode output = JSON.readTree(new V1ExecutionResultRenderer().render(
                "bounded-eos", context("1234abcd"), expectation, result));
        JsonNode evidence = output.at("/evidence/kafkaTransactionVersion");
        assertEquals("unconfirmed", evidence.path("status").textValue());
        assertEquals(1, evidence.path("requested").intValue());
        assertEquals(2, evidence.at("/observations/0/finalized/max").intValue());
        assertEquals(0, evidence.at("/observations/0/supported/min").intValue());
        assertEquals(5, evidence.at("/observations/0/metadataEpoch").longValue());
        assertEquals(selection.error().orElseThrow(), evidence.path("error").textValue());
    }

    @Test
    void partialEvidenceIsExplicitAndDoesNotPublishProvisionalDefectTotals()
            throws Exception {
        FlinkProcessWriteFenceEvidence processFence = runtimeFence();
        KafkaIdSetValidationResult terminal = new KafkaIdSetValidationResult(
                KafkaIdSetValidationResult.Status.FAIL,
                "verification.kafka.incomplete-after-timeout",
                "Kafka snapshot was incomplete",
                new KafkaIdSetValidationResult.Evidence(
                        2,
                        1,
                        Optional.empty(),
                        List.of(new KafkaIdSetValidationResult.RecordSample(0, 0, "0")),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        Map.of(0, 0L),
                        Map.of(0, 2L),
                        false));
        V1ScenarioExecutionResult result = new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.FAIL,
                terminal.reason(),
                terminal.message(),
                Optional.of(partialInputManifest()),
                Optional.empty(),
                Optional.empty(),
                Optional.of(processFence),
                Optional.empty(),
                Optional.of(terminal),
                Optional.empty(),
                SUBJECT_ORIGINS,
                runtimeComponents(),
                EXPECTED_RUNTIME,
                Optional.empty(),
                KafkaTransactionVersion.Selection.notRequested(),
                List.of());

        JsonNode output = JSON.readTree(new V1ExecutionResultRenderer().render(
                "bounded-eos", context("1234abcd"), expectation, result));
        JsonNode input = output.path("evidence").path("input");
        JsonNode validation = output.path("evidence").path("terminalValidation");

        assertAll(
                () -> assertEquals("not-run",
                        output.at("/evidence/flinkJob/status").textValue()),
                () -> assertTrue(output.at("/evidence/taskManagerKills").isArray()),
                () -> assertTrue(output.at("/evidence/taskManagerKills").isEmpty()),
                () -> assertTrue(output.at("/evidence/taskManagerRestarts").isArray()),
                () -> assertTrue(output.at("/evidence/taskManagerRestarts").isEmpty()),
                () -> assertEquals("partial", input.path("status").textValue()),
                () -> assertEquals(1, input.path("observed").longValue()),
                () -> assertFalse(input.path("reconciliationComplete").booleanValue()),
                () -> assertFalse(validation.path("snapshotComplete").booleanValue()),
                () -> assertFalse(validation.has("distinctExpected")),
                () -> assertFalse(validation.has("malformed")),
                () -> assertFalse(validation.has("unexpected")),
                () -> assertFalse(validation.has("duplicates")),
                () -> assertFalse(validation.has("missing")));
    }

    @Test
    void rendersPeerAttributionWithoutReplacingTheReportingTaskManagerIdentity() throws Exception {
        String type = "org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException";
        var peerFailure = new FlinkJobObservation.Failure(1_300, type,
                type + ": Connection unexpectedly closed by remote task manager '172.20.0.5:36917 [ tm-2-old ] '. "
                        + "This might indicate that the remote task manager was lost.", Optional.of("tm-1"));
        var before = new FlinkJobObservation(1_000, FlinkJobState.RUNNING, 2, 0,
                Optional.empty(), List.of(), List.of(
                        new FlinkJobObservation.Subtask("Source", 0, 0, "RUNNING", Optional.of("tm-2-old")),
                        new FlinkJobObservation.Subtask("Sink", 1, 0, "RUNNING", Optional.of("tm-1"))));
        var kill = new PhaseExecutionEvidence.TaskManagerKill(
                "$/phases/1/steps/0", List.of(), "taskmanager-2",
                new FlinkJobObservation.Attempt(Optional.of(before), Optional.empty()),
                java.util.OptionalLong.of(1_200), java.util.OptionalLong.of(1_500),
                Optional.of(new TaskManagerControl.Identity("taskmanager-2", "container-2", "tm-2-old")));
        var after = new FlinkJobObservation(9_000, FlinkJobState.FINISHED, 3, 1,
                Optional.of(new FlinkJobObservation.Restore(2, 1_400)), List.of(peerFailure), List.of());
        JsonNode rendered = renderEvidence(new PhaseExecutionEvidence(List.of(), List.of(kill), List.of()),
                Optional.of(new FlinkJobObservation.Attempt(Optional.of(after), Optional.empty())))
                .at("/taskManagerKills/0");

        assertTrue(rendered.path("confirmed").booleanValue());
        assertEquals(1_200, rendered.path("jobManagerTimeBeforeKill").longValue());
        assertEquals(1_500, rendered.path("jobManagerTimeAfterKill").longValue());
        assertEquals(1_400, rendered.path("restoredAtMillis").longValue());
        assertEquals(200, rendered.path("restoredAfterPreInjectionMs").longValue());
        assertEquals(-100, rendered.path("restoredAfterKillObservationMs").longValue());
        assertEquals(1, rendered.path("qualifyingTargetFailures").size());
        assertEquals("tm-1", rendered.at("/qualifyingTargetFailures/0/taskManagerId").textValue());
        assertEquals("tm-2-old", rendered.at("/qualifyingTargetFailures/0/targetResourceId").textValue());
        assertEquals("remote-transport", rendered.at("/qualifyingTargetFailures/0/attribution").textValue());
        assertEquals(peerFailure.rootCause(), rendered.at("/qualifyingTargetFailures/0/rootCause").textValue());
    }

    @Test
    void rendersTheJobObservationAndTheEffectOfEachTaskManagerKill() throws Exception {
        FlinkJobObservation.Failure lostTaskManager = new FlinkJobObservation.Failure(
                12_000, "ResourceManagerException",
                "TaskManager with id tm-1 is no longer reachable.", Optional.of("tm-1"));
        FlinkJobObservation.Failure otherTaskManager = new FlinkJobObservation.Failure(
                14_000, "OtherTaskManagerException",
                "Unrelated failure on tm-other.", Optional.of("tm-other"));
        PhaseExecutionEvidence phases = new PhaseExecutionEvidence(
                List.of(),
                List.of(new PhaseExecutionEvidence.TaskManagerKill(
                        "$/phases/1/steps/0",
                        List.of(),
                        "taskmanager-1",
                        new FlinkJobObservation.Attempt(
                                Optional.of(new FlinkJobObservation(
                                        1_000, FlinkJobState.RUNNING, 4, 0, Optional.empty(),
                                        List.of(),
                                        List.of(new FlinkJobObservation.Subtask(
                                                "Kafka Source", 0, 0, "RUNNING",
                                                Optional.of("tm-1")),
                                                new FlinkJobObservation.Subtask(
                                                        "Kafka Source", 1, 2, "RUNNING",
                                                        Optional.of("tm-other")),
                                                new FlinkJobObservation.Subtask(
                                                        "Kafka Sink", 0, 1, "DEPLOYING",
                                                        Optional.of("tm-1"))))),
                                Optional.empty()), java.util.OptionalLong.of(1_500),
                        java.util.OptionalLong.of(2_000),
                        Optional.of(new TaskManagerControl.Identity(
                                "taskmanager-1", "tm", "tm-1")))),
                List.of(new PhaseExecutionEvidence.NetworkFault(
                        "$/phases/2/steps/0",
                        "phases-2-steps-0",
                        "kafka-proxy",
                        "quay.io/kroxylicious/kroxylicious:0.21.0",
                        ExecutableScenarioPlan.NetworkFaultAction.DROP_RESPONSE,
                        1,
                        java.time.Duration.ofMinutes(2),
                        15_000,
                        15_700,
                        List.of(new PhaseExecutionEvidence.DroppedMessage(
                                1, 1, 15_500, true, "eos-0-2", 2, (short) 0, true,
                                Optional.of(new PhaseExecutionEvidence.BrokerAnswer(
                                        "NONE", 2, (short) 1)),
                                Optional.of(new PhaseExecutionEvidence.Retry(
                                        45_600, "producer-eos-0-2")))),
                        List.of())));
        FlinkJobObservation.Attempt atFence = new FlinkJobObservation.Attempt(
                Optional.of(new FlinkJobObservation(
                        20_000, FlinkJobState.FINISHED, 20, 1,
                        Optional.of(new FlinkJobObservation.Restore(4, 13_000)),
                        List.of(otherTaskManager, lostTaskManager), List.of())),
                Optional.empty());
        FlinkProcessWriteFenceEvidence processes = runtimeFence();
        KafkaIdSetValidationResult terminal = passResult().terminalValidation().orElseThrow();
        V1ScenarioExecutionResult result = new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.PASS,
                terminal.reason(),
                terminal.message(),
                Optional.empty(),
                Optional.of(phases),
                Optional.of(new FlinkTerminalWriteFence.Evidence(
                        FlinkJobState.FINISHED, processes, atFence)),
                Optional.of(processes),
                Optional.of(atFence),
                Optional.of(terminal),
                Optional.empty(),
                SUBJECT_ORIGINS,
                runtimeComponents(),
                EXPECTED_RUNTIME,
                Optional.empty(),
                KafkaTransactionVersion.Selection.notRequested(),
                List.of());

        JsonNode evidence = JSON.readTree(new V1ExecutionResultRenderer().render(
                "bounded-eos", context("1234abcd"), expectation, result)).path("evidence");
        JsonNode job = evidence.path("flinkJob");
        JsonNode kill = evidence.path("taskManagerKills").path(0);
        JsonNode fault = evidence.path("networkFaults").path(0);

        assertAll(
                () -> assertEquals("observed", job.path("status").textValue()),
                () -> assertEquals("FINISHED", job.path("state").textValue()),
                () -> assertEquals(1, job.path("restoredCheckpoints").longValue()),
                () -> assertEquals(4, job.path("latestRestoredCheckpoint").longValue()),
                () -> assertEquals(2, job.path("failures").intValue()),
                () -> assertEquals("$/phases/1/steps/0", kill.path("path").textValue()),
                () -> assertEquals("taskmanager-1", kill.at("/identity/logicalName").textValue()),
                () -> assertEquals("tm", kill.at("/identity/runtimeId").textValue()),
                () -> assertEquals("tm-1", kill.at("/identity/resourceId").textValue()),
                () -> assertTrue(kill.path("identityFailure").isNull()),
                () -> assertEquals("checkpoint-restored", kill.path("outcome").textValue()),
                () -> assertTrue(kill.path("confirmed").booleanValue()),
                () -> assertEquals("RUNNING", kill.path("jobStateBeforeKill").textValue()),
                () -> assertEquals(4, kill.path("completedCheckpointsBeforeKill").longValue()),
                () -> assertEquals(3, kill.path("activeSubtasksBeforeKill").longValue()),
                () -> assertEquals(3, kill.path("subtasksBeforeKill").size()),
                () -> assertEquals("tm-other", kill.at("/subtasksBeforeKill/1/taskManagerId").textValue()),
                () -> assertEquals("DEPLOYING", kill.at("/subtasksBeforeKill/2/status").textValue()),
                () -> assertEquals(1, kill.path("targetedRunningSubtasksBeforeKill").size()),
                () -> assertEquals(JSON.valueToTree(Map.of(
                        "vertexName", "Kafka Source", "index", 0, "attempt", 0,
                        "status", "RUNNING", "taskManagerId", "tm-1")),
                        kill.at("/targetedRunningSubtasksBeforeKill/0")),
                () -> assertEquals(4, kill.path("restoredCheckpoint").longValue()),
                () -> assertEquals(1_500, kill.path("jobManagerTimeBeforeKill").longValue()),
                () -> assertEquals(2_000, kill.path("jobManagerTimeAfterKill").longValue()),
                () -> assertEquals(11_000, kill.path("restoredAfterKillObservationMs").longValue()),
                () -> assertEquals(2, kill.path("failuresAfterKill").intValue()),
                () -> assertEquals(1, kill.path("qualifyingTargetFailures").size()),
                () -> assertEquals(JSON.valueToTree(Map.of(
                        "taskManagerId", "tm-1", "targetResourceId", "tm-1", "attribution", "reporter",
                        "timestampMillis", 12_000,
                        "exceptionName", "ResourceManagerException",
                        "rootCause", lostTaskManager.rootCause())),
                        kill.at("/qualifyingTargetFailures/0")),
                () -> assertFalse(kill.path("qualifyingTargetFailures").toString()
                        .contains(otherTaskManager.rootCause())),
                () -> assertEquals("TaskManager with id tm-1 is no longer reachable.",
                        kill.path("firstFailureAfterKill").textValue()),
                () -> assertEquals("drop-response", fault.path("action").textValue()),
                () -> assertEquals("quay.io/kroxylicious/kroxylicious:0.21.0",
                        fault.path("proxyImage").textValue()),
                () -> assertTrue(fault.path("triggered").booleanValue()),
                () -> assertTrue(fault.at("/dropped/0/beforeDeadline").booleanValue()),
                () -> assertEquals(30_100, fault.at("/dropped/0/retryAfterMillis").longValue()),
                () -> assertEquals("producer-eos-0-2",
                        fault.at("/dropped/0/retryClientId").textValue()),
                () -> assertEquals("eos-0-2", fault.at("/dropped/0/transactionalId").textValue()),
                () -> assertEquals("NONE", fault.at("/dropped/0/brokerError").textValue()),
                () -> assertEquals(1, fault.at("/dropped/0/brokerProducerEpoch").intValue()),
                () -> assertEquals("confirmed",
                        evidence.at("/subjectClasses/status").textValue()),
                () -> assertEquals("taskmanager-1#1",
                        evidence.at("/subjectClasses/processes/0/process").textValue()));
    }

    @Test
    void rendersDistinctKillAndReplacementIdentitiesForTwoNamedTaskManagers() throws Exception {
        List<PhaseExecutionEvidence.StepEvidence> steps = new ArrayList<>();
        List<PhaseExecutionEvidence.TaskManagerKill> kills = new ArrayList<>();
        List<PhaseExecutionEvidence.TaskManagerRestart> restarts = new ArrayList<>();
        FlinkJobObservation.Attempt unavailable = new FlinkJobObservation.Attempt(
                Optional.empty(), Optional.of("job observation unavailable"));
        for (int slot = 1; slot <= 2; slot++) {
            String target = "taskmanager-" + slot;
            String killPath = "$/phases/1/steps/" + ((slot - 1) * 2);
            String restartPath = "$/phases/1/steps/" + ((slot - 1) * 2 + 1);
            var previous = new TaskManagerControl.Identity(
                    target, "container-" + slot + "-old", "resource-" + slot + "-old");
            var replacement = new TaskManagerControl.Identity(
                    target, "container-" + slot + "-new", "resource-" + slot + "-new");
            kills.add(new PhaseExecutionEvidence.TaskManagerKill(
                    killPath, List.of(), target, unavailable,
                    java.util.OptionalLong.empty(), java.util.OptionalLong.empty(),
                    Optional.of(previous)));
            restarts.add(new PhaseExecutionEvidence.TaskManagerRestart(
                    restartPath, List.of(), target, Optional.of(previous), Optional.of(replacement)));
            steps.add(new PhaseExecutionEvidence.StepEvidence(
                    1, "recovery", restartPath, List.of(),
                    PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER,
                    PhaseExecutionEvidence.StepStatus.SUCCEEDED, "replacement started"));
        }
        JsonNode evidence = renderIncompletePhases(
                new PhaseExecutionEvidence(steps, kills, List.of(), restarts));

        assertEquals(2, evidence.path("taskManagerKills").size());
        assertEquals(2, evidence.path("taskManagerRestarts").size());
        for (int slot = 1; slot <= 2; slot++) {
            JsonNode kill = evidence.path("taskManagerKills").get(slot - 1);
            JsonNode restart = evidence.path("taskManagerRestarts").get(slot - 1);
            JsonNode previous = JSON.valueToTree(Map.of(
                    "logicalName", "taskmanager-" + slot,
                    "runtimeId", "container-" + slot + "-old",
                    "resourceId", "resource-" + slot + "-old"));
            JsonNode replacement = JSON.valueToTree(Map.of(
                    "logicalName", "taskmanager-" + slot,
                    "runtimeId", "container-" + slot + "-new",
                    "resourceId", "resource-" + slot + "-new"));
            assertEquals("taskmanager-" + slot, kill.path("target").textValue());
            assertEquals("$/phases/1/steps/" + ((slot - 1) * 2), kill.path("path").textValue());
            assertEquals(previous, kill.path("identity"));
            assertTrue(kill.path("identityFailure").isNull());
            assertFalse(kill.path("confirmed").booleanValue());
            assertEquals("taskmanager-" + slot, restart.path("target").textValue());
            assertEquals("$/phases/1/steps/" + ((slot - 1) * 2 + 1), restart.path("path").textValue());
            assertEquals(previous, restart.path("previousIdentity"));
            assertEquals(replacement, restart.path("replacementIdentity"));
            assertTrue(restart.path("previousIdentityFailure").isNull());
            assertTrue(restart.path("replacementIdentityFailure").isNull());
            assertTrue(restart.path("loopIterations").isArray());
            assertTrue(restart.path("loopIterations").isEmpty());
        }
    }

    @Test
    void keepsMissingKillAndRestartIdentitiesExplicitWithoutLosingLoopCoordinates() throws Exception {
        List<PhaseExecutionEvidence.LoopIteration> iterations = List.of(
                new PhaseExecutionEvidence.LoopIteration("$/phases/1/steps/0", 2, 3));
        var kill = new PhaseExecutionEvidence.TaskManagerKill(
                "$/phases/1/steps/0/loop/steps/0", iterations, "taskmanager-2",
                new FlinkJobObservation.Attempt(
                        Optional.empty(), Optional.of("job observation unavailable")),
                java.util.OptionalLong.empty(), java.util.OptionalLong.empty(), Optional.empty(),
                Optional.of("IllegalStateException: original identity lookup failed"));
        var restart = new PhaseExecutionEvidence.TaskManagerRestart(
                "$/phases/1/steps/0/loop/steps/1", iterations, "taskmanager-2",
                Optional.empty(), Optional.empty(),
                Optional.of("IllegalStateException: previous identity unavailable"),
                Optional.of("IOException: replacement identity lookup failed"));
        JsonNode evidence = renderIncompletePhases(new PhaseExecutionEvidence(
                List.of(), List.of(kill), List.of(), List.of(restart)));
        JsonNode renderedKill = evidence.at("/taskManagerKills/0");
        JsonNode renderedRestart = evidence.at("/taskManagerRestarts/0");

        assertAll(
                () -> assertEquals("taskmanager-2", renderedKill.path("target").textValue()),
                () -> assertTrue(renderedKill.path("identity").isNull()),
                () -> assertEquals("IllegalStateException: original identity lookup failed",
                        renderedKill.path("identityFailure").textValue()),
                () -> assertTrue(renderedKill.path("subtasksBeforeKill").isEmpty()),
                () -> assertTrue(renderedKill.path("targetedRunningSubtasksBeforeKill").isEmpty()),
                () -> assertTrue(renderedKill.path("qualifyingTargetFailures").isEmpty()),
                () -> assertFalse(renderedKill.path("confirmed").booleanValue()),
                () -> assertEquals(kill.path(), renderedKill.path("path").textValue()),
                () -> assertEquals("2/3", renderedKill.at("/loopIterations/0").textValue()),
                () -> assertEquals("taskmanager-2", renderedRestart.path("target").textValue()),
                () -> assertEquals(restart.path(), renderedRestart.path("path").textValue()),
                () -> assertEquals("2/3", renderedRestart.at("/loopIterations/0").textValue()),
                () -> assertTrue(renderedRestart.path("previousIdentity").isNull()),
                () -> assertTrue(renderedRestart.path("replacementIdentity").isNull()),
                () -> assertEquals("IllegalStateException: previous identity unavailable",
                        renderedRestart.path("previousIdentityFailure").textValue()),
                () -> assertEquals("IOException: replacement identity lookup failed",
                        renderedRestart.path("replacementIdentityFailure").textValue()));
    }

    @Test
    void aNegativeControlThatFailsAsPinnedExitsZeroWithItsAttemptReported() throws Exception {
        expectation = ExecutableScenarioPlan.ExpectedOutcome.failure(
                "kafka.id-set", "validator.kafka.id-set.duplicate-ids");
        List<String> events = new ArrayList<>();
        RunScenarioCommand command = new RunScenarioCommand(
                (root, name, overrides, options) ->
                        prepared(events, new AtomicInteger(), duplicateResult()),
                () -> context("1234abcd"),
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());

        Invocation invocation = execute(command,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "selftest-duplicates");
        JsonNode output = JSON.readTree(invocation.stdout());

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.OK, invocation.exitCode()),
                () -> assertEquals("pass", output.path("status").textValue()),
                () -> assertEquals("validator.kafka.id-set.duplicate-ids",
                        output.path("reason").textValue()),
                () -> assertEquals("fail", output.at("/attempt/status").textValue()),
                () -> assertEquals("validator.kafka.id-set.duplicate-ids",
                        output.at("/attempt/reason").textValue()),
                () -> assertEquals("fail", output.at("/expectation/outcome").textValue()),
                () -> assertEquals("kafka.id-set",
                        output.at("/expectation/oracle").textValue()),
                () -> assertTrue(output.at("/expectation/matched").booleanValue()),
                () -> assertEquals(3, output.at("/evidence/terminalValidation/duplicates")
                        .intValue()));
    }

    @Test
    void anExpectedFailureThatDoesNotOccurFailsTheRun() throws Exception {
        expectation = ExecutableScenarioPlan.ExpectedOutcome.failure(
                "kafka.id-set", "validator.kafka.id-set.duplicate-ids");
        RunScenarioCommand command = new RunScenarioCommand(
                (root, name, overrides, options) ->
                        prepared(new ArrayList<>(), new AtomicInteger(), passResult()),
                () -> context("1234abcd"),
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());

        Invocation invocation = execute(command,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "selftest-duplicates");
        JsonNode output = JSON.readTree(invocation.stdout());

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.SOFTWARE, invocation.exitCode()),
                () -> assertEquals("fail", output.path("status").textValue()),
                () -> assertEquals("expectation.mismatch", output.path("reason").textValue()),
                () -> assertEquals("pass", output.at("/attempt/status").textValue()),
                () -> assertFalse(output.at("/expectation/matched").booleanValue()));
    }

    @Test
    void syntaxAndUnknownScenarioUseUsageExitWithoutStartingAnAttempt() {
        AtomicInteger preparations = new AtomicInteger();
        AtomicInteger contexts = new AtomicInteger();
        RunScenarioCommand command = new RunScenarioCommand(
                (root, name, overrides, options) -> {
                    preparations.incrementAndGet();
                    throw new UnknownSpecificationTargetException(
                            "Catalog contains no scenario 'missing'");
                },
                () -> {
                    contexts.incrementAndGet();
                    return context("1234abcd");
                },
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());

        Invocation malformed = execute(command,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "missing", "-p", "not-an-assignment");
        RunScenarioCommand unknownCommand = new RunScenarioCommand(
                (root, name, overrides, options) -> {
                    preparations.incrementAndGet();
                    throw new UnknownSpecificationTargetException(
                            "Catalog contains no scenario 'missing'");
                },
                () -> {
                    contexts.incrementAndGet();
                    return context("1234abcd");
                },
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());
        Invocation unknown = execute(unknownCommand,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "missing");

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.USAGE, malformed.exitCode()),
                () -> assertTrue(malformed.stderr().contains("NAME=VALUE")),
                () -> assertEquals(CommandLine.ExitCode.USAGE, unknown.exitCode()),
                () -> assertTrue(unknown.stderr().contains("no scenario 'missing'")),
                () -> assertEquals(1, preparations.get()),
                () -> assertEquals(0, contexts.get()));
    }

    @Test
    void executionAndContextFailuresStillClosePreparedArtifacts() {
        List<String> executionEvents = new ArrayList<>();
        RunScenarioCommand executionFailure = new RunScenarioCommand(
                (root, name, overrides, options) -> new RunScenarioCommand.PreparedExecution() {
                    @Override
                    public ExecutableScenarioPlan.ExpectedOutcome expectedOutcome() {
                        return expectation;
                    }

                    @Override
                    public V1ScenarioExecutionResult execute(V1AttemptContext context) {
                        executionEvents.add("execute");
                        throw new IllegalStateException("executor exploded");
                    }

                    @Override
                    public void close() {
                        executionEvents.add("close");
                    }
                },
                () -> context("1234abcd"),
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());
        Invocation execution = execute(executionFailure,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "bounded-eos");

        List<String> contextEvents = new ArrayList<>();
        RunScenarioCommand contextFailure = new RunScenarioCommand(
                (root, name, overrides, options) -> new RunScenarioCommand.PreparedExecution() {
                    @Override
                    public ExecutableScenarioPlan.ExpectedOutcome expectedOutcome() {
                        return expectation;
                    }

                    @Override
                    public V1ScenarioExecutionResult execute(V1AttemptContext context) {
                        contextEvents.add("execute");
                        return passResult();
                    }

                    @Override
                    public void close() {
                        contextEvents.add("close");
                    }
                },
                () -> {
                    contextEvents.add("context");
                    throw new IOException("disk unavailable");
                },
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());
        Invocation context = execute(contextFailure,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "bounded-eos");

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.SOFTWARE, execution.exitCode()),
                () -> assertTrue(execution.stderr().contains("executor exploded")),
                () -> assertEquals(List.of("execute", "close"), executionEvents),
                () -> assertEquals(CommandLine.ExitCode.SOFTWARE, context.exitCode()),
                () -> assertTrue(context.stderr().contains("checkpoint storage")),
                () -> assertEquals(List.of("context", "close"), contextEvents));
    }

    @Test
    void preparedCleanupFailureConvertsPassToStructuredInconclusive() throws Exception {
        RunScenarioCommand command = new RunScenarioCommand(
                (root, name, overrides, options) ->
                        new RunScenarioCommand.PreparedExecution() {
                            @Override
                            public ExecutableScenarioPlan.ExpectedOutcome expectedOutcome() {
                                return expectation;
                            }

                            @Override
                            public V1ScenarioExecutionResult execute(
                                    V1AttemptContext context) {
                                return passResult();
                            }

                            @Override
                            public void close() {
                                throw new UncheckedIOException(
                                        "cleanup exploded", new IOException("locked"));
                            }
                        },
                () -> context("1234abcd"),
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());

        Invocation invocation = execute(command,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "bounded-eos");
        JsonNode output = JSON.readTree(invocation.stdout());

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.SOFTWARE, invocation.exitCode()),
                () -> assertEquals("", invocation.stderr()),
                () -> assertEquals("inconclusive", output.path("status").textValue()),
                () -> assertEquals(
                        "infrastructure.prepared-artifact-cleanup-failed",
                        output.path("reason").textValue()),
                () -> assertTrue(output.path("diagnostics").get(0).textValue().contains(
                        "infrastructure.prepared-artifact-cleanup-failed")),
                () -> assertTrue(output.path("diagnostics").get(0).textValue().contains(
                        "cleanup exploded")));
    }

    @Test
    void preparedCleanupFailurePreservesAuthoritativeFailureAndFenceEvidence()
            throws Exception {
        V1ScenarioExecutionResult failure = authoritativeFailureResult();
        RunScenarioCommand command = new RunScenarioCommand(
                (root, name, overrides, options) ->
                        new RunScenarioCommand.PreparedExecution() {
                            @Override
                            public ExecutableScenarioPlan.ExpectedOutcome expectedOutcome() {
                                return expectation;
                            }

                            @Override
                            public V1ScenarioExecutionResult execute(
                                    V1AttemptContext context) {
                                return failure;
                            }

                            @Override
                            public void close() {
                                throw new UncheckedIOException(
                                        "cleanup exploded", new IOException("locked"));
                            }
                        },
                () -> context("1234abcd"),
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());

        Invocation invocation = execute(command,
                "--catalog-root", temporaryDirectory.toString(),
                "--scenario", "bounded-eos");
        JsonNode output = JSON.readTree(invocation.stdout());

        assertAll(
                () -> assertEquals(CommandLine.ExitCode.SOFTWARE, invocation.exitCode()),
                () -> assertEquals("", invocation.stderr()),
                () -> assertEquals("fail", output.path("status").textValue()),
                () -> assertEquals(
                        "verification.kafka.missing-records",
                        output.path("reason").textValue()),
                () -> assertTrue(output.at(
                        "/evidence/processFence/completed").booleanValue()),
                () -> assertEquals("fail", output.at(
                        "/evidence/terminalValidation/status").textValue()),
                () -> assertTrue(output.path("diagnostics").toString().contains(
                        "infrastructure.prepared-artifact-cleanup-failed")));
    }

    @Test
    void attemptContextFactorySkipsRetainedCollisionAndKeepsRootBelowRealBase()
            throws IOException {
        Path base = Files.createDirectories(temporaryDirectory.resolve("checkpoints"));
        Files.createDirectory(base.resolve("attempt-1-deadbeef"));
        Queue<String> nonces = new ArrayDeque<>(List.of("deadbeef", "abc123de"));

        V1AttemptContext context = new V1AttemptContextFactory(base, nonces::remove).create();

        assertAll(
                () -> assertEquals(1, context.attemptOrdinal()),
                () -> assertEquals("abc123de", context.attemptNonce8()),
                () -> assertEquals(base.toRealPath(),
                        context.checkpointStorageRoot().getParent()),
                () -> assertEquals("attempt-1-abc123de",
                        context.checkpointStorageRoot().getFileName().toString()),
                () -> assertFalse(Files.exists(context.checkpointStorageRoot()),
                        "Flink infrastructure must create the root with container permissions"));
    }

    private RunScenarioCommand.PreparedExecution prepared(
            List<String> events,
            AtomicInteger executions,
            V1ScenarioExecutionResult result) {
        return new RunScenarioCommand.PreparedExecution() {
            @Override
            public ExecutableScenarioPlan.ExpectedOutcome expectedOutcome() {
                return expectation;
            }

            @Override
            public V1ScenarioExecutionResult execute(V1AttemptContext context) {
                events.add("execute");
                executions.incrementAndGet();
                return result;
            }

            @Override
            public void close() {
                events.add("close");
            }
        };
    }

    private V1AttemptContext context(String nonce) {
        return new V1AttemptContext(
                1,
                nonce,
                temporaryDirectory.resolve("checkpoints/attempt-1-" + nonce));
    }

    private JsonNode renderIncompletePhases(PhaseExecutionEvidence phases) throws Exception {
        return renderEvidence(phases, Optional.empty());
    }

    private JsonNode renderEvidence(PhaseExecutionEvidence phases,
                                   Optional<FlinkJobObservation.Attempt> observation) throws Exception {
        V1ScenarioExecutionResult result = new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.INCONCLUSIVE,
                "test.partial",
                "attempt ended before final job observation",
                Optional.empty(), Optional.of(phases), Optional.empty(), Optional.empty(),
                observation, Optional.empty(), Optional.empty(), Optional.empty(),
                List.of(), EXPECTED_RUNTIME, Optional.empty(),
                KafkaTransactionVersion.Selection.notRequested(), List.of());
        return JSON.readTree(new V1ExecutionResultRenderer().render(
                "distributed-recovery", context("1234abcd"), expectation, result)).path("evidence");
    }

    /** Runtime proof that the subject connector's classes ran; a PASS requires it. */
    private static final Optional<SubjectClassOrigins> SUBJECT_ORIGINS = Optional.of(
            new SubjectClassOrigins(
                    "/opt/flink/lib/flink-stability-connector-00000000-subject.jar",
                    List.of(new SubjectClassOrigins.ProcessOrigin("taskmanager-1#1", Map.of(
                            "org.apache.flink.connector.kafka.source.KafkaSource",
                            List.of("/opt/flink/lib/flink-stability-connector-00000000-subject.jar"),
                            "org.apache.flink.connector.kafka.sink.KafkaSink",
                            List.of("/opt/flink/lib/flink-stability-connector-00000000-subject.jar")))),
                    Optional.empty()));

    private static final FlinkJobObservation.Attempt FINISHED_JOB =
            new FlinkJobObservation.Attempt(
                    Optional.of(new FlinkJobObservation(
                            20_000, FlinkJobState.FINISHED, 6, 0, Optional.empty(),
                            List.of(), List.of())),
                    Optional.empty());

    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);
    private static final FlinkRuntimeIdentity.ExpectedTarget EXPECTED_RUNTIME =
            new FlinkRuntimeIdentity.ExpectedTarget(Optional.of(IMAGE_ID), Map.of(
                    "jobmanager-1", FlinkComponentRole.JOB_MANAGER,
                    "taskmanager-1", FlinkComponentRole.TASK_MANAGER));

    private static List<FlinkComponentProvisioningEvidence> runtimeComponents() {
        return List.of(
                FlinkComponentProvisioningEvidence.verified("jobmanager-1",
                        FlinkComponentRole.JOB_MANAGER, "jm", "flink:2.2.0", IMAGE_ID,
                        "0".repeat(64), "1".repeat(64), List.of()),
                FlinkComponentProvisioningEvidence.verified("taskmanager-1",
                        FlinkComponentRole.TASK_MANAGER, "tm", "flink:2.2.0", IMAGE_ID,
                        "0".repeat(64), "1".repeat(64), List.of()));
    }

    private static FlinkProcessWriteFenceEvidence runtimeFence() {
        return new FlinkProcessWriteFenceEvidence(runtimeComponents().stream()
                .map(component -> new FlinkProcessWriteFenceEvidence.Component(
                        component.logicalName(), component.role(), Optional.of(component.runtimeId()),
                        FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED)).toList(), Instant.EPOCH);
    }

    private static V1ScenarioExecutionResult passResult() {
        FlinkProcessWriteFenceEvidence processes = runtimeFence();
        FlinkTerminalWriteFence.Evidence fence = new FlinkTerminalWriteFence.Evidence(
                FlinkJobState.FINISHED,
                processes,
                FINISHED_JOB);
        KafkaIdSetValidationResult terminal = new KafkaIdSetValidationResult(
                KafkaIdSetValidationResult.Status.PASS,
                "validator.kafka.id-set.match",
                "Exact terminal ID set matched",
                new KafkaIdSetValidationResult.Evidence(
                        10,
                        10,
                        Optional.of(new KafkaIdSetValidationResult.DefectTotals(
                                10, 0, 0, 0, 0)),
                        List.of(), List.of(), List.of(), List.of(), List.of(),
                        Map.of(0, 0L), Map.of(0, 10L), true));
        return new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.PASS,
                terminal.reason(),
                terminal.message(),
                Optional.empty(),
                Optional.of(new PhaseExecutionEvidence(List.of())),
                Optional.of(fence),
                Optional.of(processes),
                Optional.of(FINISHED_JOB),
                Optional.of(terminal),
                Optional.empty(),
                SUBJECT_ORIGINS,
                runtimeComponents(),
                EXPECTED_RUNTIME,
                Optional.empty(),
                KafkaTransactionVersion.Selection.notRequested(),
                List.of());
    }

    private static V1ScenarioExecutionResult authoritativeFailureResult() {
        FlinkProcessWriteFenceEvidence processes = runtimeFence();
        FlinkTerminalWriteFence.Evidence fence = new FlinkTerminalWriteFence.Evidence(
                FlinkJobState.FINISHED,
                processes,
                FINISHED_JOB);
        KafkaIdSetValidationResult terminal = new KafkaIdSetValidationResult(
                KafkaIdSetValidationResult.Status.FAIL,
                "verification.kafka.missing-records",
                "One expected record was missing",
                new KafkaIdSetValidationResult.Evidence(
                        10,
                        9,
                        Optional.of(new KafkaIdSetValidationResult.DefectTotals(
                                9, 0, 0, 0, 1)),
                        List.of(), List.of(), List.of(), List.of(), List.of(),
                        Map.of(0, 0L), Map.of(0, 9L), true));
        return new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.FAIL,
                terminal.reason(),
                terminal.message(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(fence),
                Optional.of(processes),
                Optional.of(FINISHED_JOB),
                Optional.of(terminal),
                Optional.empty(),
                SUBJECT_ORIGINS,
                runtimeComponents(),
                EXPECTED_RUNTIME,
                Optional.empty(),
                KafkaTransactionVersion.Selection.notRequested(),
                List.of());
    }

    private static V1ScenarioExecutionResult duplicateResult() {
        FlinkProcessWriteFenceEvidence processes = runtimeFence();
        FlinkTerminalWriteFence.Evidence fence = new FlinkTerminalWriteFence.Evidence(
                FlinkJobState.FINISHED,
                processes,
                FINISHED_JOB);
        KafkaIdSetValidationResult terminal = new KafkaIdSetValidationResult(
                KafkaIdSetValidationResult.Status.FAIL,
                "validator.kafka.id-set.duplicate-ids",
                "Three record IDs were written twice",
                new KafkaIdSetValidationResult.Evidence(
                        10,
                        13,
                        Optional.of(new KafkaIdSetValidationResult.DefectTotals(
                                10, 0, 0, 3, 0)),
                        List.of(), List.of(), List.of(), List.of(), List.of(),
                        Map.of(0, 0L), Map.of(0, 13L), true));
        return new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.FAIL,
                terminal.reason(),
                terminal.message(),
                Optional.empty(),
                Optional.of(new PhaseExecutionEvidence(List.of())),
                Optional.of(fence),
                Optional.of(processes),
                Optional.of(FINISHED_JOB),
                Optional.of(terminal),
                Optional.empty(),
                SUBJECT_ORIGINS,
                runtimeComponents(),
                EXPECTED_RUNTIME,
                Optional.empty(),
                KafkaTransactionVersion.Selection.notRequested(),
                List.of());
    }

    private static KafkaInputManifest partialInputManifest() {
        KafkaInputManifest.ProducerAttempt firstAttempt =
                new KafkaInputManifest.ProducerAttempt(
                        1,
                        0,
                        0,
                        0,
                        KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED);
        KafkaInputManifest.ProducerAttempt secondAttempt =
                new KafkaInputManifest.ProducerAttempt(
                        1,
                        0,
                        0,
                        1,
                        KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED);
        return new KafkaInputManifest(
                "kafka",
                "input",
                2,
                List.of(
                        new KafkaInputManifest.RecordAcknowledgement(
                                0,
                                "0".repeat(64),
                                List.of(firstAttempt),
                                0,
                                0,
                                KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED,
                                KafkaInputManifest.TerminalDisposition.PRESENT),
                        new KafkaInputManifest.RecordAcknowledgement(
                                1,
                                "1".repeat(64),
                                List.of(secondAttempt),
                                0,
                                1,
                                KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED,
                                KafkaInputManifest.TerminalDisposition.INDETERMINATE)),
                Map.of(0, 0L),
                Map.of(0, 2L),
                KafkaInputManifest.EvidenceStatus.PARTIAL,
                new KafkaInputManifest.ReconciliationEvidence(
                        Map.of("isolation.level", "read_uncommitted"),
                        List.of(0L),
                        false));
    }

    private static V1ScenarioExecutionResult result(
            V1ScenarioExecutionResult.Status status,
            String reason) {
        return result(status, reason, List.of());
    }

    private static V1ScenarioExecutionResult result(
            V1ScenarioExecutionResult.Status status,
            String reason,
            List<FlinkComponentProvisioningEvidence> components) {
        return new V1ScenarioExecutionResult(
                status,
                reason,
                "attempt ended",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                components,
                new FlinkRuntimeIdentity.ExpectedTarget(Optional.empty(), EXPECTED_RUNTIME.components()),
                Optional.empty(),
                KafkaTransactionVersion.Selection.notRequested(),
                List.of("diagnostic"));
    }

    private static Invocation executeRoot(String... arguments) {
        return execute(new FlinkStabilityCommand(), arguments);
    }

    private static Invocation execute(Object command, String... arguments) {
        StringWriter stdout = new StringWriter();
        StringWriter stderr = new StringWriter();
        CommandLine commandLine = new CommandLine(command)
                .setOut(new PrintWriter(stdout, true))
                .setErr(new PrintWriter(stderr, true));
        int exitCode = commandLine.execute(arguments);
        return new Invocation(exitCode, stdout.toString(), stderr.toString());
    }

    private record Invocation(int exitCode, String stdout, String stderr) {}
}
