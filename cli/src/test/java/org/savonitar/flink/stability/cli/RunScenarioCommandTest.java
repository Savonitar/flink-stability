package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
                () -> assertEquals(0,
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
    void partialEvidenceIsExplicitAndDoesNotPublishProvisionalDefectTotals()
            throws Exception {
        FlinkProcessWriteFenceEvidence processFence = new FlinkProcessWriteFenceEvidence(
                List.of(), Instant.EPOCH);
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
                List.of(),
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
    void rendersTheJobObservationAndTheEffectOfEachTaskManagerKill() throws Exception {
        FlinkJobObservation.Failure lostTaskManager = new FlinkJobObservation.Failure(
                12_000, "ResourceManagerException",
                "TaskManager with id tm-1 is no longer reachable.", Optional.of("tm-1"));
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
                                                Optional.of("tm-1"))))),
                                Optional.empty()),
                        java.util.OptionalLong.of(2_000))));
        FlinkJobObservation.Attempt atFence = new FlinkJobObservation.Attempt(
                Optional.of(new FlinkJobObservation(
                        20_000, FlinkJobState.FINISHED, 20, 1,
                        Optional.of(new FlinkJobObservation.Restore(4, 13_000)),
                        List.of(lostTaskManager), List.of())),
                Optional.empty());
        FlinkProcessWriteFenceEvidence processes = new FlinkProcessWriteFenceEvidence(
                List.of(), Instant.EPOCH);
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
                List.of(),
                List.of());

        JsonNode evidence = JSON.readTree(new V1ExecutionResultRenderer().render(
                "bounded-eos", context("1234abcd"), expectation, result)).path("evidence");
        JsonNode job = evidence.path("flinkJob");
        JsonNode kill = evidence.path("taskManagerKills").path(0);

        assertAll(
                () -> assertEquals("observed", job.path("status").textValue()),
                () -> assertEquals("FINISHED", job.path("state").textValue()),
                () -> assertEquals(1, job.path("restoredCheckpoints").longValue()),
                () -> assertEquals(4, job.path("latestRestoredCheckpoint").longValue()),
                () -> assertEquals(1, job.path("failures").intValue()),
                () -> assertEquals("$/phases/1/steps/0", kill.path("path").textValue()),
                () -> assertEquals("checkpoint-restored", kill.path("outcome").textValue()),
                () -> assertTrue(kill.path("confirmed").booleanValue()),
                () -> assertEquals("RUNNING", kill.path("jobStateBeforeKill").textValue()),
                () -> assertEquals(4, kill.path("completedCheckpointsBeforeKill").longValue()),
                () -> assertEquals(1, kill.path("activeSubtasksBeforeKill").longValue()),
                () -> assertEquals(4, kill.path("restoredCheckpoint").longValue()),
                () -> assertEquals(2_000, kill.path("jobManagerTimeAfterKill").longValue()),
                () -> assertEquals(11_000,
                        kill.path("restoredAfterKillObservationMs").longValue()),
                () -> assertEquals(1, kill.path("failuresAfterKill").intValue()),
                () -> assertEquals("TaskManager with id tm-1 is no longer reachable.",
                        kill.path("firstFailureAfterKill").textValue()),
                () -> assertEquals("confirmed",
                        evidence.at("/subjectClasses/status").textValue()),
                () -> assertEquals("taskmanager-1#1",
                        evidence.at("/subjectClasses/processes/0/process").textValue()));
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

    private static V1ScenarioExecutionResult passResult() {
        FlinkProcessWriteFenceEvidence processes = new FlinkProcessWriteFenceEvidence(
                List.of(), Instant.EPOCH);
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
                Optional.empty(),
                Optional.of(fence),
                Optional.of(processes),
                Optional.of(FINISHED_JOB),
                Optional.of(terminal),
                Optional.empty(),
                SUBJECT_ORIGINS,
                List.of(),
                List.of());
    }

    private static V1ScenarioExecutionResult authoritativeFailureResult() {
        FlinkProcessWriteFenceEvidence processes = new FlinkProcessWriteFenceEvidence(
                List.of(), Instant.EPOCH);
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
                List.of(),
                List.of());
    }

    private static V1ScenarioExecutionResult duplicateResult() {
        FlinkProcessWriteFenceEvidence processes = new FlinkProcessWriteFenceEvidence(
                List.of(), Instant.EPOCH);
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
                List.of(),
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
                List.of(),
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
