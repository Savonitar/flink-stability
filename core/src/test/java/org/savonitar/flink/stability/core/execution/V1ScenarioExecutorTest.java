package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputPreparationException;
import org.savonitar.flink.stability.core.execution.kafka.PreparedKafkaInput;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.flink.FlinkJobSubmission;
import org.savonitar.flink.stability.core.flink.FlinkRestTimeoutException;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.core.artifact.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.artifact.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler;
import org.savonitar.flink.stability.core.execution.plan.PreparedExecutableScenarioPlan;
import org.savonitar.flink.stability.core.validation.kafka.KafkaIdSetValidationResult;
import org.savonitar.flink.stability.core.validation.kafka.KafkaTransactionListing;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TaskManagerActionTimeoutException;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class V1ScenarioExecutorTest {
    private static final FlinkJobHandle JOB = new FlinkJobHandle("job-1");
    private static final KafkaRuntimeEndpoints ENDPOINTS = new KafkaRuntimeEndpoints(
            "main", "apache/kafka:4.0.0", "kafka-main:19092", "localhost:39092");

    @TempDir
    Path temporaryDirectory;

    private V1ScenarioExecutor.TransactionListing transactionListing =
            (bootstrapServers, prefix, timeout) -> new KafkaTransactionListing(
                    prefix,
                    List.of(new KafkaTransactionListing.Transaction(
                            prefix + "-0-1", "CompleteCommit", 7, 0, List.of())));

    @Test
    void validatesOnlyAfterNaturalCompletionAndThePhysicalProcessFence() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            FakeFlink flink = new FakeFlink(events);
            AtomicBoolean validationObservedFence = new AtomicBoolean();
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> {
                        events.add("terminal-validation");
                        validationObservedFence.set(runtime.fenced);
                        assertEquals("localhost:39092", bootstrap);
                        assertEquals("output", topic);
                        assertArrayEquals(
                                java.util.stream.LongStream.range(0, 10).toArray(), count);
                        assertEquals(Duration.ofMinutes(2), timeout);
                        return passResult();
                    });

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status());
            assertEquals("validator.kafka.id-set.match", result.reason());
            assertTrue(validationObservedFence.get());
            assertTrue(result.writeFenceEvidence().isPresent());
            assertTrue(result.processFenceEvidence().isPresent());
            assertTrue(result.terminalValidation().isPresent());
            assertEquals(List.of(
                            "runtime-create",
                            "kafka-start",
                            "input-prepare",
                            "flink-start",
                            "flink-open",
                            "jar-upload",
                            "job-submit",
                            "await-running",
                            "await-finished",
                            "observe-job",
                            "process-fence",
                            "terminal-validation",
                            "list-transactions",
                            "flink-close",
                            "runtime-close"),
                    events);
            assertEquals("kafka-main:19092", flink.submission.flinkConfiguration().get(
                    "flink-stability.workload.v1.source.bootstrap-servers"));
            assertEquals("0:10", flink.submission.flinkConfiguration().get(
                    "flink-stability.workload.v1.source.stopping-offsets"));
            assertEquals(
                    fixture.bound().workloadArtifact().sha256(),
                    flink.uploadedJarSha256);
        }
    }

    @Test
    void kafkaMismatchFailsOnlyAfterTheWriteFence() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            FakeFlink flink = new FakeFlink(events);
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> {
                        assertTrue(runtime.fenced);
                        events.add("terminal-validation");
                        return missingResult();
                    });

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.FAIL, result.status());
            assertEquals("validator.kafka.id-set.missing-ids", result.reason());
            assertTrue(result.writeFenceEvidence().isPresent());
            assertEquals(1, result.terminalValidation().orElseThrow()
                    .evidence().defectTotals().orElseThrow().missingCount());
        }
    }

    @Test
    void completionTimeoutForceFencesAndFailsWithoutStartingKafkaValidation()
            throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            FakeFlink flink = new FakeFlink(events);
            flink.awaitFinishedFailure = new FlinkRestTimeoutException("deadline");
            AtomicBoolean validationCalled = new AtomicBoolean();
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> {
                        validationCalled.set(true);
                        return passResult();
                    });

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.FAIL, result.status());
            assertEquals("verification.flink.job-completion-timeout", result.reason());
            assertTrue(runtime.fenced);
            assertFalse(validationCalled.get());
            assertTrue(result.terminalValidation().isEmpty());
            assertTrue(result.processFenceEvidence().isPresent());
            assertTrue(events.indexOf("process-fence") < events.indexOf("runtime-close"));
        }
    }

    @Test
    void taskManagerActionTimeoutIsInconclusiveAndSkipsFenceAndKafkaValidation()
            throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(document -> {
            ArrayNode steps = (ArrayNode) document.at("/phases/0/steps");
            steps.removeAll();
            ObjectNode target = steps.addObject().putObject("kill").putObject("target");
            target.put("kind", "named");
            target.put("role", "taskmanager");
            target.put("name", "taskmanager-1");
            steps.addObject().putObject("restart").put("component", "taskmanager");
        })) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.killFailure = new TaskManagerActionTimeoutException(
                    TaskManagerActionTimeoutException.Action.KILL,
                    ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT,
                    new IllegalStateException("driver deadline"));
            FakeFlink flink = new FakeFlink(events);
            AtomicBoolean validationCalled = new AtomicBoolean();
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> {
                        validationCalled.set(true);
                        return passResult();
                    });

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals("taskmanager.kill.timeout", result.reason());
            assertFalse(validationCalled.get());
            assertFalse(runtime.fenced);
            assertTrue(result.writeFenceEvidence().isEmpty());
            assertTrue(result.processFenceEvidence().isEmpty());
            assertTrue(result.terminalValidation().isEmpty());
            assertEquals("taskmanager.kill.timeout",
                    result.phaseEvidence().orElseThrow().steps().getLast().detail());
            // The job is still observed once, to explain the stop.
            assertTrue(result.finalJobObservation().orElseThrow().observation().isPresent());
        }
    }

    @Test
    void passingOracleIsInconclusiveWhenTheKillHitAnAlreadyFinishedJob() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::killAndRestart)) {
            FakeFlink flink = new FakeFlink(events);
            flink.observations.add(job(1_000, FlinkJobState.FINISHED, 2, 0));
            flink.observations.add(job(9_000, FlinkJobState.FINISHED, 2, 0));
            V1ScenarioExecutor executor = executor(
                    events, new FakeRuntime(events), flink,
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals("taskmanager.kill.effect-unconfirmed", result.reason());
            assertTrue(result.message().contains("JOB_TERMINAL_BEFORE_KILL"), result.message());
            assertTrue(result.terminalValidation().isPresent(),
                    "the oracle still runs; only its pass is withheld");
            assertEquals(
                    List.of(TaskManagerKillEffect.Outcome.JOB_TERMINAL_BEFORE_KILL),
                    outcomes(result));
        }
    }

    @Test
    void passingOracleIsInconclusiveWhenTheKilledTaskManagerHostedNoSubtask() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::killAndRestart)) {
            FakeFlink flink = new FakeFlink(events);
            // The TaskManager hosted no deployed subtask, so killing it disturbed nothing.
            flink.observations.add(new FlinkJobObservation(
                    1_000, FlinkJobState.RUNNING, 0, 0, Optional.empty(), List.of(),
                    List.of(new FlinkJobObservation.Subtask(
                            "Kafka Source", 0, 0, "SCHEDULED", Optional.empty()))));
            flink.observations.add(job(9_000, FlinkJobState.FINISHED, 3, 0));
            V1ScenarioExecutor executor = executor(
                    events, new FakeRuntime(events), flink,
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals("taskmanager.kill.effect-unconfirmed", result.reason());
            assertEquals(
                    List.of(TaskManagerKillEffect.Outcome.NO_ACTIVE_SUBTASK_BEFORE_KILL),
                    outcomes(result));
        }
    }

    @Test
    void confirmedRecoverySupportsPositiveAndNegativeControls() throws Exception {
        for (boolean oraclePasses : List.of(false, true)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(V1ScenarioExecutorTest::killAndRestart)) {
                FakeFlink flink = new FakeFlink(events);
                flink.observations.add(new FlinkJobObservation(
                        1_000, FlinkJobState.RUNNING, 2, 0, Optional.empty(), List.of(),
                        List.of(new FlinkJobObservation.Subtask(
                                "Kafka Source", 0, 0, "RUNNING", Optional.of("tm-1")))));
                flink.observations.add(new FlinkJobObservation(
                        9_000, FlinkJobState.FINISHED, 6, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        List.of(new FlinkJobObservation.Failure(
                                4_000, "ResourceManagerException",
                                "TaskManager with id tm-1 is no longer reachable.",
                                Optional.of("tm-1"))),
                        List.of()));
                V1ScenarioExecutor executor = executor(
                        events, new FakeRuntime(events), flink,
                        (bootstrap, topic, count, timeout) -> oraclePasses ? passResult() : missingResult());

                V1ScenarioExecutionResult result = executor.execute(
                        fixture.bound(), attemptContext());

                assertEquals(oraclePasses ? V1ScenarioExecutionResult.Status.PASS
                        : V1ScenarioExecutionResult.Status.FAIL, result.status());
                ScenarioVerdict verdict = ScenarioVerdict.of(oraclePasses
                        ? ExecutableScenarioPlan.ExpectedOutcome.pass()
                        : ExecutableScenarioPlan.ExpectedOutcome.failure("kafka.id-set", result.reason()), result);
                assertEquals(ScenarioVerdict.Status.PASS, verdict.status());
                TaskManagerKillEffect effect = result.taskManagerKillEffects().getFirst();
                assertEquals(TaskManagerKillEffect.Outcome.CHECKPOINT_RESTORED, effect.outcome());
                assertEquals(Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        effect.restore());
                assertEquals(1, effect.failuresAfterKill().size());
                assertTrue(events.indexOf("observe-job") < events.indexOf("taskmanager-kill"));
            }
        }
    }

    @Test
    void failingOracleStaysFailEvenWhenTheKillWasIneffective() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::killAndRestart)) {
            FakeFlink flink = new FakeFlink(events);
            flink.observations.add(job(1_000, FlinkJobState.FINISHED, 2, 0));
            V1ScenarioExecutor executor = executor(
                    events, new FakeRuntime(events), flink,
                    (bootstrap, topic, count, timeout) -> missingResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.FAIL, result.status());
            assertEquals("validator.kafka.id-set.missing-ids", result.reason());
            assertEquals(
                    List.of(TaskManagerKillEffect.Outcome.JOB_TERMINAL_BEFORE_KILL),
                    outcomes(result));
            ScenarioVerdict verdict = ScenarioVerdict.of(
                    ExecutableScenarioPlan.ExpectedOutcome.failure("kafka.id-set", result.reason()), result);
            assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_KILL_EFFECT_UNCONFIRMED, verdict.reason());
        }
    }

    @Test
    void aPassResultCannotCarryAnUnconfirmedKill() {
        PhaseExecutionEvidence phases = new PhaseExecutionEvidence(
                List.of(),
                List.of(new PhaseExecutionEvidence.TaskManagerKill(
                        "$/phases/0/steps/0",
                        List.of(),
                        "taskmanager-1",
                        new FlinkJobObservation.Attempt(
                                Optional.of(job(1_000, FlinkJobState.FINISHED, 1, 0)),
                                Optional.empty()),
                        OptionalLong.of(1_500))));
        FlinkJobObservation.Attempt atFence = new FlinkJobObservation.Attempt(
                Optional.of(job(2_000, FlinkJobState.FINISHED, 1, 0)), Optional.empty());
        FlinkProcessWriteFenceEvidence processFence = new FlinkProcessWriteFenceEvidence(
                List.of(), Instant.parse("2026-08-26T12:00:00Z"));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new V1ScenarioExecutionResult(
                        V1ScenarioExecutionResult.Status.PASS,
                        "validator.kafka.id-set.match",
                        "passed",
                        Optional.empty(),
                        Optional.of(phases),
                        Optional.of(new FlinkTerminalWriteFence.Evidence(
                                FlinkJobState.FINISHED, processFence, atFence)),
                        Optional.of(processFence),
                        Optional.of(atFence),
                        Optional.of(passResult()),
                        Optional.empty(),
                        Optional.of(confirmedOrigins()),
                        List.of(),
                        List.of()));

        assertTrue(failure.getMessage().contains("confirmed effect"));
    }

    static SubjectClassOrigins confirmedOrigins() {
        String primary = "/opt/flink/lib/flink-stability-connector-00000000-subject.jar";
        return new SubjectClassOrigins(
                primary,
                List.of(new SubjectClassOrigins.ProcessOrigin("taskmanager-1#1", Map.of(
                        "org.apache.flink.connector.kafka.source.KafkaSource", List.of(primary),
                        "org.apache.flink.connector.kafka.sink.KafkaSink", List.of(primary)))),
                Optional.empty());
    }

    private static void killAndRestart(ObjectNode document) {
        ArrayNode steps = (ArrayNode) document.at("/phases/0/steps");
        steps.removeAll();
        ObjectNode target = steps.addObject().putObject("kill").putObject("target");
        target.put("kind", "named");
        target.put("role", "taskmanager");
        target.put("name", "taskmanager-1");
        steps.addObject().putObject("restart").put("component", "taskmanager");
    }

    private static FlinkJobObservation job(
            long jobManagerTimeMillis,
            FlinkJobState state,
            long completedCheckpoints,
            long restoredCheckpoints) {
        return new FlinkJobObservation(
                jobManagerTimeMillis, state, completedCheckpoints, restoredCheckpoints,
                Optional.empty(), List.of(), List.of());
    }

    private static List<TaskManagerKillEffect.Outcome> outcomes(
            V1ScenarioExecutionResult result) {
        return result.taskManagerKillEffects().stream()
                .map(TaskManagerKillEffect::outcome)
                .toList();
    }

    @Test
    void listsTheSinkTransactionsAfterTheFenceAsEvidenceOnly() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            transactionListing = (bootstrapServers, prefix, timeout) -> new KafkaTransactionListing(
                    prefix,
                    List.of(
                            new KafkaTransactionListing.Transaction(
                                    prefix + "-0-1", "CompleteCommit", 7, 0, List.of()),
                            new KafkaTransactionListing.Transaction(
                                    prefix + "-0-2", "Ongoing", 8, 1, List.of("output-0"))));
            V1ScenarioExecutor executor = executor(
                    events, new FakeRuntime(events), new FakeFlink(events),
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            KafkaTransactionListing listing = result.sinkTransactions().orElseThrow();
            assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status());
            assertEquals(fixture.bound().executablePlan().job().sink().transactionalIdPrefix(),
                    Optional.of(listing.transactionalIdPrefix()));
            assertEquals(List.of("Ongoing"), listing.unresolved().stream()
                    .map(KafkaTransactionListing.Transaction::state)
                    .toList());
            assertTrue(events.indexOf("process-fence") < events.indexOf("list-transactions"));
        }
    }

    @Test
    void passingOracleIsInconclusiveWhenAnotherJarSuppliedTheConnectorClasses()
            throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.loadedFrom = "/opt/flink/lib/flink-connector-kafka-released.jar";
            V1ScenarioExecutor executor = executor(
                    events, runtime, new FakeFlink(events),
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(V1ScenarioExecutor.SUBJECT_ORIGIN_MISMATCH, result.reason());
            assertTrue(result.message().contains("flink-connector-kafka-released.jar"),
                    result.message());
        }
    }

    @Test
    void passingOracleIsInconclusiveWhenTheClassLoadLogsAreUnavailable() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.classLoadLogsFailure = new IllegalStateException("attempt directory gone");
            V1ScenarioExecutor executor = executor(
                    events, runtime, new FakeFlink(events),
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(V1ScenarioExecutor.SUBJECT_ORIGIN_UNCONFIRMED, result.reason());
            assertTrue(result.message().contains("attempt directory gone"), result.message());
        }
    }

    @Test
    void anAtLeastOnceSinkHasNoTransactionsToList() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(document -> {
            ObjectNode sink = (ObjectNode) document.at("/workload/jobs/0/sink");
            sink.put("delivery_guarantee", "AT_LEAST_ONCE");
            sink.remove("transactional_id_prefix");
            sink.remove("transaction_id_naming_strategy");
        })) {
            V1ScenarioExecutor executor = executor(
                    events, new FakeRuntime(events), new FakeFlink(events),
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status());
            assertFalse(events.contains("list-transactions"));
            assertTrue(result.sinkTransactions().isEmpty());
            assertTrue(result.diagnostics().isEmpty());
        }
    }

    @Test
    void anUnavailableTransactionListingIsADiagnosticAndKeepsTheVerdict() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            transactionListing = (bootstrapServers, prefix, timeout) -> {
                throw new java.util.concurrent.TimeoutException("admin timed out");
            };
            V1ScenarioExecutor executor = executor(
                    events, new FakeRuntime(events), new FakeFlink(events),
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status());
            assertTrue(result.sinkTransactions().isEmpty());
            assertEquals(List.of("kafka.sink-transactions-unavailable: TimeoutException:"
                            + " admin timed out"),
                    result.diagnostics());
        }
    }

    @Test
    void unexpectedKafkaVerificationFailureStillFailsRatherThanBecomingInconclusive()
            throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            FakeFlink flink = new FakeFlink(events);
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> {
                        assertTrue(runtime.fenced);
                        throw new IllegalStateException("unexpected validator failure");
                    });

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.FAIL, result.status());
            assertEquals("verification.kafka.snapshot-unavailable", result.reason());
            assertTrue(result.writeFenceEvidence().isPresent());
            assertTrue(result.terminalValidation().isEmpty());
        }
    }

    @Test
    void indeterminateInputNeverStartsFlinkAndStillCleansTheAttempt() throws Exception {
        List<String> events = new ArrayList<>();
        KafkaInputManifest partialEvidence = inputManifest(
                KafkaInputManifest.EvidenceStatus.PARTIAL,
                9,
                false,
                KafkaInputManifest.TerminalDisposition.INDETERMINATE);
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            FakeFlink flink = new FakeFlink(events);
            V1ScenarioExecutor executor = new V1ScenarioExecutor(
                    checkpointRoot -> {
                        events.add("runtime-create");
                        return runtime;
                    },
                    (plan, endpoints) -> {
                        events.add("input-prepare");
                        throw new KafkaInputPreparationException(
                                KafkaInputPreparationException.INDETERMINATE,
                                "input reconciliation timed out",
                                partialEvidence);
                    },
                    url -> {
                        events.add("flink-open");
                        return flink;
                    },
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(KafkaInputPreparationException.INDETERMINATE, result.reason());
            assertEquals(partialEvidence, result.inputManifest().orElseThrow());
            assertEquals(List.of(
                    "runtime-create", "kafka-start", "input-prepare", "runtime-close"), events);
        }
    }

    @Test
    void completeInputEvidenceSurvivesInfrastructureCloseFailure() throws Exception {
        List<String> events = new ArrayList<>();
        KafkaInputManifest completeEvidence = inputManifest(
                KafkaInputManifest.EvidenceStatus.COMPLETE,
                10,
                true,
                KafkaInputManifest.TerminalDisposition.PRESENT);
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            FakeFlink flink = new FakeFlink(events);
            V1ScenarioExecutor executor = new V1ScenarioExecutor(
                    checkpointRoot -> {
                        events.add("runtime-create");
                        return runtime;
                    },
                    (plan, endpoints) -> {
                        events.add("input-prepare");
                        throw new KafkaInputPreparationException(
                                KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                                "Kafka Admin failed to close after input was evidenced",
                                new IOException("admin close failed"),
                                completeEvidence);
                    },
                    url -> {
                        events.add("flink-open");
                        return flink;
                    },
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(
                    KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                    result.reason());
            assertEquals(completeEvidence, result.inputManifest().orElseThrow());
            assertEquals(KafkaInputManifest.EvidenceStatus.COMPLETE,
                    result.inputManifest().orElseThrow().evidenceStatus());
            assertEquals(List.of(
                    "runtime-create", "kafka-start", "input-prepare", "runtime-close"), events);
        }
    }

    @Test
    void fatalUnexpectedFailureStillClosesFlinkAndTheAttemptRuntime() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            FakeFlink flink = new FakeFlink(events);
            flink.uploadFatal = new AssertionError("fatal upload failure");
            V1ScenarioExecutor executor = executor(
                    events, runtime, flink,
                    (bootstrap, topic, count, timeout) -> passResult());

            AssertionError failure = assertThrows(
                    AssertionError.class,
                    () -> executor.execute(fixture.bound(), attemptContext()));

            assertEquals("fatal upload failure", failure.getMessage());
            assertTrue(events.contains("flink-close"));
            assertTrue(events.contains("runtime-close"));
            assertTrue(events.indexOf("flink-close") < events.indexOf("runtime-close"));
        }
    }

    @Test
    void checkedInfrastructureInterruptionRestoresTheThreadFlagAndCleansTheAttempt()
            throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.startFlinkFailure = new InterruptedException("cancelled");
            FakeFlink flink = new FakeFlink(events);
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> passResult());

            try {
                V1ScenarioExecutionResult result = executor.execute(
                        fixture.bound(), attemptContext());

                assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
                assertEquals("infrastructure.flink-start-failed", result.reason());
                assertTrue(result.diagnostics().stream().anyMatch(diagnostic ->
                        diagnostic.startsWith("infrastructure.attempt-cleanup-failed:")
                                && diagnostic.contains(
                                        "Interrupted while waiting for attempt cleanup")));
                assertTrue(Thread.currentThread().isInterrupted());
                Thread.interrupted();
                assertTrue(runtime.closeEntered.await(1, TimeUnit.SECONDS));
                Thread.currentThread().interrupt();
                assertEquals(List.of(
                                "runtime-create",
                                "kafka-start",
                                "input-prepare",
                                "flink-start",
                                "runtime-close"),
                        events);
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void cleanupTimeoutReturnsInconclusiveWithoutWaitingForReleaseAndLeavesOwnerIndependent()
            throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        Fixture fixture = fixture();
        Path preparedWorkload = fixture.bound().workloadArtifact().preparedPath();
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        FakeRuntime runtime = new FakeRuntime(events);
        runtime.closeRelease = releaseCleanup;
        FakeFlink flink = new FakeFlink(events);
        V1ScenarioExecutor executor = executor(
                events,
                runtime,
                flink,
                (bootstrap, topic, count, timeout) -> passResult(),
                Duration.ofMillis(100));
        AtomicReference<V1ScenarioExecutionResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean ownerClosed = new AtomicBoolean();

        Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
            try {
                result.set(executor.execute(fixture.bound(), attemptContext()));
            } catch (Throwable unexpected) {
                failure.set(unexpected);
            }
        });

        try {
            assertTrue(runtime.closeEntered.await(1, TimeUnit.SECONDS));
            caller.join(Duration.ofSeconds(2));

            assertFalse(caller.isAlive(), "attempt cleanup exceeded its wall-clock bound");
            assertNull(failure.get());
            V1ScenarioExecutionResult outcome = result.get();
            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, outcome.status());
            assertEquals("infrastructure.attempt-cleanup-failed", outcome.reason());
            assertTrue(outcome.terminalValidation().isPresent());
            assertTrue(outcome.diagnostics().stream().anyMatch(diagnostic ->
                    diagnostic.startsWith("infrastructure.attempt-cleanup-failed:")
                            && diagnostic.contains("AttemptCleanupTimeoutException")
                            && diagnostic.contains("PT0.1S")));
            assertEquals(1, runtime.closeCalls.get());
            assertTrue(Files.isRegularFile(preparedWorkload));

            // The executor/background cleanup never acquires prepared-workspace ownership.
            fixture.close();
            ownerClosed.set(true);
            assertFalse(Files.exists(preparedWorkload));
            assertEquals(1, runtime.closeCalls.get());
        } finally {
            releaseCleanup.countDown();
            caller.join(Duration.ofSeconds(1));
            if (!ownerClosed.get()) {
                fixture.close();
            }
        }
    }

    @Test
    void cleanupDiagnosticsRetainBothFlinkAndRuntimeCloseFailures() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.closeFailure = new IllegalStateException("runtime close failed");
            FakeFlink flink = new FakeFlink(events);
            flink.closeFailure = new IllegalArgumentException("flink close failed");
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> passResult());

            V1ScenarioExecutionResult result = executor.execute(
                    fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals("infrastructure.attempt-cleanup-failed", result.reason());
            assertTrue(result.diagnostics().stream().anyMatch(diagnostic ->
                    diagnostic.contains("IllegalArgumentException: flink close failed")));
            assertTrue(result.diagnostics().stream().anyMatch(diagnostic ->
                    diagnostic.contains("suppressed IllegalStateException: runtime close failed")));
        }
    }

    @Test
    void processFenceFailureRemainsAuthoritativeAndSkipsValidationWhenCleanupTimesOut()
            throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.processFenceFailure = new IllegalStateException("driver fence timed out");
            runtime.closeRelease = releaseCleanup;
            FakeFlink flink = new FakeFlink(events);
            AtomicBoolean validationCalled = new AtomicBoolean();
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> {
                        validationCalled.set(true);
                        return passResult();
                    },
                    Duration.ofMillis(100));
            AtomicReference<V1ScenarioExecutionResult> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
                try {
                    result.set(executor.execute(fixture.bound(), attemptContext()));
                } catch (Throwable unexpected) {
                    failure.set(unexpected);
                }
            });

            try {
                assertTrue(runtime.closeEntered.await(1, TimeUnit.SECONDS));
                caller.join(Duration.ofSeconds(2));

                assertFalse(caller.isAlive(), "attempt cleanup exceeded its wall-clock bound");
                assertNull(failure.get());
                V1ScenarioExecutionResult outcome = result.get();
                assertEquals(V1ScenarioExecutionResult.Status.FAIL, outcome.status());
                assertEquals("verification.flink.process-fence-failed", outcome.reason());
                assertFalse(validationCalled.get());
                assertTrue(outcome.writeFenceEvidence().isEmpty());
                assertTrue(outcome.processFenceEvidence().isEmpty());
                assertTrue(outcome.terminalValidation().isEmpty());
                assertTrue(outcome.diagnostics().stream().anyMatch(diagnostic ->
                        diagnostic.startsWith("infrastructure.attempt-cleanup-failed:")
                                && diagnostic.contains("AttemptCleanupTimeoutException")));
                assertEquals(1, runtime.closeCalls.get());
            } finally {
                releaseCleanup.countDown();
                caller.join(Duration.ofSeconds(1));
            }
        }
    }

    @Test
    void interruptionWhileWaitingForCleanupReturnsAndPreservesCallerInterrupt()
            throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.closeRelease = releaseCleanup;
            FakeFlink flink = new FakeFlink(events);
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> passResult(),
                    Duration.ofSeconds(30));
            AtomicReference<V1ScenarioExecutionResult> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interruptedOnReturn = new AtomicBoolean();

            Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
                try {
                    result.set(executor.execute(fixture.bound(), attemptContext()));
                    interruptedOnReturn.set(Thread.currentThread().isInterrupted());
                } catch (Throwable unexpected) {
                    failure.set(unexpected);
                }
            });

            try {
                assertTrue(runtime.closeEntered.await(1, TimeUnit.SECONDS));
                caller.interrupt();
                caller.join(Duration.ofSeconds(2));

                assertFalse(caller.isAlive());
                assertNull(failure.get());
                V1ScenarioExecutionResult outcome = result.get();
                assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, outcome.status());
                assertEquals("infrastructure.attempt-cleanup-failed", outcome.reason());
                assertTrue(outcome.diagnostics().stream().anyMatch(diagnostic ->
                        diagnostic.startsWith("infrastructure.attempt-cleanup-failed:")
                                && diagnostic.contains(
                                        "Interrupted while waiting for attempt cleanup")));
                assertTrue(interruptedOnReturn.get());
                assertEquals(1, runtime.closeCalls.get());
            } finally {
                releaseCleanup.countDown();
                caller.join(Duration.ofSeconds(1));
            }
        }
    }

    @Test
    void interruptAlreadySetBeforeCleanupReturnsPromptlyAndIsPreserved() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.closeRelease = releaseCleanup;
            FakeFlink flink = new FakeFlink(events);
            V1ScenarioExecutor executor = executor(
                    events,
                    runtime,
                    flink,
                    (bootstrap, topic, count, timeout) -> {
                        Thread.currentThread().interrupt();
                        return passResult();
                    },
                    Duration.ofSeconds(30));
            AtomicReference<V1ScenarioExecutionResult> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interruptedOnReturn = new AtomicBoolean();

            Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
                try {
                    result.set(executor.execute(fixture.bound(), attemptContext()));
                    interruptedOnReturn.set(Thread.currentThread().isInterrupted());
                } catch (Throwable unexpected) {
                    failure.set(unexpected);
                }
            });

            try {
                caller.join(Duration.ofSeconds(2));

                assertFalse(caller.isAlive());
                assertNull(failure.get());
                V1ScenarioExecutionResult outcome = result.get();
                assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, outcome.status());
                assertEquals("infrastructure.attempt-cleanup-failed", outcome.reason());
                assertTrue(outcome.diagnostics().stream().anyMatch(diagnostic ->
                        diagnostic.startsWith("infrastructure.attempt-cleanup-failed:")
                                && diagnostic.contains(
                                        "Interrupted while waiting for attempt cleanup")));
                assertTrue(interruptedOnReturn.get());
            } finally {
                releaseCleanup.countDown();
                caller.join(Duration.ofSeconds(1));
            }
        }
    }

    private V1ScenarioExecutor executor(
            List<String> events,
            FakeRuntime runtime,
            FakeFlink flink,
            V1ScenarioExecutor.TerminalValidation validation) {
        return executor(
                events,
                runtime,
                flink,
                validation,
                V1ScenarioExecutor.DEFAULT_ATTEMPT_CLEANUP_TIMEOUT);
    }

    private V1ScenarioExecutor executor(
            List<String> events,
            FakeRuntime runtime,
            FakeFlink flink,
            V1ScenarioExecutor.TerminalValidation validation,
            Duration cleanupTimeout) {
        return new V1ScenarioExecutor(
                checkpointRoot -> {
                    events.add("runtime-create");
                    return runtime;
                },
                (plan, endpoints) -> {
                    events.add("input-prepare");
                    return preparedInput();
                },
                url -> {
                    events.add("flink-open");
                    assertEquals("http://localhost:8081", url);
                    return flink;
                },
                validation,
                (bootstrapServers, prefix, timeout) -> {
                    events.add("list-transactions");
                    assertEquals("localhost:39092", bootstrapServers);
                    assertEquals(V1ScenarioExecutor.SINK_TRANSACTION_LISTING_TIMEOUT, timeout);
                    return transactionListing.list(bootstrapServers, prefix, timeout);
                },
                cleanupTimeout);
    }

    private Fixture fixture() throws IOException {
        return fixture(document -> {});
    }

    private Fixture fixture(Consumer<ObjectNode> mutation) throws IOException {
        Path connector = createJar(temporaryDirectory.resolve("connector.jar"), false, null);
        Path workload = createJar(temporaryDirectory.resolve("job.jar"), true, "v1");
        SpecificationLoader loader = new SpecificationLoader();
        ScenarioSpecification base = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode document = base.document();
        document.put("health_retry_limit", 0);
        ObjectNode connectorDocument = (ObjectNode) document.at("/subject/connectors/kafka");
        connectorDocument.put("artifact", connector.getFileName().toString());
        connectorDocument.putArray("runtime_dependencies");
        ((ObjectNode) document.at("/workload/jobs/0"))
                .put("jar", workload.getFileName().toString());
        mutation.accept(document);
        Path scenarioPath = temporaryDirectory.resolve("minimal.yaml");
        Path expectedPath = temporaryDirectory.resolve("minimal.expected.yaml");
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        yaml.writeValue(scenarioPath.toFile(), document);
        yaml.writeValue(
                expectedPath.toFile(),
                loader.loadExpectedResult(resource("minimal.expected.yaml")).document());
        ScenarioSpecification scenario = loader.loadScenario(scenarioPath);
        ExpectedResultSpecification expected = loader.loadExpectedResult(expectedPath);
        ResolvedScenarioPlan resolved = new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected), ResolutionRequest.none());
        ExecutableScenarioPlanCompiler compiler = new ExecutableScenarioPlanCompiler();
        ExecutableScenarioPlan executable = compiler.compile(resolved);
        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(temporaryDirectory));
        try {
            return new Fixture(prepared, compiler.bind(prepared, executable));
        } catch (RuntimeException failure) {
            prepared.close();
            throw failure;
        }
    }

    private V1AttemptContext attemptContext() {
        return new V1AttemptContext(
                1, "a1b2c3d4", temporaryDirectory.resolve("checkpoints"));
    }

    private static PreparedKafkaInput preparedInput() {
        return new PreparedKafkaInput(
                ENDPOINTS,
                inputManifest(
                        KafkaInputManifest.EvidenceStatus.COMPLETE,
                        10,
                        true,
                        KafkaInputManifest.TerminalDisposition.PRESENT));
    }

    private static KafkaInputManifest inputManifest(
            KafkaInputManifest.EvidenceStatus status,
            int observedRecords,
            boolean reachedEveryEnd,
            KafkaInputManifest.TerminalDisposition absentDisposition) {
        List<KafkaInputManifest.RecordAcknowledgement> records = new ArrayList<>();
        for (long id = 0; id < 10; id++) {
            records.add(new KafkaInputManifest.RecordAcknowledgement(
                    id,
                    "a".repeat(64),
                    List.of(new KafkaInputManifest.ProducerAttempt(
                            1,
                            0,
                            0,
                            id,
                            KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED)),
                    0,
                    id,
                    KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED,
                    id < observedRecords
                            ? KafkaInputManifest.TerminalDisposition.PRESENT
                            : absentDisposition));
        }
        List<Long> observedIds = new ArrayList<>();
        for (long id = 0; id < observedRecords; id++) {
            observedIds.add(id);
        }
        return new KafkaInputManifest(
                "main",
                "input",
                10,
                records,
                Map.of(0, 0L),
                Map.of(0, 10L),
                status,
                new KafkaInputManifest.ReconciliationEvidence(
                        Map.of(
                                "bootstrap.servers", "localhost:39092",
                                "enable.auto.commit", "false"),
                        observedIds,
                        reachedEveryEnd));
    }

    private static KafkaIdSetValidationResult passResult() {
        return result(
                KafkaIdSetValidationResult.Status.PASS,
                "validator.kafka.id-set.match",
                10,
                10,
                0);
    }

    private static KafkaIdSetValidationResult missingResult() {
        return result(
                KafkaIdSetValidationResult.Status.FAIL,
                "validator.kafka.id-set.missing-ids",
                9,
                9,
                1);
    }

    private static KafkaIdSetValidationResult result(
            KafkaIdSetValidationResult.Status status,
            String reason,
            long observed,
            long distinct,
            long missing) {
        return new KafkaIdSetValidationResult(
                status,
                reason,
                status == KafkaIdSetValidationResult.Status.PASS
                        ? "Kafka output exactly matches the input manifest"
                        : "Missing record IDs",
                new KafkaIdSetValidationResult.Evidence(
                        10,
                        observed,
                        Optional.of(new KafkaIdSetValidationResult.DefectTotals(
                                distinct, 0, 0, 0, missing)),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        missing == 0 ? List.of() : List.of(9L),
                        Map.of(0, 0L),
                        Map.of(0, observed),
                        true));
    }

    private static Path createJar(Path path, boolean executable, String protocol)
            throws IOException {
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (executable) {
            attributes.put(Attributes.Name.MAIN_CLASS, "example.Main");
        }
        if (protocol != null) {
            attributes.putValue(
                    ExecutableScenarioPlanCompiler.WORKLOAD_PROTOCOL_ATTRIBUTE, protocol);
        }
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            List<String> classes = new ArrayList<>(List.of("example/Main.class"));
            if (!executable) {
                // The protocol-v1 subject entry classes (SPEC-001 R5.6d).
                classes.add("org/apache/flink/connector/kafka/source/KafkaSource.class");
                classes.add("org/apache/flink/connector/kafka/sink/KafkaSink.class");
            }
            for (String entry : classes) {
                output.putNextEntry(new JarEntry(entry));
                output.write(new byte[] {0, 1, 2, 3});
                output.closeEntry();
            }
        }
        return path;
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private record Fixture(
            PreparedScenarioPlan owner,
            PreparedExecutableScenarioPlan bound) implements AutoCloseable {
        @Override
        public void close() {
            owner.close();
        }
    }

    private static final class FakeRuntime implements V1AttemptRuntime {
        private final List<String> events;
        /** The subject primary's container path, captured when Flink starts. */
        private String primarySource;
        /** Replaces the source that the fake class-load log reports, to model a mismatch. */
        private String loadedFrom;
        private RuntimeException classLoadLogsFailure;
        private boolean fenced;
        private Exception startFlinkFailure;
        private RuntimeException processFenceFailure;
        private RuntimeException closeFailure;
        private IOException killFailure;
        private CountDownLatch closeRelease;
        private final CountDownLatch closeEntered = new CountDownLatch(1);
        private final AtomicInteger closeCalls = new AtomicInteger();

        private FakeRuntime(List<String> events) {
            this.events = events;
        }

        @Override
        public KafkaRuntimeEndpoints startKafka(KafkaRuntimeTarget target) {
            events.add("kafka-start");
            assertEquals("main", target.clusterAlias());
            return ENDPOINTS;
        }

        @Override
        public String startFlink(FlinkRuntimeTarget target) throws Exception {
            events.add("flink-start");
            if (startFlinkFailure != null) {
                throw startFlinkFailure;
            }
            assertEquals("flink:2.2.0", target.imageReference());
            assertEquals(
                    target.imageReference(),
                    target.connectorBundle().targetFlinkImageReference());
            primarySource = target.connectorBundle().classpathManifest().entries()
                    .getFirst().containerPath();
            return "http://localhost:8081";
        }

        @Override
        public void killTaskManager(String targetName, Duration timeout) throws IOException {
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT, timeout);
            events.add("taskmanager-kill");
            if (killFailure != null) {
                throw killFailure;
            }
        }

        @Override
        public void restartTaskManager(Duration timeout) {
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT, timeout);
            events.add("taskmanager-restart");
        }

        @Override
        public FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout) {
            assertEquals(FlinkTerminalWriteFence.PROCESS_FENCE_TIMEOUT, timeout);
            events.add("process-fence");
            if (processFenceFailure != null) {
                throw processFenceFailure;
            }
            fenced = true;
            return new FlinkProcessWriteFenceEvidence(
                    List.of(), Instant.parse("2026-08-26T12:00:00Z"));
        }

        @Override
        public List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence() {
            return List.of();
        }

        /** One TaskManager log that loads both protocol-v1 entry classes. */
        @Override
        public List<FlinkClassLoadLog> flinkClassLoadLogs() {
            if (classLoadLogsFailure != null) {
                throw classLoadLogsFailure;
            }
            String source = loadedFrom != null ? loadedFrom : primarySource;
            try {
                Path log = Files.createTempFile("flink-stability-class-load-", ".log");
                log.toFile().deleteOnExit();
                Files.writeString(log, String.join("\n",
                        "[0.1s][info][class,load] java.lang.Object source: jrt:/java.base",
                        "[1.0s][info][class,load] "
                                + "org.apache.flink.connector.kafka.source.KafkaSource source: file:"
                                + source,
                        "[1.1s][info][class,load] "
                                + "org.apache.flink.connector.kafka.sink.KafkaSink source: file:"
                                + source));
                return List.of(new FlinkClassLoadLog("taskmanager-1#1", log));
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            events.add("runtime-close");
            closeEntered.countDown();
            if (closeRelease != null) {
                awaitUninterruptibly(closeRelease);
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        private static void awaitUninterruptibly(CountDownLatch latch) {
            boolean interrupted = false;
            while (true) {
                try {
                    latch.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class FakeFlink implements FlinkScenarioControl {
        private final List<String> events;
        /** Observations served in order; an empty queue serves a finished, undisturbed job. */
        private final java.util.Deque<FlinkJobObservation> observations =
                new java.util.ArrayDeque<>();
        private FlinkJobSubmission submission;
        private String uploadedJarSha256;
        private IOException awaitFinishedFailure;
        private Error uploadFatal;
        private RuntimeException closeFailure;

        private FakeFlink(List<String> events) {
            this.events = events;
        }

        @Override
        public String uploadJar(Path jar, String expectedSha256) {
            events.add("jar-upload");
            if (uploadFatal != null) {
                throw uploadFatal;
            }
            assertTrue(Files.isRegularFile(jar));
            uploadedJarSha256 = expectedSha256;
            return "workload.jar";
        }

        @Override
        public FlinkJobHandle submit(FlinkJobSubmission submission) {
            events.add("job-submit");
            this.submission = submission;
            return JOB;
        }

        @Override
        public FlinkJobState jobState(FlinkJobHandle job) {
            return FlinkJobState.FINISHED;
        }

        @Override
        public FlinkJobState awaitState(
                FlinkJobHandle job, FlinkJobState expected, Duration timeout) {
            events.add("await-running");
            return FlinkJobState.RUNNING;
        }

        @Override
        public long awaitCompletedCheckpoints(
                FlinkJobHandle job, long minimumCompleted, Duration timeout) {
            return minimumCompleted;
        }

        @Override
        public FlinkJobState awaitFinished(FlinkJobHandle job, Duration timeout)
                throws IOException {
            events.add("await-finished");
            if (awaitFinishedFailure != null) {
                throw awaitFinishedFailure;
            }
            return FlinkJobState.FINISHED;
        }

        @Override
        public FlinkJobObservation observe(FlinkJobHandle job) {
            events.add("observe-job");
            FlinkJobObservation next = observations.poll();
            return next != null ? next : job(10_000, FlinkJobState.FINISHED, 1, 0);
        }

        @Override
        public long jobManagerTimeMillis(FlinkJobHandle job) {
            events.add("post-kill-clock");
            return 1_500;
        }

        @Override
        public void close() {
            events.add("flink-close");
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
