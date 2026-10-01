package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.kafka.KafkaTransactionVersion;
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
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.KafkaProxyEndpoint;
import org.savonitar.flink.stability.runtime.api.KafkaProxyTarget;
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

    private V1ScenarioExecutor.TransactionVersionSelection featureSelection = new KafkaTransactionVersion()::select;

    private V1ScenarioExecutor.TransactionListing transactionListing =
            (bootstrapServers, prefix, timeout) -> new KafkaTransactionListing(
                    prefix,
                    List.of(new KafkaTransactionListing.Transaction(
                            prefix + "-0-1", "CompleteCommit", 7, 0, List.of())));

    @Test
    void kafkaCaptureDecodesAndRetainsReceiptsWithoutOverwritingEarlierEvidence() throws Exception {
        var events = new ArrayList<String>();
        try (Fixture fixture = fixture()) {
            var runtime = new FakeRuntime(events);
            runtime.captureArchive = true;
            Path componentLog = temporaryDirectory.resolve("combined-component.log");
            Files.writeString(componentLog, "2026-10-01 19:43:38,488 ERROR org.apache.kafka.Foo [] - ProducerFencedException\n");
            runtime.componentLogs = List.of(new org.savonitar.flink.stability.runtime.api.FlinkComponentLog(
                    "taskmanager-1#1", componentLog, false, Optional.empty()));
            Path output = temporaryDirectory.resolve("successful-capture");
            var result = executor(events, runtime, new FakeFlink(events),
                    (bootstrap, topic, ids, timeout) -> passResult())
                    .execute(fixture.bound(), attemptContext().withKafkaLogOutput(output));
            assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status());
            assertEquals("collected", result.kafkaLogs().status(), result.kafkaLogs().diagnostics().toString());
            assertEquals(1, result.componentErrors().events().size());
            for (var copy : List.of(result.withFlinkRestErrors(List.of()),
                    result.withCleanupFailure(new IOException("cleanup")),
                    result.withPreparedArtifactCleanupFailure(new IOException("cleanup")),
                    result.withComponentErrors(result.componentErrors()), result.withKafkaLogs(result.kafkaLogs()))) {
                assertEquals(result.componentErrors(), copy.componentErrors());
                assertEquals(result.kafkaLogs(), copy.kafkaLogs());
            }
            var decoded = result.kafkaLogs().decoded().getFirst();
            assertEquals("PARSED_AND_DECODED", decoded.status());
            byte[] retained = Files.readAllBytes(decoded.evidence().orElseThrow());
            assertEquals(org.savonitar.flink.stability.runtime.api.Digests.sha256(retained), decoded.sha256().orElseThrow());
            var retry = KafkaLogEvidence.collect(runtime, fixture.bound().executablePlan(), output);
            assertEquals("partial", retry.status());
            assertEquals(1, events.stream().filter("capture"::equals).count());
            assertArrayEquals(retained, Files.readAllBytes(decoded.evidence().orElseThrow()));
        }
    }

    @Test
    void optionalKafkaCollectionRunsAfterOracleBeforeCleanupAndNeverChangesVerdict() throws Exception {
        for (boolean enabled : List.of(false, true)) {
            for (boolean passes : List.of(false, true)) {
                List<String> events = new ArrayList<>();
                try (Fixture fixture = fixture()) {
                    FakeRuntime runtime = new FakeRuntime(events);
                    var context = attemptContext();
                    if (enabled) context = context.withKafkaLogOutput(
                            temporaryDirectory.resolve("capture-" + passes));
                    var result = executor(events, runtime, new FakeFlink(events), (bootstrap, topic, ids, timeout) -> {
                        events.add("oracle");
                        return passes ? passResult() : missingResult();
                    }).execute(fixture.bound(), context);
                    assertEquals(passes ? V1ScenarioExecutionResult.Status.PASS : V1ScenarioExecutionResult.Status.FAIL, result.status());
                    assertEquals(enabled ? "partial" : "not-requested", result.kafkaLogs().status());
                    assertEquals(enabled, events.contains("capture"));
                    if (enabled) {
                        assertTrue(events.indexOf("oracle") < events.indexOf("capture"));
                        assertTrue(events.indexOf("capture") < events.indexOf("runtime-close"));
                        assertEquals(result.kafkaLogs(), result.withCleanupFailure(new IOException("cleanup")).kafkaLogs());
                    }
                }
            }
        }
    }

    @Test
    void componentErrorsAreReadBeforeCleanupAndDoNotChangeDataVerdicts() throws Exception {
        for (boolean passing : List.of(true, false)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture()) {
                FakeRuntime runtime = new FakeRuntime(events);
                Path log = temporaryDirectory.resolve("component.log");
                Files.writeString(log, "2026-10-01 19:43:38,488 ERROR org.apache.kafka.Foo [] - ProducerFencedException producerId=7 epoch=1\n");
                runtime.componentLogs = List.of(new org.savonitar.flink.stability.runtime.api.FlinkComponentLog(
                        "taskmanager-1#1", log, true, Optional.of("capture truncated")));
                var result = executor(events, runtime, new FakeFlink(events),
                        (bootstrap, topic, ids, timeout) -> passing ? passResult() : missingResult())
                        .execute(fixture.bound(), attemptContext());
                assertEquals(passing ? V1ScenarioExecutionResult.Status.PASS : V1ScenarioExecutionResult.Status.FAIL,
                        result.status());
                assertEquals(1, result.componentErrors().events().size());
                assertFalse(result.componentErrors().diagnostics().isEmpty());
                for (var copy : List.of(result.withFlinkRestErrors(List.of()),
                        result.withCleanupFailure(new IOException("cleanup")),
                        result.withPreparedArtifactCleanupFailure(new IOException("cleanup")))) {
                    assertEquals(result.componentErrors(), copy.componentErrors());
                }
            }
        }
    }

    @Test
    void processHealthCannotHideDataFailureOrPermitCleanPassAndSurvivesCleanup() throws Exception {
        for (boolean oraclePasses : List.of(true, false)) {
            for (boolean exited : List.of(true, false)) {
                List<String> events = new ArrayList<>();
                try (Fixture fixture = fixture()) {
                    FakeRuntime runtime = new FakeRuntime(events);
                    runtime.unexpectedProcessExit = exited;
                    runtime.missingProcessObservations = !exited;
                    V1ScenarioExecutionResult result = executor(events, runtime, new FakeFlink(events),
                            (bootstrap, topic, ids, timeout) -> oraclePasses ? passResult() : missingResult())
                            .execute(fixture.bound(), attemptContext());
                    String processReason = exited ? FlinkProcessHealth.UNEXPECTED_EXIT : FlinkProcessHealth.UNCONFIRMED;
                    assertEquals(oraclePasses ? processReason : missingResult().reason(), result.reason());
                    assertEquals(oraclePasses && !exited ? V1ScenarioExecutionResult.Status.INCONCLUSIVE
                            : V1ScenarioExecutionResult.Status.FAIL, result.status());
                    assertEquals(oraclePasses ? passResult() : missingResult(), result.terminalValidation().orElseThrow());
                    assertEquals(result.processObservations(), result.withCleanupFailure(
                            new IOException("cleanup failed")).processObservations());
                    if (!oraclePasses) {
                        var verdict = ScenarioVerdict.of(ExecutableScenarioPlan.ExpectedOutcome.failure(
                                "kafka.id-set", missingResult().reason()), result);
                        assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
                        assertEquals(processReason, verdict.reason());
                    }
                }
            }
        }
    }

    @Test
    void requestedRuntimeProvenanceGatesBothPassingOraclesAndMatchingNegativeControls() throws Exception {
        for (boolean oraclePasses : List.of(false, true)) {
            for (String evidence : List.of("confirmed", "foreign", "missing", "aliased")) {
                List<String> events = new ArrayList<>();
                try (Fixture fixture = fixture(V1ScenarioExecutorTest::requestRuntimeJar)) {
                    FakeRuntime runtime = new FakeRuntime(events);
                    switch (evidence) {
                        case "foreign" -> runtime.runtimeLoadedFrom = "/opt/flink/lib/foreign-runtime.jar";
                        case "missing" -> runtime.provisioningOverride =
                                FlinkRuntimeIdentityTest.provisioning(1);
                        case "aliased" -> runtime.duplicateRuntimeLogPaths = true;
                        default -> { }
                    }
                    V1ScenarioExecutionResult result = executor(events, runtime, new FakeFlink(events),
                            (bootstrap, topic, ids, timeout) -> oraclePasses ? passResult() : missingResult())
                            .execute(fixture.bound(), attemptContext());
                    boolean confirmed = evidence.equals("confirmed");
                    String jarReason = evidence.equals("foreign")
                            ? FlinkRuntimeIdentity.RUNTIME_JAR_MISMATCH
                            : FlinkRuntimeIdentity.RUNTIME_JAR_UNCONFIRMED;

                    assertEquals(oraclePasses
                                    ? confirmed ? V1ScenarioExecutionResult.Status.PASS
                                            : V1ScenarioExecutionResult.Status.INCONCLUSIVE
                                    : V1ScenarioExecutionResult.Status.FAIL,
                            result.status(), evidence);
                    assertEquals(oraclePasses ? passResult() : missingResult(),
                            result.terminalValidation().orElseThrow(), evidence);
                    assertEquals(confirmed ? FlinkRuntimeIdentity.Outcome.CONFIRMED
                                    : evidence.equals("foreign") ? FlinkRuntimeIdentity.Outcome.MISMATCH
                                            : FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                            result.runtimeJarIdentity().orElseThrow().outcome(), evidence);
                    ScenarioVerdict verdict = ScenarioVerdict.of(oraclePasses
                            ? ExecutableScenarioPlan.ExpectedOutcome.pass()
                            : ExecutableScenarioPlan.ExpectedOutcome.failure(
                                    "kafka.id-set", missingResult().reason()), result);
                    assertEquals(confirmed ? ScenarioVerdict.Status.PASS : ScenarioVerdict.Status.INCONCLUSIVE,
                            verdict.status(), evidence);
                    if (!confirmed) {
                        assertEquals(jarReason, verdict.reason(), evidence);
                        assertEquals(oraclePasses ? jarReason : missingResult().reason(),
                                result.reason(), evidence);
                    }
                    assertEquals(result.runtimeClassOrigins(), result.withCleanupFailure(
                            new IOException("cleanup failed")).runtimeClassOrigins());
                    assertEquals(result.runtimeJarIdentity(), result.withCleanupFailure(
                            new IOException("cleanup failed")).runtimeJarIdentity());
                }
            }
        }
    }

    @Test
    void unconfirmedLeaderOperationStillFencesAndRunsOracleRetainingRawDataFailure() throws Exception {
        for (boolean oraclePasses : List.of(true, false)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document -> {
                ObjectNode flink = (ObjectNode) document.at("/setup/flink");
                flink.put("jobmanagers", 2);
                flink.putObject("high_availability").put("zookeeper_image", "zookeeper:3.9.3")
                        .put("session_timeout", "2s");
                ((ObjectNode) document.at("/workload/jobs/0/checkpointing"))
                        .putObject("storage").put("type", "filesystem");
                var steps = (com.fasterxml.jackson.databind.node.ArrayNode) document.at("/phases/0/steps");
                steps.removeAll();
                steps.addObject().putObject("leader_fault").put("mode", "pause")
                        .put("duration", "1ms").put("timeout", "2s");
            })) {
                FakeFlink flink = new FakeFlink(events);
                flink.observations.add(job(1_000, FlinkJobState.RUNNING, 1, 0));
                FakeRuntime runtime = new FakeRuntime(events);
                V1ScenarioExecutionResult result = executor(events, runtime, flink,
                        (bootstrap, topic, ids, timeout) -> {
                            events.add("oracle");
                            return oraclePasses ? passResult() : missingResult();
                        }).execute(fixture.bound(), attemptContext());

                assertTrue(events.indexOf("oracle") > events.indexOf("process-fence"));
                assertTrue(result.writeFenceEvidence().isPresent());
                assertTrue(result.terminalValidation().isPresent());
                var faults = result.phaseEvidence().orElseThrow().leaderFaults();
                assertEquals(1, faults.size());
                assertTrue(faults.getFirst().raw().errors().getFirst().contains("UnsupportedOperationException"));
                assertEquals(JOB.jobId(), faults.getFirst().jobId());
                assertEquals(1, result.expectedHa().faults().size());
                assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, result.haEvidence().outcome());
                assertEquals(oraclePasses ? V1ScenarioExecutionResult.Status.INCONCLUSIVE
                        : V1ScenarioExecutionResult.Status.FAIL, result.status());
                if (!oraclePasses) assertEquals(missingResult().reason(), result.reason());
                assertEquals(result.phaseEvidence(), result.withCleanupFailure(
                        new IOException("cleanup failed")).phaseEvidence());
            }
        }
    }

    @Test
    void requestedTokenEvidenceIsCheckedAfterOracleAndSurvivesEveryResultCopy() throws Exception {
        for (boolean oraclePasses : List.of(true, false)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document ->
                    ((ObjectNode) document.at("/setup/flink")).putObject("token_provider")
                            .put("renewal_interval", "2s"))) {
                FakeRuntime runtime = new FakeRuntime(events);
                V1ScenarioExecutionResult result = executor(events, runtime, new FakeFlink(events),
                        (bootstrap, topic, ids, timeout) -> {
                            events.add("oracle");
                            return oraclePasses ? passResult() : missingResult();
                        }).execute(fixture.bound(), attemptContext());

                assertTrue(events.indexOf("oracle") > events.indexOf("process-fence"));
                assertTrue(result.terminalValidation().isPresent());
                assertTrue(result.expectedHa().tokenProviderRequired());
                assertTrue(result.tokenEvidence().isPresent());
                assertTrue(result.tokenEvidence().orElseThrow().snapshot().isEmpty());
                assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, result.haEvidence().outcome());
                assertEquals(oraclePasses ? V1ScenarioExecutionResult.Status.INCONCLUSIVE
                        : V1ScenarioExecutionResult.Status.FAIL, result.status());
                assertEquals(oraclePasses ? "flink.ha.effect-unconfirmed"
                        : missingResult().reason(), result.reason());
                assertEquals(result.expectedHa(), result.withCleanupFailure(
                        new IOException("cleanup failed")).expectedHa());
                assertEquals(result.tokenEvidence(), result.withPreparedArtifactCleanupFailure(
                        new IOException("artifact cleanup failed")).tokenEvidence());
                if (!oraclePasses) {
                    assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, ScenarioVerdict.of(
                            new ExecutableScenarioPlan.ExpectedOutcome(
                                    ExecutableScenarioPlan.ExpectedOutcome.Outcome.FAIL,
                                    Optional.of("kafka-id-set"), Optional.of(missingResult().reason())),
                            result).status());
                }
                assertThrows(IllegalArgumentException.class, () -> new V1ScenarioExecutionResult(
                        V1ScenarioExecutionResult.Status.PASS, passResult().reason(), "forged pass",
                        result.inputManifest(), result.phaseEvidence(), result.writeFenceEvidence(),
                        result.processFenceEvidence(), result.finalJobObservation(), Optional.of(passResult()),
                        result.sinkTransactions(), result.subjectClassOrigins(), result.flinkProvisioningEvidence(),
                        result.expectedFlinkRuntime(), result.runtimeClassOrigins(), result.kafkaTransactionVersion(),
                        result.expectedHa(), result.tokenEvidence(), result.diagnostics()));
            }
        }
    }

    @Test
    void explicitTokenScopesBindActualSubmissionAndCaptureEveryLiveReceiverBeforeFencing() throws Exception {
        for (String scope : List.of("", "bootstrap", "submitted-job")) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document -> scopedTokenSetup(document, scope))) {
                var returnedJob = new FlinkJobHandle("1234567890abcdef1234567890abcdef");
                String alias = fixture.bound().executablePlan().job().alias();
                var trace = scopedTokenTrace(scope.equals("submitted-job"), returnedJob.jobId(), alias,
                        true);
                var postSubmit = tokenPrefix(trace, 3);
                FakeRuntime runtime = tokenRuntime(events, fixture, trace);
                if (!scope.isEmpty()) runtime.tokenSnapshots.add(Optional.of(postSubmit));
                FakeFlink flink = new FakeFlink(events);
                flink.submittedHandle = returnedJob;

                var result = executor(events, runtime, flink,
                        (bootstrap, topic, ids, timeout) -> passResult()).execute(fixture.bound(), attemptContext());

                assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status(), result.message());
                assertEquals(alias, flink.submission.flinkConfiguration().get("flink-stability.workload.v1.job-alias"));
                if (scope.isEmpty()) {
                    assertTrue(result.expectedHa().tokenProof().isEmpty());
                    assertEquals(1, events.stream().filter("token-snapshot"::equals).count());
                    assertTrue(events.indexOf("token-snapshot") > events.indexOf("process-fence"));
                    assertTrue(runtime.identityRequests.isEmpty(), "unscoped proof uses retained provisioning, without live scope capture");
                } else {
                    var proof = result.expectedHa().tokenProof().orElseThrow();
                    assertEquals(scope.equals("bootstrap") ? FlinkRuntimeTarget.TokenProofScope.BOOTSTRAP
                            : FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB, proof.scope());
                    var submitted = proof.submission().orElseThrow();
                    assertEquals(returnedJob.jobId(), submitted.jobId());
                    assertEquals(alias, submitted.jobAlias());
                    assertEquals(3, submitted.afterSequence());
                    assertEquals(postSubmit, submitted.snapshot());
                    assertEquals(List.of("taskmanager-1", "taskmanager-2"), runtime.identityRequests);
                    assertEquals(List.of("live-tm-1", "live-tm-2"), proof.receivers().stream()
                            .map(receiver -> receiver.identity().runtimeId()).toList());
                    assertEquals(List.of("taskmanager-1#1", "taskmanager-2#1"), proof.receivers().stream()
                            .map(TokenCheckpointBarrier.Receiver::classLoadProcess).toList());
                    assertTrue(events.indexOf("token-snapshot") > events.indexOf("job-submit"));
                    assertTrue(events.indexOf("token-snapshot") < events.indexOf("await-running"));
                    assertTrue(events.indexOf("identity:taskmanager-2") < events.indexOf("process-fence"));
                }
            }
        }
    }

    @Test
    void unscopedPartialTokenDeliveryCannotPassAndNeverMasksDataLoss() throws Exception {
        for (boolean oraclePasses : List.of(true, false)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document -> scopedTokenSetup(document, ""))) {
                var returnedJob = new FlinkJobHandle("1234567890abcdef1234567890abcdef");
                var trace = scopedTokenTrace(false, returnedJob.jobId(),
                        fixture.bound().executablePlan().job().alias(), false);
                FakeRuntime runtime = tokenRuntime(events, fixture, trace);
                FakeFlink flink = new FakeFlink(events);
                flink.submittedHandle = returnedJob;
                var oracle = oraclePasses ? passResult() : missingResult();
                var result = executor(events, runtime, flink,
                        (bootstrap, topic, ids, timeout) -> oracle).execute(fixture.bound(), attemptContext());
                assertEquals(oraclePasses ? V1ScenarioExecutionResult.Status.INCONCLUSIVE
                        : V1ScenarioExecutionResult.Status.FAIL, result.status());
                assertEquals(oraclePasses ? "flink.ha.effect-unconfirmed" : oracle.reason(), result.reason());
                assertEquals(oracle, result.terminalValidation().orElseThrow());
                assertTrue(result.expectedHa().tokenProof().isEmpty());
                assertTrue(runtime.identityRequests.isEmpty());
            }
        }
    }

    @Test
    void submittedJobProofRejectsWrongIdentityStaleRequestsMissingReceiptsAndRewrittenTrace() throws Exception {
        for (String invalid : List.of("job", "alias", "bootstrap", "in-flight", "missing-tm", "rewritten-prefix")) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document -> scopedTokenSetup(document, "submitted-job"))) {
                var returnedJob = new FlinkJobHandle("1234567890abcdef1234567890abcdef");
                String alias = fixture.bound().executablePlan().job().alias();
                var trace = scopedTokenTrace(!invalid.equals("bootstrap"),
                        invalid.equals("job") ? "ffffffffffffffffffffffffffffffff" : returnedJob.jobId(),
                        invalid.equals("alias") ? "provider-claimed-alias" : alias, !invalid.equals("missing-tm"));
                var postSubmit = tokenPrefix(trace, invalid.equals("in-flight") ? 4 : 3);
                if (invalid.equals("rewritten-prefix")) {
                    var changed = new ArrayList<>(trace.events());
                    var old = changed.getFirst();
                    changed.set(0, new TokenServiceControl.Event(old.sequence(), old.kind(), old.process(), old.role(),
                            old.timestampMillis(), old.monotonicNanos(), old.requestId(), old.revision(), old.mode(),
                            old.tokenSequence(), "rewritten observed initialization", old.registration(), old.participantInstance()));
                    trace = new TokenServiceControl.Snapshot(changed, false, false, 0, 1);
                }
                FakeRuntime runtime = tokenRuntime(events, fixture, trace);
                runtime.tokenSnapshots.add(Optional.of(postSubmit));
                FakeFlink flink = new FakeFlink(events);
                flink.submittedHandle = returnedJob;

                var result = executor(events, runtime, flink,
                        (bootstrap, topic, ids, timeout) -> passResult()).execute(fixture.bound(), attemptContext());

                assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status(), invalid);
                assertEquals("flink.ha.effect-unconfirmed", result.reason(), invalid);
                assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, result.haEvidence().outcome(), invalid);
                assertEquals(returnedJob.jobId(), result.expectedHa().tokenProof().orElseThrow()
                        .submission().orElseThrow().jobId(), "provider evidence cannot choose the expected job");
                assertEquals(Optional.of(trace), result.tokenEvidence().orElseThrow().snapshot());
                assertTrue(result.terminalValidation().isPresent(), invalid);
                assertTrue(runtime.fenced, invalid);
            }
        }
    }

    @Test
    void missingScopedSubmissionOrLiveReceiverProofCannotOverwriteADataFailure() throws Exception {
        for (String scope : List.of("bootstrap", "submitted-job")) {
            for (boolean missingSubmission : List.of(true, false)) {
                for (boolean oraclePasses : List.of(true, false)) {
                    List<String> events = new ArrayList<>();
                    try (Fixture fixture = fixture(document -> scopedTokenSetup(document, scope))) {
                        var returnedJob = new FlinkJobHandle("1234567890abcdef1234567890abcdef");
                        var trace = scopedTokenTrace(scope.equals("submitted-job"), returnedJob.jobId(),
                                fixture.bound().executablePlan().job().alias(), true);
                        FakeRuntime runtime = tokenRuntime(events, fixture, trace);
                        runtime.tokenSnapshots.add(missingSubmission ? Optional.empty() : Optional.of(tokenPrefix(trace, 3)));
                        if (!missingSubmission) runtime.identitiesOverride.remove("taskmanager-2");
                        FakeFlink flink = new FakeFlink(events);
                        flink.submittedHandle = returnedJob;

                        var result = executor(events, runtime, flink,
                                (bootstrap, topic, ids, timeout) -> oraclePasses ? passResult() : missingResult())
                                .execute(fixture.bound(), attemptContext());

                        assertEquals(oraclePasses ? V1ScenarioExecutionResult.Status.INCONCLUSIVE
                                : V1ScenarioExecutionResult.Status.FAIL, result.status());
                        assertEquals(oraclePasses ? "flink.ha.effect-unconfirmed" : missingResult().reason(), result.reason());
                        var proof = result.expectedHa().tokenProof().orElseThrow();
                        assertEquals(missingSubmission, proof.submission().isEmpty());
                        if (!missingSubmission) assertTrue(proof.receivers().isEmpty());
                        assertEquals(Optional.of(trace), result.tokenEvidence().orElseThrow().snapshot());
                        assertTrue(result.terminalValidation().isPresent());
                        assertTrue(runtime.fenced);
                        assertEquals(result.expectedHa(), result.withCleanupFailure(new IOException("cleanup")).expectedHa());
                    }
                }
            }
        }
    }

    private static void scopedTokenSetup(ObjectNode document, String scope) {
        var flink = (ObjectNode) document.at("/setup/flink");
        flink.put("taskmanagers", 2);
        var provider = flink.putObject("token_provider").put("renewal_interval", "2s");
        if (!scope.isEmpty()) provider.put("proof_scope", scope);
        ((ObjectNode) document.at("/workload/jobs/0")).put("alias", "compiled-token-job");
        ((ObjectNode) document.at("/phases/0/steps/0/await/condition")).put("job", "compiled-token-job");
    }

    private FakeRuntime tokenRuntime(List<String> events, Fixture fixture, TokenServiceControl.Snapshot trace)
            throws IOException {
        FakeRuntime runtime = new FakeRuntime(events);
        runtime.tokenSnapshot = Optional.of(trace);
        runtime.pluginSha256 = Optional.of("d".repeat(64));
        runtime.provisioningOverride = List.of(
                FlinkRuntimeIdentityTest.component("jobmanager-1", "jm", FlinkRuntimeIdentityTest.IMAGE_ID),
                FlinkRuntimeIdentityTest.component("taskmanager-1", "live-tm-1", FlinkRuntimeIdentityTest.IMAGE_ID),
                FlinkRuntimeIdentityTest.component("taskmanager-2", "live-tm-2", FlinkRuntimeIdentityTest.IMAGE_ID));
        runtime.identitiesOverride = new java.util.HashMap<>();
        runtime.classLoadLogsOverride = new ArrayList<>();
        String connector = fixture.bound().connectorBundle().entries().getFirst().containerPath();
        for (var component : runtime.provisioningOverride) {
            if (component.logicalName().startsWith("taskmanager-")) runtime.identitiesOverride.put(component.logicalName(),
                    new TaskManagerControl.Identity(component.logicalName(), component.runtimeId(), "resource-" + component.runtimeId()));
            Path log = Files.createTempFile(temporaryDirectory, "scoped-token-origin-", ".log");
            List<String> lines = new ArrayList<>();
            for (String type : FlinkHaEvidence.TOKEN_CLASSES) lines.add("[1s][info][class,load] " + type
                    + " source: file:" + FlinkHaEvidence.TOKEN_CONTAINER_PATH);
            for (String type : ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES) lines.add(
                    "[1s][info][class,load] " + type + " source: file:" + connector);
            Files.writeString(log, String.join("\n", lines));
            runtime.classLoadLogsOverride.add(new FlinkClassLoadLog(component.logicalName() + "#1", log));
        }
        runtime.fenceOverride = new FlinkProcessWriteFenceEvidence(runtime.provisioningOverride.stream().map(component ->
                new FlinkProcessWriteFenceEvidence.Component(component.logicalName(), component.role(),
                        Optional.of(component.runtimeId()), FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED)).toList(), Instant.EPOCH);
        return runtime;
    }

    private static TokenServiceControl.Snapshot scopedTokenTrace(boolean job, String jobId, String alias, boolean allReceivers) {
        String provider = "00000000-0000-0000-0000-000000000001";
        var context = new TokenServiceControl.RegistrationSnapshot(provider, job ? "JOB" : "BOOTSTRAP", job ? 1 : 0,
                job ? jobId : "-", job ? alias : "-", false, 0, job
                ? List.of(new TokenServiceControl.Lifecycle(1, "REGISTER", 1, jobId, alias)) : List.of());
        List<TokenServiceControl.Event> trace = new ArrayList<>();
        trace.add(tokenEvent(1, TokenServiceControl.Kind.PROVIDER_INITIALIZED, "jobmanager-1#1", 0,
                OptionalLong.empty(), Optional.empty(), provider));
        for (int index = 1; index <= 2; index++) trace.add(tokenEvent(trace.size() + 1,
                TokenServiceControl.Kind.RECEIVER_INITIALIZED, "taskmanager-" + index + "#1", 0,
                OptionalLong.empty(), Optional.empty(), "receiver-" + index));
        for (var kind : List.of(TokenServiceControl.Kind.REQUEST_STARTED, TokenServiceControl.Kind.ISSUED,
                TokenServiceControl.Kind.REQUEST_FINISHED)) trace.add(tokenEvent(trace.size() + 1, kind,
                "jobmanager-1#1", 1, kind == TokenServiceControl.Kind.REQUEST_STARTED
                        ? OptionalLong.empty() : OptionalLong.of(1), Optional.of(context), provider));
        for (int index = 1; index <= (allReceivers ? 2 : 1); index++) trace.add(tokenEvent(trace.size() + 1,
                TokenServiceControl.Kind.RECEIVED, "taskmanager-" + index + "#1", 0,
                OptionalLong.of(1), Optional.of(context), "receiver-" + index));
        return new TokenServiceControl.Snapshot(trace, false, false, 0, 1);
    }

    private static TokenServiceControl.Event tokenEvent(long sequence, TokenServiceControl.Kind kind, String process,
            long request, OptionalLong token, Optional<TokenServiceControl.RegistrationSnapshot> registration, String participant) {
        return new TokenServiceControl.Event(sequence, kind, process,
                process.startsWith("jobmanager-") ? "jobmanager" : "taskmanager", sequence, sequence, request, 0,
                TokenServiceControl.Mode.HEALTHY, token, "fixture", registration, Optional.of(participant));
    }

    private static TokenServiceControl.Snapshot tokenPrefix(TokenServiceControl.Snapshot trace, int size) {
        return new TokenServiceControl.Snapshot(trace.events().subList(0, size), false, false, size == 4 ? 1 : 0, size == 4 ? 1 : 0);
    }

    @Test
    void requestedFeatureIsVerifiedBeforeInputAndFlinkAndSurvivesCleanupFailure() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(document ->
                ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("transaction_version", 1))) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.closeFailure = new IllegalStateException("close failed");
            var selected = new KafkaTransactionVersion.Selection(Optional.of(1), List.of(
                    new KafkaTransactionVersion.Observation(
                            Optional.of(new KafkaTransactionVersion.Range((short) 1, (short) 1)),
                            Optional.of(new KafkaTransactionVersion.Range((short) 0, (short) 2)),
                            OptionalLong.of(7))), Optional.empty());
            featureSelection = (bootstrap, requested) -> {
                events.add("feature-selection");
                assertEquals("localhost:39092", bootstrap);
                assertEquals(Optional.of(1), requested);
                return selected;
            };
            V1ScenarioExecutionResult result = executor(events, runtime, new FakeFlink(events),
                    (bootstrap, topic, ids, timeout) -> passResult()).execute(fixture.bound(), attemptContext());
            assertEquals("infrastructure.attempt-cleanup-failed", result.reason());
            assertEquals(selected, result.kafkaTransactionVersion());
            assertTrue(events.indexOf("kafka-start") < events.indexOf("feature-selection"));
            assertTrue(events.indexOf("feature-selection") < events.indexOf("input-prepare"));
            assertTrue(events.indexOf("feature-selection") < events.indexOf("flink-open"));
        }
    }

    @Test
    void unconfirmedFeatureStopsBeforeInputOrFlinkWithOriginalEvidence() throws Exception {
        for (boolean wrongRequest : List.of(false, true)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document ->
                    ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("transaction_version", 1))) {
                var selected = new KafkaTransactionVersion.Selection(Optional.of(1), List.of(
                        new KafkaTransactionVersion.Observation(
                                Optional.of(new KafkaTransactionVersion.Range((short) 2, (short) 2)),
                                Optional.of(new KafkaTransactionVersion.Range((short) 0, (short) 2)),
                                OptionalLong.of(5))), Optional.of("safe downgrade rejected"));
                featureSelection = (bootstrap, requested) -> wrongRequest
                        ? KafkaTransactionVersion.Selection.notRequested() : selected;
                V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events),
                        new FakeFlink(events), (bootstrap, topic, ids, timeout) -> passResult())
                        .execute(fixture.bound(), attemptContext());
                assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
                assertEquals(KafkaTransactionVersion.UNCONFIRMED, result.reason());
                assertEquals(Optional.of(1), result.kafkaTransactionVersion().requested());
                assertFalse(result.kafkaTransactionVersion().permitsPass());
                if (!wrongRequest) {
                    assertEquals(selected, result.kafkaTransactionVersion());
                }
                assertFalse(events.contains("input-prepare"));
                assertFalse(events.contains("flink-open"));
                assertTrue(events.contains("runtime-close"));
                assertTrue(result.inputManifest().isEmpty());
            }
        }
    }

    @Test
    void aPassResultCannotOmitProcessHealthThroughTheCompatibilityConstructor() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::requestRuntimeJar)) {
            V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events),
                    new FakeFlink(events), (bootstrap, topic, ids, timeout) -> passResult())
                    .execute(fixture.bound(), attemptContext());
            assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status());

            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> new V1ScenarioExecutionResult(result.status(), result.reason(), result.message(),
                            result.inputManifest(), result.phaseEvidence(), result.writeFenceEvidence(),
                            result.processFenceEvidence(), result.finalJobObservation(), result.terminalValidation(),
                            result.sinkTransactions(), result.subjectClassOrigins(), result.flinkProvisioningEvidence(),
                            result.expectedFlinkRuntime(), result.runtimeClassOrigins(), result.kafkaTransactionVersion(),
                            result.expectedHa(), result.tokenEvidence(), result.diagnostics()));

            assertTrue(failure.getMessage().contains("pre-fence process health"), failure.getMessage());
        }
    }

    @Test
    void recoveredRestErrorsSurviveEveryAttemptExitAndBothCleanupCopies() throws Exception {
        for (String failureAt : List.of("success", "phase", "fence")) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document -> {
                requestRuntimeJar(document);
                ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("transaction_version", 1);
            })) {
                var selected = new KafkaTransactionVersion.Selection(Optional.of(1), List.of(
                        new KafkaTransactionVersion.Observation(
                                Optional.of(new KafkaTransactionVersion.Range((short) 1, (short) 1)),
                                Optional.of(new KafkaTransactionVersion.Range((short) 0, (short) 2)),
                                OptionalLong.of(7))), Optional.empty());
                featureSelection = (bootstrap, requested) -> selected;
                FakeFlink flink = new FakeFlink(events);
                String body = "{\"errors\":[\"NullArgumentException: input array\"]}" + "x".repeat(5000);
                var error = new FlinkScenarioControl.RestError(1, "GET", "/jobs/job-1/checkpoints", 500, body);
                flink.httpErrors.add(error);
                flink.closeFailure = new IllegalStateException("close failed after snapshot");
                if (failureAt.equals("phase")) {
                    flink.awaitStateFailure = new IOException("state query failed");
                } else if (failureAt.equals("fence")) {
                    flink.awaitFinishedFailure = new IOException("terminal state query failed");
                }
                V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events), flink,
                        (bootstrap, topic, ids, timeout) -> passResult()).execute(fixture.bound(), attemptContext());
                assertEquals(List.of(error), result.flinkRestErrors(), failureAt);
                assertTrue(flink.httpErrors.isEmpty(), "the client was closed after evidence was copied");
                assertEquals(body, result.flinkRestErrors().getFirst().body());
                if (failureAt.equals("success")) {
                    assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                            result.runtimeJarIdentity().orElseThrow().outcome());
                }
                for (V1ScenarioExecutionResult copy : List.of(result,
                        result.withCleanupFailure(new IOException("runtime close failed")),
                        result.withPreparedArtifactCleanupFailure(new IOException("artifact close failed")))) {
                    assertEquals(selected, copy.kafkaTransactionVersion(), failureAt);
                    assertEquals(List.of(error), copy.flinkRestErrors(), failureAt);
                    assertEquals(result.runtimeClassOrigins(), copy.runtimeClassOrigins(), failureAt);
                    assertEquals(result.runtimeJarIdentity(), copy.runtimeJarIdentity(), failureAt);
                    assertEquals(result.expectedHa(), copy.expectedHa(), failureAt);
                    assertEquals(result.tokenEvidence(), copy.tokenEvidence(), failureAt);
                    assertEquals(result.haObservations(), copy.haObservations(), failureAt);
                    assertEquals(result.processObservations(), copy.processObservations(), failureAt);
                }
                assertThrows(UnsupportedOperationException.class, () -> result.flinkRestErrors().clear());
            }
        }
    }

    @Test
    void restEvidenceCopiesRetainNonemptyHaTokenAndProcessEvidenceIncludingDataFailure() throws Exception {
        for (boolean oraclePasses : List.of(true, false)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document -> {
                ObjectNode flink = (ObjectNode) document.at("/setup/flink");
                flink.put("jobmanagers", 2);
                flink.putObject("high_availability").put("zookeeper_image", "zookeeper:3.9.3")
                        .put("session_timeout", "2s");
                flink.putObject("token_provider").put("renewal_interval", "2s");
                ((ObjectNode) document.at("/workload/jobs/0/checkpointing"))
                        .putObject("storage").put("type", "filesystem");
            })) {
                FakeRuntime runtime = new FakeRuntime(events);
                var tokens = new TokenServiceControl.Snapshot(List.of(new TokenServiceControl.Event(
                        1, TokenServiceControl.Kind.PROVIDER_INITIALIZED, "jobmanager-1#1", "jobmanager",
                        10, 10, 0, 0, TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), "initialized")),
                        false, false, 0, 0);
                var history = new FlinkHaControl.Observations(List.of(new FlinkHaControl.LeadershipObservation(
                        1, 1, 10, 10, FlinkHaControl.ObservationMoment.INITIAL,
                        Optional.empty(), Optional.of("leader sample unavailable"))), List.of(), false);
                runtime.tokenSnapshot = Optional.of(tokens);
                runtime.haHistory = Optional.of(history);
                FakeFlink flink = new FakeFlink(events);
                var error = new FlinkScenarioControl.RestError(
                        1, "GET", "/jobs/job-1/checkpoints", 503, "original HA read failure");
                flink.httpErrors.add(error);

                V1ScenarioExecutionResult result = executor(events, runtime, flink,
                        (bootstrap, topic, ids, timeout) -> oraclePasses ? passResult() : missingResult())
                        .execute(fixture.bound(), attemptContext());

                assertTrue(result.expectedHa().haRequired());
                assertTrue(result.expectedHa().tokenProviderRequired());
                assertEquals(Optional.of(tokens), result.tokenEvidence().orElseThrow().snapshot());
                assertEquals(Optional.of(history), result.haObservations());
                assertFalse(result.processObservations().orElseThrow().observations().isEmpty());
                assertEquals(List.of(error), result.flinkRestErrors());
                assertEquals(oraclePasses ? V1ScenarioExecutionResult.Status.INCONCLUSIVE
                        : V1ScenarioExecutionResult.Status.FAIL, result.status());
                if (!oraclePasses) assertEquals(missingResult().reason(), result.reason());

                var replacementErrors = List.of(new FlinkScenarioControl.RestError(
                        1, "POST", "/jobs/job-1/checkpoints", 503, "checkpoint submission unknown"));
                var copied = result.withFlinkRestErrors(replacementErrors);
                for (var retained : List.of(copied,
                        copied.withCleanupFailure(new IOException("runtime cleanup failed")),
                        copied.withPreparedArtifactCleanupFailure(new IOException("artifact cleanup failed")))) {
                    assertEquals(replacementErrors, retained.flinkRestErrors());
                    assertEquals(result.expectedHa(), retained.expectedHa());
                    assertEquals(result.tokenEvidence(), retained.tokenEvidence());
                    assertEquals(result.haObservations(), retained.haObservations());
                    assertEquals(result.processObservations(), retained.processObservations());
                    assertEquals(result.phaseEvidence(), retained.phaseEvidence());
                    assertEquals(result.terminalValidation(), retained.terminalValidation());
                    assertEquals(result.status(), retained.status());
                    assertEquals(result.reason(), retained.reason());
                }
                assertEquals(List.of(error), result.flinkRestErrors(), "copying cannot mutate the original evidence");
            }
        }
    }

    @Test
    void aPassResultCannotOmitRequestedRuntimeClassEvidenceWithHealthyProcesses() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::requestRuntimeJar)) {
            V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events),
                    new FakeFlink(events), (bootstrap, topic, ids, timeout) -> passResult())
                    .execute(fixture.bound(), attemptContext());
            assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status());

            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> new V1ScenarioExecutionResult(result.status(), result.reason(), result.message(),
                            result.inputManifest(), result.phaseEvidence(), result.writeFenceEvidence(),
                            result.processFenceEvidence(), result.finalJobObservation(), result.terminalValidation(),
                            result.sinkTransactions(), result.subjectClassOrigins(), result.flinkProvisioningEvidence(),
                            result.expectedFlinkRuntime(), Optional.empty(), result.kafkaTransactionVersion(),
                            result.expectedHa(), result.tokenEvidence(), result.haObservations(),
                            result.processObservations(), result.diagnostics()));

            assertTrue(failure.getMessage().contains("runtime JAR provenance"), failure.getMessage());
        }
    }

    private static void requestRuntimeJar(ObjectNode document) {
        ((ObjectNode) document.at("/setup/flink")).putObject("runtime_jar")
                .put("container_path", RuntimeJarIdentityTest.JAR.containerPath())
                .put("sha256", RuntimeJarIdentityTest.JAR.sha256());
    }

    @Test
    void aPassingOracleRequiresCompleteAndConsistentRuntimeImageEvidence() throws Exception {
        for (String gap : List.of("missing", "mixed", "missing-fenced-process", "unavailable")) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture()) {
                FakeRuntime runtime = new FakeRuntime(events);
                if (gap.equals("unavailable")) {
                    runtime.provisioningFailure = new IllegalStateException("inspection unavailable");
                } else {
                    runtime.provisioningOverride = switch (gap) {
                        case "missing" -> List.of();
                        case "mixed" -> List.of(
                                FlinkRuntimeIdentityTest.component("jobmanager-1", "jm",
                                        FlinkRuntimeIdentityTest.IMAGE_ID),
                                FlinkRuntimeIdentityTest.component("taskmanager-1", "tm-1",
                                        FlinkRuntimeIdentityTest.OTHER_IMAGE_ID));
                        default -> List.of(
                                FlinkRuntimeIdentityTest.component("jobmanager-1", "jm",
                                        FlinkRuntimeIdentityTest.IMAGE_ID),
                                FlinkRuntimeIdentityTest.component("taskmanager-1", "another-tm",
                                        FlinkRuntimeIdentityTest.IMAGE_ID));
                    };
                }
                V1ScenarioExecutionResult result = executor(events, runtime, new FakeFlink(events),
                        (bootstrap, topic, ids, timeout) -> passResult())
                        .execute(fixture.bound(), attemptContext());

                assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status(), gap);
                assertEquals(gap.equals("mixed") ? FlinkRuntimeIdentity.MISMATCH
                        : FlinkRuntimeIdentity.UNCONFIRMED, result.reason(), gap);
                assertTrue(result.terminalValidation().isPresent(), gap);
                assertEquals(KafkaIdSetValidationResult.Status.PASS,
                        result.terminalValidation().orElseThrow().status(), gap);
            }
        }
    }

    @Test
    void aDeclaredPinIsCheckedAndRetainedByCleanupFailures() throws Exception {
        for (String expected : List.of(FlinkRuntimeIdentityTest.IMAGE_ID,
                FlinkRuntimeIdentityTest.OTHER_IMAGE_ID)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(document -> ((ObjectNode) document.at("/setup/flink"))
                    .put("image_id", expected))) {
                V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events),
                        new FakeFlink(events), (bootstrap, topic, ids, timeout) -> passResult())
                        .execute(fixture.bound(), attemptContext());
                assertEquals(expected.equals(FlinkRuntimeIdentityTest.IMAGE_ID)
                        ? V1ScenarioExecutionResult.Status.PASS
                        : V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
                assertEquals(Optional.of(expected), result.withCleanupFailure(
                        new IllegalStateException("cleanup failed")).expectedFlinkRuntime().imageId());
                assertEquals(fixture.bound().executablePlan().flink().expectedComponents(),
                        result.expectedFlinkRuntime().components());
                assertEquals(result.expectedFlinkRuntime(), result.withCleanupFailure(
                        new IllegalStateException("cleanup failed")).expectedFlinkRuntime());
            }
        }
    }

    @Test
    void wrappedPrestartImageMismatchKeepsExpectedAndObservedIds() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture()) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.startFlinkFailure = new IllegalStateException("Container failed to start",
                    new IllegalStateException("Expected " + FlinkRuntimeIdentityTest.IMAGE_ID
                            + ", actual " + FlinkRuntimeIdentityTest.OTHER_IMAGE_ID));
            V1ScenarioExecutionResult result = executor(events, runtime, new FakeFlink(events),
                    (bootstrap, topic, ids, timeout) -> passResult())
                    .execute(fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertTrue(result.diagnostics().stream().anyMatch(detail ->
                    detail.contains(FlinkRuntimeIdentityTest.IMAGE_ID)
                            && detail.contains(FlinkRuntimeIdentityTest.OTHER_IMAGE_ID)));
        }
    }

    @Test
    void declaredImageMismatchRetainsDataFailureButCannotValidateANegativeControl() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(document -> ((ObjectNode) document.at("/setup/flink"))
                .put("image_id", FlinkRuntimeIdentityTest.OTHER_IMAGE_ID))) {
            V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events),
                    new FakeFlink(events), (bootstrap, topic, ids, timeout) -> missingResult())
                    .execute(fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.FAIL, result.status());
            assertEquals(missingResult().reason(), result.reason());
            assertEquals(Optional.of(FlinkRuntimeIdentityTest.OTHER_IMAGE_ID),
                    result.expectedFlinkRuntime().imageId());
            ScenarioVerdict verdict = ScenarioVerdict.of(
                    ExecutableScenarioPlan.ExpectedOutcome.failure("kafka.id-set", result.reason()),
                    result);
            assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
            assertEquals(FlinkRuntimeIdentity.MISMATCH, verdict.reason());
        }
    }

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
                    1_000, FlinkJobState.RUNNING, 0, 0,
                    Optional.empty(), List.of(),
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
                        1_000, FlinkJobState.RUNNING, 2, 0,
                        Optional.empty(), List.of(),
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
                    ExecutableScenarioPlan.ExpectedOutcome.failure("kafka.id-set", result.reason()),
                    result);
            assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_KILL_EFFECT_UNCONFIRMED, verdict.reason());
        }
    }

    @Test
    void matchingFailureWithUnconfirmedSubjectKeepsDataFailureButCannotPass() throws Exception {
        for (boolean unreadable : List.of(false, true)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture()) {
                FakeRuntime runtime = new FakeRuntime(events);
                if (unreadable) {
                    runtime.classLoadLogsFailure = new IllegalStateException("missing process log");
                } else {
                    runtime.loadedFrom = "/opt/flink/lib/foreign-connector.jar";
                }
                V1ScenarioExecutionResult result = executor(events, runtime, new FakeFlink(events),
                        (bootstrap, topic, ids, timeout) -> missingResult())
                        .execute(fixture.bound(), attemptContext());

                ScenarioVerdict verdict = ScenarioVerdict.of(
                        ExecutableScenarioPlan.ExpectedOutcome.failure("kafka.id-set", result.reason()),
                        result);

                assertEquals(V1ScenarioExecutionResult.Status.FAIL, result.status());
                assertEquals(missingResult().reason(), result.reason());
                assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
                assertEquals(unreadable ? V1ScenarioExecutor.SUBJECT_ORIGIN_UNCONFIRMED
                        : V1ScenarioExecutor.SUBJECT_ORIGIN_MISMATCH, verdict.reason());
            }
        }
    }

    @Test
    void missingTargetIdentityKeepsTheKillUnconfirmedWithoutPreventingRecoveryOrFence()
            throws Exception {
        missingTargetIdentityKeepsTheKillUnconfirmed(null);
    }

    @Test
    void identityLookupTimeoutRetainsItsCauseWithoutPreventingRecoveryOrFence() throws Exception {
        missingTargetIdentityKeepsTheKillUnconfirmed(new IllegalStateException(
                "identity lookup failed", new java.util.concurrent.TimeoutException("Docker inspect deadline")));
    }

    private void missingTargetIdentityKeepsTheKillUnconfirmed(RuntimeException identityFailure)
            throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::killAndRestart)) {
            FakeRuntime runtime = new FakeRuntime(events);
            runtime.identityUnavailable = true;
            runtime.identityFailure = identityFailure;
            FakeFlink flink = new FakeFlink(events);
            flink.observations.add(new FlinkJobObservation(
                    1_000, FlinkJobState.RUNNING, 2, 0, Optional.empty(), List.of(),
                    List.of(new FlinkJobObservation.Subtask("Kafka Source", 0, 0,
                            "RUNNING", Optional.of("tm-1")))));
            flink.observations.add(new FlinkJobObservation(
                    9_000, FlinkJobState.FINISHED, 6, 1,
                    Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                    List.of(new FlinkJobObservation.Failure(4_000,
                            "ResourceManagerException", "lost tm-1", Optional.of("tm-1"))), List.of()));
            V1ScenarioExecutionResult result = executor(events, runtime, flink,
                    (bootstrap, topic, ids, timeout) -> passResult()).execute(fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_KILL_EFFECT_UNCONFIRMED, result.reason());
            assertEquals(TaskManagerKillEffect.Outcome.EVIDENCE_UNAVAILABLE,
                    result.taskManagerKillEffects().getFirst().outcome());
            assertTrue(events.contains("taskmanager-restart"));
            assertTrue(events.contains("process-fence"));
            assertTrue(result.terminalValidation().isPresent());
            var phases = result.phaseEvidence().orElseThrow();
            assertTrue(phases.taskManagerKills().getFirst().identity().isEmpty());
            assertTrue(phases.taskManagerRestarts().getFirst().previousIdentity().isEmpty());
            assertTrue(phases.taskManagerRestarts().getFirst().replacementIdentity().isEmpty());
            Optional<String> expectedFailure = identityFailure == null ? Optional.empty()
                    : Optional.of("IllegalStateException: identity lookup failed; caused by "
                            + "TimeoutException: Docker inspect deadline");
            assertEquals(expectedFailure, phases.taskManagerKills().getFirst().identityFailure());
            assertEquals(expectedFailure,
                    phases.taskManagerRestarts().getFirst().previousIdentityFailure());
            assertEquals(expectedFailure,
                    phases.taskManagerRestarts().getFirst().replacementIdentityFailure());
        }
    }

    @Test
    void aPassResultCannotCarryAnUnconfirmedKill() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::killAndRestart)) {
            FakeFlink flink = new FakeFlink(events);
            flink.observations.add(job(1_000, FlinkJobState.FINISHED, 2, 0));
            flink.observations.add(job(9_000, FlinkJobState.FINISHED, 2, 0));
            V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events), flink,
                    (bootstrap, topic, count, timeout) -> passResult())
                    .execute(fixture.bound(), attemptContext());
            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_KILL_EFFECT_UNCONFIRMED, result.reason());
            assertEquals(FlinkProcessHealth.Outcome.HEALTHY, result.processHealth().outcome());
            assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED, result.flinkRuntimeIdentity().outcome());
            assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, result.haEvidence().outcome());

            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> new V1ScenarioExecutionResult(V1ScenarioExecutionResult.Status.PASS,
                            "validator.kafka.id-set.match", "passed", result.inputManifest(),
                            result.phaseEvidence(), result.writeFenceEvidence(), result.processFenceEvidence(),
                            result.finalJobObservation(), result.terminalValidation(), result.sinkTransactions(),
                            result.subjectClassOrigins(), result.flinkProvisioningEvidence(), result.expectedFlinkRuntime(),
                            result.runtimeClassOrigins(), result.kafkaTransactionVersion(), result.expectedHa(),
                            result.tokenEvidence(), result.haObservations(), result.processObservations(),
                            result.diagnostics()));

            assertTrue(failure.getMessage().contains("confirmed effect"), failure.getMessage());
        }
    }

    @Test
    void aPassResultCannotCarryANetworkFaultThatMissedItsOccurrences() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::networkFault)) {
            V1ScenarioExecutionResult result = executor(events, new FakeRuntime(events),
                    new FakeFlink(events), (bootstrap, topic, ids, timeout) -> passResult(),
                    V1ScenarioExecutor.DEFAULT_ATTEMPT_CLEANUP_TIMEOUT,
                    (path, fault) -> new PhaseExecutionEvidence.NetworkFault(
                            path, "fault-1", fault.proxy(), "test/proxy", fault.action(),
                            fault.occurrences(), fault.triggerDeadline(), 100, 200, List.of(), List.of()))
                    .execute(fixture.bound(), attemptContext());
            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(ExecutablePhaseExecutor.NETWORK_FAULT_TRIGGER_MISSED, result.reason());
            assertEquals(FlinkProcessHealth.Outcome.HEALTHY, result.processHealth().outcome());
            assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED, result.flinkRuntimeIdentity().outcome());
            assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, result.haEvidence().outcome());

            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> new V1ScenarioExecutionResult(V1ScenarioExecutionResult.Status.PASS,
                            "validator.kafka.id-set.match", "passed", result.inputManifest(),
                            result.phaseEvidence(), result.writeFenceEvidence(), result.processFenceEvidence(),
                            result.finalJobObservation(), result.terminalValidation(), result.sinkTransactions(),
                            result.subjectClassOrigins(), result.flinkProvisioningEvidence(), result.expectedFlinkRuntime(),
                            result.runtimeClassOrigins(), result.kafkaTransactionVersion(), result.expectedHa(),
                            result.tokenEvidence(), result.haObservations(), result.processObservations(),
                            result.diagnostics()));

            assertTrue(failure.getMessage().contains("network fault"), failure.getMessage());
        }
    }

    @Test
    void networkFaultEffectGatesPositiveAndNegativeControlVerdicts() throws Exception {
        for (boolean triggered : List.of(false, true)) {
            for (boolean oraclePasses : List.of(false, true)) {
                List<String> events = new ArrayList<>();
                try (Fixture fixture = fixture(V1ScenarioExecutorTest::networkFault)) {
                    V1ScenarioExecutor executor = executor(events, new FakeRuntime(events),
                            new FakeFlink(events),
                            (bootstrap, topic, ids, timeout) -> oraclePasses
                                    ? passResult() : missingResult(),
                            V1ScenarioExecutor.DEFAULT_ATTEMPT_CLEANUP_TIMEOUT,
                            (path, fault) -> new PhaseExecutionEvidence.NetworkFault(
                                    path, "fault-1", fault.proxy(), "test/proxy", fault.action(),
                                    fault.occurrences(), fault.triggerDeadline(), 100, 200,
                                    triggered ? List.of(new PhaseExecutionEvidence.DroppedMessage(
                                            1, 1, 150, true, "minimal-0-1", 1, (short) 0, true,
                                            Optional.of(new PhaseExecutionEvidence.BrokerAnswer(
                                                    "NONE", 1, (short) 1)),
                                            Optional.empty())) : List.of(),
                                    List.of()));

                    V1ScenarioExecutionResult result = executor.execute(
                            fixture.bound(), attemptContext());

                    assertEquals(oraclePasses
                                    ? (triggered ? V1ScenarioExecutionResult.Status.PASS
                                            : V1ScenarioExecutionResult.Status.INCONCLUSIVE)
                                    : V1ScenarioExecutionResult.Status.FAIL,
                            result.status(), "triggered=" + triggered + ", oracle=" + oraclePasses);
                    assertEquals(triggered,
                            result.phaseEvidence().orElseThrow().networkFaults().getFirst().triggered());
                    assertTrue(result.processFenceEvidence().isPresent());
                    assertTrue(result.terminalValidation().isPresent());
                    var expected = oraclePasses ? ExecutableScenarioPlan.ExpectedOutcome.pass()
                            : ExecutableScenarioPlan.ExpectedOutcome.failure(
                                    "kafka.id-set", missingResult().reason());
                    ScenarioVerdict verdict = ScenarioVerdict.of(expected, result);
                    assertEquals(triggered ? ScenarioVerdict.Status.PASS
                            : ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
                    if (!triggered) {
                        assertEquals(ExecutablePhaseExecutor.NETWORK_FAULT_TRIGGER_MISSED,
                                verdict.reason());
                    }
                    assertTrue(events.indexOf("kafka-proxy-start") < events.indexOf("flink-start"));
                }
            }
        }
    }

    @Test
    void readsRetryWitnessesAfterTheProcessFenceWithoutChangingTheVerdict() throws Exception {
        PhaseExecutionEvidence.Retry retry =
                new PhaseExecutionEvidence.Retry(30_150, "producer-minimal-0-1");
        for (boolean readable : List.of(true, false)) {
            List<String> events = new ArrayList<>();
            try (Fixture fixture = fixture(V1ScenarioExecutorTest::networkFault)) {
                ExecutablePhaseExecutor.NetworkFaults faults =
                        new ExecutablePhaseExecutor.NetworkFaults() {
                            @Override
                            public PhaseExecutionEvidence.NetworkFault inject(
                                    String path, ExecutableScenarioPlan.EndTxnFault fault) {
                                return new PhaseExecutionEvidence.NetworkFault(
                                        path, "fault-1", fault.proxy(), "test/proxy",
                                        fault.action(), fault.occurrences(),
                                        fault.triggerDeadline(), 100, 200,
                                        List.of(new PhaseExecutionEvidence.DroppedMessage(
                                                1, 1, 150, true, "minimal-0-1", 1, (short) 0,
                                                true,
                                                Optional.of(new PhaseExecutionEvidence
                                                        .BrokerAnswer("NONE", 1, (short) 1)),
                                                Optional.empty())),
                                        List.of());
                            }

                            @Override
                            public PhaseExecutionEvidence.NetworkFault withObservedRetries(
                                    PhaseExecutionEvidence.NetworkFault fault)
                                    throws IOException {
                                events.add("read-retries");
                                if (!readable) {
                                    throw new IOException("event file unreadable");
                                }
                                return fault.withDropped(List.of(
                                        fault.dropped().getFirst().withRetry(retry)));
                            }
                        };
                V1ScenarioExecutor executor = executor(events, new FakeRuntime(events),
                        new FakeFlink(events), (bootstrap, topic, ids, timeout) -> passResult(),
                        V1ScenarioExecutor.DEFAULT_ATTEMPT_CLEANUP_TIMEOUT, faults);

                V1ScenarioExecutionResult result = executor.execute(
                        fixture.bound(), attemptContext());

                assertEquals(V1ScenarioExecutionResult.Status.PASS, result.status(),
                        "readable=" + readable);
                assertEquals(readable ? Optional.of(retry) : Optional.empty(),
                        result.phaseEvidence().orElseThrow().networkFaults().getFirst()
                                .dropped().getFirst().retry());
                assertEquals(!readable, result.diagnostics().stream().anyMatch(diagnostic ->
                        diagnostic.startsWith("network-fault.retry-evidence-unavailable")));
                assertTrue(events.indexOf("process-fence") < events.indexOf("read-retries"),
                        "retries are read only once no Flink process can send again: " + events);
            }
        }
    }

    @Test
    void failedNetworkFaultIsInconclusiveAndSkipsTerminalValidation() throws Exception {
        List<String> events = new ArrayList<>();
        try (Fixture fixture = fixture(V1ScenarioExecutorTest::networkFault)) {
            AtomicBoolean validated = new AtomicBoolean();
            V1ScenarioExecutor executor = executor(events, new FakeRuntime(events),
                    new FakeFlink(events), (bootstrap, topic, ids, timeout) -> {
                        validated.set(true);
                        return passResult();
                    }, V1ScenarioExecutor.DEFAULT_ATTEMPT_CLEANUP_TIMEOUT,
                    (path, fault) -> { throw new IOException("proxy never confirmed heal"); });

            V1ScenarioExecutionResult result = executor.execute(fixture.bound(), attemptContext());

            assertEquals(V1ScenarioExecutionResult.Status.INCONCLUSIVE, result.status());
            assertEquals(ExecutablePhaseExecutor.NETWORK_FAULT_INFRASTRUCTURE, result.reason());
            assertFalse(validated.get());
            assertEquals(PhaseExecutionEvidence.StepStatus.FAILED,
                    result.phaseEvidence().orElseThrow().steps().getLast().status());
        }
    }

    private static void networkFault(ObjectNode document) {
        ObjectNode proxy = ((ObjectNode) document.at("/setup")).putObject("proxies")
                .putObject("kafka-proxy");
        proxy.put("type", "kroxylicious").put("cluster", "main").put("listen", "kafka-proxy:9092");
        proxy.putObject("bootstrap").put("cluster", "main");
        ((ObjectNode) document.at("/workload/jobs/0/sink"))
                .put("connect_via_proxy", "kafka-proxy");
        ObjectNode fault = document.putArray("phases").addObject().put("name", "fault")
                .putArray("steps").addObject().putObject("network_fault");
        fault.put("proxy", "kafka-proxy");
        fault.putObject("target").put("cluster", "main");
        fault.putObject("match").put("api", "end-txn").put("result", "commit");
        fault.putObject("fault").put("type", "drop-response");
        fault.put("occurrences", 1).put("trigger_deadline", "1s")
                .put("heal", "restore-proxy-rule");
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
        return executor(events, runtime, flink, validation, cleanupTimeout,
                ExecutablePhaseExecutor.NetworkFaults.NONE);
    }

    private V1ScenarioExecutor executor(
            List<String> events,
            FakeRuntime runtime,
            FakeFlink flink,
            V1ScenarioExecutor.TerminalValidation validation,
            Duration cleanupTimeout,
            ExecutablePhaseExecutor.NetworkFaults networkFaults) {
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
                cleanupTimeout,
                endpoint -> networkFaults, featureSelection);
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
        private List<org.savonitar.flink.stability.runtime.api.FlinkComponentLog> componentLogs = List.of();
        public List<org.savonitar.flink.stability.runtime.api.FlinkComponentLog> flinkComponentLogs() {
            assertEquals(0, closeCalls.get());
            return componentLogs;
        }

        private final List<String> events;
        /** The subject primary's container path, captured when Flink starts. */
        private String primarySource;
        /** Replaces the source that the fake class-load log reports, to model a mismatch. */
        private String loadedFrom;
        private Optional<FlinkRuntimeTarget.RuntimeJar> expectedRuntimeJar = Optional.empty();
        private String runtimeLoadedFrom;
        private boolean duplicateRuntimeLogPaths;
        private RuntimeException classLoadLogsFailure;
        private boolean fenced;
        private boolean unexpectedProcessExit;
        private boolean missingProcessObservations;
        private int taskManagerIncarnations;
        private boolean taskManagerRunning;
        private boolean identityUnavailable;
        private RuntimeException identityFailure;
        private List<FlinkComponentProvisioningEvidence> provisioningOverride;
        private RuntimeException provisioningFailure;
        private Exception startFlinkFailure;
        private RuntimeException processFenceFailure;
        private RuntimeException closeFailure;
        private Optional<TokenServiceControl.Snapshot> tokenSnapshot = Optional.empty();
        private final java.util.Deque<Optional<TokenServiceControl.Snapshot>> tokenSnapshots = new java.util.ArrayDeque<>();
        private Optional<String> pluginSha256 = Optional.empty();
        private Map<String, TaskManagerControl.Identity> identitiesOverride;
        private final List<String> identityRequests = new ArrayList<>();
        private List<FlinkClassLoadLog> classLoadLogsOverride;
        private FlinkProcessWriteFenceEvidence fenceOverride;
        private Optional<FlinkHaControl.Observations> haHistory = Optional.empty();

        private IOException killFailure;
        private CountDownLatch closeRelease;
        private final CountDownLatch closeEntered = new CountDownLatch(1);
        private final AtomicInteger closeCalls = new AtomicInteger();

        private boolean captureArchive;
        @Override
        public org.savonitar.flink.stability.runtime.api.KafkaLogCapture captureKafkaLogs(
                List<org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Partition> partitions,
                Path output, org.savonitar.flink.stability.runtime.api.MonotonicDeadline deadline) {
            events.add("capture");
            assertEquals(2, partitions.size());
            assertTrue(Files.isDirectory(output));
            if (!captureArchive) throw new IllegalStateException("fake archive transport failure");
            try {
                String name = "00000000000000000000.log";
                byte[] tar = new byte[1536];
                System.arraycopy(name.getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0, tar, 0, name.length());
                tar[156] = '0';
                System.arraycopy("ustar\0".getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0, tar, 257, 6);
                tar[263] = '0'; tar[264] = '0';
                java.util.Arrays.fill(tar, 148, 156, (byte) ' ');
                long checksum = 0; for (byte value : tar) checksum += value & 255;
                byte[] field = String.format(java.util.Locale.ROOT, "%06o\0 ", checksum)
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                System.arraycopy(field, 0, tar, 148, field.length);
                Path file = output.resolve("segment.tar.part"); Files.write(file, tar);
                var archive = new org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Archive(
                        partitions.getFirst(), name, "container", "image", "network", 1, file,
                        "TRANSPORT_EOF", tar.length, Optional.of(org.savonitar.flink.stability.runtime.api.Digests.sha256(tar)),
                        true, "fake finished transport");
                return new org.savonitar.flink.stability.runtime.api.KafkaLogCapture(
                        List.of(), List.of(archive), tar.length, 3, List.of());
            } catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
        }

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
        public KafkaProxyEndpoint startKafkaProxy(KafkaProxyTarget target) {
            events.add("kafka-proxy-start");
            return new KafkaProxyEndpoint(target.proxyAlias(), target.bootstrapServers(),
                    "test/kroxylicious", Path.of("proxy-control"));
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
            taskManagerIncarnations = 1;
            taskManagerRunning = true;
            primarySource = target.connectorBundle().classpathManifest().entries()
                    .getFirst().containerPath();
            expectedRuntimeJar = target.expectedRuntimeJar();
            return "http://localhost:8081";
        }

        @Override
        public Optional<org.savonitar.flink.stability.runtime.api.TaskManagerControl.Identity>
                taskManagerIdentity(String targetName) {
            if (identitiesOverride != null) {
                assertFalse(fenced, "live token receiver identities must be captured before the process fence");
                identityRequests.add(targetName);
                events.add("identity:" + targetName);
                return Optional.ofNullable(identitiesOverride.get(targetName));
            }
            if (identityFailure != null) {
                throw identityFailure;
            }
            if (identityUnavailable || !taskManagerRunning) {
                return Optional.empty();
            }
            return Optional.of(new org.savonitar.flink.stability.runtime.api.TaskManagerControl.Identity(
                    targetName, "tm-" + taskManagerIncarnations, "tm-" + taskManagerIncarnations));
        }

        @Override
        public void killTaskManager(String targetName, Duration timeout) throws IOException {
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT, timeout);
            events.add("taskmanager-kill");
            if (killFailure != null) {
                throw killFailure;
            }
            taskManagerRunning = false;
        }

        @Override
        public void restartTaskManager(Duration timeout) {
            assertEquals(ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT, timeout);
            events.add("taskmanager-restart");
            taskManagerIncarnations++;
            taskManagerRunning = true;
        }

        @Override
        public FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout) {
            assertEquals(FlinkTerminalWriteFence.PROCESS_FENCE_TIMEOUT, timeout);
            events.add("process-fence");
            if (processFenceFailure != null) {
                throw processFenceFailure;
            }
            fenced = true;
            return fenceOverride != null ? fenceOverride : FlinkRuntimeIdentityTest.fence(taskManagerIncarnations);
        }

        @Override
        public Optional<FlinkProcessWriteFenceEvidence.Observations> flinkProcessObservations() {
            if (!fenced || missingProcessObservations) {
                return Optional.empty();
            }
            var snapshot = FlinkRuntimeIdentityTest.healthyProcesses(
                    fenceOverride != null ? fenceOverride : FlinkRuntimeIdentityTest.fence(taskManagerIncarnations));
            if (!unexpectedProcessExit) {
                return Optional.of(snapshot);
            }
            var events = new ArrayList<>(snapshot.observations());
            var original = events.getFirst();
            events.set(0, new FlinkProcessWriteFenceEvidence.Observation(original.logicalName(), original.role(),
                    original.runtimeId(), original.moment(), original.observedAt(), Optional.of(
                    new org.savonitar.flink.stability.runtime.api.FlinkHaControl.ProcessState(
                            original.runtimeId().orElseThrow(), false, false, Optional.of(1L),
                            Optional.of(false), Optional.of("2026-09-27T12:00:00Z"))), false, Optional.empty()));
            return Optional.of(new FlinkProcessWriteFenceEvidence.Observations(events, snapshot.fenced(), false));
        }

        @Override
        public Optional<TokenServiceControl.Snapshot> tokenServiceEvidence() {
            if (pluginSha256.isPresent()) events.add("token-snapshot");
            return tokenSnapshots.isEmpty() ? tokenSnapshot : tokenSnapshots.removeFirst();
        }

        @Override
        public Optional<String> tokenPluginSha256() {
            return pluginSha256;
        }

        @Override
        public Optional<FlinkHaControl.Observations> haObservations() {
            return haHistory;
        }

        @Override
        public List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence() {
            if (provisioningFailure != null) {
                throw provisioningFailure;
            }
            return provisioningOverride != null ? provisioningOverride
                    : taskManagerIncarnations == 0 ? List.of()
                    : runtimeProvisioning();
        }

        private List<FlinkComponentProvisioningEvidence> runtimeProvisioning() {
            return FlinkRuntimeIdentityTest.provisioning(taskManagerIncarnations).stream()
                    .map(component -> expectedRuntimeJar.map(jar -> component.withRuntimeJarEvidence(
                            jar, component.logicalName() + "#" + component.runtimeId())).orElse(component))
                    .toList();
        }

        /** Runtime provenance requests retain a separately registered log for every incarnation. */
        @Override
        public List<FlinkClassLoadLog> flinkClassLoadLogs() {
            if (classLoadLogsOverride != null) return classLoadLogsOverride;
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
                if (expectedRuntimeJar.isEmpty()) {
                    return List.of(new FlinkClassLoadLog("taskmanager-1#1", log));
                }
                String runtimeSource = runtimeLoadedFrom != null ? runtimeLoadedFrom
                        : expectedRuntimeJar.orElseThrow().containerPath();
                // Both classes are present so aliased paths would otherwise satisfy either role.
                String content = Files.readString(log) + "\n"
                        + "[1.2s][info][class,load] " + FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS
                        + " source: file:" + runtimeSource + "\n"
                        + "[1.3s][info][class,load] " + FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS
                        + " source: file:" + runtimeSource;
                Files.writeString(log, content);
                List<FlinkClassLoadLog> logs = new ArrayList<>();
                for (FlinkComponentProvisioningEvidence component : runtimeProvisioning()) {
                    Path processLog = log;
                    if (!duplicateRuntimeLogPaths) {
                        processLog = Files.createTempFile("flink-stability-runtime-load-", ".log");
                        processLog.toFile().deleteOnExit();
                        Files.writeString(processLog, content);
                    }
                    logs.add(new FlinkClassLoadLog(
                            component.runtimeJarEvidence().orElseThrow().classLoadProcess(), processLog));
                }
                return logs;
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
        private FlinkJobHandle submittedHandle = JOB;
        private String uploadedJarSha256;
        private IOException awaitFinishedFailure;
        private IOException awaitStateFailure;
        private final List<FlinkScenarioControl.RestError> httpErrors = new ArrayList<>();
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
            return submittedHandle;
        }

        @Override
        public FlinkJobState jobState(FlinkJobHandle job) {
            return FlinkJobState.FINISHED;
        }

        @Override
        public FlinkJobState awaitState(
                FlinkJobHandle job, FlinkJobState expected, Duration timeout) throws IOException {
            events.add("await-running");
            if (awaitStateFailure != null) {
                throw awaitStateFailure;
            }
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
            events.add("sample-jobmanager-time");
            return 1_500;
        }

        @Override
        public List<FlinkScenarioControl.RestError> restErrors() {
            return List.copyOf(httpErrors);
        }

        @Override
        public void close() {
            events.add("flink-close");
            httpErrors.clear();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
