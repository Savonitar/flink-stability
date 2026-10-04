package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.TaskManagerActionTimeoutException;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.KafkaProxyEndpoint;
import org.savonitar.flink.stability.runtime.api.KafkaProxyTarget;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.flink.FlinkJobSubmission;
import org.savonitar.flink.stability.core.flink.FlinkRestTimeoutException;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutablePhaseExecutorTest {
    private static final FlinkJobHandle JOB = new FlinkJobHandle("job-1");

    private final SpecificationLoader loader = new SpecificationLoader();
    private final ExecutableScenarioPlanCompiler compiler =
            new ExecutableScenarioPlanCompiler();
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    @TempDir
    Path temporaryDirectory;

    @Test
    void checkpointPollArmsTheNextKillAndObservesAllSinkPartitions() throws Exception {
        var plan = plan(document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main/topics/1")).put("partitions", 3);
            var steps = replaceSteps(document);
            addAwaitInProgress(steps, "inconclusive");
            steps.addObject().putObject("kill").putObject("target")
                    .put("kind", "named").put("role", "taskmanager").put("name", "taskmanager-1");
            steps.addObject().putObject("restart").put("component", "taskmanager");
        });
        var events = new ArrayList<String>();
        var flink = new FakeFlink(events);
        var stats = CheckpointKillWindowTest.overview();
        flink.checkpointOverviews.add(new ObjectMapper().createObjectNode());
        flink.checkpointOverviews.add(stats);
        flink.checkpointOverviews.add(stats);
        flink.checkpointOverviews.add(stats);
        var runtime = new FakeTaskManagers(events);
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var executor = new ExecutablePhaseExecutor(flink, runtime, ExecutablePhaseExecutor.NetworkFaults.NONE,
                duration -> clock.addAndGet(duration.toNanos()), clock::get);
        var evidence = executor.execute(plan, JOB);
        assertEquals(4, flink.overviewCalls);
        assertEquals(PhaseExecutionEvidence.StepKind.AWAIT_CHECKPOINT_IN_PROGRESS, evidence.steps().getFirst().kind());
        var window = evidence.taskManagerKills().getFirst().checkpointWindow().orElseThrow();
        assertTrue(window.confirmed());
        assertEquals(stats, window.armed());
        assertEquals(List.of(0, 1, 2), window.brokerBeforeKill().stream().map(value -> value.partition()).toList());
        assertEquals(List.of(0, 1, 2), window.brokerAfterKill().stream().map(value -> value.partition()).toList());
        assertEquals(List.of("main/output/0", "main/output/1", "main/output/2",
                "main/output/0", "main/output/1", "main/output/2"), runtime.producerRequests);
    }

    @Test
    void absenceOfAnEligibleCheckpointUsesTheDeclaredTimeoutPolicy() {
        for (String policy : List.of("fail", "inconclusive")) {
            var plan = plan(document -> addAwaitInProgress(replaceSteps(document), policy));
            var clock = new java.util.concurrent.atomic.AtomicLong();
            var executor = new ExecutablePhaseExecutor(new FakeFlink(new ArrayList<>()),
                    new FakeTaskManagers(new ArrayList<>()), ExecutablePhaseExecutor.NetworkFaults.NONE,
                    duration -> clock.addAndGet(duration.toNanos()), clock::get);
            var failure = assertThrows(PhaseExecutionException.class, () -> executor.execute(plan, JOB));
            assertEquals("checkpoint-window.missed", failure.reason());
            assertEquals(policy.toUpperCase(java.util.Locale.ROOT), failure.outcome().name());
            assertEquals(PhaseExecutionEvidence.StepStatus.FAILED, failure.evidence().steps().getFirst().status());
        }
    }

    @Test
    void checkpointPollingErrorsAreInfrastructureAndRetainTheCause() {
        for (IOException problem : List.of(new IOException("HTTP 500: NullArgumentException: input array"),
                new FlinkRestTimeoutException("HTTP 500 retries exhausted"), new IOException("malformed JSON"))) {
            var plan = plan(document -> addAwaitInProgress(replaceSteps(document), "fail"));
            var flink = new FakeFlink(new ArrayList<>());
            flink.overviewFailure = problem;
            var failure = assertThrows(PhaseExecutionException.class, () -> executor(flink).execute(plan, JOB));
            assertEquals(ExecutablePhaseExecutor.AWAIT_CHECKPOINT_IN_PROGRESS_INFRASTRUCTURE, failure.reason());
            assertEquals(PhaseExecutionException.Outcome.INCONCLUSIVE, failure.outcome());
            assertEquals(problem, failure.getCause());
        }
    }

    @Test
    void checkpointPollInterruptionRestoresInterruptFlag() {
        var plan = plan(document -> addAwaitInProgress(replaceSteps(document), "fail"));
        var executor = new ExecutablePhaseExecutor(new FakeFlink(new ArrayList<>()),
                new FakeTaskManagers(new ArrayList<>()), ExecutablePhaseExecutor.NetworkFaults.NONE,
                duration -> { throw new InterruptedException("stop"); });
        try {
            var failure = assertThrows(PhaseExecutionException.class, () -> executor.execute(plan, JOB));
            assertEquals(ExecutablePhaseExecutor.AWAIT_CHECKPOINT_IN_PROGRESS_INFRASTRUCTURE, failure.reason());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private static void addAwaitInProgress(ArrayNode steps, String policy) {
        var await = steps.addObject().putObject("await");
        await.putObject("condition").put("type", "checkpoint-in-progress").put("job", "eos-job");
        await.put("timeout", "200ms").put("on_timeout", policy);
    }

    @Test
    void executesAtomicStepsInDocumentOrderAndReturnsImmutableEvidence() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            ArrayNode steps = replaceSteps(document);
            addAwaitJobState(steps, "fail");
            steps.addObject().putObject("wait").put("duration", "25ms");
            addAwaitCheckpoints(steps, 2, "inconclusive");
        });
        List<String> events = new ArrayList<>();
        FakeFlink flink = new FakeFlink(events);
        FakeTaskManagers taskManagers = new FakeTaskManagers(events);
        ExecutablePhaseExecutor executor = new ExecutablePhaseExecutor(
                flink,
                taskManagers,
                ExecutablePhaseExecutor.NetworkFaults.NONE,
                duration -> events.add("sleep:" + duration));

        PhaseExecutionEvidence evidence = executor.execute(plan, JOB);

        assertEquals(List.of(
                        "await-state:RUNNING:PT2M",
                        "sleep:PT0.025S",
                        "await-checkpoints:2:PT2M"),
                events);
        assertEquals(List.of(
                        "$/phases/0/steps/0",
                        "$/phases/0/steps/1",
                        "$/phases/0/steps/2"),
                evidence.steps().stream()
                        .map(PhaseExecutionEvidence.StepEvidence::path)
                        .toList());
        assertEquals(List.of(
                        PhaseExecutionEvidence.StepKind.AWAIT_JOB_STATE,
                        PhaseExecutionEvidence.StepKind.WAIT,
                        PhaseExecutionEvidence.StepKind.AWAIT_CHECKPOINTS),
                evidence.steps().stream()
                        .map(PhaseExecutionEvidence.StepEvidence::kind)
                        .toList());
        assertTrue(evidence.steps().stream().allMatch(step ->
                step.status() == PhaseExecutionEvidence.StepStatus.SUCCEEDED
                        && step.phaseIndex() == 0
                        && step.phaseName().equals("verify-running")
                        && step.loopIterations().isEmpty()));
        assertThrows(UnsupportedOperationException.class, evidence.steps()::clear);
    }

    @Test
    void recursivelyExecutesNestedLoopsWithExactIterationFrames() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            ArrayNode steps = replaceSteps(document);
            ObjectNode outer = steps.addObject().putObject("loop");
            outer.put("times", 2);
            ArrayNode outerSteps = outer.putArray("steps");
            outerSteps.addObject().putObject("wait").put("duration", "1ms");
            ObjectNode inner = outerSteps.addObject().putObject("loop");
            inner.put("times", 2);
            inner.putArray("steps").addObject().putObject("wait").put("duration", "2ms");
        });
        List<Duration> waits = new ArrayList<>();
        ExecutablePhaseExecutor executor = new ExecutablePhaseExecutor(
                new FakeFlink(new ArrayList<>()),
                new FakeTaskManagers(new ArrayList<>()),
                ExecutablePhaseExecutor.NetworkFaults.NONE,
                waits::add);

        PhaseExecutionEvidence evidence = executor.execute(plan, JOB);

        assertEquals(List.of(
                        Duration.ofMillis(1),
                        Duration.ofMillis(2),
                        Duration.ofMillis(2),
                        Duration.ofMillis(1),
                        Duration.ofMillis(2),
                        Duration.ofMillis(2)),
                waits);
        assertEquals(List.of(
                        "$/phases/0/steps/0/loop/steps/0",
                        "$/phases/0/steps/0/loop/steps/1/loop/steps/0",
                        "$/phases/0/steps/0/loop/steps/1/loop/steps/0",
                        "$/phases/0/steps/0/loop/steps/0",
                        "$/phases/0/steps/0/loop/steps/1/loop/steps/0",
                        "$/phases/0/steps/0/loop/steps/1/loop/steps/0"),
                evidence.steps().stream()
                        .map(PhaseExecutionEvidence.StepEvidence::path)
                        .toList());
        assertEquals(List.of(
                        List.of(frame("$/phases/0/steps/0", 1, 2)),
                        List.of(
                                frame("$/phases/0/steps/0", 1, 2),
                                frame("$/phases/0/steps/0/loop/steps/1", 1, 2)),
                        List.of(
                                frame("$/phases/0/steps/0", 1, 2),
                                frame("$/phases/0/steps/0/loop/steps/1", 2, 2)),
                        List.of(frame("$/phases/0/steps/0", 2, 2)),
                        List.of(
                                frame("$/phases/0/steps/0", 2, 2),
                                frame("$/phases/0/steps/0/loop/steps/1", 1, 2)),
                        List.of(
                                frame("$/phases/0/steps/0", 2, 2),
                                frame("$/phases/0/steps/0/loop/steps/1", 2, 2))),
                evidence.steps().stream()
                        .map(PhaseExecutionEvidence.StepEvidence::loopIterations)
                        .toList());
        assertThrows(
                UnsupportedOperationException.class,
                evidence.steps().get(1).loopIterations()::clear);
    }

    @Test
    void appliesEachAwaitTimeoutPolicyWithStableReasonsAndFailureEvidence() {
        ExecutableScenarioPlan failPlan = plan(document ->
                addAwaitJobState(replaceSteps(document), "fail"));
        FakeFlink failFlink = new FakeFlink(new ArrayList<>());
        failFlink.awaitStateFailure = new FlinkRestTimeoutException("deadline");

        PhaseExecutionException fail = assertThrows(
                PhaseExecutionException.class,
                () -> executor(failFlink).execute(failPlan, JOB));

        assertEquals(PhaseExecutionException.Outcome.FAIL, fail.outcome());
        assertEquals("await.job-state.timeout", fail.reason());
        assertEquals("$/phases/0/steps/0", fail.path());
        assertEquals(PhaseExecutionEvidence.StepStatus.FAILED,
                fail.evidence().steps().getLast().status());
        assertTrue(fail.getCause() instanceof FlinkRestTimeoutException);

        ExecutableScenarioPlan inconclusivePlan = plan(document ->
                addAwaitCheckpoints(replaceSteps(document), 3, "inconclusive"));
        FakeFlink inconclusiveFlink = new FakeFlink(new ArrayList<>());
        inconclusiveFlink.checkpointFailure = new FlinkRestTimeoutException("deadline");

        PhaseExecutionException inconclusive = assertThrows(
                PhaseExecutionException.class,
                () -> executor(inconclusiveFlink).execute(inconclusivePlan, JOB));

        assertEquals(
                PhaseExecutionException.Outcome.INCONCLUSIVE,
                inconclusive.outcome());
        assertEquals("await.checkpoint-completed.timeout", inconclusive.reason());
        assertEquals("$/phases/0/steps/0", inconclusive.path());
    }

    @Test
    void nonTimeoutFlinkFailureOverridesDeclaredFailPolicyAsInfrastructure() {
        ExecutableScenarioPlan plan = plan(document ->
                addAwaitCheckpoints(replaceSteps(document), 2, "fail"));
        FakeFlink flink = new FakeFlink(new ArrayList<>());
        flink.checkpointFailure = new IOException("REST response unavailable");

        PhaseExecutionException failure = assertThrows(
                PhaseExecutionException.class,
                () -> executor(flink).execute(plan, JOB));

        assertEquals(
                PhaseExecutionException.Outcome.INCONCLUSIVE,
                failure.outcome());
        assertEquals("await.checkpoint-completed.infrastructure", failure.reason());
        assertEquals("await.checkpoint-completed.infrastructure",
                failure.evidence().steps().getLast().detail());
        assertTrue(failure.getCause() instanceof IOException);
    }

    @Test
    void namedRestartsKeepIndependentPredecessorsAcrossRepeatedTwoTaskManagerLoops() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
            ObjectNode loop = replaceSteps(document).addObject().putObject("loop");
            loop.put("times", 2);
            ArrayNode steps = loop.putArray("steps");
            for (String name : List.of("taskmanager-2", "taskmanager-1")) {
                steps.addObject().putObject("kill").putObject("target")
                        .put("kind", "named").put("role", "taskmanager").put("name", name);
                steps.addObject().putObject("restart").put("component", "taskmanager").put("name", name);
            }
        });
        List<String> events = new ArrayList<>();
        FakeTaskManagers taskManagers = new FakeTaskManagers(events);
        PhaseExecutionEvidence evidence = new ExecutablePhaseExecutor(
                new FakeFlink(events), taskManagers, ExecutablePhaseExecutor.NetworkFaults.NONE,
                duration -> {}).execute(plan, JOB);

        assertEquals(List.of("taskmanager-2", "taskmanager-1", "taskmanager-2", "taskmanager-1"),
                evidence.taskManagerRestarts().stream()
                        .map(PhaseExecutionEvidence.TaskManagerRestart::target).toList());
        for (int index = 0; index < evidence.taskManagerRestarts().size(); index++) {
            var restart = evidence.taskManagerRestarts().get(index);
            int iteration = index / 2 + 1;
            assertEquals(List.of(frame("$/phases/0/steps/0", iteration, 2)), restart.loopIterations());
            assertEquals(evidence.taskManagerKills().get(index).identity(), restart.previousIdentity());
            assertEquals(restart.target() + "-container-" + iteration,
                    restart.previousIdentity().orElseThrow().runtimeId());
            assertEquals(restart.target() + "-container-" + (iteration + 1),
                    restart.replacementIdentity().orElseThrow().runtimeId());
        }
        assertThrows(UnsupportedOperationException.class, evidence.taskManagerRestarts()::clear);
    }

    @Test
    void killsAndRestartsTheNamedTaskmanagerAndClassifiesComponentFailure() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            ArrayNode steps = replaceSteps(document);
            ObjectNode target = steps.addObject().putObject("kill").putObject("target");
            target.put("kind", "named");
            target.put("role", "taskmanager");
            target.put("name", "taskmanager-1");
            steps.addObject().putObject("restart").put("component", "taskmanager");
        });
        List<String> events = new ArrayList<>();
        FakeTaskManagers taskManagers = new FakeTaskManagers(events);
        ExecutablePhaseExecutor executor = new ExecutablePhaseExecutor(
                new FakeFlink(events), taskManagers,
                ExecutablePhaseExecutor.NetworkFaults.NONE, duration -> {});

        PhaseExecutionEvidence evidence = executor.execute(plan, JOB);

        assertEquals(
                List.of("observe-job", "sample-jobmanager-time", "kill:taskmanager-1", "sample-jobmanager-time",
                        "restart:taskmanager"), events);
        assertEquals(1, evidence.taskManagerKills().size());
        PhaseExecutionEvidence.TaskManagerKill kill = evidence.taskManagerKills().getFirst();
        assertEquals("$/phases/0/steps/0", kill.path());
        assertEquals("taskmanager-1", kill.target());
        assertEquals(Optional.of(RUNNING_JOB), kill.jobBeforeKill().observation());
        assertEquals(OptionalLong.of(1_250), kill.jobManagerTimeBeforeKill());
        assertEquals(OptionalLong.of(1_500), kill.jobManagerTimeAfterKill());
        assertEquals(Optional.of(new TaskManagerControl.Identity(
                "taskmanager-1", "taskmanager-1-container-1", "taskmanager-1-resource-1")),
                kill.identity());
        assertEquals(1, evidence.taskManagerRestarts().size());
        var restart = evidence.taskManagerRestarts().getFirst();
        assertEquals("taskmanager-1", restart.target());
        assertEquals(kill.identity(), restart.previousIdentity());
        assertEquals(Optional.of(new TaskManagerControl.Identity(
                "taskmanager-1", "taskmanager-1-container-2", "taskmanager-1-resource-2")),
                restart.replacementIdentity());
        assertEquals(
                ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT,
                taskManagers.killTimeout);
        assertEquals(
                ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT,
                taskManagers.restartTimeout);
        assertEquals(List.of(
                        PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER,
                        PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER),
                evidence.steps().stream()
                        .map(PhaseExecutionEvidence.StepEvidence::kind)
                        .toList());

        events.clear();
        taskManagers.restartFailure = new IOException("container create failed");
        PhaseExecutionException failure = assertThrows(
                PhaseExecutionException.class,
                () -> executor.execute(plan, JOB));

        assertEquals(
                List.of("observe-job", "sample-jobmanager-time", "kill:taskmanager-1", "sample-jobmanager-time",
                        "restart:taskmanager"), events);
        assertEquals(
                PhaseExecutionException.Outcome.INCONCLUSIVE,
                failure.outcome());
        assertEquals("taskmanager.restart.infrastructure", failure.reason());
        assertEquals("$/phases/0/steps/1", failure.path());
        assertEquals(2, failure.evidence().steps().size());
        assertEquals(1, failure.evidence().taskManagerKills().size());

        events.clear();
        taskManagers.restartFailure = null;
        taskManagers.killFailure = new IOException("container kill failed");
        PhaseExecutionException killFailure = assertThrows(
                PhaseExecutionException.class,
                () -> executor.execute(plan, JOB));

        assertEquals(List.of("observe-job", "sample-jobmanager-time", "kill:taskmanager-1"), events);
        assertEquals(PhaseExecutionException.Outcome.INCONCLUSIVE,
                killFailure.outcome());
        assertEquals("taskmanager.kill.infrastructure", killFailure.reason());
        assertEquals("$/phases/0/steps/0", killFailure.path());
        assertEquals(1, killFailure.evidence().steps().size());
        assertTrue(killFailure.evidence().taskManagerKills().isEmpty(),
                "an unconfirmed kill has no effect to judge");

        taskManagers.killFailure = new TaskManagerActionTimeoutException(
                TaskManagerActionTimeoutException.Action.KILL,
                ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT,
                new IllegalStateException("driver deadline"));
        PhaseExecutionException killTimeout = assertThrows(
                PhaseExecutionException.class,
                () -> executor.execute(plan, JOB));

        assertEquals(PhaseExecutionException.Outcome.INCONCLUSIVE, killTimeout.outcome());
        assertEquals("taskmanager.kill.timeout", killTimeout.reason());
        assertEquals("taskmanager.kill.timeout",
                killTimeout.evidence().steps().getLast().detail());

        taskManagers.killFailure = null;
        taskManagers.restartFailure = new TaskManagerActionTimeoutException(
                TaskManagerActionTimeoutException.Action.RESTART,
                ExecutablePhaseExecutor.TASKMANAGER_ACTION_TIMEOUT,
                new IllegalStateException("driver deadline"));
        PhaseExecutionException restartTimeout = assertThrows(
                PhaseExecutionException.class,
                () -> executor.execute(plan, JOB));

        assertEquals(PhaseExecutionException.Outcome.INCONCLUSIVE, restartTimeout.outcome());
        assertEquals("taskmanager.restart.timeout", restartTimeout.reason());
        assertEquals("$/phases/0/steps/1", restartTimeout.path());
    }

    @Test
    void anUnavailableJobObservationIsRecordedAndDoesNotPreventTheKill() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            ArrayNode steps = replaceSteps(document);
            ObjectNode target = steps.addObject().putObject("kill").putObject("target");
            target.put("kind", "named");
            target.put("role", "taskmanager");
            target.put("name", "taskmanager-1");
            steps.addObject().putObject("restart").put("component", "taskmanager");
        });
        List<String> events = new ArrayList<>();
        FakeFlink flink = new FakeFlink(events);
        flink.observeFailure = new IOException("REST unavailable");
        ExecutablePhaseExecutor executor = new ExecutablePhaseExecutor(
                flink, new FakeTaskManagers(events),
                ExecutablePhaseExecutor.NetworkFaults.NONE, duration -> {});

        PhaseExecutionEvidence evidence = executor.execute(plan, JOB);

        assertEquals(
                List.of("observe-job", "sample-jobmanager-time", "kill:taskmanager-1", "sample-jobmanager-time",
                        "restart:taskmanager"), events);
        FlinkJobObservation.Attempt observed =
                evidence.taskManagerKills().getFirst().jobBeforeKill();
        assertTrue(observed.observation().isEmpty());
        assertEquals(Optional.of("IOException: REST unavailable"), observed.failure());
    }

    @Test
    void eitherUnavailableKillClockSampleDoesNotPreventRestart() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            ArrayNode steps = replaceSteps(document);
            ObjectNode target = steps.addObject().putObject("kill").putObject("target");
            target.put("kind", "named");
            target.put("role", "taskmanager");
            target.put("name", "taskmanager-1");
            steps.addObject().putObject("restart").put("component", "taskmanager");
        });
        for (int failedCall : List.of(1, 2)) {
            List<String> events = new ArrayList<>();
            FakeFlink flink = new FakeFlink(events);
            flink.clockFailure = new IOException("JobManager clock unavailable");
            flink.clockFailureCall = failedCall;

            PhaseExecutionEvidence evidence = new ExecutablePhaseExecutor(
                    flink, new FakeTaskManagers(events),
                    ExecutablePhaseExecutor.NetworkFaults.NONE, duration -> {}).execute(plan, JOB);

            assertEquals(List.of("observe-job", "sample-jobmanager-time", "kill:taskmanager-1",
                    "sample-jobmanager-time", "restart:taskmanager"), events);
            var kill = evidence.taskManagerKills().getFirst();
            assertEquals(failedCall == 1 ? OptionalLong.empty() : OptionalLong.of(1_250),
                    kill.jobManagerTimeBeforeKill());
            assertEquals(failedCall == 2 ? OptionalLong.empty() : OptionalLong.of(1_500),
                    kill.jobManagerTimeAfterKill());
            assertEquals(PhaseExecutionEvidence.StepStatus.SUCCEEDED,
                    evidence.steps().getLast().status());
        }
    }

    @Test
    void injectsEachProtocolFaultAndKeepsWhatItDid() throws Exception {
        ExecutableScenarioPlan plan = plan(ExecutablePhaseExecutorTest::addProtocolFault);
        List<String> injected = new ArrayList<>();
        ExecutablePhaseExecutor executor = new ExecutablePhaseExecutor(
                new FakeFlink(new ArrayList<>()),
                new FakeTaskManagers(new ArrayList<>()),
                (path, fault) -> {
                    injected.add(path + " " + fault.action());
                    return new PhaseExecutionEvidence.NetworkFault(
                            path, "phases-0-steps-0", fault.proxy(), "test/proxy",
                            fault.action(),
                            fault.occurrences(), fault.triggerDeadline(), 1, 2, List.of(),
                            List.of());
                },
                duration -> {});

        PhaseExecutionEvidence evidence = executor.execute(plan, JOB);

        assertEquals(List.of("$/phases/0/steps/0 DROP_REQUEST"), injected);
        assertEquals("phases-0-steps-0", evidence.networkFaults().getFirst().faultId());
        PhaseExecutionEvidence.StepEvidence step = evidence.steps().getFirst();
        assertEquals(PhaseExecutionEvidence.StepKind.NETWORK_FAULT, step.kind());
        assertEquals(PhaseExecutionEvidence.StepStatus.SUCCEEDED, step.status());
        assertEquals("fault=phases-0-steps-0 dropped=0/1", step.detail());
    }

    @Test void armedRecoveryUsesBoundedLifecycleAndRetainsDisruptionAndReplacement() throws Exception {
        var plan = plan(document -> {
            addProtocolFault(document);
            ((ObjectNode) document.at("/phases/0/steps/0/network_fault")).putObject("restart").put("component", "taskmanager");
        });
        List<String> events = new ArrayList<>();
        var runtime = new FakeTaskManagers(events);
        var faults = new ExecutablePhaseExecutor.NetworkFaults() {
            public PhaseExecutionEvidence.NetworkFault inject(String path, ExecutableScenarioPlan.ProtocolFault fault) {
                throw new AssertionError("Recovery must be synchronized with arm");
            }
            public PhaseExecutionEvidence.NetworkFault inject(String path, ExecutableScenarioPlan.ProtocolFault fault, ArmedAction action) throws Exception {
                events.add("armed"); action.run(); events.add("healed");
                return new PhaseExecutionEvidence.NetworkFault(path, "fault", fault.proxy(), "proxy-image", fault.action(),
                        1, fault.triggerDeadline(), 1, 2, List.of(), List.of());
            }
        };
        var evidence = new ExecutablePhaseExecutor(new FakeFlink(events), runtime, faults, duration -> {}).execute(plan, JOB);
        assertEquals("armed", events.getFirst()); assertEquals("healed", events.getLast());
        assertTrue(events.indexOf("kill:taskmanager-1") < events.indexOf("restart:taskmanager"));
        assertEquals(Duration.ofMinutes(2), runtime.killTimeout); assertEquals(Duration.ofMinutes(2), runtime.restartTimeout);
        assertEquals(1, evidence.taskManagerKills().size()); assertEquals(1, evidence.taskManagerRestarts().size());
        assertEquals("taskmanager-1-container-2", evidence.taskManagerRestarts().getFirst().replacementIdentity().orElseThrow().runtimeId());
    }

    @Test void packetFaultRequiresRunningJobCountersAndHealingBeforeProceeding() throws Exception {
        var plan=plan(document -> {
            ((ObjectNode)document.at("/setup/kafka/clusters/main")).put("brokers",3);
            document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode)topic).put("replication_factor",3));
            var steps=replaceSteps(document);
            var packet=steps.addObject().putObject("packet_fault").put("taskmanager","taskmanager-1")
                    .put("mode","loss").put("loss_percent",25).put("duration","15s").put("timeout","2m");
            packet.putObject("target").put("kind","selector").put("role","broker").put("cluster","main")
                    .put("type","partition-leader").put("topic","output").put("partition",0);
            steps.addObject().putObject("wait").put("duration","1ms");
        });
        for (int mode=0;mode<3;mode++) {
            List<String> events=new ArrayList<>();var flink=new FakeFlink(events);var runtime=new FakeTaskManagers(events);
            flink.haObservations.add(RUNNING_JOB);
            flink.haObservations.add(mode==1 ? new FlinkJobObservation(2000,FlinkJobState.FINISHED,3,0,Optional.empty(),List.of(),List.of()) : RUNNING_JOB);
            runtime.packetUnconfirmed=mode==2;
            var result=new ExecutablePhaseExecutor(flink,runtime,ExecutablePhaseExecutor.NetworkFaults.NONE,
                    duration -> events.add("after-packet")).execute(plan,JOB);
            assertEquals(mode==0,result.packetFaults().getFirst().confirmed());
            assertEquals(mode==0,events.contains("after-packet"));
            assertTrue(events.contains("packet-fault"));
        }
    }

    @Test void rollingFaultRequiresTheJobToRemainRunningAndRetainsBothObservations() throws Exception {
        var plan = plan(document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
            document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode) topic).put("replication_factor", 3));
            var steps = replaceSteps(document);
            var fault = steps.addObject().putObject("broker_fault").put("mode", "rolling-restart").put("order", "fixed").put("timeout", "2m");
            fault.putObject("target").put("kind", "selector").put("role", "broker").put("cluster", "main")
                    .put("type", "transaction-coordinator").put("job", "eos-job");
            steps.addObject().putObject("wait").put("duration", "1ms");
        });
        for (boolean finished : List.of(false, true)) {
            List<String> events = new ArrayList<>(); var flink = new FakeFlink(events);
            flink.haObservations.add(RUNNING_JOB);
            flink.haObservations.add(finished ? new FlinkJobObservation(2000, FlinkJobState.FINISHED, 3, 0, Optional.empty(), List.of(), List.of()) : RUNNING_JOB);
            var runtime = new FakeTaskManagers(events);
            runtime.brokerObservations = rollingEvidence();
            var evidence = new ExecutablePhaseExecutor(flink, runtime, ExecutablePhaseExecutor.NetworkFaults.NONE,
                    duration -> events.add("after-roll")).execute(plan, JOB);
            assertEquals(!finished, events.contains("after-roll"));
            assertEquals(6, evidence.brokerOperations().size());
            assertEquals(!finished, evidence.brokerOperations().getFirst().raw().confirmed());
            assertEquals(FlinkJobState.RUNNING, evidence.brokerOperations().getFirst().jobBefore().observation().orElseThrow().state());
            assertEquals(finished ? FlinkJobState.FINISHED : FlinkJobState.RUNNING,
                    evidence.brokerOperations().getFirst().jobAfter().observation().orElseThrow().state());
        }
    }
    private static List<org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence> rollingEvidence() {
        var result = new ArrayList<org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence>();
        var target = new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Target(
                org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR, null, null, -1, "minimal");
        var reference = new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Selection(target, "broker-1", "__transaction_state", 0, 1, "minimal-0", 42L, 0, 1);
        for (int id = 1; id <= 3; id++) {
            int next = id == 3 ? 1 : id + 1;
            var running = new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Snapshot("c"+id,"image","network",id,true);
            var stopped = new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Snapshot("c"+id,"image","network",id,false);
            var before = List.of(new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Leadership("__transaction_state",0,id,List.of(1,2,3),List.of(1,2,3)));
            var isr = new ArrayList<>(List.of(1,2,3)); isr.remove(Integer.valueOf(id));
            var during = List.of(new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Leadership("__transaction_state",0,next,List.of(1,2,3),isr));
            var ready = List.of(new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Leadership("__transaction_state",0,next,List.of(1,2,3),List.of(1,2,3)));
            var progress = new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.RollingProgress(id,List.of("broker-1","broker-2","broker-3"),
                    org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.RollingOrder.FIXED,1,2,reference,ready,false,false);
            result.add(new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence("broker-"+id,false,running,stopped,before,during,null,
                    org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Action.STOP,null).withRolling(progress));
            result.add(new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence("broker-"+id,true,stopped,running,during,ready,null).withRolling(progress));
        }
        return result;
    }

    @Test
    void aProxyThatCannotInjectMakesTheAttemptInconclusive() {
        ExecutableScenarioPlan plan = plan(ExecutablePhaseExecutorTest::addProtocolFault);
        ExecutablePhaseExecutor executor = new ExecutablePhaseExecutor(
                new FakeFlink(new ArrayList<>()),
                new FakeTaskManagers(new ArrayList<>()),
                (path, fault) -> {
                    throw new IOException("The proxy did not arm fault phases-0-steps-0");
                },
                duration -> {});

        PhaseExecutionException failure = assertThrows(
                PhaseExecutionException.class, () -> executor.execute(plan, JOB));

        assertEquals(PhaseExecutionException.Outcome.INCONCLUSIVE, failure.outcome());
        assertEquals(ExecutablePhaseExecutor.NETWORK_FAULT_INFRASTRUCTURE, failure.reason());
    }

    @Test
    void leaderFaultSharesOneBudgetWithBothJobObservationsAndPreservesDeclaration() throws Exception {
        ExecutableScenarioPlan plan = plan(ExecutablePhaseExecutorTest::addLeaderFault);
        List<String> events = new ArrayList<>();
        java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
        FakeFlink flink = new FakeFlink(events);
        flink.haObservationCompleted = () -> clock.addAndGet(Duration.ofMillis(500).toNanos());
        FakeTaskManagers runtime = new FakeTaskManagers(events);
        runtime.leaderEvidence = request -> {
            clock.addAndGet(Duration.ofMillis(250).toNanos());
            return new FlinkHaControl.LeaderFaultEvidence(request,
                    Optional.empty(), Optional.empty(), Optional.empty(), false, false,
                    Optional.empty(), Optional.empty(), 0, 0, 0, 0, false,
                    Optional.empty(), Optional.empty(), Optional.empty(), List.of());
        };

        PhaseExecutionEvidence result = new ExecutablePhaseExecutor(flink, runtime,
                ExecutablePhaseExecutor.NetworkFaults.NONE, duration -> {}, clock::get)
                .execute(plan, JOB);

        assertEquals(Duration.ofMillis(1500), runtime.leaderBudget);
        assertEquals(List.of(Duration.ofSeconds(2), Duration.ofMillis(1250)), flink.haTimeouts);
        assertEquals(Duration.ofSeconds(2), result.leaderFaults().getFirst().raw().request().timeout());
        assertEquals(List.of("observe-job", "fault-leader", "observe-job"), events);
    }

    @Test
    void leaderFaultIsNotInjectedAfterThePreFaultObservationExhaustsItsBudget() throws Exception {
        ExecutableScenarioPlan plan = plan(ExecutablePhaseExecutorTest::addLeaderFault);
        List<String> events = new ArrayList<>();
        java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
        FakeFlink flink = new FakeFlink(events);
        flink.haObservationCompleted = () -> clock.addAndGet(Duration.ofSeconds(2).toNanos());
        FakeTaskManagers runtime = new FakeTaskManagers(events);

        PhaseExecutionEvidence result = new ExecutablePhaseExecutor(flink, runtime,
                ExecutablePhaseExecutor.NetworkFaults.NONE, duration -> {}, clock::get)
                .execute(plan, JOB);

        assertEquals(List.of("observe-job"), events);
        var fault = result.leaderFaults().getFirst();
        assertFalse(fault.raw().applied());
        assertEquals(Duration.ofSeconds(2), fault.raw().request().timeout());
        assertTrue(fault.raw().errors().stream()
                .anyMatch(error -> error.contains("pre-fault job observation")));
        assertTrue(fault.jobAfter().observation().isEmpty());
    }

    @Test
    void leaderFaultFailureRetainsCauseAndContinuesToLaterSteps() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            addLeaderFault(document);
            ((ArrayNode) document.at("/phases/0/steps")).addObject()
                    .putObject("wait").put("duration", "1ms");
        });
        List<String> events = new ArrayList<>();
        FakeFlink flink = new FakeFlink(events);
        FakeTaskManagers runtime = new FakeTaskManagers(events);
        runtime.leaderFailure = new IllegalStateException("leader operation failed",
                new IOException("proxy could not heal"));
        PhaseExecutionEvidence result = new ExecutablePhaseExecutor(flink, runtime,
                ExecutablePhaseExecutor.NetworkFaults.NONE,
                duration -> events.add("sleep:" + duration)).execute(plan, JOB);

        assertEquals(List.of("observe-job", "fault-leader", "observe-job", "sleep:PT0.001S"), events);
        assertEquals(2, result.steps().size());
        assertEquals(PhaseExecutionEvidence.StepStatus.FAILED, result.steps().getFirst().status());
        assertEquals(PhaseExecutionEvidence.StepStatus.SUCCEEDED, result.steps().getLast().status());
        var retained = result.leaderFaults().getFirst();
        assertEquals(JOB.jobId(), retained.jobId());
        assertEquals("$/phases/0/steps/0", retained.path());
        assertTrue(retained.raw().errors().contains("IOException: proxy could not heal"));
        assertEquals(Optional.of(RUNNING_JOB), retained.jobBefore().observation());
        assertTrue(retained.jobAfter().observation().isPresent());
    }

    @Test
    void leaderRecoveryQueriesSameJobAndRetainsTransientObservationFailure() throws Exception {
        ExecutableScenarioPlan plan = plan(ExecutablePhaseExecutorTest::addLeaderFault);
        List<String> events = new ArrayList<>();
        FakeFlink flink = new FakeFlink(events);
        FlinkJobObservation restored = new FlinkJobObservation(2_000, FlinkJobState.RUNNING,
                0, 1, Optional.of(new FlinkJobObservation.Restore(2, 1_900)), List.of(), List.of());
        flink.haObservations.add(RUNNING_JOB);
        flink.haObservations.add(new IOException("new leader REST not ready"));
        flink.haObservations.add(restored);
        FakeTaskManagers runtime = new FakeTaskManagers(events);
        runtime.leaderEvidence = request -> new FlinkHaControl.LeaderFaultEvidence(request,
                Optional.empty(), Optional.empty(), Optional.empty(), true, true,
                Optional.empty(), Optional.empty(), 1, 2, 0, 0, false,
                Optional.empty(), Optional.empty(), Optional.empty(), List.of());

        PhaseExecutionEvidence result = new ExecutablePhaseExecutor(flink, runtime,
                ExecutablePhaseExecutor.NetworkFaults.NONE, duration -> {}).execute(plan, JOB);

        var fault = result.leaderFaults().getFirst();
        assertEquals(Optional.of(restored), fault.jobAfter().observation());
        assertEquals(List.of("IOException: new leader REST not ready"), fault.observationErrors());
        assertEquals(List.of(JOB, JOB, JOB), flink.haJobs);
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, FlinkHaEvidence.evaluate(
                FlinkHaEvidence.Expected.from(plan.phases(), true, false), Optional.of(result), Optional.empty())
                .outcome(), "a restored job alone must not fabricate physical leadership proof");
        assertTrue(FlinkHaEvidence.faultFailure(fault.raw()).isPresent(),
                "the fault itself lacks physical proof independently of sampled-history validation");
    }

    @Test
    void unconfirmedOptInBarrierStopsLaterActionsAndReturnsEvidenceForTheTerminalOracle() throws Exception {
        ExecutableScenarioPlan plan = plan(document -> {
            addLeaderFault(document);
            ((ObjectNode) document.at("/setup/flink")).putObject("token_provider").put("renewal_interval", "2s");
            ((ObjectNode) document.at("/phases/0/steps/0/leader_fault"))
                    .put("recovery_barrier", "token-checkpoint");
            ((ArrayNode) document.at("/phases/0/steps")).addObject().putObject("wait").put("duration", "1ms");
        });
        List<String> events = new ArrayList<>();
        var result = new ExecutablePhaseExecutor(new FakeFlink(events), new FakeTaskManagers(events),
                ExecutablePhaseExecutor.NetworkFaults.NONE, delay -> events.add("unexpected-sleep"))
                .execute(plan, JOB);
        assertEquals(1, result.steps().size());
        assertEquals(PhaseExecutionEvidence.StepStatus.FAILED, result.steps().getFirst().status());
        assertFalse(events.contains("fault-leader"));
        assertFalse(events.contains("unexpected-sleep"));
        assertTrue(result.leaderFaults().getFirst().raw().errors().stream().anyMatch(error ->
                error.contains("Pre-fault token-checkpoint readiness")));
    }

    private static void addLeaderFault(ObjectNode document) {
        ObjectNode setup = (ObjectNode) document.at("/setup/flink");
        setup.put("jobmanagers", 2);
        setup.putObject("high_availability").put("zookeeper_image", "zookeeper:3.9.3")
                .put("session_timeout", "2s");
        ((ObjectNode) document.at("/workload/jobs/0/checkpointing"))
                .putObject("storage").put("type", "filesystem");
        replaceSteps(document).addObject().putObject("leader_fault")
                .put("mode", "pause").put("duration", "1ms").put("timeout", "2s");
    }

    /** Routes the sink through kafka-proxy; the only step drops a commit request there. */
    private static void addProtocolFault(ObjectNode document) {
        ObjectNode proxy = ((ObjectNode) document.at("/setup")).putObject("proxies")
                .putObject("kafka-proxy");
        proxy.put("type", "kroxylicious");
        proxy.put("cluster", "main");
        proxy.put("listen", "kafka-proxy:9092");
        proxy.putObject("bootstrap").put("cluster", "main");
        ((ObjectNode) document.at("/workload/jobs/0/sink"))
                .put("connect_via_proxy", "kafka-proxy");
        ObjectNode fault = replaceSteps(document).addObject().putObject("network_fault");
        fault.put("proxy", "kafka-proxy");
        fault.putObject("target").put("cluster", "main");
        fault.putObject("match").put("api", "end-txn").put("result", "commit");
        fault.putObject("fault").put("type", "drop-request");
        fault.put("occurrences", 1);
        fault.put("trigger_deadline", "1m");
        fault.put("heal", "restore-proxy-rule");
    }

    private ExecutablePhaseExecutor executor(FakeFlink flink) {
        return new ExecutablePhaseExecutor(
                flink,
                new FakeTaskManagers(new ArrayList<>()),
                ExecutablePhaseExecutor.NetworkFaults.NONE,
                duration -> {});
    }

    private ExecutableScenarioPlan plan(Consumer<ObjectNode> mutation) {
        ScenarioSpecification base = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode document = base.document();
        document.put("health_retry_limit", 0);
        mutation.accept(document);
        Path scenarioPath = temporaryDirectory.resolve("minimal.yaml");
        Path expectedPath = temporaryDirectory.resolve("minimal.expected.yaml");
        try {
            yamlMapper.writeValue(scenarioPath.toFile(), document);
            yamlMapper.writeValue(
                    expectedPath.toFile(),
                    loader.loadExpectedResult(resource("minimal.expected.yaml")).document());
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot write mutated test bundle", exception);
        }
        ScenarioSpecification scenario = loader.loadScenario(scenarioPath);
        ExpectedResultSpecification expected = loader.loadExpectedResult(expectedPath);
        ResolvedScenarioPlan resolved = new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected), ResolutionRequest.none());
        return compiler.compile(resolved);
    }

    private static ArrayNode replaceSteps(ObjectNode document) {
        ArrayNode steps = (ArrayNode) document.at("/phases/0/steps");
        steps.removeAll();
        return steps;
    }

    private static void addAwaitJobState(ArrayNode steps, String onTimeout) {
        ObjectNode await = steps.addObject().putObject("await");
        ObjectNode condition = await.putObject("condition");
        condition.put("type", "job-state");
        condition.put("job", "eos-job");
        condition.put("state", "RUNNING");
        await.put("timeout", "2m");
        await.put("on_timeout", onTimeout);
    }

    private static void addAwaitCheckpoints(
            ArrayNode steps,
            long completed,
            String onTimeout) {
        ObjectNode await = steps.addObject().putObject("await");
        ObjectNode condition = await.putObject("condition");
        condition.put("type", "checkpoint-completed");
        condition.put("job", "eos-job");
        condition.put("count", completed);
        await.put("timeout", "2m");
        await.put("on_timeout", onTimeout);
    }

    private static PhaseExecutionEvidence.LoopIteration frame(
            String path,
            int iteration,
            int total) {
        return new PhaseExecutionEvidence.LoopIteration(path, iteration, total);
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static final class FakeTaskManagers implements V1AttemptRuntime {
        private final List<String> events;
        private IOException killFailure;
        private IOException restartFailure;
        private Duration killTimeout;
        private Duration restartTimeout;
        private final java.util.Map<String, Integer> incarnations = new java.util.HashMap<>();
        private final java.util.Set<String> stopped = new java.util.HashSet<>();
        private RuntimeException leaderFailure;
        private java.util.function.Function<LeaderFaultRequest, LeaderFaultEvidence> leaderEvidence;
        private Duration leaderBudget;
        private boolean packetUnconfirmed;
        private final List<String> producerRequests = new ArrayList<>();
        @Override
        public org.savonitar.flink.stability.runtime.api.KafkaProducerSnapshot observeKafkaProducers(
                String cluster, String topic, int partition, String prefix, Duration timeout) {
            producerRequests.add(cluster + "/" + topic + "/" + partition);
            assertTrue(timeout.compareTo(Duration.ZERO) > 0);
            return new org.savonitar.flink.stability.runtime.api.KafkaProducerSnapshot(topic, partition, 100, List.of(), List.of());
        }
        @Override public org.savonitar.flink.stability.runtime.api.PacketFaultControl.Evidence packetFault(
                org.savonitar.flink.stability.runtime.api.PacketFaultControl.Request request) {
            events.add("packet-fault");
            if (packetUnconfirmed) return org.savonitar.flink.stability.runtime.api.PacketFaultControl.unconfirmed(request,"no counters");
            var target=request.broker();
            var binding=new org.savonitar.flink.stability.runtime.api.PacketFaultControl.Binding("a".repeat(64),"tm-image","b".repeat(64),"broker-image","network","172.18.0.5",19092,
                    new org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Selection(target,"broker-1","output",0,0,null,null,null,1));
            return new org.savonitar.flink.stability.runtime.api.PacketFaultControl.Evidence(request,binding,"sidecar","image","eth0",1,2,request.duration().toNanos(),
                    new org.savonitar.flink.stability.runtime.api.PacketFaultControl.Counters(0,0),
                    new org.savonitar.flink.stability.runtime.api.PacketFaultControl.Counters(10,2),true,true,List.of(),null,List.of());
        }
        private List<org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence> brokerObservations;
        @Override public List<org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence> brokerFault(
                org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Request request,
                List<org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Partition> partitions) { return brokerObservations; }

        private FakeTaskManagers(List<String> events) {
            this.events = events;
        }

        @Override public KafkaRuntimeEndpoints startKafka(KafkaRuntimeTarget target) {
            throw new AssertionError("Phase execution cannot start Kafka");
        }
        @Override public KafkaProxyEndpoint startKafkaProxy(KafkaProxyTarget target) {
            throw new AssertionError("Phase execution cannot start a proxy");
        }
        @Override public String startFlink(FlinkRuntimeTarget target) {
            throw new AssertionError("Phase execution cannot start Flink");
        }
        @Override public FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout) {
            throw new AssertionError("The terminal fence belongs to the scenario executor");
        }
        @Override public List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence() {
            return List.of();
        }
        @Override public List<FlinkClassLoadLog> flinkClassLoadLogs() { return List.of(); }
        @Override public void close() { throw new AssertionError("Phase execution cannot close the runtime"); }

        @Override
        public LeaderFaultEvidence faultLeader(LeaderFaultRequest request, Duration remainingBudget) {
            events.add("fault-leader");
            leaderBudget = remainingBudget;
            if (leaderFailure != null) {
                throw leaderFailure;
            }
            return leaderEvidence.apply(request);
        }

        @Override
        public Optional<TaskManagerControl.Identity> taskManagerIdentity(String targetName) {
            if (stopped.contains(targetName)) {
                return Optional.empty();
            }
            int incarnation = incarnations.getOrDefault(targetName, 1);
            return Optional.of(new TaskManagerControl.Identity(targetName,
                    targetName + "-container-" + incarnation, targetName + "-resource-" + incarnation));
        }

        @Override
        public void killTaskManager(String targetName, Duration timeout) throws IOException {
            events.add("kill:" + targetName);
            killTimeout = timeout;
            if (killFailure != null) {
                throw killFailure;
            }
            stopped.add(targetName);
        }

        @Override
        public void restartTaskManager(Duration timeout) throws IOException {
            restartTaskManager("taskmanager-1", timeout);
        }

        @Override
        public void restartTaskManager(String targetName, Duration timeout) throws IOException {
            events.add("restart:taskmanager");
            restartTimeout = timeout;
            if (restartFailure != null) {
                throw restartFailure;
            }
            incarnations.put(targetName, incarnations.getOrDefault(targetName, 1) + 1);
            stopped.remove(targetName);
        }
    }

    private static final FlinkJobObservation RUNNING_JOB = new FlinkJobObservation(
            1_000, FlinkJobState.RUNNING, 2, 0, Optional.empty(), List.of(), List.of());

    private static final class FakeFlink implements FlinkScenarioControl {
        private final List<String> events;
        private final java.util.Deque<com.fasterxml.jackson.databind.JsonNode> checkpointOverviews = new java.util.ArrayDeque<>();
        private IOException overviewFailure;
        private int overviewCalls;
        @Override
        public com.fasterxml.jackson.databind.JsonNode checkpointOverview(FlinkJobHandle job, Duration timeout) throws IOException {
            overviewCalls++;
            if (overviewFailure != null) throw overviewFailure;
            return checkpointOverviews.isEmpty() ? new ObjectMapper().createObjectNode() : checkpointOverviews.removeFirst();
        }
        private IOException awaitStateFailure;
        private IOException checkpointFailure;
        private IOException observeFailure;
        private IOException clockFailure;
        private int clockFailureCall;
        private int clockCalls;
        private final java.util.Deque<Object> haObservations = new java.util.ArrayDeque<>();
        private final List<FlinkJobHandle> haJobs = new ArrayList<>();
        private final List<Duration> haTimeouts = new ArrayList<>();
        private Runnable haObservationCompleted = () -> {};

        private FakeFlink(List<String> events) {
            this.events = events;
        }

        @Override
        public String uploadJar(Path jar, String expectedSha256) {
            throw new AssertionError("not used by phase execution");
        }

        @Override
        public FlinkJobHandle submit(FlinkJobSubmission submission) {
            throw new AssertionError("not used by phase execution");
        }

        @Override
        public FlinkJobState awaitState(
                FlinkJobHandle job,
                FlinkJobState expected,
                Duration timeout) throws IOException {
            events.add("await-state:" + expected + ":" + timeout);
            if (awaitStateFailure != null) {
                throw awaitStateFailure;
            }
            return expected;
        }

        @Override
        public long awaitCompletedCheckpoints(
                FlinkJobHandle job,
                long minimumCompleted,
                Duration timeout) throws IOException {
            events.add("await-checkpoints:" + minimumCompleted + ":" + timeout);
            if (checkpointFailure != null) {
                throw checkpointFailure;
            }
            return minimumCompleted;
        }

        @Override
        public FlinkJobState jobState(FlinkJobHandle job) {
            throw new AssertionError("not used by phase execution");
        }

        @Override
        public FlinkJobState awaitFinished(FlinkJobHandle job, Duration timeout) {
            throw new AssertionError("not used by phase execution");
        }

        @Override
        public FlinkJobObservation observe(FlinkJobHandle job, Duration timeout) throws IOException {
            haTimeouts.add(timeout);
            FlinkJobObservation result = observe(job);
            haObservationCompleted.run();
            return result;
        }

        @Override
        public FlinkJobObservation observe(FlinkJobHandle job) throws IOException {
            events.add("observe-job");
            haJobs.add(job);
            Object supplied = haObservations.poll();
            if (supplied instanceof IOException failure) {
                throw failure;
            }
            if (supplied instanceof FlinkJobObservation observation) {
                return observation;
            }
            if (observeFailure != null) {
                throw observeFailure;
            }
            return RUNNING_JOB;
        }

        @Override
        public long jobManagerTimeMillis(FlinkJobHandle job) throws IOException {
            events.add("sample-jobmanager-time");
            clockCalls++;
            if (clockFailure != null && (clockFailureCall == 0 || clockFailureCall == clockCalls)) {
                throw clockFailure;
            }
            return 1_000 + 250L * clockCalls;
        }

        @Override
        public void close() {}
    }
}
