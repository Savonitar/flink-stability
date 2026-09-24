package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.TaskManagerActionTimeoutException;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;

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
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                new FakeFlink(events), taskManagers, duration -> {});

        PhaseExecutionEvidence evidence = executor.execute(plan, JOB);

        assertEquals(
                List.of("observe-job", "kill:taskmanager-1", "restart:taskmanager"), events);
        assertEquals(1, evidence.taskManagerKills().size());
        PhaseExecutionEvidence.TaskManagerKill kill = evidence.taskManagerKills().getFirst();
        assertEquals("$/phases/0/steps/0", kill.path());
        assertEquals("taskmanager-1", kill.target());
        assertEquals(Optional.of(RUNNING_JOB), kill.jobBeforeKill().observation());
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
                List.of("observe-job", "kill:taskmanager-1", "restart:taskmanager"), events);
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

        assertEquals(List.of("observe-job", "kill:taskmanager-1"), events);
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
                flink, new FakeTaskManagers(events), duration -> {});

        PhaseExecutionEvidence evidence = executor.execute(plan, JOB);

        assertEquals(
                List.of("observe-job", "kill:taskmanager-1", "restart:taskmanager"), events);
        FlinkJobObservation.Attempt observed =
                evidence.taskManagerKills().getFirst().jobBeforeKill();
        assertTrue(observed.observation().isEmpty());
        assertEquals(Optional.of("IOException: REST unavailable"), observed.failure());
    }

    private ExecutablePhaseExecutor executor(FakeFlink flink) {
        return new ExecutablePhaseExecutor(
                flink,
                new FakeTaskManagers(new ArrayList<>()),
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

    private static final class FakeTaskManagers implements TaskManagerControl {
        private final List<String> events;
        private IOException killFailure;
        private IOException restartFailure;
        private Duration killTimeout;
        private Duration restartTimeout;

        private FakeTaskManagers(List<String> events) {
            this.events = events;
        }

        @Override
        public void killTaskManager(String targetName, Duration timeout) throws IOException {
            events.add("kill:" + targetName);
            killTimeout = timeout;
            if (killFailure != null) {
                throw killFailure;
            }
        }

        @Override
        public void restartTaskManager(Duration timeout) throws IOException {
            events.add("restart:taskmanager");
            restartTimeout = timeout;
            if (restartFailure != null) {
                throw restartFailure;
            }
        }
    }

    private static final FlinkJobObservation RUNNING_JOB = new FlinkJobObservation(
            1_000, FlinkJobState.RUNNING, 2, 0, Optional.empty(), List.of(), List.of());

    private static final class FakeFlink implements FlinkScenarioControl {
        private final List<String> events;
        private IOException awaitStateFailure;
        private IOException checkpointFailure;
        private IOException observeFailure;

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
        public FlinkJobObservation observe(FlinkJobHandle job) throws IOException {
            events.add("observe-job");
            if (observeFailure != null) {
                throw observeFailure;
            }
            return RUNNING_JOB;
        }

        @Override
        public void close() {}
    }
}
