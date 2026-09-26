package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.flink.FlinkRestTimeoutException;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.TaskManagerActionTimeoutException;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Executes the compiler-approved v1 phase subset in exact document order. */
public final class ExecutablePhaseExecutor {
    public static final Duration TASKMANAGER_ACTION_TIMEOUT = Duration.ofMinutes(2);
    public static final String AWAIT_JOB_STATE_TIMEOUT = "await.job-state.timeout";
    public static final String AWAIT_CHECKPOINT_TIMEOUT =
            "await.checkpoint-completed.timeout";
    public static final String AWAIT_JOB_STATE_INFRASTRUCTURE =
            "await.job-state.infrastructure";
    public static final String AWAIT_CHECKPOINT_INFRASTRUCTURE =
            "await.checkpoint-completed.infrastructure";
    public static final String WAIT_INFRASTRUCTURE = "wait.infrastructure";
    public static final String TASKMANAGER_KILL_INFRASTRUCTURE =
            "taskmanager.kill.infrastructure";
    public static final String TASKMANAGER_KILL_TIMEOUT = "taskmanager.kill.timeout";
    /** A passing oracle, but a kill that did not observably disrupt the job (R6.12a). */
    public static final String TASKMANAGER_KILL_EFFECT_UNCONFIRMED =
            "taskmanager.kill.effect-unconfirmed";
    public static final String TASKMANAGER_RESTART_INFRASTRUCTURE =
            "taskmanager.restart.infrastructure";
    public static final String TASKMANAGER_RESTART_TIMEOUT = "taskmanager.restart.timeout";
    /** The proxy could not arm or heal a network fault, or its evidence was unreadable. */
    public static final String NETWORK_FAULT_INFRASTRUCTURE = "network-fault.infrastructure";
    /** A passing oracle, but a network fault missed occurrences by its trigger deadline. */
    public static final String NETWORK_FAULT_TRIGGER_MISSED = "network-fault.trigger-missed";

    private final FlinkScenarioControl flink;
    private final TaskManagerControl taskManagers;
    private final NetworkFaults networkFaults;
    private final PhaseSleeper sleeper;

    public ExecutablePhaseExecutor(
            FlinkScenarioControl flink,
            TaskManagerControl taskManagers,
            NetworkFaults networkFaults) {
        this(flink, taskManagers, networkFaults, ExecutablePhaseExecutor::sleep);
    }

    public ExecutablePhaseExecutor(
            FlinkScenarioControl flink,
            TaskManagerControl taskManagers,
            NetworkFaults networkFaults,
            PhaseSleeper sleeper) {
        this.flink = Objects.requireNonNull(flink, "flink");
        this.taskManagers = Objects.requireNonNull(taskManagers, "taskManagers");
        this.networkFaults = Objects.requireNonNull(networkFaults, "networkFaults");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    /** Injects one EndTxn fault at the proxy and reports what it did. */
    @FunctionalInterface
    public interface NetworkFaults {
        /** For plans without a proxy, which the compiler never gives a network fault step. */
        NetworkFaults NONE = (path, fault) -> {
            throw new IllegalStateException("The plan declares no Kafka proxy");
        };

        PhaseExecutionEvidence.NetworkFault inject(
                String path,
                ExecutableScenarioPlan.EndTxnFault fault) throws IOException, InterruptedException;

        /** Completes a fault's evidence once no client can send again (SPEC-004 K6.12). */
        default PhaseExecutionEvidence.NetworkFault withObservedRetries(
                PhaseExecutionEvidence.NetworkFault fault) throws IOException {
            return fault;
        }
    }

    public PhaseExecutionEvidence execute(
            ExecutableScenarioPlan plan,
            FlinkJobHandle job) throws PhaseExecutionException {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(job, "job");
        Recorder evidence = new Recorder();
        for (int phaseIndex = 0; phaseIndex < plan.phases().size(); phaseIndex++) {
            ExecutableScenarioPlan.Phase phase = plan.phases().get(phaseIndex);
            executeSteps(
                    phaseIndex,
                    phase.name(),
                    "$/phases/" + phaseIndex + "/steps",
                    phase.steps(),
                    List.of(),
                    job,
                    evidence);
        }
        return evidence.snapshot();
    }

    private void executeSteps(
            int phaseIndex,
            String phaseName,
            String stepsPath,
            List<ExecutableScenarioPlan.Step> steps,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            FlinkJobHandle job,
            Recorder evidence)
            throws PhaseExecutionException {
        for (int stepIndex = 0; stepIndex < steps.size(); stepIndex++) {
            ExecutableScenarioPlan.Step step = steps.get(stepIndex);
            String path = stepsPath + "/" + stepIndex;
            if (step instanceof ExecutableScenarioPlan.AwaitJobState await) {
                awaitJobState(
                        phaseIndex, phaseName, path, loopIterations, job, await, evidence);
            } else if (step instanceof ExecutableScenarioPlan.AwaitCheckpoints await) {
                awaitCheckpoints(
                        phaseIndex, phaseName, path, loopIterations, job, await, evidence);
            } else if (step instanceof ExecutableScenarioPlan.Wait wait) {
                wait(phaseIndex, phaseName, path, loopIterations, wait, evidence);
            } else if (step instanceof ExecutableScenarioPlan.KillTaskManager kill) {
                killTaskManager(
                        phaseIndex, phaseName, path, loopIterations, job, kill, evidence);
            } else if (step instanceof ExecutableScenarioPlan.RestartTaskManager) {
                restartTaskManager(
                        phaseIndex, phaseName, path, loopIterations, evidence);
            } else if (step instanceof ExecutableScenarioPlan.EndTxnFault fault) {
                injectNetworkFault(
                        phaseIndex, phaseName, path, loopIterations, fault, evidence);
            } else if (step instanceof ExecutableScenarioPlan.Loop loop) {
                executeLoop(
                        phaseIndex,
                        phaseName,
                        path,
                        loopIterations,
                        loop,
                        job,
                        evidence);
            } else {
                throw new IllegalStateException(
                        "Unsupported typed phase step " + step.getClass().getName());
            }
        }
    }

    private void awaitJobState(
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            FlinkJobHandle job,
            ExecutableScenarioPlan.AwaitJobState await,
            Recorder evidence)
            throws PhaseExecutionException {
        try {
            FlinkJobState observed = flink.awaitState(
                    job, FlinkJobState.RUNNING, await.timeout());
            if (observed != FlinkJobState.RUNNING) {
                throw new IOException("Flink returned " + observed + " while awaiting RUNNING");
            }
            succeeded(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.AWAIT_JOB_STATE,
                    "state=" + observed);
        } catch (FlinkRestTimeoutException timeout) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.AWAIT_JOB_STATE,
                    timeoutOutcome(await.onTimeout()),
                    AWAIT_JOB_STATE_TIMEOUT,
                    timeout);
        } catch (Exception failure) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.AWAIT_JOB_STATE,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    AWAIT_JOB_STATE_INFRASTRUCTURE,
                    failure);
        }
    }

    private void awaitCheckpoints(
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            FlinkJobHandle job,
            ExecutableScenarioPlan.AwaitCheckpoints await,
            Recorder evidence)
            throws PhaseExecutionException {
        try {
            long completed = flink.awaitCompletedCheckpoints(
                    job, await.completedCount(), await.timeout());
            if (completed < await.completedCount()) {
                throw new IOException("Flink returned " + completed + " completed checkpoints; "
                        + "expected at least " + await.completedCount());
            }
            succeeded(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.AWAIT_CHECKPOINTS,
                    "completed=" + completed);
        } catch (FlinkRestTimeoutException timeout) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.AWAIT_CHECKPOINTS,
                    timeoutOutcome(await.onTimeout()),
                    AWAIT_CHECKPOINT_TIMEOUT,
                    timeout);
        } catch (Exception failure) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.AWAIT_CHECKPOINTS,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    AWAIT_CHECKPOINT_INFRASTRUCTURE,
                    failure);
        }
    }

    private void wait(
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            ExecutableScenarioPlan.Wait wait,
            Recorder evidence)
            throws PhaseExecutionException {
        try {
            sleeper.sleep(wait.duration());
            succeeded(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.WAIT,
                    "duration=" + wait.duration());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.WAIT,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    WAIT_INFRASTRUCTURE,
                    interrupted);
        } catch (RuntimeException failure) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.WAIT,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    WAIT_INFRASTRUCTURE,
                    failure);
        }
    }

    private void killTaskManager(
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            FlinkJobHandle job,
            ExecutableScenarioPlan.KillTaskManager kill,
            Recorder evidence)
            throws PhaseExecutionException {
        // Observe first: whether the kill affected the job is judged against this baseline.
        FlinkJobObservation.Attempt jobBeforeKill = FlinkJobObservation.Attempt.of(flink, job);
        try {
            taskManagers.killTaskManager(kill.targetName(), TASKMANAGER_ACTION_TIMEOUT);
            evidence.kills.add(new PhaseExecutionEvidence.TaskManagerKill(
                    path, loopIterations, kill.targetName(), jobBeforeKill,
                    jobManagerTimeAfterKill(job)));
            succeeded(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER,
                    "target=" + kill.targetName());
        } catch (TaskManagerActionTimeoutException timeout) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    TASKMANAGER_KILL_TIMEOUT,
                    timeout);
        } catch (Exception failure) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    TASKMANAGER_KILL_INFRASTRUCTURE,
                    failure);
        }
    }

    private OptionalLong jobManagerTimeAfterKill(FlinkJobHandle job) {
        try {
            return OptionalLong.of(flink.jobManagerTimeMillis(job));
        } catch (IOException | RuntimeException unavailable) {
            // The injection succeeded. Missing timing evidence must not skip a later restart,
            // but it cannot confirm that a failure or restore followed the injection.
            return OptionalLong.empty();
        }
    }

    private void restartTaskManager(
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            Recorder evidence)
            throws PhaseExecutionException {
        try {
            taskManagers.restartTaskManager(TASKMANAGER_ACTION_TIMEOUT);
            succeeded(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER,
                    "component=taskmanager");
        } catch (TaskManagerActionTimeoutException timeout) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    TASKMANAGER_RESTART_TIMEOUT,
                    timeout);
        } catch (Exception failure) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    TASKMANAGER_RESTART_INFRASTRUCTURE,
                    failure);
        }
    }

    private void injectNetworkFault(
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            ExecutableScenarioPlan.EndTxnFault fault,
            Recorder evidence)
            throws PhaseExecutionException {
        try {
            PhaseExecutionEvidence.NetworkFault observed = networkFaults.inject(path, fault);
            evidence.networkFaults.add(observed);
            succeeded(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.NETWORK_FAULT,
                    "fault=" + observed.faultId() + " dropped=" + observed.dropped().size()
                            + "/" + observed.occurrences());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.NETWORK_FAULT,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    NETWORK_FAULT_INFRASTRUCTURE,
                    interrupted);
        } catch (Exception failure) {
            throw failed(
                    evidence,
                    phaseIndex,
                    phaseName,
                    path,
                    loopIterations,
                    PhaseExecutionEvidence.StepKind.NETWORK_FAULT,
                    PhaseExecutionException.Outcome.INCONCLUSIVE,
                    NETWORK_FAULT_INFRASTRUCTURE,
                    failure);
        }
    }

    private void executeLoop(
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> outerIterations,
            ExecutableScenarioPlan.Loop loop,
            FlinkJobHandle job,
            Recorder evidence)
            throws PhaseExecutionException {
        for (int iteration = 1; iteration <= loop.times(); iteration++) {
            List<PhaseExecutionEvidence.LoopIteration> iterations =
                    new ArrayList<>(outerIterations);
            iterations.add(new PhaseExecutionEvidence.LoopIteration(
                    path, iteration, loop.times()));
            executeSteps(
                    phaseIndex,
                    phaseName,
                    path + "/loop/steps",
                    loop.steps(),
                    List.copyOf(iterations),
                    job,
                    evidence);
        }
    }

    private static void succeeded(
            Recorder evidence,
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            PhaseExecutionEvidence.StepKind kind,
            String detail) {
        evidence.steps.add(new PhaseExecutionEvidence.StepEvidence(
                phaseIndex,
                phaseName,
                path,
                loopIterations,
                kind,
                PhaseExecutionEvidence.StepStatus.SUCCEEDED,
                detail));
    }

    private static PhaseExecutionException failed(
            Recorder evidence,
            int phaseIndex,
            String phaseName,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            PhaseExecutionEvidence.StepKind kind,
            PhaseExecutionException.Outcome outcome,
            String reason,
            Throwable cause) {
        evidence.steps.add(new PhaseExecutionEvidence.StepEvidence(
                phaseIndex,
                phaseName,
                path,
                loopIterations,
                kind,
                PhaseExecutionEvidence.StepStatus.FAILED,
                reason));
        return new PhaseExecutionException(
                outcome,
                reason,
                path,
                loopIterations,
                evidence.snapshot(),
                cause);
    }

    private static PhaseExecutionException.Outcome timeoutOutcome(
            ExecutableScenarioPlan.TimeoutOutcome outcome) {
        return switch (outcome) {
            case FAIL -> PhaseExecutionException.Outcome.FAIL;
            case INCONCLUSIVE -> PhaseExecutionException.Outcome.INCONCLUSIVE;
        };
    }

    static void sleep(Duration duration) throws InterruptedException {
        Thread.sleep(duration.toMillis(), duration.toNanosPart() % 1_000_000);
    }

    /** Evidence accumulated by one {@link #execute} call. */
    private static final class Recorder {
        private final List<PhaseExecutionEvidence.StepEvidence> steps = new ArrayList<>();
        private final List<PhaseExecutionEvidence.TaskManagerKill> kills = new ArrayList<>();
        private final List<PhaseExecutionEvidence.NetworkFault> networkFaults = new ArrayList<>();

        private PhaseExecutionEvidence snapshot() {
            return new PhaseExecutionEvidence(steps, kills, networkFaults);
        }
    }
}
