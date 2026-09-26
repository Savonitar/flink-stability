package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/**
 * Immutable ordered evidence for every atomic typed phase step that was attempted, plus the job
 * as observed just before each confirmed TaskManager kill.
 */
public record PhaseExecutionEvidence(
        List<StepEvidence> steps,
        List<TaskManagerKill> taskManagerKills,
        List<NetworkFault> networkFaults) {
    public PhaseExecutionEvidence {
        steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
        taskManagerKills = List.copyOf(Objects.requireNonNull(
                taskManagerKills, "taskManagerKills"));
        networkFaults = List.copyOf(Objects.requireNonNull(networkFaults, "networkFaults"));
    }

    public PhaseExecutionEvidence(List<StepEvidence> steps) {
        this(steps, List.of(), List.of());
    }

    /**
     * A confirmed process exit, its pre-kill job observation, and a JobManager clock sample
     * requested after exit. Recovery events must follow that sample to prove post-kill timing.
     */
    public record TaskManagerKill(
            String path,
            List<LoopIteration> loopIterations,
            String target,
            FlinkJobObservation.Attempt jobBeforeKill,
            OptionalLong jobManagerTimeAfterKill) {
        public TaskManagerKill {
            path = requireNonBlank(path, "path");
            loopIterations = List.copyOf(Objects.requireNonNull(
                    loopIterations, "loopIterations"));
            target = requireNonBlank(target, "target");
            Objects.requireNonNull(jobBeforeKill, "jobBeforeKill");
            Objects.requireNonNull(jobManagerTimeAfterKill, "jobManagerTimeAfterKill");
        }
    }

    /**
     * What one counted network fault did at the proxy (SPEC-004 K6): the proxy image that ran
     * it, when the proxy confirmed the rule armed and healed, each message it dropped, and the
     * errors of matching responses it let through. It triggered only if every requested
     * occurrence completed before the trigger deadline.
     */
    public record NetworkFault(
            String path,
            String faultId,
            String proxy,
            String proxyImage,
            ExecutableScenarioPlan.NetworkFaultAction action,
            int occurrences,
            Duration triggerDeadline,
            long armedAtMillis,
            long healedAtMillis,
            List<DroppedMessage> dropped,
            List<String> forwardedErrors) {
        public NetworkFault {
            path = requireNonBlank(path, "path");
            faultId = requireNonBlank(faultId, "faultId");
            proxy = requireNonBlank(proxy, "proxy");
            proxyImage = requireNonBlank(proxyImage, "proxyImage");
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(triggerDeadline, "triggerDeadline");
            dropped = List.copyOf(Objects.requireNonNull(dropped, "dropped"));
            forwardedErrors = List.copyOf(Objects.requireNonNull(
                    forwardedErrors, "forwardedErrors"));
        }

        public long droppedBeforeDeadline() {
            return dropped.stream().filter(DroppedMessage::beforeDeadline)
                    .filter(message -> action == ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST
                            || message.brokerAnswer()
                                    .filter(answer -> "NONE".equals(answer.error())).isPresent())
                    .count();
        }

        public boolean triggered() {
            return droppedBeforeDeadline() >= occurrences;
        }

        /** The same fault with completed evidence for its dropped messages. */
        public NetworkFault withDropped(List<DroppedMessage> completed) {
            return new NetworkFault(path, faultId, proxy, proxyImage, action, occurrences,
                    triggerDeadline, armedAtMillis, healedAtMillis, completed, forwardedErrors);
        }
    }

    /**
     * One EndTxn message the proxy dropped, and whether the proxy completed it before its
     * monotonic trigger deadline: the client's request and, when the response was dropped, what the
     * broker answered, which the client never saw.
     */
    public record DroppedMessage(
            int occurrence,
            int claim,
            long droppedAtMillis,
            boolean beforeDeadline,
            String transactionalId,
            long producerId,
            short producerEpoch,
            boolean committed,
            Optional<BrokerAnswer> brokerAnswer,
            Optional<Retry> retry) {
        public DroppedMessage {
            transactionalId = requireNonBlank(transactionalId, "transactionalId");
            Objects.requireNonNull(brokerAnswer, "brokerAnswer");
            Objects.requireNonNull(retry, "retry");
        }

        public DroppedMessage withRetry(Retry observed) {
            return new DroppedMessage(occurrence, claim, droppedAtMillis, beforeDeadline,
                    transactionalId, producerId, producerEpoch, committed, brokerAnswer,
                    Optional.of(observed));
        }
    }

    /**
     * The client sent the dropped EndTxn again, with the same transactional ID, producer ID,
     * epoch, and outcome, after the fault healed (SPEC-004 K6.12). Evidence only: it shows the
     * client retried, not that the retry reached the broker.
     */
    public record Retry(long observedAtMillis, String clientId) {
        public Retry {
            Objects.requireNonNull(clientId, "clientId");
        }
    }

    /** The broker's EndTxn response: its error code name and the producer epoch it returned. */
    public record BrokerAnswer(String error, long producerId, short producerEpoch) {
        public BrokerAnswer {
            error = requireNonBlank(error, "error");
        }
    }

    public record StepEvidence(
            int phaseIndex,
            String phaseName,
            String path,
            List<LoopIteration> loopIterations,
            StepKind kind,
            StepStatus status,
            String detail) {
        public StepEvidence {
            if (phaseIndex < 0) {
                throw new IllegalArgumentException("phaseIndex must not be negative");
            }
            phaseName = requireNonBlank(phaseName, "phaseName");
            path = requireNonBlank(path, "path");
            if (!path.startsWith("$/phases/")) {
                throw new IllegalArgumentException("path must identify an exact phase step");
            }
            loopIterations = List.copyOf(Objects.requireNonNull(
                    loopIterations, "loopIterations"));
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(status, "status");
            detail = requireNonBlank(detail, "detail");
        }
    }

    /** One active structural loop and its one-based iteration at the recorded step. */
    public record LoopIteration(String loopPath, int iteration, int totalIterations) {
        public LoopIteration {
            loopPath = requireNonBlank(loopPath, "loopPath");
            if (!loopPath.startsWith("$/phases/")) {
                throw new IllegalArgumentException("loopPath must identify an exact phase loop");
            }
            if (iteration < 1 || totalIterations < 1 || iteration > totalIterations) {
                throw new IllegalArgumentException(
                        "iteration must be within the loop's one-based iteration range");
            }
        }
    }

    public enum StepKind {
        AWAIT_JOB_STATE,
        AWAIT_CHECKPOINTS,
        WAIT,
        KILL_TASKMANAGER,
        RESTART_TASKMANAGER,
        NETWORK_FAULT
    }

    public enum StepStatus {
        SUCCEEDED,
        FAILED
    }
}
