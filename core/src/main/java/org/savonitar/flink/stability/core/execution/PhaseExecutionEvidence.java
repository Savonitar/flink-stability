package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;

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
        List<NetworkFault> networkFaults,
        List<TaskManagerRestart> taskManagerRestarts,
        List<LeaderFault> leaderFaults, List<BrokerOperation> brokerOperations) {
    public PhaseExecutionEvidence {
        steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
        taskManagerKills = List.copyOf(Objects.requireNonNull(
                taskManagerKills, "taskManagerKills"));
        networkFaults = List.copyOf(Objects.requireNonNull(networkFaults, "networkFaults"));
        taskManagerRestarts = List.copyOf(Objects.requireNonNull(
                taskManagerRestarts, "taskManagerRestarts"));
        brokerOperations = List.copyOf(brokerOperations);
        leaderFaults = List.copyOf(Objects.requireNonNull(leaderFaults, "leaderFaults"));
    }

    public record BrokerOperation(String path, List<LoopIteration> loopIterations,
                                  org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence raw) {
        public BrokerOperation { loopIterations = List.copyOf(loopIterations); Objects.requireNonNull(raw); }
    }

    public PhaseExecutionEvidence(List<StepEvidence> steps, List<TaskManagerKill> kills,
            List<NetworkFault> network, List<TaskManagerRestart> restarts, List<LeaderFault> leaders) {
        this(steps, kills, network, restarts, leaders, List.of());
    }

    public PhaseExecutionEvidence(List<StepEvidence> steps,
                                  List<TaskManagerKill> taskManagerKills,
                                  List<NetworkFault> networkFaults,
                                  List<TaskManagerRestart> taskManagerRestarts) {
        this(steps, taskManagerKills, networkFaults, taskManagerRestarts, List.of());
    }

    public PhaseExecutionEvidence(List<StepEvidence> steps,
                                  List<TaskManagerKill> taskManagerKills,
                                  List<NetworkFault> networkFaults) {
        this(steps, taskManagerKills, networkFaults, List.of());
    }

    public PhaseExecutionEvidence(List<StepEvidence> steps) {
        this(steps, List.of(), List.of());
    }

    /** Both observations query this same submitted job ID through the current REST leader. */
    public record LeaderFault(
            String path,
            List<LoopIteration> loopIterations,
            String jobId,
            FlinkJobObservation.Attempt jobBefore,
            FlinkJobObservation.Attempt jobAfter,
            FlinkHaControl.LeaderFaultEvidence raw,
            List<String> observationErrors,
            Optional<TokenCheckpointBarrier.Evidence> recoveryBarrier) {
        public LeaderFault {
            path = requireNonBlank(path, "path");
            loopIterations = List.copyOf(Objects.requireNonNull(loopIterations, "loopIterations"));
            jobId = requireNonBlank(jobId, "jobId");
            Objects.requireNonNull(jobBefore, "jobBefore");
            Objects.requireNonNull(jobAfter, "jobAfter");
            Objects.requireNonNull(raw, "raw");
            observationErrors = List.copyOf(Objects.requireNonNull(observationErrors, "observationErrors"));
            Objects.requireNonNull(recoveryBarrier, "recoveryBarrier");
        }

        public LeaderFault(String path, List<LoopIteration> loopIterations, String jobId,
                           FlinkJobObservation.Attempt jobBefore, FlinkJobObservation.Attempt jobAfter,
                           FlinkHaControl.LeaderFaultEvidence raw, List<String> observationErrors) {
            this(path, loopIterations, jobId, jobBefore, jobAfter, raw, observationErrors, Optional.empty());
        }

        public LeaderFault(String path, List<LoopIteration> loopIterations, String jobId,
                           FlinkJobObservation.Attempt jobBefore, FlinkJobObservation.Attempt jobAfter,
                           FlinkHaControl.LeaderFaultEvidence raw) {
            this(path, loopIterations, jobId, jobBefore, jobAfter, raw, List.of());
        }
    }

    /**
     * A confirmed process exit, its baseline job observation, and fresh JobManager clock samples
     * requested after baseline/identity collection immediately before injection and after exit.
     */
    public record TaskManagerKill(
            String path,
            List<LoopIteration> loopIterations,
            String target,
            FlinkJobObservation.Attempt jobBeforeKill,
            OptionalLong jobManagerTimeBeforeKill,
            OptionalLong jobManagerTimeAfterKill,
            Optional<TaskManagerControl.Identity> identity,
            Optional<String> identityFailure) {
        public TaskManagerKill {
            path = requireNonBlank(path, "path");
            loopIterations = List.copyOf(Objects.requireNonNull(
                    loopIterations, "loopIterations"));
            target = requireNonBlank(target, "target");
            Objects.requireNonNull(jobBeforeKill, "jobBeforeKill");
            Objects.requireNonNull(jobManagerTimeBeforeKill, "jobManagerTimeBeforeKill");
            Objects.requireNonNull(jobManagerTimeAfterKill, "jobManagerTimeAfterKill");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(identityFailure, "identityFailure");
            if (identity.isPresent() && identityFailure.isPresent()) {
                throw new IllegalArgumentException("Identity observation cannot both succeed and fail");
            }
        }

        public TaskManagerKill(String path, List<LoopIteration> loopIterations, String target,
                               FlinkJobObservation.Attempt jobBeforeKill,
                               OptionalLong jobManagerTimeBeforeKill,
                               OptionalLong jobManagerTimeAfterKill,
                               Optional<TaskManagerControl.Identity> identity) {
            this(path, loopIterations, target, jobBeforeKill,
                    jobManagerTimeBeforeKill, jobManagerTimeAfterKill,
                    identity, Optional.empty());
        }

        public TaskManagerKill(String path, List<LoopIteration> loopIterations, String target,
                               FlinkJobObservation.Attempt jobBeforeKill,
                               OptionalLong jobManagerTimeBeforeKill,
                               OptionalLong jobManagerTimeAfterKill) {
            this(path, loopIterations, target, jobBeforeKill,
                    jobManagerTimeBeforeKill, jobManagerTimeAfterKill,
                    Optional.empty());
        }
    }

    /** A successful named restart and the physical incarnation it replaced. */
    public record TaskManagerRestart(
            String path,
            List<LoopIteration> loopIterations,
            String target,
            Optional<TaskManagerControl.Identity> previousIdentity,
            Optional<TaskManagerControl.Identity> replacementIdentity,
            Optional<String> previousIdentityFailure,
            Optional<String> replacementIdentityFailure) {
        public TaskManagerRestart {
            path = requireNonBlank(path, "path");
            loopIterations = List.copyOf(Objects.requireNonNull(loopIterations, "loopIterations"));
            target = requireNonBlank(target, "target");
            Objects.requireNonNull(previousIdentity, "previousIdentity");
            Objects.requireNonNull(replacementIdentity, "replacementIdentity");
            Objects.requireNonNull(previousIdentityFailure, "previousIdentityFailure");
            Objects.requireNonNull(replacementIdentityFailure, "replacementIdentityFailure");
            if ((previousIdentity.isPresent() && previousIdentityFailure.isPresent())
                    || (replacementIdentity.isPresent() && replacementIdentityFailure.isPresent())) {
                throw new IllegalArgumentException("Identity observation cannot both succeed and fail");
            }
        }

        public TaskManagerRestart(String path, List<LoopIteration> loopIterations, String target,
                                  Optional<TaskManagerControl.Identity> previousIdentity,
                                  Optional<TaskManagerControl.Identity> replacementIdentity) {
            this(path, loopIterations, target, previousIdentity, replacementIdentity,
                    Optional.empty(), Optional.empty());
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
            List<String> forwardedErrors,
            String api,
            List<ProtocolMessage> affected) {
        public NetworkFault(String path, String faultId, String proxy, String proxyImage,
                            ExecutableScenarioPlan.NetworkFaultAction action, int occurrences, Duration triggerDeadline,
                            long armedAtMillis, long healedAtMillis, List<DroppedMessage> dropped, List<String> forwardedErrors) {
            this(path, faultId, proxy, proxyImage, action, occurrences, triggerDeadline, armedAtMillis, healedAtMillis,
                    dropped, forwardedErrors, "end-txn", List.of());
        }
        public NetworkFault {
            api = requireNonBlank(api, "api");
            affected = List.copyOf(affected);
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
            if (!affected.isEmpty() || !"end-txn".equals(api)
                    || (action != ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST
                        && action != ExecutableScenarioPlan.NetworkFaultAction.DROP_RESPONSE)) {
                return affected.stream().filter(message -> api.equals(message.api()) && message.qualifies(action))
                        .filter(message -> message.occurrence() > 0 && message.occurrence() <= occurrences)
                        .map(ProtocolMessage::claim).distinct().count() >= occurrences;
            }
            return droppedBeforeDeadline() >= occurrences;
        }

        /** The same fault with completed evidence for its dropped messages. */
        public NetworkFault withDropped(List<DroppedMessage> completed) {
            return new NetworkFault(path, faultId, proxy, proxyImage, action, occurrences,
                    triggerDeadline, armedAtMillis, healedAtMillis, completed, forwardedErrors, api, affected);
        }
    }

    /** One affected protocol message. Null identity/error fields mean absent on the wire, never guessed. */
    public record ProtocolMessage(int occurrence, int claim, long timeMillis, boolean beforeDeadline,
                                  String event, String api, short apiVersion, int correlationId,
                                  String transactionalId, Long producerId, Short producerEpoch,
                                  Boolean committed, java.util.Map<String, Short> originalErrorCodes,
                                  java.util.Map<String, Long> originalBaseOffsets,
                                  Short substitutedErrorCode, boolean forwardedToBroker,
                                  long requestedDelayMillis, long actualDelayNanos) {
        public ProtocolMessage {
            originalErrorCodes = java.util.Map.copyOf(originalErrorCodes);
            originalBaseOffsets = java.util.Map.copyOf(originalBaseOffsets);
            transactionalId = requireNonBlank(transactionalId, "transactionalId");
        }
        boolean qualifies(ExecutableScenarioPlan.NetworkFaultAction action) {
            if (!beforeDeadline) return false;
            return switch (action) {
                case DROP_REQUEST -> "request-dropped".equals(event) && !forwardedToBroker;
                case DROP_RESPONSE -> "response-dropped".equals(event) && forwardedToBroker
                        && !originalErrorCodes.isEmpty() && originalErrorCodes.values().stream().allMatch(code -> code == 0);
                case DELAY -> "request-delayed".equals(event) && forwardedToBroker && requestedDelayMillis > 0
                        && actualDelayNanos >= java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(requestedDelayMillis);
                case ERROR_AFTER_APPEND -> "response-error-after-append".equals(event) && "produce".equals(api)
                        && forwardedToBroker && producerId != null && producerId >= 0 && producerEpoch != null && producerEpoch >= 0
                        && substitutedErrorCode != null && substitutedErrorCode == org.apache.kafka.common.protocol.Errors.REQUEST_TIMED_OUT.code()
                        && !originalErrorCodes.isEmpty() && originalErrorCodes.values().stream().allMatch(code -> code == 0)
                        && originalErrorCodes.keySet().equals(originalBaseOffsets.keySet())
                        && originalBaseOffsets.values().stream().allMatch(offset -> offset >= 0);
                case ERROR_RESPONSE -> "response-substituted".equals(event) && substitutedErrorCode != null && !forwardedToBroker && originalErrorCodes.isEmpty();
            };
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
        BROKER_FAULT, KILL_BROKER,
        RESTART_BROKER,
        NETWORK_FAULT,
        LEADER_FAULT
    }

    public enum StepStatus {
        SUCCEEDED,
        FAILED
    }
}
