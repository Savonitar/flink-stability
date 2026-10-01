package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Bounded leadership faults and observations from the attempt-owned ZooKeeper ensemble. */
public interface FlinkHaControl {
    Duration TOKEN_HEAL_TIMEOUT = Duration.ofSeconds(5);
    Duration PROCESS_HEAL_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Resolves an owned endpoint within the caller's remaining operation budget.
     * Only expiration of that budget is a {@link TimeoutException}; other discovery
     * failures must retain their original classification and cause.
     */
    default String currentFlinkRestEndpoint(Duration timeout) throws TimeoutException {
        throw new UnsupportedOperationException("Dynamic Flink REST routing is unsupported");
    }

    /** Bounded observations already made by startup, routing, faults and the pre-fence boundary. */
    default Optional<Observations> haObservations() {
        return Optional.empty();
    }

    enum ObservationMoment { INITIAL, ROUTING, FAULT, PRE_FENCE }

    record LeadershipObservation(long sequence, long sampleCount,
                                 long firstObservedAtMillis, long lastObservedAtMillis,
                                 ObservationMoment moment, Optional<Leadership> leadership,
                                 Optional<String> error) {
        public LeadershipObservation {
            if (sequence < 1 || sampleCount < 1) {
                throw new IllegalArgumentException("Observation sequence and count must be positive");
            }
            Objects.requireNonNull(moment, "moment");
            Objects.requireNonNull(leadership, "leadership");
            Objects.requireNonNull(error, "error");
            if (leadership.isPresent() && error.isPresent()) {
                throw new IllegalArgumentException("A failed observation has no confirmed leadership");
            }
        }
    }

    /** One session establishment message from the identified Flink container's own stdout. */
    record NegotiatedSession(String sessionId, long timeoutMillis, String logLine) {
        public NegotiatedSession {
            sessionId = requireNonBlank(sessionId, "sessionId");
            logLine = requireNonBlank(logLine, "logLine");
            if (timeoutMillis < 1) throw new IllegalArgumentException("Session timeout must be positive");
        }
    }

    record SessionEvidence(String logicalName, FlinkComponentRole role, String runtimeId,
                           String classLoadProcess, long requestedTimeoutMillis,
                           List<NegotiatedSession> negotiated, boolean overflow) {
        public SessionEvidence {
            logicalName = requireNonBlank(logicalName, "logicalName");
            Objects.requireNonNull(role, "role");
            runtimeId = requireNonBlank(runtimeId, "runtimeId");
            classLoadProcess = requireNonBlank(classLoadProcess, "classLoadProcess");
            if (requestedTimeoutMillis < 1) throw new IllegalArgumentException("Session timeout must be positive");
            negotiated = List.copyOf(Objects.requireNonNull(negotiated, "negotiated"));
        }
    }

    /** Not a continuous election history: intervals between samples remain unobserved. */
    record Observations(List<LeadershipObservation> leadership, List<SessionEvidence> sessions,
                        boolean overflow) {
        public Observations {
            leadership = List.copyOf(Objects.requireNonNull(leadership, "leadership"));
            sessions = List.copyOf(Objects.requireNonNull(sessions, "sessions"));
        }
    }

    /**
     * The requested timeout bounds fault work and leadership observation. Safety cleanup has
     * separate budgets: at most five seconds for the token service, then thirty seconds for
     * the process or gate. Both heals are attempted; cleanup never extends the observation
     * deadline or turns its timeout into a confirmed transfer.
     */
    default LeaderFaultEvidence faultLeader(LeaderFaultRequest request) {
        return faultLeader(request, request.timeout());
    }

    /**
     * Uses only the remaining part of the caller's shared observation/fault budget. The raw
     * evidence retains the original declaration; the budget must be positive and no greater
     * than its timeout. Independent safety-cleanup budgets still apply.
     */
    default LeaderFaultEvidence faultLeader(LeaderFaultRequest request, Duration remainingBudget) {
        throw new UnsupportedOperationException("JobManager leadership faults are unsupported");
    }

    default LeaderFaultEvidence faultLeader(LeaderFaultRequest request, Duration remainingBudget,
                                            TokenServiceControl.JobTarget target) {
        throw new UnsupportedOperationException("Submitted-job token faults are unsupported");
    }

    enum Mode { KILL, PAUSE, ISOLATE_ZOOKEEPER }

    record TokenFault(TokenServiceControl.Mode mode, Duration delay, boolean submittedJob) {
        public TokenFault(TokenServiceControl.Mode mode, Duration delay) {
            this(mode, delay, false);
        }
        public TokenFault {
            Objects.requireNonNull(mode, "mode");
            if (submittedJob && mode != TokenServiceControl.Mode.DELAY && mode != TokenServiceControl.Mode.FAIL) {
                throw new IllegalArgumentException("Submitted-job targeting supports delay and fail only");
            }
            if (Objects.requireNonNull(delay, "delay").isNegative()) {
                throw new IllegalArgumentException("delay must be non-negative");
            }
        }
    }

    record LeaderFaultRequest(Mode mode, Duration duration, Duration timeout,
                              Optional<TokenFault> tokenFault) {
        public LeaderFaultRequest {
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(tokenFault, "tokenFault");
            if (Objects.requireNonNull(duration, "duration").isNegative() || duration.isZero()
                    || Objects.requireNonNull(timeout, "timeout").compareTo(duration) <= 0) {
                throw new IllegalArgumentException("Require a positive hold duration shorter than timeout");
            }
        }
    }

    record LeaderIdentity(String logicalName, String runtimeId, String address, String sessionId) {
        public LeaderIdentity {
            logicalName = requireNonBlank(logicalName, "logicalName");
            runtimeId = requireNonBlank(runtimeId, "runtimeId");
            address = requireNonBlank(address, "address");
            sessionId = requireNonBlank(sessionId, "sessionId");
        }
    }

    record Leadership(LeaderIdentity resourceManager, LeaderIdentity dispatcher,
                      LeaderIdentity restServer) {
        public Leadership {
            Objects.requireNonNull(resourceManager, "resourceManager");
            Objects.requireNonNull(dispatcher, "dispatcher");
            Objects.requireNonNull(restServer, "restServer");
        }
    }

    record ProcessState(String runtimeId, boolean running, boolean paused,
                        Optional<Long> exitCode, Optional<Boolean> oomKilled,
                        Optional<String> finishedAt) {
        public ProcessState {
            runtimeId = requireNonBlank(runtimeId, "runtimeId");
            Objects.requireNonNull(exitCode, "exitCode");
            Objects.requireNonNull(oomKilled, "oomKilled");
            finishedAt = Objects.requireNonNull(finishedAt, "finishedAt")
                    .map(value -> requireNonBlank(value, "finishedAt"));
            if (running && (exitCode.isPresent() || finishedAt.isPresent())) {
                throw new IllegalArgumentException("A running process has no observed exit");
            }
        }

        public ProcessState(String runtimeId, boolean running, boolean paused) {
            this(runtimeId, running, paused, Optional.empty(), Optional.empty(), Optional.empty());
        }
    }

    /** Missing observations and operation errors are retained for fail-closed evaluation. */
    record LeaderFaultEvidence(
            LeaderFaultRequest request,
            Optional<Leadership> before,
            Optional<Leadership> after,
            Optional<LeaderIdentity> target,
            boolean applied,
            boolean healed,
            Optional<ProcessState> faultState,
            Optional<ProcessState> healedState,
            long armedAtMillis,
            long healedAtMillis,
            long closedConnections,
            long rejectedConnections,
            boolean isolationActiveAfterHeal,
            Optional<TokenServiceControl.Snapshot> tokensBefore,
            Optional<TokenServiceControl.Snapshot> tokensDuring,
            Optional<TokenServiceControl.Snapshot> tokensAfter,
            List<String> errors) {
        public LeaderFaultEvidence {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(before, "before");
            Objects.requireNonNull(after, "after");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(faultState, "faultState");
            Objects.requireNonNull(healedState, "healedState");
            Objects.requireNonNull(tokensBefore, "tokensBefore");
            Objects.requireNonNull(tokensDuring, "tokensDuring");
            Objects.requireNonNull(tokensAfter, "tokensAfter");
            errors = List.copyOf(errors);
        }
    }
}
