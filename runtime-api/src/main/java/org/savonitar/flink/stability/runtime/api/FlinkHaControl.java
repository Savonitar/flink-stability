package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Bounded leadership faults and observations from the attempt-owned ZooKeeper ensemble. */
public interface FlinkHaControl {
    Duration TOKEN_HEAL_TIMEOUT = Duration.ofSeconds(5);
    Duration PROCESS_HEAL_TIMEOUT = Duration.ofSeconds(30);

    default String currentFlinkRestEndpoint(Duration timeout) {
        throw new UnsupportedOperationException("Dynamic Flink REST routing is unsupported");
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

    enum Mode { KILL, PAUSE, ISOLATE_ZOOKEEPER }

    record TokenFault(TokenServiceControl.Mode mode, Duration delay) {
        public TokenFault {
            Objects.requireNonNull(mode, "mode");
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

    record ProcessState(String runtimeId, boolean running, boolean paused) {
        public ProcessState {
            runtimeId = requireNonBlank(runtimeId, "runtimeId");
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
