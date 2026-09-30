package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Independent proof that every declared leader fault and optional token fault actually occurred. */
public record FlinkHaEvidence(
        Expected expected, Optional<TokenEvidence> tokens,
        Optional<FlinkHaControl.Observations> observations, Outcome outcome, String detail) {
    public static final String TOKEN_CONTAINER_PATH =
            "/opt/flink/plugins/flink-stability-token/flink-stability-token.jar";
    public static final String TOKEN_PROVIDER =
            "org.savonitar.flink.stability.token.SyntheticDelegationTokenProvider";
    public static final String TOKEN_RECEIVER =
            "org.savonitar.flink.stability.token.SyntheticDelegationTokenReceiver";
    public static final List<String> TOKEN_CLASSES = List.of(TOKEN_PROVIDER, TOKEN_RECEIVER);

    public enum Outcome { CONFIRMED, UNCONFIRMED }

    public FlinkHaEvidence {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(tokens, "tokens");
        Objects.requireNonNull(observations, "observations");
        Objects.requireNonNull(outcome, "outcome");
        detail = requireNonBlank(detail, "detail");
    }

    /** Retained independently of observations, including faults not reached by execution. */
    public record Expected(List<DeclaredFault> faults, boolean haRequired, boolean tokenProviderRequired,
                           int expectedTaskManagers, Optional<TokenScopeProof.Requirement> tokenProof) {
        public Expected {
            faults = List.copyOf(Objects.requireNonNull(faults, "faults"));
            Objects.requireNonNull(tokenProof, "tokenProof");
            if (tokenProof.isPresent() && !tokenProviderRequired) {
                throw new IllegalArgumentException("Scoped token proof requires a token provider");
            }
        }

        public Expected(List<DeclaredFault> faults, boolean haRequired, boolean tokenProviderRequired,
                        int expectedTaskManagers) {
            this(faults, haRequired, tokenProviderRequired, expectedTaskManagers, Optional.empty());
        }

        Expected withTokenProof(Optional<TokenScopeProof.Requirement> proof) {
            return new Expected(faults, haRequired, tokenProviderRequired, expectedTaskManagers, proof);
        }

        public Expected(List<DeclaredFault> faults, boolean haRequired, boolean tokenProviderRequired) {
            this(faults, haRequired, tokenProviderRequired, 0);
        }

        public static Expected from(List<ExecutableScenarioPlan.Phase> phases, boolean haRequired, boolean tokens) {
            return from(phases, haRequired, tokens, 0);
        }

        public static Expected from(List<ExecutableScenarioPlan.Phase> phases, boolean haRequired, boolean tokens,
                                    int taskManagers) {
            List<DeclaredFault> faults = new ArrayList<>();
            for (int index = 0; index < phases.size(); index++) {
                collect(phases.get(index).steps(), "$/phases/" + index + "/steps",
                        List.of(), faults);
            }
            return new Expected(faults, haRequired, tokens, taskManagers);
        }

        private static void collect(List<ExecutableScenarioPlan.Step> steps, String parent,
                                    List<PhaseExecutionEvidence.LoopIteration> iterations,
                                    List<DeclaredFault> faults) {
            for (int index = 0; index < steps.size(); index++) {
                String path = parent + "/" + index;
                var step = steps.get(index);
                if (step instanceof ExecutableScenarioPlan.LeaderFault fault) {
                    if (faults.size() == 100) {
                        throw new IllegalArgumentException("At most 100 expanded leader faults are supported");
                    }
                    faults.add(new DeclaredFault(path, iterations, fault.request(), fault.recoveryBarrier()));
                } else if (step instanceof ExecutableScenarioPlan.Loop loop && containsLeaderFault(loop.steps())) {
                    for (int iteration = 1; iteration <= loop.times(); iteration++) {
                        List<PhaseExecutionEvidence.LoopIteration> nested = new ArrayList<>(iterations);
                        nested.add(new PhaseExecutionEvidence.LoopIteration(path, iteration, loop.times()));
                        collect(loop.steps(), path + "/loop/steps", nested, faults);
                    }
                }
            }
        }

        private static boolean containsLeaderFault(List<ExecutableScenarioPlan.Step> steps) {
            return steps.stream().anyMatch(step -> step instanceof ExecutableScenarioPlan.LeaderFault
                    || step instanceof ExecutableScenarioPlan.Loop loop && containsLeaderFault(loop.steps()));
        }
    }

    public record DeclaredFault(String path, List<PhaseExecutionEvidence.LoopIteration> loopIterations,
                                FlinkHaControl.LeaderFaultRequest request,
                                Optional<ExecutableScenarioPlan.RecoveryBarrier> recoveryBarrier) {
        public DeclaredFault {
            path = requireNonBlank(path, "path");
            loopIterations = List.copyOf(Objects.requireNonNull(loopIterations, "loopIterations"));
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(recoveryBarrier, "recoveryBarrier");
        }

        public DeclaredFault(String path, List<PhaseExecutionEvidence.LoopIteration> loops,
                             FlinkHaControl.LeaderFaultRequest request) {
            this(path, loops, request, Optional.empty());
        }
    }

    /** The runtime verifies these embedded plugin bytes in each accepted provisioned process. */
    public record TokenEvidence(Optional<String> pluginSha256, Optional<SubjectClassOrigins> origins,
                                Optional<TokenServiceControl.Snapshot> snapshot, List<String> errors) {
        public TokenEvidence {
            Objects.requireNonNull(pluginSha256, "pluginSha256");
            Objects.requireNonNull(origins, "origins");
            Objects.requireNonNull(snapshot, "snapshot");
            errors = List.copyOf(Objects.requireNonNull(errors, "errors"));
        }
    }

    public static FlinkHaEvidence evaluate(Expected expected, Optional<PhaseExecutionEvidence> phases,
                                            Optional<TokenEvidence> tokens) {
        return evaluate(expected, phases, tokens, List.of());
    }

    public static FlinkHaEvidence evaluate(Expected expected, Optional<PhaseExecutionEvidence> phases,
                                            Optional<TokenEvidence> tokens,
                                            List<FlinkComponentProvisioningEvidence> provisioning) {
        return evaluate(expected, phases, tokens, provisioning, Optional.empty());
    }

    public static FlinkHaEvidence evaluate(Expected expected, Optional<PhaseExecutionEvidence> phases,
                                            Optional<TokenEvidence> tokens,
                                            List<FlinkComponentProvisioningEvidence> provisioning,
                                            Optional<FlinkHaControl.Observations> observations) {
        Optional<String> failure = sampledLeadershipFailure(expected, observations, provisioning);
        if (failure.isEmpty()) failure = failure(expected, phases, tokens, provisioning, observations);
        return new FlinkHaEvidence(expected, tokens, observations,
                failure.isPresent() ? Outcome.UNCONFIRMED : Outcome.CONFIRMED,
                failure.orElse("Every declared leader fault, recovery and required token observation is confirmed"));
    }

    private static Optional<String> sampledLeadershipFailure(Expected expected,
            Optional<FlinkHaControl.Observations> observations,
            List<FlinkComponentProvisioningEvidence> provisioning) {
        if (!expected.haRequired()) {
            return expected.faults().isEmpty() ? Optional.empty()
                    : Optional.of("Declared leader faults require an HA target");
        }
        if (observations.isEmpty() || observations.orElseThrow().overflow()) {
            return Optional.of("Required sampled HA history is missing or overflowed");
        }
        var history = observations.orElseThrow();
        Map<String, FlinkComponentProvisioningEvidence> components = new HashMap<>();
        for (var component : provisioning) {
            if (components.putIfAbsent(component.runtimeId(), component) != null) {
                return Optional.of("HA provisioning contains duplicate physical processes");
            }
        }
        Set<String> sessionProcesses = new HashSet<>();
        Set<String> classProcesses = new HashSet<>();
        Long requestedTimeout = null;
        for (var session : history.sessions()) {
            var component = components.get(session.runtimeId());
            if (component == null || !component.logicalName().equals(session.logicalName())
                    || component.role() != session.role() || !sessionProcesses.add(session.runtimeId())
                    || !classProcesses.add(session.classLoadProcess())
                    || component.runtimeJarEvidence().filter(jar ->
                            !jar.classLoadProcess().equals(session.classLoadProcess())).isPresent()
                    || session.overflow() || session.negotiated().isEmpty()
                    || session.requestedTimeoutMillis() < 2_000 || session.requestedTimeoutMillis() > 60_000
                    || requestedTimeout != null && requestedTimeout != session.requestedTimeoutMillis()
                    || session.negotiated().stream().anyMatch(value ->
                            value.timeoutMillis() != session.requestedTimeoutMillis())) {
                return Optional.of("Flink process session timeout or exact incarnation binding is unconfirmed");
            }
            requestedTimeout = session.requestedTimeoutMillis();
        }
        if (components.isEmpty() || !sessionProcesses.equals(components.keySet())) {
            return Optional.of("A provisioned Flink incarnation lacks its own negotiated session evidence");
        }
        FlinkHaControl.Leadership initial = null;
        boolean preFence = false;
        long sequence = 0;
        long lastObservedAtMillis = 0;
        for (var sample : history.leadership()) {
            if (sample.sequence() != sequence + 1 || sample.firstObservedAtMillis() <= 0
                    || sample.firstObservedAtMillis() < lastObservedAtMillis
                    || sample.lastObservedAtMillis() < sample.firstObservedAtMillis()
                    || sample.sampleCount() > 1 && (sample.moment() != FlinkHaControl.ObservationMoment.ROUTING
                            || sample.leadership().isEmpty() || sample.error().isPresent())
                    || preFence || initial == null && sample.moment() != FlinkHaControl.ObservationMoment.INITIAL) {
                return Optional.of("Sampled HA history is unordered, incomplete or continues after its fence boundary");
            }
            sequence += sample.sampleCount();
            lastObservedAtMillis = sample.lastObservedAtMillis();
            if (sample.leadership().isPresent()) {
                var leader = sample.leadership().orElseThrow();
                var component = components.get(leader.resourceManager().runtimeId());
                if (!coherent(leader) || component == null || component.role() != FlinkComponentRole.JOB_MANAGER
                        || !component.logicalName().equals(leader.resourceManager().logicalName())) {
                    return Optional.of("A sampled leader is torn or not bound to a provisioned JobManager");
                }
                if (initial == null && sample.moment() == FlinkHaControl.ObservationMoment.INITIAL) initial = leader;
                if (initial != null && expected.faults().isEmpty() && !initial.equals(leader)) {
                    return Optional.of("An undeclared process or leadership session change was observed in the HA control");
                }
                if (sample.moment() == FlinkHaControl.ObservationMoment.PRE_FENCE) preFence = true;
            } else if (initial != null && expected.faults().isEmpty()) {
                return Optional.of("The HA control has an unsuccessful leadership sample after its initial boundary");
            }
        }
        return initial != null && preFence ? Optional.empty()
                : Optional.of("Coherent initial and pre-fence HA leadership boundaries are required");
    }

    private static Optional<String> failure(Expected expected, Optional<PhaseExecutionEvidence> phases,
                                             Optional<TokenEvidence> tokens,
                                             List<FlinkComponentProvisioningEvidence> provisioning,
                                             Optional<FlinkHaControl.Observations> history) {
        if (phases.isEmpty()) {
            return expected.faults().isEmpty() && !expected.tokenProviderRequired() ? Optional.empty()
                    : Optional.of("No phase evidence establishes the declared HA operations");
        }
        if (expected.faults().stream().anyMatch(fault -> fault.recoveryBarrier().isPresent())
                && expected.faults().stream().anyMatch(fault -> fault.recoveryBarrier().isEmpty())) {
            return Optional.of("Mixed leader faults lack explicit recovery-barrier ordering for every operation");
        }
        PhaseExecutionEvidence phase = phases.orElseThrow();
        List<PhaseExecutionEvidence.StepEvidence> steps = phase.steps().stream()
                .filter(step -> step.kind() == PhaseExecutionEvidence.StepKind.LEADER_FAULT).toList();
        if (steps.size() != expected.faults().size() || phase.leaderFaults().size() != steps.size()) {
            return Optional.of("Declared leader faults, attempted steps and raw observations do not correspond");
        }
        Set<DeclaredFault> locations = new HashSet<>();
        Set<String> transferredSessions = new HashSet<>();
        Set<String> checkpointTriggers = new HashSet<>();
        Set<FlinkJobObservation.Restore> recoveries = new HashSet<>();
        long lastHealedAtMillis = 0;
        Optional<TokenServiceControl.Snapshot> previousTokens = Optional.empty();
        String jobId = null;
        for (int index = 0; index < steps.size(); index++) {
            var declaration = expected.faults().get(index);
            var step = steps.get(index);
            var observed = phase.leaderFaults().get(index);
            if (!locations.add(declaration)
                    || step.status() != PhaseExecutionEvidence.StepStatus.SUCCEEDED
                    || !declaration.path().equals(step.path())
                    || !declaration.loopIterations().equals(step.loopIterations())
                    || !declaration.path().equals(observed.path())
                    || !declaration.loopIterations().equals(observed.loopIterations())
                    || !declaration.request().equals(observed.raw().request())) {
                return Optional.of("Leader fault request, location, loop order or successful step is unconfirmed");
            }
            Optional<String> invalid = faultFailure(observed.raw());
            if (invalid.isPresent()) return invalid;
            if (observed.raw().armedAtMillis() < lastHealedAtMillis
                    || !transferredSessions.add(observed.raw().after().orElseThrow()
                    .resourceManager().sessionId())) {
                return Optional.of("Leader faults overlap or reuse an earlier leadership transfer");
            }
            lastHealedAtMillis = observed.raw().healedAtMillis();
            if (observed.jobId().isBlank()
                    || expected.tokenProof().flatMap(TokenScopeProof.Requirement::submission)
                            .filter(binding -> !binding.jobId().equals(observed.jobId())).isPresent()
                    || (jobId != null && !jobId.equals(observed.jobId()))
                    || observed.jobBefore().observation().isEmpty()
                    || observed.jobAfter().observation().isEmpty()) {
                return Optional.of("Leader fault lacks observations of the same retained job");
            }
            jobId = observed.jobId();
            var before = observed.jobBefore().observation().orElseThrow();
            var after = observed.jobAfter().observation().orElseThrow();
            // A new leader may reset counters and has a different clock. Require a changed restore
            // observation instead of comparing cross-process counts or wall-clock timestamps.
            if (before.state() != FlinkJobState.RUNNING || before.completedCheckpoints() < 1
                    || (after.state() != FlinkJobState.RUNNING && after.state() != FlinkJobState.FINISHED)
                    || after.restoredCheckpoints() < 1 || after.latestRestore().isEmpty()
                    || after.latestRestore().orElseThrow().checkpointId() < 1
                    || after.latestRestore().equals(before.latestRestore())
                    || !recoveries.add(after.latestRestore().orElseThrow())) {
                return Optional.of("Leader fault lacks a checkpointed running job and a distinct recovered state");
            }
            if (expected.tokenProviderRequired()) {
                var raw = observed.raw();
                if (raw.tokensBefore().isEmpty() || raw.tokensDuring().isEmpty() || raw.tokensAfter().isEmpty()
                        || !completeTrace(raw.tokensBefore().orElseThrow())
                        || !completeTrace(raw.tokensDuring().orElseThrow())
                        || !completeTrace(raw.tokensAfter().orElseThrow())
                        || !prefix(raw.tokensBefore().orElseThrow(), raw.tokensDuring().orElseThrow())
                        || !prefix(raw.tokensDuring().orElseThrow(), raw.tokensAfter().orElseThrow())
                        || previousTokens.filter(previous -> !prefix(previous,
                                raw.tokensBefore().orElseThrow())).isPresent()) {
                    return Optional.of("Leader faults lack append-only token snapshots in operation order");
                }
                previousTokens = raw.tokensAfter();
            }
            if (declaration.recoveryBarrier().isPresent()) {
                if (!expected.tokenProviderRequired() || observed.recoveryBarrier().isEmpty() || tokens.isEmpty()) {
                    return Optional.of("Declared recovery barrier is missing");
                }
                invalid = TokenCheckpointBarrier.failure(observed.recoveryBarrier().orElseThrow(), observed.raw(),
                        expected.expectedTaskManagers(), tokens.orElseThrow(), provisioning, history, expected.tokenProof());
                if (invalid.isPresent()) return invalid;
                String trigger = observed.recoveryBarrier().orElseThrow().checkpoint().orElseThrow().triggerId();
                if (!checkpointTriggers.add(trigger)) return Optional.of("Recovery barriers replayed a checkpoint trigger");
                if (index + 1 < phase.leaderFaults().size()) {
                    invalid = TokenCheckpointBarrier.orderFailure(observed.recoveryBarrier().orElseThrow(),
                            phase.leaderFaults().get(index + 1).recoveryBarrier());
                    if (invalid.isPresent()) return invalid;
                    var settled = observed.recoveryBarrier().orElseThrow().afterHeal().orElseThrow().snapshot();
                    if (phase.leaderFaults().get(index + 1).raw().tokensBefore()
                            .filter(next -> prefix(settled, next)).isEmpty()) {
                        return Optional.of("A later leader fault preceded the token recovery barrier");
                    }
                }
            } else if (observed.recoveryBarrier().isPresent()) {
                return Optional.of("An undeclared recovery barrier changed the experiment");
            }
            if (declaration.request().tokenFault().isPresent()) {
                if (!expected.tokenProviderRequired() || tokens.isEmpty()
                        || tokens.orElseThrow().snapshot().isEmpty()) {
                    return Optional.of("A declared token fault lacks token-provider evidence");
                }
                Optional<TokenServiceControl.Snapshot> nextTokens = index + 1 < phase.leaderFaults().size()
                        ? phase.leaderFaults().get(index + 1).raw().tokensBefore() : Optional.empty();
                invalid = tokenFaultFailure(observed.raw(), tokens.orElseThrow(), provisioning, nextTokens, expected);
                if (invalid.isPresent()) return invalid;
            }
        }
        if (expected.tokenProviderRequired()) {
            if (previousTokens.isPresent() && (tokens.isEmpty() || tokens.orElseThrow().snapshot().isEmpty()
                    || !prefix(previousTokens.orElseThrow(), tokens.orElseThrow().snapshot().orElseThrow()))) {
                return Optional.of("Final token trace does not retain the last leader operation's observations");
            }
            return tokens.isEmpty() ? Optional.of("Required token-provider evidence is missing")
                    : tokenFailure(tokens.orElseThrow(), provisioning, expected);
        }
        return Optional.empty();
    }

    /** Shared with process identity accounting; no success flag substitutes for physical proof. */
    static Optional<String> faultFailure(FlinkHaControl.LeaderFaultEvidence raw) {
        if (!raw.errors().isEmpty() || !raw.applied() || !raw.healed()
                || raw.before().isEmpty() || raw.after().isEmpty() || raw.target().isEmpty()
                || raw.faultState().isEmpty() || raw.healedState().isEmpty()
                || raw.armedAtMillis() <= 0 || raw.healedAtMillis() < raw.armedAtMillis()
                || java.time.Duration.ofMillis(raw.healedAtMillis() - raw.armedAtMillis())
                        .compareTo(raw.request().duration()) < 0
                || raw.closedConnections() < 0 || raw.rejectedConnections() < 0
                || raw.isolationActiveAfterHeal()) {
            return Optional.of("Leader fault application, healing or raw observations are incomplete");
        }
        var before = raw.before().orElseThrow();
        var after = raw.after().orElseThrow();
        var target = raw.target().orElseThrow();
        if (!coherent(before) || !coherent(after) || !target.equals(before.resourceManager())
                || target.runtimeId().equals(after.resourceManager().runtimeId())
                || target.logicalName().equals(after.resourceManager().logicalName())
                || before.resourceManager().sessionId().equals(after.resourceManager().sessionId())
                || before.dispatcher().sessionId().equals(after.dispatcher().sessionId())
                || before.restServer().sessionId().equals(after.restServer().sessionId())) {
            return Optional.of("RM, dispatcher and REST leadership did not coherently move to another process with new sessions");
        }
        var fault = raw.faultState().orElseThrow();
        var healed = raw.healedState().orElseThrow();
        if (!target.runtimeId().equals(fault.runtimeId()) || !healed.running() || healed.paused()) {
            return Optional.of("Fault and healed process states do not establish the targeted operation");
        }
        boolean physical = switch (raw.request().mode()) {
            case KILL -> !fault.running() && !fault.paused()
                    && !target.runtimeId().equals(healed.runtimeId());
            case PAUSE -> fault.running() && fault.paused()
                    && target.runtimeId().equals(healed.runtimeId());
            case ISOLATE_ZOOKEEPER -> fault.running() && !fault.paused()
                    && target.runtimeId().equals(healed.runtimeId())
                    && (raw.closedConnections() > 0 || raw.rejectedConnections() > 0);
        };
        return physical ? Optional.empty()
                : Optional.of("Physical process or ZooKeeper connection evidence does not prove the declared fault");
    }

    static boolean coherent(FlinkHaControl.Leadership leadership) {
        var rm = leadership.resourceManager();
        return rm.runtimeId().equals(leadership.dispatcher().runtimeId())
                && rm.runtimeId().equals(leadership.restServer().runtimeId())
                && rm.logicalName().equals(leadership.dispatcher().logicalName())
                && rm.logicalName().equals(leadership.restServer().logicalName())
                && rm.sessionId().equals(leadership.dispatcher().sessionId())
                && rm.sessionId().equals(leadership.restServer().sessionId());
    }

    private static Optional<String> tokenFailure(TokenEvidence tokens,
                                                  List<FlinkComponentProvisioningEvidence> provisioning, Expected expected) {
        if (!tokens.errors().isEmpty() || tokens.pluginSha256().isEmpty()
                || !tokens.pluginSha256().orElseThrow().matches("[0-9a-f]{64}")
                || tokens.origins().isEmpty() || tokens.snapshot().isEmpty()) {
            return Optional.of("Token plugin identity, class origins or final trace is missing");
        }
        var origins = tokens.origins().orElseThrow();
        if (origins.failure().isPresent() || !TOKEN_CONTAINER_PATH.equals(origins.expectedSource())) {
            return Optional.of("Token plugin class origin inspection is incomplete");
        }
        boolean jobManager = false;
        boolean taskManager = false;
        Set<String> processes = new HashSet<>();
        for (var process : origins.processes()) {
            if (!processes.add(process.process()) || TOKEN_CLASSES.stream().anyMatch(type ->
                    process.sources().getOrDefault(type, List.of()).stream()
                            .anyMatch(source -> !TOKEN_CONTAINER_PATH.equals(source)))) {
                return Optional.of("Token classes have duplicate process evidence or a foreign JAR origin");
            }
            boolean receiver = !process.sources().getOrDefault(TOKEN_RECEIVER, List.of()).isEmpty();
            jobManager |= process.process().startsWith("jobmanager-") && receiver
                    && !process.sources().getOrDefault(TOKEN_PROVIDER, List.of()).isEmpty();
            taskManager |= process.process().startsWith("taskmanager-") && receiver;
        }
        if (!jobManager || !taskManager) {
            return Optional.of("Token SPI classes were not observed on both JobManager and TaskManager");
        }
        var snapshot = tokens.snapshot().orElseThrow();
        if (!completeTrace(snapshot) || snapshot.activeRequests() != 0) {
            return Optional.of("Token trace overflowed, saturated, has gaps or retains unfinished requests");
        }
        if (expected.tokenProof().isPresent()) {
            var proof = expected.tokenProof().orElseThrow();
            if (proof.submission().isEmpty() || !TokenScopeProof.coverageValid(snapshot)
                    || !TokenCheckpointBarrier.validReceivers(proof.receivers(), expected.expectedTaskManagers(),
                            tokens, provisioning)) {
                return Optional.of("Scoped token proof lacks submitted-job binding, valid coverage or all live receiver identities");
            }
            return participants(tokens, provisioning).issuers().stream().anyMatch(issuer ->
                    TokenScopeProof.issuance(snapshot, issuer, proof.receivers(),
                            proof.submission().orElseThrow().afterSequence(), expected.tokenProof()).isPresent())
                    ? Optional.empty() : Optional.of("No fresh scoped token reached every expected live TaskManager");
        }
        return healthyDelivery(snapshot.events(), 0, Optional.empty(), participants(tokens, provisioning))
                ? Optional.empty()
                : Optional.of("No healthy JobManager issuance was acknowledged by a TaskManager");
    }

    private static Optional<String> tokenFaultFailure(FlinkHaControl.LeaderFaultEvidence raw,
                                                       TokenEvidence tokens,
                                                       List<FlinkComponentProvisioningEvidence> provisioning,
                                                       Optional<TokenServiceControl.Snapshot> nextTokens, Expected expected) {
        if (raw.tokensBefore().isEmpty() || raw.tokensDuring().isEmpty() || raw.tokensAfter().isEmpty()) {
            return Optional.of("Token fault lacks before, during or healed trace snapshots");
        }
        var before = raw.tokensBefore().orElseThrow();
        var during = raw.tokensDuring().orElseThrow();
        var after = raw.tokensAfter().orElseThrow();
        var finalSnapshot = tokens.snapshot().orElseThrow();
        if (!completeTrace(before) || !completeTrace(during) || !completeTrace(after)
                || !prefix(before, during) || !prefix(during, after) || !prefix(after, finalSnapshot)
                || nextTokens.filter(next -> !completeTrace(next) || !prefix(after, next)
                        || !prefix(next, finalSnapshot)).isPresent()) {
            return Optional.of("Token snapshots are incomplete or contradict their retained sequence");
        }
        Optional<String> process = processLabel(provisioning, raw.after().orElseThrow().resourceManager());
        if (process.isEmpty() || tokens.origins().isEmpty()
                || tokens.origins().orElseThrow().processes().stream().noneMatch(origin ->
                origin.process().equals(process.orElseThrow()) && TOKEN_CLASSES.stream().allMatch(type ->
                        origin.sources().getOrDefault(type, List.of()).contains(TOKEN_CONTAINER_PATH)))) {
            return Optional.of("New ResourceManager token acquisition has no provisioned process and plugin origin binding");
        }
        var finalWindow = nextTokens.orElse(finalSnapshot);
        Optional<String> effect = tokenFaultEffectFailure(raw, finalWindow, process.orElseThrow(), expected.tokenProof());
        if (effect.isPresent()) return effect;
        long healed = after.events().stream().filter(event -> event.sequence() > before.events().size()
                && event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                && event.mode() == TokenServiceControl.Mode.HEALTHY).mapToLong(TokenServiceControl.Event::sequence)
                .max().orElseThrow();
        if (expected.tokenProof().isPresent()) {
            var proof = expected.tokenProof().orElseThrow();
            return TokenCheckpointBarrier.validReceivers(proof.receivers(), expected.expectedTaskManagers(), tokens, provisioning)
                    && TokenScopeProof.issuance(finalWindow, process.orElseThrow(), proof.receivers(), healed,
                            expected.tokenProof()).isPresent()
                    ? Optional.empty() : Optional.of("No fresh scoped all-TaskManager delivery followed token fault healing");
        }
        return healthyDelivery(finalWindow.events(), healed, process, participants(tokens, provisioning))
                ? Optional.empty() : Optional.of("No healthy token delivery followed token fault healing");
    }

    /** The actual fault trace predicate is also required before an opt-in barrier can advance. */
    static Optional<String> tokenFaultEffectFailure(FlinkHaControl.LeaderFaultEvidence raw,
                                                    TokenServiceControl.Snapshot finalSnapshot, String process) {
        return tokenFaultEffectFailure(raw, finalSnapshot, process, Optional.empty());
    }

    static Optional<String> tokenFaultEffectFailure(FlinkHaControl.LeaderFaultEvidence raw,
            TokenServiceControl.Snapshot finalSnapshot, String process,
            Optional<TokenScopeProof.Requirement> requirement) {
        if (raw.request().tokenFault().isEmpty()) return Optional.empty();
        if (requirement.isPresent() && !TokenScopeProof.coverageValid(finalSnapshot)) {
            return Optional.of("Token fault trace contains invalid scoped coverage");
        }
        if (raw.tokensBefore().isEmpty() || raw.tokensDuring().isEmpty() || raw.tokensAfter().isEmpty()) {
            return Optional.of("Token fault snapshots are missing");
        }
        var before = raw.tokensBefore().orElseThrow();
        var during = raw.tokensDuring().orElseThrow();
        var after = raw.tokensAfter().orElseThrow();
        if (!completeTrace(before) || !completeTrace(during) || !completeTrace(after)
                || !completeTrace(finalSnapshot) || !prefix(before, during) || !prefix(during, after)
                || !prefix(after, finalSnapshot)) return Optional.of("Token fault trace is incomplete");
        var requested = raw.request().tokenFault().orElseThrow();
        long previous = before.events().size();
        // A later fault's recovery cannot supply the missing healthy delivery of this one.
        List<TokenServiceControl.Event> events = finalSnapshot.events();
        var armed = during.events().stream().filter(event -> event.sequence() > previous
                && event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                && event.mode() == requested.mode()
                && event.detail().equals("delayMillis=" + requested.delay().toMillis())).toList();
        if (armed.size() != 1) return Optional.of("Token fault has no unique matching mode revision");
        var revision = armed.getFirst();
        var heal = after.events().stream().filter(event -> event.sequence() > revision.sequence()
                && event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                && event.mode() == TokenServiceControl.Mode.HEALTHY
                && event.revision() > revision.revision()).findFirst();
        if (heal.isEmpty()) return Optional.of("Token fault has no subsequent healthy revision");
        var starts = during.events().stream().filter(event -> event.kind() == TokenServiceControl.Kind.REQUEST_STARTED
                && event.sequence() > revision.sequence() && event.sequence() < heal.orElseThrow().sequence()
                && event.revision() == revision.revision() && event.mode() == requested.mode()
                && "jobmanager".equals(event.role())
                && process.equals(event.process())
                && TokenScopeProof.request(events, event, requirement)
                && initialized(events, event, TokenServiceControl.Kind.PROVIDER_INITIALIZED)).toList();
        boolean completed = starts.stream().anyMatch(start -> events.stream().anyMatch(outcome ->
                sameRequest(start, outcome) && outcome.sequence() > start.sequence()
                        && tokenFaultOutcome(requested, start, outcome)
                        && events.stream().noneMatch(event -> sameRequest(start, event)
                                && event.kind() == TokenServiceControl.Kind.FAILED
                                && !event.detail().equals(outcome.detail()))
                        && (requested.mode() == TokenServiceControl.Mode.DELAY
                                || events.stream().anyMatch(ack -> sameRequest(start, ack)
                                && ack.kind() == TokenServiceControl.Kind.FAULT_OBSERVED
                                && ack.sequence() > outcome.sequence() && ack.detail().equals(outcome.detail())))
                        && events.stream().anyMatch(finished -> sameRequest(start, finished)
                        && finished.sequence() > outcome.sequence()
                        && finished.kind() == TokenServiceControl.Kind.REQUEST_FINISHED)));
        if (!completed) return Optional.of("No actual acquisition and outcome occurred under the declared token revision");
        return Optional.empty();
    }

    private static boolean tokenFaultOutcome(FlinkHaControl.TokenFault requested,
                                             TokenServiceControl.Event start,
                                             TokenServiceControl.Event outcome) {
        return switch (requested.mode()) {
            case DELAY -> outcome.kind() == TokenServiceControl.Kind.ISSUED
                    && outcome.monotonicNanos() - start.monotonicNanos() >= 0
                    && java.time.Duration.ofNanos(outcome.monotonicNanos() - start.monotonicNanos())
                            .compareTo(requested.delay()) >= 0;
            case FAIL -> outcome.kind() == TokenServiceControl.Kind.FAILED
                    && "HTTP 503".equals(outcome.detail());
            case LINKAGE_ERROR -> outcome.kind() == TokenServiceControl.Kind.FAILED
                    && "HTTP 598".equals(outcome.detail());
            case HEALTHY -> false;
        };
    }

    static Optional<String> processLabel(List<FlinkComponentProvisioningEvidence> provisioning,
                                                  FlinkHaControl.LeaderIdentity leader) {
        int incarnation = 0;
        Set<String> runtimeIds = new HashSet<>();
        String process = null;
        for (var component : provisioning) {
            if (!runtimeIds.add(component.runtimeId())) return Optional.empty();
            if (component.logicalName().equals(leader.logicalName())) {
                if (component.role() != FlinkComponentRole.JOB_MANAGER) return Optional.empty();
                incarnation++;
                if (component.runtimeId().equals(leader.runtimeId())) {
                    process = component.logicalName() + "#" + incarnation;
                    if (component.runtimeJarEvidence().isPresent()
                            && !process.equals(component.runtimeJarEvidence().orElseThrow().classLoadProcess())) {
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.ofNullable(process);
    }

    static boolean sameRequest(TokenServiceControl.Event start, TokenServiceControl.Event event) {
        return TokenScopeProof.sameRequest(start, event);
    }

    private static boolean healthyDelivery(List<TokenServiceControl.Event> events, long afterSequence,
                                            Optional<String> process, TokenParticipants participants) {
        return events.stream().filter(event -> event.sequence() > afterSequence
                && event.kind() == TokenServiceControl.Kind.ISSUED
                && event.mode() == TokenServiceControl.Mode.HEALTHY
                && process.map(value -> value.equals(event.process())).orElse(true)
                && participants.issuers().contains(event.process())
                && "jobmanager".equals(event.role()) && event.tokenSequence().isPresent())
                .anyMatch(issued -> events.stream().anyMatch(start -> sameRequest(start, issued)
                        && start.kind() == TokenServiceControl.Kind.REQUEST_STARTED
                        && start.sequence() > afterSequence
                        && start.sequence() < issued.sequence()
                        && initialized(events, start, TokenServiceControl.Kind.PROVIDER_INITIALIZED))
                        && events.stream().noneMatch(event -> sameRequest(issued, event)
                                && event.kind() == TokenServiceControl.Kind.FAILED)
                        && events.stream().anyMatch(finished -> sameRequest(issued, finished)
                        && finished.kind() == TokenServiceControl.Kind.REQUEST_FINISHED
                        && finished.sequence() > issued.sequence())
                        && events.stream().anyMatch(received -> received.sequence() > issued.sequence()
                        && received.kind() == TokenServiceControl.Kind.RECEIVED
                        && "taskmanager".equals(received.role())
                        && participants.receivers().contains(received.process())
                        && received.tokenSequence().equals(issued.tokenSequence())
                        && initialized(events, received, TokenServiceControl.Kind.RECEIVER_INITIALIZED)));
    }

    static boolean initialized(List<TokenServiceControl.Event> events,
                                       TokenServiceControl.Event operation, TokenServiceControl.Kind kind) {
        return events.stream().anyMatch(event -> event.kind() == kind
                && event.sequence() < operation.sequence()
                && event.process().equals(operation.process()) && event.role().equals(operation.role())
                && (operation.registration().isEmpty() && operation.participantInstance().isEmpty()
                    || operation.participantInstance().isPresent()
                        && event.participantInstance().equals(operation.participantInstance())));
    }

    private record TokenParticipants(Set<String> issuers, Set<String> receivers) {}

    private static TokenParticipants participants(TokenEvidence tokens,
            List<FlinkComponentProvisioningEvidence> provisioning) {
        TokenParticipants missing = new TokenParticipants(Set.of(), Set.of());
        if (tokens.origins().isEmpty()) return missing;
        Map<String, Integer> incarnations = new HashMap<>();
        Map<String, FlinkComponentRole> roles = new HashMap<>();
        Set<String> runtimeIds = new HashSet<>();
        for (var component : provisioning) {
            if (!runtimeIds.add(component.runtimeId())) return missing;
            String label = component.logicalName() + "#"
                    + incarnations.merge(component.logicalName(), 1, Integer::sum);
            if (component.runtimeJarEvidence().isPresent()
                    && !label.equals(component.runtimeJarEvidence().orElseThrow().classLoadProcess())) return missing;
            roles.put(label, component.role());
        }
        Set<String> issuers = new HashSet<>();
        Set<String> receivers = new HashSet<>();
        for (var origin : tokens.origins().orElseThrow().processes()) {
            if (!origin.sources().getOrDefault(TOKEN_RECEIVER, List.of()).contains(TOKEN_CONTAINER_PATH)) continue;
            if (roles.get(origin.process()) == FlinkComponentRole.TASK_MANAGER) receivers.add(origin.process());
            if (roles.get(origin.process()) == FlinkComponentRole.JOB_MANAGER
                    && origin.sources().getOrDefault(TOKEN_PROVIDER, List.of()).contains(TOKEN_CONTAINER_PATH)) {
                issuers.add(origin.process());
            }
        }
        return new TokenParticipants(Set.copyOf(issuers), Set.copyOf(receivers));
    }

    static boolean completeTrace(TokenServiceControl.Snapshot snapshot) {
        if (snapshot.overflow() || snapshot.saturated()) return false;
        long sequence = 0;
        Map<Long, TokenServiceControl.Event> requests = new HashMap<>();
        Set<Long> finished = new HashSet<>();
        int active = 0, maximum = 0;
        for (var event : snapshot.events()) {
            if (event.sequence() != ++sequence) return false;
            switch (event.kind()) {
                case REQUEST_STARTED -> {
                    if (event.requestId() < 1 || requests.putIfAbsent(event.requestId(), event) != null) return false;
                    maximum = Math.max(maximum, ++active);
                }
                case ISSUED, FAILED, REQUEST_FINISHED, FAULT_OBSERVED -> {
                    var start = requests.get(event.requestId());
                    if (start == null || !sameRequest(start, event)
                            || event.kind() != TokenServiceControl.Kind.FAULT_OBSERVED
                            && finished.contains(event.requestId())) return false;
                    if (event.kind() == TokenServiceControl.Kind.REQUEST_FINISHED) {
                        finished.add(event.requestId());
                        active--;
                    }
                }
                default -> { }
            }
        }
        return active == snapshot.activeRequests() && maximum == snapshot.maxConcurrentRequests();
    }

    static boolean prefix(TokenServiceControl.Snapshot first, TokenServiceControl.Snapshot second) {
        return first.events().size() <= second.events().size()
                && second.events().subList(0, first.events().size()).equals(first.events());
    }
}
