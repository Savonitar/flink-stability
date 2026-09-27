package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkCheckpointTrigger;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Opt-in synchronization; incomplete proof stops later phase actions, never changes the data oracle. */
public final class TokenCheckpointBarrier {
    private TokenCheckpointBarrier() {}

    public record Receiver(TaskManagerControl.Identity identity, String classLoadProcess) {
        public Receiver {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(classLoadProcess, "classLoadProcess");
        }
    }

    /** Token sequence numbers refer to the retained append-only fixture event trace. */
    public record Ready(FlinkHaControl.LeadershipObservation beforeTokens,
                        FlinkHaControl.LeadershipObservation afterTokens,
                        List<Receiver> receivers, String issuerProcess,
                        TokenServiceControl.Snapshot entrySnapshot, TokenServiceControl.Snapshot snapshot,
                        long afterSequence, long issuedSequence) {
        public Ready {
            Objects.requireNonNull(beforeTokens, "beforeTokens");
            Objects.requireNonNull(afterTokens, "afterTokens");
            receivers = List.copyOf(receivers);
            Objects.requireNonNull(issuerProcess, "issuerProcess");
            Objects.requireNonNull(entrySnapshot, "entrySnapshot");
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }

    public record Checkpoint(String triggerId, boolean submissionAttempted, Optional<String> acknowledgedId,
                             List<FlinkCheckpointTrigger.Observation> observations) {
        public Checkpoint {
            Objects.requireNonNull(triggerId, "triggerId");
            Objects.requireNonNull(acknowledgedId, "acknowledgedId");
            observations = List.copyOf(observations);
        }
    }

    public record Evidence(Optional<Ready> beforeFault, Optional<Ready> afterHeal,
                           Optional<FlinkHaControl.LeadershipObservation> beforeCheckpoint,
                           Optional<FlinkHaControl.LeadershipObservation> afterCheckpoint,
                           Optional<Checkpoint> checkpoint, List<Receiver> afterCheckpointReceivers, List<String> errors) {
        public Evidence {
            Objects.requireNonNull(beforeFault, "beforeFault");
            Objects.requireNonNull(afterHeal, "afterHeal");
            Objects.requireNonNull(beforeCheckpoint, "beforeCheckpoint");
            Objects.requireNonNull(afterCheckpoint, "afterCheckpoint");
            Objects.requireNonNull(checkpoint, "checkpoint");
            afterCheckpointReceivers = List.copyOf(afterCheckpointReceivers);
            errors = List.copyOf(errors);
        }
    }

    /** Mutable only within one phase invocation; snapshots survive every failure boundary. */
    static final class Operation {
        private final V1AttemptRuntime runtime;
        private final FlinkScenarioControl flink;
        private final FlinkJobHandle job;
        private final int expectedTaskManagers;
        private final MonotonicDeadline deadline;
        private final PhaseSleeper sleeper;
        private Optional<Ready> before = Optional.empty(), after = Optional.empty();
        private Optional<FlinkHaControl.LeadershipObservation> checkpointBefore = Optional.empty(), checkpointAfter = Optional.empty();
        private Optional<Checkpoint> checkpoint = Optional.empty();
        private final List<String> errors = new ArrayList<>();
        private List<Receiver> finalReceivers = List.of();

        Operation(V1AttemptRuntime runtime, FlinkScenarioControl flink, FlinkJobHandle job,
                  int expectedTaskManagers, MonotonicDeadline deadline, PhaseSleeper sleeper) {
            this.runtime = runtime;
            this.flink = flink;
            this.job = job;
            this.expectedTaskManagers = expectedTaskManagers;
            this.deadline = deadline;
            this.sleeper = sleeper;
        }

        boolean beforeFault() {
            try {
                before = Optional.of(awaitReady(Optional.empty(), -1));
                return true;
            } catch (Exception failure) {
                failed(failure);
                return false;
            }
        }

        void afterHeal(FlinkHaControl.LeaderFaultEvidence raw) {
            try {
                Optional<String> physicalFailure = FlinkHaEvidence.faultFailure(raw);
                if (physicalFailure.isPresent()) throw new IOException(physicalFailure.orElseThrow());
                var leader = raw.after().orElseThrow(() -> new IOException("No recovered HA leadership"));
                if (!raw.healed() || !raw.errors().isEmpty()) throw new IOException("Leader fault did not heal cleanly");
                var initial = before.orElseThrow(() -> new IOException("Initial healthy-token readiness is missing"));
                if (raw.before().isEmpty() || raw.tokensBefore().isEmpty()
                        || !initial.afterTokens().leadership().equals(raw.before())
                        || !FlinkHaEvidence.prefix(initial.snapshot(), raw.tokensBefore().orElseThrow())
                        || !preFaultStable(initial, raw, runtime.haObservations().orElseThrow())) {
                    throw new IOException("Ready leadership or token history changed before actual fault selection");
                }
                long boundary = healBoundary(raw);
                after = Optional.of(awaitReady(Optional.of(leader), boundary));
                Optional<String> effect = FlinkHaEvidence.tokenFaultEffectFailure(raw,
                        after.orElseThrow().snapshot(), after.orElseThrow().issuerProcess());
                if (effect.isPresent()) throw new IOException(effect.orElseThrow());
                checkpointBefore = Optional.of(sampleLeader(runtime, remaining()));
                requireLeader(checkpointBefore.orElseThrow(), leader);
                if (!receivers(runtime, expectedTaskManagers, deadline).equals(after.orElseThrow().receivers())) {
                    throw new IOException("TaskManager incarnation changed before the checkpoint trigger");
                }
                String trigger = UUID.randomUUID().toString().replace("-", "");
                checkpoint = Optional.of(new Checkpoint(trigger, true, Optional.empty(), List.of()));
                String acknowledged = flink.triggerCheckpoint(job, trigger, remaining());
                checkpoint = Optional.of(new Checkpoint(trigger, true, Optional.of(acknowledged), List.of()));
                if (!trigger.equals(acknowledged)) throw new IOException("Checkpoint acknowledged another trigger");
                List<FlinkCheckpointTrigger.Observation> polls = new ArrayList<>();
                while (true) {
                    if (polls.size() >= 1500) throw new IOException("Checkpoint barrier observation limit reached");
                    var observed = flink.checkpointStatus(job, trigger, remaining());
                    polls.add(observed);
                    checkpoint = Optional.of(new Checkpoint(trigger, true, Optional.of(acknowledged), polls));
                    if (observed.state() == FlinkCheckpointTrigger.State.COMPLETED) {
                        if (observed.failure().isPresent() || observed.checkpointId().isEmpty()
                                || observed.checkpointId().orElseThrow() < 1) {
                            throw new IOException("Triggered checkpoint failed: " + observed);
                        }
                        break;
                    }
                    pause();
                }
                checkpointAfter = Optional.of(sampleLeader(runtime, remaining()));
                requireLeader(checkpointAfter.orElseThrow(), leader);
                if (!stable(runtime.haObservations().orElseThrow(), after.orElseThrow().beforeTokens(),
                        checkpointAfter.orElseThrow(), leader)) {
                    throw new IOException("Observed leadership changed or was unavailable during recovery barrier");
                }
                finalReceivers = receivers(runtime, expectedTaskManagers, deadline);
                if (!finalReceivers.equals(after.orElseThrow().receivers())) {
                    throw new IOException("TaskManager incarnation changed across the checkpoint barrier");
                }
            } catch (Exception failure) {
                failed(failure);
            }
        }

        Evidence evidence() {
            return new Evidence(before, after, checkpointBefore, checkpointAfter, checkpoint, finalReceivers, errors);
        }

        private Ready awaitReady(Optional<FlinkHaControl.Leadership> expected, long boundary) throws Exception {
            var first = sampleLeader(runtime, remaining());
            var leader = first.leadership().orElseThrow();
            if (expected.isPresent()) requireLeader(first, expected.orElseThrow());
            var entry = runtime.tokenServiceEvidence().orElseThrow(() -> new IOException("Token trace unavailable"));
            if (!FlinkHaEvidence.completeTrace(entry)) throw new IOException("Initial token trace is incomplete");
            long minimumSequence = boundary < 0 ? entry.events().size() : boundary;
            var targets = receivers(runtime, expectedTaskManagers, deadline);
            String issuer = FlinkHaEvidence.processLabel(runtime.flinkProvisioningEvidence(), leader.resourceManager())
                    .orElseThrow(() -> new IOException("Current ResourceManager has no provisioned class-process identity"));
            int polls = 0;
            while (true) {
                if (++polls > 1500) throw new IOException("Token barrier observation limit reached");
                remaining();
                var trace = runtime.tokenServiceEvidence().orElseThrow(() -> new IOException("Token trace unavailable"));
                if (!FlinkHaEvidence.completeTrace(trace)) throw new IOException("Token trace is incomplete or saturated");
                Optional<TokenServiceControl.Event> issued = issuance(trace, issuer, targets, minimumSequence);
                if (issued.isPresent()) {
                    var last = sampleLeader(runtime, remaining());
                    requireLeader(last, leader);
                    if (!targets.equals(receivers(runtime, expectedTaskManagers, deadline))) {
                        throw new IOException("TaskManager incarnation changed during token delivery");
                    }
                    if (!FlinkHaEvidence.prefix(entry, trace)
                            || !stable(runtime.haObservations().orElseThrow(), first, last, leader)) {
                        throw new IOException("Observed leadership or token history changed during delivery");
                    }
                    return new Ready(first, last, targets, issuer, entry, trace,
                            minimumSequence, issued.orElseThrow().sequence());
                }
                pause();
            }
        }

        private Duration remaining() throws IOException {
            return deadline.remainingOrThrow(() -> new IOException("Token-checkpoint recovery barrier deadline expired"));
        }

        private void pause() throws Exception {
            Duration left = remaining();
            sleeper.sleep(left.compareTo(Duration.ofMillis(250)) < 0 ? left : Duration.ofMillis(250));
        }

        private void failed(Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            errors.addAll(V1ScenarioExecutor.diagnostics(failure));
        }
    }

    private static long healBoundary(FlinkHaControl.LeaderFaultEvidence raw) throws IOException {
        var after = raw.tokensAfter().orElseThrow(() -> new IOException("Healed token trace is missing"));
        if (raw.request().tokenFault().isEmpty()) return after.events().size();
        long before = raw.tokensBefore().orElseThrow().events().size();
        return after.events().stream().filter(event -> event.sequence() > before
                && event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                && event.mode() == TokenServiceControl.Mode.HEALTHY).mapToLong(TokenServiceControl.Event::sequence)
                .max().orElseThrow(() -> new IOException("No token heal revision was retained"));
    }

    private static FlinkHaControl.LeadershipObservation sampleLeader(V1AttemptRuntime runtime, Duration timeout)
            throws IOException {
        long previous = runtime.haObservations().map(TokenCheckpointBarrier::lastSequence).orElse(0L);
        runtime.currentFlinkRestEndpoint(timeout);
        var history = runtime.haObservations().orElseThrow(() -> new IOException("HA sample history unavailable"));
        if (history.overflow() || history.leadership().isEmpty() || lastSequence(history) <= previous) {
            throw new IOException("No fresh HA routing observation was retained");
        }
        var sample = history.leadership().getLast();
        if (sample.moment() != FlinkHaControl.ObservationMoment.ROUTING || sample.error().isPresent()
                || sample.leadership().isEmpty() || !FlinkHaEvidence.coherent(sample.leadership().orElseThrow())) {
            throw new IOException("Fresh HA routing observation is not coherent");
        }
        return sample;
    }

    private static long lastSequence(FlinkHaControl.Observations history) {
        return history.leadership().isEmpty() ? 0 : endSequence(history.leadership().getLast());
    }

    static long endSequence(FlinkHaControl.LeadershipObservation sample) {
        return sample.sequence() + sample.sampleCount() - 1;
    }

    private static void requireLeader(FlinkHaControl.LeadershipObservation sample,
                                      FlinkHaControl.Leadership expected) throws IOException {
        if (sample.error().isPresent() || !sample.leadership().equals(Optional.of(expected))
                || !FlinkHaEvidence.coherent(expected)) throw new IOException("HA leadership changed across recovery barrier");
    }

    private static List<Receiver> receivers(V1AttemptRuntime runtime, int count, MonotonicDeadline deadline)
            throws IOException {
        if (count < 1) throw new IOException("Expected TaskManager topology is absent");
        List<Receiver> result = new ArrayList<>();
        List<FlinkComponentProvisioningEvidence> components = runtime.flinkProvisioningEvidence();
        for (int index = 1; index <= count; index++) {
            String name = "taskmanager-" + index;
            Duration remaining = deadline.remainingOrThrow(() -> new IOException("Token receiver identity deadline expired"));
            var identity = runtime.taskManagerIdentity(name, remaining)
                    .orElseThrow(() -> new IOException("Expected live TaskManager identity is missing: " + name));
            if (!identity.logicalName().equals(name)) throw new IOException("TaskManager lookup returned another logical slot");
            String process = process(components, identity.runtimeId(), name, FlinkComponentRole.TASK_MANAGER)
                    .orElseThrow(() -> new IOException("TaskManager identity is not provisioned: " + name));
            result.add(new Receiver(identity, process));
        }
        if (result.stream().map(value -> value.identity().runtimeId()).distinct().count() != count) {
            throw new IOException("Expected TaskManagers share a physical process identity");
        }
        return List.copyOf(result);
    }

    static Optional<String> process(List<FlinkComponentProvisioningEvidence> components, String runtimeId,
                                    String logicalName, FlinkComponentRole role) {
        int incarnation = 0;
        for (var component : components) {
            if (!component.logicalName().equals(logicalName)) continue;
            incarnation++;
            if (component.runtimeId().equals(runtimeId) && component.role() == role) {
                String label = logicalName + "#" + incarnation;
                return component.runtimeJarEvidence().filter(jar -> !jar.classLoadProcess().equals(label)).isPresent()
                        ? Optional.empty() : Optional.of(label);
            }
        }
        return Optional.empty();
    }

    static Optional<TokenServiceControl.Event> issuance(TokenServiceControl.Snapshot snapshot, String issuer,
                                                       List<Receiver> receivers, long boundary) {
        if (receivers.isEmpty() || !FlinkHaEvidence.completeTrace(snapshot)) return Optional.empty();
        var events = snapshot.events();
        long currentRevision = events.stream().filter(event -> event.kind() == TokenServiceControl.Kind.MODE_CHANGED)
                .mapToLong(TokenServiceControl.Event::revision).max().orElse(0);
        return events.stream().filter(issued -> issued.sequence() > boundary
                && issued.kind() == TokenServiceControl.Kind.ISSUED && issuer.equals(issued.process())
                && "jobmanager".equals(issued.role()) && issued.mode() == TokenServiceControl.Mode.HEALTHY
                && issued.revision() == currentRevision && issued.tokenSequence().isPresent()
                && events.stream().anyMatch(start -> FlinkHaEvidence.sameRequest(start, issued)
                    && start.kind() == TokenServiceControl.Kind.REQUEST_STARTED && start.sequence() > boundary
                    && start.sequence() < issued.sequence()
                    && FlinkHaEvidence.initialized(events, start, TokenServiceControl.Kind.PROVIDER_INITIALIZED))
                && events.stream().noneMatch(event -> FlinkHaEvidence.sameRequest(issued, event)
                    && event.kind() == TokenServiceControl.Kind.FAILED)
                && events.stream().anyMatch(event -> FlinkHaEvidence.sameRequest(issued, event)
                    && event.kind() == TokenServiceControl.Kind.REQUEST_FINISHED && event.sequence() > issued.sequence())
                && receivers.stream().allMatch(receiver -> events.stream().anyMatch(received ->
                    received.sequence() > issued.sequence() && received.kind() == TokenServiceControl.Kind.RECEIVED
                    && "taskmanager".equals(received.role()) && receiver.classLoadProcess().equals(received.process())
                    && received.tokenSequence().equals(issued.tokenSequence())
                    && FlinkHaEvidence.initialized(events, received, TokenServiceControl.Kind.RECEIVER_INITIALIZED))))
                .findFirst();
    }

    static Optional<String> failure(Evidence evidence, FlinkHaControl.LeaderFaultEvidence raw, int count,
                                    FlinkHaEvidence.TokenEvidence tokens,
                                    List<FlinkComponentProvisioningEvidence> provisioning,
                                    Optional<FlinkHaControl.Observations> history) {
        if (!evidence.errors().isEmpty() || evidence.beforeFault().isEmpty() || evidence.afterHeal().isEmpty()
                || evidence.beforeCheckpoint().isEmpty() || evidence.afterCheckpoint().isEmpty()
                || evidence.checkpoint().isEmpty() || raw.before().isEmpty() || raw.after().isEmpty()
                || raw.tokensBefore().isEmpty() || raw.tokensAfter().isEmpty() || tokens.snapshot().isEmpty() || tokens.origins().isEmpty()
                || history.isEmpty() || history.orElseThrow().overflow()) {
            return Optional.of("Required token-checkpoint recovery barrier is incomplete");
        }
        var before = evidence.beforeFault().orElseThrow();
        var after = evidence.afterHeal().orElseThrow();
        if (!ready(before, raw.before().orElseThrow(), count, tokens, provisioning, history.orElseThrow())
                || !ready(after, raw.after().orElseThrow(), count, tokens, provisioning, history.orElseThrow())
                || before.afterSequence() != before.entrySnapshot().events().size()
                || !FlinkHaEvidence.prefix(before.snapshot(), raw.tokensBefore().orElseThrow())
                || !preFaultStable(before, raw, history.orElseThrow())
                || !FlinkHaEvidence.prefix(raw.tokensAfter().orElseThrow(), after.snapshot())
                || !FlinkHaEvidence.prefix(after.snapshot(), tokens.snapshot().orElseThrow())) {
            return Optional.of("Token barrier lacks healthy exact-incarnation delivery before and after the fault");
        }
        try {
            if (after.afterSequence() != healBoundary(raw)) return Optional.of("Token barrier reused a pre-heal request");
        } catch (IOException failure) {
            return Optional.of(failure.getMessage());
        }
        var first = evidence.beforeCheckpoint().orElseThrow();
        var last = evidence.afterCheckpoint().orElseThrow();
        var checkpoint = evidence.checkpoint().orElseThrow();
        if (!sample(first, raw.after().orElseThrow(), history.orElseThrow())
                || !sample(last, raw.after().orElseThrow(), history.orElseThrow())
                || endSequence(first) <= endSequence(after.afterTokens())
                || endSequence(last) <= endSequence(first)
                || !stable(history.orElseThrow(), after.beforeTokens(), last, raw.after().orElseThrow())
                || !evidence.afterCheckpointReceivers().equals(after.receivers())
                || !checkpoint.submissionAttempted() || !checkpoint.triggerId().matches("[0-9a-f]{32}")
                || !checkpoint.acknowledgedId().equals(Optional.of(checkpoint.triggerId()))
                || checkpoint.observations().isEmpty()) {
            return Optional.of("Checkpoint trigger or its unchanged leadership brackets are unconfirmed");
        }
        var polls = checkpoint.observations();
        for (int index = 0; index < polls.size(); index++) {
            var poll = polls.get(index);
            if (poll.failure().isPresent() || (index < polls.size() - 1
                    ? poll.state() != FlinkCheckpointTrigger.State.IN_PROGRESS || poll.checkpointId().isPresent()
                    : poll.state() != FlinkCheckpointTrigger.State.COMPLETED
                        || poll.checkpointId().isEmpty() || poll.checkpointId().orElseThrow() < 1)) {
                return Optional.of("Exact triggered checkpoint completion is unconfirmed");
            }
        }
        return Optional.empty();
    }

    /** Independent ordering for two adjacent opt-in operations, including coalesced samples. */
    static Optional<String> orderFailure(Evidence previous, Optional<Evidence> next) {
        if (previous.afterCheckpoint().isEmpty() || next.isEmpty()
                || next.orElseThrow().beforeFault().isEmpty()) {
            return Optional.of("Adjacent recovery barriers lack explicit sampled ordering boundaries");
        }
        long completed = endSequence(previous.afterCheckpoint().orElseThrow());
        long nextReadiness = endSequence(next.orElseThrow().beforeFault().orElseThrow().beforeTokens());
        return completed < nextReadiness ? Optional.empty()
                : Optional.of("A later leader fault began readiness before the previous checkpoint barrier completed");
    }

    private static boolean ready(Ready value, FlinkHaControl.Leadership leader, int count,
                                 FlinkHaEvidence.TokenEvidence tokens,
                                 List<FlinkComponentProvisioningEvidence> components,
                                 FlinkHaControl.Observations history) {
        if (count < 1 || value.receivers().size() != count
                || !sample(value.beforeTokens(), leader, history) || !sample(value.afterTokens(), leader, history)
                || endSequence(value.afterTokens()) <= endSequence(value.beforeTokens())
                || !stable(history, value.beforeTokens(), value.afterTokens(), leader)
                || !FlinkHaEvidence.completeTrace(value.entrySnapshot())
                || !FlinkHaEvidence.prefix(value.entrySnapshot(), value.snapshot())
                || !FlinkHaEvidence.processLabel(components, leader.resourceManager()).equals(Optional.of(value.issuerProcess()))
                || !hasOrigin(tokens, value.issuerProcess(), FlinkHaEvidence.TOKEN_PROVIDER)
                || !hasOrigin(tokens, value.issuerProcess(), FlinkHaEvidence.TOKEN_RECEIVER)) return false;
        HashSet<String> physical = new HashSet<>();
        for (int index = 0; index < count; index++) {
            var receiver = value.receivers().get(index);
            var identity = receiver.identity();
            if (!identity.logicalName().equals("taskmanager-" + (index + 1)) || !physical.add(identity.runtimeId())
                    || !process(components, identity.runtimeId(), identity.logicalName(), FlinkComponentRole.TASK_MANAGER)
                        .equals(Optional.of(receiver.classLoadProcess()))
                    || !hasOrigin(tokens, receiver.classLoadProcess(), FlinkHaEvidence.TOKEN_RECEIVER)) return false;
        }
        return issuance(value.snapshot(), value.issuerProcess(), value.receivers(), value.afterSequence())
                .filter(event -> event.sequence() == value.issuedSequence()).isPresent();
    }

    private static boolean hasOrigin(FlinkHaEvidence.TokenEvidence tokens, String process, String type) {
        return tokens.origins().orElseThrow().processes().stream().anyMatch(origin ->
                origin.process().equals(process) && origin.sources().getOrDefault(type, List.of())
                        .equals(List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH)));
    }

    private static boolean preFaultStable(Ready ready, FlinkHaControl.LeaderFaultEvidence raw,
                                          FlinkHaControl.Observations history) {
        return raw.before().isPresent() && history.leadership().stream().filter(sample ->
                endSequence(sample) > endSequence(ready.afterTokens())
                        && sample.moment() == FlinkHaControl.ObservationMoment.FAULT).findFirst()
                .filter(selected -> selected.leadership().equals(raw.before())
                        && stable(history, ready.beforeTokens(), selected, raw.before().orElseThrow())).isPresent();
    }

    private static boolean stable(FlinkHaControl.Observations history,
                                  FlinkHaControl.LeadershipObservation first,
                                  FlinkHaControl.LeadershipObservation last,
                                  FlinkHaControl.Leadership leader) {
        if (history.overflow()) return false;
        long cursor = endSequence(first);
        long end = endSequence(last);
        for (var observed : history.leadership()) {
            if (endSequence(observed) < cursor || observed.sequence() > end) continue;
            if (observed.sequence() > cursor || observed.error().isPresent()
                    || !observed.leadership().equals(Optional.of(leader))) return false;
            cursor = endSequence(observed) + 1;
            if (cursor > end) return true;
        }
        return false;
    }

    private static boolean sample(FlinkHaControl.LeadershipObservation sample, FlinkHaControl.Leadership leader,
                                  FlinkHaControl.Observations history) {
        return sample.moment() == FlinkHaControl.ObservationMoment.ROUTING && sample.error().isEmpty()
                && sample.leadership().equals(Optional.of(leader)) && FlinkHaEvidence.coherent(leader)
                && history.leadership().stream().anyMatch(recorded -> recorded.sequence() == sample.sequence()
                    && recorded.sampleCount() >= sample.sampleCount() && recorded.moment() == sample.moment()
                    && recorded.firstObservedAtMillis() == sample.firstObservedAtMillis()
                    && recorded.lastObservedAtMillis() >= sample.lastObservedAtMillis()
                    && recorded.leadership().equals(sample.leadership()) && recorded.error().isEmpty());
    }
}
