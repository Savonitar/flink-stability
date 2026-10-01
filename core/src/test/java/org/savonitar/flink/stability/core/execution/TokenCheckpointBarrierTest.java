package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.flink.*;
import org.savonitar.flink.stability.runtime.api.*;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class TokenCheckpointBarrierTest {
    private static final FlinkJobHandle JOB = new FlinkJobHandle("0123456789abcdef0123456789abcdef");
    private static final String SHA = "a".repeat(64);

    @Test
    void completesOnlyAfterNewTokenReachesEveryLiveTaskManagerAndExactCheckpointCompletes() {
        Fixture f = new Fixture();
        var operation = f.operation();
        assertTrue(operation.beforeFault());
        var raw = f.transfer();
        operation.afterHeal(raw);
        var evidence = operation.evidence();
        assertEquals(List.of(), evidence.errors());
        assertEquals(1, f.submissions);
        assertEquals(2, evidence.afterHeal().orElseThrow().receivers().size());
        assertEquals(42, evidence.checkpoint().orElseThrow().observations().getLast().checkpointId().orElseThrow());
        assertEquals(Optional.empty(), TokenCheckpointBarrier.failure(evidence, raw, 2,
                f.tokens(), f.components, Optional.of(f.history())));
        assertTrue(TokenCheckpointBarrier.failure(evidence, raw, 3,
                f.tokens(), f.components, Optional.of(f.history())).isPresent());
    }

    @Test
    void independentGateRejectsContradictoryCheckpointAndPartialReceiverProof() {
        Fixture f = new Fixture();
        var operation = f.operation();
        assertTrue(operation.beforeFault());
        var raw = f.transfer();
        operation.afterHeal(raw);
        var good = operation.evidence();
        var trigger = good.checkpoint().orElseThrow();
        var contradictory = new TokenCheckpointBarrier.Checkpoint(trigger.triggerId(), true,
                Optional.of("0".repeat(32)), trigger.observations());
        var wrongAck = new TokenCheckpointBarrier.Evidence(good.beforeFault(), good.afterHeal(),
                good.beforeCheckpoint(), good.afterCheckpoint(), Optional.of(contradictory),
                good.afterCheckpointReceivers(), List.of());
        var missingReceiver = new TokenCheckpointBarrier.Evidence(good.beforeFault(), good.afterHeal(),
                good.beforeCheckpoint(), good.afterCheckpoint(), good.checkpoint(),
                good.afterCheckpointReceivers().subList(0, 1), List.of());
        for (var changed : List.of(wrongAck, missingReceiver)) {
            assertTrue(TokenCheckpointBarrier.failure(changed, raw, 2,
                    f.tokens(), f.components, Optional.of(f.history())).isPresent());
        }
    }

    @Test
    void returningProcessCannotReuseAncientTokenReceiptsBeforeAnotherInjection() {
        Fixture f = new Fixture();
        f.issue("jobmanager-1#1", 0);
        f.issueOnPoll = false;
        var operation = f.operation();
        assertFalse(operation.beforeFault());
        assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("deadline")));
        assertEquals(0, f.submissions);
    }

    @Test
    void missingOneReceiverOrOldIncarnationDoesNotEstablishDelivery() {
        for (String receiver : List.of("missing", "taskmanager-2#0")) {
            Fixture f = new Fixture();
            f.secondReceiver = receiver;
            var operation = f.operation();
            assertFalse(operation.beforeFault());
            assertEquals(0, f.submissions);
        }
    }

    @Test
    void postHealCannotReuseOldRequestOrAnotherResourceManagersDelivery() {
        for (boolean otherManager : List.of(false, true)) {
            Fixture f = new Fixture();
            var operation = f.operation();
            assertTrue(operation.beforeFault());
            var raw = f.transfer();
            f.issueOnPoll = otherManager;
            f.wrongIssuer = otherManager;
            operation.afterHeal(raw);
            assertFalse(operation.evidence().errors().isEmpty());
            assertEquals(0, f.submissions);
        }
    }

    @Test
    void healthyRecoveryDoesNotAllowCheckpointOrNextFaultWhenDeclaredFaultWasMissedOrShortened() {
        for (boolean shortened : List.of(false, true)) {
            Fixture f = new Fixture();
            f.missedFault = !shortened;
            f.faultMode = shortened ? TokenServiceControl.Mode.DELAY : TokenServiceControl.Mode.FAIL;
            var operation = f.operation();
            assertTrue(operation.beforeFault());
            operation.afterHeal(f.transfer());
            assertTrue(operation.evidence().afterHeal().isPresent(), "healthy recovery is still retained");
            assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("No actual acquisition")));
            assertEquals(0, f.submissions);
        }
    }

    @Test
    void leaderChangeBetweenReadinessAndActualFaultSelectionStopsBarrier() {
        Fixture f = new Fixture();
        var operation = f.operation();
        assertTrue(operation.beforeFault());
        f.sample(leader(2));
        operation.afterHeal(f.transfer());
        assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("before actual fault selection")));
        assertEquals(0, f.submissions);
    }

    @Test
    void physicalFaultFlagsWithoutExactGateOrProcessProofCannotAdvance() {
        for (boolean wrongProcess : List.of(false, true)) {
            Fixture f = new Fixture();
            f.wrongFaultState = wrongProcess;
            f.closedConnections = wrongProcess ? 1 : 0;
            var operation = f.operation();
            assertTrue(operation.beforeFault());
            operation.afterHeal(f.transfer());
            assertFalse(operation.evidence().errors().isEmpty());
            assertTrue(operation.evidence().afterHeal().isEmpty());
            assertEquals(0, f.submissions);
        }
    }

    @Test
    void appendOnlyTokensCannotHideOverlappingBarriersOrMissingMixedBoundary() {
        Fixture f = new Fixture();
        var operation = f.operation();
        assertTrue(operation.beforeFault());
        operation.afterHeal(f.transfer());
        var complete = operation.evidence();
        assertEquals(List.of(), complete.errors());
        // Reusing a complete individually valid proof retains its entire token prefix,
        // but its readiness precedes its own checkpoint completion.
        assertTrue(TokenCheckpointBarrier.orderFailure(complete, Optional.of(complete), Optional.of(f.history())).isPresent());
        assertTrue(TokenCheckpointBarrier.orderFailure(complete, Optional.empty(), Optional.of(f.history())).isPresent());
        var oldReady = complete.afterHeal().orElseThrow();
        var completed = complete.afterCheckpoint().orElseThrow();
        var laterSample = new FlinkHaControl.LeadershipObservation(completed.sequence(),
                completed.sampleCount() + 1, completed.firstObservedAtMillis(), completed.lastObservedAtMillis() + 1,
                completed.moment(), completed.leadership(), completed.error());
        var laterReady = new TokenCheckpointBarrier.Ready(laterSample, laterSample, oldReady.receivers(),
                oldReady.issuerProcess(), oldReady.entrySnapshot(), oldReady.snapshot(),
                oldReady.afterSequence(), oldReady.issuedSequence());
        var later = new TokenCheckpointBarrier.Evidence(Optional.of(laterReady), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), List.of(), List.of());
        var samples = new ArrayList<>(f.history().leadership());
        samples.set(samples.size() - 1, laterSample);
        var history = new FlinkHaControl.Observations(samples, List.of(), false);
        assertEquals(Optional.empty(), TokenCheckpointBarrier.orderFailure(complete, Optional.of(later), Optional.of(history)),
                "a genuinely later observation within the same coalesced routing entry establishes order");
    }

    @Test
    void sampledElectionOrUnavailableObservationBetweenRecoveryBarriersCannotPass() {
        for (boolean unavailable : new boolean[] {false, true}) {
            Fixture f = new Fixture();
            var operation = f.operation();
            assertTrue(operation.beforeFault());
            operation.afterHeal(f.transfer());
            var complete = operation.evidence();
            var completed = complete.afterCheckpoint().orElseThrow();
            var samples = new ArrayList<>(f.history().leadership());
            long sequence = completed.sequence() + completed.sampleCount();
            samples.add(new FlinkHaControl.LeadershipObservation(sequence, 1,
                    completed.lastObservedAtMillis() + 1, completed.lastObservedAtMillis() + 1,
                    FlinkHaControl.ObservationMoment.ROUTING,
                    unavailable ? Optional.empty() : Optional.of(leader(1)),
                    unavailable ? Optional.of("transient discovery failure") : Optional.empty()));
            var resumed = new FlinkHaControl.LeadershipObservation(sequence + 1, 1,
                    completed.lastObservedAtMillis() + 2, completed.lastObservedAtMillis() + 2,
                    FlinkHaControl.ObservationMoment.ROUTING, completed.leadership(), Optional.empty());
            samples.add(resumed);
            var oldReady = complete.afterHeal().orElseThrow();
            var ready = new TokenCheckpointBarrier.Ready(resumed, resumed, oldReady.receivers(),
                    oldReady.issuerProcess(), oldReady.entrySnapshot(), oldReady.snapshot(),
                    oldReady.afterSequence(), oldReady.issuedSequence());
            var next = new TokenCheckpointBarrier.Evidence(Optional.of(ready), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty(), List.of(), List.of());
            assertTrue(TokenCheckpointBarrier.orderFailure(complete, Optional.of(next),
                    Optional.of(new FlinkHaControl.Observations(samples, List.of(), false))).orElseThrow()
                    .contains("between recovery barriers"));
            samples.set(samples.size() - 2, new FlinkHaControl.LeadershipObservation(sequence, 1,
                    completed.lastObservedAtMillis() + 1, completed.lastObservedAtMillis() + 1,
                    FlinkHaControl.ObservationMoment.ROUTING, completed.leadership(), Optional.empty()));
            assertEquals(Optional.empty(), TokenCheckpointBarrier.orderFailure(complete, Optional.of(next),
                    Optional.of(new FlinkHaControl.Observations(samples, List.of(), false))));
        }
    }

    @Test
    void ambiguousSubmissionIsRetainedAndNeverReplayed() {
        Fixture f = new Fixture();
        f.submissionFailure = true;
        var operation = f.operation();
        assertTrue(operation.beforeFault());
        operation.afterHeal(f.transfer());
        var checkpoint = operation.evidence().checkpoint().orElseThrow();
        assertTrue(checkpoint.submissionAttempted());
        assertTrue(checkpoint.acknowledgedId().isEmpty());
        assertTrue(checkpoint.observations().isEmpty());
        assertEquals(1, f.submissions);
        assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("unknown submission")));
    }

    @Test
    void interveningElectionAndReturnCannotPassMatchingLastSession() {
        Fixture f = new Fixture();
        f.interveningElection = true;
        var operation = f.operation();
        assertTrue(operation.beforeFault());
        var raw = f.transfer();
        operation.afterHeal(raw);
        assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("leadership changed")));
        assertTrue(TokenCheckpointBarrier.failure(operation.evidence(), raw, 2,
                f.tokens(), f.components, Optional.of(f.history())).isPresent());
    }

    @Test
    void receiverReplacementAcrossCheckpointDoesNotEstablishBarrier() {
        Fixture f = new Fixture();
        f.replaceAfterCheckpoint = true;
        var operation = f.operation();
        assertTrue(operation.beforeFault());
        operation.afterHeal(f.transfer());
        assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("incarnation changed")));
    }

    @Test
    void retentionBoundStopsBrokenClockAndSleeperFromGrowingUnboundedProof() {
        Fixture f = new Fixture();
        f.issueOnPoll = false;
        var operation = new TokenCheckpointBarrier.Operation(f.runtime, f.flink, JOB, 2,
                MonotonicDeadline.start(Duration.ofMinutes(2), () -> 0), ignored -> {});
        assertFalse(operation.beforeFault());
        assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("observation limit")));
    }

    @Test
    void explicitScopesUseTheSamePredicateOnlineAndDuringFinalBarrierEvaluation() {
        for (var scope : FlinkRuntimeTarget.TokenProofScope.values()) {
            Fixture f = new Fixture(true);
            f.bootstrap = scope == FlinkRuntimeTarget.TokenProofScope.BOOTSTRAP;
            var requirement = new TokenScopeProof.Requirement(scope,
                    Optional.of(new TokenScopeProof.Submission(JOB.jobId(), "copy", f.snapshot())), List.of());
            var operation = f.operation(Optional.of(requirement));
            assertTrue(operation.beforeFault());
            var raw = f.transfer();
            operation.afterHeal(raw);
            assertEquals(List.of(), operation.evidence().errors());
            assertEquals(1, f.submissions);
            assertEquals(Optional.empty(), TokenCheckpointBarrier.failure(operation.evidence(), raw, 2,
                    f.tokens(), f.components, Optional.of(f.history()), Optional.of(requirement)));
            var otherScope = new TokenScopeProof.Requirement(f.bootstrap
                    ? FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB : FlinkRuntimeTarget.TokenProofScope.BOOTSTRAP,
                    requirement.submission(), List.of());
            assertTrue(TokenCheckpointBarrier.failure(operation.evidence(), raw, 2,
                    f.tokens(), f.components, Optional.of(f.history()), Optional.of(otherScope)).isPresent());
        }
    }

    @Test
    void submittedJobBarrierRejectsWrongJobBeforeAnyFaultOrCheckpoint() {
        Fixture f = new Fixture(true);
        f.tokenJobId = "f".repeat(32);
        var requirement = new TokenScopeProof.Requirement(FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB,
                Optional.of(new TokenScopeProof.Submission(JOB.jobId(), "copy", f.snapshot())), List.of());
        var operation = f.operation(Optional.of(requirement));
        assertFalse(operation.beforeFault());
        assertEquals(0, f.submissions);
    }

    @Test
    void bootstrapFaultCannotBeExcusedByLaterHealthySubmittedJobTokens() {
        Fixture f = new Fixture(true);
        f.bootstrapFault = true;
        var requirement = new TokenScopeProof.Requirement(FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB,
                Optional.of(new TokenScopeProof.Submission(JOB.jobId(), "copy", f.snapshot())), List.of());
        var operation = f.operation(Optional.of(requirement));
        assertTrue(operation.beforeFault());
        operation.afterHeal(f.transfer());
        assertTrue(operation.evidence().afterHeal().isPresent());
        assertTrue(operation.evidence().errors().stream().anyMatch(error -> error.contains("No actual acquisition")));
        assertEquals(0, f.submissions);
    }

    @Test
    void controlEvaluationRequiresTheActualBindingAndEveryLiveReceiver() {
        Fixture f = new Fixture(true);
        var submission = new TokenScopeProof.Submission(JOB.jobId(), "copy", f.snapshot());
        f.issue("jobmanager-1#1", 0);
        var receivers = List.of(new TokenCheckpointBarrier.Receiver(
                        new TaskManagerControl.Identity("taskmanager-1", "tm-1", "resource-tm-1"), "taskmanager-1#1"),
                new TokenCheckpointBarrier.Receiver(
                        new TaskManagerControl.Identity("taskmanager-2", "tm-2", "resource-tm-2"), "taskmanager-2#1"));
        var requirement = new TokenScopeProof.Requirement(FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB,
                Optional.of(submission), receivers);
        var expected = new FlinkHaEvidence.Expected(List.of(), false, true, 2, Optional.of(requirement));
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, FlinkHaEvidence.evaluate(expected,
                Optional.of(new PhaseExecutionEvidence(List.of())), Optional.of(f.tokens()), f.components).outcome());
        for (var incomplete : List.of(new TokenScopeProof.Requirement(requirement.scope(), Optional.empty(), receivers),
                new TokenScopeProof.Requirement(requirement.scope(), Optional.of(submission), receivers.subList(0, 1)))) {
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, FlinkHaEvidence.evaluate(
                    expected.withTokenProof(Optional.of(incomplete)), Optional.of(new PhaseExecutionEvidence(List.of())),
                    Optional.of(f.tokens()), f.components).outcome());
        }
        f.events.removeLast();
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, FlinkHaEvidence.evaluate(expected,
                Optional.of(new PhaseExecutionEvidence(List.of())), Optional.of(f.tokens()), f.components).outcome());
    }

    private static FlinkHaControl.Leadership leader(int number) {
        var identity = new FlinkHaControl.LeaderIdentity("jobmanager-" + number, "jm-" + number,
                "http://jm-" + number, "session-" + number);
        return new FlinkHaControl.Leadership(identity, identity, identity);
    }

    private static final class Fixture {
        final AtomicLong clock = new AtomicLong();
        final List<TokenServiceControl.Event> events = new ArrayList<>();
        final List<FlinkHaControl.LeadershipObservation> samples = new ArrayList<>();
        final List<FlinkComponentProvisioningEvidence> components = new ArrayList<>();
        FlinkHaControl.Leadership current = leader(1);
        boolean issueOnPoll = true, wrongIssuer, submissionFailure, interveningElection, replaceAfterCheckpoint;
        boolean replacement, issuedCurrent, missedFault, wrongFaultState;
        long closedConnections = 1;
        TokenServiceControl.Mode faultMode = TokenServiceControl.Mode.FAIL;
        String secondReceiver = "taskmanager-2#1";
        int traceReads, submissions, checkpointPolls;
        long requestId, revision;
        final boolean scoped;
        boolean bootstrap, bootstrapFault;
        String tokenJobId = JOB.jobId();

        final V1AttemptRuntime runtime = (V1AttemptRuntime) Proxy.newProxyInstance(
                V1AttemptRuntime.class.getClassLoader(), new Class<?>[]{V1AttemptRuntime.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "haObservations": return Optional.of(history());
                        case "currentFlinkRestEndpoint": sample(current); return "http://localhost:8081";
                        case "flinkProvisioningEvidence": return List.copyOf(components);
                        case "taskManagerIdentity": {
                            String name = (String) args[0];
                            String id = name.equals("taskmanager-1") ? "tm-1" : replacement ? "tm-2-new" : "tm-2";
                            return Optional.of(new TaskManagerControl.Identity(name, id, "resource-" + id));
                        }
                        case "tokenServiceEvidence":
                            if (++traceReads % 2 == 0 && issueOnPoll && !issuedCurrent) {
                                issue(wrongIssuer ? "jobmanager-1#1" : current.resourceManager().logicalName() + "#1", revision);
                                issuedCurrent = true;
                            }
                            return Optional.of(snapshot());
                        default: throw new UnsupportedOperationException(method.getName());
                    }
                });

        final FlinkScenarioControl flink = (FlinkScenarioControl) Proxy.newProxyInstance(
                FlinkScenarioControl.class.getClassLoader(), new Class<?>[]{FlinkScenarioControl.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "triggerCheckpoint":
                            submissions++;
                            if (submissionFailure) throw new IOException("unknown submission outcome");
                            return args[1];
                        case "checkpointStatus":
                            checkpointPolls++;
                            if (checkpointPolls == 1) return new FlinkCheckpointTrigger.Observation(
                                    FlinkCheckpointTrigger.State.IN_PROGRESS, OptionalLong.empty(), Optional.empty());
                            if (interveningElection) { sample(leader(1)); sample(current); }
                            if (replaceAfterCheckpoint) {
                                replacement = true;
                                components.add(component("taskmanager-2", "tm-2-new", FlinkComponentRole.TASK_MANAGER));
                            }
                            return new FlinkCheckpointTrigger.Observation(FlinkCheckpointTrigger.State.COMPLETED,
                                    OptionalLong.of(42), Optional.empty());
                        default: throw new UnsupportedOperationException(method.getName());
                    }
                });

        Fixture() { this(false); }

        Fixture(boolean scoped) {
            this.scoped = scoped;
            for (int n = 1; n <= 2; n++) {
                components.add(component("jobmanager-" + n, "jm-" + n, FlinkComponentRole.JOB_MANAGER));
                components.add(component("taskmanager-" + n, "tm-" + n, FlinkComponentRole.TASK_MANAGER));
                event(TokenServiceControl.Kind.PROVIDER_INITIALIZED, "jobmanager-" + n + "#1", "jobmanager", 0, 0,
                        TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), "initialized");
                event(TokenServiceControl.Kind.RECEIVER_INITIALIZED, "taskmanager-" + n + "#1", "taskmanager", 0, 0,
                        TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), "initialized");
            }
        }

        TokenCheckpointBarrier.Operation operation() { return operation(Optional.empty()); }

        TokenCheckpointBarrier.Operation operation(Optional<TokenScopeProof.Requirement> proof) {
            return new TokenCheckpointBarrier.Operation(runtime, flink, JOB, 2,
                    MonotonicDeadline.start(Duration.ofSeconds(2), clock::get),
                    delay -> clock.addAndGet(delay.toNanos()), proof);
        }

        FlinkHaControl.LeaderFaultEvidence transfer() {
            var before = snapshot();
            sample(current, FlinkHaControl.ObservationMoment.FAULT);
            revision = 1;
            event(TokenServiceControl.Kind.MODE_CHANGED, "service", "service", 0, revision,
                    faultMode, OptionalLong.empty(), "delayMillis=" + (faultMode == TokenServiceControl.Mode.DELAY ? 5000 : 0));
            if (!missedFault) {
                long request = ++requestId;
                event(TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-2#1", "jobmanager", request, revision,
                        faultMode, OptionalLong.empty(), "started");
                if (faultMode == TokenServiceControl.Mode.DELAY) {
                    event(TokenServiceControl.Kind.ISSUED, "jobmanager-2#1", "jobmanager", request, revision,
                            faultMode, OptionalLong.of(request), "shortened delay");
                } else {
                    event(TokenServiceControl.Kind.FAILED, "jobmanager-2#1", "jobmanager", request, revision,
                            faultMode, OptionalLong.empty(), "HTTP 503");
                    event(TokenServiceControl.Kind.FAULT_OBSERVED, "jobmanager-2#1", "jobmanager", request, revision,
                            faultMode, OptionalLong.empty(), "HTTP 503");
                }
                event(TokenServiceControl.Kind.REQUEST_FINISHED, "jobmanager-2#1", "jobmanager", request, revision,
                        faultMode, OptionalLong.empty(), "finished");
            }
            var during = snapshot();
            revision = 2;
            event(TokenServiceControl.Kind.MODE_CHANGED, "service", "service", 0, revision,
                    TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), "delayMillis=0");
            var after = snapshot();
            current = leader(2);
            issuedCurrent = false;
            traceReads = 0;
            return new FlinkHaControl.LeaderFaultEvidence(new FlinkHaControl.LeaderFaultRequest(
                    FlinkHaControl.Mode.ISOLATE_ZOOKEEPER, Duration.ofSeconds(1), Duration.ofSeconds(2),
                    Optional.of(new FlinkHaControl.TokenFault(faultMode,
                            faultMode == TokenServiceControl.Mode.DELAY ? Duration.ofSeconds(5) : Duration.ZERO))),
                    Optional.of(leader(1)), Optional.of(leader(2)), Optional.of(leader(1).resourceManager()), true, true,
                    Optional.of(new FlinkHaControl.ProcessState(wrongFaultState ? "another-jm" : "jm-1", true, false)),
                    Optional.of(new FlinkHaControl.ProcessState("jm-1", true, false)), 1000, 2000, closedConnections, 0, false,
                    Optional.of(before), Optional.of(during), Optional.of(after), List.of());
        }

        void issue(String process, long rev) {
            long request = ++requestId;
            event(TokenServiceControl.Kind.REQUEST_STARTED, process, "jobmanager", request, rev,
                    TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), "started");
            event(TokenServiceControl.Kind.ISSUED, process, "jobmanager", request, rev,
                    TokenServiceControl.Mode.HEALTHY, OptionalLong.of(request), "issued");
            event(TokenServiceControl.Kind.REQUEST_FINISHED, process, "jobmanager", request, rev,
                    TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), "finished");
            event(TokenServiceControl.Kind.RECEIVED, "taskmanager-1#1", "taskmanager", 0, rev,
                    TokenServiceControl.Mode.HEALTHY, OptionalLong.of(request), "received");
            if (!secondReceiver.equals("missing")) event(TokenServiceControl.Kind.RECEIVED, secondReceiver, "taskmanager", 0, rev,
                    TokenServiceControl.Mode.HEALTHY, OptionalLong.of(request), "received");
        }

        void event(TokenServiceControl.Kind kind, String process, String role, long request, long rev,
                   TokenServiceControl.Mode mode, OptionalLong token, String detail) {
            long seq = events.size() + 1;
            Optional<TokenServiceControl.RegistrationSnapshot> registration = Optional.empty();
            Optional<String> participant = Optional.empty();
            if (scoped && !"service".equals(role)) {
                participant = Optional.of(java.util.UUID.nameUUIDFromBytes(
                        process.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString());
                if (kind == TokenServiceControl.Kind.RECEIVED) {
                    registration = events.stream().filter(event -> event.kind() == TokenServiceControl.Kind.ISSUED
                            && event.tokenSequence().equals(token)).findFirst().orElseThrow().registration();
                } else if (kind != TokenServiceControl.Kind.PROVIDER_INITIALIZED
                        && kind != TokenServiceControl.Kind.RECEIVER_INITIALIZED) {
                    boolean boot = bootstrap || bootstrapFault && rev == 1;
                    registration = Optional.of(new TokenServiceControl.RegistrationSnapshot(participant.orElseThrow(),
                            boot ? "BOOTSTRAP" : "JOB", boot ? 0 : 1, boot ? "-" : tokenJobId, boot ? "-" : "copy", false, 0,
                            boot ? List.of() : List.of(new TokenServiceControl.Lifecycle(1, "REGISTER", 1, tokenJobId, "copy"))));
                }
            }
            events.add(new TokenServiceControl.Event(seq, kind, process, role, seq, seq, request, rev, mode, token, detail,
                    registration, participant));
        }

        TokenServiceControl.Snapshot snapshot() {
            return new TokenServiceControl.Snapshot(events, false, false, 0, requestId == 0 ? 0 : 1);
        }

        void sample(FlinkHaControl.Leadership value) { sample(value, FlinkHaControl.ObservationMoment.ROUTING); }

        void sample(FlinkHaControl.Leadership value, FlinkHaControl.ObservationMoment moment) {
            long seq = samples.size() + 1;
            samples.add(new FlinkHaControl.LeadershipObservation(seq, 1, seq, seq,
                    moment, Optional.of(value), Optional.empty()));
        }

        FlinkHaControl.Observations history() { return new FlinkHaControl.Observations(samples, List.of(), false); }

        FlinkHaEvidence.TokenEvidence tokens() {
            List<SubjectClassOrigins.ProcessOrigin> origins = components.stream().map(value ->
                    new SubjectClassOrigins.ProcessOrigin(value.logicalName() + "#1", Map.of(
                            FlinkHaEvidence.TOKEN_PROVIDER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH),
                            FlinkHaEvidence.TOKEN_RECEIVER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH)))).toList();
            return new FlinkHaEvidence.TokenEvidence(Optional.of(SHA), Optional.of(new SubjectClassOrigins(
                    FlinkHaEvidence.TOKEN_CONTAINER_PATH, origins, Optional.empty())), Optional.of(snapshot()), List.of());
        }
    }

    private static FlinkComponentProvisioningEvidence component(String name, String runtime, FlinkComponentRole role) {
        return FlinkComponentProvisioningEvidence.verified(name, role, runtime, "flink:2.2.0", "sha256:" + SHA,
                SHA, SHA, List.of());
    }
}
