package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkHaEvidenceTest {
    private static final String PATH = "$/phases/0/steps/0";

    @Test
    void everyFaultModeRequiresARealLeaderMoveAndAHealedPhysicalOperation() {
        for (var mode : FlinkHaControl.Mode.values()) {
            Fixture fixture = new Fixture(mode);
            assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, fixture.evaluate().outcome());
            fixture.applied = false;
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        }
    }

    @Test
    void physicalSuccessFlagsDoNotExcuseAShortenedDeclaredFaultDuration() {
        Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
        fixture.healedAtMillis = 1_999;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void absenceOfUnrequestedHaEvidenceDoesNotAddAFinding() {
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, FlinkHaEvidence.evaluate(
                new FlinkHaEvidence.Expected(List.of(), false, false), Optional.empty(), Optional.empty()).outcome());
    }

    @Test
    void killSuccessFlagsCannotReplaceAnObservedExitOrReplacement() {
        Fixture fixture = new Fixture(FlinkHaControl.Mode.KILL);
        fixture.faultState = Optional.of(new FlinkHaControl.ProcessState("jm-1", true, false));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        fixture.faultState = Optional.of(new FlinkHaControl.ProcessState("jm-1", false, false));
        fixture.healedState = Optional.of(new FlinkHaControl.ProcessState("jm-1", true, false));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        fixture.healedState = Optional.empty();
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void pauseAndIsolationRequireTheirSpecificPhysicalEvidenceAndHealing() {
        Fixture pause = new Fixture(FlinkHaControl.Mode.PAUSE);
        pause.faultState = Optional.of(new FlinkHaControl.ProcessState("jm-1", true, false));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, pause.evaluate().outcome());
        Fixture isolation = new Fixture(FlinkHaControl.Mode.ISOLATE_ZOOKEEPER);
        isolation.closed = 0;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, isolation.evaluate().outcome());
        isolation.closed = 1;
        isolation.isolationActiveAfterHeal = true;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, isolation.evaluate().outcome());
        isolation.isolationActiveAfterHeal = false;
        isolation.errors = List.of("could not restore ZooKeeper route");
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, isolation.evaluate().outcome());
    }

    @Test
    void allLeaderRolesMustMoveTogetherAndEachSessionMustChange() {
        var old = leadership("jobmanager-1", "jm-1", "old");
        var moved = leadership("jobmanager-2", "jm-2", "new");
        for (var invalid : List.of(old,
                new FlinkHaControl.Leadership(moved.resourceManager(), old.dispatcher(), moved.restServer()),
                new FlinkHaControl.Leadership(moved.resourceManager(), moved.dispatcher(), old.restServer()),
                leadership("jobmanager-2", "jm-2", "old"))) {
            Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
            fixture.after = Optional.of(invalid);
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        }
        Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
        fixture.before = Optional.of(new FlinkHaControl.Leadership(old.resourceManager(),
                moved.dispatcher(), old.restServer()));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        fixture.before = Optional.of(old);
        fixture.target = Optional.of(moved.resourceManager());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void aDeclaredButUnreachedFaultCannotDisappearWithItsObservations() {
        Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                FlinkHaEvidence.evaluate(fixture.expected(), Optional.of(new PhaseExecutionEvidence(List.of())),
                        Optional.empty()).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                FlinkHaEvidence.evaluate(fixture.expected(), Optional.empty(), Optional.empty()).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                FlinkHaEvidence.evaluate(new FlinkHaEvidence.Expected(List.of(), false, false),
                        Optional.of(fixture.phase()), Optional.empty()).outcome());
    }

    @Test
    void aTornSessionSnapshotCannotConfirmAnOtherwiseCoherentLeaderMove() {
        for (boolean beforeFault : List.of(true, false)) {
            for (boolean dispatcher : List.of(true, false)) {
                Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
                var original = (beforeFault ? fixture.before : fixture.after).orElseThrow();
                var role = dispatcher ? original.dispatcher() : original.restServer();
                var tornRole = new FlinkHaControl.LeaderIdentity(role.logicalName(), role.runtimeId(),
                        role.address(), "different-session");
                var torn = new FlinkHaControl.Leadership(original.resourceManager(),
                        dispatcher ? tornRole : original.dispatcher(),
                        dispatcher ? original.restServer() : tornRole);
                if (beforeFault) fixture.before = Optional.of(torn);
                else fixture.after = Optional.of(torn);
                assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
            }
        }
    }

    @Test
    void requestLocationLoopAndSuccessfulStepMustExactlyMatchThePlan() {
        Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
        var declaration = fixture.expected().faults().getFirst();
        var loop = new PhaseExecutionEvidence.LoopIteration(PATH, 1, 2);
        for (var wrong : List.of(
                new FlinkHaEvidence.DeclaredFault(PATH + "/loop/steps/0", List.of(), declaration.request()),
                new FlinkHaEvidence.DeclaredFault(PATH, List.of(loop), declaration.request()),
                new FlinkHaEvidence.DeclaredFault(PATH, List.of(), request(FlinkHaControl.Mode.KILL)))) {
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                    FlinkHaEvidence.evaluate(new FlinkHaEvidence.Expected(List.of(wrong), true, false),
                            Optional.of(fixture.phase()), Optional.empty(), provisioning(),
                            Optional.of(haHistory(fixture.before.orElseThrow(), fixture.after.orElseThrow()))).outcome());
        }
        fixture.status = PhaseExecutionEvidence.StepStatus.FAILED;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void nestedLoopsExpandInExecutionOrderWithTheirFullCoordinates() {
        var request = request(FlinkHaControl.Mode.PAUSE);
        var phases = List.of(new ExecutableScenarioPlan.Phase("ha", List.of(
                new ExecutableScenarioPlan.Loop(2, List.of(
                        new ExecutableScenarioPlan.LeaderFault(request),
                        new ExecutableScenarioPlan.Loop(2, List.of(
                                new ExecutableScenarioPlan.LeaderFault(request))))))));
        var expected = FlinkHaEvidence.Expected.from(phases, true, true);
        assertEquals(6, expected.faults().size());
        assertEquals(true, expected.tokenProviderRequired());
        assertEquals("$/phases/0/steps/0/loop/steps/1/loop/steps/0", expected.faults().get(2).path());
        assertEquals(List.of(new PhaseExecutionEvidence.LoopIteration(PATH, 1, 2),
                        new PhaseExecutionEvidence.LoopIteration(PATH + "/loop/steps/1", 2, 2)),
                expected.faults().get(2).loopIterations());
        assertEquals(List.of(new PhaseExecutionEvidence.LoopIteration(PATH, 2, 2)),
                expected.faults().get(3).loopIterations());
    }

    @Test
    void ordinaryLoopsAreSkippedAndExpandedLeaderFaultsAreIndependentlyBounded() {
        assertEquals(List.of(), FlinkHaEvidence.Expected.from(List.of(
                new ExecutableScenarioPlan.Phase("ordinary", List.of(new ExecutableScenarioPlan.Loop(
                        Integer.MAX_VALUE, List.of(new ExecutableScenarioPlan.Wait(Duration.ofMillis(1))))))),
                false, false).faults());
        assertThrows(IllegalArgumentException.class, () -> FlinkHaEvidence.Expected.from(List.of(
                new ExecutableScenarioPlan.Phase("too-many", List.of(new ExecutableScenarioPlan.Loop(
                101, List.of(new ExecutableScenarioPlan.LeaderFault(request(FlinkHaControl.Mode.KILL))))))), true, false));
    }

    @Test
    void twoLoopIterationsCannotReuseOnePhysicalOperationTransferOrRestore() {
        Fixture first = new Fixture(FlinkHaControl.Mode.PAUSE);
        Fixture second = laterFault();
        // A spontaneous election between operations is allowed; the next before session
        // need not equal the preceding after session.
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, evaluateLoop(first, second).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateLoop(first, first).outcome());
        second.armedAtMillis = 1_999;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateLoop(first, second).outcome());
        second = laterFault();
        second.after = first.after;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateLoop(first, second).outcome());
        second = laterFault();
        second.jobAfter = first.jobAfter;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateLoop(first, second).outcome());
    }

    @Test
    void twoTokenFaultIterationsCannotReuseOneRevisionAndItsHealthyRecovery() {
        Fixture first = tokenFixture(TokenServiceControl.Mode.FAIL);
        Fixture second = tokenFixture(TokenServiceControl.Mode.FAIL);
        second.armedAtMillis = 3_000;
        second.healedAtMillis = 4_000;
        second.before = Optional.of(leadership("jobmanager-1", "jm-1", "later-before"));
        second.target = second.before.map(FlinkHaControl.Leadership::resourceManager);
        second.after = Optional.of(leadership("jobmanager-2", "jm-2", "later-after"));
        second.jobAfter = observation(FlinkJobState.RUNNING, 0, 1,
                Optional.of(new FlinkJobObservation.Restore(2, 3_100)));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateLoop(first, second).outcome());
        mergeRepeatedTokens(first, second, true);
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, evaluateLoop(first, second).outcome());

        first = tokenFixture(TokenServiceControl.Mode.FAIL);
        second = tokenFixture(TokenServiceControl.Mode.FAIL);
        second.armedAtMillis = 3_000;
        second.healedAtMillis = 4_000;
        second.before = Optional.of(leadership("jobmanager-1", "jm-1", "later-before"));
        second.target = second.before.map(FlinkHaControl.Leadership::resourceManager);
        second.after = Optional.of(leadership("jobmanager-2", "jm-2", "later-after"));
        second.jobAfter = observation(FlinkJobState.RUNNING, 0, 1,
                Optional.of(new FlinkJobObservation.Restore(2, 3_100)));
        mergeRepeatedTokens(first, second, false);
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateLoop(first, second).outcome());
    }

    private static void mergeRepeatedTokens(Fixture first, Fixture second, boolean firstRecovered) {
        var firstEvents = first.tokens.orElseThrow().snapshot().orElseThrow().events();
        int limit = firstRecovered ? firstEvents.size() : (int) firstEvents.stream().filter(event ->
                event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                        && event.mode() == TokenServiceControl.Mode.HEALTHY).findFirst().orElseThrow().sequence();
        List<TokenServiceControl.Event> combined = new ArrayList<>(firstEvents.subList(0, limit));
        first.tokensAfter = Optional.of(snapshot(combined));
        second.tokensBefore = Optional.of(snapshot(combined));
        var secondEvents = second.tokens.orElseThrow().snapshot().orElseThrow().events();
        for (var event : secondEvents.subList(9, secondEvents.size())) {
            combined.add(new TokenServiceControl.Event(combined.size() + 1, event.kind(), event.process(),
                    event.role(), event.timestampMillis() + 1_000, event.monotonicNanos() + 1_000_000_000,
                    event.requestId() == 0 ? 0 : event.requestId() + 2, event.revision() + 2,
                    event.mode(), event.tokenSequence().isEmpty() ? OptionalLong.empty()
                    : OptionalLong.of(event.tokenSequence().getAsLong() + 2), event.detail()));
        }
        long secondHeal = combined.stream().filter(event -> event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                && event.mode() == TokenServiceControl.Mode.HEALTHY && event.revision() == 4)
                .findFirst().orElseThrow().sequence();
        second.tokensDuring = Optional.of(snapshot(combined.subList(0, (int) secondHeal - 1)));
        second.tokensAfter = Optional.of(snapshot(combined));
        first.tokens = second.tokens = Optional.of(tokens(snapshot(combined)));
    }

    private static Fixture laterFault() {
        Fixture second = new Fixture(FlinkHaControl.Mode.PAUSE);
        second.armedAtMillis = 3_000;
        second.healedAtMillis = 4_000;
        second.before = Optional.of(leadership("jobmanager-1", "jm-1", "later-before"));
        second.target = second.before.map(FlinkHaControl.Leadership::resourceManager);
        second.after = Optional.of(leadership("jobmanager-2", "jm-2", "later-after"));
        second.jobAfter = observation(FlinkJobState.RUNNING, 0, 1,
                Optional.of(new FlinkJobObservation.Restore(2, 3_100)));
        return second;
    }

    private static FlinkHaEvidence evaluateLoop(Fixture first, Fixture second) {
        List<PhaseExecutionEvidence.StepEvidence> steps = new ArrayList<>();
        List<PhaseExecutionEvidence.LeaderFault> observed = new ArrayList<>();
        String path = PATH + "/loop/steps/0";
        for (Fixture fixture : List.of(first, second)) {
            var iterations = List.of(new PhaseExecutionEvidence.LoopIteration(PATH, steps.size() + 1, 2));
            var raw = fixture.phase().leaderFaults().getFirst();
            steps.add(new PhaseExecutionEvidence.StepEvidence(0, "ha", path, iterations,
                    PhaseExecutionEvidence.StepKind.LEADER_FAULT, fixture.status, "fault"));
            observed.add(new PhaseExecutionEvidence.LeaderFault(path, iterations, raw.jobId(),
                    raw.jobBefore(), raw.jobAfter(), raw.raw(), raw.observationErrors()));
        }
        var expected = FlinkHaEvidence.Expected.from(List.of(new ExecutableScenarioPlan.Phase("ha",
                List.of(new ExecutableScenarioPlan.Loop(2,
                        List.of(new ExecutableScenarioPlan.LeaderFault(first.request)))))), true, first.tokens.isPresent());
        return FlinkHaEvidence.evaluate(expected, Optional.of(new PhaseExecutionEvidence(
                steps, List.of(), List.of(), List.of(), observed)), second.tokens, provisioning(),
                Optional.of(haHistory(first.before.orElseThrow(), second.after.orElseThrow())));
    }

    @Test
    void recoveryNeedsACompletedCheckpointAndANewRestoreButNotCrossLeaderCounterGrowth() {
        Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
        fixture.jobBefore = observation(FlinkJobState.RUNNING, 4, 9,
                Optional.of(new FlinkJobObservation.Restore(7, 400)));
        fixture.jobAfter = observation(FlinkJobState.FINISHED, 0, 1,
                Optional.of(new FlinkJobObservation.Restore(8, 300)));
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, fixture.evaluate().outcome());
        fixture.jobAfter = fixture.jobBefore;
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        fixture.jobAfter = observation(FlinkJobState.RUNNING, 1, 1,
                Optional.of(new FlinkJobObservation.Restore(8, 600)));
        fixture.jobBefore = observation(FlinkJobState.RUNNING, 0, 0, Optional.empty());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        fixture.jobBefore = new FlinkJobObservation.Attempt(Optional.empty(), Optional.of("REST unavailable"));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void recoveredObservationKeepsTransientPollErrorsWithoutInvalidatingCompleteProof() {
        Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
        fixture.observationErrors = List.of("IOException: GET /jobs/job-id returned HTTP 404");
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, fixture.evaluate().outcome());
        assertEquals(fixture.observationErrors, fixture.phase().leaderFaults().getFirst().observationErrors());
        fixture.jobAfter = new FlinkJobObservation.Attempt(Optional.empty(), Optional.of("HTTP 404"));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void healthyTokensNeedVerifiedOriginsAndLinkedJobManagerIssueToTaskManagerReceipt() {
        var events = healthyEvents();
        var tokens = tokens(snapshot(events));
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, evaluateTokens(tokens).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(tokens(snapshot(events.subList(0, events.size() - 1)))).outcome());
        var foreign = new SubjectClassOrigins(FlinkHaEvidence.TOKEN_CONTAINER_PATH,
                List.of(new SubjectClassOrigins.ProcessOrigin("jobmanager-1#1", Map.of(
                        FlinkHaEvidence.TOKEN_PROVIDER, List.of("/foreign.jar")))), Optional.empty());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(new FlinkHaEvidence.TokenEvidence(tokens.pluginSha256(), Optional.of(foreign),
                        tokens.snapshot(), List.of())).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(new FlinkHaEvidence.TokenEvidence(Optional.empty(), tokens.origins(),
                        tokens.snapshot(), List.of())).outcome());
        for (var invalid : List.of(new TokenServiceControl.Snapshot(events, true, false, 0, 1),
                new TokenServiceControl.Snapshot(events, false, true, 0, 1))) {
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateTokens(tokens(invalid)).outcome());
        }
    }

    @Test
    void healthyReceiptFromAnUnprovisionedTaskManagerCannotConfirmDelivery() {
        List<TokenServiceControl.Event> events = new ArrayList<>(healthyEvents());
        var receipt = events.getLast();
        events.set(events.size() - 1, new TokenServiceControl.Event(receipt.sequence(), receipt.kind(),
                "taskmanager-1#2", receipt.role(), receipt.timestampMillis(), receipt.monotonicNanos(),
                receipt.requestId(), receipt.revision(), receipt.mode(), receipt.tokenSequence(), receipt.detail()));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateTokens(tokens(snapshot(events))).outcome());
    }

    @Test
    void healthyControlRequiresTheSameTokenOnEveryDeclaredCurrentTaskManager() {
        var components = new ArrayList<>(provisioning());
        components.add(FlinkRuntimeIdentityTest.component("taskmanager-2", "tm-2", FlinkRuntimeIdentityTest.IMAGE_ID));
        var events = new ArrayList<>(healthyEvents());
        var expected = new FlinkHaEvidence.Expected(List.of(), false, true, 2);
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(expected, tokensWithReceiver(events, "taskmanager-2#1"), components).outcome());
        events.add(event(8, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "taskmanager-2#1", "taskmanager",
                0, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(9, TokenServiceControl.Kind.RECEIVED, "taskmanager-2#1", "taskmanager",
                0, 0, TokenServiceControl.Mode.HEALTHY, 1, ""));
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED,
                evaluateTokens(expected, tokensWithReceiver(events, "taskmanager-2#1"), components).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(expected, tokens(snapshot(events)), components).outcome(), "missing second origin");
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(new FlinkHaEvidence.Expected(List.of(), false, true, 3),
                        tokensWithReceiver(events, "taskmanager-2#1"), components).outcome());

        events.removeLast();
        events.add(event(9, TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-1#1", "jobmanager",
                2, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(10, TokenServiceControl.Kind.ISSUED, "jobmanager-1#1", "jobmanager",
                2, 0, TokenServiceControl.Mode.HEALTHY, 2, ""));
        events.add(event(11, TokenServiceControl.Kind.REQUEST_FINISHED, "jobmanager-1#1", "jobmanager",
                2, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(12, TokenServiceControl.Kind.RECEIVED, "taskmanager-2#1", "taskmanager",
                0, 0, TokenServiceControl.Mode.HEALTHY, 2, ""));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(expected, tokensWithReceiver(events, "taskmanager-2#1"), components).outcome(),
                "different tokens on different receivers do not prove one all-TM delivery");
    }

    @Test
    void retiredTaskManagerReceiptsCannotSupplyTheCurrentIncarnationsDelivery() {
        var components = new ArrayList<>(provisioning());
        components.add(FlinkRuntimeIdentityTest.component("taskmanager-1", "tm-replacement", FlinkRuntimeIdentityTest.IMAGE_ID));
        var expected = new FlinkHaEvidence.Expected(List.of(), false, true, 1);
        var events = new ArrayList<>(healthyEvents());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluateTokens(expected, tokensWithReceiver(events, "taskmanager-1#2"), components).outcome());
        events.add(event(8, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "taskmanager-1#2", "taskmanager",
                0, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(9, TokenServiceControl.Kind.RECEIVED, "taskmanager-1#2", "taskmanager",
                0, 0, TokenServiceControl.Mode.HEALTHY, 1, ""));
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED,
                evaluateTokens(expected, tokensWithReceiver(events, "taskmanager-1#2"), components).outcome());
    }

    @Test
    void healthyDeliveryRequiresProviderAndReceiverInitializationOnTheAttributedProcesses() {
        for (var kind : List.of(TokenServiceControl.Kind.PROVIDER_INITIALIZED,
                TokenServiceControl.Kind.RECEIVER_INITIALIZED)) {
            List<TokenServiceControl.Event> events = healthyEvents().stream().map(event -> event.kind() == kind
                    ? new TokenServiceControl.Event(event.sequence(), event.kind(), "jobmanager-9#9", "jobmanager",
                    event.timestampMillis(), event.monotonicNanos(), event.requestId(), event.revision(),
                    event.mode(), event.tokenSequence(), event.detail()) : event).toList();
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateTokens(tokens(snapshot(events))).outcome());
        }
    }

    @Test
    void aHealthyDeliveryCannotHideAnUnfinishedRequestOrReusedRequestIdentity() {
        List<TokenServiceControl.Event> events = new ArrayList<>(healthyEvents());
        events.add(event(8, TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-1#1", "jobmanager",
                2, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateTokens(tokens(snapshot(events))).outcome());
        events.set(7, event(8, TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-1#1", "jobmanager",
                1, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluateTokens(tokens(snapshot(events))).outcome());
    }

    @Test
    void faultRevisionNeedsAnActualRequestOutcomeAndHealthyDeliveryAfterHealing() {
        for (var mode : List.of(TokenServiceControl.Mode.FAIL, TokenServiceControl.Mode.LINKAGE_ERROR,
                TokenServiceControl.Mode.DELAY)) {
            Fixture fixture = tokenFixture(mode);
            assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, fixture.evaluate().outcome());
            List<TokenServiceControl.Event> changed = new ArrayList<>(fixture.tokens.orElseThrow()
                    .snapshot().orElseThrow().events());
            var original = changed.stream().filter(event -> event.requestId() == 2
                    && event.kind() == TokenServiceControl.Kind.REQUEST_STARTED).findFirst().orElseThrow();
            changed.set((int) original.sequence() - 1, new TokenServiceControl.Event(original.sequence(), original.kind(),
                    original.process(), original.role(), original.timestampMillis(), original.monotonicNanos(),
                    original.requestId(), 99, original.mode(), original.tokenSequence(), original.detail()));
            replaceTokenSnapshots(fixture, changed);
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        }
        Fixture fixture = tokenFixture(TokenServiceControl.Mode.FAIL);
        var events = fixture.tokens.orElseThrow().snapshot().orElseThrow().events();
        var healed = events.stream().filter(event -> event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                && event.mode() == TokenServiceControl.Mode.HEALTHY).findFirst().orElseThrow();
        fixture.tokensAfter = Optional.of(snapshot(events.subList(0, (int) healed.sequence())));
        fixture.tokens = Optional.of(tokens(snapshot(events.subList(0, (int) healed.sequence()))));
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void configuredDelayCannotStandInForItsActualMonotonicDuration() {
        Fixture fixture = tokenFixture(TokenServiceControl.Mode.DELAY);
        replaceFaultEvents(fixture, event -> event.kind() == TokenServiceControl.Kind.ISSUED
                ? new TokenServiceControl.Event(event.sequence(), event.kind(), event.process(), event.role(),
                event.timestampMillis(), 90_000_001, event.requestId(), event.revision(), event.mode(),
                event.tokenSequence(), event.detail()) : event);
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
    }

    @Test
    void transportFailureCannotImpersonateTheDeclaredServiceFailure() {
        for (var mode : List.of(TokenServiceControl.Mode.FAIL, TokenServiceControl.Mode.LINKAGE_ERROR)) {
            Fixture fixture = tokenFixture(mode);
            replaceFaultEvents(fixture, event -> event.kind() == TokenServiceControl.Kind.FAILED
                    ? new TokenServiceControl.Event(event.sequence(), event.kind(), event.process(), event.role(),
                    event.timestampMillis(), event.monotonicNanos(), event.requestId(), event.revision(), event.mode(),
                    event.tokenSequence(), "HTTP response delivery failed") : event);
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        }
    }

    @Test
    void serverFailureNeedsTheClientsMatchingAcknowledgementAndNoTransportFailure() {
        for (var mode : List.of(TokenServiceControl.Mode.FAIL, TokenServiceControl.Mode.LINKAGE_ERROR)) {
            Fixture fixture = tokenFixture(mode);
            replaceFaultEvents(fixture, event -> event.kind() == TokenServiceControl.Kind.FAULT_OBSERVED
                    ? new TokenServiceControl.Event(event.sequence(), TokenServiceControl.Kind.FAILED,
                    event.process(), event.role(), event.timestampMillis(), event.monotonicNanos(),
                    event.requestId(), event.revision(), event.mode(), event.tokenSequence(),
                    "HTTP response delivery failed") : event);
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
            fixture = tokenFixture(mode);
            replaceFaultEvents(fixture, event -> event.kind() == TokenServiceControl.Kind.FAULT_OBSERVED
                    ? new TokenServiceControl.Event(event.sequence(), event.kind(), event.process(), event.role(),
                    event.timestampMillis(), event.monotonicNanos(), 99, event.revision(), event.mode(),
                    event.tokenSequence(), event.detail()) : event);
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());

            fixture = tokenFixture(mode);
            List<TokenServiceControl.Event> contradictory = new ArrayList<>(fixture.tokens.orElseThrow()
                    .snapshot().orElseThrow().events());
            var finished = contradictory.stream().filter(event -> event.requestId() == 2
                    && event.kind() == TokenServiceControl.Kind.REQUEST_FINISHED).findFirst().orElseThrow();
            contradictory.add((int) finished.sequence() - 1, new TokenServiceControl.Event(finished.sequence(),
                    TokenServiceControl.Kind.FAILED, finished.process(), finished.role(), finished.timestampMillis(),
                    finished.monotonicNanos(), finished.requestId(), finished.revision(), finished.mode(),
                    OptionalLong.empty(), "HTTP response delivery failed"));
            for (int index = 0; index < contradictory.size(); index++) {
                var event = contradictory.get(index);
                contradictory.set(index, new TokenServiceControl.Event(index + 1, event.kind(), event.process(),
                        event.role(), event.timestampMillis(), event.monotonicNanos(), event.requestId(),
                        event.revision(), event.mode(), event.tokenSequence(), event.detail()));
            }
            replaceTokenSnapshots(fixture, contradictory);
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        }
    }

    @Test
    void aFaultedAcquisitionMustBelongToTheNewLeaderAndItsExactIncarnation() {
        for (String wrongProcess : List.of("jobmanager-1#1", "jobmanager-2#2")) {
            Fixture fixture = tokenFixture(TokenServiceControl.Mode.FAIL);
            replaceFaultEvents(fixture, event -> new TokenServiceControl.Event(event.sequence(), event.kind(),
                    wrongProcess, event.role(), event.timestampMillis(), event.monotonicNanos(),
                    event.requestId(), event.revision(), event.mode(), event.tokenSequence(), event.detail()));
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, fixture.evaluate().outcome());
        }
    }

    private static void replaceFaultEvents(Fixture fixture,
            java.util.function.UnaryOperator<TokenServiceControl.Event> transform) {
        List<TokenServiceControl.Event> events = new ArrayList<>(fixture.tokens.orElseThrow()
                .snapshot().orElseThrow().events());
        for (int index = 0; index < events.size(); index++) {
            if (events.get(index).requestId() == 2) events.set(index, transform.apply(events.get(index)));
        }
        replaceTokenSnapshots(fixture, events);
    }

    private static void replaceTokenSnapshots(Fixture fixture, List<TokenServiceControl.Event> events) {
        long healing = events.stream().filter(event -> event.kind() == TokenServiceControl.Kind.MODE_CHANGED
                && event.mode() == TokenServiceControl.Mode.HEALTHY).findFirst().orElseThrow().sequence();
        fixture.tokensDuring = Optional.of(snapshot(events.subList(0, (int) healing - 1)));
        fixture.tokensAfter = Optional.of(snapshot(events));
        fixture.tokens = Optional.of(tokens(snapshot(events)));
    }

    private static Fixture tokenFixture(TokenServiceControl.Mode mode) {
        Fixture fixture = new Fixture(FlinkHaControl.Mode.PAUSE);
        Duration delay = mode == TokenServiceControl.Mode.DELAY ? Duration.ofMillis(10) : Duration.ZERO;
        fixture.request = new FlinkHaControl.LeaderFaultRequest(FlinkHaControl.Mode.PAUSE,
                Duration.ofSeconds(1), Duration.ofSeconds(10),
                Optional.of(new FlinkHaControl.TokenFault(mode, delay)));
        List<TokenServiceControl.Event> events = new ArrayList<>(healthyEvents());
        events.add(event(8, TokenServiceControl.Kind.PROVIDER_INITIALIZED, "jobmanager-2#1", "jobmanager",
                0, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(9, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "jobmanager-2#1", "jobmanager",
                0, 0, TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(10, TokenServiceControl.Kind.MODE_CHANGED, "service", "service", 0, 1, mode, 0,
                "delayMillis=" + delay.toMillis()));
        events.add(event(11, TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-2#1", "jobmanager", 2, 1, mode, 0, ""));
        events.add(event(12, mode == TokenServiceControl.Mode.DELAY ? TokenServiceControl.Kind.ISSUED
                : TokenServiceControl.Kind.FAILED, "jobmanager-2#1", "jobmanager", 2, 1, mode, 2,
                mode == TokenServiceControl.Mode.FAIL ? "HTTP 503" : "HTTP 598"));
        if (mode != TokenServiceControl.Mode.DELAY) {
            events.add(event(events.size() + 1, TokenServiceControl.Kind.FAULT_OBSERVED, "jobmanager-2#1", "jobmanager",
                    2, 1, mode, 0, mode == TokenServiceControl.Mode.FAIL ? "HTTP 503" : "HTTP 598"));
        }
        events.add(event(events.size() + 1, TokenServiceControl.Kind.REQUEST_FINISHED, "jobmanager-2#1", "jobmanager", 2, 1, mode, 0, ""));
        fixture.tokensBefore = Optional.of(snapshot(events.subList(0, 9)));
        fixture.tokensDuring = Optional.of(snapshot(events));
        events.add(event(events.size() + 1, TokenServiceControl.Kind.MODE_CHANGED, "service", "service", 0, 2,
                TokenServiceControl.Mode.HEALTHY, 0, "delayMillis=0"));
        events.add(event(events.size() + 1, TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-2#1", "jobmanager", 3, 2,
                TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(events.size() + 1, TokenServiceControl.Kind.ISSUED, "jobmanager-2#1", "jobmanager", 3, 2,
                TokenServiceControl.Mode.HEALTHY, 3, ""));
        events.add(event(events.size() + 1, TokenServiceControl.Kind.REQUEST_FINISHED, "jobmanager-2#1", "jobmanager", 3, 2,
                TokenServiceControl.Mode.HEALTHY, 0, ""));
        events.add(event(events.size() + 1, TokenServiceControl.Kind.RECEIVED, "taskmanager-1#1", "taskmanager", 0, 2,
                TokenServiceControl.Mode.HEALTHY, 3, ""));
        fixture.tokensAfter = Optional.of(snapshot(events));
        fixture.tokens = Optional.of(tokens(snapshot(events)));
        return fixture;
    }

    private static FlinkHaEvidence evaluateTokens(FlinkHaEvidence.TokenEvidence tokens) {
        return FlinkHaEvidence.evaluate(new FlinkHaEvidence.Expected(List.of(), false, true),
                Optional.of(new PhaseExecutionEvidence(List.of())), Optional.of(tokens), provisioning());
    }

    private static FlinkHaEvidence evaluateTokens(FlinkHaEvidence.Expected expected,
            FlinkHaEvidence.TokenEvidence tokens,
            List<org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence> components) {
        return FlinkHaEvidence.evaluate(expected, Optional.of(new PhaseExecutionEvidence(List.of())),
                Optional.of(tokens), components);
    }

    private static FlinkHaEvidence.TokenEvidence tokensWithReceiver(List<TokenServiceControl.Event> events,
                                                                    String process) {
        var base = tokens(snapshot(events));
        var origins = new ArrayList<>(base.origins().orElseThrow().processes());
        origins.add(new SubjectClassOrigins.ProcessOrigin(process, Map.of(
                FlinkHaEvidence.TOKEN_RECEIVER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH))));
        return new FlinkHaEvidence.TokenEvidence(base.pluginSha256(), Optional.of(new SubjectClassOrigins(
                FlinkHaEvidence.TOKEN_CONTAINER_PATH, origins, Optional.empty())), base.snapshot(), List.of());
    }

    private static List<org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence> provisioning() {
        return List.of(
                FlinkRuntimeIdentityTest.component("jobmanager-1", "jm-1", FlinkRuntimeIdentityTest.IMAGE_ID),
                FlinkRuntimeIdentityTest.component("jobmanager-2", "jm-2", FlinkRuntimeIdentityTest.IMAGE_ID),
                FlinkRuntimeIdentityTest.component("taskmanager-1", "tm-1", FlinkRuntimeIdentityTest.IMAGE_ID));
    }

    private static FlinkHaControl.Observations haHistory(FlinkHaControl.Leadership initial,
                                                         FlinkHaControl.Leadership last) {
        var sessions = provisioning().stream().map(component -> new FlinkHaControl.SessionEvidence(
                component.logicalName(), component.role(), component.runtimeId(), component.logicalName() + "#1",
                6_000, List.of(new FlinkHaControl.NegotiatedSession("0x123", 6_000, "own Flink log")), false)).toList();
        return new FlinkHaControl.Observations(List.of(
                new FlinkHaControl.LeadershipObservation(1, 1, 100, 100,
                        FlinkHaControl.ObservationMoment.INITIAL, Optional.of(initial), Optional.empty()),
                new FlinkHaControl.LeadershipObservation(2, 1, 5_000, 5_000,
                        FlinkHaControl.ObservationMoment.PRE_FENCE, Optional.of(last), Optional.empty())), sessions, false);
    }

    private static FlinkHaEvidence.TokenEvidence tokens(TokenServiceControl.Snapshot snapshot) {
        var sources = new SubjectClassOrigins(FlinkHaEvidence.TOKEN_CONTAINER_PATH, List.of(
                new SubjectClassOrigins.ProcessOrigin("jobmanager-1#1", Map.of(
                        FlinkHaEvidence.TOKEN_PROVIDER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH),
                        FlinkHaEvidence.TOKEN_RECEIVER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH))),
                new SubjectClassOrigins.ProcessOrigin("jobmanager-2#1", Map.of(
                        FlinkHaEvidence.TOKEN_PROVIDER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH),
                        FlinkHaEvidence.TOKEN_RECEIVER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH))),
                new SubjectClassOrigins.ProcessOrigin("taskmanager-1#1", Map.of(
                        FlinkHaEvidence.TOKEN_RECEIVER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH)))), Optional.empty());
        return new FlinkHaEvidence.TokenEvidence(Optional.of("a".repeat(64)), Optional.of(sources),
                Optional.of(snapshot), List.of());
    }

    private static List<TokenServiceControl.Event> healthyEvents() {
        var healthy = TokenServiceControl.Mode.HEALTHY;
        return List.of(event(1, TokenServiceControl.Kind.PROVIDER_INITIALIZED, "jobmanager-1#1", "jobmanager", 0, 0, healthy, 0, ""),
                event(2, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "jobmanager-1#1", "jobmanager", 0, 0, healthy, 0, ""),
                event(3, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "taskmanager-1#1", "taskmanager", 0, 0, healthy, 0, ""),
                event(4, TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-1#1", "jobmanager", 1, 0, healthy, 0, ""),
                event(5, TokenServiceControl.Kind.ISSUED, "jobmanager-1#1", "jobmanager", 1, 0, healthy, 1, ""),
                event(6, TokenServiceControl.Kind.REQUEST_FINISHED, "jobmanager-1#1", "jobmanager", 1, 0, healthy, 0, ""),
                event(7, TokenServiceControl.Kind.RECEIVED, "taskmanager-1#1", "taskmanager", 0, 0, healthy, 1, ""));
    }

    private static TokenServiceControl.Event event(long sequence, TokenServiceControl.Kind kind,
            String process, String role, long request, long revision, TokenServiceControl.Mode mode,
            long token, String detail) {
        return new TokenServiceControl.Event(sequence, kind, process, role, sequence * 10,
                sequence * 10_000_000, request, revision, mode,
                token == 0 ? OptionalLong.empty() : OptionalLong.of(token), detail);
    }

    private static TokenServiceControl.Snapshot snapshot(List<TokenServiceControl.Event> events) {
        return new TokenServiceControl.Snapshot(events, false, false, 0, 1);
    }

    static FlinkHaControl.Leadership leadership(String logical, String runtime, String epoch) {
        return new FlinkHaControl.Leadership(
                new FlinkHaControl.LeaderIdentity(logical, runtime, "rpc-rm-" + runtime, epoch),
                new FlinkHaControl.LeaderIdentity(logical, runtime, "rpc-dispatcher-" + runtime, epoch),
                new FlinkHaControl.LeaderIdentity(logical, runtime, "rest-" + runtime, epoch));
    }

    private static FlinkHaControl.LeaderFaultRequest request(FlinkHaControl.Mode mode) {
        return new FlinkHaControl.LeaderFaultRequest(mode, Duration.ofSeconds(1),
                Duration.ofSeconds(10), Optional.empty());
    }

    private static FlinkJobObservation.Attempt observation(FlinkJobState state, long completed,
            long restored, Optional<FlinkJobObservation.Restore> restore) {
        return new FlinkJobObservation.Attempt(Optional.of(new FlinkJobObservation(
                1_000, state, completed, restored, restore, List.of(), List.of())), Optional.empty());
    }

    private static final class Fixture {
        FlinkHaControl.LeaderFaultRequest request;
        Optional<FlinkHaControl.Leadership> before = Optional.of(leadership("jobmanager-1", "jm-1", "old"));
        Optional<FlinkHaControl.Leadership> after = Optional.of(leadership("jobmanager-2", "jm-2", "new"));
        Optional<FlinkHaControl.LeaderIdentity> target = before.map(FlinkHaControl.Leadership::resourceManager);
        boolean applied = true;
        Optional<FlinkHaControl.ProcessState> faultState;
        Optional<FlinkHaControl.ProcessState> healedState;
        long closed;
        long armedAtMillis = 1_000;
        long healedAtMillis = 2_000;
        boolean isolationActiveAfterHeal;
        List<String> errors = List.of();
        List<String> observationErrors = List.of();
        PhaseExecutionEvidence.StepStatus status = PhaseExecutionEvidence.StepStatus.SUCCEEDED;
        FlinkJobObservation.Attempt jobBefore = observation(FlinkJobState.RUNNING, 1, 0, Optional.empty());
        FlinkJobObservation.Attempt jobAfter = observation(FlinkJobState.RUNNING, 0, 1,
                Optional.of(new FlinkJobObservation.Restore(1, 1_100)));
        Optional<TokenServiceControl.Snapshot> tokensBefore = Optional.empty();
        Optional<TokenServiceControl.Snapshot> tokensDuring = Optional.empty();
        Optional<TokenServiceControl.Snapshot> tokensAfter = Optional.empty();
        Optional<FlinkHaEvidence.TokenEvidence> tokens = Optional.empty();

        Fixture(FlinkHaControl.Mode mode) {
            request = request(mode);
            faultState = Optional.of(new FlinkHaControl.ProcessState("jm-1",
                    mode != FlinkHaControl.Mode.KILL, mode == FlinkHaControl.Mode.PAUSE));
            healedState = Optional.of(new FlinkHaControl.ProcessState(
                    mode == FlinkHaControl.Mode.KILL ? "jm-1-new" : "jm-1", true, false));
            closed = mode == FlinkHaControl.Mode.ISOLATE_ZOOKEEPER ? 1 : 0;
        }

        FlinkHaEvidence.Expected expected() {
            return new FlinkHaEvidence.Expected(List.of(new FlinkHaEvidence.DeclaredFault(PATH, List.of(), request)),
                    true, tokens.isPresent());
        }

        PhaseExecutionEvidence phase() {
            var raw = new FlinkHaControl.LeaderFaultEvidence(request, before, after, target, applied, true,
                    faultState, healedState, armedAtMillis, healedAtMillis, closed, 0, isolationActiveAfterHeal,
                    tokensBefore, tokensDuring, tokensAfter, errors);
            return new PhaseExecutionEvidence(List.of(new PhaseExecutionEvidence.StepEvidence(
                    0, "ha", PATH, List.of(), PhaseExecutionEvidence.StepKind.LEADER_FAULT, status, "fault")),
                    List.of(), List.of(), List.of(), List.of(new PhaseExecutionEvidence.LeaderFault(
                            PATH, List.of(), "job-id", jobBefore, jobAfter, raw, observationErrors)));
        }

        FlinkHaEvidence evaluate() {
            return FlinkHaEvidence.evaluate(expected(), Optional.of(phase()), tokens, provisioning(), Optional.of(haHistory(
                    before.orElseGet(() -> leadership("jobmanager-1", "jm-1", "old")),
                    after.orElseGet(() -> leadership("jobmanager-2", "jm-2", "new")))));
        }
    }
}
