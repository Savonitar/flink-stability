package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget.TokenProofScope;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl.Event;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl.Kind;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl.Lifecycle;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl.Mode;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl.RegistrationSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenScopeProofTest {
    private static final String PROVIDER = "00000000-0000-0000-0000-000000000001";
    private static final String RECEIVER_ONE = "00000000-0000-0000-0000-000000000002";
    private static final String RECEIVER_TWO = "00000000-0000-0000-0000-000000000003";
    private static final String FOREIGN = "00000000-0000-0000-0000-000000000004";
    private static final String JOB = "a".repeat(32);
    private static final String ISSUER = "jobmanager-1#1";
    private static final List<TokenCheckpointBarrier.Receiver> RECEIVERS = List.of(
            receiver(1), receiver(2));

    @Test
    void freshSubmittedJobRequiresItsCapturedRegistrationAndBothReceivers() {
        Trace trace = new Trace();
        Event issued = trace.issue(job(1, 0, List.of(register(1, 1))), 2);

        assertEquals(Optional.of(issued), issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3));
        assertEquals(Optional.of(issued), TokenScopeProof.issuance(trace.snapshot(), ISSUER, RECEIVERS, 3,
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB)), true));
        assertTrue(TokenScopeProof.request(trace.events, trace.start(1),
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB))));
    }

    @Test
    void explicitBootstrapWorksAfterSubmissionButCannotSupplySubmittedJobProof() {
        Trace trace = new Trace();
        Event issued = trace.issue(bootstrap(0, 0, List.of()), 2);

        assertEquals(Optional.of(issued), issuance(trace, requirement(TokenProofScope.BOOTSTRAP), 3));
        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        Trace registered = new Trace();
        registered.issue(job(1, 0, List.of(register(1, 1))), 2);
        assertTrue(issuance(registered, requirement(TokenProofScope.BOOTSTRAP), 3).isEmpty());
    }

    @Test
    void omittedScopeStillAcceptsLegacyContextFreeEvidence() {
        Trace trace = new Trace();
        trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        List<Event> legacy = trace.events.stream().map(event -> new Event(event.sequence(), event.kind(),
                event.process(), event.role(), event.timestampMillis(), event.monotonicNanos(), event.requestId(),
                event.revision(), event.mode(), event.tokenSequence(), event.detail())).toList();
        var snapshot = new TokenServiceControl.Snapshot(legacy, false, false, 0, 1);

        assertTrue(TokenScopeProof.issuance(snapshot, ISSUER, RECEIVERS, 3, Optional.empty()).isPresent());
        assertTrue(TokenScopeProof.issuance(snapshot, ISSUER, RECEIVERS, 3,
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB))).isEmpty());
    }

    @Test
    void missingActualSubmissionWrongBindingAndPreSubmissionRequestsFailClosed() {
        Trace trace = new Trace();
        trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        List<TokenScopeProof.Requirement> invalid = List.of(
                new TokenScopeProof.Requirement(TokenProofScope.SUBMITTED_JOB, Optional.empty(), RECEIVERS),
                new TokenScopeProof.Requirement(TokenProofScope.BOOTSTRAP, Optional.empty(), RECEIVERS),
                bound(TokenProofScope.SUBMITTED_JOB, "b".repeat(32), "job", trace.prefix(3)),
                bound(TokenProofScope.SUBMITTED_JOB, JOB, "other-alias", trace.prefix(3)),
                bound(TokenProofScope.SUBMITTED_JOB, JOB, "job", trace.prefix((int) trace.start(1).sequence())));

        for (var expected : invalid) assertTrue(issuance(trace, expected, 3).isEmpty(), expected.toString());
        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), trace.start(1).sequence()).isEmpty());
    }

    @Test
    void declaredJobWithoutItsFullKnownPrefixAndMatchingGenerationIsInsufficient() {
        List<RegistrationSnapshot> invalid = List.of(
                job(1, 0, List.of()),
                job(1, 1, List.of()),
                job(2, 0, List.of(register(1, 1))),
                job(1, 0, List.of(register(1, 2))),
                job(1, 0, List.of(new Lifecycle(1, "REGISTER", 1, JOB, "wrong-alias"))),
                job(1, 0, List.of(new Lifecycle(1, "REGISTER", 1, "b".repeat(32), "job"))));

        for (var context : invalid) {
            Trace trace = new Trace();
            trace.issue(context, 2);
            assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty(), context.toString());
        }
    }

    @Test
    void aWatermarkCountCannotSubstituteForTheExactCompletePostSubmitPrefix() {
        Trace trace = new Trace();
        TokenServiceControl.Snapshot prefix = trace.snapshot();
        trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        Event initialized = prefix.events().getFirst();
        List<Event> different = new ArrayList<>(prefix.events());
        different.set(0, new Event(initialized.sequence(), initialized.kind(), initialized.process(),
                initialized.role(), initialized.timestampMillis() + 1, initialized.monotonicNanos(),
                initialized.requestId(), initialized.revision(), initialized.mode(), initialized.tokenSequence(),
                initialized.detail(), initialized.registration(), initialized.participantInstance()));
        for (var invalid : List.of(
                new TokenServiceControl.Snapshot(different, false, false, 0, 0),
                new TokenServiceControl.Snapshot(prefix.events(), true, false, 0, 0),
                new TokenServiceControl.Snapshot(prefix.events(), false, true, 0, 0),
                new TokenServiceControl.Snapshot(prefix.events(), false, false, 1, 1))) {
            assertTrue(issuance(trace, bound(TokenProofScope.SUBMITTED_JOB, JOB, "job", invalid), 3).isEmpty());
        }
    }

    @Test
    void futureRegisterCannotRepairAnEarlierRequestWithLostHistory() {
        Trace trace = new Trace();
        Event missing = trace.begin(job(1, 1, List.of()));
        trace.begin(job(1, 0, List.of(register(1, 1))));

        assertFalse(TokenScopeProof.request(trace.events, missing,
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB))));
    }

    @Test
    void providerContextAndInitializationMustNameTheActualRequestParticipant() {
        for (boolean initializationMismatch : List.of(false, true)) {
            Trace trace = new Trace();
            var context = initializationMismatch ? job(1, 0, List.of(register(1, 1)))
                    : new RegistrationSnapshot(FOREIGN, "JOB", 1, JOB, "job", false, 0, List.of(register(1, 1)));
            trace.issue(context, 2);
            if (initializationMismatch) {
                Event initialized = trace.events.getFirst();
                trace.events.set(0, context(initialized, initialized.registration(), Optional.of(FOREIGN)));
            }
            assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        }
    }

    @Test
    void successfulKnownAckRetiresPrefixAndIdenticalRegistrationKeepsGeneration() {
        Trace trace = new Trace();
        trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        trace.issue(job(1, 1, List.of(register(2, 1))), 2);
        long boundary = trace.events.size();
        Event issued = trace.issue(job(1, 2, List.of()), 2);

        assertEquals(Optional.of(issued), issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), boundary));
    }

    @Test
    void failedRequestDoesNotAuthorizeRetiringItsSentRegisterPrefix() {
        Trace trace = new Trace();
        trace.configure(Mode.FAIL);
        trace.fail(job(1, 0, List.of(register(1, 1))));
        trace.configure(Mode.HEALTHY);
        long boundary = trace.events.size();
        trace.issue(job(1, 1, List.of()), 2);

        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), boundary).isEmpty());
    }

    @Test
    void anAckInsideAKnownReturnedPrefixMustStillHaveBeenAnActualResponseEndpoint() {
        Trace trace = new Trace();
        trace.issue(job(1, 0, List.of(register(1, 1), register(2, 1), register(3, 1))), 2);
        long boundary = trace.events.size();
        trace.issue(job(1, 2, List.of(register(3, 1))), 2);

        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), boundary).isEmpty());
    }

    @Test
    void failedRequestCanRetransmitItsUnacknowledgedJournalWithoutInventingAnAck() {
        Trace trace = new Trace();
        trace.configure(Mode.FAIL);
        var registration = job(1, 0, List.of(register(1, 1)));
        trace.fail(registration);
        trace.configure(Mode.HEALTHY);
        long boundary = trace.events.size();
        Event issued = trace.issue(registration, 2);

        assertEquals(Optional.of(issued), issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), boundary));
    }

    @Test
    void aReturnedAckCanPrecedeTheServicesFinishEventWhenThatFinishIsRetainedLater() {
        Trace trace = new Trace();
        Event first = trace.begin(job(1, 0, List.of(register(1, 1))));
        Event firstIssued = trace.issued(first);
        Event next = trace.begin(job(1, 1, List.of()));
        trace.finish(first);
        Event nextIssued = trace.issued(next);
        trace.finish(next);
        trace.deliver(nextIssued, 2);

        assertEquals(Optional.of(nextIssued), issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB),
                firstIssued.sequence()));
    }

    @Test
    void aFutureIssuanceCannotRetroactivelyAuthorizeAnAlreadyClaimedAck() {
        Trace trace = new Trace();
        Event first = trace.begin(job(1, 0, List.of(register(1, 1))));
        Event next = trace.begin(job(1, 1, List.of()));
        trace.issued(first);
        trace.finish(first);
        Event nextIssued = trace.issued(next);
        trace.finish(next);
        trace.deliver(nextIssued, 2);

        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
    }

    @Test
    void aLaterConflictingJournalCannotHideBehindAnEarlierValidIssuance() {
        Trace trace = new Trace();
        trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        trace.begin(job(1, 0, List.of(new Lifecycle(1, "REGISTER", 1, JOB, "contradiction"))));

        assertFalse(TokenScopeProof.coverageValid(trace.snapshot()));
        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
    }

    @Test
    void stickyInvalidCoverageAndInvalidLifecycleCannotBeRehabilitatedByAnotherToken() {
        for (boolean explicitFlag : List.of(false, true)) {
            Trace trace = new Trace();
            var invalid = new RegistrationSnapshot(PROVIDER, "JOB", 1, JOB, "job", explicitFlag, 0,
                    explicitFlag ? List.of(register(1, 1))
                            : List.of(register(1, 1), new Lifecycle(2, "INVALID", 1, JOB, "job")));
            trace.issue(invalid, 2);
            trace.issue(job(1, 0, List.of(register(1, 1))), 2);

            assertFalse(TokenScopeProof.coverageValid(trace.snapshot()));
            assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        }
    }

    @Test
    void anOlderInFlightPrefixCannotQualifyButDoesNotContradictItsPreviouslySentRecords() {
        Trace trace = new Trace();
        var old = job(1, 0, List.of(register(1, 1)));
        trace.issue(old, 2);
        var newer = job(3, 0, List.of(register(1, 1),
                new Lifecycle(2, "UNREGISTER", 2, JOB, "job"), register(3, 3)));
        long boundary = trace.events.size();
        Event current = trace.issue(newer, 2);
        Event stale = trace.begin(old);

        assertFalse(TokenScopeProof.request(trace.events, stale,
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB))));
        assertTrue(TokenScopeProof.coverageValid(trace.snapshot()));
        assertEquals(Optional.of(current), issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), boundary));
    }

    @Test
    void matchingUnregisterAllowsExplicitBootstrapAndAReplacementRegisterAdvancesGeneration() {
        Trace trace = new Trace();
        trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        long boundary = trace.events.size();
        Event removed = trace.issue(bootstrap(2, 1,
                List.of(new Lifecycle(2, "UNREGISTER", 2, JOB, "job"))), 2);
        assertEquals(Optional.of(removed), issuance(trace, requirement(TokenProofScope.BOOTSTRAP), boundary));
        boundary = trace.events.size();
        Event replacement = trace.issue(job(3, 2, List.of(register(3, 3))), 2);
        assertEquals(Optional.of(replacement), issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), boundary));
    }

    @Test
    void secondJobAndChangedAliasCannotMasqueradeAsRepeatedRegistration() {
        for (var conflicting : List.of(new Lifecycle(2, "REGISTER", 1, "b".repeat(32), "job"),
                new Lifecycle(2, "REGISTER", 1, JOB, "other-alias"))) {
            Trace trace = new Trace();
            trace.issue(job(1, 0, List.of(register(1, 1), conflicting)), 2);
            assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        }
    }

    @Test
    void everyExpectedLiveReceiverNeedsItsOwnInitializedExactContextReceipt() {
        Trace missing = new Trace();
        missing.issue(job(1, 0, List.of(register(1, 1))), 1);
        assertTrue(issuance(missing, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        for (boolean foreignContext : List.of(false, true)) {
            Trace trace = new Trace();
            trace.issue(job(1, 0, List.of(register(1, 1))), 2);
            Event received = trace.events.getLast();
            trace.events.set(trace.events.size() - 1, context(received,
                    foreignContext ? Optional.of(bootstrap(0, 0, List.of())) : received.registration(),
                    foreignContext ? received.participantInstance() : Optional.of(FOREIGN)));
            assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        }
    }

    @Test
    void laterContradictoryOrMissingReceiptContextCannotHideBehindAnEarlierValidReceipt() {
        for (var conflicting : List.of(Optional.<RegistrationSnapshot>empty(),
                Optional.of(bootstrap(0, 0, List.of())))) {
            Trace trace = new Trace();
            Event issued = trace.issue(job(1, 0, List.of(register(1, 1))), 2);
            trace.add(Kind.RECEIVED, "taskmanager-2#1", "taskmanager", 0,
                    issued.tokenSequence(), conflicting, RECEIVER_TWO);

            assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        }
    }

    @Test
    void anOldJobRequestCannotProveDeliveryAfterItsObservedUnregister() {
        Trace trace = new Trace();
        Event old = trace.begin(job(1, 0, List.of(register(1, 1))));
        trace.issue(bootstrap(2, 0, List.of(register(1, 1),
                new Lifecycle(2, "UNREGISTER", 2, JOB, "job"))), 2);
        Event oldIssued = trace.issued(old);
        trace.finish(old);
        trace.deliver(oldIssued, 2);

        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        assertTrue(TokenScopeProof.issuance(trace.snapshot(), ISSUER, RECEIVERS, 3,
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB)), true).isEmpty());
    }

    @Test
    void completedJobDeliverySurvivesLaterNormalUnregisterButIsNotCurrentReadiness() {
        Trace trace = new Trace();
        Event delivered = trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        trace.issue(bootstrap(2, 1, List.of(new Lifecycle(2, "UNREGISTER", 2, JOB, "job"))), 2);

        assertEquals(Optional.of(delivered), issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3));
        assertTrue(TokenScopeProof.issuance(trace.snapshot(), ISSUER, RECEIVERS, 3,
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB)), true).isEmpty());
    }

    @Test
    void historicalFaultRequestRetainsItsCapturedJobScopeAfterNormalUnregister() {
        Trace trace = new Trace();
        trace.configure(Mode.FAIL);
        trace.fail(job(1, 0, List.of(register(1, 1))));
        Event faultRequest = trace.start(1);
        trace.configure(Mode.HEALTHY);
        trace.issue(bootstrap(2, 0, List.of(register(1, 1),
                new Lifecycle(2, "UNREGISTER", 2, JOB, "job"))), 2);

        assertTrue(TokenScopeProof.request(trace.events, faultRequest,
                Optional.of(requirement(TokenProofScope.SUBMITTED_JOB))));
        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
    }

    @Test
    void receiverInitializationCannotLegitimizeReusingTheIssuingProviderInstance() {
        Trace trace = new Trace();
        trace.issue(job(1, 0, List.of(register(1, 1))), 2);
        for (int index = 0; index < trace.events.size(); index++) {
            Event event = trace.events.get(index);
            if (event.process().equals("taskmanager-2#1")) {
                trace.events.set(index, context(event, event.registration(), Optional.of(PROVIDER)));
            }
        }

        assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
    }

    @Test
    void issuanceAndCompletionCannotMixOtherwiseMatchingRequestContexts() {
        for (Kind kind : List.of(Kind.ISSUED, Kind.REQUEST_FINISHED)) {
            Trace trace = new Trace();
            trace.issue(job(1, 0, List.of(register(1, 1))), 2);
            for (int index = 0; index < trace.events.size(); index++) {
                Event event = trace.events.get(index);
                if (event.kind() == kind) trace.events.set(index,
                        context(event, Optional.of(bootstrap(0, 0, List.of())), event.participantInstance()));
            }
            assertTrue(issuance(trace, requirement(TokenProofScope.SUBMITTED_JOB), 3).isEmpty());
        }
    }

    private static Optional<Event> issuance(Trace trace, TokenScopeProof.Requirement expected, long boundary) {
        return TokenScopeProof.issuance(trace.snapshot(), ISSUER, RECEIVERS, boundary, Optional.of(expected));
    }

    private static TokenScopeProof.Requirement requirement(TokenProofScope scope) {
        return bound(scope, JOB, "job", new Trace().snapshot());
    }

    private static TokenScopeProof.Requirement bound(TokenProofScope scope, String job, String alias,
                                                   TokenServiceControl.Snapshot prefix) {
        return new TokenScopeProof.Requirement(scope,
                Optional.of(new TokenScopeProof.Submission(job, alias, prefix)), RECEIVERS);
    }

    private static TokenCheckpointBarrier.Receiver receiver(int index) {
        return new TokenCheckpointBarrier.Receiver(new TaskManagerControl.Identity("taskmanager-" + index,
                "container-" + index, "resource-" + index), "taskmanager-" + index + "#1");
    }

    private static RegistrationSnapshot job(long generation, long acknowledged, List<Lifecycle> journal) {
        return new RegistrationSnapshot(PROVIDER, "JOB", generation, JOB, "job", false, acknowledged, journal);
    }

    private static RegistrationSnapshot bootstrap(long generation, long acknowledged, List<Lifecycle> journal) {
        return new RegistrationSnapshot(PROVIDER, "BOOTSTRAP", generation, "-", "-", false, acknowledged, journal);
    }

    private static Lifecycle register(long sequence, long generation) {
        return new Lifecycle(sequence, "REGISTER", generation, JOB, "job");
    }

    private static Event context(Event event, Optional<RegistrationSnapshot> registration, Optional<String> participant) {
        return new Event(event.sequence(), event.kind(), event.process(), event.role(), event.timestampMillis(),
                event.monotonicNanos(), event.requestId(), event.revision(), event.mode(), event.tokenSequence(),
                event.detail(), registration, participant);
    }

    private static final class Trace {
        private final List<Event> events = new ArrayList<>();
        private long requests;
        private long tokens;
        private long revision;
        private Mode mode = Mode.HEALTHY;
        private int active;
        private int maximum;

        private Trace() {
            add(Kind.PROVIDER_INITIALIZED, ISSUER, "jobmanager", 0, OptionalLong.empty(), Optional.empty(), PROVIDER);
            add(Kind.RECEIVER_INITIALIZED, "taskmanager-1#1", "taskmanager", 0,
                    OptionalLong.empty(), Optional.empty(), RECEIVER_ONE);
            add(Kind.RECEIVER_INITIALIZED, "taskmanager-2#1", "taskmanager", 0,
                    OptionalLong.empty(), Optional.empty(), RECEIVER_TWO);
        }

        private Event begin(RegistrationSnapshot registration) {
            active++;
            maximum = Math.max(maximum, active);
            return add(Kind.REQUEST_STARTED, ISSUER, "jobmanager", ++requests,
                    OptionalLong.empty(), Optional.of(registration), PROVIDER);
        }

        private Event start(long request) {
            return events.stream().filter(event -> event.kind() == Kind.REQUEST_STARTED && event.requestId() == request)
                    .findFirst().orElseThrow();
        }

        private Event issue(RegistrationSnapshot registration, int receivers) {
            Event start = begin(registration);
            Event issued = issued(start);
            finish(start);
            deliver(issued, receivers);
            return issued;
        }

        private Event issued(Event start) {
            return add(Kind.ISSUED, ISSUER, "jobmanager", start.requestId(),
                    OptionalLong.of(++tokens), start.registration(), PROVIDER);
        }

        private void deliver(Event issued, int receivers) {
            for (int index = 1; index <= receivers; index++) {
                add(Kind.RECEIVED, "taskmanager-" + index + "#1", "taskmanager", 0,
                        issued.tokenSequence(), issued.registration(), index == 1 ? RECEIVER_ONE : RECEIVER_TWO);
            }
        }

        private void fail(RegistrationSnapshot registration) {
            Event start = begin(registration);
            add(Kind.FAILED, ISSUER, "jobmanager", start.requestId(),
                    OptionalLong.empty(), Optional.of(registration), PROVIDER);
            finish(start);
        }

        private void finish(Event start) {
            add(Kind.REQUEST_FINISHED, ISSUER, "jobmanager", start.requestId(),
                    OptionalLong.empty(), start.registration(), PROVIDER);
            active--;
        }

        private void configure(Mode next) {
            mode = next;
            revision++;
            add(Kind.MODE_CHANGED, "service", "service", 0, OptionalLong.empty(), Optional.empty(), null);
        }

        private Event add(Kind kind, String process, String role, long request, OptionalLong token,
                          Optional<RegistrationSnapshot> registration, String participant) {
            long sequence = events.size() + 1L;
            Event event = new Event(sequence, kind, process, role, sequence * 1000, sequence * 1_000_000,
                    request, revision, mode, token, kind == Kind.FAILED ? "HTTP 503" : "synthetic",
                    registration, Optional.ofNullable(participant));
            events.add(event);
            return event;
        }

        private TokenServiceControl.Snapshot snapshot() {
            return new TokenServiceControl.Snapshot(events, false, false, active, maximum);
        }

        private TokenServiceControl.Snapshot prefix(int count) {
            List<Event> prefix = events.subList(0, count);
            int active = 0, maximum = 0;
            for (Event event : prefix) {
                if (event.kind() == Kind.REQUEST_STARTED) maximum = Math.max(maximum, ++active);
                if (event.kind() == Kind.REQUEST_FINISHED) active--;
            }
            return new TokenServiceControl.Snapshot(prefix, false, false, active, maximum);
        }
    }
}
