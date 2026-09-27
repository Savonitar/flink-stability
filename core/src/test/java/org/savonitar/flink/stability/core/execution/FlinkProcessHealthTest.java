package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlinkProcessHealthTest {
    private final FlinkProcessWriteFenceEvidence fence = FlinkRuntimeIdentityTest.fence(1);
    private final FlinkProcessWriteFenceEvidence.Observations healthy =
            FlinkRuntimeIdentityTest.healthyProcesses(fence);

    @Test
    void onlyCompleteCoherentLiveObservationsPermitPass() {
        assertEquals(FlinkProcessHealth.Outcome.HEALTHY, evaluate(healthy).outcome());
        assertEquals(FlinkProcessHealth.Outcome.UNCONFIRMED, FlinkProcessHealth.evaluate(
                FlinkRuntimeIdentityTest.expected(Optional.empty()), Optional.of(fence), Optional.empty()).outcome());
        for (var evidence : List.of(
                new FlinkProcessWriteFenceEvidence.Observations(healthy.observations(), healthy.fenced(), true),
                new FlinkProcessWriteFenceEvidence.Observations(healthy.observations().subList(0, 1), healthy.fenced(), false),
                new FlinkProcessWriteFenceEvidence.Observations(healthy.observations(), List.of(), false),
                replaceFirst(Optional.empty(), false, Optional.of("inspect unavailable")),
                replaceFirst(Optional.of(new FlinkHaControl.ProcessState("jm", true, true)), false, Optional.empty()))) {
            assertEquals(FlinkProcessHealth.Outcome.UNCONFIRMED, evaluate(evidence).outcome());
        }
    }

    @Test
    void stoppedOrMissingProcessIsAnUnexpectedExitRegardlessOfExitCode() {
        for (long code : List.of(0L, 1L, 137L)) {
            var evidence = replaceFirst(Optional.of(new FlinkHaControl.ProcessState("jm", false, false,
                    Optional.of(code), Optional.of(code == 137), Optional.of("2026-09-27T12:00:00Z"))),
                    false, Optional.empty());
            assertEquals(FlinkProcessHealth.Outcome.UNEXPECTED_EXIT, evaluate(evidence).outcome());
        }
        assertEquals(FlinkProcessHealth.Outcome.UNEXPECTED_EXIT,
                evaluate(replaceFirst(Optional.empty(), true, Optional.of("container missing"))).outcome());
    }

    @Test
    void expectedTerminationRequiresAnExactPriorDeclaredKillAndCannotBorrowAnotherIncarnation() {
        var stopped = replaceFirst(Optional.of(new FlinkHaControl.ProcessState("jm", false, false)),
                false, Optional.empty());
        var original = stopped.observations().getFirst();
        for (String id : List.of("jm", "earlier-jm")) {
            var events = new ArrayList<>(stopped.observations());
            events.add(new FlinkProcessWriteFenceEvidence.Observation(original.logicalName(), original.role(),
                    Optional.of(id), FlinkProcessWriteFenceEvidence.Moment.AFTER_DECLARED_KILL,
                    Instant.EPOCH.minusSeconds(1), Optional.of(new FlinkHaControl.ProcessState(id, false, false)),
                    false, Optional.empty()));
            assertEquals(id.equals("jm") ? FlinkProcessHealth.Outcome.HEALTHY
                    : FlinkProcessHealth.Outcome.UNEXPECTED_EXIT,
                    evaluate(new FlinkProcessWriteFenceEvidence.Observations(events, healthy.fenced(), false)).outcome());
        }
    }

    @Test
    void anOomObservedAfterARequestedKillIsStillAnUnexpectedFinding() {
        var events = new ArrayList<>(healthy.observations());
        var original = events.getFirst();
        events.add(new FlinkProcessWriteFenceEvidence.Observation(original.logicalName(), original.role(),
                original.runtimeId(), FlinkProcessWriteFenceEvidence.Moment.AFTER_DECLARED_KILL,
                Instant.EPOCH, Optional.of(new FlinkHaControl.ProcessState("jm", false, false,
                        Optional.of(137L), Optional.of(true), Optional.of("2026-09-27T12:00:00Z"))),
                false, Optional.empty()));
        assertEquals(FlinkProcessHealth.Outcome.UNEXPECTED_EXIT,
                evaluate(new FlinkProcessWriteFenceEvidence.Observations(events, healthy.fenced(), false)).outcome());
    }

    @Test
    void anAlreadyStoppedFenceOutcomeDoesNotProveTheHarnessCausedALaterExit() {
        var first = fence.components().getFirst();
        var components = new ArrayList<>(fence.components());
        components.set(0, new FlinkProcessWriteFenceEvidence.Component(first.logicalName(), first.role(),
                first.runtimeId(), FlinkProcessWriteFenceEvidence.Outcome.ALREADY_STOPPED));
        assertEquals(FlinkProcessHealth.Outcome.UNCONFIRMED, FlinkProcessHealth.evaluate(
                FlinkRuntimeIdentityTest.expected(Optional.empty()),
                Optional.of(new FlinkProcessWriteFenceEvidence(components, fence.completedAt())),
                Optional.of(new FlinkProcessWriteFenceEvidence.Observations(healthy.observations(), components, false)))
                .outcome());
    }

    @Test
    void partialFenceRetainsUnexpectedExitButCannotClaimCompletion() {
        assertEquals(FlinkProcessHealth.Outcome.UNCONFIRMED, FlinkProcessHealth.evaluate(
                FlinkRuntimeIdentityTest.expected(Optional.empty()), Optional.empty(), Optional.of(healthy)).outcome());
        var stopped = replaceFirst(Optional.of(new FlinkHaControl.ProcessState("jm", false, false)),
                false, Optional.empty());
        assertEquals(FlinkProcessHealth.Outcome.UNEXPECTED_EXIT, FlinkProcessHealth.evaluate(
                FlinkRuntimeIdentityTest.expected(Optional.empty()), Optional.empty(), Optional.of(stopped)).outcome());
    }

    private FlinkProcessWriteFenceEvidence.Observations replaceFirst(Optional<FlinkHaControl.ProcessState> state,
            boolean missing, Optional<String> diagnostic) {
        var first = healthy.observations().getFirst();
        var events = new ArrayList<>(healthy.observations());
        events.set(0, new FlinkProcessWriteFenceEvidence.Observation(first.logicalName(), first.role(),
                first.runtimeId(), first.moment(), first.observedAt(), state, missing, diagnostic));
        return new FlinkProcessWriteFenceEvidence.Observations(events, healthy.fenced(), false);
    }

    private FlinkProcessHealth evaluate(FlinkProcessWriteFenceEvidence.Observations observations) {
        return FlinkProcessHealth.evaluate(FlinkRuntimeIdentityTest.expected(Optional.empty()),
                Optional.of(fence), Optional.of(observations));
    }
}
