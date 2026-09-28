package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlinkHaObservationsTest {
    private static final FlinkHaControl.Leadership INITIAL =
            FlinkHaEvidenceTest.leadership("jobmanager-1", "jm-1", "initial-session");
    private static final List<FlinkComponentProvisioningEvidence> COMPONENTS = List.of(
            FlinkRuntimeIdentityTest.component("jobmanager-1", "jm-1", FlinkRuntimeIdentityTest.IMAGE_ID),
            FlinkRuntimeIdentityTest.component("jobmanager-2", "jm-2", FlinkRuntimeIdentityTest.IMAGE_ID),
            FlinkRuntimeIdentityTest.component("taskmanager-1", "tm-1", FlinkRuntimeIdentityTest.IMAGE_ID));

    @Test
    void requiredHaControlNeedsCoherentInitialAndPreFenceBoundaries() {
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED, evaluate(history()).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluate(Optional.empty()).outcome());
        for (var retained : List.of(List.of(history().leadership().getFirst()),
                List.of(sample(1, FlinkHaControl.ObservationMoment.PRE_FENCE, INITIAL)),
                List.<FlinkHaControl.LeadershipObservation>of())) {
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                    evaluate(new FlinkHaControl.Observations(retained, sessions(), false)).outcome());
        }
        var other = FlinkHaEvidenceTest.leadership("jobmanager-2", "jm-2", "other-session");
        var torn = new FlinkHaControl.Leadership(INITIAL.resourceManager(), other.dispatcher(), INITIAL.restServer());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluate(withLast(torn)).outcome());
    }

    @Test
    void samePhysicalLeaderWithANewSessionAndTransientObservedTransferBothFailTheControl() {
        for (var changed : List.of(FlinkHaEvidenceTest.leadership("jobmanager-1", "jm-1", "new-session"),
                FlinkHaEvidenceTest.leadership("jobmanager-2", "jm-2", "other-session"))) {
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluate(withLast(changed)).outcome());
            var samples = List.of(sample(1, FlinkHaControl.ObservationMoment.INITIAL, INITIAL),
                    sample(2, FlinkHaControl.ObservationMoment.ROUTING, changed),
                    sample(3, FlinkHaControl.ObservationMoment.PRE_FENCE, INITIAL));
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                    evaluate(new FlinkHaControl.Observations(samples, sessions(), false)).outcome());
        }
    }

    @Test
    void missingOrFailedRoutingSamplesCannotHideAnUnobservedControlInterval() {
        for (var error : List.of(Optional.<String>empty(), Optional.of("ZooKeeper disconnected"))) {
            var samples = List.of(sample(1, FlinkHaControl.ObservationMoment.INITIAL, INITIAL),
                    new FlinkHaControl.LeadershipObservation(2, 1, 20, 20,
                            FlinkHaControl.ObservationMoment.ROUTING, Optional.empty(), error),
                    sample(3, FlinkHaControl.ObservationMoment.PRE_FENCE, INITIAL));
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                    evaluate(new FlinkHaControl.Observations(samples, sessions(), false)).outcome());
        }
    }

    @Test
    void coalescedRoutingCountsRemainContiguousAndOverflowFailsClosed() {
        var samples = List.of(sample(1, FlinkHaControl.ObservationMoment.INITIAL, INITIAL),
                new FlinkHaControl.LeadershipObservation(2, 100, 20, 25,
                        FlinkHaControl.ObservationMoment.ROUTING, Optional.of(INITIAL), Optional.empty()),
                sample(102, FlinkHaControl.ObservationMoment.PRE_FENCE, INITIAL));
        assertEquals(FlinkHaEvidence.Outcome.CONFIRMED,
                evaluate(new FlinkHaControl.Observations(samples, sessions(), false)).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                evaluate(new FlinkHaControl.Observations(samples, sessions(), true)).outcome());
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, evaluate(new FlinkHaControl.Observations(List.of(
                samples.getFirst(), sample(3, FlinkHaControl.ObservationMoment.PRE_FENCE, INITIAL)), sessions(), false)).outcome());
    }

    @Test
    void everyProvisionedIncarnationRequiresItsOwnUnclampedNegotiation() {
        for (var invalid : List.of(
                List.of(sessions().getFirst()),
                List.of(sessions().get(0), sessions().get(0), sessions().get(2)),
                List.of(session(COMPONENTS.get(0), "wrong-runtime", 6_000, false), sessions().get(1), sessions().get(2)),
                List.of(session(COMPONENTS.get(0), "jm-1", 10_000, false), sessions().get(1), sessions().get(2)),
                List.of(session(COMPONENTS.get(0), "jm-1", 6_000, true), sessions().get(1), sessions().get(2)))) {
            assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED,
                    evaluate(new FlinkHaControl.Observations(history().leadership(), invalid, false)).outcome());
        }
        var replacement = FlinkRuntimeIdentityTest.component("jobmanager-1", "jm-1-new", FlinkRuntimeIdentityTest.IMAGE_ID);
        var withReplacement = new ArrayList<>(COMPONENTS);
        withReplacement.add(replacement);
        assertEquals(FlinkHaEvidence.Outcome.UNCONFIRMED, FlinkHaEvidence.evaluate(
                new FlinkHaEvidence.Expected(List.of(), true, false), Optional.of(new PhaseExecutionEvidence(List.of())),
                Optional.empty(), withReplacement, Optional.of(history())).outcome());
    }

    @Test
    void genericHealthyTokenControlConfirmsOneValidReceiptAmongTwoProvisionedTaskManagers() {
        var components = new ArrayList<>(COMPONENTS);
        components.add(FlinkRuntimeIdentityTest.component("taskmanager-2", "tm-2", FlinkRuntimeIdentityTest.IMAGE_ID));
        var observations = new FlinkHaControl.Observations(List.of(
                sample(1, FlinkHaControl.ObservationMoment.INITIAL, INITIAL),
                new FlinkHaControl.LeadershipObservation(2, 1, 100, 100,
                        FlinkHaControl.ObservationMoment.PRE_FENCE, Optional.of(INITIAL), Optional.empty())),
                components.stream().map(component -> session(component, component.runtimeId(), 6_000, false)).toList(), false);
        var origins = new SubjectClassOrigins(FlinkHaEvidence.TOKEN_CONTAINER_PATH,
                components.stream().map(component -> new SubjectClassOrigins.ProcessOrigin(
                        component.logicalName() + "#1", component.logicalName().startsWith("jobmanager-")
                        ? Map.of(FlinkHaEvidence.TOKEN_PROVIDER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH),
                                FlinkHaEvidence.TOKEN_RECEIVER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH))
                        : Map.of(FlinkHaEvidence.TOKEN_RECEIVER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH)))).toList(),
                Optional.empty());
        var events = List.of(
                healthyEvent(1, TokenServiceControl.Kind.PROVIDER_INITIALIZED, "jobmanager-1", 0, 0),
                healthyEvent(2, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "jobmanager-1", 0, 0),
                healthyEvent(3, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "taskmanager-1", 0, 0),
                healthyEvent(4, TokenServiceControl.Kind.RECEIVER_INITIALIZED, "taskmanager-2", 0, 0),
                healthyEvent(5, TokenServiceControl.Kind.REQUEST_STARTED, "jobmanager-1", 1, 0),
                healthyEvent(6, TokenServiceControl.Kind.ISSUED, "jobmanager-1", 1, 1),
                healthyEvent(7, TokenServiceControl.Kind.REQUEST_FINISHED, "jobmanager-1", 1, 0),
                healthyEvent(8, TokenServiceControl.Kind.RECEIVED, "taskmanager-1", 0, 1));
        for (boolean retainReceipt : List.of(true, false)) {
            var snapshot = new TokenServiceControl.Snapshot(
                    retainReceipt ? events : events.subList(0, events.size() - 1), false, false, 0, 1);
            var tokens = new FlinkHaEvidence.TokenEvidence(Optional.of("a".repeat(64)), Optional.of(origins),
                    Optional.of(snapshot), List.of());
            assertEquals(retainReceipt ? FlinkHaEvidence.Outcome.CONFIRMED : FlinkHaEvidence.Outcome.UNCONFIRMED,
                    FlinkHaEvidence.evaluate(new FlinkHaEvidence.Expected(List.of(), true, true, 2),
                            Optional.of(new PhaseExecutionEvidence(List.of())), Optional.of(tokens), components,
                            Optional.of(observations)).outcome());
        }
    }

    private static TokenServiceControl.Event healthyEvent(long sequence, TokenServiceControl.Kind kind,
            String process, long request, long token) {
        return new TokenServiceControl.Event(sequence, kind, process + "#1",
                process.startsWith("jobmanager-") ? "jobmanager" : "taskmanager", sequence * 10, sequence * 10_000_000,
                request, 0, TokenServiceControl.Mode.HEALTHY,
                token == 0 ? OptionalLong.empty() : OptionalLong.of(token), "");
    }

    private static FlinkHaControl.Observations withLast(FlinkHaControl.Leadership last) {
        return new FlinkHaControl.Observations(List.of(
                sample(1, FlinkHaControl.ObservationMoment.INITIAL, INITIAL),
                sample(2, FlinkHaControl.ObservationMoment.PRE_FENCE, last)), sessions(), false);
    }

    private static FlinkHaControl.Observations history() { return withLast(INITIAL); }

    private static List<FlinkHaControl.SessionEvidence> sessions() {
        return COMPONENTS.stream().map(component -> session(component, component.runtimeId(), 6_000, false)).toList();
    }

    private static FlinkHaControl.SessionEvidence session(FlinkComponentProvisioningEvidence component,
            String runtime, long negotiated, boolean overflow) {
        return new FlinkHaControl.SessionEvidence(component.logicalName(), component.role(), runtime,
                component.logicalName() + "#1", 6_000,
                List.of(new FlinkHaControl.NegotiatedSession("0x123", negotiated, "own Flink log")), overflow);
    }

    private static FlinkHaControl.LeadershipObservation sample(long sequence,
            FlinkHaControl.ObservationMoment moment, FlinkHaControl.Leadership leadership) {
        return new FlinkHaControl.LeadershipObservation(sequence, 1, sequence * 10, sequence * 10,
                moment, Optional.of(leadership), Optional.empty());
    }

    private static FlinkHaEvidence evaluate(FlinkHaControl.Observations observations) {
        return evaluate(Optional.of(observations));
    }

    private static FlinkHaEvidence evaluate(Optional<FlinkHaControl.Observations> observations) {
        return FlinkHaEvidence.evaluate(new FlinkHaEvidence.Expected(List.of(), true, false),
                Optional.of(new PhaseExecutionEvidence(List.of())), Optional.empty(), COMPONENTS, observations);
    }
}
