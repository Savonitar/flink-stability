package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.FlinkHaEvidence;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkTokenTraceTest {
    private static final String PROVIDER = "00000000-0000-0000-0000-000000000001";
    private static final String PARTICIPANT = "00000000-0000-0000-0000-000000000002";
    private static final String JOB = "a".repeat(32);

    @Test
    void oneHundredFaultsShareTenThousandEventsWithoutRepeatingFullTraces() throws Exception {
        List<TokenServiceControl.Event> events = IntStream.rangeClosed(1, 10_000)
                .mapToObj(index -> event(index, "request"))
                .toList();
        var snapshot = snapshot(events);
        ObjectNode result = render(IntStream.range(0, 100)
                .mapToObj(index -> fault(index, snapshot, snapshot, snapshot)).toList(), snapshot);

        assertEquals(10_000, result.path("tokenEvents").size());
        assertEquals(100, result.path("leaderFaults").size());
        for (JsonNode fault : result.path("leaderFaults")) {
            for (String name : List.of("tokensBefore", "tokensDuring", "tokensAfter")) {
                assertEquals(10_000, fault.path(name).path("eventCount").asInt());
                assertEquals("[[0,10000]]", fault.path(name).path("eventRanges").toString());
            }
        }
        assertEquals("[[0,10000]]", result.at("/tokens/snapshot/eventRanges").toString());
        assertTrue(new ObjectMapper().writeValueAsBytes(result).length < 4_000_000,
                "The maximum healthy prefix trace must remain proportional to unique events");
    }

    @Test
    void conflictingSequencesReorderingAndDuplicateEntriesSurviveReconstruction() {
        var first = event(1, "first observation");
        var second = event(2, "second observation");
        var conflict = event(1, "contradictory observation");
        var before = snapshot(List.of(first, second));
        var during = snapshot(List.of(first, conflict, second, first));
        var after = new TokenServiceControl.Snapshot(List.of(second, conflict), true, true, 1, 3);
        ObjectNode result = render(List.of(fault(0, before, during, after)), after);

        assertEquals(3, result.path("tokenEvents").size());
        assertEquals(List.of("first observation", "second observation"),
                details(result, result.at("/leaderFaults/0/tokensBefore")));
        assertEquals(List.of("first observation", "contradictory observation", "second observation",
                        "first observation"), details(result, result.at("/leaderFaults/0/tokensDuring")));
        assertEquals(List.of("second observation", "contradictory observation"),
                details(result, result.at("/tokens/snapshot")));
        assertTrue(result.at("/tokens/snapshot/overflow").asBoolean());
        assertTrue(result.at("/tokens/snapshot/saturated").asBoolean());
        assertEquals(1, result.at("/tokens/snapshot/activeRequests").asInt());
        assertEquals(3, result.at("/tokens/snapshot/maxConcurrentRequests").asInt());
    }

    @Test
    void preservesRegistrationJournalAndIndependentParticipantWithoutNumericTruncation() throws Exception {
        long generation = 4_294_967_297L;
        long acknowledged = 4_294_967_298L;
        var registration = new TokenServiceControl.RegistrationSnapshot(PROVIDER, "JOB", generation,
                JOB, "job\n\u0430", true, acknowledged, List.of(new TokenServiceControl.Lifecycle(
                        acknowledged + 1, "REGISTER", generation, JOB, "job\n\u0430")));
        var event = context(Optional.of(registration), Optional.of(PARTICIPANT));
        ObjectNode result = render(List.of(), snapshot(List.of(event)));

        JsonNode encoded = result.path("tokenEvents").get(0);
        assertEquals(PARTICIPANT, encoded.path("participantInstance").asText());
        JsonNode expected = new ObjectMapper().readTree("""
                {
                  "providerInstance": "00000000-0000-0000-0000-000000000001",
                  "scope": "JOB",
                  "generation": 4294967297,
                  "jobId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                  "jobAlias": "job\\n\u0430",
                  "coverageInvalid": true,
                  "acknowledgedSequence": 4294967298,
                  "journal": [{
                    "sequence": 4294967299,
                    "kind": "REGISTER",
                    "generation": 4294967297,
                    "jobId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "jobAlias": "job\\n\u0430"
                  }]
                }
                """);
        assertEquals(expected, encoded.path("registration"));
        assertEquals(expected, new ObjectMapper().readTree(encoded.toString()).path("registration"));
    }

    @Test
    void everyContextFieldKeepsConflictingEventsDistinctAcrossRangeEncoding() {
        var journal = new TokenServiceControl.Lifecycle(1, "REGISTER", 2, JOB, "job");
        var base = registration(PROVIDER, "JOB", 2, JOB, "job", false, 0, List.of());
        List<TokenServiceControl.Event> variants = new ArrayList<>();
        variants.add(event(1, "request"));
        variants.add(context(Optional.empty(), Optional.of(PARTICIPANT)));
        variants.add(context(Optional.of(base), Optional.empty()));
        variants.add(context(base));
        variants.add(context(registration(PARTICIPANT, "JOB", 2, JOB, "job", false, 0, List.of())));
        variants.add(context(registration(PROVIDER, "BOOTSTRAP", 2, "-", "-", false, 0, List.of())));
        variants.add(context(registration(PROVIDER, "JOB", 3, JOB, "job", false, 0, List.of())));
        variants.add(context(registration(PROVIDER, "JOB", 2, "b".repeat(32), "job", false, 0, List.of())));
        variants.add(context(registration(PROVIDER, "JOB", 2, JOB, "other-alias", false, 0, List.of())));
        variants.add(context(registration(PROVIDER, "JOB", 2, JOB, "job", true, 0, List.of())));
        variants.add(context(registration(PROVIDER, "JOB", 2, JOB, "job", false, 1, List.of())));
        variants.add(context(registration(PROVIDER, "JOB", 2, JOB, "job", false, 0, List.of(journal))));
        for (var record : List.of(
                new TokenServiceControl.Lifecycle(1, "UNREGISTER", 2, JOB, "job"),
                new TokenServiceControl.Lifecycle(1, "REGISTER", 3, JOB, "job"),
                new TokenServiceControl.Lifecycle(1, "REGISTER", 2, "b".repeat(32), "job"),
                new TokenServiceControl.Lifecycle(1, "REGISTER", 2, JOB, "other-alias"))) {
            variants.add(context(registration(PROVIDER, "JOB", 2, JOB, "job", false, 0, List.of(record))));
        }
        variants.add(context(registration(PROVIDER, "JOB", 2, JOB, "job", false, 1,
                List.of(new TokenServiceControl.Lifecycle(2, "REGISTER", 2, JOB, "job")))));
        variants.add(context(registration(PROVIDER, "JOB", 2, JOB, "job", false, 0,
                List.of(journal, new TokenServiceControl.Lifecycle(2, "INVALID", 2, JOB, "job")))));
        variants.add(context(Optional.of(base), Optional.of(PROVIDER)));
        List<TokenServiceControl.Event> reordered = new ArrayList<>(variants);
        Collections.reverse(reordered);
        reordered.add(variants.get(3));
        reordered.add(variants.get(3));
        var before = snapshot(variants);
        var during = snapshot(reordered);
        ObjectNode result = render(List.of(fault(0, before, during, before)), during);

        assertEquals((long) variants.size(), variants.stream().distinct().count());
        assertEquals(variants.size(), result.path("tokenEvents").size());
        List<JsonNode> encoded = entries(result, result.at("/leaderFaults/0/tokensBefore"));
        assertEquals((long) variants.size(), encoded.stream().distinct().count(),
                "Distinct raw contexts must not serialize as indistinguishable events");
        assertFalse(encoded.get(0).has("registration"));
        assertFalse(encoded.get(0).has("participantInstance"));
        assertFalse(encoded.get(1).has("registration"));
        assertFalse(encoded.get(2).has("participantInstance"));
        List<JsonNode> expectedOrder = new ArrayList<>(encoded);
        Collections.reverse(expectedOrder);
        expectedOrder.add(encoded.get(3));
        expectedOrder.add(encoded.get(3));
        assertEquals(expectedOrder, entries(result, result.at("/leaderFaults/0/tokensDuring")));
        assertEquals(expectedOrder, entries(result, result.at("/tokens/snapshot")));
    }

    private static TokenServiceControl.RegistrationSnapshot registration(String provider, String scope,
            long generation, String job, String alias, boolean invalid, long acknowledged,
            List<TokenServiceControl.Lifecycle> journal) {
        return new TokenServiceControl.RegistrationSnapshot(provider, scope, generation, job,
                alias, invalid, acknowledged, journal);
    }

    private static TokenServiceControl.Event context(TokenServiceControl.RegistrationSnapshot registration) {
        return context(Optional.of(registration), Optional.of(PARTICIPANT));
    }

    private static TokenServiceControl.Event context(Optional<TokenServiceControl.RegistrationSnapshot> registration,
            Optional<String> participant) {
        return new TokenServiceControl.Event(1, TokenServiceControl.Kind.REQUEST_STARTED,
                "jobmanager-1#1", "provider", 1, 1, 1, 1,
                TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), "request", registration, participant);
    }

    private static List<JsonNode> entries(JsonNode root, JsonNode snapshot) {
        List<JsonNode> result = new ArrayList<>();
        for (JsonNode range : snapshot.path("eventRanges")) {
            for (int index = range.get(0).asInt(); index < range.get(1).asInt(); index++) {
                result.add(root.path("tokenEvents").get(index));
            }
        }
        assertEquals(snapshot.path("eventCount").asInt(), result.size());
        return result;
    }

    private static List<String> details(JsonNode root, JsonNode snapshot) {
        return entries(root, snapshot).stream().map(event -> event.path("detail").asText()).toList();
    }

    private static TokenServiceControl.Event event(int sequence, String detail) {
        return new TokenServiceControl.Event(sequence, TokenServiceControl.Kind.REQUEST_STARTED,
                "jobmanager-1#1", "provider", sequence, sequence, sequence, 1,
                TokenServiceControl.Mode.HEALTHY, OptionalLong.empty(), detail);
    }

    private static TokenServiceControl.Snapshot snapshot(List<TokenServiceControl.Event> events) {
        return new TokenServiceControl.Snapshot(events, false, false, 0, 1);
    }

    private static PhaseExecutionEvidence.LeaderFault fault(int index,
            TokenServiceControl.Snapshot before, TokenServiceControl.Snapshot during,
            TokenServiceControl.Snapshot after) {
        var request = new FlinkHaControl.LeaderFaultRequest(FlinkHaControl.Mode.ISOLATE_ZOOKEEPER,
                Duration.ofSeconds(15), Duration.ofMinutes(2), Optional.empty());
        var raw = new FlinkHaControl.LeaderFaultEvidence(request, Optional.empty(), Optional.empty(),
                Optional.empty(), false, false, Optional.empty(), Optional.empty(),
                0, 0, 0, 0, false, Optional.of(before), Optional.of(during), Optional.of(after), List.of());
        var unavailable = new FlinkJobObservation.Attempt(Optional.empty(), Optional.of("unobserved"));
        return new PhaseExecutionEvidence.LeaderFault("$/phases/0/steps/" + index, List.of(), "job",
                unavailable, unavailable, raw, List.of());
    }

    private static ObjectNode render(List<PhaseExecutionEvidence.LeaderFault> faults,
            TokenServiceControl.Snapshot snapshot) {
        var evidence = new FlinkHaEvidence(new FlinkHaEvidence.Expected(List.of(), true, true),
                Optional.of(new FlinkHaEvidence.TokenEvidence(Optional.empty(), Optional.empty(),
                        Optional.of(snapshot), List.of())), Optional.empty(), FlinkHaEvidence.Outcome.UNCONFIRMED,
                "Raw snapshots retained independently of the verdict");
        ObjectNode result = new ObjectMapper().createObjectNode();
        FlinkHaEvidenceRenderer.render(result, evidence, faults);
        return result;
    }
}
