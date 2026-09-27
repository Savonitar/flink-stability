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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkTokenTraceTest {
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

    private static List<String> details(JsonNode root, JsonNode snapshot) {
        List<String> result = new ArrayList<>();
        for (JsonNode range : snapshot.path("eventRanges")) {
            for (int index = range.get(0).asInt(); index < range.get(1).asInt(); index++) {
                result.add(root.path("tokenEvents").get(index).path("detail").asText());
            }
        }
        assertEquals(snapshot.path("eventCount").asInt(), result.size());
        return result;
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
