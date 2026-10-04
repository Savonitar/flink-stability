package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointKillWindowTest {
    static ObjectNode overview() throws Exception {
        return (ObjectNode) new ObjectMapper().readTree("""
            {"latest":{"completed":{"id":1,"status":"COMPLETED","latest_ack_timestamp":100}},
             "history":[{"id":2,"status":"IN_PROGRESS","trigger_timestamp":120}],
             "observedJobVertices":[{"id":"sink","name":"Kafka Sink: Writer -> Kafka Sink: Committer","parallelism":1}],
             "observedCheckpointDetails":{"id":2,"trigger_timestamp":120,"status":"IN_PROGRESS","tasks":{"sink":{"id":2,"status":"IN_PROGRESS","num_subtasks":1,"num_acknowledged_subtasks":1,"latest_ack_timestamp":125}}}}
            """);
    }

    @Test void requiresSameCheckpointAcrossKill() throws Exception {
        var stats = overview();
        assertTrue(new CheckpointKillWindow(2, stats, stats, stats, null).confirmed());
        var after = overview();
        ((ObjectNode) after.path("history").get(0)).put("status", "COMPLETED");
        assertFalse(new CheckpointKillWindow(2, stats, stats, after, null).confirmed());
        assertFalse(new CheckpointKillWindow(2, stats, stats, null, "REST unavailable").confirmed());
        var pendingSink = overview();
        ((ObjectNode) pendingSink.at("/observedCheckpointDetails/tasks/sink")).put("num_acknowledged_subtasks", 0);
        assertFalse(new CheckpointKillWindow(2, stats, pendingSink, stats, null).confirmed());
    }

    @Test void rejectsOverlapGapsMissingTimesAndAmbiguousInflight() throws Exception {
        var stats = overview();
        ((ObjectNode) stats.at("/latest/completed")).put("latest_ack_timestamp", 120);
        assertEquals(-1, CheckpointKillWindow.eligibleCheckpoint(stats));
        stats = overview();
        ((ObjectNode) stats.path("history").get(0)).put("id", 3);
        assertEquals(-1, CheckpointKillWindow.eligibleCheckpoint(stats));
        stats = overview();
        ((ObjectNode) stats.at("/latest/completed")).remove("latest_ack_timestamp");
        assertEquals(-1, CheckpointKillWindow.eligibleCheckpoint(stats));
        stats = overview();
        ((com.fasterxml.jackson.databind.node.ArrayNode) stats.path("history")).add(stats.path("history").get(0).deepCopy());
        assertEquals(-1, CheckpointKillWindow.eligibleCheckpoint(stats));
    }
}
