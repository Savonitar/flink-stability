package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.savonitar.flink.stability.runtime.api.KafkaProducerSnapshot;

/** REST observations around the physical kill. Transaction/sink proof additionally needs logs. */
public record CheckpointKillWindow(long checkpointId, JsonNode armed, JsonNode beforeKill,
                                   JsonNode afterKill, String observationFailure,
        List<KafkaProducerSnapshot> brokerBeforeKill,
        List<KafkaProducerSnapshot> brokerAfterKill) {
    public CheckpointKillWindow(long id, JsonNode armed, JsonNode before, JsonNode after, String failure) {
        this(id, armed, before, after, failure, List.of(), List.of());
    }
    public CheckpointKillWindow {
        brokerBeforeKill = List.copyOf(brokerBeforeKill);
        brokerAfterKill = List.copyOf(brokerAfterKill);
        armed = armed == null ? null : armed.deepCopy();
        beforeKill = beforeKill == null ? null : beforeKill.deepCopy();
        afterKill = afterKill == null ? null : afterKill.deepCopy();
    }

    /** Requires an immediately preceding completed checkpoint before the selected trigger. */
    public static long eligibleCheckpoint(JsonNode overview) {
        if (overview == null || !overview.path("history").isArray()) return -1;
        JsonNode completed = overview.path("latest").path("completed");
        if (!completed.path("id").isIntegralNumber() || !completed.path("latest_ack_timestamp").isIntegralNumber()
                || !"COMPLETED".equals(completed.path("status").asText())) return -1;
        long previous = completed.path("id").asLong();
        long found = -1;
        for (JsonNode candidate : overview.path("history")) {
            if (!"IN_PROGRESS".equals(candidate.path("status").asText())) continue;
            if (found != -1 || !candidate.path("id").isIntegralNumber()
                    || !candidate.path("trigger_timestamp").isIntegralNumber()) return -1;
            long id = candidate.path("id").asLong();
            if (previous < 1 || id != previous + 1
                    || completed.path("latest_ack_timestamp").asLong() >= candidate.path("trigger_timestamp").asLong()) return -1;
            found = id;
        }
        return found;
    }

    public boolean confirmed() {
        return observationFailure == null && checkpointId > 1
                && eligibleCheckpoint(armed) == checkpointId
                && eligibleCheckpoint(beforeKill) == checkpointId
                && eligibleCheckpoint(afterKill) == checkpointId
                && sinkAcknowledged(beforeKill);
    }

    /** The source's async delay must not be mistaken for an unsnapshotted sink. */
    public static boolean sinkAcknowledged(JsonNode overview) {
        if (overview == null) return false;
        JsonNode details = overview.path("observedCheckpointDetails");
        if (!"IN_PROGRESS".equals(details.path("status").asText())) return false;
        int sinks = 0;
        for (JsonNode vertex : overview.path("observedJobVertices")) {
            if (!vertex.path("name").asText().contains("Kafka Sink: Writer")) continue;
            sinks++;
            JsonNode task = details.path("tasks").path(vertex.path("id").asText());
            // Task summary status mirrors the global checkpoint status in Flink 2.2.
            // Acknowledgement count and time prove this vertex finished its snapshot.
            if (!vertex.path("parallelism").isIntegralNumber()
                    || task.path("id").asLong(-1) != details.path("id").asLong(-2)
                    || !task.path("latest_ack_timestamp").isIntegralNumber()
                    || task.path("latest_ack_timestamp").asLong() < details.path("trigger_timestamp").asLong(Long.MAX_VALUE)
                    || task.path("num_subtasks").asInt(-1) != vertex.path("parallelism").asInt()
                    || vertex.path("parallelism").asInt() < 1
                    || task.path("num_acknowledged_subtasks").asInt(-1) != vertex.path("parallelism").asInt()) return false;
        }
        return sinks == 1;
    }
}
