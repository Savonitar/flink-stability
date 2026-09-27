package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

/** Flink 2.2 asynchronous checkpoint REST contract; mutation retries are forbidden. */
public final class FlinkCheckpointTrigger {
    private FlinkCheckpointTrigger() {}

    public enum State { IN_PROGRESS, COMPLETED }

    public record Observation(State state, OptionalLong checkpointId, Optional<String> failure) {
        public Observation {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(checkpointId, "checkpointId");
            Objects.requireNonNull(failure, "failure");
        }
    }

    @FunctionalInterface
    interface Request {
        JsonNode execute(String method, String path, byte[] body, MonotonicDeadline deadline) throws IOException;
    }

    static String submit(FlinkJobHandle job, String triggerId, Duration timeout, ObjectMapper mapper,
                         Request request, LongSupplier clock) throws IOException {
        String path = path(job, triggerId);
        byte[] body = mapper.writeValueAsBytes(mapper.createObjectNode()
                .put("triggerId", triggerId).put("checkpointType", "DEFAULT"));
        JsonNode response = request.execute("POST", path, body, MonotonicDeadline.start(timeout, clock));
        JsonNode acknowledged = response.path("request-id");
        if (!acknowledged.isTextual() || !triggerId.equals(acknowledged.textValue())) {
            throw new IOException("Checkpoint submission did not acknowledge the requested trigger ID " + triggerId);
        }
        return triggerId;
    }

    static Observation status(FlinkJobHandle job, String triggerId, Duration timeout,
                              Request request, LongSupplier clock) throws IOException {
        JsonNode response = request.execute("GET", path(job, triggerId) + "/" + triggerId,
                null, MonotonicDeadline.start(timeout, clock));
        String state = response.path("status").path("id").asText();
        JsonNode operation = response.path("operation");
        if ("IN_PROGRESS".equals(state) && (operation.isMissingNode() || operation.isNull())) {
            return new Observation(State.IN_PROGRESS, OptionalLong.empty(), Optional.empty());
        }
        if (!"COMPLETED".equals(state)) throw new IOException("Invalid checkpoint trigger status: " + response);
        JsonNode id = operation.path("checkpointId");
        JsonNode failure = operation.path("failureCause");
        boolean hasFailure = !failure.isMissingNode() && !failure.isNull();
        boolean hasId = id.isIntegralNumber() && id.canConvertToLong() && id.longValue() > 0;
        if (hasId == hasFailure) throw new IOException("Invalid completed checkpoint operation: " + response);
        return new Observation(State.COMPLETED, hasId ? OptionalLong.of(id.longValue()) : OptionalLong.empty(),
                hasFailure ? Optional.of(failure.toString()) : Optional.empty());
    }

    private static String path(FlinkJobHandle job, String triggerId) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(triggerId, "triggerId");
        if (!triggerId.matches("[0-9a-f]{32}") || !job.jobId().matches("[0-9a-fA-F]{32}")) {
            throw new IllegalArgumentException("Checkpoint operations require hexadecimal job and trigger IDs");
        }
        return "/jobs/" + job.jobId() + "/checkpoints";
    }
}
