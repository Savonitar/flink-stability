package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import java.io.IOException;
import java.time.Duration;
import java.util.function.LongSupplier;

/** Exact Flink 2.2 stop/savepoint receipts; no mutation is retried. */
public final class FlinkSavepoint {
    private FlinkSavepoint() {}
    public record Status(boolean completed, String location, String failure, String response) {}
    public record RestoreProof(String location, boolean savepoint, int parallelism, FlinkJobState state, String response) {}

    static String stop(FlinkJobHandle job, String trigger, String directory, Duration timeout,
            ObjectMapper mapper, FlinkCheckpointTrigger.Request request, LongSupplier clock) throws IOException {
        validate(job, trigger);
        var body = mapper.createObjectNode().put("triggerId", trigger).put("targetDirectory", directory)
                .put("drain", false).put("formatType", "CANONICAL");
        var response = request.execute("POST", "/jobs/" + job.jobId() + "/stop", mapper.writeValueAsBytes(body),
                MonotonicDeadline.start(timeout, clock));
        if (!trigger.equals(response.path("request-id").asText())) throw new IOException("Stop trigger acknowledgement mismatch: " + response);
        return trigger;
    }
    static Status status(FlinkJobHandle job, String trigger, Duration timeout,
            FlinkCheckpointTrigger.Request request, LongSupplier clock) throws IOException {
        validate(job, trigger);
        var response = request.execute("GET", "/jobs/" + job.jobId() + "/savepoints/" + trigger, null,
                MonotonicDeadline.start(timeout, clock));
        var operation = response.path("operation");
        String status = response.path("status").path("id").asText();
        if (status.equals("IN_PROGRESS") && (operation.isMissingNode() || operation.isNull()))
            return new Status(false, null, null, response.toString());
        if (!status.equals("COMPLETED")) throw new IOException("Invalid savepoint status: " + response);
        var location = operation.path("location"); var failure = operation.path("failure-cause");
        boolean hasLocation = location.isTextual() && !location.asText().isBlank();
        boolean hasFailure = !failure.isMissingNode() && !failure.isNull();
        if (hasLocation == hasFailure) throw new IOException("Ambiguous savepoint completion: " + response);
        return new Status(true, hasLocation ? location.asText() : null, hasFailure ? failure.toString() : null, response.toString());
    }
    static RestoreProof restored(FlinkJobHandle job, Duration timeout,
            FlinkCheckpointTrigger.Request request, LongSupplier clock) throws IOException {
        validate(job, "0".repeat(32));
        var deadline = MonotonicDeadline.start(timeout, clock);
        var details = request.execute("GET", "/jobs/" + job.jobId(), null, deadline);
        var checkpoints = request.execute("GET", "/jobs/" + job.jobId() + "/checkpoints", null, deadline);
        var restored = checkpoints.path("latest").path("restored");
        int parallelism = 0;
        for (var vertex : details.path("vertices")) {
            var value = vertex.path("parallelism");
            if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 1)
                throw new IOException("Invalid restored vertex parallelism: " + value);
            parallelism = Math.max(parallelism, value.intValue());
        }
        FlinkJobState state;
        try { state = FlinkJobState.valueOf(details.path("state").asText()); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid restored job state", invalid); }
        return new RestoreProof(restored.path("external_path").textValue(), restored.path("is_savepoint").asBoolean(),
                parallelism, state, "{\"job\":" + details + ",\"checkpoints\":" + checkpoints + "}");
    }
    private static void validate(FlinkJobHandle job, String trigger) {
        if (!job.jobId().matches("[0-9a-fA-F]{32}") || !trigger.matches("[0-9a-f]{32}"))
            throw new IllegalArgumentException("Savepoint operations require hexadecimal job and trigger IDs");
    }
}
