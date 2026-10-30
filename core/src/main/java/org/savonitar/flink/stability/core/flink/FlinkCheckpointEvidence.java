package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.RawEvidenceRef;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

/** Bounded, identity-preserving observations of the released Flink checkpoint REST contract. */
public final class FlinkCheckpointEvidence {
    static final int MAX_HISTORY_ENTRIES = 1024;
    static final int MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
    private static final int MAX_FAILURE_CHARS = 32 * 1024;
    private static final int MAX_PATH_CHARS = 4096;

    private FlinkCheckpointEvidence() {}

    public enum Status { IN_PROGRESS, COMPLETED, FAILED }

    public record Counts(long total, long inProgress, long completed, long failed, long restored) {}

    public record Checkpoint(
            long id, Status status, boolean savepoint, String checkpointType,
            long triggerTimestamp, long latestAckTimestamp, long checkpointedSize, long stateSize,
            int numSubtasks, int numAcknowledgedSubtasks, OptionalLong failureTimestamp,
            Optional<String> failureMessage, Optional<String> externalPath, Optional<Boolean> discarded,
            List<RawEvidenceRef> rawEvidence) {
        public Checkpoint {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(checkpointType, "checkpointType");
            Objects.requireNonNull(failureTimestamp, "failureTimestamp");
            Objects.requireNonNull(failureMessage, "failureMessage");
            Objects.requireNonNull(externalPath, "externalPath");
            Objects.requireNonNull(discarded, "discarded");
            rawEvidence = List.copyOf(rawEvidence);
        }
        public Checkpoint(long id, Status status, boolean savepoint, String checkpointType,
                long triggerTimestamp, long latestAckTimestamp, long checkpointedSize, long stateSize,
                int numSubtasks, int numAcknowledgedSubtasks, OptionalLong failureTimestamp,
                Optional<String> failureMessage, Optional<String> externalPath, Optional<Boolean> discarded) {
            this(id, status, savepoint, checkpointType, triggerTimestamp, latestAckTimestamp, checkpointedSize,
                    stateSize, numSubtasks, numAcknowledgedSubtasks, failureTimestamp, failureMessage,
                    externalPath, discarded, List.of());
        }
        public Checkpoint withRawEvidence(List<RawEvidenceRef> raw) {
            return new Checkpoint(id, status, savepoint, checkpointType, triggerTimestamp, latestAckTimestamp,
                    checkpointedSize, stateSize, numSubtasks, numAcknowledgedSubtasks, failureTimestamp,
                    failureMessage, externalPath, discarded, raw);
        }
    }

    public record Restored(long id, long restoreTimestamp, boolean savepoint, Optional<String> externalPath) {
        public Restored { Objects.requireNonNull(externalPath, "externalPath"); }
    }

    /** History is a retained window, never a claim that all checkpoints remain in the REST cache. */
    public record History(
            String jobId, Counts counts, List<Checkpoint> retained,
            Optional<Checkpoint> latestCompleted, Optional<Checkpoint> latestFailed,
            Optional<Checkpoint> latestSavepoint, Optional<Restored> latestRestored, List<RawEvidenceRef> rawEvidence) {
        public History {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(counts, "counts");
            retained = List.copyOf(retained);
            Objects.requireNonNull(latestCompleted, "latestCompleted");
            Objects.requireNonNull(latestFailed, "latestFailed");
            Objects.requireNonNull(latestSavepoint, "latestSavepoint");
            Objects.requireNonNull(latestRestored, "latestRestored");
            rawEvidence = List.copyOf(rawEvidence);
        }

        public History(String jobId, Counts counts, List<Checkpoint> retained,
                Optional<Checkpoint> latestCompleted, Optional<Checkpoint> latestFailed,
                Optional<Checkpoint> latestSavepoint, Optional<Restored> latestRestored) {
            this(jobId, counts, retained, latestCompleted, latestFailed, latestSavepoint, latestRestored, List.of());
        }
        public History withRawEvidence(List<RawEvidenceRef> raw) {
            return new History(jobId, counts, retained, latestCompleted, latestFailed, latestSavepoint, latestRestored, raw);
        }

        public long highestObservedId() {
            long highest = retained.stream().mapToLong(Checkpoint::id).max().orElse(0);
            for (Optional<Checkpoint> latest : List.of(latestCompleted, latestFailed, latestSavepoint)) {
                if (latest.isPresent()) highest = Math.max(highest, latest.orElseThrow().id());
            }
            return Math.max(highest, latestRestored.map(Restored::id).orElse(0L));
        }

        public List<Checkpoint> inProgress() {
            return retained.stream().filter(checkpoint -> checkpoint.status() == Status.IN_PROGRESS).toList();
        }

        /**
         * Failed triggers without an observed checkpoint identity. A nonzero remainder alone
         * does not prove that every failure was ID-less: the retained window may omit older
         * checkpoints. Baseline acceptance additionally requires no retained/latest failure.
         */
        public long idlessFailures() {
            var identified = new java.util.HashSet<Long>();
            retained.stream().filter(checkpoint -> checkpoint.status() == Status.FAILED)
                    .map(Checkpoint::id).forEach(identified::add);
            latestFailed.map(Checkpoint::id).ifPresent(identified::add);
            if (counts.failed() < identified.size()) {
                throw new IllegalArgumentException("Identified failed checkpoints exceed the failed count");
            }
            return counts.failed() - identified.size();
        }

        public Optional<Checkpoint> find(long id) {
            Optional<Checkpoint> entry = retained.stream().filter(checkpoint -> checkpoint.id() == id).findFirst();
            if (entry.isPresent()) return entry;
            return List.of(latestCompleted, latestFailed, latestSavepoint).stream()
                    .flatMap(Optional::stream).filter(checkpoint -> checkpoint.id() == id).findFirst();
        }
    }

    /** Flink 2.2 does not expose the data-inline threshold in this REST response. */
    public record Configuration(
            String jobId, String mode, long intervalMillis, long timeoutMillis, long minPauseMillis,
            int maxConcurrent, int tolerableFailedCheckpoints, String stateBackend, String checkpointStorage,
            boolean unalignedCheckpoints, boolean stateChangelogEnabled,
            boolean externalized, boolean deleteOnCancellation, List<RawEvidenceRef> rawEvidence) {
        public Configuration { rawEvidence = List.copyOf(rawEvidence); }
        public Configuration(String jobId, String mode, long intervalMillis, long timeoutMillis, long minPauseMillis,
                int maxConcurrent, int tolerableFailedCheckpoints, String stateBackend, String checkpointStorage,
                boolean unalignedCheckpoints, boolean stateChangelogEnabled, boolean externalized,
                boolean deleteOnCancellation) {
            this(jobId, mode, intervalMillis, timeoutMillis, minPauseMillis, maxConcurrent, tolerableFailedCheckpoints,
                    stateBackend, checkpointStorage, unalignedCheckpoints, stateChangelogEnabled, externalized,
                    deleteOnCancellation, List.of());
        }
        public Configuration withRawEvidence(List<RawEvidenceRef> raw) {
            return new Configuration(jobId, mode, intervalMillis, timeoutMillis, minPauseMillis, maxConcurrent,
                    tolerableFailedCheckpoints, stateBackend, checkpointStorage, unalignedCheckpoints,
                    stateChangelogEnabled, externalized, deleteOnCancellation, raw);
        }
    }

    @FunctionalInterface
    interface Request {
        JsonNode execute(String endpoint, MonotonicDeadline deadline) throws IOException;
    }

    static History history(FlinkJobHandle job, Duration timeout, Request request, LongSupplier clock)
            throws IOException {
        String path = path(job);
        return parseHistory(job.jobId(), request.execute(path, MonotonicDeadline.start(timeout, clock)));
    }

    static Checkpoint details(FlinkJobHandle job, long checkpointId, Duration timeout,
            Request request, LongSupplier clock) throws IOException {
        String path = path(job);
        if (checkpointId <= 0) throw new IllegalArgumentException("checkpointId must be positive");
        Checkpoint result = parseCheckpoint(request.execute(path + "/details/" + checkpointId,
                MonotonicDeadline.start(timeout, clock)));
        if (result.id() != checkpointId) throw invalid("Checkpoint detail identity differs from request");
        return result;
    }

    static Configuration configuration(FlinkJobHandle job, Duration timeout,
            Request request, LongSupplier clock) throws IOException {
        String path = path(job);
        return parseConfiguration(job.jobId(), request.execute(path + "/config", MonotonicDeadline.start(timeout, clock)));
    }

    static History parseHistory(String jobId, JsonNode response) throws IOException {
        object(response, "checkpoint history");
        JsonNode countNode = object(response.path("counts"), "checkpoint counts");
        Counts counts = new Counts(number(countNode, "total", 0), number(countNode, "in_progress", 0),
                number(countNode, "completed", 0), number(countNode, "failed", 0), number(countNode, "restored", 0));
        try {
            if (counts.total() != Math.addExact(Math.addExact(counts.inProgress(), counts.completed()), counts.failed())) {
                throw invalid("Checkpoint counts are inconsistent");
            }
        } catch (ArithmeticException overflow) {
            throw invalid("Checkpoint counts overflow");
        }
        JsonNode historyNode = response.path("history");
        if (!historyNode.isArray() || historyNode.size() > MAX_HISTORY_ENTRIES) {
            throw invalid("Checkpoint history must be a bounded array");
        }
        List<Checkpoint> retained = new ArrayList<>();
        Map<Long, Checkpoint> byId = new HashMap<>();
        for (JsonNode entry : historyNode) {
            Checkpoint checkpoint = parseCheckpoint(entry);
            if (byId.putIfAbsent(checkpoint.id(), checkpoint) != null) throw invalid("Duplicate checkpoint history ID");
            retained.add(checkpoint);
        }
        if (retained.size() > counts.total()
                || retained.stream().filter(entry -> entry.status() == Status.IN_PROGRESS).count() > counts.inProgress()
                || retained.stream().filter(entry -> entry.status() == Status.COMPLETED).count() > counts.completed()
                || retained.stream().filter(entry -> entry.status() == Status.FAILED).count() > counts.failed()) {
            throw invalid("Checkpoint history exceeds its counts");
        }
        JsonNode latest = object(response.path("latest"), "latest checkpoints");
        Optional<Checkpoint> completed = latest(latest, "completed", Status.COMPLETED, false, byId);
        Optional<Checkpoint> failed = latest(latest, "failed", Status.FAILED, null, byId);
        Optional<Checkpoint> savepoint = latest(latest, "savepoint", Status.COMPLETED, true, byId);
        Optional<Restored> restored = Optional.empty();
        if (present(latest.path("restored"))) {
            JsonNode node = object(latest.path("restored"), "restored checkpoint");
            restored = Optional.of(new Restored(number(node, "id", 1), number(node, "restore_timestamp", 0),
                    bool(node, "is_savepoint"), optionalText(node, "external_path", MAX_PATH_CHARS)));
        }
        if ((completed.isPresent() || savepoint.isPresent()) && counts.completed() == 0
                || failed.isPresent() && counts.failed() == 0 || restored.isPresent() && counts.restored() == 0) {
            throw invalid("Latest checkpoint contradicts counts");
        }
        History result = new History(jobId, counts, retained, completed, failed, savepoint, restored);
        try { result.idlessFailures(); }
        catch (IllegalArgumentException inconsistent) { throw invalid(inconsistent.getMessage()); }
        return result;
    }

    private static Optional<Checkpoint> latest(JsonNode latest, String field, Status status, Boolean savepoint,
            Map<Long, Checkpoint> retained) throws IOException {
        if (!present(latest.path(field))) return Optional.empty();
        Checkpoint result = parseCheckpoint(latest.path(field));
        if (result.status() != status || savepoint != null && result.savepoint() != savepoint) {
            throw invalid("Invalid latest " + field + " checkpoint status/type");
        }
        Checkpoint duplicate = retained.get(result.id());
        if (duplicate != null && !duplicate.equals(result)) throw invalid("Conflicting latest/history checkpoint identity");
        for (Checkpoint entry : retained.values()) {
            if (entry.status() == status && (savepoint == null || entry.savepoint() == savepoint)
                    && entry.id() > result.id()) throw invalid("Latest checkpoint is older than retained history");
        }
        return Optional.of(result);
    }

    static Checkpoint parseCheckpoint(JsonNode node) throws IOException {
        object(node, "checkpoint");
        long id = number(node, "id", 1);
        Status status;
        try { status = Status.valueOf(text(node, "status", 32)); }
        catch (IllegalArgumentException invalid) { throw invalid("Unknown checkpoint status"); }
        boolean savepoint = bool(node, "is_savepoint");
        String type = text(node, "checkpoint_type", 32);
        if (!List.of("CHECKPOINT", "UNALIGNED_CHECKPOINT", "SAVEPOINT", "SYNC_SAVEPOINT").contains(type)
                || savepoint != (type.equals("SAVEPOINT") || type.equals("SYNC_SAVEPOINT"))) {
            throw invalid("Unknown or contradictory checkpoint type");
        }
        long triggerTimestamp = number(node, "trigger_timestamp", 0);
        long latestAckTimestamp = number(node, "latest_ack_timestamp", -1);
        long checkpointedSize = number(node, "checkpointed_size", 0);
        long stateSize = number(node, "state_size", 0);
        int numSubtasks = integer(node, "num_subtasks", 1);
        int acknowledged = integer(node, "num_acknowledged_subtasks", 0);
        if (acknowledged > numSubtasks || status == Status.COMPLETED && acknowledged != numSubtasks) {
            throw invalid("Inconsistent checkpoint subtask acknowledgments");
        }
        OptionalLong failureTimestamp = OptionalLong.empty();
        Optional<String> failureMessage = optionalText(node, "failure_message", MAX_FAILURE_CHARS);
        Optional<String> externalPath = optionalText(node, "external_path", MAX_PATH_CHARS);
        Optional<Boolean> discarded = Optional.empty();
        if (status == Status.FAILED) {
            failureTimestamp = OptionalLong.of(number(node, "failure_timestamp", 0));
            if (failureTimestamp.orElseThrow() < triggerTimestamp) throw invalid("Checkpoint failed before its trigger");
        } else if (present(node.path("failure_timestamp")) || failureMessage.isPresent()) {
            throw invalid("Non-failed checkpoint contains failure evidence");
        }
        if (status == Status.COMPLETED) discarded = Optional.of(bool(node, "discarded"));
        else if (present(node.path("discarded")) || externalPath.isPresent()) {
            throw invalid("Incomplete checkpoint contains completed storage evidence");
        }
        return new Checkpoint(id, status, savepoint, type, triggerTimestamp, latestAckTimestamp,
                checkpointedSize, stateSize, numSubtasks, acknowledged, failureTimestamp,
                failureMessage, externalPath, discarded);
    }

    static Configuration parseConfiguration(String jobId, JsonNode node) throws IOException {
        object(node, "checkpoint configuration");
        String mode = text(node, "mode", 32);
        if (!mode.equals("exactly_once") && !mode.equals("at_least_once")) throw invalid("Unknown checkpoint processing mode");
        JsonNode externalization = object(node.path("externalization"), "checkpoint externalization");
        return new Configuration(jobId, mode, number(node, "interval", 1), number(node, "timeout", 1),
                number(node, "min_pause", 0), integer(node, "max_concurrent", 1),
                integer(node, "tolerable_failed_checkpoints", 0), text(node, "state_backend", 256),
                text(node, "checkpoint_storage", 256), bool(node, "unaligned_checkpoints"),
                bool(node, "state_changelog_enabled"), bool(externalization, "enabled"),
                bool(externalization, "delete_on_cancellation"));
    }

    private static String path(FlinkJobHandle job) {
        Objects.requireNonNull(job, "job");
        if (!job.jobId().matches("[0-9a-fA-F]{32}")) throw new IllegalArgumentException("Invalid Flink job ID");
        return "/jobs/" + job.jobId() + "/checkpoints";
    }

    private static JsonNode object(JsonNode node, String label) throws IOException {
        if (node == null || !node.isObject()) throw invalid(label + " must be an object");
        return node;
    }

    private static boolean present(JsonNode node) { return !node.isMissingNode() && !node.isNull(); }

    private static long number(JsonNode node, String field, long minimum) throws IOException {
        JsonNode value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < minimum) {
            throw invalid("Invalid checkpoint field: " + field);
        }
        return value.longValue();
    }

    private static int integer(JsonNode node, String field, int minimum) throws IOException {
        long value = number(node, field, minimum);
        if (value > Integer.MAX_VALUE) throw invalid("Checkpoint field exceeds integer range: " + field);
        return (int) value;
    }

    private static boolean bool(JsonNode node, String field) throws IOException {
        if (!node.path(field).isBoolean()) throw invalid("Invalid checkpoint boolean: " + field);
        return node.path(field).booleanValue();
    }

    private static String text(JsonNode node, String field, int maximum) throws IOException {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.textValue().isBlank() || value.textValue().length() > maximum) {
            throw invalid("Invalid or excessive checkpoint text: " + field);
        }
        return value.textValue();
    }

    private static Optional<String> optionalText(JsonNode node, String field, int maximum) throws IOException {
        return present(node.path(field)) ? Optional.of(text(node, field, maximum)) : Optional.empty();
    }

    private static IOException invalid(String message) { return new IOException(message); }
}
