package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** Bounded, process-bound state-progress receipts and fail-closed recovery-window proof. */
public final class StateRecoveryEvidence {
    public static final String OPERATOR_UID = "flink-stability-keyed-prefix-v1";
    public static final int MAX_MARKERS = 4096;
    public static final long MAX_LOG_BYTES = 32L * 1024 * 1024;
    private static final int MAX_MARKER_LENGTH = 2048;
    private static final int MAX_DIAGNOSTICS = 100;
    private static final String PREFIX = "STATE_PREFIX_";
    private static final Set<String> BASE_FIELDS = Set.of("version", "jobId", "operatorUid", "attempt",
            "subtask", "checkpoint", "restoredCheckpoint");
    private static final Set<String> PROGRESS_FIELDS = Set.of("keyCount", "observedKeys",
            "minimumObservedCount", "processedThisAttempt", "maximumInputId");
    private static final Set<String> KEY_FIELDS = Set.of("keyCount", "key", "inputId",
            "previousCount", "previousSum", "newCount", "newSum");

    private StateRecoveryEvidence() {}

    public enum Event { START, SNAPSHOT, COMPLETED, ABORTED, PROGRESS, RESTORED_KEY, MUTANT_RESET }

    public record ProcessIdentity(String logicalName, String runtimeId, String process) {
        public ProcessIdentity {
            requireText(logicalName); requireText(runtimeId); requireText(process);
        }
    }

    public record Progress(int keyCount, int observedKeys, long minimumObservedCount,
                           long processedThisAttempt, long maximumInputId) {}

    public record KeyProgress(int keyCount, int key, long inputId, long previousCount,
                              long previousSum, long newCount, long newSum) {}

    public record Marker(ProcessIdentity process, Event event, String jobId, String operatorUid,
                         int executionAttempt, int subtask, long checkpointId, long restoredCheckpointId,
                         long lineNumber, Optional<Progress> progress, Optional<KeyProgress> keyProgress,
                         Optional<String> buildMode) {
        public Marker {
            Objects.requireNonNull(process); Objects.requireNonNull(event);
            requireText(jobId); requireText(operatorUid);
            Objects.requireNonNull(progress); Objects.requireNonNull(keyProgress); Objects.requireNonNull(buildMode);
        }
    }

    public record Trace(List<Marker> markers, List<String> diagnostics, boolean complete) {
        public Trace {
            markers = List.copyOf(markers); diagnostics = List.copyOf(diagnostics);
            if (markers.size() > MAX_MARKERS || diagnostics.size() > MAX_DIAGNOSTICS)
                throw new IllegalArgumentException("State trace exceeds its bounded evidence limits");
            if (complete && !diagnostics.isEmpty())
                throw new IllegalArgumentException("A complete trace cannot contain capture/parser failures");
        }
    }

    /** Produced by an independent, completely traversed read_committed snapshot. */
    public record CommittedPrefix(boolean snapshotComplete, boolean validStatePrefix,
                                  long observedRecords, long maximumId, Map<Integer, Long> perKeyCounts,
                                  Map<Integer, Long> beginningOffsets, Map<Integer, Long> endOffsets) {
        public CommittedPrefix {
            perKeyCounts = Map.copyOf(perKeyCounts);
            beginningOffsets = Map.copyOf(beginningOffsets); endOffsets = Map.copyOf(endOffsets);
        }
    }

    public record Checkpoint(long checkpointId, long acknowledgedAtMillis, Marker snapshot, Marker completion) {}

    public record Readiness(boolean ready, String reason, String jobId, int keyCount, long totalRecords,
                            TaskManagerControl.Identity target, int executionAttempt,
                            FlinkJobObservation jobObservation, Map<Long, Checkpoint> completedSnapshots,
                            CommittedPrefix committedPrefix,
                            List<org.savonitar.flink.stability.runtime.api.RawEvidenceRef> checkpointEvidence) {
        public Readiness { completedSnapshots = Map.copyOf(completedSnapshots); checkpointEvidence = List.copyOf(checkpointEvidence); }
        public Readiness(boolean ready, String reason, String jobId, int keyCount, long totalRecords,
                         TaskManagerControl.Identity target, int executionAttempt, FlinkJobObservation jobObservation,
                         Map<Long, Checkpoint> completedSnapshots, CommittedPrefix committedPrefix) {
            this(ready, reason, jobId, keyCount, totalRecords, target, executionAttempt, jobObservation,
                    completedSnapshots, committedPrefix, List.of());
        }
        public Readiness withCheckpointEvidence(List<org.savonitar.flink.stability.runtime.api.RawEvidenceRef> raw) {
            return new Readiness(ready, reason, jobId, keyCount, totalRecords, target, executionAttempt,
                    jobObservation, completedSnapshots, committedPrefix, raw);
        }
    }

    public record Recovery(boolean confirmed, String reason, Readiness readiness, long restoredCheckpointId,
                           Optional<Marker> restoredStart, Optional<Marker> postRestoreProgress,
                           List<Marker> restoredKeys, List<Marker> mutantResets) {
        public Recovery {
            Objects.requireNonNull(restoredStart); Objects.requireNonNull(postRestoreProgress);
            restoredKeys = List.copyOf(restoredKeys); mutantResets = List.copyOf(mutantResets);
        }
    }

    /** Reads only registered TaskManager process outputs, retaining complete lines of the live prefix. */
    public static Trace collect(List<FlinkComponentProvisioningEvidence> provisioning,
                                List<FlinkComponentLog> logs, String jobId) {
        return collect(provisioning, logs, jobId, false);
    }

    /** After the physical fence, an incomplete final line is lost evidence, not a growing tail. */
    public static Trace collectFenced(List<FlinkComponentProvisioningEvidence> provisioning,
                                      List<FlinkComponentLog> logs, String jobId) {
        return collect(provisioning, logs, jobId, true);
    }

    private static Trace collect(List<FlinkComponentProvisioningEvidence> provisioning,
                                 List<FlinkComponentLog> logs, String jobId, boolean finalCapture) {
        var markers = new ArrayList<Marker>();
        var diagnostics = new ArrayList<String>();
        var bindings = new LinkedHashMap<String, ProcessIdentity>();
        for (var component : provisioning) {
            if (component.role() != FlinkComponentRole.TASK_MANAGER) continue;
            Optional<String> process = component.classLoadProcess().or(() -> component.runtimeJarEvidence()
                    .map(FlinkComponentProvisioningEvidence.RuntimeJarEvidence::classLoadProcess));
            if (process.isEmpty()) {
                diagnostic(diagnostics, component.runtimeId() + ": missing process registration");
                continue;
            }
            if (bindings.putIfAbsent(process.orElseThrow(), new ProcessIdentity(component.logicalName(),
                    component.runtimeId(), process.orElseThrow())) != null)
                diagnostic(diagnostics, process.orElseThrow() + ": ambiguous process registration");
        }
        if (bindings.isEmpty()) diagnostic(diagnostics, "No registered TaskManager output");
        if (bindings.size() > 32) {
            diagnostic(diagnostics, "TaskManager process limit exceeded");
            return new Trace(List.of(), diagnostics, false);
        }
        var paths = new HashSet<Path>();
        var fileKeys = new HashSet<Object>();
        long remainingBytes = MAX_LOG_BYTES;
        for (var binding : bindings.values()) {
            List<FlinkComponentLog> selected = logs.stream()
                    .filter(log -> log.process().equals(binding.process())).toList();
            if (selected.size() != 1) {
                diagnostic(diagnostics, binding.process() + ": missing or duplicate registered output");
                continue;
            }
            var log = selected.getFirst();
            if (log.truncated() || log.error().isPresent()) {
                diagnostic(diagnostics, binding.process() + ": incomplete log capture");
                continue;
            }
            try {
                Path path = log.path().toAbsolutePath().normalize();
                var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (!attributes.isRegularFile()) throw new IOException("Output is not a regular file");
                if (!paths.add(path) || attributes.fileKey() != null && !fileKeys.add(attributes.fileKey()))
                    throw new IOException("Several process registrations share the same output file");
                int limit = (int) Math.min(FlinkComponentLog.MAX_BYTES, remainingBytes);
                byte[] bytes;
                try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
                    bytes = input.readNBytes(limit + 1);
                }
                remainingBytes -= Math.min(limit, bytes.length);
                if (bytes.length > limit) throw new IOException("State log scan byte limit exceeded");
                // A growing log can end midway through a UTF-8 codepoint or marker. Only complete
                // newline-terminated receipts are admissible for a live observation.
                int completeBytes = bytes.length;
                while (completeBytes > 0 && bytes[completeBytes - 1] != '\n') completeBytes--;
                if (finalCapture && completeBytes != bytes.length)
                    diagnostic(diagnostics, binding.process() + ": incomplete final output line after process fence");
                String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes, 0, completeBytes)).toString();
                parseLines(binding, text, jobId, markers, diagnostics);
            } catch (IOException | RuntimeException failure) {
                diagnostic(diagnostics, binding.process() + ": " + failure.getClass().getSimpleName()
                        + ": " + failure.getMessage());
            }
        }
        markers.sort(Comparator.comparing((Marker marker) -> marker.process().process())
                .thenComparingLong(Marker::lineNumber));
        return new Trace(markers, diagnostics, diagnostics.isEmpty());
    }

    /** Pure parsing seam for complete captured lines; production collection also binds physical files. */
    static Trace parse(ProcessIdentity process, String text, String jobId) {
        var markers = new ArrayList<Marker>();
        var diagnostics = new ArrayList<String>();
        if (text.length() > FlinkComponentLog.MAX_BYTES) diagnostic(diagnostics, "State log scan byte limit exceeded");
        else parseLines(process, text, jobId, markers, diagnostics);
        return new Trace(markers, diagnostics, diagnostics.isEmpty());
    }

    private static void parseLines(ProcessIdentity process, String text, String jobId,
                                   List<Marker> markers, List<String> diagnostics) {
        long lineNumber = 0;
        for (String line : text.split("\\n", -1)) {
            lineNumber++;
            int start = line.indexOf(PREFIX);
            if (start < 0) continue;
            try {
                if (line.length() - start > MAX_MARKER_LENGTH) throw new IllegalArgumentException("Marker too long");
                String[] tokens = line.substring(start).stripTrailing().split(" ", -1);
                var values = new HashMap<String, String>();
                for (int index = 1; index < tokens.length; index++) {
                    int equals = tokens[index].indexOf('=');
                    if (equals < 1 || equals == tokens[index].length() - 1
                            || values.putIfAbsent(tokens[index].substring(0, equals),
                                    tokens[index].substring(equals + 1)) != null)
                        throw new IllegalArgumentException("Malformed or duplicate marker field");
                }
                // Other jobs can share a registered process; they cannot provide this job's proof.
                if (values.containsKey("jobId") && !jobId.equals(values.get("jobId"))) continue;
                Event event = Event.valueOf(tokens[0].substring(PREFIX.length()));
                Set<String> expected = new HashSet<>(BASE_FIELDS);
                if (event == Event.START) expected.add("buildMode");
                if (event == Event.SNAPSHOT || event == Event.PROGRESS) expected.addAll(PROGRESS_FIELDS);
                if (event == Event.RESTORED_KEY || event == Event.MUTANT_RESET) expected.addAll(KEY_FIELDS);
                if (!expected.equals(values.keySet()) || !"1".equals(values.get("version"))
                        || !jobId.equals(values.get("jobId")) || !OPERATOR_UID.equals(values.get("operatorUid")))
                    throw new IllegalArgumentException("Unknown marker shape, version, job or operator identity");
                int attempt = integer(values, "attempt"), subtask = integer(values, "subtask");
                long checkpoint = signedCheckpoint(values.get("checkpoint"));
                long restored = signedCheckpoint(values.get("restoredCheckpoint"));
                if (checkpoint == 0 || restored == 0 || subtask != 0)
                    throw new IllegalArgumentException("Invalid checkpoint or subtask identity");
                if ((event == Event.SNAPSHOT || event == Event.COMPLETED || event == Event.ABORTED) && checkpoint < 1
                        || event != Event.SNAPSHOT && event != Event.COMPLETED && event != Event.ABORTED
                                && checkpoint != restored)
                    throw new IllegalArgumentException("Event checkpoint identity mismatch");
                Optional<Progress> progress = Optional.empty();
                Optional<KeyProgress> key = Optional.empty();
                Optional<String> buildMode = Optional.empty();
                if (event == Event.START) {
                    String mode = values.get("buildMode");
                    if (!Set.of("correct", "reset-state-on-restore-v1").contains(mode))
                        throw new IllegalArgumentException("Unknown workload build mode");
                    buildMode = Optional.of(mode);
                }
                if (event == Event.SNAPSHOT || event == Event.PROGRESS) {
                    int count = keyCount(values), seen = integer(values, "observedKeys");
                    long minimum = number(values, "minimumObservedCount");
                    long processed = number(values, "processedThisAttempt");
                    long maximum = signedCheckpoint(values.get("maximumInputId"));
                    if (seen > count || processed < seen || maximum < -1
                            || seen == 0 && (minimum != 0 || processed != 0 || maximum != -1)
                            || seen > 0 && (minimum < 1 || maximum < 0)
                            || event == Event.PROGRESS && seen != count)
                        throw new IllegalArgumentException("Inconsistent bounded progress summary");
                    progress = Optional.of(new Progress(count, seen, minimum, processed, maximum));
                }
                if (event == Event.RESTORED_KEY || event == Event.MUTANT_RESET) {
                    int count = keyCount(values), selectedKey = integer(values, "key");
                    long input = number(values, "inputId"), previousCount = number(values, "previousCount");
                    long previousSum = number(values, "previousSum"), newCount = number(values, "newCount");
                    long newSum = number(values, "newSum");
                    if (restored < 1 || selectedKey >= count || input % count != selectedKey
                            || newCount < 1 || newSum < 1 || (previousCount == 0) != (previousSum == 0)
                            || event == Event.MUTANT_RESET && previousCount == 0)
                        throw new IllegalArgumentException("Invalid restored-key receipt");
                    key = Optional.of(new KeyProgress(count, selectedKey, input, previousCount, previousSum, newCount, newSum));
                }
                if (markers.size() >= MAX_MARKERS) throw new IllegalArgumentException("State marker limit exceeded");
                markers.add(new Marker(process, event, jobId, OPERATOR_UID, attempt, subtask, checkpoint,
                        restored, lineNumber, progress, key, buildMode));
            } catch (IllegalArgumentException | ArithmeticException failure) {
                diagnostic(diagnostics, process.process() + ": line " + lineNumber + ": " + failure.getMessage());
            }
        }
    }

    public static Readiness ready(String jobId, int keyCount, long totalRecords,
                                  TaskManagerControl.Identity target, FlinkJobObservation observation,
                                  JsonNode checkpointOverview, Trace trace, CommittedPrefix prefix) {
        int attempt = -1;
        var checkpoints = new TreeMap<Long, Checkpoint>();
        String reason = "state-recovery.window-unconfirmed";
        if (keyCount < 1 || keyCount > 1024 || totalRecords < 2L * keyCount)
            reason = "state-recovery.input-too-small";
        else if (!trace.complete()) reason = "state-recovery.trace-incomplete";
        else if (observation.state() != FlinkJobState.RUNNING) reason = "state-recovery.job-not-running";
        else if (observation.completedCheckpoints() < 1) reason = "state-recovery.checkpoint-progress-unconfirmed";
        else if (!validPrefix(prefix, keyCount, totalRecords)) reason = "state-recovery.committed-prefix-unconfirmed";
        else {
            var active = observation.subtasks().stream()
                    .filter(task -> task.vertexName().contains("Keyed Prefix v1") && task.index() == 0)
                    .filter(task -> "RUNNING".equals(task.status()))
                    .filter(task -> task.taskManagerId().filter(target.resourceId()::equals).isPresent()).toList();
            if (active.size() != 1) reason = "state-recovery.execution-unconfirmed";
            else {
                attempt = active.getFirst().attempt();
                final int currentAttempt = attempt;
                var current = trace.markers().stream().filter(marker -> marker.jobId().equals(jobId)
                        && marker.executionAttempt() == currentAttempt && marker.subtask() == 0
                        && marker.process().runtimeId().equals(target.runtimeId())
                        && marker.process().logicalName().equals(target.logicalName())).toList();
                var starts = current.stream().filter(marker -> marker.event() == Event.START).toList();
                if (starts.size() != 1) reason = "state-recovery.execution-unconfirmed";
                else {
                    Marker start = starts.getFirst();
                    for (var entry : completedCheckpoints(checkpointOverview).entrySet()) {
                        long id = entry.getKey(), acknowledgedAt = entry.getValue();
                        var snapshots = current.stream().filter(marker -> marker.event() == Event.SNAPSHOT
                                && marker.checkpointId() == id && marker.lineNumber() > start.lineNumber()
                                && sameExecution(start, marker)).toList();
                        var completions = current.stream().filter(marker -> marker.event() == Event.COMPLETED
                                && marker.checkpointId() == id && sameExecution(start, marker)).toList();
                        boolean aborted = current.stream().anyMatch(marker -> marker.event() == Event.ABORTED
                                && marker.checkpointId() == id);
                        if (snapshots.size() != 1 || completions.size() != 1 || aborted) continue;
                        Marker snapshot = snapshots.getFirst(), completion = completions.getFirst();
                        Progress progress = snapshot.progress().orElseThrow();
                        if (completion.lineNumber() <= snapshot.lineNumber()
                                || acknowledgedAt > observation.jobManagerTimeMillis()
                                || progress.keyCount() != keyCount || progress.observedKeys() != keyCount
                                || progress.minimumObservedCount() < 1 || progress.processedThisAttempt() < keyCount
                                || progress.maximumInputId() < keyCount - 1
                                || progress.maximumInputId() >= totalRecords - keyCount) continue;
                        checkpoints.put(id, new Checkpoint(id, acknowledgedAt, snapshot, completion));
                    }
                    reason = checkpointWindowReason(checkpointOverview, checkpoints, current);
                }
            }
        }
        return new Readiness(reason.equals("state-recovery.ready"), reason, jobId, keyCount, totalRecords,
                target, attempt, observation, checkpoints, prefix);
    }

    /** An older complete checkpoint is insufficient if a newer restore candidate is visible. */
    private static String checkpointWindowReason(JsonNode overview, Map<Long, Checkpoint> proven,
                                                  List<Marker> current) {
        if (proven.isEmpty()) return "state-recovery.checkpoint-progress-unconfirmed";
        JsonNode latest = overview == null ? null : overview.path("latest").path("completed");
        if (latest == null || !latest.path("status").asText().equals("COMPLETED")
                || !latest.path("id").isIntegralNumber() || !latest.path("id").canConvertToLong()
                || !latest.path("latest_ack_timestamp").isIntegralNumber()
                || !latest.path("latest_ack_timestamp").canConvertToLong()
                || latest.path("latest_ack_timestamp").longValue() < 0
                || !proven.containsKey(latest.path("id").longValue()))
            return "state-recovery.latest-checkpoint-unproven";
        long latestId = latest.path("id").longValue();
        if (proven.get(latestId).acknowledgedAtMillis() != latest.path("latest_ack_timestamp").longValue())
            return "state-recovery.latest-checkpoint-unproven";
        if (completedCheckpoints(overview).keySet().stream().anyMatch(id -> id > latestId))
            return "state-recovery.checkpoint-boundary-stale";
        JsonNode counts = overview.path("counts");
        if (!counts.isMissingNode() && !counts.isObject())
            return "state-recovery.checkpoint-observation-invalid";
        if (counts.has("in_progress")) {
            JsonNode inProgress = counts.path("in_progress");
            if (!inProgress.isIntegralNumber() || !inProgress.canConvertToLong() || inProgress.longValue() < 0)
                return "state-recovery.checkpoint-observation-invalid";
            if (inProgress.longValue() > 0) return "state-recovery.checkpoint-in-flight";
        }
        JsonNode history = overview.path("history");
        if (!history.isMissingNode() && !history.isArray())
            return "state-recovery.checkpoint-observation-invalid";
        for (JsonNode checkpoint : history) {
            if (checkpoint.path("status").asText().equals("IN_PROGRESS"))
                return "state-recovery.checkpoint-in-flight";
        }
        if (overview.path("observedCheckpointDetails").path("status").asText().equals("IN_PROGRESS"))
            return "state-recovery.checkpoint-in-flight";
        // Logs are captured after REST. A new snapshot/completion receipt can reveal a
        // checkpoint that started or finished between those reads; wait for a fresh boundary.
        if (current.stream().anyMatch(marker -> marker.checkpointId() > latestId
                && marker.event() == Event.COMPLETED))
            return "state-recovery.checkpoint-boundary-stale";
        for (Marker marker : current) {
            if (marker.checkpointId() <= latestId) continue;
            if (marker.event() == Event.SNAPSHOT && current.stream().noneMatch(other ->
                    other.event() == Event.ABORTED && other.checkpointId() == marker.checkpointId()
                            && sameExecution(marker, other) && other.lineNumber() > marker.lineNumber()))
                return "state-recovery.checkpoint-in-flight";
        }
        return "state-recovery.ready";
    }

    public static Recovery evaluate(Readiness readiness, TaskManagerKillEffect effect, Trace trace) {
        if (!readiness.ready()) return missing(readiness, readiness.reason(), -1);
        if (!trace.complete()) return missing(readiness, "state-recovery.trace-incomplete", -1);
        if (effect.outcome() != TaskManagerKillEffect.Outcome.CHECKPOINT_RESTORED || effect.restore().isEmpty())
            return missing(readiness, "state-recovery.restore-unconfirmed", -1);
        var kill = effect.kill();
        long checkpoint = effect.restore().orElseThrow().checkpointId();
        if (kill.identity().filter(readiness.target()::equals).isEmpty()
                || !kill.target().equals(readiness.target().logicalName())
                || kill.jobBeforeKill().observation().isEmpty()
                || kill.jobBeforeKill().observation().orElseThrow().state() != FlinkJobState.RUNNING
                || kill.jobBeforeKill().observation().orElseThrow().subtasks().stream().noneMatch(task ->
                        task.vertexName().contains("Keyed Prefix v1") && task.index() == 0
                                && task.attempt() == readiness.executionAttempt() && task.status().equals("RUNNING")
                                && task.taskManagerId().filter(readiness.target().resourceId()::equals).isPresent()))
            return missing(readiness, "state-recovery.kill-binding-unconfirmed", checkpoint);
        Checkpoint restored = readiness.completedSnapshots().get(checkpoint);
        if (restored == null) return missing(readiness, "state-recovery.restore-checkpoint-unproven", checkpoint);
        var starts = trace.markers().stream().filter(marker -> marker.event() == Event.START
                && marker.jobId().equals(readiness.jobId()) && marker.subtask() == 0
                && marker.executionAttempt() > readiness.executionAttempt()
                && marker.restoredCheckpointId() == checkpoint
                && marker.process().logicalName().equals(readiness.target().logicalName())
                && !marker.process().runtimeId().equals(readiness.target().runtimeId())).toList();
        if (starts.size() != 1) return missing(readiness, "state-recovery.restored-execution-unconfirmed", checkpoint);
        Marker start = starts.getFirst();
        var current = trace.markers().stream().filter(marker -> sameExecution(start, marker)
                && marker.lineNumber() > start.lineNumber()).toList();
        var keys = current.stream().filter(marker -> marker.event() == Event.RESTORED_KEY)
                .sorted(Comparator.comparingInt(marker -> marker.keyProgress().orElseThrow().key())).toList();
        var resets = current.stream().filter(marker -> marker.event() == Event.MUTANT_RESET)
                .sorted(Comparator.comparingInt(marker -> marker.keyProgress().orElseThrow().key())).toList();
        var progress = current.stream().filter(marker -> marker.event() == Event.PROGRESS
                && marker.progress().orElseThrow().keyCount() == readiness.keyCount()
                && marker.progress().orElseThrow().observedKeys() == readiness.keyCount()
                && marker.progress().orElseThrow().processedThisAttempt() >= readiness.keyCount()).toList();
        String reason = "state-recovery.confirmed";
        if (keys.size() != readiness.keyCount() || progress.size() != 1)
            reason = "state-recovery.post-restore-progress-unconfirmed";
        else {
            long maxBefore = restored.snapshot().progress().orElseThrow().maximumInputId();
            long maxAfter = -1, minimumAfter = Long.MAX_VALUE;
            for (int key = 0; key < keys.size(); key++) {
                Marker marker = keys.get(key);
                KeyProgress receipt = marker.keyProgress().orElseThrow();
                if (receipt.keyCount() != readiness.keyCount() || receipt.key() != key
                        || receipt.inputId() <= maxBefore || receipt.inputId() >= readiness.totalRecords()
                        || marker.lineNumber() >= progress.getFirst().lineNumber())
                    reason = "state-recovery.post-restore-progress-unconfirmed";
                maxAfter = Math.max(maxAfter, receipt.inputId());
                minimumAfter = Math.min(minimumAfter, receipt.newCount());
            }
            Progress summary = progress.getFirst().progress().orElseThrow();
            if (summary.processedThisAttempt() != readiness.keyCount()
                    || summary.maximumInputId() != maxAfter || summary.minimumObservedCount() != minimumAfter)
                reason = "state-recovery.post-restore-progress-unconfirmed";
        }
        boolean mutant = start.buildMode().filter("reset-state-on-restore-v1"::equals).isPresent();
        if (!mutant && !resets.isEmpty() || mutant && !validResets(keys, resets)
                || progress.size() == 1 && resets.stream().anyMatch(reset -> reset.lineNumber() >= progress.getFirst().lineNumber()))
            reason = "state-recovery.mutant-activation-unconfirmed";
        return new Recovery(reason.equals("state-recovery.confirmed"), reason, readiness, checkpoint,
                Optional.of(start), progress.size() == 1 ? Optional.of(progress.getFirst()) : Optional.empty(), keys, resets);
    }

    private static boolean validResets(List<Marker> keys, List<Marker> resets) {
        if (keys.size() != resets.size()) return false;
        for (int index = 0; index < keys.size(); index++) {
            var key = keys.get(index).keyProgress().orElseThrow();
            var reset = resets.get(index).keyProgress().orElseThrow();
            if (!key.equals(reset) || reset.previousCount() < 1 || reset.previousSum() < 1
                    || reset.newCount() != 1 || reset.inputId() == Long.MAX_VALUE
                    || reset.newSum() != reset.inputId() + 1) return false;
        }
        return !keys.isEmpty();
    }

    private static boolean sameExecution(Marker first, Marker second) {
        return first.process().equals(second.process()) && first.jobId().equals(second.jobId())
                && first.operatorUid().equals(second.operatorUid()) && first.executionAttempt() == second.executionAttempt()
                && first.subtask() == second.subtask() && first.restoredCheckpointId() == second.restoredCheckpointId();
    }

    private static Map<Long, Long> completedCheckpoints(JsonNode overview) {
        var result = new TreeMap<Long, Long>();
        if (overview == null) return result;
        var candidates = new ArrayList<JsonNode>();
        candidates.add(overview.path("latest").path("completed"));
        if (overview.path("history").isArray()) overview.path("history").forEach(candidates::add);
        for (var checkpoint : candidates) {
            JsonNode id = checkpoint.path("id"), ack = checkpoint.path("latest_ack_timestamp");
            if (checkpoint.path("status").asText().equals("COMPLETED") && id.isIntegralNumber()
                    && id.canConvertToLong() && id.longValue() > 0 && ack.isIntegralNumber()
                    && ack.canConvertToLong() && ack.longValue() >= 0)
                result.merge(id.longValue(), ack.longValue(), Math::max);
        }
        return result;
    }

    private static boolean validPrefix(CommittedPrefix prefix, int keys, long total) {
        if (!prefix.snapshotComplete() || !prefix.validStatePrefix() || prefix.observedRecords() < keys
                || prefix.observedRecords() > total - keys || prefix.maximumId() != prefix.observedRecords() - 1
                || !prefix.beginningOffsets().equals(Map.of(0, 0L))
                || prefix.endOffsets().size() != 1 || !prefix.endOffsets().containsKey(0)
                || prefix.endOffsets().get(0) < prefix.observedRecords() || prefix.perKeyCounts().size() != keys) return false;
        for (int key = 0; key < keys; key++) {
            long expected = (prefix.maximumId() - key) / keys + 1;
            if (!Objects.equals(prefix.perKeyCounts().get(key), expected)) return false;
        }
        return true;
    }

    private static Recovery missing(Readiness readiness, String reason, long checkpoint) {
        return new Recovery(false, reason, readiness, checkpoint, Optional.empty(), Optional.empty(), List.of(), List.of());
    }

    private static int keyCount(Map<String, String> fields) {
        int count = integer(fields, "keyCount");
        if (count < 1 || count > 1024) throw new IllegalArgumentException("Key count outside 1..1024");
        return count;
    }

    private static int integer(Map<String, String> fields, String key) {
        return Math.toIntExact(number(fields, key));
    }

    private static long number(Map<String, String> fields, String key) {
        String value = fields.get(key);
        if (value == null || !value.matches("0|[1-9][0-9]*"))
            throw new IllegalArgumentException("Noncanonical nonnegative integer field " + key);
        return Long.parseLong(value);
    }

    private static long signedCheckpoint(String value) {
        if ("-1".equals(value)) return -1;
        return number(Map.of("value", value), "value");
    }

    private static void diagnostic(List<String> diagnostics, String detail) {
        if (diagnostics.size() < MAX_DIAGNOSTICS)
            diagnostics.add(detail.length() > 512 ? detail.substring(0, 512) : detail);
    }

    private static void requireText(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Blank evidence identity");
    }
}
