package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class FlinkCheckpointEvidenceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final FlinkJobHandle JOB = new FlinkJobHandle("a".repeat(32));
    private static final String PATH = "/jobs/" + JOB.jobId() + "/checkpoints";

    @Test
    void readsActualReleasedFlinkConfigurationSerialization() throws Exception {
        var upstream = new CheckpointConfigInfo(CheckpointConfigInfo.ProcessingMode.EXACTLY_ONCE,
                10_000, 60_000, 0, 1, new CheckpointConfigInfo.ExternalizedCheckpointInfo(true, false),
                "HashMapStateBackend", "FileSystemCheckpointStorage", false, 2, 0, true, false, 600_000, "memory");
        String json = RestMapperUtils.getStrictObjectMapper().writeValueAsString(upstream);
        List<String> paths = new ArrayList<>();
        var client = client(json, paths);
        var result = client.checkpointConfiguration(JOB, Duration.ofSeconds(1));
        assertEquals(List.of(PATH + "/config"), paths);
        assertEquals("exactly_once", result.mode());
        assertEquals(1, result.maxConcurrent());
        assertEquals(2, result.tolerableFailedCheckpoints());
        assertEquals(10_000, result.intervalMillis());
        assertEquals("FileSystemCheckpointStorage", result.checkpointStorage());
        assertTrue(result.externalized());
        assertFalse(result.deleteOnCancellation());
    }

    @Test
    void readsActualReleasedPendingAndFailedCheckpointSerialization() throws Exception {
        // The released DTO constructor exposes a package-private enum, so obtain that enum
        // reflectively while still serializing the real released response class.
        Object type = java.util.Arrays.stream(Class.forName(
                "org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics$RestAPICheckpointType")
                .getEnumConstants()).filter(value -> value.toString().equals("CHECKPOINT")).findFirst().orElseThrow();
        Object pending = CheckpointStatistics.PendingCheckpointStatistics.class.getConstructors()[0].newInstance(
                7L, CheckpointStatsStatus.IN_PROGRESS, false, null, 100L, -1L, 0L, 0L, 0L, 0L, 0L, 0L, 3, 0, type, Map.of());
        String pendingJson = RestMapperUtils.getStrictObjectMapper().writeValueAsString(pending);
        var pendingResult = client(pendingJson, new ArrayList<>()).checkpointDetails(JOB, 7, Duration.ofSeconds(1));
        assertEquals(FlinkCheckpointEvidence.Status.IN_PROGRESS, pendingResult.status());
        assertEquals(-1, pendingResult.latestAckTimestamp());
        Object failed = CheckpointStatistics.FailedCheckpointStatistics.class.getConstructors()[0].newInstance(
                7L, CheckpointStatsStatus.FAILED, false, null, 100L, -1L, 0L, 0L, 20L, 0L, 0L, 0L, 3, 0,
                type, Map.of(), 120L, "IOException: Input/output error");
        String failedJson = RestMapperUtils.getStrictObjectMapper().writeValueAsString(failed);
        var failedResult = client(failedJson, new ArrayList<>()).checkpointDetails(JOB, 7, Duration.ofSeconds(1));
        assertEquals(120, failedResult.failureTimestamp().orElseThrow());
        assertEquals("IOException: Input/output error", failedResult.failureMessage().orElseThrow());
        assertTrue(failedResult.externalPath().isEmpty());
    }

    @Test
    void preservesHistoryWindowLatestOutsideCacheCountsAndRestoreIdentity() throws Exception {
        ObjectNode history = history();
        var result = client(history.toString(), new ArrayList<>()).checkpointHistory(JOB, Duration.ofSeconds(1));
        assertEquals(JOB.jobId(), result.jobId());
        assertEquals(12, result.counts().total());
        assertEquals(12, result.highestObservedId());
        assertEquals(List.of(12L), result.inProgress().stream().map(FlinkCheckpointEvidence.Checkpoint::id).toList());
        assertEquals(11, result.latestFailed().orElseThrow().id());
        assertEquals(9, result.latestCompleted().orElseThrow().id());
        assertEquals(9, result.find(9).orElseThrow().id());
        assertEquals(8, result.latestRestored().orElseThrow().id());
        assertTrue(result.find(3).isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> result.retained().clear());
    }

    @Test
    void failsOnDuplicateConflictingAndExcessiveRetainedHistory() throws Exception {
        ObjectNode duplicate = history();
        duplicate.withArray("history").add(duplicate.withArray("history").get(0).deepCopy());
        assertInvalidHistory(duplicate);
        ObjectNode conflict = history();
        ((ObjectNode) conflict.path("latest").path("failed")).put("failure_message", "different");
        assertInvalidHistory(conflict);
        ObjectNode oversized = history();
        for (int i = 0; i < FlinkCheckpointEvidence.MAX_HISTORY_ENTRIES; i++) oversized.withArray("history").add(checkpoint(i + 20, "FAILED"));
        assertInvalidHistory(oversized);
    }

    @Test
    void rejectsHistoryCountsAndLatestTypeContradictions() throws Exception {
        ObjectNode counts = history();
        ((ObjectNode) counts.path("counts")).put("total", 13);
        assertInvalidHistory(counts);
        ObjectNode wrongLatest = history();
        ((ObjectNode) wrongLatest.path("latest")).set("completed", checkpoint(12, "IN_PROGRESS"));
        assertInvalidHistory(wrongLatest);
        ObjectNode newest = history();
        newest.withArray("history").set(0, checkpoint(13, "FAILED"));
        assertInvalidHistory(newest);
        ObjectNode missing = history();
        missing.remove("counts");
        assertInvalidHistory(missing);
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "status", "is_savepoint", "checkpoint_type", "trigger_timestamp", "state_size", "num_subtasks", "failure_timestamp"})
    void rejectsMissingCheckpointFields(String field) throws Exception {
        ObjectNode node = checkpoint(7, "FAILED");
        node.remove(field);
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointDetails(JOB, 7, Duration.ofSeconds(1)));
    }

    @Test
    void exactDetailsIdentityAndFailureBoundsAreEnforced() throws Exception {
        ObjectNode node = checkpoint(8, "FAILED");
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointDetails(JOB, 7, Duration.ofSeconds(1)));
        node.put("id", 7).put("failure_message", "x".repeat(32 * 1024 + 1));
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointDetails(JOB, 7, Duration.ofSeconds(1)));
        node.put("failure_message", "EIO").put("failure_timestamp", 1);
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointDetails(JOB, 7, Duration.ofSeconds(1)));
        node.put("failure_timestamp", 120).put("is_savepoint", true);
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointDetails(JOB, 7, Duration.ofSeconds(1)));
    }

    @Test
    void strictConfigurationDoesNotCoerceOrInventRequiredValues() throws Exception {
        ObjectNode node = configuration();
        node.put("max_concurrent", "1");
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointConfiguration(JOB, Duration.ofSeconds(1)));
        node.put("max_concurrent", 1).remove("state_changelog_enabled");
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointConfiguration(JOB, Duration.ofSeconds(1)));
        node.put("state_changelog_enabled", false).put("tolerable_failed_checkpoints", -1);
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointConfiguration(JOB, Duration.ofSeconds(1)));
    }

    @Test
    void rejectsDuplicateKeysTrailingJsonAndOversizedBody() {
        for (String body : List.of("{\"counts\":{},\"counts\":{}}", history().toString() + " {}",
                " ".repeat(FlinkCheckpointEvidence.MAX_RESPONSE_BYTES + 1))) {
            assertThrows(IOException.class, () -> client(body, new ArrayList<>()).checkpointHistory(JOB, Duration.ofSeconds(1)));
        }
    }

    @Test
    void responseLimitAppliesAfterHttpDecompression() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var compressed = new java.io.ByteArrayOutputStream();
        try (var gzip = new GZIPOutputStream(compressed)) {
            gzip.write(" ".repeat(FlinkCheckpointEvidence.MAX_RESPONSE_BYTES + 1).getBytes(StandardCharsets.UTF_8));
        }
        server.createContext(PATH, exchange -> {
            exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            exchange.sendResponseHeaders(200, compressed.size());
            try (var out = exchange.getResponseBody()) { out.write(compressed.toByteArray()); }
        });
        server.start();
        try (var client = new FlinkRestApiClient("http://127.0.0.1:" + server.getAddress().getPort())) {
            IOException failure = assertThrows(IOException.class, () -> client.checkpointHistory(JOB, Duration.ofSeconds(3)));
            assertTrue(failure.getMessage().contains("size limit"));
        } finally { server.stop(0); }
    }

    @Test
    void evidenceRetriesGetOnlyWithinOneDeadlineAndRejectsExpiredSuccess() {
        AtomicLong clock = new AtomicLong();
        List<Duration> budgets = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        var client = new FlinkRestApiClient((method, path, body, timeout) -> {
            assertEquals("GET", method);
            budgets.add(timeout);
            if (calls.getAndIncrement() == 0) return response(503, "unavailable");
            clock.set(Duration.ofMillis(501).toNanos());
            return response(200, history().toString());
        }, MAPPER, clock::get, duration -> clock.addAndGet(duration.toNanos()));
        assertThrows(FlinkRestTimeoutException.class, () -> client.checkpointHistory(JOB, Duration.ofMillis(500)));
        assertEquals(List.of(Duration.ofMillis(500), Duration.ofMillis(250)), budgets);
        assertEquals(1, client.restErrors().size());
    }

    @Test
    void invalidIdentityAndBudgetNeverMakeARequestAndClockUsesOneDirectGet() throws Exception {
        List<String> paths = new ArrayList<>();
        List<Duration> budgets = new ArrayList<>();
        var client = new FlinkRestApiClient((method, path, body, timeout) -> {
            paths.add(path); budgets.add(timeout); return response(200, "{\"now\":321}");
        }, MAPPER, () -> 0L);
        assertThrows(IllegalArgumentException.class, () -> client.checkpointHistory(new FlinkJobHandle("bad"), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> client.checkpointDetails(JOB, 0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> client.checkpointConfiguration(JOB, Duration.ZERO));
        assertTrue(paths.isEmpty());
        assertEquals(321, client.jobManagerTimeMillis(JOB, Duration.ofMillis(123)));
        assertEquals(List.of("/jobs/" + JOB.jobId()), paths);
        assertEquals(List.of(Duration.ofMillis(123)), budgets);
    }

    private static void assertInvalidHistory(ObjectNode node) {
        assertThrows(IOException.class, () -> client(node.toString(), new ArrayList<>()).checkpointHistory(JOB, Duration.ofSeconds(1)));
    }

    private static FlinkRestApiClient client(String json, List<String> paths) {
        return new FlinkRestApiClient((method, path, body, timeout) -> {
            assertEquals("GET", method); assertNull(body); paths.add(path); return response(200, json);
        }, MAPPER);
    }

    private static FlinkRestApiClient.HttpResponse response(int code, String json) {
        return new FlinkRestApiClient.HttpResponse(code, json.getBytes(StandardCharsets.UTF_8));
    }

    private static ObjectNode configuration() {
        ObjectNode node = MAPPER.createObjectNode().put("mode", "exactly_once").put("interval", 10_000)
                .put("timeout", 60_000).put("min_pause", 0).put("max_concurrent", 1)
                .put("tolerable_failed_checkpoints", 2).put("state_backend", "HashMapStateBackend")
                .put("checkpoint_storage", "FileSystemCheckpointStorage").put("unaligned_checkpoints", false)
                .put("state_changelog_enabled", false);
        node.putObject("externalization").put("enabled", true).put("delete_on_cancellation", false);
        return node;
    }

    private static ObjectNode history() {
        ObjectNode node = MAPPER.createObjectNode();
        node.putObject("counts").put("total", 12).put("in_progress", 1).put("completed", 9).put("failed", 2).put("restored", 1);
        node.putArray("history").add(checkpoint(11, "FAILED")).add(checkpoint(12, "IN_PROGRESS"));
        var latest = node.putObject("latest");
        latest.set("completed", checkpoint(9, "COMPLETED")); latest.set("failed", checkpoint(11, "FAILED")); latest.putNull("savepoint");
        latest.putObject("restored").put("id", 8).put("restore_timestamp", 130).put("is_savepoint", false).put("external_path", "file:///checkpoints/job/chk-8");
        return node;
    }

    private static ObjectNode checkpoint(long id, String status) {
        ObjectNode node = MAPPER.createObjectNode().put("id", id).put("status", status).put("is_savepoint", false)
                .put("checkpoint_type", "CHECKPOINT").put("trigger_timestamp", 100).put("latest_ack_timestamp", 110)
                .put("checkpointed_size", 128).put("state_size", 128).put("num_subtasks", 1).put("num_acknowledged_subtasks", 1);
        if (status.equals("FAILED")) node.put("failure_timestamp", 120).put("failure_message", "IOException: Input/output error");
        if (status.equals("COMPLETED")) node.put("external_path", "file:///checkpoints/job/chk-" + id).put("discarded", false);
        return node;
    }
}
