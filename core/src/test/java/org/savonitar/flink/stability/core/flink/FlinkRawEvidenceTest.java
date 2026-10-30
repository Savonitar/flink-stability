package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.Digests;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class FlinkRawEvidenceTest {
    @TempDir Path temporary;
    static final FlinkJobHandle JOB = new FlinkJobHandle("a".repeat(32));
    static final String HISTORY = "{ \"counts\":{\"total\":0,\"in_progress\":0,\"completed\":0,\"failed\":0,\"restored\":0},\"history\":[],\"latest\":{} }\n";

    @Test void bindsRepeatedSameEndpointToDistinctExactCallGroups() throws Exception {
        var client = client(List.of(response(200, HISTORY), response(200, HISTORY)));
        FlinkCheckpointEvidence.History first, second;
        try (var capture = client.captureRawEvidence()) {
            first = client.checkpointHistory(JOB, Duration.ofSeconds(1));
            second = client.checkpointHistory(JOB, Duration.ofSeconds(1));
            assertEquals(0, client.rawEvidenceUtilisation().orElseThrow().captures());
            capture.retain();
        }
        assertEquals(first.counts(), second.counts());
        var a = first.rawEvidence().getFirst(); var b = second.rawEvidence().getFirst();
        assertNotEquals(a.group(), b.group()); assertNotEquals(a.path(), b.path());
        assertEquals(1, a.sequence()); assertEquals(2, b.sequence());
        assertEquals(a.sha256(), b.sha256());
        assertEquals(HISTORY, Files.readString(Path.of(a.path())));
        assertEquals(Digests.sha256(Path.of(a.path())), a.sha256());
        assertEquals("/jobs/" + JOB.jobId() + "/checkpoints", a.endpoint());
    }

    @Test void jobObservationRetainsAllSourceCallsAndScalarClockUsesSeparateGroup() throws Exception {
        var client = client(List.of(response(200, "{\"now\":42,\"state\":\"RUNNING\",\"vertices\":[{\"id\":\"v\",\"name\":\"keyed\"}]}"),
                response(200, HISTORY), response(200, "{\"exceptionHistory\":{\"entries\":[]}}"),
                response(200, "{\"subtasks\":[{\"subtask\":0,\"attempt\":1,\"status\":\"RUNNING\",\"taskmanager-id\":\"tm\"}]}"),
                response(200, "{\"now\":43}")));
        var observed = captured(client, () -> client.observe(JOB, Duration.ofSeconds(1)));
        assertEquals(4, observed.rawEvidence().size());
        assertEquals(1, observed.rawEvidence().stream().map(value -> value.group()).distinct().count());
        assertEquals(42, observed.jobManagerTimeMillis()); assertEquals(1, observed.subtasks().getFirst().attempt());
        assertEquals(43, captured(client, () -> client.jobManagerTimeMillis(JOB, Duration.ofSeconds(1))));
        assertEquals("job-manager-clock-2", client.latestCallEvidence().getFirst().group());
        assertEquals(4, observed.rawEvidence().size());
    }

    @Test void retainsRetryErrorAndDoesNotReplayFailedPost() throws Exception {
        var client = client(List.of(response(503, "{\"error\":\"later\"}"), response(200, HISTORY), response(503, "{\"error\":\"unknown submission\"}")));
        var history = captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1)));
        assertEquals(List.of(503, 200), history.rawEvidence().stream().map(value -> value.status()).toList());
        assertThrows(IOException.class, () -> captured(client, () -> client.triggerCheckpoint(JOB, "b".repeat(32), Duration.ofSeconds(1))));
        assertEquals(1, client.latestCallEvidence().size());
        assertEquals("POST", client.latestCallEvidence().getFirst().method());
        assertEquals(503, client.latestCallEvidence().getFirst().status());
    }

    @Test void captureFailureOrMalformedResponseNeverReturnsTypedSuccess() throws Exception {
        var client = client(List.of(response(200, HISTORY)));
        Files.writeString(temporary.toRealPath().resolve("raw/000001.json"), "reserved");
        assertThrows(IOException.class, () -> captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1))));
        assertEquals("reserved", Files.readString(temporary.toRealPath().resolve("raw/000001.json")));
    }

    @Test void overviewRetainsOriginalBodyBeforeDerivedDetailsAreAdded() throws Exception {
        String overview = "{\"latest\":{\"completed\":{\"id\":1,\"status\":\"COMPLETED\",\"latest_ack_timestamp\":10}},"
                + "\"history\":[{\"id\":2,\"status\":\"IN_PROGRESS\",\"trigger_timestamp\":20}]}";
        var client = client(List.of(response(200, overview), response(200, "{\"id\":2,\"status\":\"IN_PROGRESS\"}"),
                response(200, "{\"vertices\":[]}")));
        var typed = captured(client, () -> client.checkpointOverview(JOB, Duration.ofSeconds(1)));
        assertTrue(typed.has("observedCheckpointDetails"));
        var references = client.latestCallEvidence();
        assertEquals(3, references.size());
        assertEquals("checkpoint-overview-1", references.getFirst().group());
        assertEquals(overview, Files.readString(Path.of(references.getFirst().path())));
        assertFalse(Files.readString(Path.of(references.getFirst().path())).contains("observedCheckpointDetails"));
    }

    @Test void malformedResponseIsRetainedBeforeStrictTypedParsingFails() throws Exception {
        String malformed = "{\"counts\":{},\"counts\":{}}";
        var client = client(List.of(response(200, malformed)));
        assertThrows(IOException.class, () -> captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1))));
        assertEquals(malformed, Files.readString(Path.of(client.latestCallEvidence().getFirst().path())));
    }

    @Test void sixHundredSecondsOfNormalAndDiscardedPollingCannotExhaustTheStore() throws Exception {
        int polls = 6000; // A 600-second attempt at the fault/readiness loops' 100-ms cadence.
        assertTrue(polls > org.savonitar.flink.stability.runtime.api.RawEvidenceStore.MAX_CAPTURES);
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var responses = new java.util.ArrayList<FlinkRestApiClient.HttpResponse>();
        for (int i = 0; i < polls; i++) {
            responses.add(response(200, "{\"state\":\"FINISHED\"}"));
            responses.add(response(200, HISTORY));
            responses.add(response(200, HISTORY));
        }
        responses.add(response(200, HISTORY));
        var client = client(responses, clock::get);
        for (int i = 0; i < polls; i++) {
            // Natural-completion polling remains ungrouped even inside a candidate scope.
            try (var capture = client.captureRawEvidence()) {
                assertEquals(FlinkJobState.FINISHED, client.awaitFinished(JOB, Duration.ofSeconds(1)));
                capture.retain();
            }
            assertTrue(client.checkpointHistory(JOB, Duration.ofSeconds(1)).rawEvidence().isEmpty());
            try (var capture = client.captureRawEvidence()) {
                var candidate = client.checkpointHistory(JOB, Duration.ofSeconds(1));
                assertEquals(1, candidate.rawEvidence().size());
                assertFalse(Files.exists(Path.of(candidate.rawEvidence().getFirst().path())));
            }
            clock.addAndGet(Duration.ofMillis(100).toNanos());
        }
        assertEquals(Duration.ofSeconds(600).toNanos(), clock.get());
        assertEquals(0, client.rawEvidenceUtilisation().orElseThrow().captures());
        assertEquals(0, client.rawEvidenceUtilisation().orElseThrow().bytes());
        var selected = captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1)));
        assertEquals(1, selected.rawEvidence().getFirst().sequence());
        assertEquals(HISTORY, Files.readString(Path.of(selected.rawEvidence().getFirst().path())));
        assertEquals(1, client.rawEvidenceUtilisation().orElseThrow().captures());
        assertTrue(client.rawEvidenceUtilisation().orElseThrow().complete());
        try (var files = Files.list(temporary.toRealPath().resolve("raw"))) { assertEquals(1, files.count()); }
    }

    @Test void emptyRetryBodyAndEmptyPostFailureAreRetainedWithoutDisablingTheClient() throws Exception {
        var client = client(List.of(response(503, ""), response(200, HISTORY), response(503, ""), response(200, HISTORY)));
        var history = captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1)));
        var empty = history.rawEvidence().getFirst();
        assertEquals(0, empty.bytes()); assertEquals(0, Files.size(Path.of(empty.path())));
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", empty.sha256());
        assertEquals(List.of(503, 200), history.rawEvidence().stream().map(value -> value.status()).toList());
        assertThrows(IOException.class, () -> captured(client,
                () -> client.triggerCheckpoint(JOB, "b".repeat(32), Duration.ofSeconds(1))));
        assertEquals(1, client.latestCallEvidence().size());
        assertEquals(0, client.latestCallEvidence().getFirst().bytes());
        assertEquals(0, captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1))).counts().total());
        assertTrue(client.rawEvidenceUtilisation().orElseThrow().complete());
        assertEquals(4, client.rawEvidenceUtilisation().orElseThrow().captures());
    }

    @Test void captureFailureDoesNotDisableOrdinaryRestPolling() throws Exception {
        var client = client(List.of(response(200, HISTORY), response(200, HISTORY)));
        Files.writeString(temporary.toRealPath().resolve("raw/000001.json"), "reserved");
        assertThrows(IOException.class, () -> captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1))));
        assertFalse(client.rawEvidenceUtilisation().orElseThrow().complete());
        assertTrue(client.checkpointHistory(JOB, Duration.ofSeconds(1)).rawEvidence().isEmpty());
    }

    @Test void oversizedUnretainedPollStillHasATransportBoundWithoutPoisoningTheStore() throws Exception {
        String oversized = " ".repeat(org.savonitar.flink.stability.runtime.api.RawEvidenceStore.MAX_RESPONSE_BYTES + 1);
        var client = client(List.of(response(200, oversized), response(200, HISTORY)));
        IOException failure = assertThrows(IOException.class, () -> client.awaitFinished(JOB, Duration.ofSeconds(1)));
        assertTrue(failure.getMessage().contains("size limit"));
        assertTrue(client.rawEvidenceUtilisation().orElseThrow().complete());
        assertEquals(0, client.rawEvidenceUtilisation().orElseThrow().captures());
        assertEquals(1, captured(client, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1)))
                .rawEvidence().getFirst().sequence());
    }

    @FunctionalInterface private interface Call<T> { T run() throws Exception; }
    private static <T> T captured(FlinkRestApiClient client, Call<T> call) throws Exception {
        try (var capture = client.captureRawEvidence()) {
            try { return call.run(); }
            finally { capture.retain(); }
        }
    }

    private FlinkRestApiClient client(List<FlinkRestApiClient.HttpResponse> responses) throws Exception {
        return client(responses, System::nanoTime);
    }

    private FlinkRestApiClient client(List<FlinkRestApiClient.HttpResponse> responses,
            java.util.function.LongSupplier clock) throws Exception {
        var queue = new ArrayDeque<>(responses);
        var client = new FlinkRestApiClient(new FlinkRestApiClient.Transport() {
            @Override public FlinkRestApiClient.HttpResponse execute(String method, String endpoint, byte[] body, Duration timeout) {
                return queue.removeFirst();
            }
            @Override public FlinkRestApiClient.HttpResponse uploadJar(Path path, Duration timeout) { throw new UnsupportedOperationException(); }
            @Override public void close() {}
        }, new ObjectMapper(), clock, ignored -> {});
        client.enableRawEvidence(temporary.toRealPath().resolve("raw")); return client;
    }
    private static FlinkRestApiClient.HttpResponse response(int status, String body) {
        return new FlinkRestApiClient.HttpResponse(status, body.getBytes(StandardCharsets.UTF_8));
    }
}
