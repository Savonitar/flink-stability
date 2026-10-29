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
        var first = client.checkpointHistory(JOB, Duration.ofSeconds(1));
        var second = client.checkpointHistory(JOB, Duration.ofSeconds(1));
        assertEquals(first.counts(), second.counts());
        var a = first.rawEvidence().getFirst(); var b = second.rawEvidence().getFirst();
        assertNotEquals(a.group(), b.group()); assertNotEquals(a.path(), b.path());
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
        var observed = client.observe(JOB, Duration.ofSeconds(1));
        assertEquals(4, observed.rawEvidence().size());
        assertEquals(1, observed.rawEvidence().stream().map(value -> value.group()).distinct().count());
        assertEquals(42, observed.jobManagerTimeMillis()); assertEquals(1, observed.subtasks().getFirst().attempt());
        assertEquals(43, client.jobManagerTimeMillis(JOB, Duration.ofSeconds(1)));
        assertEquals("job-manager-clock-2", client.latestCallEvidence().getFirst().group());
        assertEquals(4, observed.rawEvidence().size());
    }

    @Test void retainsRetryErrorAndDoesNotReplayFailedPost() throws Exception {
        var client = client(List.of(response(503, "{\"error\":\"later\"}"), response(200, HISTORY), response(503, "{\"error\":\"unknown submission\"}")));
        var history = client.checkpointHistory(JOB, Duration.ofSeconds(1));
        assertEquals(List.of(503, 200), history.rawEvidence().stream().map(value -> value.status()).toList());
        assertThrows(IOException.class, () -> client.triggerCheckpoint(JOB, "b".repeat(32), Duration.ofSeconds(1)));
        assertEquals(1, client.latestCallEvidence().size());
        assertEquals("POST", client.latestCallEvidence().getFirst().method());
        assertEquals(503, client.latestCallEvidence().getFirst().status());
    }

    @Test void captureFailureOrMalformedResponseNeverReturnsTypedSuccess() throws Exception {
        var client = client(List.of(response(200, HISTORY)));
        Files.writeString(temporary.toRealPath().resolve("raw/000001.json"), "reserved");
        assertThrows(IOException.class, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1)));
        assertEquals("reserved", Files.readString(temporary.toRealPath().resolve("raw/000001.json")));
    }

    @Test void overviewRetainsOriginalBodyBeforeDerivedDetailsAreAdded() throws Exception {
        String overview = "{\"latest\":{\"completed\":{\"id\":1,\"status\":\"COMPLETED\",\"latest_ack_timestamp\":10}},"
                + "\"history\":[{\"id\":2,\"status\":\"IN_PROGRESS\",\"trigger_timestamp\":20}]}";
        var client = client(List.of(response(200, overview), response(200, "{\"id\":2,\"status\":\"IN_PROGRESS\"}"),
                response(200, "{\"vertices\":[]}")));
        var typed = client.checkpointOverview(JOB, Duration.ofSeconds(1));
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
        assertThrows(IOException.class, () -> client.checkpointHistory(JOB, Duration.ofSeconds(1)));
        assertEquals(malformed, Files.readString(Path.of(client.latestCallEvidence().getFirst().path())));
    }

    private FlinkRestApiClient client(List<FlinkRestApiClient.HttpResponse> responses) throws Exception {
        var queue = new ArrayDeque<>(responses);
        var client = new FlinkRestApiClient(new FlinkRestApiClient.Transport() {
            @Override public FlinkRestApiClient.HttpResponse execute(String method, String endpoint, byte[] body, Duration timeout) {
                return queue.removeFirst();
            }
            @Override public FlinkRestApiClient.HttpResponse uploadJar(Path path, Duration timeout) { throw new UnsupportedOperationException(); }
            @Override public void close() {}
        }, new ObjectMapper(), System::nanoTime, ignored -> {});
        client.enableRawEvidence(temporary.toRealPath().resolve("raw")); return client;
    }
    private static FlinkRestApiClient.HttpResponse response(int status, String body) {
        return new FlinkRestApiClient.HttpResponse(status, body.getBytes(StandardCharsets.UTF_8));
    }
}
