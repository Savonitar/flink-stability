package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkRestApiClientTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
    private final AtomicReference<String> jobState = new AtomicReference<>("FINISHED");
    private final AtomicLong completedCheckpoints = new AtomicLong(0);
    private Path uploadedJar;
    private byte[] uploadedJarBytes;
    private FakeTransport transport;
    private FlinkRestApiClient client;

    @BeforeEach
    void createClient() {
        transport = new FakeTransport();
        client = new FlinkRestApiClient(transport, mapper);
    }

    @Test
    void submitsStructuredArgumentsAndConfigurationWithoutStringSplitting()
            throws Exception {
        Map<String, String> configuration = new LinkedHashMap<>();
        configuration.put("z-last", "last");
        configuration.put("a-first", "first");
        List<String> arguments = List.of(
                "--processingDelayMs", "0", "one value with spaces", "--literal=a b");
        FlinkJobSubmission submission = new FlinkJobSubmission(
                "workload.jar",
                3,
                configuration,
                arguments);

        FlinkJobHandle handle = client.submit(submission);

        assertEquals("0123456789abcdef0123456789abcdef", handle.jobId());
        CapturedRequest request = only("POST", "/jars/workload.jar/run");
        JsonNode body = mapper.readTree(request.body());
        assertEquals(3, body.path("parallelism").asInt());
        assertEquals(arguments, strings(body.path("programArgsList")));
        assertEquals("first", body.path("flinkConfiguration").path("a-first").asText());
        assertEquals("last", body.path("flinkConfiguration").path("z-last").asText());
        assertFalse(body.has("jobId"));
    }

    @Test
    void uploadsTheExactPreparedJarAndReturnsTheFlinkJarId() throws Exception {
        Path jar = Files.createTempFile("flink-workload-", ".jar");
        Files.writeString(jar, "prepared bytes", StandardCharsets.UTF_8);

        String jarId = client.uploadJar(jar, sha256(jar));

        assertEquals("uploaded-workload.jar", jarId);
        assertFalse(jar.toAbsolutePath().normalize().equals(uploadedJar));
        assertEquals("prepared bytes", new String(uploadedJarBytes, StandardCharsets.UTF_8));
        assertFalse(Files.exists(uploadedJar));
    }

    @Test
    void refusesChangedPreparedWorkloadBytesBeforeTransportUpload() throws Exception {
        Path jar = Files.createTempFile("flink-workload-", ".jar");
        Files.writeString(jar, "original bytes", StandardCharsets.UTF_8);
        String expectedSha256 = sha256(jar);
        Files.writeString(jar, "changed bytes", StandardCharsets.UTF_8);

        IOException failure = assertThrows(
                IOException.class,
                () -> client.uploadJar(jar, expectedSha256));

        assertTrue(failure.getMessage().contains("changed before upload"));
        assertNull(uploadedJar);
    }

    @Test
    void uploadsTheVerifiedSnapshotWhenThePreparedPathChangesBeforeTransportReads()
            throws Exception {
        Path jar = Files.createTempFile("flink-workload-", ".jar");
        Files.writeString(jar, "verified bytes", StandardCharsets.UTF_8);
        String expectedSha256 = sha256(jar);
        AtomicReference<byte[]> uploaded = new AtomicReference<>();
        FlinkRestApiClient snapshotClient = new FlinkRestApiClient(
                new FlinkRestApiClient.Transport() {
                    @Override
                    public byte[] execute(
                            String method,
                            String endpoint,
                            byte[] body,
                            Duration timeout) throws IOException {
                        throw new IOException("Unexpected REST request");
                    }

                    @Override
                    public byte[] uploadJar(Path snapshot, Duration timeout) throws IOException {
                        Files.writeString(jar, "later mutation", StandardCharsets.UTF_8);
                        uploaded.set(Files.readAllBytes(snapshot));
                        return "{\"filename\":\"/tmp/flink-web/uploaded-workload.jar\"}"
                                .getBytes(StandardCharsets.UTF_8);
                    }
                },
                mapper);

        assertEquals("uploaded-workload.jar", snapshotClient.uploadJar(jar, expectedSha256));
        assertEquals("verified bytes", new String(uploaded.get(), StandardCharsets.UTF_8));
        assertEquals("later mutation", Files.readString(jar, StandardCharsets.UTF_8));
    }

    @Test
    void waitsForRunningAndCompletedCheckpointsWithTypedResponses() throws Exception {
        jobState.set("RUNNING");
        completedCheckpoints.set(3);
        FlinkJobHandle job = new FlinkJobHandle(
                "0123456789abcdef0123456789abcdef");

        assertEquals(
                FlinkJobState.RUNNING,
                client.awaitState(job, FlinkJobState.RUNNING, Duration.ofSeconds(1)));
        assertEquals(3, client.awaitCompletedCheckpoints(job, 2, Duration.ofSeconds(1)));
    }

    @Test
    void checkpointWaitFailsIfTheJobTerminatesBeforeTheRequiredCount() {
        jobState.set("FAILED");
        completedCheckpoints.set(1);

        IOException failure = assertThrows(IOException.class, () ->
                client.awaitCompletedCheckpoints(
                        new FlinkJobHandle("0123456789abcdef0123456789abcdef"),
                        2,
                        Duration.ofSeconds(1)));

        assertTrue(failure.getMessage().contains("after only 1 completed checkpoints"));
    }

    @Test
    void refreshesCheckpointCountOnceWhenTheJobBecomesTerminal() throws Exception {
        AtomicInteger checkpointRequests = new AtomicInteger();
        FlinkRestApiClient timingClient = new FlinkRestApiClient(
                (method, path, body, timeout) -> {
                    String response;
                    if (path.endsWith("/checkpoints")) {
                        int request = checkpointRequests.incrementAndGet();
                        response = "{\"counts\":{\"completed\":"
                                + (request == 1 ? 0 : 1) + "}}";
                    } else {
                        response = "{\"state\":\"FINISHED\"}";
                    }
                    return response.getBytes(StandardCharsets.UTF_8);
                },
                mapper);

        assertEquals(
                1,
                timingClient.awaitCompletedCheckpoints(
                        new FlinkJobHandle("0123456789abcdef0123456789abcdef"),
                        1,
                        Duration.ofSeconds(1)));
        assertEquals(2, checkpointRequests.get());
    }

    @Test
    void transportTimeoutInsideAnOperationDeadlineIsTypedAsDeadlineExpiry() {
        FlinkRestApiClient timingClient = new FlinkRestApiClient(
                (method, path, body, timeout) -> {
                    throw new SocketTimeoutException("simulated socket timeout");
                },
                mapper);

        FlinkRestTimeoutException failure = assertThrows(
                FlinkRestTimeoutException.class,
                () -> timingClient.awaitFinished(
                        new FlinkJobHandle("0123456789abcdef0123456789abcdef"),
                        Duration.ofSeconds(1)));

        assertTrue(failure.getCause() instanceof SocketTimeoutException);
        assertTrue(failure.getMessage().contains("Timed out"));
    }

    @Test
    void negativeNanoTimeOriginDoesNotExpireAHealthyRestWait() throws Exception {
        AtomicLong clock = new AtomicLong(-1_000_000L);
        FlinkRestApiClient timingClient = new FlinkRestApiClient(
                (method, path, body, timeout) ->
                        "{\"state\":\"FINISHED\"}".getBytes(StandardCharsets.UTF_8),
                mapper,
                clock::get);

        assertEquals(
                FlinkJobState.FINISHED,
                timingClient.awaitFinished(
                        new FlinkJobHandle("0123456789abcdef0123456789abcdef"),
                        Duration.ofSeconds(1)));
    }

    private CapturedRequest only(String method, String path) {
        List<CapturedRequest> matches = requests.stream()
                .filter(request -> request.method().equals(method) && request.path().equals(path))
                .toList();
        assertEquals(1, matches.size(), method + " " + path);
        return matches.getFirst();
    }

    private static List<String> strings(JsonNode array) {
        List<String> values = new ArrayList<>();
        array.forEach(value -> values.add(value.asText()));
        return values;
    }

    private static String sha256(Path path) throws IOException {
        try {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private record CapturedRequest(String method, String path, String body) {}

    private final class FakeTransport implements FlinkRestApiClient.Transport {
        @Override
        public byte[] uploadJar(Path jar, Duration timeout) throws IOException {
            uploadedJar = jar;
            uploadedJarBytes = Files.readAllBytes(jar);
            return "{\"filename\":\"/tmp/flink-web/uploaded-workload.jar\"}"
                    .getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] execute(
                String method,
                String path,
                byte[] body,
                Duration timeout) throws IOException {
            requests.add(new CapturedRequest(
                    method,
                    path,
                    body == null ? "" : new String(body, StandardCharsets.UTF_8)));
            String response;
            if (method.equals("POST") && path.equals("/jars/workload.jar/run")) {
                response = "{\"jobid\":\"0123456789abcdef0123456789abcdef\"}";
            } else if (method.equals("GET")
                    && path.equals("/jobs/0123456789abcdef0123456789abcdef")) {
                response = "{\"state\":\"" + jobState.get() + "\"}";
            } else if (method.equals("GET")
                    && path.equals(
                            "/jobs/0123456789abcdef0123456789abcdef/checkpoints")) {
                response = "{\"counts\":{\"completed\":"
                        + completedCheckpoints.get() + "}}";
            } else {
                throw new IOException("Unexpected fake request: " + method + " " + path);
            }
            return response.getBytes(StandardCharsets.UTF_8);
        }
    }
}
