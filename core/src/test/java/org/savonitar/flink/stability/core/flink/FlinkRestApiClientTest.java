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
import java.util.Optional;
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
    private static final String JOB_ID = "0123456789abcdef0123456789abcdef";

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

    @Test
    void observesRecoveryEvidenceFromFlink22RestShapes() throws Exception {
        // Field names and shapes recorded from a Flink 2.2.0 JobManager after a TaskManager kill.
        Map<String, String> responses = Map.of(
                "/jobs/" + JOB_ID, """
                        {"jid":"%s","state":"RUNNING","now":1790275134494,
                         "vertices":[{"id":"v1","name":"Source: Kafka Source -> Source Throttle"},
                                     {"id":"v2","name":"Managed State Pass-Through"}]}
                        """.formatted(JOB_ID),
                "/jobs/" + JOB_ID + "/checkpoints", """
                        {"counts":{"restored":1,"total":22,"in_progress":0,"completed":6,
                                   "failed":16},
                         "latest":{"restored":{"id":4,"restore_timestamp":1790275131762,
                                   "is_savepoint":false}}}
                        """,
                "/jobs/" + JOB_ID + "/exceptions?maxExceptions=20", """
                        {"exceptionHistory":{"entries":[{
                           "exceptionName":"org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException",
                           "stacktrace":"org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException: TaskManager with id tm-old is no longer reachable.\\n\\tat org.apache.flink.X.y(X.java:1)\\n",
                           "timestamp":1790275130739,
                           "taskName":"Managed State Pass-Through (1/1) - execution #0",
                           "taskManagerId":"tm-old",
                           "failureLabels":{},"concurrentExceptions":[]}],
                         "truncated":false}}
                        """,
                "/jobs/" + JOB_ID + "/vertices/v1", """
                        {"subtasks":[{"subtask":0,"attempt":1,"status":"RUNNING",
                                      "taskmanager-id":"tm-new"}]}
                        """,
                "/jobs/" + JOB_ID + "/vertices/v2", """
                        {"subtasks":[{"subtask":0,"attempt":1,"status":"RUNNING",
                                      "taskmanager-id":"tm-new"}]}
                        """);
        FlinkRestApiClient observing = new FlinkRestApiClient(
                cannedTransport(responses), mapper);

        FlinkJobObservation observed = observing.observe(new FlinkJobHandle(JOB_ID));

        assertEquals(1790275134494L, observed.jobManagerTimeMillis());
        assertEquals(FlinkJobState.RUNNING, observed.state());
        assertEquals(6, observed.completedCheckpoints());
        assertEquals(1, observed.restoredCheckpoints());
        assertEquals(Optional.of(new FlinkJobObservation.Restore(4, 1790275131762L)),
                observed.latestRestore());
        assertEquals(List.of(new FlinkJobObservation.Failure(
                        1790275130739L,
                        "org.apache.flink.runtime.resourcemanager.exceptions."
                                + "ResourceManagerException",
                        "org.apache.flink.runtime.resourcemanager.exceptions."
                                + "ResourceManagerException: TaskManager with id tm-old is no"
                                + " longer reachable.",
                        Optional.of("tm-old"))),
                observed.failures());
        assertEquals(List.of(
                        new FlinkJobObservation.Subtask("Source: Kafka Source -> Source Throttle",
                                0, 1, "RUNNING", Optional.of("tm-new")),
                        new FlinkJobObservation.Subtask("Managed State Pass-Through",
                                0, 1, "RUNNING", Optional.of("tm-new"))),
                observed.subtasks());
        assertEquals(2, observed.activeSubtasks().size());
    }

    @Test
    void observationKeepsTheInnermostCauseAndUnassignedSubtasks() throws Exception {
        Map<String, String> responses = Map.of(
                "/jobs/" + JOB_ID, """
                        {"state":"RESTARTING","now":2000,"vertices":[{"id":"v1","name":"Src"}]}
                        """,
                "/jobs/" + JOB_ID + "/checkpoints", """
                        {"counts":{"restored":0,"completed":1},"latest":{"restored":null}}
                        """,
                "/jobs/" + JOB_ID + "/exceptions?maxExceptions=20", """
                        {"exceptionHistory":{"entries":[{"exceptionName":"FlinkRuntimeException",
                         "timestamp":1500,
                         "stacktrace":"FlinkRuntimeException: tolerable failure threshold\\n\\tat a\\nCaused by: CheckpointException: async failed\\n\\tat b\\nCaused by: java.io.IOException: Size of the state is larger than the maximum permitted memory-backed state.\\n\\tat c\\n"}],
                         "truncated":false}}
                        """,
                "/jobs/" + JOB_ID + "/vertices/v1", """
                        {"subtasks":[{"subtask":0,"attempt":0,"status":"SCHEDULED",
                                      "taskmanager-id":"(unassigned)"}]}
                        """);

        FlinkJobObservation observed = new FlinkRestApiClient(cannedTransport(responses), mapper)
                .observe(new FlinkJobHandle(JOB_ID));

        assertEquals(FlinkJobState.RESTARTING, observed.state());
        assertEquals(Optional.empty(), observed.latestRestore());
        assertEquals("java.io.IOException: Size of the state is larger than the maximum"
                        + " permitted memory-backed state.",
                observed.failures().getFirst().rootCause());
        assertEquals(Optional.empty(), observed.failures().getFirst().taskManagerId());
        assertEquals(Optional.empty(), observed.subtasks().getFirst().taskManagerId());
        assertTrue(observed.activeSubtasks().isEmpty());
    }

    @Test
    void observationWithoutJobManagerTimeIsRejected() {
        Map<String, String> responses = Map.of(
                "/jobs/" + JOB_ID, "{\"state\":\"RUNNING\",\"vertices\":[]}",
                "/jobs/" + JOB_ID + "/checkpoints", "{\"counts\":{\"restored\":0,\"completed\":0}}",
                "/jobs/" + JOB_ID + "/exceptions?maxExceptions=20",
                "{\"exceptionHistory\":{\"entries\":[]}}");

        IOException failure = assertThrows(IOException.class,
                () -> new FlinkRestApiClient(cannedTransport(responses), mapper)
                        .observe(new FlinkJobHandle(JOB_ID)));

        assertTrue(failure.getMessage().contains("now"), failure.getMessage());
    }

    private static FlinkRestApiClient.Transport cannedTransport(Map<String, String> responses) {
        return (method, path, body, timeout) -> {
            String response = responses.get(path);
            if (!method.equals("GET") || response == null) {
                throw new IOException("Unexpected fake request: " + method + " " + path);
            }
            return response.getBytes(StandardCharsets.UTF_8);
        };
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
