package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.net.SocketTimeoutException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.zip.GZIPOutputStream;

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

    @TempDir
    Path temporaryDirectory;

    @BeforeEach
    void createClient() {
        transport = new FakeTransport();
        client = new FlinkRestApiClient(transport, mapper);
    }

    @Test
    void successfulRetriesKeepEveryOriginalBodyAndImmutableSnapshotsAfterClose() throws Exception {
        String original = "NullArgumentException: input array\n" + "λ".repeat(5_000);
        AtomicInteger calls = new AtomicInteger();
        AtomicLong clock = new AtomicLong();
        List<Duration> timeouts = new ArrayList<>();
        FlinkRestApiClient recovering = timedClient((method, endpoint, body, timeout) -> {
            timeouts.add(timeout);
            int call = calls.incrementAndGet();
            return call <= 2 || !endpoint.endsWith("/checkpoints")
                    ? response(500, original) : response(200, "{\"counts\":{\"completed\":1}}");
        }, clock);
        List<FlinkScenarioControl.RestError> before = recovering.restErrors();

        assertEquals(1, recovering.awaitCompletedCheckpoints(job(), 1, Duration.ofSeconds(1)));

        List<FlinkScenarioControl.RestError> recovered = recovering.restErrors();
        assertEquals(List.of(
                new FlinkScenarioControl.RestError(1, "GET", "/jobs/" + JOB_ID + "/checkpoints", 500, original),
                new FlinkScenarioControl.RestError(2, "GET", "/jobs/" + JOB_ID + "/checkpoints", 500, original)), recovered);
        assertEquals(List.of(Duration.ofSeconds(1), Duration.ofMillis(750), Duration.ofMillis(500)), timeouts);
        assertTrue(before.isEmpty());
        assertThrows(UnsupportedOperationException.class, recovered::clear);
        IOException singleRead = assertThrows(IOException.class, () -> recovering.jobState(job()));
        assertTrue(singleRead.getMessage().contains("HTTP 500"));
        assertTrue(singleRead.getMessage().length() < original.length());
        assertEquals(2, recovered.size());
        assertEquals(3, recovering.restErrors().size());
        recovering.close();
        assertEquals(original, recovering.restErrors().getLast().body());
        assertEquals(3, recovering.restErrors().getLast().sequence());
    }

    @Test
    void repeatedServerErrorsExhaustOneBudgetIncludingCallsAndPauses() {
        AtomicLong clock = new AtomicLong();
        List<Duration> timeouts = new ArrayList<>();
        FlinkRestApiClient failing = timedClient((method, endpoint, body, timeout) -> {
            timeouts.add(timeout);
            clock.addAndGet(Duration.ofMillis(100).toNanos());
            return response(503, "same original failure");
        }, clock);

        assertThrows(FlinkRestTimeoutException.class,
                () -> failing.awaitFinished(job(), Duration.ofMillis(500)));

        assertEquals(List.of(Duration.ofMillis(500), Duration.ofMillis(150)), timeouts);
        assertEquals(Duration.ofMillis(500).toNanos(), clock.get());
        assertEquals(List.of(1L, 2L), failing.restErrors().stream()
                .map(FlinkScenarioControl.RestError::sequence).toList());
        assertTrue(failing.restErrors().stream().allMatch(error ->
                error.httpStatus() == 503 && error.body().equals("same original failure")));
    }

    @Test
    void stateWaitAndTerminalWaitCanRecoverFromServerErrors() throws Exception {
        for (boolean terminal : List.of(false, true)) {
            AtomicInteger calls = new AtomicInteger();
            FlinkRestApiClient recovering = timedClient((method, endpoint, body, timeout) ->
                    calls.incrementAndGet() == 1 ? response(502, "gateway unavailable")
                            : response(200, terminal ? "{\"state\":\"FINISHED\"}" : "{\"state\":\"RUNNING\"}"),
                    new AtomicLong());

            assertEquals(terminal ? FlinkJobState.FINISHED : FlinkJobState.RUNNING,
                    terminal ? recovering.awaitFinished(job(), Duration.ofSeconds(1))
                            : recovering.awaitState(job(), FlinkJobState.RUNNING, Duration.ofSeconds(1)));
            assertEquals(2, calls.get());
            assertEquals(1, recovering.restErrors().size());
        }
    }

    @Test
    void observationRetriesOnlyTheFailedReadWithinItsSharedDeadline() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger checkpointReads = new AtomicInteger();
        List<String> endpoints = new ArrayList<>();
        List<Duration> timeouts = new ArrayList<>();
        FlinkRestApiClient observing = timedClient((method, endpoint, body, timeout) -> {
            endpoints.add(endpoint);
            timeouts.add(timeout);
            clock.addAndGet(Duration.ofSeconds(6).toNanos());
            if (endpoint.endsWith("/checkpoints")) {
                return checkpointReads.incrementAndGet() == 1 ? response(500, "checkpoint stats failed")
                        : response(200, "{\"counts\":{\"completed\":1,\"restored\":0}}");
            }
            return response(200, endpoint.contains("/exceptions?")
                    ? "{\"exceptionHistory\":{\"entries\":[]}}"
                    : "{\"now\":1000,\"state\":\"RUNNING\",\"vertices\":[]}");
        }, clock);

        assertEquals(1, observing.observe(job()).completedCheckpoints());

        assertEquals(List.of("/jobs/" + JOB_ID, "/jobs/" + JOB_ID + "/checkpoints",
                "/jobs/" + JOB_ID + "/checkpoints", "/jobs/" + JOB_ID + "/exceptions?maxExceptions=20"), endpoints);
        assertEquals(List.of(Duration.ofSeconds(30), Duration.ofSeconds(24),
                Duration.ofMillis(17_750), Duration.ofMillis(11_750)), timeouts);
        assertEquals("checkpoint stats failed", observing.restErrors().getFirst().body());
    }

    @Test
    void postKillClockSamplingKeepsRecoveredAndUnrecoveredHttpErrors() throws Exception {
        for (int status : List.of(500, 404)) {
            AtomicInteger calls = new AtomicInteger();
            FlinkRestApiClient sampling = timedClient((method, endpoint, body, timeout) ->
                    calls.incrementAndGet() == 1 ? response(status, "clock endpoint unavailable")
                            : response(200, "{\"now\":6000}"), new AtomicLong());

            if (status == 500) {
                assertEquals(6_000, sampling.jobManagerTimeMillis(job()));
                assertEquals(2, calls.get());
            } else {
                assertThrows(IOException.class, () -> sampling.jobManagerTimeMillis(job()));
                assertEquals(1, calls.get());
            }
            assertEquals(List.of(new FlinkScenarioControl.RestError(1, "GET", "/jobs/" + JOB_ID,
                    status, "clock endpoint unavailable")), sampling.restErrors());
        }
    }

    @Test
    void clientErrorsMalformedSuccessAndGenericIoAreNotRetried() {
        for (String kind : List.of("client-error", "malformed", "io")) {
            AtomicInteger calls = new AtomicInteger();
            FlinkRestApiClient failing = timedClient((method, endpoint, body, timeout) -> {
                calls.incrementAndGet();
                if (kind.equals("io")) {
                    throw new IOException("connection failed");
                }
                return kind.equals("client-error") ? response(404, "unknown job") : response(200, "{broken");
            }, new AtomicLong());

            IOException failure = assertThrows(IOException.class,
                    () -> failing.awaitState(job(), FlinkJobState.RUNNING, Duration.ofSeconds(1)));

            assertFalse(failure instanceof FlinkRestTimeoutException);
            assertEquals(1, calls.get());
            assertEquals(kind.equals("client-error") ? 1 : 0, failing.restErrors().size());
        }
    }

    @Test
    void postSubmissionAndUploadAreNeverRetriedAndKeepTheirBodies() throws Exception {
        AtomicInteger submissions = new AtomicInteger();
        AtomicInteger uploads = new AtomicInteger();
        String original = "failed POST\n" + "body".repeat(2_000);
        FlinkRestApiClient failing = new FlinkRestApiClient(new FlinkRestApiClient.Transport() {
            @Override
            public FlinkRestApiClient.HttpResponse execute(String method, String endpoint,
                                                           byte[] body, Duration timeout) {
                assertEquals("POST", method);
                submissions.incrementAndGet();
                return response(503, original);
            }

            @Override
            public FlinkRestApiClient.HttpResponse uploadJar(Path jar, Duration timeout) {
                uploads.incrementAndGet();
                return response(503, original);
            }
        }, mapper);
        Path jar = temporaryDirectory.resolve("workload.jar");
        Files.writeString(jar, "prepared bytes");

        assertThrows(IOException.class, () -> failing.submit(submission()));
        assertThrows(IOException.class, () -> failing.uploadJar(jar, Digests.sha256(jar)));

        assertEquals(1, submissions.get());
        assertEquals(1, uploads.get());
        assertEquals(List.of("/jars/workload.jar/run", "/jars/upload"), failing.restErrors().stream()
                .map(FlinkScenarioControl.RestError::endpoint).toList());
        assertTrue(failing.restErrors().stream().allMatch(error ->
                error.method().equals("POST") && error.httpStatus() == 503 && error.body().equals(original)));
    }

    @Test
    void okhttpDoesNotReplayPostForRetryAfterZeroOrRedirects() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        AtomicInteger status = new AtomicInteger(503);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (var gzip = new GZIPOutputStream(encoded)) {
            gzip.write("original HTTP error".getBytes(StandardCharsets.UTF_8));
        }
        byte[] original = encoded.toByteArray();
        server.createContext("/", exchange -> {
            requests.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Retry-After", "0");
            exchange.getResponseHeaders().add("Location", "/redirected");
            exchange.getResponseHeaders().add("Content-Encoding", "gzip");
            exchange.sendResponseHeaders(status.get(), original.length);
            try (var output = exchange.getResponseBody()) {
                output.write(original);
            }
        });
        server.start();
        try (FlinkRestApiClient real = new FlinkRestApiClient(
                "http://127.0.0.1:" + server.getAddress().getPort())) {
            for (int code : List.of(503, 302)) {
                status.set(code);
                int before = requests.get();

                IOException failure = assertThrows(IOException.class, () -> real.submit(submission()));

                assertEquals(before + 1, requests.get(), "HTTP " + code + " must not replay POST");
                assertTrue(failure.getMessage().contains("HTTP " + code));
                assertEquals(code, real.restErrors().getLast().httpStatus());
                assertEquals("original HTTP error", real.restErrors().getLast().body());
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void successfulResponsesCannotExtendAnExpiredDeadline() {
        AtomicLong clock = new AtomicLong();
        FlinkRestApiClient late = timedClient((method, endpoint, body, timeout) -> {
            clock.addAndGet(Duration.ofSeconds(2).toNanos());
            return response(200, "{\"state\":\"FINISHED\"}");
        }, clock);

        assertThrows(FlinkRestTimeoutException.class,
                () -> late.awaitFinished(job(), Duration.ofSeconds(1)));
        assertTrue(late.restErrors().isEmpty());
    }

    @Test
    void interruptionDuringRetryRestoresTheFlagWithoutDiscardingTheError() {
        AtomicInteger calls = new AtomicInteger();
        FlinkRestApiClient interrupted = new FlinkRestApiClient((method, endpoint, body, timeout) -> {
            calls.incrementAndGet();
            return response(500, "retained before interrupt");
        }, mapper, () -> 0L, duration -> { throw new InterruptedException("stop retry"); });
        boolean originallyInterrupted = Thread.interrupted();
        try {
            assertThrows(IOException.class,
                    () -> interrupted.awaitFinished(job(), Duration.ofSeconds(1)));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, calls.get());
            assertEquals("retained before interrupt", interrupted.restErrors().getFirst().body());
        } finally {
            Thread.interrupted();
            if (originallyInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void responseBytesAndRetainedErrorsRemainImmutableEvenWhenCloseFails() {
        byte[] bytes = "original".getBytes(StandardCharsets.UTF_8);
        var response = new FlinkRestApiClient.HttpResponse(500, bytes);
        bytes[0] = 'x';
        response.body()[0] = 'y';
        FlinkRestApiClient failing = new FlinkRestApiClient(new FlinkRestApiClient.Transport() {
            @Override
            public FlinkRestApiClient.HttpResponse execute(String method, String endpoint,
                                                           byte[] body, Duration timeout) {
                return response;
            }

            @Override
            public void close() {
                throw new IllegalStateException("close failed");
            }
        }, mapper);

        assertThrows(IOException.class, () -> failing.jobState(job()));
        List<FlinkScenarioControl.RestError> retained = failing.restErrors();
        assertThrows(IllegalStateException.class, failing::close);

        assertEquals("original", retained.getFirst().body());
        assertEquals(retained, failing.restErrors());
        assertThrows(UnsupportedOperationException.class, retained::clear);
    }

    private FlinkRestApiClient timedClient(FlinkRestApiClient.Transport source, AtomicLong clock) {
        return new FlinkRestApiClient(source, mapper, clock::get,
                duration -> clock.addAndGet(duration.toNanos()));
    }

    private static FlinkRestApiClient.HttpResponse response(int status, String body) {
        return new FlinkRestApiClient.HttpResponse(status, body.getBytes(StandardCharsets.UTF_8));
    }

    private static FlinkJobHandle job() {
        return new FlinkJobHandle(JOB_ID);
    }

    private static FlinkJobSubmission submission() {
        return new FlinkJobSubmission("workload.jar", 1, Map.of(), List.of());
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

        String jarId = client.uploadJar(jar, Digests.sha256(jar));

        assertEquals("uploaded-workload.jar", jarId);
        assertFalse(jar.toAbsolutePath().normalize().equals(uploadedJar));
        assertEquals("prepared bytes", new String(uploadedJarBytes, StandardCharsets.UTF_8));
        assertFalse(Files.exists(uploadedJar));
    }

    @Test
    void refusesChangedPreparedWorkloadBytesBeforeTransportUpload() throws Exception {
        Path jar = Files.createTempFile("flink-workload-", ".jar");
        Files.writeString(jar, "original bytes", StandardCharsets.UTF_8);
        String expectedSha256 = Digests.sha256(jar);
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
        String expectedSha256 = Digests.sha256(jar);
        AtomicReference<byte[]> uploaded = new AtomicReference<>();
        FlinkRestApiClient snapshotClient = new FlinkRestApiClient(
                new FlinkRestApiClient.Transport() {
                    @Override
                    public FlinkRestApiClient.HttpResponse execute(
                            String method,
                            String endpoint,
                            byte[] body,
                            Duration timeout) throws IOException {
                        throw new IOException("Unexpected REST request");
                    }

                    @Override
                    public FlinkRestApiClient.HttpResponse uploadJar(Path snapshot, Duration timeout) throws IOException {
                        Files.writeString(jar, "later mutation", StandardCharsets.UTF_8);
                        uploaded.set(Files.readAllBytes(snapshot));
                        return response(200, "{\"filename\":\"/tmp/flink-web/uploaded-workload.jar\"}");
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
                    return response(200, response);
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
                        response(200, "{\"state\":\"FINISHED\"}"),
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

    @Test
    void samplesTheJobManagerClockWithOneBoundedJobDetailsRead() throws Exception {
        List<String> endpoints = new ArrayList<>();
        FlinkRestApiClient sampling = new FlinkRestApiClient((method, path, body, timeout) -> {
            endpoints.add(method + " " + path);
            assertTrue(timeout.compareTo(Duration.ZERO) > 0);
            assertTrue(timeout.compareTo(Duration.ofSeconds(30)) <= 0);
            // Clock sampling must not depend on checkpoint, exception, or vertex responses.
            return response(200, "{\"now\":6000}");
        }, mapper);

        assertEquals(6_000, sampling.jobManagerTimeMillis(new FlinkJobHandle(JOB_ID)));
        assertEquals(List.of("GET /jobs/" + JOB_ID), endpoints);
    }

    @Test
    void aClockSampleWithoutJobManagerTimeIsRejected() {
        FlinkRestApiClient sampling = new FlinkRestApiClient(cannedTransport(
                Map.of("/jobs/" + JOB_ID, "{\"state\":\"RUNNING\"}")), mapper);

        IOException failure = assertThrows(IOException.class,
                () -> sampling.jobManagerTimeMillis(new FlinkJobHandle(JOB_ID)));

        assertTrue(failure.getMessage().contains("now"), failure.getMessage());
    }

    private static FlinkRestApiClient.Transport cannedTransport(Map<String, String> responses) {
        return (method, path, body, timeout) -> {
            String response = responses.get(path);
            if (!method.equals("GET") || response == null) {
                throw new IOException("Unexpected fake request: " + method + " " + path);
            }
            return response(200, response);
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

    private record CapturedRequest(String method, String path, String body) {}

    private final class FakeTransport implements FlinkRestApiClient.Transport {
        @Override
        public FlinkRestApiClient.HttpResponse uploadJar(Path jar, Duration timeout) throws IOException {
            uploadedJar = jar;
            uploadedJarBytes = Files.readAllBytes(jar);
            return response(200, "{\"filename\":\"/tmp/flink-web/uploaded-workload.jar\"}");
        }

        @Override
        public FlinkRestApiClient.HttpResponse execute(
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
            return response(200, response);
        }
    }
}
