package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Retention and explicit deadline-bound retry policy for the Flink REST boundary. */
class FlinkRestApiClientRestEvidenceTest {
    private static final String JOB_ID = "0123456789abcdef0123456789abcdef";

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path temporaryDirectory;

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

}
