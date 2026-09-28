package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerRequestBody;
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
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class FlinkCheckpointTriggerTest {
    private static final FlinkJobHandle JOB = new FlinkJobHandle("a".repeat(32));
    private static final String TRIGGER = "1".repeat(32);
    private static final String PATH = "/jobs/" + JOB.jobId() + "/checkpoints";
    private static final Duration BUDGET = Duration.ofMillis(123);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void usesTheReleasedAsyncCheckpointRoutesFieldsAndExactTriggerAcrossLeaderRouting() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger discoveries = new AtomicInteger();
        AtomicReference<String> problem = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(PATH, exchange -> {
            int call = calls.incrementAndGet();
            String expectedMethod = call == 1 ? "POST" : "GET";
            String expectedPath = call == 1 ? PATH : PATH + "/" + TRIGGER;
            if (!expectedMethod.equals(exchange.getRequestMethod())
                    || !expectedPath.equals(exchange.getRequestURI().getPath())) {
                problem.set("Unexpected method or exact-trigger route");
            }
            if (call == 1) {
                byte[] requestBytes = exchange.getRequestBody().readAllBytes();
                try {
                    // Parse our wire bytes with the released Flink 2.2 REST contract.
                    for (var mapper : List.of(RestMapperUtils.getStrictObjectMapper(),
                            RestMapperUtils.getFlexibleObjectMapper())) {
                        var body = mapper.readValue(requestBytes, CheckpointTriggerRequestBody.class);
                        if (!TRIGGER.equals(body.getTriggerId().orElseThrow().toString())
                                || body.getCheckpointType() != CheckpointType.CONFIGURED) {
                            problem.set("Released checkpoint parser did not preserve trigger ID or configured default");
                        }
                    }
                } catch (IOException | RuntimeException failure) {
                    problem.set("Released checkpoint parser rejected the client request: " + failure);
                }
            }
            String body = switch (call) {
                case 1 -> "{\"request-id\":\"" + TRIGGER + "\"}";
                case 2 -> "{\"status\":{\"id\":\"IN_PROGRESS\"},\"operation\":null}";
                default -> "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"checkpointId\":42,\"failureCause\":null}}";
            };
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(call == 1 ? 202 : 200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try (FlinkRestApiClient client = new FlinkRestApiClient(url(server))) {
            client.useEndpointResolver(timeout -> {
                assertTrue(timeout.isPositive());
                assertTrue(timeout.compareTo(Duration.ofSeconds(2)) <= 0);
                discoveries.incrementAndGet();
                return url(server);
            });
            assertEquals(TRIGGER, client.triggerCheckpoint(JOB, TRIGGER, Duration.ofSeconds(2)));
            var pending = client.checkpointStatus(JOB, TRIGGER, Duration.ofSeconds(2));
            assertEquals(FlinkCheckpointTrigger.State.IN_PROGRESS, pending.state());
            assertTrue(pending.checkpointId().isEmpty());
            var completed = client.checkpointStatus(JOB, TRIGGER, Duration.ofSeconds(2));
            assertEquals(FlinkCheckpointTrigger.State.COMPLETED, completed.state());
            assertEquals(OptionalLong.of(42), completed.checkpointId());
            assertTrue(completed.failure().isEmpty());
            assertEquals(3, calls.get());
            assertEquals(3, discoveries.get());
            assertEquals(null, problem.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void releasedCheckpointParsersRejectTheDefaultAliasUsedByTheOriginalClient() {
        byte[] originalBody = ("{\"triggerId\":\"" + TRIGGER
                + "\",\"checkpointType\":\"DEFAULT\"}").getBytes(StandardCharsets.UTF_8);
        for (var mapper : List.of(RestMapperUtils.getStrictObjectMapper(),
                RestMapperUtils.getFlexibleObjectMapper())) {
            assertThrows(IOException.class,
                    () -> mapper.readValue(originalBody, CheckpointTriggerRequestBody.class));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {307, 400, 408, 503})
    void failedCheckpointPostPreservesTheOriginalErrorAndIsNeverReplayed(int status) throws Exception {
        AtomicInteger calls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("Retry-After", "0");
            exchange.getResponseHeaders().set("Location", url(server) + "/redirected-checkpoint");
            byte[] body = "original checkpoint outcome unknown".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try (FlinkRestApiClient client = new FlinkRestApiClient(url(server))) {
            IOException failure = assertThrows(IOException.class,
                    () -> client.triggerCheckpoint(JOB, TRIGGER, Duration.ofSeconds(2)));
            assertTrue(failure.getMessage().contains("HTTP " + status));
            assertTrue(failure.getMessage().contains("original checkpoint outcome unknown"));
            assertEquals(1, calls.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aLostSubmissionAcknowledgementCannotCauseAnotherPost() {
        AtomicInteger calls = new AtomicInteger();
        try (FlinkRestApiClient client = new FlinkRestApiClient((method, path, body, timeout) -> {
            calls.incrementAndGet();
            throw new IOException("connection lost after request was sent");
        }, MAPPER)) {
            IOException failure = assertThrows(IOException.class,
                    () -> client.triggerCheckpoint(JOB, TRIGGER, BUDGET));
            assertTrue(failure.getMessage().contains("connection lost"));
            assertEquals(1, calls.get());
        }
    }

    @Test
    void acknowledgementMustBeTheExactRequestedTextualTriggerId() {
        for (String response : List.of("{}", "{\"request-id\":\"" + "2".repeat(32) + "\"}",
                "{\"request-id\":" + TRIGGER + "}")) {
            AtomicInteger calls = new AtomicInteger();
            try (FlinkRestApiClient client = new FlinkRestApiClient((method, path, body, timeout) -> {
                calls.incrementAndGet();
                return new FlinkRestApiClient.HttpResponse(200, response.getBytes(StandardCharsets.UTF_8));
            }, MAPPER)) {
                assertThrows(IOException.class, () -> client.triggerCheckpoint(JOB, TRIGGER, BUDGET));
                assertEquals(1, calls.get());
            }
        }
    }

    @Test
    void failedCheckpointRetainsItsFailureAndMalformedStatusesCannotClaimCompletion() throws Exception {
        String failed = "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"checkpointId\":null,"
                + "\"failureCause\":{\"class\":\"CheckpointException\",\"message\":\"declined\"}}}";
        try (FlinkRestApiClient client = response(failed)) {
            var observation = client.checkpointStatus(JOB, TRIGGER, BUDGET);
            assertEquals(FlinkCheckpointTrigger.State.COMPLETED, observation.state());
            assertTrue(observation.checkpointId().isEmpty());
            assertTrue(observation.failure().orElseThrow().contains("declined"));
        }
        for (String invalid : List.of("{}", "{\"status\":{\"id\":\"UNKNOWN\"}}",
                "{\"status\":{\"id\":\"IN_PROGRESS\"},\"operation\":{\"checkpointId\":42}}",
                "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{}}",
                "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"checkpointId\":0}}",
                "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"checkpointId\":\"42\"}}",
                "{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"checkpointId\":42,\"failureCause\":{}}}")) {
            try (FlinkRestApiClient client = response(invalid)) {
                assertThrows(IOException.class, () -> client.checkpointStatus(JOB, TRIGGER, BUDGET), invalid);
            }
        }
    }

    @Test
    void callerBudgetIsPassedWithoutDefaultTimeoutAndInvalidArgumentsMakeNoRequests() throws Exception {
        List<Duration> budgets = new ArrayList<>();
        try (FlinkRestApiClient client = new FlinkRestApiClient((method, path, body, timeout) -> {
            budgets.add(timeout);
            return new FlinkRestApiClient.HttpResponse(200, (method.equals("POST")
                    ? "{\"request-id\":\"" + TRIGGER + "\"}"
                    : "{\"status\":{\"id\":\"IN_PROGRESS\"}}").getBytes(StandardCharsets.UTF_8));
        }, MAPPER, () -> 0L)) {
            client.triggerCheckpoint(JOB, TRIGGER, BUDGET);
            client.checkpointStatus(JOB, TRIGGER, BUDGET);
            assertEquals(List.of(BUDGET, BUDGET), budgets);
            for (Duration invalid : List.of(Duration.ZERO, Duration.ofNanos(-1))) {
                assertThrows(IllegalArgumentException.class, () -> client.triggerCheckpoint(JOB, TRIGGER, invalid));
                assertThrows(IllegalArgumentException.class, () -> client.checkpointStatus(JOB, TRIGGER, invalid));
            }
            assertThrows(IllegalArgumentException.class, () -> client.triggerCheckpoint(JOB, "../other", BUDGET));
            assertThrows(IllegalArgumentException.class, () -> client.checkpointStatus(new FlinkJobHandle("bad/id"), TRIGGER, BUDGET));
            assertEquals(2, budgets.size());
        }
    }

    private static FlinkRestApiClient response(String json) {
        return new FlinkRestApiClient((method, path, body, timeout) ->
                new FlinkRestApiClient.HttpResponse(200, json.getBytes(StandardCharsets.UTF_8)), MAPPER);
    }

    private static String url(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }
}
