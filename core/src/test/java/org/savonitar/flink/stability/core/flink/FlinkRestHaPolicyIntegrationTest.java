package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The shared REST policy must also preserve HA routing and checkpoint mutation semantics. */
class FlinkRestHaPolicyIntegrationTest {
    private static final FlinkJobHandle JOB = new FlinkJobHandle("0123456789abcdef0123456789abcdef");
    private static final String TRIGGER = "fedcba9876543210fedcba9876543210";

    @Test
    void boundedGetRetryResolvesTheNewLeaderAndRetainsTheFirstLeadersFullError() throws Exception {
        HttpServer original = server();
        HttpServer replacement = server();
        AtomicInteger originalCalls = new AtomicInteger();
        AtomicInteger replacementCalls = new AtomicInteger();
        String body = "original leader unavailable\n" + "λ".repeat(5_000);
        original.createContext("/", exchange -> {
            originalCalls.incrementAndGet();
            exchange.getResponseHeaders().set("Retry-After", "0");
            reply(exchange, 503, body);
        });
        replacement.createContext("/", exchange -> {
            replacementCalls.incrementAndGet();
            reply(exchange, 200, "{\"state\":\"RUNNING\"}");
        });
        original.start();
        replacement.start();
        try (var client = new FlinkRestApiClient(url(original))) {
            List<Duration> budgets = new ArrayList<>();
            client.useEndpointResolver(remaining -> {
                budgets.add(remaining);
                return budgets.size() == 1 ? url(original) : url(replacement);
            });

            assertEquals(FlinkJobState.RUNNING,
                    client.awaitState(JOB, FlinkJobState.RUNNING, Duration.ofSeconds(5)));

            assertEquals(1, originalCalls.get());
            assertEquals(1, replacementCalls.get());
            assertEquals(2, budgets.size());
            assertTrue(budgets.getFirst().compareTo(Duration.ofSeconds(5)) <= 0);
            assertTrue(budgets.getLast().isPositive());
            assertTrue(budgets.getLast().compareTo(budgets.getFirst()) < 0,
                    "leader rediscovery and retry must consume the original operation budget");
            assertEquals(List.of(new FlinkScenarioControl.RestError(
                    1, "GET", "/jobs/" + JOB.jobId(), 503, body)), client.restErrors());
        } finally {
            original.stop(0);
            replacement.stop(0);
        }
    }

    @Test
    void checkpointPost503WithRetryAfterZeroIsNotReplayedAndKeepsTheCompleteBody() throws Exception {
        HttpServer server = server();
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger discoveries = new AtomicInteger();
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<JsonNode> request = new AtomicReference<>();
        String body = "checkpoint acknowledgement unknown\n" + "λ".repeat(5_000);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().getPath());
            request.set(new ObjectMapper().readTree(exchange.getRequestBody()));
            exchange.getResponseHeaders().set("Retry-After", "0");
            exchange.getResponseHeaders().set("Location", "/redirected-checkpoint");
            reply(exchange, 503, body);
        });
        server.start();
        try (var client = new FlinkRestApiClient(url(server))) {
            client.useEndpointResolver(remaining -> {
                discoveries.incrementAndGet();
                return url(server);
            });

            IOException failure = assertThrows(IOException.class,
                    () -> client.triggerCheckpoint(JOB, TRIGGER, Duration.ofSeconds(5)));

            assertTrue(failure.getMessage().contains("HTTP 503"));
            assertTrue(failure.getMessage().length() < body.length(), "only the exception preview is bounded");
            assertEquals(1, calls.get());
            assertEquals(1, discoveries.get());
            assertEquals("POST", method.get());
            assertEquals("/jobs/" + JOB.jobId() + "/checkpoints", path.get());
            assertEquals(TRIGGER, request.get().path("triggerId").textValue());
            assertFalse(request.get().has("checkpointType"));
            assertEquals(List.of(new FlinkScenarioControl.RestError(
                    1, "POST", path.get(), 503, body)), client.restErrors());
        } finally {
            server.stop(0);
        }
    }

    private static HttpServer server() throws IOException {
        return HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    }

    private static String url(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void reply(HttpExchange exchange, int status, String text) throws IOException {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, body.length);
        try (var output = exchange.getResponseBody()) {
            output.write(body);
        }
        exchange.close();
    }
}
