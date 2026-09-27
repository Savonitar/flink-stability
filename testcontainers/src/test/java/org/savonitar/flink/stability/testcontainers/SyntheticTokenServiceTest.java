package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class SyntheticTokenServiceTest {
    @Test
    void reportsInitializationIssuanceAndExactReceiptWithImmutableOrderedEvidence() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            assertEquals(200, request(client, service, "/init-provider", "jobmanager-1#1", new byte[0]).get().statusCode());
            var initial = service.snapshot();
            var token = request(client, service, "/token", "jobmanager-1#1", new byte[0]).get();
            assertEquals(200, token.statusCode());
            assertEquals(200, request(client, service, "/init-receiver", "taskmanager-2#2", new byte[0]).get().statusCode());
            assertEquals(200, request(client, service, "/receipt", "taskmanager-2#2", token.body()).get().statusCode());
            assertEquals(400, request(client, service, "/receipt", "taskmanager-2#2", new byte[]{1}).get().statusCode());
            var snapshot = service.snapshot();
            assertEquals(1, initial.events().size());
            assertThrows(UnsupportedOperationException.class, () -> snapshot.events().clear());
            for (int index = 0; index < snapshot.events().size(); index++) {
                assertEquals(index + 1, snapshot.events().get(index).sequence());
            }
            var issued = snapshot.events().stream().filter(event -> event.kind() == TokenServiceControl.Kind.ISSUED).findFirst().orElseThrow();
            var received = snapshot.events().stream().filter(event -> event.kind() == TokenServiceControl.Kind.RECEIVED).findFirst().orElseThrow();
            assertEquals(issued.tokenSequence(), received.tokenSequence());
            assertEquals("taskmanager-2#2", received.process());
            assertTrue(received.monotonicNanos() >= issued.monotonicNanos());
            assertFalse(snapshot.overflow());
        }
    }

    @Test
    void delayedRequestsExposeConcurrencyAndOriginalFaultRevisionThenHeal() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            assertThrows(IllegalArgumentException.class,
                    () -> service.configure(TokenServiceControl.Mode.DELAY, Duration.ofNanos(1)));
            assertThrows(IllegalArgumentException.class,
                    () -> service.configure(TokenServiceControl.Mode.DELAY, Duration.ofSeconds(31)));
            long revision = service.configure(TokenServiceControl.Mode.DELAY, Duration.ofSeconds(30));
            var first = request(client, service, "/token", "jobmanager-1#1", new byte[0]);
            var second = request(client, service, "/token", "jobmanager-2#1", new byte[0]);
            await(() -> service.snapshot().activeRequests() == 2);
            assertEquals(2, service.snapshot().maxConcurrentRequests());
            service.configure(TokenServiceControl.Mode.HEALTHY, Duration.ZERO);
            assertEquals(200, first.get(2, TimeUnit.SECONDS).statusCode());
            assertEquals(200, second.get(2, TimeUnit.SECONDS).statusCode());
            assertEquals(2, service.snapshot().events().stream()
                    .filter(event -> event.kind() == TokenServiceControl.Kind.ISSUED
                            && event.mode() == TokenServiceControl.Mode.DELAY && event.revision() == revision).count());
        }
    }

    @Test
    void failureModesAreRecordedAndRecoveryDoesNotEraseThem() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            service.configure(TokenServiceControl.Mode.FAIL, Duration.ZERO);
            assertEquals(503, request(client, service, "/token", "jobmanager-1#1", new byte[0]).get().statusCode());
            service.configure(TokenServiceControl.Mode.LINKAGE_ERROR, Duration.ZERO);
            assertEquals(598, request(client, service, "/token", "jobmanager-1#1", new byte[0]).get().statusCode());
            service.configure(TokenServiceControl.Mode.HEALTHY, Duration.ZERO);
            assertEquals(200, request(client, service, "/token", "jobmanager-1#1", new byte[0]).get().statusCode());
            assertEquals(2, service.snapshot().events().stream()
                    .filter(event -> event.kind() == TokenServiceControl.Kind.FAILED).count());
        }
    }

    @Test
    void clientAcknowledgementMustIdentifyTheActualRequestProcessRevisionAndStatus() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            for (var mode : java.util.List.of(TokenServiceControl.Mode.FAIL, TokenServiceControl.Mode.LINKAGE_ERROR)) {
                long revision = service.configure(mode, Duration.ZERO);
                var response = request(client, service, "/token", "jobmanager-2#3", new byte[0]).get();
                String id = response.headers().firstValue("X-Flink-Stability-Request-Id").orElseThrow();
                assertEquals(Long.toString(revision), response.headers().firstValue("X-Flink-Stability-Revision").orElseThrow());
                byte[] body = (id + "\n" + revision + "\n" + response.statusCode() + "\n").getBytes(StandardCharsets.UTF_8);
                assertEquals(400, request(client, service, "/fault-observed", "jobmanager-2#4", body).get().statusCode());
                for (String invalid : java.util.List.of("9999\n" + revision + "\n" + response.statusCode() + "\n",
                        id + "\n99\n" + response.statusCode() + "\n", id + "\n" + revision + "\n200\n")) {
                    assertEquals(400, request(client, service, "/fault-observed", "jobmanager-2#3",
                            invalid.getBytes(StandardCharsets.UTF_8)).get().statusCode());
                }
                assertEquals(200, request(client, service, "/fault-observed", "jobmanager-2#3", body).get().statusCode());
                assertEquals(400, request(client, service, "/fault-observed", "jobmanager-2#3", body).get().statusCode());
                var observed = service.snapshot().events().stream().filter(event -> event.kind()
                        == TokenServiceControl.Kind.FAULT_OBSERVED && event.requestId() == Long.parseLong(id))
                        .findFirst().orElseThrow();
                assertEquals(mode, observed.mode());
                assertEquals(revision, observed.revision());
                assertEquals("HTTP " + response.statusCode(), observed.detail());
            }
        }
    }

    @Test
    void eventOverflowRejectsTokensAndRetainsTheOriginalPrefix() throws Exception {
        try (var service = new SyntheticTokenService(Duration.ofSeconds(2), 1, 1, 1);
             var client = HttpClient.newHttpClient()) {
            assertEquals(503, request(client, service, "/token", "jobmanager-1#1", new byte[0]).get().statusCode());
            assertTrue(service.snapshot().overflow());
            assertEquals(TokenServiceControl.Kind.REQUEST_STARTED, service.snapshot().events().getFirst().kind());
            assertEquals(1, service.snapshot().events().size());
            assertEquals(503, request(client, service, "/token", "jobmanager-1#1", new byte[0]).get().statusCode());
        }
    }

    @Test
    void closeCancelsHeldRequestsAndRetainsTheirEvidence() throws Exception {
        var service = SyntheticTokenService.start(Duration.ofSeconds(2));
        try (var client = HttpClient.newHttpClient()) {
            service.configure(TokenServiceControl.Mode.DELAY, Duration.ofSeconds(30));
            var held = request(client, service, "/token", "jobmanager-1#1", new byte[0]);
            await(() -> service.snapshot().activeRequests() == 1);
            service.close();
            assertEquals(0, service.snapshot().activeRequests());
            assertTrue(held.handle((result, failure) -> true).get(2, TimeUnit.SECONDS));
            assertTrue(service.snapshot().events().stream().anyMatch(event -> event.kind() == TokenServiceControl.Kind.REQUEST_FINISHED));
            assertThrows(IllegalArgumentException.class, () -> service.configure(TokenServiceControl.Mode.HEALTHY, Duration.ZERO));
        } finally {
            service.close();
        }
    }

    @Test
    void workerSaturationIsVisibleAndCannotRecoverIntoCompleteEvidence() throws Exception {
        try (var service = new SyntheticTokenService(Duration.ofSeconds(2), 100, 1, 1);
             var client = HttpClient.newHttpClient()) {
            service.configure(TokenServiceControl.Mode.DELAY, Duration.ofSeconds(30));
            var pending = new ArrayList<CompletableFuture<HttpResponse<byte[]>>>();
            pending.add(request(client, service, "/token", "jobmanager-1#1", new byte[0]));
            await(() -> service.snapshot().activeRequests() == 1);
            for (int index = 0; index < 8; index++) {
                pending.add(request(client, service, "/token", "jobmanager-2#1", new byte[0]));
            }
            await(() -> service.snapshot().saturated());
            service.configure(TokenServiceControl.Mode.HEALTHY, Duration.ZERO);
            service.close();
            assertTrue(service.snapshot().saturated());
            assertTrue(service.snapshot().events().stream().anyMatch(event ->
                    event.kind() == TokenServiceControl.Kind.REJECTED
                            && event.detail().equals("HTTP worker queue saturated")));
            for (var response : pending) {
                response.cancel(true);
            }
        }
    }

    private static CompletableFuture<HttpResponse<byte[]>> request(HttpClient client,
            SyntheticTokenService service, String path, String process, byte[] body) {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + service.port() + path))
                .timeout(Duration.ofSeconds(5)).header("X-Flink-Stability-Process", process)
                .header("X-Flink-Stability-Role", process.startsWith("jobmanager") ? "jobmanager" : "taskmanager")
                .header("X-Flink-Stability-Request", "1")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static void await(BooleanSupplier ready) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!ready.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(ready.getAsBoolean(), "Expected fixture event before deadline");
    }
}
