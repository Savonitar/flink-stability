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
import java.util.Base64;
import java.util.List;
import java.util.UUID;
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
            assertEquals(issued.registration(), received.registration());
            assertEquals(instance("jobmanager-1#1"), issued.registration().orElseThrow().providerInstance());
            assertEquals("BOOTSTRAP", issued.registration().orElseThrow().scope());
            assertEquals(instance("taskmanager-2#2"), received.participantInstance().orElseThrow());
            assertNotEquals(issued.participantInstance(), received.participantInstance());
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
    void sharedReceiverKeepsEachExactTokensOriginalJobAndRejectsUnissuedBytes() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            String receiver = "taskmanager-1#1";
            for (int index = 1; index <= 2; index++) {
                String process = "jobmanager-" + index + "#1";
                String job = Integer.toString(index).repeat(32);
                String alias = "job-" + index;
                var response = request(client, service, "/token", process,
                        snapshot(process, "JOB", 1, job, alias, 0,
                                lifecycle(1, "REGISTER", 1, job, alias))).get();
                String[] token = tokenFields(response);
                assertEquals(200, request(client, service, "/receipt", receiver, response.body()).get().statusCode());
                var received = service.snapshot().events().stream()
                        .filter(event -> event.kind() == TokenServiceControl.Kind.RECEIVED
                                && event.tokenSequence().orElseThrow() == Long.parseLong(token[2]))
                        .findFirst().orElseThrow();
                assertEquals(job, received.registration().orElseThrow().jobId());
                assertEquals(alias, received.registration().orElseThrow().jobAlias());
                assertEquals(instance(process), received.registration().orElseThrow().providerInstance());
                assertEquals(instance(receiver), received.participantInstance().orElseThrow());

                // Still valid token syntax, but never issued by this service.
                token[8] = "f".repeat(32);
                byte[] changed = String.join("\n", token).getBytes(StandardCharsets.UTF_8);
                assertEquals(400, request(client, service, "/receipt", receiver, changed).get().statusCode());
            }
            assertEquals(2, service.snapshot().events().stream()
                    .filter(event -> event.kind() == TokenServiceControl.Kind.RECEIVED).count());
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
                String job = "a".repeat(32);
                byte[] submitted = snapshot("jobmanager-2#3", "JOB", 1, job, "submitted-job", 0,
                        lifecycle(1, "REGISTER", 1, job, "submitted-job"));
                var response = request(client, service, "/token", "jobmanager-2#3", submitted).get();
                String id = response.headers().firstValue("X-Flink-Stability-Request-Id").orElseThrow();
                assertEquals(Long.toString(revision), response.headers().firstValue("X-Flink-Stability-Revision").orElseThrow());
                byte[] body = (id + "\n" + revision + "\n" + response.statusCode() + "\n").getBytes(StandardCharsets.UTF_8);
                assertEquals(400, request(client, service, "/fault-observed", "jobmanager-2#4", body).get().statusCode());
                assertEquals(400, request(client, service, "/fault-observed", "jobmanager-2#3", body,
                        UUID.randomUUID().toString()).get().statusCode());
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
                var started = service.snapshot().events().stream().filter(event -> event.kind()
                        == TokenServiceControl.Kind.REQUEST_STARTED && event.requestId() == Long.parseLong(id))
                        .findFirst().orElseThrow();
                assertEquals(started.registration(), observed.registration());
                assertEquals("JOB", observed.registration().orElseThrow().scope());
                assertEquals(job, observed.registration().orElseThrow().jobId());
                assertEquals("submitted-job", observed.registration().orElseThrow().jobAlias());
            }
        }
    }

    @Test
    void journalReplayAcknowledgesOnlyTheSentPrefixAndRejectsConflictingDuplicatesAndGaps() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            String process = "jobmanager-1#1";
            String job = "b".repeat(32);
            byte[] registered = snapshot(process, "JOB", 1, job, "first-job", 0,
                    lifecycle(1, "REGISTER", 1, job, "first-job"));
            var first = request(client, service, "/token", process, registered).get();
            assertEquals(200, first.statusCode());
            assertEquals("1", tokenFields(first)[11]);
            var removed = request(client, service, "/token", process,
                    snapshot(process, "BOOTSTRAP", 2, "-", "-", 1,
                            lifecycle(2, "UNREGISTER", 2, job, "first-job"))).get();
            assertEquals(200, removed.statusCode());
            assertEquals("2", tokenFields(removed)[11]);
            // Ledger already has seq2; an older repeated request must only acknowledge its own seq1.
            var replay = request(client, service, "/token", process, registered).get();
            assertEquals(200, replay.statusCode());
            assertEquals("1", tokenFields(replay)[11]);
            assertEquals("JOB", tokenFields(replay)[6]);
            assertEquals(400, request(client, service, "/token", process,
                    snapshot(process, "JOB", 1, job, "conflict", 0,
                            lifecycle(1, "REGISTER", 1, job, "conflict"))).get().statusCode());
            assertEquals(400, request(client, service, "/token", process,
                    snapshot(process, "BOOTSTRAP", 2, "-", "-", 2,
                            lifecycle(4, "CLOSE", 2, "-", "-"))).get().statusCode());
            assertEquals(400, request(client, service, "/token", process,
                    snapshot(process, "BOOTSTRAP", 2, "-", "-", 99)).get().statusCode());
            assertEquals(200, request(client, service, "/token", process,
                    snapshot(process, "BOOTSTRAP", 2, "-", "-", 2)).get().statusCode());
        }
    }

    @Test
    void delayedSnapshotCannotBeRelabelledByALaterRegistration() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            String process = "jobmanager-1#1";
            service.configure(TokenServiceControl.Mode.DELAY, Duration.ofSeconds(30));
            var bootstrap = request(client, service, "/token", process, new byte[0]);
            await(() -> service.snapshot().activeRequests() == 1);
            var captured = service.snapshot().events().stream()
                    .filter(event -> event.kind() == TokenServiceControl.Kind.REQUEST_STARTED).findFirst().orElseThrow();
            service.configure(TokenServiceControl.Mode.HEALTHY, Duration.ZERO);
            String job = "c".repeat(32);
            var registered = request(client, service, "/token", process,
                    snapshot(process, "JOB", 1, job, "later-job", 0,
                            lifecycle(1, "REGISTER", 1, job, "later-job"))).get();
            assertEquals("JOB", tokenFields(registered)[6]);
            assertEquals("BOOTSTRAP", tokenFields(bootstrap.get(2, TimeUnit.SECONDS))[6]);
            var oldIssued = service.snapshot().events().stream().filter(event -> event.kind()
                    == TokenServiceControl.Kind.ISSUED && event.requestId() == captured.requestId()).findFirst().orElseThrow();
            assertEquals(captured.registration(), oldIssued.registration());
            assertEquals("BOOTSTRAP", oldIssued.registration().orElseThrow().scope());
        }
    }

    @Test
    void rejectsMalformedOrUnboundedSnapshotsAndMismatchedProviderIdentity() throws Exception {
        try (var service = SyntheticTokenService.start(Duration.ofSeconds(2));
             var client = HttpClient.newHttpClient()) {
            String process = "jobmanager-1#1";
            byte[] valid = snapshot(process, "BOOTSTRAP", 0, "-", "-", 0);
            assertEquals(400, request(client, service, "/token", process, valid,
                    UUID.randomUUID().toString()).get().statusCode());
            assertEquals(400, request(client, service, "/token", "taskmanager-1#1", new byte[0]).get().statusCode());
            for (byte[] invalid : List.of(new byte[] {1}, new byte[] {(byte) 0xff},
                    new String(valid, StandardCharsets.UTF_8).replace("BOOTSTRAP", "JOB").getBytes(StandardCharsets.UTF_8))) {
                assertEquals(400, request(client, service, "/token", process, invalid).get().statusCode());
            }
            assertEquals(413, request(client, service, "/token", process, new byte[16_385]).get().statusCode());
            var records = new ArrayList<String>();
            for (int index = 1; index <= 33; index++) records.add(lifecycle(index, "CLOSE", 0, "-", "-"));
            assertEquals(400, request(client, service, "/token", process,
                    snapshot(process, "BOOTSTRAP", 0, "-", "-", 0, records.toArray(String[]::new))).get().statusCode());
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
        if (path.equals("/token") && body.length == 0) {
            body = snapshot(process, "BOOTSTRAP", 0, "-", "-", 0);
        }
        return request(client, service, path, process, body, instance(process));
    }

    private static CompletableFuture<HttpResponse<byte[]>> request(HttpClient client,
            SyntheticTokenService service, String path, String process, byte[] body, String instance) {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + service.port() + path))
                .timeout(Duration.ofSeconds(5)).header("X-Flink-Stability-Process", process)
                .header("X-Flink-Stability-Role", process.startsWith("jobmanager") ? "jobmanager" : "taskmanager")
                .header("X-Flink-Stability-Request", "1")
                .header("X-Flink-Stability-Instance", instance)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static String instance(String process) {
        return UUID.nameUUIDFromBytes(process.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static byte[] snapshot(String process, String scope, long generation, String job,
                                   String alias, long acknowledged, String... journal) {
        return ("flink-stability-request-v2\n" + instance(process) + "\n" + scope + "\n" + generation
                + "\n" + job + "\n" + encodedAlias(job, alias) + "\nfalse\n" + acknowledged + "\n"
                + journal.length + "\n" + String.join("", journal)).getBytes(StandardCharsets.UTF_8);
    }

    private static String lifecycle(long sequence, String kind, long generation, String job, String alias) {
        return sequence + "\t" + kind + "\t" + generation + "\t" + job + "\t" + encodedAlias(job, alias) + "\n";
    }

    private static String encodedAlias(String job, String alias) {
        return job.equals("-") ? "-" : Base64.getUrlEncoder().withoutPadding()
                .encodeToString(alias.getBytes(StandardCharsets.UTF_8));
    }

    private static String[] tokenFields(HttpResponse<byte[]> response) {
        assertEquals(200, response.statusCode());
        return new String(response.body(), StandardCharsets.UTF_8).split("\n", -1);
    }

    private static void await(BooleanSupplier ready) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!ready.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(ready.getAsBoolean(), "Expected fixture event before deadline");
    }
}
