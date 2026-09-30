package org.savonitar.flink.stability.token;

import com.sun.net.httpserver.HttpServer;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

class SyntheticDelegationTokenTest {
    private static final byte[] TOKEN = ("flink-stability-token-v2\n"
            + "12345678-1234-1234-1234-123456789abc\n7\n1000\n2000\n"
            + "87654321-4321-4321-4321-cba987654321\nBOOTSTRAP\n0\n-\n-\nfalse\n0\n")
            .getBytes(StandardCharsets.UTF_8);

    @Test
    void serviceLoaderFindsBothSpisAndActualReceiptsMatchObtainedBytes() throws Exception {
        List<String> paths = new CopyOnWriteArrayList<>();
        List<byte[]> bodies = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            byte[] request = exchange.getRequestBody().readAllBytes();
            bodies.add(request);
            assertEquals("jobmanager-1#2", exchange.getRequestHeaders().getFirst("X-Flink-Stability-Process"));
            byte[] body = exchange.getRequestURI().getPath().equals("/token") ? tokenFor(request) : new byte[0];
            exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            DelegationTokenProvider provider = ServiceLoader.load(DelegationTokenProvider.class).stream()
                    .filter(item -> item.type() == SyntheticDelegationTokenProvider.class)
                    .findFirst().orElseThrow().get();
            DelegationTokenReceiver receiver = ServiceLoader.load(DelegationTokenReceiver.class).stream()
                    .filter(item -> item.type() == SyntheticDelegationTokenReceiver.class)
                    .findFirst().orElseThrow().get();
            provider.init(configuration(server));
            receiver.init(configuration(server));
            assertEquals(provider.serviceName(), receiver.serviceName());
            assertTrue(provider.delegationTokensRequired());
            var obtained = provider.obtainDelegationTokens();
            assertEquals(2000L, obtained.getValidUntil().orElseThrow());
            assertEquals("BOOTSTRAP", TokenServiceClient.token(obtained.getTokens()).scope());
            receiver.onNewTokensObtained(obtained.getTokens());
            assertEquals(List.of("/init-provider", "/init-receiver", "/token", "/receipt"), paths);
            assertArrayEquals(obtained.getTokens(), bodies.get(3));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void failuresAndLinkageErrorsAreNotConvertedToTokensOrRetried() throws Exception {
        AtomicInteger code = new AtomicInteger(200);
        AtomicInteger calls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(code.get(), -1);
            exchange.close();
        });
        server.start();
        try {
            var provider = new SyntheticDelegationTokenProvider();
            provider.init(configuration(server));
            for (int failure : List.of(503, 302, 598)) {
                code.set(failure);
                int before = calls.get();
                if (failure == 598) {
                    assertThrows(LinkageError.class, provider::obtainDelegationTokens);
                } else {
                    assertThrows(IOException.class, provider::obtainDelegationTokens);
                }
                assertEquals(before + 1, calls.get());
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void receiverRejectsNonFixtureBytesBeforeContactingAnyService() throws Exception {
        var receiver = new SyntheticDelegationTokenReceiver();
        assertThrows(IOException.class, () -> receiver.onNewTokensObtained(new byte[0]));
        assertThrows(IOException.class, () -> TokenServiceClient.expiresAt(
                new String(TOKEN, StandardCharsets.UTF_8).replace("2000", "999").getBytes(StandardCharsets.UTF_8)));
        Configuration configuration = new Configuration();
        configuration.setString(TokenServiceClient.PREFIX + "endpoint", "http://example.invalid:80");
        assertThrows(IllegalArgumentException.class, () -> new TokenServiceClient(configuration));
    }

    @Test
    void acknowledgesReceivedFaultsWithoutReplacingTheOriginalErrorWhenAcknowledgementFails() throws Exception {
        AtomicInteger faultStatus = new AtomicInteger(503);
        AtomicInteger acknowledgementStatus = new AtomicInteger(200);
        List<String> paths = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            paths.add(path);
            byte[] body = exchange.getRequestBody().readAllBytes();
            int status = 200;
            if (path.equals("/token")) {
                exchange.getResponseHeaders().set("X-Flink-Stability-Request-Id", "41");
                exchange.getResponseHeaders().set("X-Flink-Stability-Revision", "7");
                status = faultStatus.get();
            } else if (path.equals("/fault-observed")) {
                assertEquals("41\n7\n" + faultStatus.get() + "\n", new String(body, StandardCharsets.UTF_8));
                status = acknowledgementStatus.get();
            }
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
        try {
            var provider = new SyntheticDelegationTokenProvider();
            provider.init(configuration(server));
            for (int fault : List.of(503, 598)) {
                for (int acknowledgement : List.of(200, 503)) {
                    faultStatus.set(fault);
                    acknowledgementStatus.set(acknowledgement);
                    paths.clear();
                    Throwable failure = fault == 598
                            ? assertThrows(LinkageError.class, provider::obtainDelegationTokens)
                            : assertThrows(IOException.class, provider::obtainDelegationTokens);
                    assertEquals(fault == 598 ? "Synthetic delegation-token acquisition fault"
                            : "Synthetic token service returned HTTP 503 for /token", failure.getMessage());
                    assertEquals(acknowledgement == 200 ? 0 : 1, failure.getSuppressed().length);
                    assertEquals(List.of("/token", "/fault-observed"), paths);
                }
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void copiesRegistrationAndKeepsIdempotentStateWithNoHookTransport() throws Exception {
        try (var service = new EchoService()) {
            var provider = new SyntheticDelegationTokenProvider();
            provider.init(configuration(service.server));
            var bootstrap = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
            assertEquals("BOOTSTRAP", bootstrap.scope());
            JobID job = JobID.generate();
            Configuration supplied = jobConfiguration("first-job");
            int calls = service.calls.get();
            provider.registerJob(job, supplied);
            supplied.setString(TokenServiceClient.JOB_ALIAS, "changed-by-caller");
            assertEquals(calls, service.calls.get());
            var first = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
            assertEquals("JOB", first.scope());
            assertEquals(job.toString(), first.jobId());
            assertEquals("first-job", first.jobAlias());
            assertEquals(bootstrap.providerInstance(), first.providerInstance());
            provider.registerJob(job, jobConfiguration("first-job"));
            var duplicate = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
            assertEquals(first.generation(), duplicate.generation());
            provider.unregisterJob(JobID.generate());
            var ignored = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
            assertEquals(duplicate, ignored);
            assertEquals("0", service.lastRequest()[8]);
            provider.unregisterJob(job);
            var removed = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
            assertEquals("BOOTSTRAP", removed.scope());
            assertEquals(first.generation() + 1, removed.generation());
            provider.unregisterJob(job);
            provider.obtainDelegationTokens();
            assertEquals("0", service.lastRequest()[8]);
            provider.registerJob(job, jobConfiguration("registered-again"));
            assertEquals("registered-again", TokenServiceClient.token(
                    provider.obtainDelegationTokens().getTokens()).jobAlias());
            calls = service.calls.get();
            provider.close();
            provider.close();
            assertEquals(calls, service.calls.get());
            assertThrows(IOException.class, provider::obtainDelegationTokens);
        }
    }

    @Test
    void inFlightSnapshotAndAckCannotEraseLaterHooksOrBlockThem() throws Exception {
        try (var service = new EchoService()) {
            var workers = Executors.newFixedThreadPool(2);
            var provider = new SyntheticDelegationTokenProvider();
            provider.init(configuration(service.server));
            JobID job = JobID.generate();
            provider.registerJob(job, jobConfiguration("old-job"));
            CountDownLatch release = new CountDownLatch(1);
            service.release = release;
            try {
                var held = workers.submit(provider::obtainDelegationTokens);
                assertTrue(service.entered.await(2, TimeUnit.SECONDS));
                int calls = service.calls.get();
                var hooks = workers.submit(() -> {
                    provider.unregisterJob(job);
                    provider.registerJob(job, jobConfiguration("new-job"));
                });
                hooks.get(2, TimeUnit.SECONDS);
                assertFalse(held.isDone());
                assertEquals(calls, service.calls.get());
                release.countDown();
                var old = TokenServiceClient.token(held.get(2, TimeUnit.SECONDS).getTokens());
                assertEquals("old-job", old.jobAlias());
                assertEquals(1, old.acknowledgedSequence());
                var current = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
                assertEquals("new-job", current.jobAlias());
                String[] request = service.lastRequest();
                assertEquals("1", request[7]);
                assertEquals("2", request[8]);
                assertTrue(request[9].startsWith("2\tUNREGISTER\t"));
                assertTrue(request[10].startsWith("3\tREGISTER\t"));
                assertEquals(3, current.acknowledgedSequence());
            } finally {
                release.countDown();
                workers.shutdownNow();
                assertTrue(workers.awaitTermination(2, TimeUnit.SECONDS));
                provider.close();
            }
        }
    }

    @Test
    void rejectingAnUnsentAckLeavesTheEntirePrefixAvailableForRetry() throws Exception {
        try (var service = new EchoService()) {
            var provider = new SyntheticDelegationTokenProvider();
            provider.init(configuration(service.server));
            provider.registerJob(JobID.generate(), jobConfiguration("test-job"));
            service.transform = bytes -> {
                String[] fields = new String(bytes, StandardCharsets.UTF_8).split("\n", -1);
                fields[11] = "2"; // Only seq1 was sent, so this cannot retire the prefix.
                return String.join("\n", fields).getBytes(StandardCharsets.UTF_8);
            };
            assertThrows(IOException.class, provider::obtainDelegationTokens);
            service.transform = UnaryOperator.identity();
            provider.obtainDelegationTokens();
            assertEquals("0", service.lastRequest()[7]);
            assertEquals("1", service.lastRequest()[8]);
            provider.obtainDelegationTokens();
            assertEquals("1", service.lastRequest()[7]);
            assertEquals("0", service.lastRequest()[8]);
            provider.close();
        }
    }

    @Test
    void conflictsSecondJobsAndOverflowKeepCoverageInvalidAfterRemovalAndAcknowledgement() throws Exception {
        try (var service = new EchoService()) {
            for (String problem : List.of("alias", "second-job", "overflow", "missing-alias")) {
                var provider = new SyntheticDelegationTokenProvider();
                provider.init(configuration(service.server));
                JobID job = JobID.generate();
                provider.registerJob(job, jobConfiguration("retained-job"));
                switch (problem) {
                    case "alias" -> provider.registerJob(job, jobConfiguration("different-alias"));
                    case "second-job" -> provider.registerJob(JobID.generate(), jobConfiguration("second-job"));
                    case "missing-alias" -> provider.registerJob(job, new Configuration());
                    case "overflow" -> {
                        for (int count = 0; count < 33; count++) provider.registerJob(job, jobConfiguration("retained-job"));
                    }
                    default -> throw new AssertionError(problem);
                }
                var invalid = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
                assertTrue(invalid.coverageInvalid(), problem);
                assertEquals("retained-job", invalid.jobAlias());
                assertEquals(job.toString(), invalid.jobId());
                assertTrue(Integer.parseInt(service.lastRequest()[8]) <= 32);
                provider.unregisterJob(job);
                invalid = TokenServiceClient.token(provider.obtainDelegationTokens().getTokens());
                assertTrue(invalid.coverageInvalid(), problem);
                assertEquals("BOOTSTRAP", invalid.scope());
                provider.close();
            }
        }
    }

    private static Configuration jobConfiguration(String alias) {
        Configuration configuration = new Configuration();
        configuration.setString(TokenServiceClient.JOB_ALIAS, alias);
        return configuration;
    }

    /** Deliberately small wire peer: service-side validation is tested in SyntheticTokenServiceTest. */
    private static final class EchoService implements AutoCloseable {
        private final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        private final List<byte[]> requests = new CopyOnWriteArrayList<>();
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private volatile CountDownLatch release;
        private volatile UnaryOperator<byte[]> transform = UnaryOperator.identity();

        private EchoService() throws IOException {
            server.createContext("/", exchange -> {
                try {
                    calls.incrementAndGet();
                    byte[] body = exchange.getRequestBody().readNBytes(TokenServiceClient.MAX_BODY + 1);
                    byte[] response = new byte[0];
                    if (exchange.getRequestURI().getPath().equals("/token")) {
                        requests.add(body);
                        entered.countDown();
                        CountDownLatch barrier = release;
                        if (barrier != null) {
                            try {
                                if (!barrier.await(5, TimeUnit.SECONDS)) throw new IOException("Test peer release deadline");
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IOException(interrupted);
                            }
                        }
                        response = transform.apply(tokenFor(body));
                    }
                    exchange.sendResponseHeaders(200, response.length == 0 ? -1 : response.length);
                    exchange.getResponseBody().write(response);
                } finally {
                    exchange.close();
                }
            });
            server.start();
        }

        private String[] lastRequest() {
            return new String(requests.get(requests.size() - 1), StandardCharsets.UTF_8).split("\n", -1);
        }

        @Override
        public void close() {
            if (release != null) release.countDown();
            server.stop(0);
        }
    }

    private static byte[] tokenFor(byte[] request) {
        String[] fields = new String(request, StandardCharsets.UTF_8).split("\n", -1);
        long through = Long.parseLong(fields[7]) + Integer.parseInt(fields[8]);
        return ("flink-stability-token-v2\n12345678-1234-1234-1234-123456789abc\n7\n1000\n2000\n"
                + fields[1] + "\n" + fields[2] + "\n" + fields[3] + "\n" + fields[4] + "\n"
                + fields[5] + "\n" + fields[6] + "\n" + through + "\n").getBytes(StandardCharsets.UTF_8);
    }

    private static Configuration configuration(HttpServer server) {
        Configuration configuration = new Configuration();
        configuration.setString(TokenServiceClient.PREFIX + "endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        configuration.setString(TokenServiceClient.PREFIX + "process", "jobmanager-1#2");
        configuration.setString(TokenServiceClient.PREFIX + "role", "jobmanager");
        return configuration;
    }
}
