package org.savonitar.flink.stability.core.flink;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FlinkHaEndpointTest {
    @Test
    void explicitResolverBudgetExpiryPreservesItsCauseAndDoesNotRetry() {
        TimeoutException expired = new TimeoutException("owned leader budget expired");
        expired.initCause(new IllegalStateException("retained runtime deadline evidence"));
        AtomicInteger discoveries = new AtomicInteger();
        try (FlinkRestApiClient client = new FlinkRestApiClient("http://127.0.0.1:1")) {
            client.useEndpointResolver(timeout -> {
                assertTrue(timeout.isPositive());
                assertTrue(timeout.compareTo(Duration.ofSeconds(1)) <= 0);
                discoveries.incrementAndGet();
                throw expired;
            });

            FlinkRestTimeoutException failure = assertThrows(FlinkRestTimeoutException.class,
                    () -> client.awaitFinished(new FlinkJobHandle("job"), Duration.ofSeconds(1)));

            assertSame(expired, failure.getCause());
            assertTrue(failure.getMessage().contains("resolving the owned Flink REST leader"));
            assertEquals(1, discoveries.get());
            assertTrue(client.restErrors().isEmpty());
        }
    }

    @Test
    void resolverErrorsRemainErrorsEvenWhenTheyContainANestedTimeout() {
        for (Exception original : List.of(new IOException("invalid leader record"),
                new IllegalStateException("observer disconnected"),
                new IOException("failed leader lookup", new TimeoutException("inner operation")),
                new IllegalStateException("failed leader lookup", new TimeoutException("inner operation")))) {
            AtomicInteger discoveries = new AtomicInteger();
            try (FlinkRestApiClient client = new FlinkRestApiClient("http://127.0.0.1:1")) {
                client.useEndpointResolver(timeout -> {
                    discoveries.incrementAndGet();
                    if (original instanceof IOException io) throw io;
                    throw (RuntimeException) original;
                });

                IOException failure = assertThrows(IOException.class,
                        () -> client.awaitFinished(new FlinkJobHandle("job"), Duration.ofSeconds(1)));

                assertFalse(failure instanceof FlinkRestTimeoutException);
                assertSame(original, original instanceof IOException ? failure : failure.getCause());
                assertEquals(1, discoveries.get());
            }
        }
    }

    @Test
    void interruptedIoKeepsTheExistingBoundedVersusUnboundedRestClassification() {
        InterruptedIOException original = new InterruptedIOException("interrupted resolver IO");
        try (FlinkRestApiClient client = new FlinkRestApiClient("http://127.0.0.1:1")) {
            client.useEndpointResolver(timeout -> { throw original; });

            assertSame(original, assertThrows(InterruptedIOException.class,
                    () -> client.jobState(new FlinkJobHandle("job"))));
            FlinkRestTimeoutException bounded = assertThrows(FlinkRestTimeoutException.class,
                    () -> client.awaitFinished(new FlinkJobHandle("job"), Duration.ofSeconds(1)));
            assertSame(original, bounded.getCause());
        }
    }

    @Test
    void resolvesTheCurrentOwnedLeaderForEachRequest() throws Exception {
        HttpServer first = server("RUNNING");
        HttpServer second = server("FINISHED");
        try (FlinkRestApiClient client = new FlinkRestApiClient(url(first))) {
            AtomicReference<String> selected = new AtomicReference<>(url(first));
            AtomicInteger discoveries = new AtomicInteger();
            client.useEndpointResolver(timeout -> {
                assertTrue(timeout.isPositive());
                discoveries.incrementAndGet();
                return selected.get();
            });
            FlinkJobHandle job = new FlinkJobHandle("same-job");
            assertEquals(FlinkJobState.RUNNING, client.jobState(job));
            selected.set(url(second));
            assertEquals(FlinkJobState.FINISHED, client.jobState(job));
            assertEquals(2, discoveries.get());
        } finally {
            first.stop(0);
            second.stop(0);
        }
    }

    @Test
    void leaderResolutionConsumesTheExistingObservationDeadline() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try (FlinkRestApiClient client = new FlinkRestApiClient(url(server))) {
            client.useEndpointResolver(timeout -> {
                try {
                    // Exceed the 1 ms observation deadline during discovery, before any HTTP request.
                    Thread.sleep(20);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                }
                return url(server);
            });
            assertThrows(FlinkRestTimeoutException.class,
                    () -> client.observe(new FlinkJobHandle("job"), Duration.ofMillis(1)));
            assertEquals(0, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void doesNotReplaySubmissionThroughAStandbyRedirect() throws Exception {
        AtomicInteger leaderPosts = new AtomicInteger();
        HttpServer leader = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        leader.createContext("/", exchange -> {
            leaderPosts.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        leader.start();
        AtomicInteger standbyPosts = new AtomicInteger();
        HttpServer standby = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        standby.createContext("/", exchange -> {
            standbyPosts.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("Location", url(leader) + "/jars/job.jar/run");
            exchange.sendResponseHeaders(307, -1);
            exchange.close();
        });
        standby.start();
        try (FlinkRestApiClient client = new FlinkRestApiClient(url(standby))) {
            client.useEndpointResolver(timeout -> url(standby));
            IOException failure = assertThrows(IOException.class, () -> client.submit(
                    new FlinkJobSubmission("job.jar", 1, Map.of(), List.of())));
            assertTrue(failure.getMessage().contains("HTTP 307"));
            assertEquals(1, standbyPosts.get());
            assertEquals(0, leaderPosts.get());
        } finally {
            standby.stop(0);
            leader.stop(0);
        }
    }

    private static HttpServer server(String state) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = ("{\"state\":\"" + state + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        return server;
    }

    @Test
    void surfaces503WithoutRepeatingAPostEvenWhenRetryAfterIsZero() throws Exception {
        AtomicInteger posts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            posts.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("Retry-After", "0");
            byte[] body = "original provider unavailable".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try (FlinkRestApiClient client = new FlinkRestApiClient(url(server))) {
            IOException failure = assertThrows(IOException.class, () -> client.submit(
                    new FlinkJobSubmission("job.jar", 1, Map.of(), List.of())));
            assertEquals(1, posts.get());
            assertTrue(failure.getMessage().contains("HTTP 503"));
            assertTrue(failure.getMessage().contains("original provider unavailable"));
        } finally {
            server.stop(0);
        }
    }

    private static String url(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }
}
