package org.savonitar.flink.stability.core.flink;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The no-replay transport contract applies without an HA endpoint resolver too. */
class FlinkRestPolicyTest {
    @ParameterizedTest
    @ValueSource(ints = {302, 307, 408, 503})
    void standaloneGetPreservesTheFirstErrorWithoutRedirectOrReplay(int status) throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicInteger redirectedRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
        server.createContext("/redirect-target", exchange -> {
            redirectedRequests.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.createContext("/jobs/job", exchange -> {
            requests.incrementAndGet();
            exchange.getResponseHeaders().set("Location", endpoint + "/redirect-target");
            exchange.getResponseHeaders().set("Retry-After", "0");
            byte[] body = "original standalone response".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try (FlinkRestApiClient client = new FlinkRestApiClient(endpoint)) {
            IOException failure = assertThrows(IOException.class,
                    () -> client.jobState(new FlinkJobHandle("job")));
            assertTrue(failure.getMessage().contains("HTTP " + status));
            assertTrue(failure.getMessage().contains("original standalone response"));
            assertEquals(1, requests.get());
            assertEquals(0, redirectedRequests.get());
        } finally {
            server.stop(0);
        }
    }
}
