package org.savonitar.flink.stability.token;

import com.sun.net.httpserver.HttpServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SyntheticDelegationTokenTest {
    private static final byte[] TOKEN = ("flink-stability-token-v1\n"
            + "12345678-1234-1234-1234-123456789abc\n7\n1000\n2000\n")
            .getBytes(StandardCharsets.UTF_8);

    @Test
    void serviceLoaderFindsBothSpisAndActualReceiptsMatchObtainedBytes() throws Exception {
        List<String> paths = new CopyOnWriteArrayList<>();
        List<byte[]> bodies = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            bodies.add(exchange.getRequestBody().readAllBytes());
            assertEquals("jobmanager-1#2", exchange.getRequestHeaders().getFirst("X-Flink-Stability-Process"));
            byte[] body = exchange.getRequestURI().getPath().equals("/token") ? TOKEN : new byte[0];
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
            assertArrayEquals(TOKEN, obtained.getTokens());
            receiver.onNewTokensObtained(obtained.getTokens());
            assertEquals(List.of("/init-provider", "/init-receiver", "/token", "/receipt"), paths);
            assertArrayEquals(TOKEN, bodies.get(3));
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

    private static Configuration configuration(HttpServer server) {
        Configuration configuration = new Configuration();
        configuration.setString(TokenServiceClient.PREFIX + "endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        configuration.setString(TokenServiceClient.PREFIX + "process", "jobmanager-1#2");
        configuration.setString(TokenServiceClient.PREFIX + "role", "jobmanager");
        return configuration;
    }
}
