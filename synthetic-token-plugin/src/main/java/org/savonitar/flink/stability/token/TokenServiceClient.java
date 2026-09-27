package org.savonitar.flink.stability.token;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Wire format contains synthetic identifiers only; this fixture never obtains real credentials. */
final class TokenServiceClient {
    static final String SERVICE = "flink-stability-synthetic";
    static final String PREFIX = "flink-stability.token-service.";
    static final int MAX_BODY = 1024;
    private final URI endpoint;
    private final String process;
    private final String role;
    private final AtomicLong requests = new AtomicLong();

    TokenServiceClient(Configuration configuration) {
        endpoint = URI.create(configuration.getString(PREFIX + "endpoint", ""));
        if (!"http".equals(endpoint.getScheme()) || endpoint.getPort() < 1
                || !Set.of("127.0.0.1", "localhost", "host.testcontainers.internal").contains(endpoint.getHost())
                || endpoint.getRawUserInfo() != null || endpoint.getRawQuery() != null
                || endpoint.getRawFragment() != null
                || !(endpoint.getPath().isEmpty() || endpoint.getPath().equals("/"))) {
            throw new IllegalArgumentException("Synthetic token endpoint must use the local HTTP service");
        }
        process = configuration.getString(PREFIX + "process", "");
        role = configuration.getString(PREFIX + "role", "");
        if (!process.matches("(?:jobmanager|taskmanager)-[1-9][0-9]*#[1-9][0-9]*")
                || !Set.of("jobmanager", "taskmanager").contains(role)
                || !process.startsWith(role + "-")) {
            throw new IllegalArgumentException("Synthetic token process identity is required");
        }
    }

    byte[] request(String path, byte[] body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) endpoint.resolve(path).toURL()
                .openConnection(Proxy.NO_PROXY);
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(60_000);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("X-Flink-Stability-Process", process);
        connection.setRequestProperty("X-Flink-Stability-Role", role);
        connection.setRequestProperty("X-Flink-Stability-Request", Long.toString(requests.incrementAndGet()));
        connection.setDoOutput(true);
        connection.setFixedLengthStreamingMode(body.length);
        try {
            try (var out = connection.getOutputStream()) {
                out.write(body);
            }
            int status = connection.getResponseCode();
            if ((status == 503 || status == 598) && path.equals("/token")) {
                Throwable failure = status == 598
                        ? new LinkageError("Synthetic delegation-token acquisition fault")
                        : new IOException("Synthetic token service returned HTTP 503 for /token");
                try {
                    acknowledgeFault(connection, status);
                } catch (IOException acknowledgementFailure) {
                    // An unavailable evidence path must not replace the original acquisition error.
                    failure.addSuppressed(acknowledgementFailure);
                }
                if (failure instanceof LinkageError error) throw error;
                throw (IOException) failure;
            }
            if (status != 200) {
                throw new IOException("Synthetic token service returned HTTP " + status + " for " + path);
            }
            try (var input = connection.getInputStream()) {
                byte[] response = input.readNBytes(MAX_BODY + 1);
                if (response.length > MAX_BODY) {
                    throw new IOException("Synthetic token response exceeds its bound");
                }
                return response;
            }
        } finally {
            connection.disconnect();
        }
    }

    private void acknowledgeFault(HttpURLConnection response, int status) throws IOException {
        String request = response.getHeaderField("X-Flink-Stability-Request-Id");
        String revision = response.getHeaderField("X-Flink-Stability-Revision");
        try {
            if (Long.parseLong(request) < 1 || Long.parseLong(revision) < 0) {
                throw new NumberFormatException("Invalid synthetic request identity");
            }
        } catch (NumberFormatException invalid) {
            throw new IOException("Fault response has no valid synthetic request identity", invalid);
        }
        request("/fault-observed", (request + "\n" + revision + "\n" + status + "\n")
                .getBytes(StandardCharsets.UTF_8));
    }

    static long expiresAt(byte[] bytes) throws IOException {
        String[] fields = new String(bytes, StandardCharsets.UTF_8).split("\n", -1);
        try {
            if (bytes.length > MAX_BODY || fields.length != 6
                    || !fields[0].equals("flink-stability-token-v1") || !fields[5].isEmpty()
                    || !UUID.fromString(fields[1]).toString().equals(fields[1])
                    || Long.parseLong(fields[2]) < 1 || Long.parseLong(fields[3]) < 1
                    || Long.parseLong(fields[4]) <= Long.parseLong(fields[3])) {
                throw new IllegalArgumentException("Invalid synthetic token");
            }
            return Long.parseLong(fields[4]);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Received bytes are not a synthetic fixture token", invalid);
        }
    }
}
