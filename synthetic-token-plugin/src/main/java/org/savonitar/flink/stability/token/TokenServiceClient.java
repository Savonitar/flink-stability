package org.savonitar.flink.stability.token;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Wire format contains synthetic identifiers only; this fixture never obtains real credentials. */
final class TokenServiceClient {
    static final String SERVICE = "flink-stability-synthetic";
    static final String PREFIX = "flink-stability.token-service.";
    static final int MAX_BODY = 16_384;
    static final int MAX_JOURNAL = 32;
    static final String JOB_ALIAS = "flink-stability.workload.v1.job-alias";
    private final URI endpoint;
    private final String process;
    private final String role;
    private final String instance;
    private final AtomicLong requests = new AtomicLong();

    TokenServiceClient(Configuration configuration) {
        this(configuration, UUID.randomUUID().toString());
    }

    TokenServiceClient(Configuration configuration, String instance) {
        this.instance = instance;
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
        if (body.length > MAX_BODY) throw new IOException("Synthetic request exceeds its bound");
        HttpURLConnection connection = (HttpURLConnection) endpoint.resolve(path).toURL()
                .openConnection(Proxy.NO_PROXY);
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(60_000);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("X-Flink-Stability-Process", process);
        connection.setRequestProperty("X-Flink-Stability-Role", role);
        connection.setRequestProperty("X-Flink-Stability-Instance", instance);
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

    record Lifecycle(long sequence, String kind, long generation, String jobId, String jobAlias) {}

    record RequestSnapshot(String providerInstance, String scope, long generation,
                           String jobId, String jobAlias, boolean coverageInvalid,
                           long acknowledgedSequence, List<Lifecycle> journal) {
        RequestSnapshot {
            journal = List.copyOf(journal);
        }

        long sentThrough() {
            return journal.isEmpty() ? acknowledgedSequence : journal.get(journal.size() - 1).sequence();
        }

        byte[] encode() {
            StringBuilder body = new StringBuilder("flink-stability-request-v2\n")
                    .append(providerInstance).append('\n').append(scope).append('\n')
                    .append(generation).append('\n').append(jobId).append('\n')
                    .append(encodedAlias(jobId, jobAlias)).append('\n')
                    .append(coverageInvalid).append('\n').append(acknowledgedSequence).append('\n')
                    .append(journal.size()).append('\n');
            for (Lifecycle event : journal) {
                body.append(event.sequence()).append('\t').append(event.kind()).append('\t')
                        .append(event.generation()).append('\t').append(event.jobId()).append('\t')
                        .append(encodedAlias(event.jobId(), event.jobAlias())).append('\n');
            }
            return body.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    record Token(long expiresAt, String providerInstance, String scope, long generation,
                 String jobId, String jobAlias, boolean coverageInvalid, long acknowledgedSequence) {
        boolean matches(RequestSnapshot request) {
            return providerInstance.equals(request.providerInstance()) && scope.equals(request.scope())
                    && generation == request.generation() && jobId.equals(request.jobId())
                    && jobAlias.equals(request.jobAlias()) && coverageInvalid == request.coverageInvalid()
                    && acknowledgedSequence == request.sentThrough();
        }
    }

    static boolean validAlias(String alias) {
        if (alias == null || alias.length() > 128 || alias.isBlank()) return false;
        byte[] bytes = alias.getBytes(StandardCharsets.UTF_8);
        return bytes.length <= 128 && new String(bytes, StandardCharsets.UTF_8).equals(alias);
    }

    private static String encodedAlias(String jobId, String alias) {
        return jobId.equals("-") ? "-" : Base64.getUrlEncoder().withoutPadding()
                .encodeToString(alias.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodedAlias(String jobId, String encoded) throws CharacterCodingException {
        if (jobId.equals("-")) {
            if (!encoded.equals("-")) throw new IllegalArgumentException("Invalid bootstrap alias");
            return "-";
        }
        byte[] bytes = Base64.getUrlDecoder().decode(encoded);
        String alias = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        if (!validAlias(alias) || !encodedAlias(jobId, alias).equals(encoded)) {
            throw new IllegalArgumentException("Invalid synthetic alias");
        }
        return alias;
    }

    private static long number(String value) {
        if (!value.matches("0|[1-9][0-9]{0,18}")) {
            throw new IllegalArgumentException("Invalid synthetic number");
        }
        return Long.parseLong(value);
    }

    static Token token(byte[] bytes) throws IOException {
        try {
            if (bytes.length > MAX_BODY) throw new IllegalArgumentException("Oversized token");
            String[] fields = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes))
                    .toString().split("\n", -1);
            if (fields.length != 13 || !fields[12].isEmpty()
                    || !fields[0].equals("flink-stability-token-v2")
                    || !UUID.fromString(fields[1]).toString().equals(fields[1])
                    || number(fields[2]) < 1 || number(fields[3]) < 1
                    || number(fields[4]) <= number(fields[3])
                    || !UUID.fromString(fields[5]).toString().equals(fields[5])
                    || !Set.of("BOOTSTRAP", "JOB").contains(fields[6])
                    || !(fields[10].equals("true") || fields[10].equals("false"))) {
                throw new IllegalArgumentException("Invalid synthetic token");
            }
            long generation = number(fields[7]);
            if (fields[6].equals("BOOTSTRAP") ? !fields[8].equals("-")
                    : generation < 1 || !fields[8].matches("[0-9a-f]{32}")) {
                throw new IllegalArgumentException("Invalid token scope");
            }
            return new Token(number(fields[4]), fields[5], fields[6], generation, fields[8],
                    decodedAlias(fields[8], fields[9]), Boolean.parseBoolean(fields[10]), number(fields[11]));
        } catch (IllegalArgumentException | CharacterCodingException invalid) {
            throw new IOException("Received bytes are not a synthetic fixture token", invalid);
        }
    }

    static long expiresAt(byte[] bytes) throws IOException {
        return token(bytes).expiresAt();
    }
}
