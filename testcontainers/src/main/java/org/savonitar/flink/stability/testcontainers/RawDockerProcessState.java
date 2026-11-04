package org.savonitar.flink.stability.testcontainers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.RawEvidenceStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

/**
 * One inspect operation supplies both typed state and a source-byte projection. The transport
 * may retry an idempotent GET within the operation budget; the receipt binds its final returned
 * body, not a claim of one wire exchange. The retained JSON
 * consists of the original Id and State value tokens joined with fixed punctuation. The rest of
 * inspect (including Config/Env) is neither retained nor logged. This is not a complete inspect body.
 */
final class RawDockerProcessState {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final RawEvidenceStore store;
    private final String apiPrefix;
    private final Transport transport;

    record Response(int status, byte[] body) {}
    @FunctionalInterface interface Transport { Response get(String path, Duration timeout) throws IOException; }

    RawDockerProcessState(RawEvidenceStore store, URI host, String apiVersion) {
        this(store, apiVersion, transport(host));
    }

    RawDockerProcessState(RawEvidenceStore store, String apiVersion, Transport transport) {
        if (apiVersion == null || !apiVersion.matches("[0-9]+\\.[0-9]+"))
            throw new IllegalArgumentException("Explicit active Docker API version is required");
        this.store = java.util.Objects.requireNonNull(store);
        this.apiPrefix = "/v" + apiVersion;
        this.transport = java.util.Objects.requireNonNull(transport);
    }

    private static Transport transport(URI host) {
        if (!"unix".equals(host.getScheme()) || host.getAuthority() != null || host.getQuery() != null
                || host.getFragment() != null || host.getPath() == null || !host.getPath().startsWith("/"))
            throw new IllegalArgumentException("Raw Docker evidence requires the same explicit local Unix endpoint");
        return (path, timeout) -> {
            try (var client = new ZerodepDockerHttpClient.Builder().dockerHost(host)
                    .connectionTimeout(timeout).responseTimeout(timeout).maxConnections(1).build();
                 var response = client.execute(DockerHttpClient.Request.builder().method("GET").path(path).build())) {
                byte[] bytes = response.getBody().readNBytes(RawEvidenceStore.MAX_RESPONSE_BYTES + 1);
                if (bytes.length > RawEvidenceStore.MAX_RESPONSE_BYTES) throw new IOException("Docker inspect exceeds raw evidence limit");
                return new Response(response.getStatusCode(), bytes);
            }
        };
    }

    FlinkHaControl.ProcessState inspect(String id, ContainerOperationDeadline deadline) {
        if (id == null || !id.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Exact container ID required");
        String endpoint = apiPrefix + "/containers/" + id + "/json";
        long started = System.nanoTime();
        try {
            Response response = transport.get(endpoint, deadline.remaining("raw Docker process inspection"));
            deadline.remaining("raw Docker process inspection");
            if (response.status() == 404) throw new NotFoundException("Owned container is absent: " + id);
            if (response.status() != 200) throw new IOException("Docker process inspection HTTP " + response.status());
            byte[] projection = project(response.body());
            var raw = store.retain(projection, "docker-inspect-id-state-source-projection",
                    "docker-process-" + (store.cursor() + 1), "GET", endpoint, response.status(), started, System.nanoTime());
            deadline.remaining("retaining Docker process inspection");
            JsonNode object = JSON.readTree(projection);
            if (!object.path("Id").isTextual() || !id.equals(object.path("Id").textValue()))
                throw new IOException("Docker inspect container identity differs");
            JsonNode state = object.path("State");
            if (!state.isObject() || !state.path("Running").isBoolean() || !state.path("Paused").isBoolean()
                    || !state.path("OOMKilled").isBoolean() || !state.path("ExitCode").isIntegralNumber()
                    || !state.path("ExitCode").canConvertToLong() || !state.path("FinishedAt").isTextual())
                throw new IOException("Docker returned incomplete process state");
            boolean running = state.path("Running").booleanValue();
            return new FlinkHaControl.ProcessState(id, running, state.path("Paused").booleanValue(),
                    running ? Optional.empty() : Optional.of(state.path("ExitCode").longValue()),
                    Optional.of(state.path("OOMKilled").booleanValue()),
                    running ? Optional.empty() : Optional.of(state.path("FinishedAt").textValue()).filter(value -> !value.isBlank()),
                    Optional.of(raw));
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException("Raw Docker process evidence unavailable", failure);
        }
    }

    static byte[] project(byte[] body) throws IOException {
        if (body == null || body.length == 0 || body.length > RawEvidenceStore.MAX_RESPONSE_BYTES)
            throw new IOException("Docker response exceeds source projection bound");
        byte[] id = null, state = null;
        try (JsonParser parser = JSON.getFactory().createParser(body)) {
            parser.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            if (parser.nextToken() != JsonToken.START_OBJECT) throw new IOException("Docker inspect is not an object");
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) throw new IOException("Malformed Docker inspect field");
                String name = parser.currentName();
                JsonToken token = parser.nextToken();
                long start = parser.currentTokenLocation().getByteOffset();
                if (token == null) throw new IOException("Incomplete Docker inspect value");
                parser.skipChildren();
                parser.finishToken();
                long end = parser.currentLocation().getByteOffset();
                if ("Id".equals(name)) id = Arrays.copyOfRange(body, Math.toIntExact(start), Math.toIntExact(end));
                if ("State".equals(name)) state = Arrays.copyOfRange(body, Math.toIntExact(start), Math.toIntExact(end));
            }
            if (parser.nextToken() != null || id == null || state == null) throw new IOException("Incomplete/trailing Docker inspect");
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream(id.length + state.length + 20);
        output.write("{\"Id\":".getBytes(StandardCharsets.US_ASCII)); output.write(id);
        output.write(",\"State\":".getBytes(StandardCharsets.US_ASCII)); output.write(state);
        output.write("}\n".getBytes(StandardCharsets.US_ASCII));
        return output.toByteArray();
    }
}
