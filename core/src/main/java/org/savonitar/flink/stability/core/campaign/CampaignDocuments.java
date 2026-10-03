package org.savonitar.flink.stability.core.campaign;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/** Versioned, path- and clock-independent serialization for campaign inputs and output. */
public final class CampaignDocuments {
    private static final ObjectMapper JSON = new ObjectMapper(
            com.fasterxml.jackson.core.JsonFactory.builder()
                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());
    private static final YAMLMapper YAML = new YAMLMapper();
    private CampaignDocuments() {}

    public static ObjectNode object() { return JSON.createObjectNode(); }

    public static ObjectNode read(byte[] bytes) throws IOException {
        if (bytes.length > 4 * 1024 * 1024) throw new IllegalArgumentException("Campaign input exceeds 4 MiB");
        JsonNode node = JSON.reader().with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .readTree(bytes);
        if (!(node instanceof ObjectNode result)) throw new IllegalArgumentException("Expected a JSON object");
        return result;
    }

    public static byte[] json(JsonNode node) { return encode(node, JSON); }
    public static byte[] yaml(JsonNode node) { return encode(node, YAML); }
    public static String digest(JsonNode node) { return Digests.sha256(json(node)); }

    public static void fields(ObjectNode node, Set<String> required, Set<String> optional) {
        Set<String> actual = new TreeSet<>();
        node.fieldNames().forEachRemaining(actual::add);
        if (!actual.containsAll(required) || actual.stream().anyMatch(k -> !required.contains(k) && !optional.contains(k)))
            throw new IllegalArgumentException("Invalid campaign fields: " + actual + "; required " + required);
    }

    private static byte[] encode(JsonNode node, ObjectMapper mapper) {
        try { return mapper.writeValueAsBytes(sorted(node)); }
        catch (IOException exception) { throw new IllegalStateException(exception); }
    }

    private static JsonNode sorted(JsonNode node) {
        if (node.isObject()) {
            ObjectNode result = object();
            Set<String> keys = new TreeSet<>(); node.fieldNames().forEachRemaining(keys::add);
            keys.forEach(key -> result.set(key, sorted(node.get(key))));
            return result;
        }
        if (node.isArray()) {
            var result = JSON.createArrayNode(); node.forEach(value -> result.add(sorted(value))); return result;
        }
        return node.deepCopy();
    }
}
