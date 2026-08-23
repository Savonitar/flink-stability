package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Suite and submit-time values to merge over scenario parameter defaults. */
public final class ResolutionRequest {
    private final Map<String, JsonNode> suiteBindings;
    private final Map<String, JsonNode> submitOverrides;

    public ResolutionRequest(
            Map<String, ? extends JsonNode> suiteBindings,
            Map<String, ? extends JsonNode> submitOverrides) {
        this.suiteBindings = immutableNodeMap(suiteBindings, "suiteBindings");
        this.submitOverrides = immutableNodeMap(submitOverrides, "submitOverrides");
    }

    public static ResolutionRequest none() {
        return new ResolutionRequest(Map.of(), Map.of());
    }

    public Map<String, JsonNode> suiteBindings() {
        return defensiveNodeMap(suiteBindings);
    }

    public Map<String, JsonNode> submitOverrides() {
        return defensiveNodeMap(submitOverrides);
    }

    Map<String, JsonNode> internalSuiteBindings() {
        return suiteBindings;
    }

    Map<String, JsonNode> internalSubmitOverrides() {
        return submitOverrides;
    }

    private static Map<String, JsonNode> immutableNodeMap(
            Map<String, ? extends JsonNode> values, String name) {
        Objects.requireNonNull(values, name);
        Map<String, JsonNode> copy = new LinkedHashMap<>();
        values.forEach((key, value) -> copy.put(
                Objects.requireNonNull(key, name + " key"),
                Objects.requireNonNull(value, name + " value").deepCopy()));
        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, JsonNode> defensiveNodeMap(Map<String, JsonNode> values) {
        Map<String, JsonNode> copy = new LinkedHashMap<>();
        values.forEach((key, value) -> copy.put(key, value.deepCopy()));
        return Collections.unmodifiableMap(copy);
    }
}
