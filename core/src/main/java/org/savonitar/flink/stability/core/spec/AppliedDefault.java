package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

/** A schema-annotated default explicitly materialized for execution/reporting. */
public final class AppliedDefault {
    private final String path;
    private final JsonNode value;

    public AppliedDefault(String path, JsonNode value) {
        this.path = Objects.requireNonNull(path, "path");
        this.value = Objects.requireNonNull(value, "value").deepCopy();
    }

    public String path() {
        return path;
    }

    public JsonNode value() {
        return value.deepCopy();
    }
}
