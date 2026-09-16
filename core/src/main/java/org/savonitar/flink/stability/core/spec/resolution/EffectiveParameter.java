package org.savonitar.flink.stability.core.spec.resolution;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

/** A typed effective value and the source that won parameter precedence. */
public final class EffectiveParameter {
    private final JsonNode value;
    private final ParameterSource source;

    public EffectiveParameter(JsonNode value, ParameterSource source) {
        this.value = Objects.requireNonNull(value, "value").deepCopy();
        this.source = Objects.requireNonNull(source, "source");
    }

    public JsonNode value() {
        return value.deepCopy();
    }

    JsonNode internalValue() {
        return value;
    }

    public ParameterSource source() {
        return source;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof EffectiveParameter that
                && value.equals(that.value)
                && source == that.source;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, source);
    }

    @Override
    public String toString() {
        return "EffectiveParameter[value=" + value + ", source=" + source + ']';
    }
}
