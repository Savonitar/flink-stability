package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.core.execution.StateRecoveryReport;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Only the opt-in typed recovery tree uses this record adapter; legacy output is unchanged. */
final class StateRecoveryEvidenceRenderer {
    private StateRecoveryEvidenceRenderer() {}

    static JsonNode render(Object report, ObjectMapper mapper) {
        return mapper.valueToTree(value(report));
    }

    private static Object value(Object item) {
        if (item == null) return null;
        if (item instanceof Optional<?> optional) return optional.map(StateRecoveryEvidenceRenderer::value).orElse(null);
        if (item instanceof OptionalLong optional) return optional.isPresent() ? optional.getAsLong() : null;
        if (item instanceof Collection<?> collection) return collection.stream().map(StateRecoveryEvidenceRenderer::value).toList();
        if (item instanceof Map<?, ?> map) {
            var result = new LinkedHashMap<String, Object>();
            map.forEach((key, entry) -> result.put(key.toString(), value(entry)));
            return result;
        }
        if (item.getClass().isRecord()) {
            var result = new LinkedHashMap<String, Object>();
            for (var component : item.getClass().getRecordComponents()) {
                try { result.put(component.getName(), value(component.getAccessor().invoke(item))); }
                catch (IllegalAccessException | InvocationTargetException failure) {
                    throw new IllegalStateException("Cannot render typed state recovery evidence", failure);
                }
            }
            return result;
        }
        if (item instanceof String || item instanceof Number || item instanceof Boolean || item instanceof Enum<?>) return item;
        throw new IllegalArgumentException("Unsupported state recovery evidence type: " + item.getClass().getName());
    }
}
