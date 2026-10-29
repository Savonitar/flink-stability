package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.runtime.api.StorageFaultControl;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Only the opt-in typed recovery tree uses this record adapter; legacy output is unchanged. */
final class StateRecoveryEvidenceRenderer {
    private StateRecoveryEvidenceRenderer() {}

    static JsonNode render(Object report, ObjectMapper mapper) {
        return mapper.valueToTree(value(report, Set.of()));
    }

    private static Object value(Object item, Set<String> materializedPaths) {
        if (item == null) return null;
        if (item instanceof Optional<?> optional) return optional.map(entry -> value(entry, materializedPaths)).orElse(null);
        if (item instanceof OptionalLong optional) return optional.isPresent() ? optional.getAsLong() : null;
        if (item instanceof Collection<?> collection) return collection.stream().map(entry -> value(entry, materializedPaths)).toList();
        if (item instanceof Map<?, ?> map) {
            var result = new LinkedHashMap<String, Object>();
            map.forEach((key, entry) -> result.put(key.toString(), value(entry, materializedPaths)));
            return result;
        }
        if (item instanceof StorageFaultControl.Snapshot snapshot) {
            Set<String> paths = snapshot.materializedFiles().stream().map(file -> relative(file.path())).collect(Collectors.toSet());
            return record(item, paths);
        }
        if (item instanceof StorageFaultControl.InstanceEvidence instance) {
            var result = record(item, materializedPaths);
            result.put("operationsRepresentation", "bound-subset-v1");
            result.put("operationCount", instance.operations().size());
            result.put("operationCounts", value(counts(instance.operations()), materializedPaths));
            return result;
        }
        if (item.getClass().isRecord()) return record(item, materializedPaths);
        if (item instanceof String || item instanceof Number || item instanceof Boolean || item instanceof Enum<?>) return item;
        throw new IllegalArgumentException("Unsupported state recovery evidence type: " + item.getClass().getName());
    }

    private static LinkedHashMap<String, Object> record(Object item, Set<String> materializedPaths) {
            var result = new LinkedHashMap<String, Object>();
            for (var component : item.getClass().getRecordComponents()) {
                try {
                    Object field = component.getAccessor().invoke(item);
                    if (item instanceof StorageFaultControl.InstanceEvidence instance && component.getName().equals("operations"))
                        field = instance.operations().stream().filter(operation -> bound(operation, materializedPaths)).toList();
                    result.put(component.getName(), value(field, materializedPaths));
                }
                catch (IllegalAccessException | InvocationTargetException failure) {
                    throw new IllegalStateException("Cannot render typed state recovery evidence", failure);
                }
            }
            return result;
    }

    private static final Set<String> CONTROL = Set.of("START", "READY", "ARM", "HEAL", "ABORT", "STOP", "EXPIRE", "FATAL", "REJECT");

    private static boolean bound(StorageFaultControl.Operation operation, Set<String> paths) {
        return CONTROL.contains(operation.operation())
                || operation.errno() == 5 && Set.of("create", "write").contains(operation.operation())
                || paths.contains(relative(operation.path())) && Set.of("create", "write", "read").contains(operation.operation());
    }

    private static String relative(String path) {
        if (path.startsWith("/flink/checkpoints/")) return path.substring("/flink/checkpoints/".length());
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private record OperationCount(String operation, String state, int errno, long count) {}

    private static List<OperationCount> counts(List<StorageFaultControl.Operation> operations) {
        var counts = new TreeMap<String, TreeMap<String, TreeMap<Integer, Long>>>();
        for (var operation : operations)
            counts.computeIfAbsent(operation.operation(), unused -> new TreeMap<>())
                    .computeIfAbsent(operation.state().name(), unused -> new TreeMap<>()).merge(operation.errno(), 1L, Long::sum);
        var result = new java.util.ArrayList<OperationCount>();
        counts.forEach((operation, states) -> states.forEach((state, errors) -> errors.forEach((errno, count) ->
                result.add(new OperationCount(operation, state, errno, count)))));
        return List.copyOf(result);
    }
}
