package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FlinkConfiguration;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;
import org.snakeyaml.engine.v2.schema.CoreSchema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Literal standard-YAML delivery; image entrypoint text rewriting is deliberately not involved. */
final class FlinkProcessConfiguration {
    static final String PATH = "/opt/flink/conf/config.yaml";
    static final String LAUNCHER_PATH = "/opt/flink/flink-stability-launch.sh";
    static final int MAX_BYTES = 2 * 1024 * 1024;

    private FlinkProcessConfiguration() {}

    static byte[] merge(byte[] imageConfig, String managedProperties, Map<String, String> custom,
                        String jobManagerAddress) {
        Map<String, Object> values = parse(imageConfig);
        // These are the official image's networking defaults. The opt-in launcher owns them
        // explicitly instead of asking an unknown image entrypoint to interpret properties.
        values.put("jobmanager.rpc.address", jobManagerAddress);
        values.put("blob.server.port", "6124");
        values.put("query.server.port", "6125");
        values.putAll(parse(managedProperties.getBytes(StandardCharsets.UTF_8)));
        values.putAll(FlinkConfiguration.validate(custom));
        return new Dump(DumpSettings.builder().setSchema(new CoreSchema()).setDefaultFlowStyle(FlowStyle.BLOCK)
                .setSplitLines(false).setWidth(Integer.MAX_VALUE).build())
                .dumpToString(values).getBytes(StandardCharsets.UTF_8);
    }

    static List<String> launcher(String role) {
        if (!List.of("jobmanager", "taskmanager").contains(role)) {
            throw new IllegalArgumentException("Unsupported Flink process role: " + role);
        }
        return List.of("/bin/bash", LAUNCHER_PATH, role);
    }

    static byte[] launcherBytes() {
        try (var input = java.util.Objects.requireNonNull(FlinkProcessConfiguration.class
                .getResourceAsStream("/flink-stability-launch.sh"), "Missing Flink literal-config launcher")) {
            return input.readAllBytes();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    static Map<String, String> observe(byte[] actual, byte[] expected, Map<String, String> requested,
                                       String startupLog) {
        if (startupLog == null || startupLog.length() > org.savonitar.flink.stability.runtime.api.FlinkComponentLog.MAX_BYTES) {
            throw new IllegalStateException("Flink startup configuration log is unavailable or exceeds its evidence limit");
        }
        if (!java.util.Arrays.equals(actual, expected)) {
            throw new IllegalStateException("Flink effective configuration changed: expected SHA-256 "
                    + Digests.sha256(expected) + ", observed " + Digests.sha256(actual));
        }
        Map<String, Object> loaded = parse(actual);
        Map<String, String> observations = new TreeMap<>();
        for (var entry : requested.entrySet()) {
            Object value = loaded.get(entry.getKey());
            if (!(value instanceof String text) || !entry.getValue().equals(text)) {
                throw new IllegalStateException("Flink effective configuration differs at " + entry.getKey());
            }
            String marker = " configuration property: " + entry.getKey() + ", ";
            List<String> logged = startupLog.lines()
                    .filter(line -> line.contains("Loading" + marker) || line.contains("Loading dynamic" + marker))
                    .map(line -> line.substring(line.indexOf(marker) + marker.length())).distinct().toList();
            if (!startupLog.contains("Loading" + marker) || !logged.equals(List.of(text))) {
                throw new IllegalStateException("Flink startup did not confirm the literal configuration value for "
                        + entry.getKey() + ": " + (logged.isEmpty() ? "no observation" : "mismatching observations"));
            }
            observations.put(entry.getKey(), logged.getFirst());
        }
        return Map.copyOf(observations);
    }

    private static Map<String, Object> parse(byte[] bytes) {
        if (bytes.length > MAX_BYTES) throw new IllegalArgumentException("Flink configuration exceeds 2 MiB");
        Object parsed = new Load(LoadSettings.builder().setSchema(new CoreSchema()).setAllowDuplicateKeys(false)
                .setMaxAliasesForCollections(0).setCodePointLimit(MAX_BYTES).build())
                .loadFromString(new String(bytes, StandardCharsets.UTF_8));
        if (!(parsed instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Flink standard YAML configuration must be a mapping");
        }
        Map<String, Object> result = new TreeMap<>();
        flatten("", map, result);
        return result;
    }

    private static void flatten(String prefix, Map<?, ?> values, Map<String, Object> flat) {
        for (var entry : values.entrySet()) {
            if (!(entry.getKey() instanceof String key) || key.isEmpty()) {
                throw new IllegalArgumentException("Flink configuration keys must be nonempty strings");
            }
            String path = prefix.isEmpty() ? key : prefix + "." + key;
            if (entry.getValue() instanceof Map<?, ?> children && !children.isEmpty()) {
                flatten(path, children, flat);
            } else {
                if (flat.containsKey(path)) throw new IllegalArgumentException("Duplicate flattened Flink key: " + path);
                flat.put(path, entry.getValue());
            }
        }
    }

}
