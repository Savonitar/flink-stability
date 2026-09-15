package org.savonitar.flink.stability.core.flink;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Immutable, structured Flink 2.2 JAR-run request. */
public record FlinkJobSubmission(
        String uploadedJarId,
        int parallelism,
        Map<String, String> flinkConfiguration,
        List<String> programArguments) {

    public FlinkJobSubmission {
        if (uploadedJarId == null || uploadedJarId.isBlank()) {
            throw new IllegalArgumentException("uploadedJarId must not be blank");
        }
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be at least 1");
        }
        if (flinkConfiguration == null) {
            throw new NullPointerException("flinkConfiguration");
        }
        TreeMap<String, String> sortedConfiguration = new TreeMap<>();
        flinkConfiguration.forEach((key, value) -> {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("Flink configuration keys must not be blank");
            }
            if (value == null) {
                throw new IllegalArgumentException(
                        "Flink configuration value must not be null: " + key);
            }
            sortedConfiguration.put(key, value);
        });
        flinkConfiguration = Collections.unmodifiableMap(
                new LinkedHashMap<>(sortedConfiguration));
        programArguments = List.copyOf(programArguments);
        if (programArguments.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("programArguments must not contain null");
        }
    }
}
