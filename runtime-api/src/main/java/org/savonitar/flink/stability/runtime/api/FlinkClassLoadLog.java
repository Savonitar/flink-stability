package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;
import java.util.Objects;

/**
 * The JVM class-load log of one Flink process incarnation, e.g. {@code taskmanager-1#2} for
 * the second container of {@code taskmanager-1}. Each line names a loaded class and its source.
 */
public record FlinkClassLoadLog(String process, Path hostPath) {
    public FlinkClassLoadLog {
        Objects.requireNonNull(process, "process");
        Objects.requireNonNull(hostPath, "hostPath");
        if (process.isBlank()) {
            throw new IllegalArgumentException("process must not be blank");
        }
    }
}
