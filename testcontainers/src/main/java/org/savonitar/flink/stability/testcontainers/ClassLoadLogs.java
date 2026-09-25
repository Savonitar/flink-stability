package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Names of the per-JVM class-load logs that Flink containers write to the attempt directory. */
final class ClassLoadLogs {
    static final String PREFIX = "flink-stability-class-load-";
    private static final String SUFFIX = ".log";

    private final Path directory;
    private final Map<String, Integer> incarnations = new HashMap<>();
    private final List<FlinkClassLoadLog> expected = new ArrayList<>();

    ClassLoadLogs(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    }

    static String fileName(String logicalName, int incarnation) {
        return PREFIX + logicalName + "-" + incarnation + SUFFIX;
    }

    /** Registers the path before an incarnation starts, whether or not it ever writes a file. */
    synchronized FlinkClassLoadLog register(String logicalName) {
        int incarnation = incarnations.merge(logicalName, 1, Integer::sum);
        FlinkClassLoadLog log = new FlinkClassLoadLog(logicalName + "#" + incarnation,
                directory.resolve(fileName(logicalName, incarnation)));
        expected.add(log);
        return log;
    }

    /** Expected logs, including missing files and logs from killed/replaced processes. */
    synchronized List<FlinkClassLoadLog> expected() {
        return List.copyOf(expected);
    }
}
