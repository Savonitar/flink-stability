package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/** Names of the per-JVM class-load logs that Flink containers write to the attempt directory. */
final class ClassLoadLogs {
    static final String PREFIX = "flink-stability-class-load-";
    private static final String SUFFIX = ".log";

    private ClassLoadLogs() {}

    static String fileName(String logicalName, int incarnation) {
        return PREFIX + logicalName + "-" + incarnation + SUFFIX;
    }

    /** Every class-load log in the attempt directory, sorted by file name. */
    static List<FlinkClassLoadLog> list(Path directory) {
        try (Stream<Path> files = Files.list(directory)) {
            return files
                    .filter(file -> {
                        String name = file.getFileName().toString();
                        return name.startsWith(PREFIX) && name.endsWith(SUFFIX)
                                && name.lastIndexOf('-') > PREFIX.length();
                    })
                    .sorted()
                    .map(file -> new FlinkClassLoadLog(process(file), file))
                    .toList();
        } catch (IOException unreadable) {
            throw new UncheckedIOException(
                    "Cannot list Flink class-load logs in " + directory, unreadable);
        }
    }

    /** {@code flink-stability-class-load-taskmanager-1-2.log} becomes {@code taskmanager-1#2}. */
    private static String process(Path file) {
        String name = file.getFileName().toString();
        String stem = name.substring(PREFIX.length(), name.length() - SUFFIX.length());
        int separator = stem.lastIndexOf('-');
        return stem.substring(0, separator) + "#" + stem.substring(separator + 1);
    }
}
