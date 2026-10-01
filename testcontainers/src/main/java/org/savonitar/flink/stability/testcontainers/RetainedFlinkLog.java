package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/** A bounded, append-only local copy of container output; failures never escape to the runner. */
final class RetainedFlinkLog {
    private final String process;
    private final Path path;
    private long bytes;
    private boolean truncated;
    private String error;
    private String runtimeId;

    RetainedFlinkLog(FlinkClassLoadLog source) {
        process = source.process();
        path = source.hostPath().resolveSibling("flink-stability-output-" + process.replace('#', '-') + ".log");
    }

    synchronized void created(String id) {
        if (runtimeId != null) {
            if (!runtimeId.equals(id)) error = "Collector reused for another physical container";
            return;
        }
        runtimeId = id;
        try {
            Files.createFile(path);
        } catch (IOException | RuntimeException failure) {
            error = failure.toString();
        }
    }

    synchronized void accept(byte[] frame) {
        if (frame == null || error != null || truncated) return;
        if (runtimeId == null) { error = "Output preceded container identity"; return; }
        int count = (int) Math.min(frame.length, FlinkComponentLog.MAX_BYTES - bytes);
        try (var output = Files.newOutputStream(path, StandardOpenOption.APPEND)) {
            output.write(frame, 0, count);
            bytes += count;
            truncated = count != frame.length;
        } catch (IOException | RuntimeException failure) {
            error = failure.toString();
        }
    }

    synchronized FlinkComponentLog snapshot() {
        return new FlinkComponentLog(process, path, truncated, Optional.ofNullable(error));
    }
}
