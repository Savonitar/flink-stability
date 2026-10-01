package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;

import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/** A bounded, append-only local copy of container output; failures never escape to the runner. */
final class RetainedFlinkLog implements AutoCloseable {
    private final String process;
    private final Path path;
    private long bytes;
    private boolean truncated;
    private String error;
    private String runtimeId;
    private OutputStream output;
    private boolean closed;

    RetainedFlinkLog(FlinkClassLoadLog source) {
        process = source.process();
        path = source.hostPath().resolveSibling("flink-stability-output-" + process.replace('#', '-') + ".log");
    }

    synchronized void created(String id) {
        if (closed) { error = "Container creation after output collector closed"; return; }
        if (runtimeId != null) {
            if (!runtimeId.equals(id)) {
                error = "Collector reused for another physical container";
                closeStream();
            }
            return;
        }
        runtimeId = id;
        try {
            output = new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW));
        } catch (IOException | RuntimeException failure) {
            error = failure.toString();
        }
    }

    synchronized void accept(byte[] frame) {
        if (frame == null || error != null || truncated || closed) return;
        if (runtimeId == null) { error = "Output preceded container identity"; return; }
        int count = (int) Math.min(frame.length, FlinkComponentLog.MAX_BYTES - bytes);
        try {
            if (count > 0) output.write(frame, 0, count);
            bytes += count;
            truncated = count != frame.length;
            if (bytes == FlinkComponentLog.MAX_BYTES) closeStream();
        } catch (IOException | RuntimeException failure) {
            error = failure.toString();
            closeStream();
        }
    }

    synchronized FlinkComponentLog snapshot() {
        if (output != null) {
            try { output.flush(); }
            catch (IOException | RuntimeException failure) {
                error = failure.toString();
                closeStream();
            }
        }
        return new FlinkComponentLog(process, path, truncated, Optional.ofNullable(error));
    }

    @Override public synchronized void close() {
        closed = true;
        closeStream();
    }

    private void closeStream() {
        if (output == null) return;
        try { output.close(); }
        catch (IOException | RuntimeException failure) {
            if (error == null) error = failure.toString();
        } finally { output = null; }
    }
}
