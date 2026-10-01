package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;
import java.util.Optional;

/** Retained stdout/stderr for one configured physical Flink process. Diagnostic only. */
public record FlinkComponentLog(String process, Path path, boolean truncated, Optional<String> error) {
    public static final long MAX_BYTES = 4L * 1024 * 1024;
    public FlinkComponentLog {
        Checks.requireNonBlank(process, "process");
        java.util.Objects.requireNonNull(path, "path");
        java.util.Objects.requireNonNull(error, "error");
    }
}
