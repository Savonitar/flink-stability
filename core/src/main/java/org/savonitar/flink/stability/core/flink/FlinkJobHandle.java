package org.savonitar.flink.stability.core.flink;

/** Stable handle returned by Flink after one workload submission. */
public record FlinkJobHandle(String jobId) {
    public FlinkJobHandle {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
    }
}
