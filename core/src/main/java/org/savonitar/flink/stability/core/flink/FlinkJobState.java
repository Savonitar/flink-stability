package org.savonitar.flink.stability.core.flink;

/** Flink REST job states relevant to v1 orchestration. */
public enum FlinkJobState {
    INITIALIZING,
    CREATED,
    RUNNING,
    FAILING,
    FAILED,
    CANCELLING,
    CANCELED,
    FINISHED,
    RESTARTING,
    SUSPENDED,
    RECONCILING;

    public boolean terminal() {
        return this == FAILED || this == CANCELED || this == FINISHED || this == SUSPENDED;
    }
}
