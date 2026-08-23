package org.savonitar.flink.stability.core.spec;

/** Diagnostic scope for parameter binding and materialization. */
public enum ResolutionScope {
    COMMON,
    SINGLE,
    BASELINE,
    CANDIDATE
}
