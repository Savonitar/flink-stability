package org.savonitar.flink.stability.core.spec.resolution;

/** The highest-precedence source that supplied an effective parameter value. */
public enum ParameterSource {
    SCENARIO_DEFAULT,
    SUITE_BINDING,
    SUBMIT_OVERRIDE,
    EXPERIMENT_BASELINE,
    EXPERIMENT_CANDIDATE
}
