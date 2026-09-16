package org.savonitar.flink.stability.core.artifact;

/** The runtime purpose of a statically declared file artifact. */
public enum ArtifactRole {
    INPUT_FILE(false, false),
    CUSTOM_INPUT(true, true),
    SUBJECT_CONNECTOR(true, false),
    WORKLOAD_JOB(true, true),
    CUSTOM_VALIDATOR(true, true);

    private final boolean jar;
    private final boolean requiresMainClass;

    ArtifactRole(boolean jar, boolean requiresMainClass) {
        this.jar = jar;
        this.requiresMainClass = requiresMainClass;
    }

    boolean isJar() {
        return jar;
    }

    boolean requiresMainClass() {
        return requiresMainClass;
    }

    boolean permitsMavenCoordinate() {
        return this == SUBJECT_CONNECTOR;
    }

    boolean permitsBuildPattern() {
        return jar;
    }
}
