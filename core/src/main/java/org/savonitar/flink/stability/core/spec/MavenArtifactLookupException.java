package org.savonitar.flink.stability.core.spec;

final class MavenArtifactLookupException extends Exception {
    enum Kind {
        NOT_FOUND,
        REPOSITORY_UNAVAILABLE,
        OFFLINE_MISS
    }

    private final Kind kind;

    MavenArtifactLookupException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    Kind kind() {
        return kind;
    }
}
