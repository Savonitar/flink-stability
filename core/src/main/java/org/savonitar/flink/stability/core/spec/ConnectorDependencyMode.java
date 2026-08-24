package org.savonitar.flink.stability.core.spec;

/** How a subject connector's runtime classpath was selected. */
public enum ConnectorDependencyMode {
    /** The Maven primary's effective model supplied the runtime closure. */
    AUTO,

    /** Only declared runtime dependency roots supplied dependencies. */
    EXPLICIT
}
