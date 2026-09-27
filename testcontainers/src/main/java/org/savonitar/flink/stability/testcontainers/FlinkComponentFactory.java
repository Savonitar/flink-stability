package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;

import java.util.List;

interface FlinkComponentFactory {
    ContainerHandle newJobManager(String logicalName);

    ContainerHandle newTaskManager(String logicalName);

    default void configureHighAvailability(FlinkHaRuntime runtime) {
        throw new UnsupportedOperationException("HA configuration is unsupported");
    }

    default void configureTokenProvider(SyntheticTokenPlugin plugin, int servicePort) {
        throw new UnsupportedOperationException("Synthetic token configuration is unsupported");
    }

    /** Every configured incarnation's expected log, including missing files. */
    default List<FlinkClassLoadLog> classLoadLogs() {
        return List.of();
    }
}
