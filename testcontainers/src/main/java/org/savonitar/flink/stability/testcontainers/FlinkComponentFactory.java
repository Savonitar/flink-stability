package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;

import java.util.List;

interface FlinkComponentFactory {
    ContainerHandle newJobManager(String logicalName);

    ContainerHandle newTaskManager(String logicalName);

    /** Every configured incarnation's expected log, including missing files. */
    default List<FlinkClassLoadLog> classLoadLogs() {
        return List.of();
    }
}
