package org.savonitar.flink.stability.testcontainers;

interface FlinkComponentFactory {
    ContainerHandle newJobManager(String logicalName);

    ContainerHandle newTaskManager(String logicalName);
}
