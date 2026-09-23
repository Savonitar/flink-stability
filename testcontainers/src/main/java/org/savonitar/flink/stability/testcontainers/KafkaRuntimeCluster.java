package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;

interface KafkaRuntimeCluster {
    void start();

    void stop();

    KafkaRuntimeEndpoints endpoints();
}
