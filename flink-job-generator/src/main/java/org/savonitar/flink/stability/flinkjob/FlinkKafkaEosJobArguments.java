package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.util.ParameterTool;

final class FlinkKafkaEosJobArguments {

    private final String bootstrapServers;
    private final int processingDelayMs;
    private final String transactionalIdPrefix;

    private FlinkKafkaEosJobArguments(
            String bootstrapServers,
            int processingDelayMs,
            String transactionalIdPrefix) {
        this.bootstrapServers = bootstrapServers;
        this.processingDelayMs = processingDelayMs;
        this.transactionalIdPrefix = transactionalIdPrefix;
    }

    static FlinkKafkaEosJobArguments from(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        return new FlinkKafkaEosJobArguments(
                parameters.getRequired("bootstrapServers"),
                parameters.getInt("processingDelayMs", 0),
                parameters.get("transactionalIdPrefix", "flink-stability-legacy"));
    }

    String bootstrapServers() {
        return bootstrapServers;
    }

    int processingDelayMs() {
        return processingDelayMs;
    }

    String transactionalIdPrefix() {
        return transactionalIdPrefix;
    }
}
