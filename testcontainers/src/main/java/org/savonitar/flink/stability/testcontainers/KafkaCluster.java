package org.savonitar.flink.stability.testcontainers;

import java.io.IOException;

interface KafkaCluster {
    void start();

    void stop();

    String getBootstrapServers();

    void createAndFillInInputTopic() throws IOException, InterruptedException;

    void checkKafkaUniqueIds(String topic, int expectedMessages) throws Exception;

    boolean waitForAndValidateKafkaOutput(String topic, int expectedMessages) throws Exception;

    boolean performFinalValidation(String topic, int expectedMessages) throws Exception;

    void printMessages(String topic, int timeoutMs, int maxMessages)
            throws InterruptedException, IOException;
}
