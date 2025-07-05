package org.savonitar.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class ClusterManager {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
    private final Network network = Network.newNetwork();
    private ConfluentKafkaContainer kafka;
    private GenericContainer<?> jobManager;
    private GenericContainer<?> taskManager;

    public void startKafka() {
        LOG.info("Start Kafka");
        kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
                .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092,BROKER://kafka:9094")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
                .withStartupTimeout(Duration.ofSeconds(120))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("KAFKA_CONTAINER_LOGS")));

        kafka.start();
        LOG.info("Kafka started at: {}", kafka.getBootstrapServers());
    }

    public void startFlink(String version) {
        DockerImageName flinkImage = DockerImageName.parse("flink:" + version);

        jobManager = new GenericContainer<>(flinkImage)
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", "jobmanager")
                .withEnv("FLINK_PROPERTIES", "taskmanager.numberOfTaskSlots: 2")
                .withCommand("jobmanager");

        taskManager = new GenericContainer<>(flinkImage)
                .withNetwork(network)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", "jobmanager")
                .withEnv("FLINK_PROPERTIES", "taskmanager.numberOfTaskSlots: 2")
                .withCommand("taskmanager");

        jobManager.start();
        taskManager.start();

        LOG.info("Flink JobManager started at: http://localhost:{}", jobManager.getMappedPort(8081));
    }

    public void stopFlink() {
        if (taskManager != null) taskManager.stop();
        if (jobManager != null) jobManager.stop();
    }


    public void stopAll() {
        if (taskManager != null) {
            taskManager.stop();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }
}
