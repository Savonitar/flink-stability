package org.savonitar.flink.stability.testcontainers;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Paths;

public class FlinkContainer {
    private static final String CHECKPOINT_PATH = "/flink/checkpoints";
    public static final String SAVEPOINT_PATH = "file:" + CHECKPOINT_PATH;
    private static final String LOCAL_CHECKPOINT_PATH = Paths.get("checkpoints").toAbsolutePath().toString();
    private static final String TASK_SLOTS_PROPERTY = "taskmanager.numberOfTaskSlots: 2";
    private static final String JOB_MANAGER_ALIAS = "jobmanager";
    public static final int JOB_MANAGER_PORT = 8081;

    private final DockerImageName flinkImage;
    private final Network network;

    public FlinkContainer(String imageName, Network network) {
        this.flinkImage = DockerImageName.parse(imageName).asCompatibleSubstituteFor("flink");
        this.network = network;
    }

    public GenericContainer<?> createJobManager() {
        return new GenericContainer<>(flinkImage)
                .withNetwork(network)
                .withNetworkAliases(JOB_MANAGER_ALIAS)
                .withExposedPorts(JOB_MANAGER_PORT)
                .withFileSystemBind(LOCAL_CHECKPOINT_PATH, CHECKPOINT_PATH)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", JOB_MANAGER_ALIAS)
                .withEnv("FLINK_PROPERTIES", TASK_SLOTS_PROPERTY)
                .withCommand("jobmanager")
                .withLogConsumer(createLogConsumer("JOB_MANAGER_LOGS"));
    }

    public GenericContainer<?> createTaskManager() {
        return new GenericContainer<>(flinkImage)
                .withNetwork(network)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", JOB_MANAGER_ALIAS)
                .withEnv("FLINK_PROPERTIES", TASK_SLOTS_PROPERTY)
                .withCommand("taskmanager")
                .withLogConsumer(createLogConsumer("TASK_MANAGER_LOGS"));
    }

    private Slf4jLogConsumer createLogConsumer(String loggerName) {
        return new Slf4jLogConsumer(LoggerFactory.getLogger(loggerName));
    }
}