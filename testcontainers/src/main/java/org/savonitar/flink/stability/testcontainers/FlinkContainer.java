package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class FlinkContainer implements FlinkComponentFactory {
    private static final String CHECKPOINT_PATH = "/flink/checkpoints";
    private static final String TASK_SLOTS_PROPERTY = "taskmanager.numberOfTaskSlots: 2";
    private static final String PRIMARY_JOB_MANAGER_ALIAS = "jobmanager-1";
    private static final String COMPONENT_LABEL = "org.savonitar.flink-stability.component";
    static final int JOB_MANAGER_PORT = 8081;

    private final DockerImageName flinkImage;
    private final FlinkRuntimeTarget runtimeTarget;
    private final Network network;
    private final Path checkpointStorageRoot;
    private final Map<String, Integer> incarnations = new HashMap<>();

    /** Creates a factory for one exact image/bundle binding. */
    FlinkContainer(
            FlinkRuntimeTarget runtimeTarget,
            Network network,
            Path checkpointStorageRoot) {
        this.runtimeTarget = Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        this.flinkImage = DockerImageName.parse(runtimeTarget.imageReference())
                .asCompatibleSubstituteFor("flink");
        this.network = Objects.requireNonNull(network, "network");
        this.checkpointStorageRoot = prepareCheckpointStorage(checkpointStorageRoot);
    }

    GenericContainer<?> createJobManager(String logicalName) {
        return new VerifiedFlinkContainer(flinkImage, runtimeTarget)
                .withNetwork(network)
                .withNetworkAliases(logicalName)
                .withLabel(COMPONENT_LABEL, logicalName)
                .withExposedPorts(JOB_MANAGER_PORT)
                .withFileSystemBind(
                        checkpointStorageRoot.toString(), CHECKPOINT_PATH, BindMode.READ_WRITE)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", PRIMARY_JOB_MANAGER_ALIAS)
                .withEnv("FLINK_PROPERTIES", flinkProperties("jobmanager", logicalName))
                .withCommand("jobmanager")
                .waitingFor(Wait.forHttp("/overview")
                        .forPort(JOB_MANAGER_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)))
                .withLogConsumer(createLogConsumer("JOB_MANAGER_LOGS." + logicalName));
    }

    GenericContainer<?> createTaskManager(String logicalName) {
        return new VerifiedFlinkContainer(flinkImage, runtimeTarget)
                .withNetwork(network)
                .withNetworkAliases(logicalName)
                .withLabel(COMPONENT_LABEL, logicalName)
                .withFileSystemBind(
                        checkpointStorageRoot.toString(), CHECKPOINT_PATH, BindMode.READ_WRITE)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", PRIMARY_JOB_MANAGER_ALIAS)
                .withEnv("FLINK_PROPERTIES", flinkProperties("taskmanager", logicalName))
                .withCommand("taskmanager")
                .withLogConsumer(createLogConsumer("TASK_MANAGER_LOGS." + logicalName));
    }

    @Override
    public ContainerHandle newJobManager(String logicalName) {
        VerifiedFlinkContainer container = (VerifiedFlinkContainer) createJobManager(logicalName);
        return new TestcontainersContainerHandle(
                container, logicalName, FlinkComponentRole.JOB_MANAGER, runtimeTarget);
    }

    @Override
    public ContainerHandle newTaskManager(String logicalName) {
        VerifiedFlinkContainer container = (VerifiedFlinkContainer) createTaskManager(logicalName);
        return new TestcontainersContainerHandle(
                container, logicalName, FlinkComponentRole.TASK_MANAGER, runtimeTarget);
    }

    /**
     * Every Flink JVM logs each class it loads, with the source JAR, into the attempt's host
     * directory. One file per container incarnation, so a replaced TaskManager keeps its log.
     */
    private String flinkProperties(String process, String logicalName) {
        int incarnation = incarnations.merge(logicalName, 1, Integer::sum);
        // The per-process key is appended to env.java.opts.all, which the image uses for its
        // required --add-opens flags. The value stays unquoted: quotes would reach the JVM.
        return TASK_SLOTS_PROPERTY + "\n"
                + "env.java.opts." + process + ": -Xlog:class+load=info:file="
                + CHECKPOINT_PATH + "/" + ClassLoadLogs.fileName(logicalName, incarnation)
                + "::filecount=0";
    }

    private Slf4jLogConsumer createLogConsumer(String loggerName) {
        return new Slf4jLogConsumer(LoggerFactory.getLogger(loggerName));
    }

    Path checkpointStorageRoot() {
        return checkpointStorageRoot;
    }

    FlinkRuntimeTarget runtimeTarget() {
        return runtimeTarget;
    }

    private static Path prepareCheckpointStorage(Path configuredRoot) {
        Path absolute = Objects.requireNonNull(configuredRoot, "checkpointStorageRoot")
                .toAbsolutePath()
                .normalize();
        try {
            boolean createContainerWritableDirectory = Files.notExists(absolute);
            Files.createDirectories(absolute);
            // A newly created attempt directory contains only Flink state and must be writable by
            // the image's non-host UID. Existing caller-owned directories are never chmodded.
            if (createContainerWritableDirectory) {
                makeContainerWritable(absolute);
            }
            Path real = absolute.toRealPath();
            if (!Files.isDirectory(real) || !Files.isWritable(real)) {
                throw new IllegalArgumentException(
                        "Checkpoint storage must be a writable directory: " + real);
            }
            return real;
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Could not prepare checkpoint storage " + absolute, exception);
        }
    }

    private static void makeContainerWritable(Path directory) throws IOException {
        try {
            Files.setPosixFilePermissions(
                    directory, PosixFilePermissions.fromString("rwxrwxrwx"));
        } catch (UnsupportedOperationException ignored) {
            // Docker Desktop and other non-POSIX filesystems mediate bind-mount permissions.
        }
    }
}
