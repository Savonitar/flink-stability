package org.savonitar.flink.stability.testcontainers;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Objects;

public class FlinkContainer implements FlinkComponentFactory {
    private static final String CHECKPOINT_PATH = "/flink/checkpoints";
    public static final String SAVEPOINT_PATH = "file:" + CHECKPOINT_PATH;
    private static final String TASK_SLOTS_PROPERTY = "taskmanager.numberOfTaskSlots: 2";
    private static final String LEGACY_JOB_MANAGER_ALIAS = "jobmanager";
    private static final String PRIMARY_JOB_MANAGER_ALIAS = "jobmanager-1";
    private static final String COMPONENT_LABEL = "org.savonitar.flink-stability.component";
    public static final int JOB_MANAGER_PORT = 8081;

    private final DockerImageName flinkImage;
    private final FlinkRuntimeTarget runtimeTarget;
    private final Network network;
    private final Path checkpointStorageRoot;

    /** Legacy direct-construction path; prefer the explicit attempt directory overload. */
    @Deprecated
    public FlinkContainer(String imageName, Network network) {
        this(FlinkRuntimeTarget.legacy(imageName), network, Path.of("checkpoints", "legacy"));
    }

    /**
     * Creates a component factory sharing one checkpoint directory. Existing directories must be
     * writable by the Flink container UID; a newly created isolated directory is made writable.
     */
    public FlinkContainer(String imageName, Network network, Path checkpointStorageRoot) {
        this(FlinkRuntimeTarget.legacy(imageName), network, checkpointStorageRoot);
    }

    /** Creates a factory for one exact image/bundle binding. */
    public FlinkContainer(
            FlinkRuntimeTarget runtimeTarget,
            Network network,
            Path checkpointStorageRoot) {
        this.runtimeTarget = Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        this.flinkImage = DockerImageName.parse(runtimeTarget.imageReference())
                .asCompatibleSubstituteFor("flink");
        this.network = Objects.requireNonNull(network, "network");
        this.checkpointStorageRoot = prepareCheckpointStorage(checkpointStorageRoot);
    }

    public GenericContainer<?> createJobManager() {
        return createJobManager(PRIMARY_JOB_MANAGER_ALIAS);
    }

    GenericContainer<?> createJobManager(String logicalName) {
        return new VerifiedFlinkContainer(flinkImage, runtimeTarget)
                .withNetwork(network)
                .withNetworkAliases(logicalName, LEGACY_JOB_MANAGER_ALIAS)
                .withLabel(COMPONENT_LABEL, logicalName)
                .withExposedPorts(JOB_MANAGER_PORT)
                .withFileSystemBind(
                        checkpointStorageRoot.toString(), CHECKPOINT_PATH, BindMode.READ_WRITE)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", PRIMARY_JOB_MANAGER_ALIAS)
                .withEnv("FLINK_PROPERTIES", TASK_SLOTS_PROPERTY)
                .withCommand("jobmanager")
                .withLogConsumer(createLogConsumer("JOB_MANAGER_LOGS." + logicalName));
    }

    public GenericContainer<?> createTaskManager() {
        return createTaskManager("taskmanager-1");
    }

    GenericContainer<?> createTaskManager(String logicalName) {
        return new VerifiedFlinkContainer(flinkImage, runtimeTarget)
                .withNetwork(network)
                .withNetworkAliases(logicalName)
                .withLabel(COMPONENT_LABEL, logicalName)
                .withFileSystemBind(
                        checkpointStorageRoot.toString(), CHECKPOINT_PATH, BindMode.READ_WRITE)
                .withEnv("JOB_MANAGER_RPC_ADDRESS", PRIMARY_JOB_MANAGER_ALIAS)
                .withEnv("FLINK_PROPERTIES", TASK_SLOTS_PROPERTY)
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

    private Slf4jLogConsumer createLogConsumer(String loggerName) {
        return new Slf4jLogConsumer(LoggerFactory.getLogger(loggerName));
    }

    public Path checkpointStorageRoot() {
        return checkpointStorageRoot;
    }

    public FlinkRuntimeTarget runtimeTarget() {
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
