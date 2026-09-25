package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.ConnectorClasspathManifest;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterManagerTest {
    private static final Duration ACTION_TIMEOUT = Duration.ofSeconds(5);

    @TempDir
    Path temporaryDirectory;

    @Test
    void startsTheFixedV1TopologyAndReturnsItsRestEndpoint() {
        RecordingFactory factory = new RecordingFactory();

        try (ClusterManager manager = manager(factory)) {
            assertEquals("http://localhost:18081", manager.startFlink(target()));

            assertEquals(List.of("jobmanager-1"), manager.jobManagerNames());
            assertEquals(List.of("taskmanager-1"), manager.taskManagerNames());
            assertTrue(manager.isJobManagerRunning("jobmanager-1"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(
                    List.of("jobmanager-1", "taskmanager-1"),
                    manager.provisioningHistory().stream()
                            .map(FlinkComponentProvisioningEvidence::logicalName)
                            .toList());
        }
    }

    @Test
    void killDoesNotQueryTheRemovedHandleAndRestartReusesTheVerifiedTarget() {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            manager.startFlink(target());

            assertEquals("taskmanager-1-runtime-1",
                    manager.killTaskManager("taskmanager-1", ACTION_TIMEOUT));
            assertFalse(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals("taskmanager-1-runtime-2",
                    manager.restartTaskManager("taskmanager-1", ACTION_TIMEOUT));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(3, manager.provisioningHistory().size());
            assertEquals(
                    target().connectorBundle().targetBindingSha256(),
                    manager.provisioningHistory().getLast().targetBindingSha256());
        }
    }

    @Test
    void runtimeKeepsExpectedLogsAcrossReplacementAndFenceEvenWhenFilesAreMissing()
            throws Exception {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            DockerV1AttemptRuntime runtime = new DockerV1AttemptRuntime(manager);
            runtime.startFlink(target());
            runtime.killTaskManager("taskmanager-1", ACTION_TIMEOUT);
            runtime.restartTaskManager(ACTION_TIMEOUT);
            FlinkClassLoadLog replacement = runtime.flinkClassLoadLogs().getLast();
            Files.createDirectories(replacement.hostPath().getParent());
            Files.writeString(replacement.hostPath(), "replacement evidence");
            runtime.stopAllFlinkProcesses(ACTION_TIMEOUT);

            assertEquals(List.of("jobmanager-1#1", "taskmanager-1#1", "taskmanager-1#2"),
                    runtime.flinkClassLoadLogs().stream().map(FlinkClassLoadLog::process).toList());
            assertTrue(Files.notExists(runtime.flinkClassLoadLogs().get(1).hostPath()));
            assertTrue(Files.exists(replacement.hostPath()));
        }
    }

    @Test
    void aRetriedInitialStartKeepsTheEarlierLogInventoryWithoutDuplicates() {
        RecordingFactory factory = new RecordingFactory();
        factory.failTaskManagerStart = true;
        try (ClusterManager manager = manager(factory)) {
            assertThrows(IllegalStateException.class, () -> manager.startFlink(target()));
            factory.failTaskManagerStart = false;
            manager.startFlink(target());

            assertEquals(List.of("jobmanager-1#1", "taskmanager-1#1",
                            "jobmanager-1#2", "taskmanager-1#2"),
                    new DockerV1AttemptRuntime(manager).flinkClassLoadLogs().stream()
                            .map(FlinkClassLoadLog::process).toList());
        }
    }

    @Test
    void refusesToReplaceARunningTaskManager() {
        try (ClusterManager manager = manager(new RecordingFactory())) {
            manager.startFlink(target());

            assertThrows(
                    IllegalStateException.class,
                    () -> manager.restartTaskManager("taskmanager-1", ACTION_TIMEOUT));
        }
    }

    @Test
    void taskManagerActionUsesOneDeadlineAndDoesNotStartTheTerminalFence() {
        RecordingFactory factory = new RecordingFactory();
        AtomicLong monotonicNanos = new AtomicLong();
        try (ClusterManager manager = manager(
                factory, () -> monotonicNanos.getAndAdd(2L))) {
            manager.startFlink(target());

            ContainerOperationTimeoutException timeout = assertThrows(
                    ContainerOperationTimeoutException.class,
                    () -> manager.killTaskManager(
                            "taskmanager-1", Duration.ofNanos(5)));

            assertEquals("performing TaskManager kill for taskmanager-1", timeout.scope());
            assertEquals(Duration.ofNanos(5), timeout.timeout());
            assertFalse(manager.isTaskManagerRunning("taskmanager-1"));

            // A phase-action timeout must not set the irreversible process-fence latch. The
            // successfully removed handle is already released, so the next action can replace it.
            assertEquals("taskmanager-1-runtime-2", manager.restartTaskManager(
                    "taskmanager-1", Duration.ofSeconds(1)));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
        }
    }

    @Test
    void writeFenceKillsTaskManagerBeforeJobManagerAndDisablesRestarts() {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            manager.startFlink(target());
            factory.events.clear();

            FlinkProcessWriteFenceEvidence evidence =
                    manager.establishFlinkProcessWriteFence(Duration.ofSeconds(5));

            assertEquals(
                    List.of("taskmanager-1", "jobmanager-1"),
                    evidence.components().stream()
                            .map(FlinkProcessWriteFenceEvidence.Component::logicalName)
                            .toList());
            assertEquals(
                    List.of("fence-kill:taskmanager-1", "fence-kill:jobmanager-1"),
                    factory.events.stream().filter(event -> event.startsWith("fence-kill"))
                            .toList());
            assertThrows(
                    IllegalStateException.class,
                    () -> manager.restartTaskManager("taskmanager-1", ACTION_TIMEOUT));
        }
    }

    @Test
    void failedStartupCleansAlreadyCreatedComponentsAndAllowsClose() {
        RecordingFactory factory = new RecordingFactory();
        factory.failTaskManagerStart = true;
        ClusterManager manager = manager(factory);

        assertThrows(IllegalStateException.class, () -> manager.startFlink(target()));
        assertEquals(
                List.of("stop:taskmanager-1", "stop:jobmanager-1"),
                factory.events.stream().filter(event -> event.startsWith("stop:"))
                        .toList());

        manager.close();
        manager.close();
    }

    @Test
    void rejectsProvisioningEvidenceThatDoesNotMatchTheTarget() {
        RecordingFactory factory = new RecordingFactory();
        factory.badEvidence = true;
        ClusterManager manager = manager(factory);

        assertThrows(
                ConnectorBundleProvisioningException.class,
                () -> manager.startFlink(target()));

        manager.close();
    }

    private ClusterManager manager(RecordingFactory factory) {
        return manager(factory, System::nanoTime);
    }

    private ClusterManager manager(
            RecordingFactory factory,
            java.util.function.LongSupplier monotonicNanos) {
        return new ClusterManager(
                Network.SHARED,
                false,
                (runtimeTarget, network, checkpointRoot) -> factory.bind(runtimeTarget, checkpointRoot),
                (network, runtimeTarget) -> {
                    throw new AssertionError("Kafka must not be started");
                },
                temporaryDirectory.resolve("checkpoints"),
                monotonicNanos);
    }

    private static FlinkRuntimeTarget target() {
        String image = "flink:2.2.0";
        return FlinkRuntimeTarget.withConnectorBundle(
                image,
                new FlinkConnectorBundleInstallation(
                        image, List.of(), new ConnectorClasspathManifest(List.of())));
    }

    private static final class RecordingFactory implements FlinkComponentFactory {
        private final List<String> events = new ArrayList<>();
        private final Map<String, Integer> generations = new LinkedHashMap<>();
        private FlinkRuntimeTarget target;
        private ClassLoadLogs logs;
        private boolean failTaskManagerStart;
        private boolean badEvidence;

        private RecordingFactory bind(FlinkRuntimeTarget target, Path checkpointRoot) {
            this.target = target;
            if (logs == null) {
                this.logs = new ClassLoadLogs(checkpointRoot);
            }
            return this;
        }

        @Override
        public List<FlinkClassLoadLog> classLoadLogs() {
            return logs.expected();
        }

        @Override
        public ContainerHandle newJobManager(String logicalName) {
            return newHandle(logicalName, FlinkComponentRole.JOB_MANAGER);
        }

        @Override
        public ContainerHandle newTaskManager(String logicalName) {
            return newHandle(logicalName, FlinkComponentRole.TASK_MANAGER);
        }

        private ContainerHandle newHandle(String name, FlinkComponentRole role) {
            int generation = generations.merge(name, 1, Integer::sum);
            logs.register(name);
            return new RecordingHandle(
                    name,
                    role,
                    name + "-runtime-" + generation,
                    target,
                    events,
                    failTaskManagerStart && role == FlinkComponentRole.TASK_MANAGER,
                    badEvidence);
        }
    }

    private static final class RecordingHandle implements ContainerHandle {
        private final String name;
        private final FlinkComponentRole role;
        private final String runtimeId;
        private final FlinkRuntimeTarget target;
        private final List<String> events;
        private final boolean failStart;
        private final boolean badEvidence;
        private boolean running;
        private boolean removed;

        private RecordingHandle(
                String name,
                FlinkComponentRole role,
                String runtimeId,
                FlinkRuntimeTarget target,
                List<String> events,
                boolean failStart,
                boolean badEvidence) {
            this.name = name;
            this.role = role;
            this.runtimeId = runtimeId;
            this.target = target;
            this.events = events;
            this.failStart = failStart;
            this.badEvidence = badEvidence;
        }

        @Override
        public void start() {
            events.add("start:" + name);
            removed = false;
            running = true;
            if (failStart) {
                throw new IllegalStateException("start failed");
            }
        }

        @Override
        public void startWithin(ContainerOperationDeadline deadline) {
            deadline.remaining("test start");
            start();
        }

        @Override
        public void stop() {
            events.add("stop:" + name);
            running = false;
        }

        @Override
        public void killAndRemoveWithin(ContainerOperationDeadline deadline) {
            deadline.remaining("test kill");
            events.add("kill:" + name);
            running = false;
            removed = true;
        }

        @Override
        public void killProcessForWriteFence(ContainerOperationDeadline deadline) {
            deadline.remaining("test fence kill");
            events.add("fence-kill:" + name);
            running = false;
        }

        @Override
        public boolean isRunningWithin(ContainerOperationDeadline deadline) {
            deadline.remaining("test liveness");
            if (removed) {
                throw new IllegalStateException(
                        "Removed container has no runtime ID: " + name);
            }
            return running;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public int mappedPort(int containerPort) {
            return 18081;
        }

        @Override
        public String runtimeId() {
            if (removed) {
                throw new IllegalStateException(
                        "Removed container has no runtime ID: " + name);
            }
            return runtimeId;
        }

        @Override
        public FlinkComponentProvisioningEvidence provisioningEvidence() {
            FlinkConnectorBundleInstallation installation = target.connectorBundle();
            return FlinkComponentProvisioningEvidence.verified(
                    name,
                    role,
                    runtimeId,
                    badEvidence ? "flink:2.2.1" : target.imageReference(),
                    installation.targetBindingSha256(),
                    installation.classpathManifest().manifestSha256(),
                    List.of());
        }
    }
}
