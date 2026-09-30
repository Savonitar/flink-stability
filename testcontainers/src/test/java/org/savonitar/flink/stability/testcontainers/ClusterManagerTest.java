package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.ConnectorClasspathManifest;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterManagerTest {
    private static final Duration ACTION_TIMEOUT = Duration.ofSeconds(5);
    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);
    private static final String OTHER_IMAGE_ID = "sha256:" + "b".repeat(64);

    @TempDir
    Path temporaryDirectory;

    @Test
    void endpointAdapterPreservesTypedBudgetExpiryAndDoesNotUnwrapOtherResolverFailures() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        AtomicLong nanos = new AtomicLong();
        AtomicBoolean expire = new AtomicBoolean();
        AtomicReference<RuntimeException> resolverFailure = new AtomicReference<>();
        AtomicReference<ContainerOperationTimeoutException> expired = new AtomicReference<>();
        var configuration = new FlinkRuntimeTarget.HighAvailability("zookeeper:3.9.3", Duration.ofSeconds(6));
        try (ClusterManager manager = new ClusterManager(Network.SHARED, false,
                (target, network, storage) -> factory.bind(target, storage),
                (network, target) -> { throw new AssertionError("Kafka must not be started"); },
                temporaryDirectory.resolve("endpoint-timeout"), nanos::get,
                (network, target, clock) -> new FlinkHaRuntime(target, clock, deadline -> {
                    if (expire.get()) {
                        nanos.addAndGet(deadline.remaining("reading ZooKeeper leader records").toNanos());
                        try {
                            deadline.remaining("reading ZooKeeper leader records");
                            throw new AssertionError("The supplied deadline must have expired");
                        } catch (ContainerOperationTimeoutException timeout) {
                            expired.set(timeout);
                            throw timeout;
                        }
                    }
                    if (resolverFailure.get() != null) throw resolverFailure.get();
                    var handle = factory.handles.get("jobmanager-1");
                    var leader = new FlinkHaControl.LeaderIdentity("jobmanager-1", handle.runtimeId(),
                            "http://" + handle.advertisedAlias() + ":8081", "session-1");
                    return Optional.of(new FlinkHaControl.Leadership(leader, leader, leader));
                }, Map.of()))) {
            DockerV1AttemptRuntime runtime = new DockerV1AttemptRuntime(manager);
            runtime.startFlink(target().withHighAvailability(configuration));
            assertEquals("http://localhost:18081", runtime.currentFlinkRestEndpoint(Duration.ofSeconds(1)));

            expire.set(true);
            TimeoutException timeout = assertThrows(TimeoutException.class,
                    () -> runtime.currentFlinkRestEndpoint(Duration.ofNanos(17)));
            assertSame(expired.get(), timeout.getCause());
            var original = assertInstanceOf(ContainerOperationTimeoutException.class, timeout.getCause());
            assertEquals(Duration.ofNanos(17), original.timeout());
            assertEquals("reading ZooKeeper leader records", original.operation());
            assertEquals(original.getMessage(), timeout.getMessage());
            assertTrue(runtime.haObservations().orElseThrow().leadership().getLast().error()
                    .orElseThrow().contains("ContainerOperationTimeoutException"));

            expire.set(false);
            for (RuntimeException failure : List.of(new IllegalStateException("observer disconnected"),
                    new IllegalStateException("resolver failure", new TimeoutException("unrelated inner timeout")))) {
                resolverFailure.set(failure);
                assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> runtime.currentFlinkRestEndpoint(Duration.ofSeconds(1))));
            }
        }
    }

    @Test
    void failedStartupRetainsOwnedHaServicesAndRejectsRetryUntilAttemptCleanup() throws Exception {
        AtomicInteger creations = new AtomicInteger();
        var configuration = new FlinkRuntimeTarget.HighAvailability("zookeeper:3.9.3", Duration.ofSeconds(6));
        try (var gate = new FlinkHaRuntime.TcpGate("127.0.0.1", 1)) {
            int ownedPort = gate.port();
            ClusterManager manager = new ClusterManager(Network.SHARED, false,
                    (target, network, storage) -> { throw new IllegalStateException("flink-factory-failed"); },
                    (network, target) -> { throw new AssertionError("Kafka is not needed"); },
                    temporaryDirectory, System::nanoTime,
                    (network, target, clock) -> {
                        creations.incrementAndGet();
                        return new FlinkHaRuntime(target, clock, unused -> Optional.empty(),
                                Map.of("jobmanager-1", gate));
                    });
            try {
                assertEquals("flink-factory-failed", assertThrows(IllegalStateException.class,
                        () -> manager.startFlink(target().withHighAvailability(configuration))).getMessage());
                assertThrows(IllegalStateException.class,
                        () -> manager.startFlink(target().withHighAvailability(configuration)));
                assertThrows(IllegalStateException.class, () -> manager.startFlink(target()));
                assertEquals(1, creations.get());
                assertFalse(gate.blocked());
            } finally {
                manager.close();
            }
            assertTrue(gate.blocked());
            try (ServerSocket reclaimed = new ServerSocket(ownedPort, 1, InetAddress.getLoopbackAddress())) {
                assertEquals(ownedPort, reclaimed.getLocalPort());
            }
        }
    }

    @Test
    void namedRestartReplacesOnlyItsTaskManagerAndFenceCoversEverySlot() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            DockerV1AttemptRuntime runtime = new DockerV1AttemptRuntime(manager);
            runtime.startFlink(target().withTaskManagers(2));
            assertEquals(List.of("taskmanager-1", "taskmanager-2"), manager.taskManagerNames());
            TaskManagerControl.Identity first = runtime.taskManagerIdentity("taskmanager-1").orElseThrow();
            TaskManagerControl.Identity second = runtime.taskManagerIdentity("taskmanager-2").orElseThrow();
            assertNotEquals(first.resourceId(), second.resourceId());
            assertTrue(runtime.taskManagerIdentity("taskmanager-3").isEmpty());

            runtime.killTaskManager("taskmanager-2", ACTION_TIMEOUT);
            assertTrue(runtime.taskManagerIdentity("taskmanager-2").isEmpty());
            assertEquals(first, runtime.taskManagerIdentity("taskmanager-1").orElseThrow());
            runtime.restartTaskManager("taskmanager-2", ACTION_TIMEOUT);
            TaskManagerControl.Identity replacement = runtime.taskManagerIdentity("taskmanager-2").orElseThrow();
            assertEquals(second.logicalName(), replacement.logicalName());
            assertNotEquals(second.runtimeId(), replacement.runtimeId());
            assertNotEquals(second.resourceId(), replacement.resourceId());
            assertEquals(first, runtime.taskManagerIdentity("taskmanager-1").orElseThrow());
            assertEquals(List.of("jobmanager-1", "taskmanager-1", "taskmanager-2", "taskmanager-2"),
                    manager.provisioningHistory().stream()
                            .map(FlinkComponentProvisioningEvidence::logicalName).toList());

            FlinkProcessWriteFenceEvidence fence = runtime.stopAllFlinkProcesses(ACTION_TIMEOUT);
            assertEquals(List.of("taskmanager-2", "taskmanager-1", "jobmanager-1"),
                    fence.components().stream()
                            .map(FlinkProcessWriteFenceEvidence.Component::logicalName).toList());
            assertTrue(runtime.taskManagerIdentity("taskmanager-1").isEmpty());
            assertTrue(runtime.taskManagerIdentity("taskmanager-2").isEmpty());
            assertThrows(IllegalStateException.class,
                    () -> runtime.restartTaskManager("taskmanager-2", ACTION_TIMEOUT));
        }
    }

    @Test
    void secondTaskManagerStartupFailureCleansEveryCreatedProcessBeforeRetry() {
        RecordingFactory factory = new RecordingFactory();
        factory.failedTaskManagerName = "taskmanager-2";
        try (ClusterManager manager = manager(factory)) {
            assertThrows(IllegalStateException.class,
                    () -> manager.startFlink(target().withTaskManagers(3)));
            assertEquals(List.of("stop:taskmanager-2", "stop:taskmanager-1", "stop:jobmanager-1"),
                    factory.events.stream().filter(event -> event.startsWith("stop:")).toList());
            assertFalse(factory.events.contains("start:taskmanager-3"));
            assertTrue(manager.taskManagerNames().isEmpty());
            assertTrue(manager.taskManagerIdentity("taskmanager-1").isEmpty());
            assertEquals(2, manager.provisioningHistory().size());

            factory.failedTaskManagerName = null;
            manager.startFlink(target().withTaskManagers(3));
            assertEquals(List.of("taskmanager-1", "taskmanager-2", "taskmanager-3"),
                    manager.taskManagerNames());
        }
    }

    @Test
    void identityInspectionHonorsItsDeadline() {
        AtomicLong clock = new AtomicLong();
        try (ClusterManager manager = manager(new RecordingFactory(),
                () -> clock.getAndAdd(Duration.ofSeconds(31).toNanos()))) {
            manager.startFlink(target());
            ContainerOperationTimeoutException failure = assertThrows(
                    ContainerOperationTimeoutException.class,
                    () -> manager.taskManagerIdentity("taskmanager-1"));
            assertEquals("reading TaskManager identity for taskmanager-1", failure.scope());
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
        }
    }

    @Test
    void identityInspectionUsesTheCallerBudgetAndRejectsNonpositiveTimeouts() {
        AtomicLong clock = new AtomicLong();
        try (ClusterManager manager = manager(new RecordingFactory(),
                () -> clock.getAndAdd(Duration.ofMillis(2).toNanos()))) {
            manager.startFlink(target());
            ContainerOperationTimeoutException failure = assertThrows(
                    ContainerOperationTimeoutException.class,
                    () -> manager.taskManagerIdentity("taskmanager-1", Duration.ofMillis(1)));
            assertEquals(Duration.ofMillis(1), failure.timeout());
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            for (Duration invalid : List.of(Duration.ZERO, Duration.ofNanos(-1))) {
                assertThrows(IllegalArgumentException.class,
                        () -> manager.taskManagerIdentity("taskmanager-1", invalid));
                assertThrows(IllegalArgumentException.class,
                        () -> manager.taskManagerIdentity("taskmanager-99", invalid));
            }
        }
    }

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
    void missingRequestedRuntimeJarEvidencePreventsSuccessfulProvisioning() {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            FlinkRuntimeTarget.RuntimeJar jar = new FlinkRuntimeTarget.RuntimeJar(
                    "/opt/flink/lib/flink-dist-2.2.0.jar", "c".repeat(64));
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> manager.startFlink(target().withExpectedRuntimeJar(jar)));

            assertTrue(failure.getMessage().contains("runtime JAR evidence"));
            assertTrue(manager.provisioningHistory().isEmpty());
            assertTrue(factory.events.contains("stop:jobmanager-1"));
        }
    }

    @Test
    void validatesDeclaredImageIdentityAndRetainsItForEachPhysicalProcess() {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            manager.startFlink(target().withExpectedImageId(IMAGE_ID));
            assertEquals(List.of(IMAGE_ID, IMAGE_ID), manager.provisioningHistory().stream()
                    .map(FlinkComponentProvisioningEvidence::imageId).toList());
        }
        try (ClusterManager manager = manager(new RecordingFactory())) {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> manager.startFlink(target().withExpectedImageId(OTHER_IMAGE_ID)));
            assertTrue(failure.getMessage().contains("expected " + OTHER_IMAGE_ID));
            assertTrue(failure.getMessage().contains("actual " + IMAGE_ID));
            assertTrue(manager.provisioningHistory().isEmpty());
        }
    }

    @Test
    void rejectsDifferentImagesUnderTheSameTagForInitialComponents() {
        RecordingFactory factory = new RecordingFactory();
        factory.taskManagerImageId = OTHER_IMAGE_ID;
        try (ClusterManager manager = manager(factory)) {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> manager.startFlink(target()));

            assertTrue(failure.getMessage().contains("expected " + IMAGE_ID));
            assertTrue(failure.getMessage().contains("actual " + OTHER_IMAGE_ID));
            assertEquals(1, manager.provisioningHistory().size());
            assertEquals(IMAGE_ID, manager.provisioningHistory().getFirst().imageId());
            assertTrue(factory.events.contains("stop:taskmanager-1"));
            assertTrue(factory.events.contains("stop:jobmanager-1"));
        }
    }

    @Test
    void failedReplacementCannotChangeTheImageOrEraseEarlierEvidence() {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            manager.startFlink(target());
            manager.killTaskManager("taskmanager-1", ACTION_TIMEOUT);
            factory.imageId = OTHER_IMAGE_ID;

            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> manager.restartTaskManager("taskmanager-1", ACTION_TIMEOUT));
            assertTrue(failure.getMessage().contains("expected " + IMAGE_ID));
            assertTrue(failure.getMessage().contains("actual " + OTHER_IMAGE_ID));
            assertFalse(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(List.of(IMAGE_ID, IMAGE_ID), manager.provisioningHistory().stream()
                    .map(FlinkComponentProvisioningEvidence::imageId).toList());

            factory.imageId = IMAGE_ID;
            manager.restartTaskManager("taskmanager-1", ACTION_TIMEOUT);
            assertEquals(List.of(IMAGE_ID, IMAGE_ID, IMAGE_ID), manager.provisioningHistory().stream()
                    .map(FlinkComponentProvisioningEvidence::imageId).toList());
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

            assertEquals(IMAGE_ID, factory.target.expectedImageId().orElseThrow());
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
    void snapshotsAllProcessesBeforeFirstFenceKillAndRetainsPartialOutcomesAfterFailure() {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            manager.startFlink(target());
            factory.events.clear();
            factory.handles.get("jobmanager-1").fenceFailure = new IllegalStateException("kill unavailable");
            assertThrows(IllegalStateException.class,
                    () -> manager.establishFlinkProcessWriteFence(Duration.ofSeconds(5)));
            assertEquals(List.of("inspect:taskmanager-1", "inspect:jobmanager-1", "fence-kill:taskmanager-1"),
                    factory.events.subList(0, 3));
            var snapshot = manager.processObservations();
            assertEquals(2, snapshot.observations().stream().filter(observation ->
                    observation.moment() == FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE).count());
            assertEquals(List.of("taskmanager-1"), snapshot.fenced().stream()
                    .map(FlinkProcessWriteFenceEvidence.Component::logicalName).toList());
            manager.close();
            assertEquals(snapshot, manager.processObservations());
        }
    }

    @Test
    void expiredSharedTelemetryBudgetStillAttemptsPhysicalKillsUnderOriginalFenceDeadline() {
        RecordingFactory factory = new RecordingFactory();
        AtomicLong clock = new AtomicLong();
        try (ClusterManager manager = manager(factory, clock::get)) {
            manager.startFlink(target());
            factory.handles.get("taskmanager-1").duringInspection =
                    () -> clock.set(Duration.ofSeconds(11).toNanos());
            var fence = manager.establishFlinkProcessWriteFence(Duration.ofMinutes(2));
            assertEquals(2, fence.components().size());
            assertEquals(List.of("fence-kill:taskmanager-1", "fence-kill:jobmanager-1"),
                    factory.events.stream().filter(event -> event.startsWith("fence-kill:")).toList());
            var snapshots = manager.processObservations().observations().stream()
                    .filter(item -> item.moment() == FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE).toList();
            assertEquals(2, snapshots.size());
            assertTrue(snapshots.stream().allMatch(item -> item.state().isEmpty() && item.diagnostic().isPresent()));
            assertEquals(2, manager.processObservations().observations().stream()
                    .filter(item -> item.moment() == FlinkProcessWriteFenceEvidence.Moment.AFTER_FENCE_KILL).count());
        }
    }

    @Test
    void observedTerminationIsRetainedWhenLaterFenceConfirmationFails() {
        RecordingFactory factory = new RecordingFactory();
        try (ClusterManager manager = manager(factory)) {
            manager.startFlink(target());
            factory.handles.get("taskmanager-1").failLivenessAfterKill = true;
            assertThrows(IllegalStateException.class,
                    () -> manager.establishFlinkProcessWriteFence(Duration.ofSeconds(5)));
            assertTrue(manager.processObservations().observations().stream().anyMatch(item ->
                    item.logicalName().equals("taskmanager-1")
                            && item.moment() == FlinkProcessWriteFenceEvidence.Moment.AFTER_FENCE_KILL
                            && item.state().filter(state -> !state.running()).isPresent()));
            assertEquals(List.of("jobmanager-1"), manager.processObservations().fenced().stream()
                    .map(FlinkProcessWriteFenceEvidence.Component::logicalName).toList());
        }
    }

    @Test
    void unexpectedExitAndUnavailableInspectionRemainDistinctFromFenceSuccess() {
        for (boolean stopped : List.of(true, false)) {
            RecordingFactory factory = new RecordingFactory();
            try (ClusterManager manager = manager(factory)) {
                manager.startFlink(target());
                var handle = factory.handles.get("jobmanager-1");
                if (stopped) {
                    handle.running = false;
                } else {
                    handle.inspectionFailure = new IllegalStateException("inspect unavailable");
                }
                var fence = manager.establishFlinkProcessWriteFence(Duration.ofSeconds(5));
                var observation = manager.processObservations().observations().stream()
                        .filter(item -> item.logicalName().equals("jobmanager-1")
                                && item.moment() == FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE)
                        .findFirst().orElseThrow();
                assertEquals(stopped, observation.state().isPresent());
                assertEquals(!stopped, observation.diagnostic().isPresent());
                assertEquals(stopped ? FlinkProcessWriteFenceEvidence.Outcome.ALREADY_STOPPED
                                : FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED,
                        fence.components().getLast().outcome());
            }
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
        private final Map<String, RecordingHandle> handles = new LinkedHashMap<>();
        private FlinkRuntimeTarget target;
        private ClassLoadLogs logs;
        private boolean failTaskManagerStart;
        private String failedTaskManagerName;
        private boolean badEvidence;
        private String imageId = IMAGE_ID;
        private String taskManagerImageId;

        @Override
        public void configureHighAvailability(FlinkHaRuntime runtime) {
            // Recorded handles need no container configuration; routing still uses the real HA adapter.
        }

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
            RecordingHandle handle = new RecordingHandle(
                    name,
                    role,
                    name + "-runtime-" + generation,
                    target,
                    role == FlinkComponentRole.TASK_MANAGER && taskManagerImageId != null
                            ? taskManagerImageId : imageId,
                    events,
                    (failTaskManagerStart && role == FlinkComponentRole.TASK_MANAGER)
                            || name.equals(failedTaskManagerName),
                    badEvidence);
            handles.put(name, handle);
            return handle;
        }
    }

    private static final class RecordingHandle implements ContainerHandle {
        private final String name;
        private final FlinkComponentRole role;
        private final String runtimeId;
        private final FlinkRuntimeTarget target;
        private final String imageId;
        private final List<String> events;
        private final boolean failStart;
        private final boolean badEvidence;
        private boolean running;
        private boolean removed;
        private RuntimeException fenceFailure;
        private RuntimeException inspectionFailure;
        private Runnable duringInspection = () -> {};
        private boolean failLivenessAfterKill;

        private RecordingHandle(
                String name,
                FlinkComponentRole role,
                String runtimeId,
                FlinkRuntimeTarget target,
                String imageId,
                List<String> events,
                boolean failStart,
                boolean badEvidence) {
            this.name = name;
            this.role = role;
            this.runtimeId = runtimeId;
            this.target = target;
            this.imageId = imageId;
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
        public org.savonitar.flink.stability.runtime.api.FlinkHaControl.ProcessState killAndRemoveWithin(ContainerOperationDeadline deadline) {
            deadline.remaining("test kill");
            events.add("kill:" + name);
            running = false;
            removed = true;
            return new org.savonitar.flink.stability.runtime.api.FlinkHaControl.ProcessState(runtimeId, false, false);
        }

        @Override
        public org.savonitar.flink.stability.runtime.api.FlinkHaControl.ProcessState killProcessForWriteFence(ContainerOperationDeadline deadline) {
            deadline.remaining("test fence kill");
            events.add("fence-kill:" + name);
            if (fenceFailure != null) {
                throw fenceFailure;
            }
            running = false;
            return new org.savonitar.flink.stability.runtime.api.FlinkHaControl.ProcessState(runtimeId, false, false);
        }

        @Override
        public org.savonitar.flink.stability.runtime.api.FlinkHaControl.ProcessState processState(
                ContainerOperationDeadline deadline) {
            deadline.remaining("test state");
            events.add("inspect:" + name);
            duringInspection.run();
            deadline.remaining("completing test state inspection");
            if (inspectionFailure != null) {
                throw inspectionFailure;
            }
            return new org.savonitar.flink.stability.runtime.api.FlinkHaControl.ProcessState(runtimeId(), running, false);
        }

        @Override
        public boolean isRunningWithin(ContainerOperationDeadline deadline) {
            deadline.remaining("test liveness");
            if (!running && failLivenessAfterKill) {
                throw new IllegalStateException("post-kill inspection failed");
            }
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
        public String advertisedAlias() {
            return runtimeId;
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
        public Optional<TaskManagerControl.Identity> taskManagerIdentity() {
            return role == FlinkComponentRole.TASK_MANAGER && running
                    ? Optional.of(new TaskManagerControl.Identity(name, runtimeId, "resource-" + runtimeId))
                    : Optional.empty();
        }

        @Override
        public FlinkComponentProvisioningEvidence provisioningEvidence() {
            FlinkConnectorBundleInstallation installation = target.connectorBundle();
            return FlinkComponentProvisioningEvidence.verified(
                    name,
                    role,
                    runtimeId,
                    badEvidence ? "flink:2.2.1" : target.imageReference(),
                    imageId,
                    installation.targetBindingSha256(),
                    installation.classpathManifest().manifestSha256(),
                    List.of());
        }
    }
}
