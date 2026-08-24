package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterManagerTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void startsAndTracksCanonicalNamesInOrdinalOrder() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.20.0", 1, 3);

            assertEquals(List.of("jobmanager-1"), manager.jobManagerNames());
            assertEquals(
                    List.of("taskmanager-1", "taskmanager-2", "taskmanager-3"),
                    manager.taskManagerNames());
            assertTrue(manager.isJobManagerRunning("jobmanager-1"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-2"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-3"));
            assertEquals("http://localhost:18081", manager.getJobManagerRestUrl());
            assertEquals(
                    List.of(
                            "create:jobmanager-1:jobmanager-1-runtime-1",
                            "start:jobmanager-1:jobmanager-1-runtime-1",
                            "create:taskmanager-1:taskmanager-1-runtime-1",
                            "start:taskmanager-1:taskmanager-1-runtime-1",
                            "create:taskmanager-2:taskmanager-2-runtime-1",
                            "start:taskmanager-2:taskmanager-2-runtime-1",
                            "create:taskmanager-3:taskmanager-3-runtime-1",
                            "start:taskmanager-3:taskmanager-3-runtime-1"),
                    flink.events);
        }
    }

    @Test
    void killTerminatesOnlyTheNamedSlotAndNeverCreatesAReplacement() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.20.0", 1, 2);
            flink.events.clear();

            String killedRuntime = manager.killTaskManager("taskmanager-2");

            assertEquals("taskmanager-2-runtime-1", killedRuntime);
            assertEquals(List.of("kill:taskmanager-2:taskmanager-2-runtime-1"), flink.events);
            assertFalse(manager.isTaskManagerRunning("taskmanager-2"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(1, flink.generationCount("taskmanager-2"));
        }
    }

    @Test
    void explicitStartRecreatesTheSameLogicalSlotWithANewPhysicalId() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.20.0", 1, 2);
            String killedRuntime = manager.killTaskManager("taskmanager-2");
            flink.events.clear();

            String replacementRuntime = manager.startTaskManager("taskmanager-2");

            assertNotEquals(killedRuntime, replacementRuntime);
            assertEquals("taskmanager-2-runtime-2", replacementRuntime);
            assertEquals(
                    List.of(
                            "create:taskmanager-2:taskmanager-2-runtime-2",
                            "start:taskmanager-2:taskmanager-2-runtime-2"),
                    flink.events);
            assertTrue(manager.isTaskManagerRunning("taskmanager-2"));
            assertEquals(
                    List.of("taskmanager-1", "taskmanager-2"), manager.taskManagerNames());
        }
    }

    @Test
    void gracefulStopIsNotAKillAndRequiresAnExplicitStart() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.20.0", 1, 1);
            flink.events.clear();

            assertEquals(
                    "taskmanager-1-runtime-1", manager.stopTaskManager("taskmanager-1"));

            assertEquals(List.of("stop:taskmanager-1:taskmanager-1-runtime-1"), flink.events);
            assertFalse(manager.isTaskManagerRunning("taskmanager-1"));
            assertThrows(
                    IllegalStateException.class,
                    () -> manager.stopTaskManager("taskmanager-1"));

            manager.startTaskManager("taskmanager-1");
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
        }
    }

    @Test
    void rejectsUnknownTargetsAndStartingAnAlreadyRunningSlotWithoutCreatingContainers()
            throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.20.0", 1, 1);
            flink.events.clear();

            assertThrows(
                    IllegalStateException.class,
                    () -> manager.startTaskManager("taskmanager-1"));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> manager.killTaskManager("taskmanager-2"));
            manager.setRunningJobId("job-1");
            assertThrows(
                    IllegalArgumentException.class,
                    () -> manager.killJobManager("jobmanager-2"));

            assertEquals(List.of(), flink.events);
            assertEquals(1, flink.generationCount("taskmanager-1"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals("http://localhost:18081", manager.getJobManagerRestUrl());
            assertEquals("job-1", manager.getRunningJobId());
        }
    }

    @Test
    void cleansAPartialStartInReverseOrderAndPublishesNoRegistry() {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        flink.failStart.add("taskmanager-3-runtime-1");
        ClusterManager manager = manager(flink);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> manager.startFlink("flink:1.20.0", 1, 3));

        assertEquals("start failed: taskmanager-3-runtime-1", failure.getMessage());
        assertEquals(
                List.of(
                        "create:jobmanager-1:jobmanager-1-runtime-1",
                        "start:jobmanager-1:jobmanager-1-runtime-1",
                        "create:taskmanager-1:taskmanager-1-runtime-1",
                        "start:taskmanager-1:taskmanager-1-runtime-1",
                        "create:taskmanager-2:taskmanager-2-runtime-1",
                        "start:taskmanager-2:taskmanager-2-runtime-1",
                        "create:taskmanager-3:taskmanager-3-runtime-1",
                        "start:taskmanager-3:taskmanager-3-runtime-1",
                        "stop:taskmanager-3:taskmanager-3-runtime-1",
                        "stop:taskmanager-2:taskmanager-2-runtime-1",
                        "stop:taskmanager-1:taskmanager-1-runtime-1",
                        "stop:jobmanager-1:jobmanager-1-runtime-1"),
                flink.events);
        assertEquals(List.of(), manager.jobManagerNames());
        assertEquals(List.of(), manager.taskManagerNames());
        assertThrows(IllegalStateException.class, manager::getJobManagerRestUrl);

        manager.close();
    }

    @Test
    void retriesAFailedPartialStartCleanupBeforeDiscardingTheHandle() {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        flink.failStart.add("taskmanager-2-runtime-1");
        flink.failStopOnce.add("taskmanager-2-runtime-1");
        ClusterManager manager = manager(flink);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> manager.startFlink("flink:1.20.0", 1, 2));

        assertEquals("start failed: taskmanager-2-runtime-1", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals(
                2,
                flink.events.stream()
                        .filter("stop:taskmanager-2:taskmanager-2-runtime-1"::equals)
                        .count());
        assertEquals(List.of(), manager.taskManagerNames());
        manager.close();
    }

    @Test
    void closeIsNullSafeAndIdempotentBeforeAnyComponentStarts() {
        ClusterManager manager = manager(new RecordingFlinkFactory());

        manager.close();
        manager.close();

        assertThrows(
                IllegalStateException.class,
                () -> manager.startFlink("flink:1.20.0", 1, 1));
    }

    @Test
    void closesOnlyOwnedNetworksAndRetriesAFailedNetworkClose() {
        List<String> ownedEvents = new ArrayList<>();
        AtomicBoolean failOwnedCloseOnce = new AtomicBoolean(true);
        Network owned = recordingNetwork(ownedEvents, failOwnedCloseOnce);
        ClusterManager ownedManager = new ClusterManager(
                owned,
                true,
                (image, network, checkpointRoot) -> new RecordingFlinkFactory(),
                (network, messages) -> new NoOpKafkaCluster());

        assertThrows(IllegalStateException.class, ownedManager::close);
        ownedManager.close();
        ownedManager.close();
        assertEquals(List.of("close", "close"), ownedEvents);

        List<String> borrowedEvents = new ArrayList<>();
        Network borrowed = recordingNetwork(borrowedEvents, new AtomicBoolean(false));
        ClusterManager borrowedManager = new ClusterManager(
                borrowed,
                false,
                (image, network, checkpointRoot) -> new RecordingFlinkFactory(),
                (network, messages) -> new NoOpKafkaCluster());
        borrowedManager.close();
        assertEquals(List.of(), borrowedEvents);
    }

    @Test
    void reusesOneCheckpointNamespaceAcrossFlinkVersionRestarts() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        List<Path> observedRoots = new ArrayList<>();
        Path attemptRoot = temporaryDirectory.resolve("attempt-state");
        ClusterManager manager = new ClusterManager(
                Network.SHARED,
                false,
                (image, network, checkpointRoot) -> {
                    observedRoots.add(checkpointRoot);
                    return flink;
                },
                (network, messages) -> new NoOpKafkaCluster(),
                attemptRoot);

        manager.startFlink("flink:1.19", 1, 1);
        manager.stopFlink();
        manager.startFlink("flink:1.20", 1, 1);
        manager.close();

        assertEquals(
                List.of(attemptRoot.toAbsolutePath(), attemptRoot.toAbsolutePath()),
                observedRoots);
        assertEquals(attemptRoot.toAbsolutePath(), manager.checkpointStorageRoot());
    }

    @Test
    void assignsUniqueDefaultCheckpointStorageAndRetainsCallerOwnedState() throws Exception {
        ClusterManager first = new ClusterManager();
        ClusterManager second = new ClusterManager();
        assertNotEquals(first.checkpointStorageRoot(), second.checkpointStorageRoot());
        first.close();
        second.close();

        Path callerRoot = temporaryDirectory.resolve("retained-state");
        Files.createDirectories(callerRoot);
        Files.writeString(callerRoot.resolve("state"), "caller-owned");
        ClusterManager callerOwned = new ClusterManager(callerRoot);
        callerOwned.close();
        assertTrue(Files.exists(callerRoot.resolve("state")));
    }

    @Test
    void closeStopsEveryRunningSlotOnceInReverseOrderAndIsIdempotent() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        ClusterManager manager = manager(flink);
        manager.startFlink("flink:1.20.0", 1, 3);
        flink.events.clear();

        manager.close();
        manager.close();

        assertEquals(
                List.of(
                        "stop:taskmanager-3:taskmanager-3-runtime-1",
                        "stop:taskmanager-2:taskmanager-2-runtime-1",
                        "stop:taskmanager-1:taskmanager-1-runtime-1",
                        "stop:jobmanager-1:jobmanager-1-runtime-1"),
                flink.events);
    }

    @Test
    void failedKafkaPreloadStopsTheCandidateAndDoesNotPublishIt() {
        RecordingKafkaCluster kafka = new RecordingKafkaCluster();
        kafka.failPreload = true;
        ClusterManager manager = manager(new RecordingFlinkFactory(), kafka);

        IOException failure = assertThrows(IOException.class, manager::startKafka);

        assertEquals("preload failed", failure.getMessage());
        assertEquals(List.of("start", "preload", "stop"), kafka.events);
        assertThrows(
                IllegalStateException.class,
                () -> manager.checkKafkaUniqueIds("output", 1));
        manager.close();
        assertEquals(List.of("start", "preload", "stop"), kafka.events);
    }

    @Test
    void successfulKafkaStartupIsStoppedExactlyOnceByIdempotentClose() throws Exception {
        RecordingKafkaCluster kafka = new RecordingKafkaCluster();
        ClusterManager manager = manager(new RecordingFlinkFactory(), kafka);

        manager.startKafka();
        manager.close();
        manager.close();

        assertEquals(List.of("start", "preload", "bootstrap", "stop"), kafka.events);
    }

    @Test
    void retainsKafkaForACloseRetryWhenFailureCleanupAlsoFails() {
        RecordingKafkaCluster kafka = new RecordingKafkaCluster();
        kafka.failPreload = true;
        kafka.failStopOnce = true;
        ClusterManager manager = manager(new RecordingFlinkFactory(), kafka);

        IOException failure = assertThrows(IOException.class, manager::startKafka);

        assertEquals(1, failure.getSuppressed().length);
        assertThrows(
                IllegalStateException.class,
                () -> manager.checkKafkaUniqueIds("output", 1));
        manager.close();
        manager.close();
        assertEquals(List.of("start", "preload", "stop", "stop"), kafka.events);
    }

    @SuppressWarnings("deprecation")
    @Test
    void failedAdditionalTaskManagerStartDoesNotSkipItsLogicalOrdinal() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.20.0", 1, 1);
            flink.failStart.add("taskmanager-2-runtime-1");

            assertThrows(IllegalStateException.class, manager::startNewTaskManager);
            manager.startNewTaskManager();

            assertEquals(List.of("taskmanager-1", "taskmanager-2"), manager.taskManagerNames());
            assertEquals(2, flink.generationCount("taskmanager-2"));
            assertEquals(0, flink.generationCount("taskmanager-3"));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    void retainsAFailedAdditionalTaskManagerWhenImmediateCleanupAlsoFails() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        ClusterManager manager = manager(flink);
        manager.startFlink("flink:1.20.0", 1, 1);
        flink.failStart.add("taskmanager-2-runtime-1");
        flink.failStopOnce.add("taskmanager-2-runtime-1");

        IllegalStateException failure = assertThrows(
                IllegalStateException.class, manager::startNewTaskManager);

        assertEquals(1, failure.getSuppressed().length);
        manager.close();
        assertEquals(
                2,
                flink.events.stream()
                        .filter("stop:taskmanager-2:taskmanager-2-runtime-1"::equals)
                        .count());
    }

    @Test
    void jobManagerStartStopsCandidateWhenItsRestPortCannotBePublished() throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.20.0", 1, 1);
            manager.killJobManager("jobmanager-1");
            flink.failMappedPort.add("jobmanager-1-runtime-2");
            flink.events.clear();

            assertThrows(
                    IllegalStateException.class,
                    () -> manager.startJobManager("jobmanager-1"));

            assertEquals(
                    List.of(
                            "create:jobmanager-1:jobmanager-1-runtime-2",
                            "start:jobmanager-1:jobmanager-1-runtime-2",
                            "mapped-port:jobmanager-1:jobmanager-1-runtime-2",
                            "stop:jobmanager-1:jobmanager-1-runtime-2"),
                    flink.events);
            assertFalse(manager.isJobManagerRunning("jobmanager-1"));
            assertThrows(IllegalStateException.class, manager::getJobManagerRestUrl);

            assertEquals("jobmanager-1-runtime-3", manager.startJobManager("jobmanager-1"));
            assertTrue(manager.isJobManagerRunning("jobmanager-1"));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    void legacyKillThenStartSequenceRestartsThePrimarySlotWithoutLeakingAnExtraWorker()
            throws Exception {
        RecordingFlinkFactory flink = new RecordingFlinkFactory();
        try (ClusterManager manager = manager(flink)) {
            manager.startFlink("flink:1.19", 1, 1);

            manager.simulateTaskManagerFailureAndRecovery();
            assertFalse(manager.isTaskManagerRunning("taskmanager-1"));
            manager.startNewTaskManager();

            assertEquals(List.of("taskmanager-1"), manager.taskManagerNames());
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(2, flink.generationCount("taskmanager-1"));
        }
    }

    private static ClusterManager manager(RecordingFlinkFactory flink) {
        return manager(flink, new NoOpKafkaCluster());
    }

    private static ClusterManager manager(
            RecordingFlinkFactory flink, KafkaCluster kafka) {
        return new ClusterManager(
                Network.SHARED,
                false,
                (image, network, checkpointRoot) -> flink,
                (network, messages) -> kafka);
    }

    private static Network recordingNetwork(
            List<String> events, AtomicBoolean failCloseOnce) {
        return (Network) Proxy.newProxyInstance(
                Network.class.getClassLoader(),
                new Class<?>[]{Network.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "close" -> {
                        events.add("close");
                        if (failCloseOnce.compareAndSet(true, false)) {
                            throw new IllegalStateException("network close failed");
                        }
                        yield null;
                    }
                    case "getId" -> "recording-network";
                    case "apply" -> args[0];
                    case "toString" -> "recording-network";
                    default -> null;
                });
    }

    private static final class RecordingFlinkFactory implements FlinkComponentFactory {
        private final List<String> events = new ArrayList<>();
        private final Map<String, Integer> generations = new LinkedHashMap<>();
        private final Set<String> failStart = new java.util.HashSet<>();
        private final Set<String> failStopOnce = new java.util.HashSet<>();
        private final Set<String> failMappedPort = new java.util.HashSet<>();

        @Override
        public ContainerHandle newJobManager(String logicalName) {
            return newHandle(logicalName, 18081);
        }

        @Override
        public ContainerHandle newTaskManager(String logicalName) {
            return newHandle(logicalName, -1);
        }

        int generationCount(String logicalName) {
            return generations.getOrDefault(logicalName, 0);
        }

        private ContainerHandle newHandle(String logicalName, int mappedPort) {
            int generation = generations.merge(logicalName, 1, Integer::sum);
            String runtimeId = logicalName + "-runtime-" + generation;
            events.add("create:" + logicalName + ":" + runtimeId);
            return new RecordingHandle(
                    runtimeId, mappedPort, events, failStart, failStopOnce, failMappedPort);
        }
    }

    private static final class RecordingHandle implements ContainerHandle {
        private final String runtimeId;
        private final int mappedPort;
        private final List<String> events;
        private final Set<String> failStart;
        private final Set<String> failStopOnce;
        private final Set<String> failMappedPort;
        private boolean running;

        private RecordingHandle(
                String runtimeId,
                int mappedPort,
                List<String> events,
                Set<String> failStart,
                Set<String> failStopOnce,
                Set<String> failMappedPort) {
            this.runtimeId = runtimeId;
            this.mappedPort = mappedPort;
            this.events = events;
            this.failStart = failStart;
            this.failStopOnce = failStopOnce;
            this.failMappedPort = failMappedPort;
        }

        @Override
        public void start() {
            events.add("start:" + logicalName() + ":" + runtimeId);
            if (failStart.remove(runtimeId)) {
                throw new IllegalStateException("start failed: " + runtimeId);
            }
            running = true;
        }

        @Override
        public void stop() {
            events.add("stop:" + logicalName() + ":" + runtimeId);
            if (failStopOnce.remove(runtimeId)) {
                throw new IllegalStateException("stop failed: " + runtimeId);
            }
            running = false;
        }

        @Override
        public void kill() {
            events.add("kill:" + logicalName() + ":" + runtimeId);
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public int mappedPort(int containerPort) {
            if (failMappedPort.remove(runtimeId)) {
                events.add("mapped-port:" + logicalName() + ":" + runtimeId);
                throw new IllegalStateException("mapped port failed: " + runtimeId);
            }
            return mappedPort;
        }

        @Override
        public String runtimeId() {
            return runtimeId;
        }

        private String logicalName() {
            return runtimeId.substring(0, runtimeId.indexOf("-runtime-"));
        }
    }

    private static final class NoOpKafkaCluster implements KafkaCluster {
        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public String getBootstrapServers() {
            return "kafka:9092";
        }

        @Override
        public void createAndFillInInputTopic() {
        }

        @Override
        public void checkKafkaUniqueIds(String topic, int expectedMessages) {
        }

        @Override
        public boolean waitForAndValidateKafkaOutput(String topic, int expectedMessages) {
            return true;
        }

        @Override
        public boolean performFinalValidation(String topic, int expectedMessages) {
            return true;
        }

        @Override
        public void printMessages(String topic, int timeoutMs, int maxMessages)
                throws InterruptedException, IOException {
        }
    }

    private static final class RecordingKafkaCluster implements KafkaCluster {
        private final List<String> events = new ArrayList<>();
        private boolean failPreload;
        private boolean failStopOnce;

        @Override
        public void start() {
            events.add("start");
        }

        @Override
        public void stop() {
            events.add("stop");
            if (failStopOnce) {
                failStopOnce = false;
                throw new IllegalStateException("stop failed");
            }
        }

        @Override
        public String getBootstrapServers() {
            events.add("bootstrap");
            return "kafka:9092";
        }

        @Override
        public void createAndFillInInputTopic() throws IOException {
            events.add("preload");
            if (failPreload) {
                throw new IOException("preload failed");
            }
        }

        @Override
        public void checkKafkaUniqueIds(String topic, int expectedMessages) {
        }

        @Override
        public boolean waitForAndValidateKafkaOutput(String topic, int expectedMessages) {
            return true;
        }

        @Override
        public boolean performFinalValidation(String topic, int expectedMessages) {
            return true;
        }

        @Override
        public void printMessages(String topic, int timeoutMs, int maxMessages) {
        }
    }
}
