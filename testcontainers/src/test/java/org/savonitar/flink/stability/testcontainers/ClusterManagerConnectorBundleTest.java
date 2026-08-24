package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterManagerConnectorBundleTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void installsOneManifestInEverySlotAndRetainsImmutableEvidence() throws Exception {
        FlinkRuntimeTarget target = target(
                "flink:2.2.0", "a", manifest("connector.jar", "connector"));
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink(target, 1, 2);

            List<FlinkComponentProvisioningEvidence> history =
                    manager.provisioningHistory();
            assertEquals(3, history.size());
            assertEquals(
                    List.of("jobmanager-1", "taskmanager-1", "taskmanager-2"),
                    history.stream()
                            .map(FlinkComponentProvisioningEvidence::logicalName)
                            .toList());
            assertTrue(history.stream().allMatch(
                    FlinkComponentProvisioningEvidence::verifiedBeforeProcessStart));
            assertEquals(
                    List.of(target.connectorBundle().orElseThrow()
                            .classpathManifest().manifestSha256()),
                    history.stream()
                            .map(evidence -> evidence.classpathManifestSha256().orElseThrow())
                            .distinct()
                            .toList());
            assertEquals(
                    List.of(target.connectorBundle().orElseThrow().targetBindingSha256()),
                    history.stream()
                            .map(evidence -> evidence.targetBindingSha256().orElseThrow())
                            .distinct()
                            .toList());
            assertThrows(UnsupportedOperationException.class, history::clear);
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> history.getFirst().connectorArtifacts().clear());
        }
    }

    @Test
    void componentUpgradeChangesBindingButPreservesByteManifest() throws Exception {
        ConnectorClasspathManifest manifest = manifest("connector.jar", "same-bytes");
        FlinkRuntimeTarget from = target("flink:2.2.0", "a", manifest);
        FlinkRuntimeTarget to = target("flink:2.2.1", "b", manifest);
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink(from, 1, 1);
            String oldTaskManager = manager.provisioningHistory().getLast().runtimeId();

            String replacement = manager.restartTaskManager("taskmanager-1", to);

            assertFalse(oldTaskManager.equals(replacement));
            assertEquals(3, manager.provisioningHistory().size());
            FlinkComponentProvisioningEvidence taskManager =
                    manager.provisioningHistory().getLast();
            assertEquals("flink:2.2.1", taskManager.imageReference());
            assertEquals(
                    to.connectorBundle().orElseThrow().targetBindingSha256(),
                    taskManager.targetBindingSha256().orElseThrow());
            assertEquals(
                    manifest.manifestSha256(),
                    taskManager.classpathManifestSha256().orElseThrow());

            List<String> eventsBeforeAmbiguousRestart = List.copyOf(harness.events);
            assertThrows(IllegalStateException.class, manager::restartFlink);
            assertEquals(eventsBeforeAmbiguousRestart, harness.events);

            manager.restartFlink(to);

            assertEquals(5, manager.provisioningHistory().size());
            assertEquals(
                    List.of(to.connectorBundle().orElseThrow().targetBindingSha256()),
                    manager.provisioningHistory().subList(3, 5).stream()
                            .map(evidence -> evidence.targetBindingSha256().orElseThrow())
                            .distinct()
                            .toList());
            assertTrue(manager.isJobManagerRunning("jobmanager-1"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertTrue(harness.checkpointRoots.stream()
                    .allMatch(manager.checkpointStorageRoot()::equals));
        }
    }

    @Test
    void rejectsDifferentRoleManifestBeforeStoppingTheExistingComponent() throws Exception {
        FlinkRuntimeTarget current = target(
                "flink:2.2.0", "a", manifest("connector-a.jar", "a"));
        FlinkRuntimeTarget incompatible = target(
                "flink:2.2.1", "b", manifest("connector-b.jar", "different"));
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink(current, 1, 1);
            List<String> before = List.copyOf(harness.events);

            ConnectorBundleProvisioningException failure = assertThrows(
                    ConnectorBundleProvisioningException.class,
                    () -> manager.restartTaskManager("taskmanager-1", incompatible));

            assertTrue(failure.getMessage().contains("different connector classpath manifests"));
            assertEquals(before, harness.events);
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(2, manager.provisioningHistory().size());
        }
    }

    @Test
    void rejectsMutatedDestinationBeforeStoppingAClusterForUpgrade() throws Exception {
        ConnectorClasspathManifest manifest = manifest("connector.jar", "before");
        FlinkRuntimeTarget current = target("flink:2.2.0", "a", manifest);
        FlinkRuntimeTarget destination = target("flink:2.2.1", "b", manifest);
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink(current, 1, 1);
            Files.writeString(manifest.entries().getFirst().preparedPath(), "after");
            List<String> before = List.copyOf(harness.events);

            assertThrows(
                    ConnectorBundleProvisioningException.class,
                    () -> manager.restartFlink(destination));

            assertEquals(before, harness.events);
            assertTrue(manager.isJobManagerRunning("jobmanager-1"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
        }
    }

    @Test
    void missingVerificationEvidenceFailsStartupAndCleansTheCandidate() throws Exception {
        FlinkRuntimeTarget target = target(
                "flink:2.2.0", "a", manifest("connector.jar", "connector"));
        RuntimeHarness harness = new RuntimeHarness();
        harness.omitBundleEvidence = true;
        ClusterManager manager = manager(harness);

        ConnectorBundleProvisioningException failure = assertThrows(
                ConnectorBundleProvisioningException.class,
                () -> manager.startFlink(target, 1, 1));

        assertTrue(failure.getMessage().contains("no provisioning evidence"));
        assertEquals(
                List.of(
                        "create:jobmanager-1:jobmanager-1-runtime-1:flink:2.2.0",
                        "start:jobmanager-1:jobmanager-1-runtime-1",
                        "stop:jobmanager-1:jobmanager-1-runtime-1"),
                harness.events);
        assertEquals(List.of(), manager.jobManagerNames());
        assertEquals(List.of(), manager.taskManagerNames());
        assertEquals(List.of(), manager.provisioningHistory());
        manager.close();
    }

    @Test
    void legacyStartRemainsInjectionDisabledAndRecordsNoBundleIdentity() throws Exception {
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink("flink:1.20.0", 1, 1);

            assertEquals(2, manager.provisioningHistory().size());
            assertTrue(manager.provisioningHistory().stream()
                    .allMatch(evidence -> evidence.targetBindingSha256().isEmpty()
                            && evidence.classpathManifestSha256().isEmpty()
                            && !evidence.verifiedBeforeProcessStart()));
            assertTrue(harness.targets.stream()
                    .allMatch(target -> !target.connectorBundleEnabled()));
        }
    }

    @Test
    void failedTaskManagerCandidateStartKeepsAndRetriesTheDesiredTarget() throws Exception {
        ConnectorClasspathManifest manifest = manifest("start-retry.jar", "same-bytes");
        FlinkRuntimeTarget from = target("flink:2.2.0", "a", manifest);
        FlinkRuntimeTarget to = target("flink:2.2.1", "b", manifest);
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink(from, 1, 1);
            harness.failStart.add("taskmanager-1-runtime-2");

            assertThrows(
                    IllegalStateException.class,
                    () -> manager.restartTaskManager("taskmanager-1", to));

            assertFalse(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(2, manager.provisioningHistory().size());
            assertEquals(
                    from.connectorBundle().orElseThrow().targetBindingSha256(),
                    manager.provisioningHistory().getLast()
                            .targetBindingSha256().orElseThrow());

            assertEquals(
                    "taskmanager-1-runtime-3",
                    manager.restartTaskManager("taskmanager-1"));
            assertTrue(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(
                    to.connectorBundle().orElseThrow().targetBindingSha256(),
                    manager.provisioningHistory().getLast()
                            .targetBindingSha256().orElseThrow());
            assertEquals(
                    List.of(
                            "create:taskmanager-1:taskmanager-1-runtime-2:flink:2.2.1",
                            "create:taskmanager-1:taskmanager-1-runtime-3:flink:2.2.1"),
                    replacementCreates(harness, "taskmanager-1"));
        }
    }

    @Test
    void failedBundleEvidenceKeepsAndRetriesTheDesiredTarget() throws Exception {
        ConnectorClasspathManifest manifest = manifest("evidence-retry.jar", "same-bytes");
        FlinkRuntimeTarget from = target("flink:2.2.0", "a", manifest);
        FlinkRuntimeTarget to = target("flink:2.2.1", "b", manifest);
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink(from, 1, 1);
            harness.omitBundleEvidenceFor.add("taskmanager-1-runtime-2");

            assertThrows(
                    ConnectorBundleProvisioningException.class,
                    () -> manager.restartTaskManager("taskmanager-1", to));

            assertFalse(manager.isTaskManagerRunning("taskmanager-1"));
            assertEquals(2, manager.provisioningHistory().size());
            assertEquals(
                    from.connectorBundle().orElseThrow().targetBindingSha256(),
                    manager.provisioningHistory().getLast()
                            .targetBindingSha256().orElseThrow());

            assertEquals(
                    "taskmanager-1-runtime-3",
                    manager.startTaskManager("taskmanager-1"));
            assertEquals(
                    to.connectorBundle().orElseThrow().targetBindingSha256(),
                    manager.provisioningHistory().getLast()
                            .targetBindingSha256().orElseThrow());
            assertEquals(
                    List.of(
                            "create:taskmanager-1:taskmanager-1-runtime-2:flink:2.2.1",
                            "create:taskmanager-1:taskmanager-1-runtime-3:flink:2.2.1"),
                    replacementCreates(harness, "taskmanager-1"));
        }
    }

    @Test
    void failedJobManagerEndpointKeepsDesiredTargetAndRetainsItsEvidence() throws Exception {
        ConnectorClasspathManifest manifest = manifest("endpoint-retry.jar", "same-bytes");
        FlinkRuntimeTarget from = target("flink:2.2.0", "a", manifest);
        FlinkRuntimeTarget to = target("flink:2.2.1", "b", manifest);
        RuntimeHarness harness = new RuntimeHarness();

        try (ClusterManager manager = manager(harness)) {
            manager.startFlink(from, 1, 1);
            harness.failMappedPort.add("jobmanager-1-runtime-2");

            assertThrows(
                    IllegalStateException.class,
                    () -> manager.restartJobManager("jobmanager-1", to));

            assertFalse(manager.isJobManagerRunning("jobmanager-1"));
            assertThrows(IllegalStateException.class, manager::getJobManagerRestUrl);
            assertEquals(3, manager.provisioningHistory().size());
            assertEquals(
                    to.connectorBundle().orElseThrow().targetBindingSha256(),
                    manager.provisioningHistory().getLast()
                            .targetBindingSha256().orElseThrow());

            assertEquals(
                    "jobmanager-1-runtime-3",
                    manager.restartJobManager("jobmanager-1"));
            assertEquals("http://localhost:18081", manager.getJobManagerRestUrl());
            assertEquals(4, manager.provisioningHistory().size());
            assertEquals(
                    List.of(
                            "create:jobmanager-1:jobmanager-1-runtime-2:flink:2.2.1",
                            "create:jobmanager-1:jobmanager-1-runtime-3:flink:2.2.1"),
                    replacementCreates(harness, "jobmanager-1"));
        }
    }

    private static List<String> replacementCreates(
            RuntimeHarness harness, String logicalName) {
        return harness.events.stream()
                .filter(event -> event.startsWith("create:" + logicalName + ":"))
                .skip(1)
                .toList();
    }

    private ClusterManager manager(RuntimeHarness harness) {
        return new ClusterManager(
                Network.SHARED,
                false,
                (target, network, checkpointRoot) -> harness.factory(target, checkpointRoot),
                (network, messages) -> new NoOpKafkaCluster(),
                temporaryDirectory.resolve("checkpoint-state"));
    }

    private ConnectorClasspathManifest manifest(String name, String bytes) throws Exception {
        Path path = Files.writeString(temporaryDirectory.resolve(name), bytes);
        return new ConnectorClasspathManifest(List.of(new ConnectorClasspathManifest.Entry(
                0, path, ConnectorClasspathManifest.sha256(Files.readAllBytes(path)))));
    }

    private static FlinkRuntimeTarget target(
            String image,
            String bindingCharacter,
            ConnectorClasspathManifest manifest) {
        return FlinkRuntimeTarget.withConnectorBundle(
                image,
                new FlinkConnectorBundleInstallation(
                        image,
                        List.of(new FlinkConnectorBundleInstallation.ClosureLockHash(
                                "connector", bindingCharacter.repeat(64))),
                        manifest));
    }

    private static final class RuntimeHarness {
        private final List<String> events = new ArrayList<>();
        private final List<FlinkRuntimeTarget> targets = new ArrayList<>();
        private final List<Path> checkpointRoots = new ArrayList<>();
        private final Map<String, Integer> generations = new LinkedHashMap<>();
        private final Set<String> failStart = new java.util.HashSet<>();
        private final Set<String> omitBundleEvidenceFor = new java.util.HashSet<>();
        private final Set<String> failMappedPort = new java.util.HashSet<>();
        private boolean omitBundleEvidence;

        FlinkComponentFactory factory(FlinkRuntimeTarget target, Path checkpointRoot) {
            targets.add(target);
            checkpointRoots.add(checkpointRoot);
            return new FlinkComponentFactory() {
                @Override
                public ContainerHandle newJobManager(String logicalName) {
                    return newHandle(
                            logicalName, FlinkComponentRole.JOB_MANAGER, target, 18081);
                }

                @Override
                public ContainerHandle newTaskManager(String logicalName) {
                    return newHandle(
                            logicalName, FlinkComponentRole.TASK_MANAGER, target, -1);
                }
            };
        }

        private ContainerHandle newHandle(
                String logicalName,
                FlinkComponentRole role,
                FlinkRuntimeTarget target,
                int mappedPort) {
            int generation = generations.merge(logicalName, 1, Integer::sum);
            String runtimeId = logicalName + "-runtime-" + generation;
            events.add("create:" + logicalName + ":" + runtimeId
                    + ":" + target.imageReference());
            return new RecordingHandle(
                    logicalName, role, runtimeId, target, mappedPort, this);
        }
    }

    private static final class RecordingHandle implements ContainerHandle {
        private final String logicalName;
        private final FlinkComponentRole role;
        private final String runtimeId;
        private final FlinkRuntimeTarget target;
        private final int mappedPort;
        private final RuntimeHarness harness;
        private boolean running;

        private RecordingHandle(
                String logicalName,
                FlinkComponentRole role,
                String runtimeId,
                FlinkRuntimeTarget target,
                int mappedPort,
                RuntimeHarness harness) {
            this.logicalName = logicalName;
            this.role = role;
            this.runtimeId = runtimeId;
            this.target = target;
            this.mappedPort = mappedPort;
            this.harness = harness;
        }

        @Override
        public void start() {
            harness.events.add("start:" + logicalName + ":" + runtimeId);
            if (harness.failStart.remove(runtimeId)) {
                throw new IllegalStateException("start failed: " + runtimeId);
            }
            running = true;
        }

        @Override
        public void stop() {
            harness.events.add("stop:" + logicalName + ":" + runtimeId);
            running = false;
        }

        @Override
        public void kill() {
            harness.events.add("kill:" + logicalName + ":" + runtimeId);
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public int mappedPort(int containerPort) {
            if (harness.failMappedPort.remove(runtimeId)) {
                harness.events.add("mapped-port:" + logicalName + ":" + runtimeId);
                throw new IllegalStateException("mapped port failed: " + runtimeId);
            }
            return mappedPort;
        }

        @Override
        public String runtimeId() {
            return runtimeId;
        }

        @Override
        public Optional<FlinkComponentProvisioningEvidence> provisioningEvidence() {
            if (!target.connectorBundleEnabled()) {
                return Optional.of(FlinkComponentProvisioningEvidence.legacy(
                        logicalName, role, runtimeId, target.imageReference()));
            }
            if (harness.omitBundleEvidence
                    || harness.omitBundleEvidenceFor.remove(runtimeId)) {
                return Optional.empty();
            }
            FlinkConnectorBundleInstallation installation =
                    target.connectorBundle().orElseThrow();
            List<ProvisionedConnectorArtifact> artifacts =
                    installation.classpathManifest().entries().stream()
                            .map(entry -> new ProvisionedConnectorArtifact(
                                    entry.index(), entry.containerPath(), entry.sha256()))
                            .toList();
            return Optional.of(FlinkComponentProvisioningEvidence.verified(
                    logicalName,
                    role,
                    runtimeId,
                    target.imageReference(),
                    installation.targetBindingSha256(),
                    installation.classpathManifest().manifestSha256(),
                    artifacts));
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
}
