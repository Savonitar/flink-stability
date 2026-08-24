package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkContainerTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void mountsTheSameWritableCheckpointDirectoryIntoEveryFlinkProcess() {
        FlinkContainer factory = new FlinkContainer(
                "flink:1.20.0", Network.SHARED, temporaryDirectory.resolve("attempt-a"));

        GenericContainer<?> jobManager = factory.createJobManager("jobmanager-1");
        GenericContainer<?> firstTaskManager = factory.createTaskManager("taskmanager-1");
        GenericContainer<?> replacementTaskManager = factory.createTaskManager("taskmanager-1");

        Bind jobManagerBind = onlyBind(jobManager);
        Bind firstTaskManagerBind = onlyBind(firstTaskManager);
        Bind replacementTaskManagerBind = onlyBind(replacementTaskManager);

        assertEquals(jobManagerBind.getPath(), firstTaskManagerBind.getPath());
        assertEquals(jobManagerBind.getPath(), replacementTaskManagerBind.getPath());
        assertEquals(factory.checkpointStorageRoot().toString(), jobManagerBind.getPath());
        assertTrue(Files.isWritable(factory.checkpointStorageRoot()));
        try {
            assertTrue(Files.getPosixFilePermissions(factory.checkpointStorageRoot())
                    .contains(PosixFilePermission.OTHERS_WRITE));
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX test filesystem; Docker Desktop mediates bind permissions there.
        } catch (java.io.IOException exception) {
            throw new AssertionError(exception);
        }
        assertCheckpointMount(jobManagerBind);
        assertCheckpointMount(firstTaskManagerBind);
        assertCheckpointMount(replacementTaskManagerBind);
    }

    @Test
    void assignsCanonicalLogicalAliasesAndLabels() {
        FlinkContainer factory = new FlinkContainer(
                "flink:1.20.0", Network.SHARED, temporaryDirectory.resolve("attempt-b"));

        GenericContainer<?> jobManager = factory.createJobManager("jobmanager-1");
        GenericContainer<?> taskManager = factory.createTaskManager("taskmanager-2");

        // Testcontainers prepends its own generated alias; the harness aliases remain stable.
        assertTrue(jobManager.getNetworkAliases().containsAll(List.of("jobmanager-1", "jobmanager")));
        assertTrue(taskManager.getNetworkAliases().contains("taskmanager-2"));
        assertEquals(
                "jobmanager-1", jobManager.getEnvMap().get("JOB_MANAGER_RPC_ADDRESS"));
        assertEquals(
                "jobmanager-1", taskManager.getEnvMap().get("JOB_MANAGER_RPC_ADDRESS"));
        assertEquals(
                "jobmanager-1",
                jobManager.getLabels().get("org.savonitar.flink-stability.component"));
        assertEquals(
                "taskmanager-2",
                taskManager.getLabels().get("org.savonitar.flink-stability.component"));
    }

    @Test
    void configuresTheIdenticalByteOnlyBundleForEveryFlinkProcess() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("connector.jar"), "connector");
        String sha256 = ConnectorClasspathManifest.sha256(Files.readAllBytes(jar));
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(0, jar, sha256)));
        FlinkRuntimeTarget target = FlinkRuntimeTarget.withConnectorBundle(
                "flink:2.2.0",
                new FlinkConnectorBundleInstallation(
                        "flink:2.2.0",
                        List.of(new FlinkConnectorBundleInstallation.ClosureLockHash(
                                "connector", "a".repeat(64))),
                        manifest));
        FlinkContainer factory = new FlinkContainer(
                target, Network.SHARED, temporaryDirectory.resolve("attempt-c"));

        VerifiedFlinkContainer jobManager = assertInstanceOf(
                VerifiedFlinkContainer.class, factory.createJobManager("jobmanager-1"));
        VerifiedFlinkContainer taskManager = assertInstanceOf(
                VerifiedFlinkContainer.class, factory.createTaskManager("taskmanager-1"));

        List<String> expectedTargets = List.of(
                manifest.entries().getFirst().containerPath(),
                ConnectorClasspathManifest.CONTAINER_MANIFEST_PATH);
        assertEquals(expectedTargets, jobManager.configuredBundleTargets());
        assertEquals(expectedTargets, taskManager.configuredBundleTargets());
        assertEquals(
                Set.of(manifest.entries().getFirst().containerPath()),
                Set.copyOf(jobManager.getCopyToFileContainerPathMap().values()));
        assertEquals(
                Set.of(manifest.entries().getFirst().containerPath()),
                Set.copyOf(taskManager.getCopyToFileContainerPathMap().values()));
        assertEquals(target, factory.runtimeTarget());
    }

    @Test
    void legacyFactoryConfiguresNoConnectorCopies() {
        FlinkContainer factory = new FlinkContainer(
                "flink:2.2.0", Network.SHARED, temporaryDirectory.resolve("attempt-d"));

        VerifiedFlinkContainer jobManager = assertInstanceOf(
                VerifiedFlinkContainer.class, factory.createJobManager("jobmanager-1"));
        VerifiedFlinkContainer taskManager = assertInstanceOf(
                VerifiedFlinkContainer.class, factory.createTaskManager("taskmanager-1"));

        assertEquals(List.of(), jobManager.configuredBundleTargets());
        assertEquals(List.of(), taskManager.configuredBundleTargets());
        assertEquals(0, jobManager.getCopyToFileContainerPathMap().size());
        assertEquals(0, taskManager.getCopyToFileContainerPathMap().size());
    }

    @Test
    void rehashesStagedBytesBeforeEachPhysicalContainerIsConfigured() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("mutable.jar"), "before");
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(
                        0, jar, ConnectorClasspathManifest.sha256(Files.readAllBytes(jar)))));
        FlinkContainer factory = new FlinkContainer(
                FlinkRuntimeTarget.withConnectorBundle(
                        "flink:2.2.0",
                        new FlinkConnectorBundleInstallation(
                                "flink:2.2.0",
                                List.of(new FlinkConnectorBundleInstallation.ClosureLockHash(
                                        "connector", "a".repeat(64))),
                                manifest)),
                Network.SHARED,
                temporaryDirectory.resolve("attempt-e"));
        factory.createJobManager("jobmanager-1");
        Files.writeString(jar, "after");

        assertThrows(
                ConnectorBundleProvisioningException.class,
                () -> factory.createTaskManager("taskmanager-1"));
    }

    private static Bind onlyBind(GenericContainer<?> container) {
        return container.getBinds().getFirst();
    }

    private static void assertCheckpointMount(Bind bind) {
        assertEquals("/flink/checkpoints", bind.getVolume().getPath());
        assertEquals(AccessMode.rw, bind.getAccessMode());
    }
}
