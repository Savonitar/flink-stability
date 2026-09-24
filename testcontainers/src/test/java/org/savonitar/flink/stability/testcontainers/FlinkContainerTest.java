package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.ConnectorClasspathManifest;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkContainerTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void everyFlinkJvmLogsItsClassLoadsToAPerIncarnationFileInTheAttemptDirectory() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory.resolve("attempt-logs"));

        String jobManager = factory.createJobManager("jobmanager-1")
                .getEnvMap().get("FLINK_PROPERTIES");
        String firstTaskManager = factory.createTaskManager("taskmanager-1")
                .getEnvMap().get("FLINK_PROPERTIES");
        String replacement = factory.createTaskManager("taskmanager-1")
                .getEnvMap().get("FLINK_PROPERTIES");

        assertTrue(jobManager.contains("env.java.opts.jobmanager: -Xlog:class+load=info:file="
                + "/flink/checkpoints/flink-stability-class-load-jobmanager-1-1.log"), jobManager);
        assertTrue(firstTaskManager.contains("env.java.opts.taskmanager: -Xlog:class+load"
                + "=info:file=/flink/checkpoints/flink-stability-class-load-taskmanager-1-1.log"),
                firstTaskManager);
        assertTrue(replacement.contains("flink-stability-class-load-taskmanager-1-2.log"),
                replacement);
        // The image keeps its --add-opens flags in env.java.opts.all; never override them.
        assertTrue(!jobManager.contains("env.java.opts.all"), jobManager);
    }

    @Test
    void retainsExpectedLogsForMissingIncarnationsAndIgnoresUnregisteredFiles() throws Exception {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory);
        factory.createJobManager("jobmanager-1");
        factory.createTaskManager("taskmanager-1");
        factory.createTaskManager("taskmanager-1");
        FlinkClassLoadLog replacement = factory.classLoadLogs().getLast();
        Files.writeString(replacement.hostPath(), "replacement log");
        Files.writeString(temporaryDirectory.resolve(
                ClassLoadLogs.fileName("taskmanager-9", 1)), "stale unrelated log");

        assertEquals(
                List.of("jobmanager-1#1", "taskmanager-1#1", "taskmanager-1#2"),
                factory.classLoadLogs().stream().map(FlinkClassLoadLog::process)
                        .toList());
        assertTrue(Files.notExists(factory.classLoadLogs().get(1).hostPath()),
                "the missing predecessor stays in the inventory for fail-closed reading");
        assertTrue(Files.exists(replacement.hostPath()));
    }

    @Test
    void retriedStartupFactoriesCannotOverwriteEarlierIncarnationLogs() {
        ClassLoadLogs logs = new ClassLoadLogs(temporaryDirectory);
        FlinkContainer first = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory, logs);
        first.createJobManager("jobmanager-1");
        first.createTaskManager("taskmanager-1");
        FlinkContainer retry = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory, logs);
        String retriedJobManager = retry.createJobManager("jobmanager-1")
                .getEnvMap().get("FLINK_PROPERTIES");
        String retriedTaskManager = retry.createTaskManager("taskmanager-1")
                .getEnvMap().get("FLINK_PROPERTIES");

        assertTrue(retriedJobManager.contains(ClassLoadLogs.fileName("jobmanager-1", 2)));
        assertTrue(retriedTaskManager.contains(ClassLoadLogs.fileName("taskmanager-1", 2)));
        assertEquals(List.of("jobmanager-1#1", "taskmanager-1#1",
                        "jobmanager-1#2", "taskmanager-1#2"),
                retry.classLoadLogs().stream().map(FlinkClassLoadLog::process).toList());
        assertEquals(4, retry.classLoadLogs().stream().map(FlinkClassLoadLog::hostPath)
                .distinct().count());
    }

    @Test
    void mountsTheSameWritableCheckpointDirectoryIntoEveryFlinkProcess() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory.resolve("attempt-a"));

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
                emptyTarget(), Network.SHARED, temporaryDirectory.resolve("attempt-b"));

        GenericContainer<?> jobManager = factory.createJobManager("jobmanager-1");
        GenericContainer<?> taskManager = factory.createTaskManager("taskmanager-2");

        // Testcontainers prepends its own generated alias; the harness aliases remain stable.
        assertTrue(jobManager.getNetworkAliases().contains("jobmanager-1"));
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
        assertInstanceOf(
                HttpWaitStrategy.class,
                ((VerifiedFlinkContainer) jobManager).configuredWaitStrategy());
    }

    @Test
    void configuresTheIdenticalByteOnlyBundleForEveryFlinkProcess() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("connector.jar"), "connector");
        String sha256 = Digests.sha256(Files.readAllBytes(jar));
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
        assertEquals(target, factory.runtimeTarget());
    }

    @Test
    void emptyBundleStillCopiesTheCanonicalVerificationManifest() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory.resolve("attempt-d"));

        VerifiedFlinkContainer jobManager = assertInstanceOf(
                VerifiedFlinkContainer.class, factory.createJobManager("jobmanager-1"));
        VerifiedFlinkContainer taskManager = assertInstanceOf(
                VerifiedFlinkContainer.class, factory.createTaskManager("taskmanager-1"));

        assertEquals(
                List.of(ConnectorClasspathManifest.CONTAINER_MANIFEST_PATH),
                jobManager.configuredBundleTargets());
        assertEquals(
                List.of(ConnectorClasspathManifest.CONTAINER_MANIFEST_PATH),
                taskManager.configuredBundleTargets());
    }

    @Test
    void rehashesStagedBytesBeforeEachPhysicalContainerIsConfigured() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("mutable.jar"), "before");
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(
                        0, jar, Digests.sha256(Files.readAllBytes(jar)))));
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

    private static FlinkRuntimeTarget emptyTarget() {
        String image = "flink:2.2.0";
        return FlinkRuntimeTarget.withConnectorBundle(
                image,
                new FlinkConnectorBundleInstallation(
                        image, List.of(), new ConnectorClasspathManifest(List.of())));
    }

    private static void assertCheckpointMount(Bind bind) {
        assertEquals("/flink/checkpoints", bind.getVolume().getPath());
        assertEquals(AccessMode.rw, bind.getAccessMode());
    }
}
