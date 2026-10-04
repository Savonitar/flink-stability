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
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkContainerTest {
    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);
    private static final String OTHER_IMAGE_ID = "sha256:" + "b".repeat(64);
    private static final String RUNTIME_JAR_PATH = "/opt/flink/lib/flink-dist-2.2.0.jar";
    private static final byte[] RUNTIME_JAR_BYTES = {1, 4, 9};
    private static final FlinkRuntimeTarget.RuntimeJar RUNTIME_JAR = new FlinkRuntimeTarget.RuntimeJar(
            RUNTIME_JAR_PATH, Digests.sha256(RUNTIME_JAR_BYTES));

    @TempDir
    Path temporaryDirectory;

    @Test
    void customConfigurationRequiresFileAndParsedStartupProofForEveryIncarnation() {
        Map<String, String> config = Map.of("execution.checkpointing.unaligned.enabled", "true", "custom.value", "# literal: value");
        FlinkContainer factory = new FlinkContainer(emptyTarget().withConfig(config), Network.SHARED,
                temporaryDirectory.resolve("custom-config"));
        for (var process : List.of(factory.createJobManager("jobmanager-1"),
                factory.createTaskManager("taskmanager-1"), factory.createTaskManager("taskmanager-1"))) {
            VerifiedFlinkContainer verified = (VerifiedFlinkContainer) process;
            String properties = process.getEnvMap().get("FLINK_PROPERTIES");
            assertTrue(properties.contains("execution.checkpointing.unaligned.enabled: 'true'"));
            assertTrue(properties.contains("custom.value: '# literal: value'"));
            String role = process.getCommandParts()[0];
            var launcher = FlinkProcessConfiguration.launcher(role);
            verified.verifyConfigurationLaunch(List.of("FLINK_PROPERTIES=" + properties),
                    launcher.subList(0, 2), List.of(role));
            assertThrows(IllegalStateException.class,
                    () -> verified.verifyConfigurationLaunch(List.of("FLINK_PROPERTIES=changed"),
                            launcher.subList(0, 2), List.of(role)));
            assertThrows(IllegalStateException.class,
                    () -> verified.verifyConfigurationLaunch(List.of("FLINK_PROPERTIES=" + properties),
                            List.of("/docker-entrypoint.sh"), List.of(role)));
            var file = new AtomicReference<>("taskmanager.memory.process.size: 1728m\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            var reader = (Function<String, byte[]>) ignored -> file.get();
            verified.prepareProcessConfiguration("id", reader, (path, bytes) -> file.set(bytes));
            assertEquals(Map.of(), verified.observedConfig(), "Requested values are not observations");
            assertThrows(IllegalStateException.class,
                    () -> verified.effectiveConfigurationEvidence("other-incarnation", reader, ""));
            assertThrows(IllegalStateException.class,
                    () -> verified.effectiveConfigurationEvidence("id", reader, ""));
            String startup = "Loading configuration property: execution.checkpointing.unaligned.enabled, true\n"
                    + "Loading configuration property: custom.value, # literal: value\n";
            var receipt = verified.effectiveConfigurationEvidence("id", reader, startup).orElseThrow();
            assertEquals(config, verified.observedConfig());
            assertEquals(receipt.sourceSha256(), receipt.observedSha256());
            assertEquals(launcher, receipt.launcher());
            file.set("custom.value: changed\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            assertThrows(IllegalStateException.class,
                    () -> verified.effectiveConfigurationEvidence("id", reader, startup));
        }
    }

    @Test
    void imageEntrypointAndConfigurationAreUntouchedWithoutCustomValues() {
        var factory = new FlinkContainer(emptyTarget(), Network.SHARED, temporaryDirectory);
        for (var container : List.of(factory.createJobManager("jobmanager-1"), factory.createTaskManager("taskmanager-1"))) {
            assertTrue(!container.getEnvMap().containsKey("FLINK_CONF_DIR"));
            var verified = (VerifiedFlinkContainer) container;
            verified.prepareProcessConfiguration("id", unused -> { throw new AssertionError("No file read"); },
                    (path, bytes) -> { throw new AssertionError("No file write"); });
            assertTrue(verified.effectiveConfigurationEvidence("id", unused -> { throw new AssertionError("No read"); }, "").isEmpty());
        }
    }

    @Test
    void anonymousHaDisablesZooKeeperSaslForInitialAndReplacementProcesses() throws Exception {
        var configuration = new FlinkRuntimeTarget.HighAvailability(
                "zookeeper:3.9.3", Duration.ofSeconds(6));
        try (var firstGate = new FlinkHaRuntime.TcpGate("127.0.0.1", 1);
             var secondGate = new FlinkHaRuntime.TcpGate("127.0.0.1", 1);
             var runtime = new FlinkHaRuntime(configuration, System::nanoTime,
                     unused -> Optional.empty(),
                     Map.of("jobmanager-1", firstGate, "jobmanager-2", secondGate))) {
            FlinkContainer factory = new FlinkContainer(
                    emptyTarget().withTaskManagers(2).withHighAvailability(configuration),
                    Network.SHARED, temporaryDirectory.resolve("anonymous-ha"));
            factory.configureHighAvailability(runtime);
            List<GenericContainer<?>> processes = List.of(
                    factory.createJobManager("jobmanager-1"), factory.createJobManager("jobmanager-2"),
                    factory.createTaskManager("taskmanager-1"), factory.createTaskManager("taskmanager-2"),
                    factory.createJobManager("jobmanager-1"), factory.createTaskManager("taskmanager-2"));
            for (GenericContainer<?> process : processes) {
                assertHarnessPropertiesReserved(process);
                assertEquals(List.of("zookeeper.sasl.disable: true"),
                        process.getEnvMap().get("FLINK_PROPERTIES").lines()
                                .filter(line -> line.startsWith("zookeeper.sasl.disable:")).toList());
            }
        }
    }

    @Test
    void standaloneProcessesDoNotOverrideZooKeeperSasl() {
        FlinkContainer factory = new FlinkContainer(emptyTarget().withTaskManagers(2),
                Network.SHARED, temporaryDirectory.resolve("standalone-sasl"));
        List<GenericContainer<?>> processes = List.of(factory.createJobManager("jobmanager-1"),
                factory.createTaskManager("taskmanager-1"), factory.createTaskManager("taskmanager-2"),
                factory.createJobManager("jobmanager-1"), factory.createTaskManager("taskmanager-2"));
        for (GenericContainer<?> process : processes) {
            assertHarnessPropertiesReserved(process);
            assertTrue(process.getEnvMap().get("FLINK_PROPERTIES").lines()
                    .noneMatch(line -> line.startsWith("zookeeper.sasl.disable:")));
        }
    }

    private static void assertHarnessPropertiesReserved(GenericContainer<?> process) {
        process.getEnvMap().get("FLINK_PROPERTIES").lines().filter(line -> !line.isBlank()).forEach(line -> {
            String key = line.substring(0, line.indexOf(':'));
            assertTrue(org.savonitar.flink.stability.runtime.api.FlinkConfiguration.reserved(key), key);
        });
    }

    @Test
    void explicitTokenRetryBackoffReachesEveryProcessAndOmissionPreservesRuntimeDefaults() {
        for (Optional<Duration> backoff : List.of(Optional.<Duration>empty(), Optional.of(Duration.ofSeconds(3)))) {
            FlinkRuntimeTarget target = emptyTarget().withTokenProvider(
                    new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2), backoff));
            FlinkContainer factory = new FlinkContainer(target, Network.SHARED,
                    temporaryDirectory.resolve(backoff.isPresent() ? "fixed-backoff" : "default-backoff"));
            factory.configureTokenProvider(new SyntheticTokenPlugin(), 1234);
            List<GenericContainer<?>> processes = List.of(factory.createJobManager("jobmanager-1"),
                    factory.createJobManager("jobmanager-2"), factory.createTaskManager("taskmanager-1"),
                    factory.createJobManager("jobmanager-1"));
            for (GenericContainer<?> process : processes) {
                assertHarnessPropertiesReserved(process);
                List<String> retries = process.getEnvMap().get("FLINK_PROPERTIES").lines()
                        .filter(line -> line.startsWith("security.delegation.tokens.renewal.retry."))
                        .toList();
                assertEquals(backoff.isPresent() ? List.of(
                        "security.delegation.tokens.renewal.retry.backoff: 3000 ms",
                        "security.delegation.tokens.renewal.retry.initial.backoff: 3000 ms",
                        "security.delegation.tokens.renewal.retry.max.backoff: 3000 ms") : List.of(), retries);
            }
        }
    }

    @Test
    void tokenPluginVerificationWaitsForTaskManagerEntrypointWithoutRuntimeJarPin() {
        FlinkRuntimeTarget target = emptyTarget()
                .withTokenProvider(new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2)));
        FlinkContainer factory = new FlinkContainer(target, Network.SHARED,
                temporaryDirectory.resolve("token-readiness"));
        VerifiedFlinkContainer taskManager = (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-1");
        assertTrue(target.expectedRuntimeJar().isEmpty());
        assertInstanceOf(LogMessageWaitStrategy.class, taskManager.configuredWaitStrategy());
    }

    @Test
    void taskManagerResourceIdsBindEachLogIncarnationAndStayUniqueAcrossAttempts() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget().withTaskManagers(2), Network.SHARED, temporaryDirectory.resolve("first"));
        VerifiedFlinkContainer first = (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-1");
        VerifiedFlinkContainer second = (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-2");
        VerifiedFlinkContainer replacement = (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-1");
        VerifiedFlinkContainer anotherAttempt = (VerifiedFlinkContainer) new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory.resolve("second"))
                .createTaskManager("taskmanager-1");

        assertTrue(first.resourceId().startsWith("flink-stability-taskmanager-1-1-"));
        assertTrue(second.resourceId().startsWith("flink-stability-taskmanager-2-1-"));
        assertTrue(replacement.resourceId().startsWith("flink-stability-taskmanager-1-2-"));
        assertNotEquals(first.resourceId(), anotherAttempt.resourceId());
        List<VerifiedFlinkContainer> containers = List.of(first, second, replacement, anotherAttempt);
        assertEquals(4, containers.stream().map(VerifiedFlinkContainer::resourceId).distinct().count());
        for (VerifiedFlinkContainer container : containers) {
            assertTrue(container.resourceId().matches("[A-Za-z0-9-]+"));
            List<String> properties = container.getEnvMap().get("FLINK_PROPERTIES").lines().toList();
            assertEquals(List.of("taskmanager.resource-id: " + container.resourceId()),
                    properties.stream().filter(line -> line.startsWith("taskmanager.resource-id:")).toList());
            assertTrue(properties.contains("taskmanager.numberOfTaskSlots: "
                    + FlinkRuntimeTarget.TASK_SLOTS_PER_TASK_MANAGER));
        }
        assertTrue(factory.createJobManager("jobmanager-1").getEnvMap().get("FLINK_PROPERTIES")
                .lines().noneMatch(line -> line.startsWith("taskmanager.resource-id:")));
    }

    @Test
    void runtimeJarReadsBothBeforeAndAfterStartupAndBindsTheRegisteredLog() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget().withExpectedRuntimeJar(RUNTIME_JAR), Network.SHARED, temporaryDirectory);
        VerifiedFlinkContainer container = (VerifiedFlinkContainer) factory.createJobManager("jobmanager-1");
        java.util.ArrayList<String> pathsRead = new java.util.ArrayList<>();
        Function<String, String> archive = path -> {
            pathsRead.add(path);
            return Digests.sha256(RUNTIME_JAR_BYTES);
        };

        assertThrows(IllegalStateException.class,
                () -> container.runtimeJarEvidence("physical-jm", archive));
        container.verifyRuntimeJarBeforeStart("physical-jm", archive);
        var evidence = container.runtimeJarEvidence("physical-jm", archive).orElseThrow();

        assertEquals(List.of(RUNTIME_JAR_PATH, RUNTIME_JAR_PATH), pathsRead);
        assertEquals(RUNTIME_JAR, evidence.jar());
        assertEquals("jobmanager-1#1", evidence.classLoadProcess());
        assertEquals(factory.classLoadLogs().getFirst().process(), evidence.classLoadProcess());
        assertThrows(IllegalStateException.class,
                () -> container.runtimeJarEvidence("different-physical-jm", archive));
    }

    @Test
    void wrongMissingAndEntrypointReplacedRuntimeBytesCannotProduceEvidence() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget().withExpectedRuntimeJar(RUNTIME_JAR), Network.SHARED, temporaryDirectory);
        VerifiedFlinkContainer container = (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-1");
        Map<String, byte[]> files = new HashMap<>();
        Function<String, String> archive = path -> {
            byte[] bytes = files.get(path);
            if (bytes == null) {
                throw new IllegalStateException("synthetic missing archive entry");
            }
            return Digests.sha256(bytes);
        };

        IllegalStateException missing = assertThrows(IllegalStateException.class,
                () -> container.verifyRuntimeJarBeforeStart("physical-tm", archive));
        assertTrue(missing.getMessage().contains(RUNTIME_JAR_PATH));
        assertTrue(missing.getMessage().contains("actual unavailable"));
        assertEquals("synthetic missing archive entry", missing.getCause().getMessage());
        files.put(RUNTIME_JAR_PATH, new byte[] {2, 5, 8});
        IllegalStateException wrong = assertThrows(IllegalStateException.class,
                () -> container.verifyRuntimeJarBeforeStart("physical-tm", archive));
        assertTrue(wrong.getMessage().contains("expected " + RUNTIME_JAR.sha256()));
        assertTrue(wrong.getMessage().contains("actual " + Digests.sha256(new byte[] {2, 5, 8})));
        files.put(RUNTIME_JAR_PATH, RUNTIME_JAR_BYTES);
        assertThrows(IllegalStateException.class,
                () -> container.runtimeJarEvidence("physical-tm", archive));
        container.verifyRuntimeJarBeforeStart("physical-tm", archive);
        files.put(RUNTIME_JAR_PATH, new byte[] {3, 6, 7});
        assertThrows(IllegalStateException.class,
                () -> container.runtimeJarEvidence("physical-tm", archive));
        files.remove(RUNTIME_JAR_PATH);
        assertThrows(IllegalStateException.class,
                () -> container.runtimeJarEvidence("physical-tm", archive));
    }

    @Test
    void replacementsAndRetriedFactoriesKeepTheirOwnLogBindingDespiteMissingStarts() {
        ClassLoadLogs logs = new ClassLoadLogs(temporaryDirectory);
        FlinkRuntimeTarget target = emptyTarget().withExpectedRuntimeJar(RUNTIME_JAR);
        FlinkContainer first = new FlinkContainer(target, Network.SHARED, temporaryDirectory, logs);
        first.createJobManager("jobmanager-1");
        first.createTaskManager("taskmanager-1");
        FlinkContainer retry = new FlinkContainer(target, Network.SHARED, temporaryDirectory, logs);
        VerifiedFlinkContainer replacement = (VerifiedFlinkContainer) retry.createTaskManager("taskmanager-1");
        retry.createJobManager("jobmanager-1");
        replacement.verifyRuntimeJarBeforeStart("replacement-container", ignored -> RUNTIME_JAR.sha256());
        var observed = replacement.runtimeJarEvidence("replacement-container",
                ignored -> RUNTIME_JAR.sha256()).orElseThrow();

        assertEquals("taskmanager-1#2", observed.classLoadProcess());
        assertTrue(replacement.getEnvMap().get("FLINK_PROPERTIES").contains(
                ClassLoadLogs.fileName("taskmanager-1", 2)));
        assertInstanceOf(LogMessageWaitStrategy.class, replacement.configuredWaitStrategy());
        assertThrows(IllegalStateException.class,
                () -> replacement.verifyRuntimeJarBeforeStart("retried-container", ignored -> "wrong"));
        assertThrows(IllegalStateException.class,
                () -> replacement.runtimeJarEvidence("replacement-container", ignored -> RUNTIME_JAR.sha256()));
    }

    @Test
    void scenariosWithoutRuntimeJarPinDoNotReadOrReportRuntimeJarEvidence() {
        FlinkContainer factory = new FlinkContainer(emptyTarget(), Network.SHARED, temporaryDirectory);
        VerifiedFlinkContainer container = (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-1");
        Function<String, String> unexpectedRead = ignored -> {
            throw new AssertionError("No runtime JAR was requested");
        };
        container.verifyRuntimeJarBeforeStart("physical-tm", unexpectedRead);
        assertTrue(container.runtimeJarEvidence("physical-tm", unexpectedRead).isEmpty());
        assertTrue(!(container.configuredWaitStrategy() instanceof LogMessageWaitStrategy));
    }

    @Test
    void verifiesCreatedContainerImageIdentityForEveryInitialAndReplacementProcess() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory);
        List<VerifiedFlinkContainer> containers = List.of(
                (VerifiedFlinkContainer) factory.createJobManager("jobmanager-1"),
                (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-1"),
                (VerifiedFlinkContainer) factory.createTaskManager("taskmanager-1"));

        for (int index = 0; index < containers.size(); index++) {
            String runtimeId = "created-container-" + index;
            VerifiedFlinkContainer container = containers.get(index);
            assertThrows(IllegalStateException.class, container::verifiedImageId);
            container.verifyImageIdentity(runtimeId, inspectedId -> {
                assertEquals(runtimeId, inspectedId);
                return IMAGE_ID;
            });
            assertEquals(IMAGE_ID, container.verifiedImageId());
        }
    }

    @Test
    void mutableTagCannotChangeTheImageBetweenJobManagerTaskManagerAndReplacement() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory);
        VerifiedFlinkContainer jobManager = (VerifiedFlinkContainer)
                factory.createJobManager("jobmanager-1");
        jobManager.verifyImageIdentity("jobmanager-container", ignored -> IMAGE_ID);
        VerifiedFlinkContainer wrongTaskManager = (VerifiedFlinkContainer)
                factory.createTaskManager("taskmanager-1");
        assertImageMismatch(wrongTaskManager, "wrong-taskmanager", OTHER_IMAGE_ID);

        VerifiedFlinkContainer taskManager = (VerifiedFlinkContainer)
                factory.createTaskManager("taskmanager-1");
        taskManager.verifyImageIdentity("taskmanager-container", ignored -> IMAGE_ID);
        VerifiedFlinkContainer replacement = (VerifiedFlinkContainer)
                factory.createTaskManager("taskmanager-1");
        assertImageMismatch(replacement, "replacement-container", OTHER_IMAGE_ID);
        assertEquals(IMAGE_ID, jobManager.verifiedImageId());
        assertEquals(IMAGE_ID, taskManager.verifiedImageId());
    }

    @Test
    void declaredExpectedImageIsCheckedBeforeTheFirstProcessCanStart() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget().withExpectedImageId(IMAGE_ID), Network.SHARED, temporaryDirectory);
        VerifiedFlinkContainer wrong = (VerifiedFlinkContainer)
                factory.createJobManager("jobmanager-1");
        assertImageMismatch(wrong, "wrong-jobmanager", OTHER_IMAGE_ID);

        VerifiedFlinkContainer matching = (VerifiedFlinkContainer)
                factory.createJobManager("jobmanager-1");
        matching.verifyImageIdentity("matching-jobmanager", ignored -> IMAGE_ID);
        assertEquals(IMAGE_ID, matching.verifiedImageId());
        // A later failed create/inspect attempt must not reuse the prior incarnation's evidence.
        assertImageMismatch(matching, "retried-jobmanager", OTHER_IMAGE_ID);
    }

    @Test
    void retryKeepsTheFirstCreatedImageEvenWithoutAnySuccessfulProvisioning() {
        AtomicReference<String> pin = new AtomicReference<>();
        ClassLoadLogs logs = new ClassLoadLogs(temporaryDirectory);
        FlinkContainer first = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory, logs, pin);
        VerifiedFlinkContainer created = (VerifiedFlinkContainer)
                first.createJobManager("jobmanager-1");
        created.verifyImageIdentity("first-created-container", ignored -> IMAGE_ID);
        // No bundle verification or process start completed before the factory was replaced.
        FlinkContainer retry = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory, logs, pin);
        VerifiedFlinkContainer different = (VerifiedFlinkContainer)
                retry.createJobManager("jobmanager-1");
        assertImageMismatch(different, "retried-created-container", OTHER_IMAGE_ID);
        assertEquals(IMAGE_ID, pin.get());

        VerifiedFlinkContainer matching = (VerifiedFlinkContainer)
                retry.createJobManager("jobmanager-1");
        matching.verifyImageIdentity("matching-retry-container", ignored -> IMAGE_ID);
        assertEquals(IMAGE_ID, matching.verifiedImageId());
    }

    @Test
    void missingMalformedOrFailedInspectionCannotPinAnImageOrCreateIdentityEvidence() {
        FlinkContainer factory = new FlinkContainer(
                emptyTarget(), Network.SHARED, temporaryDirectory);
        for (String invalid : new String[] {null, "", "a".repeat(64), "sha256:short"}) {
            VerifiedFlinkContainer container = (VerifiedFlinkContainer)
                    factory.createJobManager("jobmanager-1");
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> container.verifyImageIdentity("invalid-container", ignored -> invalid));
            assertTrue(failure.getMessage().contains("actual " + invalid), failure.getMessage());
            assertThrows(IllegalStateException.class, container::verifiedImageId);
        }
        VerifiedFlinkContainer failed = (VerifiedFlinkContainer)
                factory.createJobManager("jobmanager-1");
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> failed.verifyImageIdentity("unavailable-container", ignored -> {
                    throw new IllegalStateException("synthetic inspect failure");
                }));
        assertTrue(failure.getMessage().contains("actual unavailable"), failure.getMessage());
        assertEquals("synthetic inspect failure", failure.getCause().getMessage());
        assertThrows(IllegalStateException.class, failed::verifiedImageId);

        VerifiedFlinkContainer valid = (VerifiedFlinkContainer)
                factory.createJobManager("jobmanager-1");
        valid.verifyImageIdentity("valid-container", ignored -> IMAGE_ID);
        assertEquals(IMAGE_ID, valid.verifiedImageId());
    }

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
        assertTrue(jobManager.getNetworkAliases().stream()
                .noneMatch(alias -> alias.matches("jobmanager-1-[1-9][0-9]*")),
                "Standalone execution must not reserve HA incarnation aliases");
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

    private static void assertImageMismatch(
            VerifiedFlinkContainer container, String runtimeId, String observed) {
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> container.verifyImageIdentity(runtimeId, ignored -> observed));
        assertTrue(failure.getMessage().contains("expected " + IMAGE_ID), failure.getMessage());
        assertTrue(failure.getMessage().contains("actual " + observed), failure.getMessage());
        assertThrows(IllegalStateException.class, container::verifiedImageId);
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
