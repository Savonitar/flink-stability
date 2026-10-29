package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateVolumeCmd;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.RemoveVolumeCmd;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the actual storage/lifecycle deadline boundaries with an uncooperative driver. */
class FuseCheckpointStorageConcurrencyTest {
    @TempDir Path temporary;
    private static final Duration SHORT = Duration.ofMillis(100);
    private static final Duration TEST_BOUND = Duration.ofSeconds(3);

    @Test void startupTimeoutDoesNotLeaveHelperCopyHoldingTheEvidenceLock() throws Exception {
        Fixture fixture = new Fixture("startup", Block.COPY);
        var mount = fixture.configure();
        CountDownLatch startupFinished = new CountDownLatch(1);
        var starting = new StartingContainer(() -> {
            try { fixture.runtime.verifyCreated(mount); }
            finally { startupFinished.countDown(); }
        });
        var lifecycle = new TestcontainersContainerHandle.ContainerLifecycle(starting);
        try {
            assertTimeoutPreemptively(TEST_BOUND, () -> assertThrows(ContainerOperationTimeoutException.class,
                    () -> lifecycle.startWithin(deadline(Duration.ofMillis(250)))));
            assertTrue(fixture.entered.await(1, TimeUnit.SECONDS), "The real helper-copy boundary must be reached");
            assertTrue(startupFinished.await(1, TimeUnit.SECONDS), "Startup must release its evidence lock after cancellation");
            var capture = assertTimeoutPreemptively(TEST_BOUND, () -> fixture.runtime.storageSnapshot(SHORT));
            assertFalse(capture.complete()); assertFalse(capture.quiescent());
            assertTrue(capture.diagnostics().stream().anyMatch(value -> value.contains("failed")));
            assertTimeoutPreemptively(TEST_BOUND, () -> fixture.runtime.close(SHORT));
            assertEquals(1, fixture.removals.get());
        } finally { fixture.release.countDown(); }
        assertTrue(fixture.driverReturned.await(1, TimeUnit.SECONDS));
        assertEquals(1, fixture.copies.get(), "A canceled copy must not advance to the next helper");
        assertEquals(0, fixture.inspections.get());
        assertFalse(fixture.runtime.storageSnapshot(SHORT).complete());
    }

    @Test void captureAndCleanupHaveTheirOwnDeadlinesWhileCopyOrInspectStillIgnoresInterrupts() throws Exception {
        for (Block block : List.of(Block.COPY, Block.INSPECT)) {
            Fixture fixture = new Fixture(block.name(), block);
            var mount = fixture.configure();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread verifier = worker(() -> fixture.runtime.verifyCreated(mount), failure);
            try {
                assertTrue(fixture.entered.await(1, TimeUnit.SECONDS));
                // The owner has a 30-second budget. This independent 100-ms capture must
                // include lock acquisition, not begin its deadline after entering a monitor.
                var capture = assertTimeoutPreemptively(TEST_BOUND, () -> fixture.runtime.storageSnapshot(SHORT));
                assertFalse(capture.complete()); assertFalse(capture.quiescent());
                assertTrue(capture.diagnostics().stream().anyMatch(value -> value.contains("lock")));
                assertTimeoutPreemptively(TEST_BOUND, () -> fixture.runtime.close(SHORT));
                assertEquals(1, fixture.removals.get());
                assertTrue(verifier.isAlive(), "The driver has deliberately not returned yet");
            } finally { fixture.release.countDown(); }
            verifier.join(1000);
            assertFalse(verifier.isAlive()); assertNotNull(failure.get(), "Late startup cannot succeed after close");
            assertFalse(fixture.runtime.storageSnapshot(SHORT).complete());
            assertEquals(block == Block.COPY ? 1 : 3, fixture.copies.get());
            assertEquals(block == Block.COPY ? 0 : 1, fixture.inspections.get());
        }
    }

    @Test void timedOutJournalCopyCannotPublishLateBytesOrConfirmQuiescence() throws Exception {
        Fixture fixture = new Fixture("journal", Block.JOURNAL);
        var mount = fixture.configure();
        mount.instance = new StorageFaultControl.Instance("taskmanager-1", "taskmanager-1#1", "container-a",
                mount.instanceId, FlinkComponentRole.TASK_MANAGER, 25);
        mount.status = new StorageFaultControl.Status(StorageFaultControl.State.HEALTHY, null, 0, 1, 0, 0, true, false);
        mount.verification = new StorageFaultControl.Verification("sha256:" + "a".repeat(64),
                "a".repeat(64), "b".repeat(64), "generated mount", List.of(9999L), List.of(9999L), "0", 42,
                "generated JVM", true, true, true, List.of());
        var stopped = new FlinkHaControl.ProcessState("container-a", false, false,
                Optional.of(137L), Optional.of(false), Optional.of("generated stop"));
        try {
            assertTimeoutPreemptively(TEST_BOUND, () -> assertThrows(ContainerOperationTimeoutException.class,
                    () -> fixture.runtime.processDead(mount, stopped, deadline(SHORT))));
            assertTrue(fixture.entered.await(1, TimeUnit.SECONDS));
            var capture = assertTimeoutPreemptively(TEST_BOUND, () -> fixture.runtime.storageSnapshot(SHORT));
            assertFalse(capture.complete()); assertFalse(capture.quiescent());
            assertEquals(1, capture.instances().size());
            assertTrue(capture.instances().getFirst().processDead());
            assertFalse(capture.instances().getFirst().journalComplete());
            assertNull(capture.instances().getFirst().journalPath());
            assertTimeoutPreemptively(TEST_BOUND, () -> fixture.runtime.close(SHORT));
        } finally { fixture.release.countDown(); }
        assertTrue(fixture.driverReturned.await(1, TimeUnit.SECONDS));
        assertNull(mount.journalPath);
        try (var files = Files.list(fixture.evidence.resolve("checkpoint-storage-journals"))) {
            assertEquals(0, files.count(), "Late driver output is never persisted as accepted evidence");
        }
    }

    @Test void abandonedExecCreationCannotStartTheCommandAfterCancellation() throws Exception {
        Fixture fixture = new Fixture("exec", Block.EXEC_CREATE);
        var mount = fixture.configure();
        CountDownLatch startupFinished = new CountDownLatch(1);
        var starting = new StartingContainer(() -> {
            try { fixture.runtime.start(mount); }
            finally { startupFinished.countDown(); }
        });
        var lifecycle = new TestcontainersContainerHandle.ContainerLifecycle(starting);
        try {
            assertTimeoutPreemptively(TEST_BOUND, () -> assertThrows(ContainerOperationTimeoutException.class,
                    () -> lifecycle.startWithin(deadline(Duration.ofMillis(250)))));
            assertTrue(fixture.entered.await(1, TimeUnit.SECONDS));
            assertTrue(startupFinished.await(1, TimeUnit.SECONDS));
            assertFalse(fixture.runtime.storageSnapshot(SHORT).complete());
            // Keep the lifecycle open to prove the per-call cancellation flag, independently
            // of close() and of interrupts that the fake transport deliberately consumes.
        } finally { fixture.release.countDown(); }
        assertTrue(fixture.driverReturned.await(1, TimeUnit.SECONDS));
        assertFalse(fixture.execStarted.await(250, TimeUnit.MILLISECONDS), "Late execCreate result must not start a command");
        assertEquals(0, fixture.execStarts.get());
        fixture.runtime.close(SHORT);
    }

    @Test void timedOutVolumeCreationDoesNotBlockCaptureOrCleanupAndLateCreateIsRemovedInOrder() throws Exception {
        Fixture fixture = new Fixture("volume", Block.CREATE);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread configure = worker(fixture::configure, failure);
        try {
            assertTrue(fixture.entered.await(1, TimeUnit.SECONDS));
            assertFalse(assertTimeoutPreemptively(TEST_BOUND, () -> fixture.runtime.storageSnapshot(SHORT)).complete());
            assertTimeoutPreemptively(TEST_BOUND, () -> assertThrows(ContainerOperationTimeoutException.class,
                    () -> fixture.runtime.close(SHORT)));
            assertEquals(0, fixture.removals.get(), "Removal must wait for the pending create acknowledgement");
        } finally { fixture.release.countDown(); }
        configure.join(1000);
        assertFalse(configure.isAlive()); assertNotNull(failure.get());
        assertTrue(fixture.removed.await(1, TimeUnit.SECONDS), "The exact owned late volume still receives cleanup");
        assertEquals(1, fixture.removals.get());
        assertFalse(fixture.runtime.storageSnapshot(SHORT).complete());
    }

    private static ContainerOperationDeadline deadline(Duration budget) {
        return ContainerOperationDeadline.start("generated storage regression", budget, System::nanoTime);
    }

    private static Thread worker(Runnable action, AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> { try { action.run(); } catch (Throwable caught) { failure.set(caught); } });
        thread.setDaemon(true); thread.start(); return thread;
    }

    private enum Block { COPY, INSPECT, JOURNAL, CREATE, EXEC_CREATE }

    private static final class StartingContainer extends GenericContainer<StartingContainer> {
        private final Runnable startup;
        StartingContainer(Runnable startup) { super(DockerImageName.parse("flink:2.2.0")); this.startup = startup; }
        @Override public void start() { startup.run(); }
    }

    private final class Fixture {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1),
                driverReturned = new CountDownLatch(1), removed = new CountDownLatch(1), execStarted = new CountDownLatch(1);
        final AtomicInteger copies = new AtomicInteger(), inspections = new AtomicInteger(), removals = new AtomicInteger(),
                execStarts = new AtomicInteger();
        final Path evidence;
        final FuseCheckpointStorageRuntime runtime;
        final VerifiedFlinkContainer container;

        Fixture(String name, Block block) throws Exception {
            Path root = temporary.toRealPath().resolve(name);
            Path bundle = root.resolve("prepared");
            Files.createDirectories(bundle.resolve("bundle"));
            Files.writeString(bundle.resolve("build-evidence.json"), "{}");
            var hashes = new HashMap<String, String>();
            for (String file : FuseCheckpointStorage.REQUIRED_FILES) {
                byte[] bytes = file.getBytes(StandardCharsets.UTF_8);
                Files.write(bundle.resolve("bundle").resolve(file), bytes); hashes.put(file, Digests.sha256(bytes));
            }
            var target = new FuseCheckpointStorage(bundle, Digests.sha256("{}".getBytes(StandardCharsets.UTF_8)),
                    "sha256:" + "a".repeat(64), FuseCheckpointStorage.SUPPORTED_PLATFORM, hashes);
            evidence = root.resolve("evidence"); Files.createDirectory(evidence);
            DockerClient docker = proxy(DockerClient.class, (value, method, args) -> switch (method.getName()) {
                case "createVolumeCmd" -> command(CreateVolumeCmd.class, () -> { if (block == Block.CREATE) block(); });
                case "removeVolumeCmd" -> command(RemoveVolumeCmd.class, () -> { removals.incrementAndGet(); removed.countDown(); });
                case "inspectContainerCmd" -> command(InspectContainerCmd.class, () -> { inspections.incrementAndGet(); if (block == Block.INSPECT) block(); });
                case "execCreateCmd" -> command(ExecCreateCmd.class, () -> { if (block == Block.EXEC_CREATE) block(); });
                case "execStartCmd" -> { execStarts.incrementAndGet(); execStarted.countDown(); throw new AssertionError("Abandoned exec must not start"); }
                default -> throw new AssertionError("Unexpected Docker call " + method.getName());
            });
            runtime = new FuseCheckpointStorageRuntime(target, evidence, () -> docker, (unused, path, maximum) -> {
                copies.incrementAndGet();
                if (block == Block.COPY || (block == Block.JOURNAL && path.equals(FuseCheckpointStorageRuntime.JOURNAL))) block();
                return Path.of(path).getFileName().toString().getBytes(StandardCharsets.UTF_8);
            });
            String image = "flink:2.2.0";
            var flink = FlinkRuntimeTarget.withConnectorBundle(image,
                    new FlinkConnectorBundleInstallation(image, List.of(), new ConnectorClasspathManifest(List.of())));
            container = new VerifiedFlinkContainer(DockerImageName.parse(image), flink, unused -> {},
                    new FlinkClassLoadLog("taskmanager-1#1", root.resolve("classes.log")));
        }

        FuseCheckpointStorageRuntime.Mount configure() {
            return runtime.configure(container, "taskmanager-1", FlinkComponentRole.TASK_MANAGER);
        }

        private void block() {
            entered.countDown();
            while (true) {
                try { release.await(); break; }
                catch (InterruptedException ignored) { /* Deliberately model a non-cooperative Docker transport. */ }
            }
            driverReturned.countDown();
        }

        private <T> T command(Class<T> type, Runnable execute) {
            return proxy(type, (value, method, args) -> {
                if (method.getName().equals("exec")) {
                    execute.run();
                    return type == ExecCreateCmd.class ? new ExecCreateCmdResponse() {
                        @Override public String getId() { return "generated-exec"; }
                    } : null;
                }
                if (method.getName().equals("close")) return null;
                if (method.getName().startsWith("with")) return value;
                throw new AssertionError("Unexpected command call " + method.getName());
            });
        }
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
