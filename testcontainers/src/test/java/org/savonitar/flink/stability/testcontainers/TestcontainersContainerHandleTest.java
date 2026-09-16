package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.KillContainerCmd;
import com.github.dockerjava.api.exception.DockerException;
import com.github.dockerjava.api.exception.NotFoundException;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestcontainersContainerHandleTest {

    @Test
    void killUsesDockerKillThenRemovesTheExitedContainerWithoutStartingAReplacement() {
        RecordingGenericContainer container = new RecordingGenericContainer();
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);

        handle.killAndRemoveWithin(actionDeadline(Duration.ofSeconds(1)));

        assertEquals(List.of("kill:physical-1", "stop"), container.events);
        assertFalse(container.running);
    }

    @Test
    void explicitStartAndGracefulStopRemainSeparateOperations() {
        RecordingGenericContainer container = new RecordingGenericContainer();
        container.running = false;
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);

        handle.start();
        assertTrue(container.running);
        handle.stop();

        assertEquals(List.of("start", "stop"), container.events);
        assertFalse(container.running);
    }

    @Test
    void timedOutStartCannotRaceCleanupOnTheSamePhysicalHandle() throws Exception {
        RecordingGenericContainer container = new RecordingGenericContainer(
                false, false, true);
        container.running = false;
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);
        AtomicReference<Throwable> result = new AtomicReference<>();

        Thread action = Thread.ofPlatform().daemon(true).start(() -> {
            try {
                handle.startWithin(actionDeadline(Duration.ofMillis(100)));
            } catch (Throwable failure) {
                result.set(failure);
            }
        });

        assertTrue(container.startEntered.await(1, TimeUnit.SECONDS));
        action.join(Duration.ofSeconds(2));
        assertFalse(action.isAlive(), "hung Docker start exceeded the action deadline");
        ContainerOperationTimeoutException timeout = assertInstanceOf(
                ContainerOperationTimeoutException.class, result.get());
        assertEquals("performing TaskManager action", timeout.scope());

        Thread cleanup = Thread.ofPlatform().daemon(true).start(handle::stop);
        try {
            assertFalse(container.stopEntered.await(100, TimeUnit.MILLISECONDS),
                    "cleanup raced a late container start");
        } finally {
            container.releaseStart.countDown();
            cleanup.join(Duration.ofSeconds(1));
        }

        assertFalse(cleanup.isAlive());
        assertEquals(List.of("start", "stop"), container.events);
        assertFalse(container.running);
    }

    @Test
    void processWriteFenceKillsButRetainsThePhysicalContainerForLaterCleanup() {
        RecordingGenericContainer container = new RecordingGenericContainer();
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);

        handle.killProcessForWriteFence(fenceDeadline(Duration.ofSeconds(1)));

        assertEquals(List.of("kill:physical-1"), container.events);
        assertFalse(container.running);

        handle.stop();
        assertEquals(List.of("kill:physical-1", "stop"), container.events);
    }

    @Test
    void hungDockerKillCannotOutliveTheProcessFenceDeadline() throws Exception {
        RecordingGenericContainer container = new RecordingGenericContainer(true, false);
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);
        AtomicReference<Throwable> result = new AtomicReference<>();
        long startedAt = System.nanoTime();

        Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
            try {
                handle.killProcessForWriteFence(fenceDeadline(Duration.ofMillis(100)));
            } catch (Throwable failure) {
                result.set(failure);
            }
        });

        assertTrue(container.killEntered.await(1, TimeUnit.SECONDS));
        boolean returnedWithinBound;
        try {
            caller.join(Duration.ofSeconds(2));
            returnedWithinBound = !caller.isAlive();
        } finally {
            container.releaseKill.countDown();
            caller.join(Duration.ofSeconds(1));
        }

        assertTrue(returnedWithinBound, "hung Docker kill exceeded the generous wall-clock bound");
        assertTrue(Duration.ofNanos(System.nanoTime() - startedAt)
                .compareTo(Duration.ofSeconds(3)) < 0);
        IllegalStateException failure = assertInstanceOf(
                IllegalStateException.class, result.get());
        assertTrue(failure.getMessage().contains("sending SIGKILL to physical-1"));
    }

    @Test
    void hungDockerInspectCannotOutliveTheProcessFenceDeadline() throws Exception {
        RecordingGenericContainer container = new RecordingGenericContainer(false, true);
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);
        AtomicReference<Throwable> result = new AtomicReference<>();

        Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
            try {
                handle.isRunningWithin(fenceDeadline(Duration.ofMillis(100)));
            } catch (Throwable failure) {
                result.set(failure);
            }
        });

        assertTrue(container.inspectEntered.await(1, TimeUnit.SECONDS));
        boolean returnedWithinBound;
        try {
            caller.join(Duration.ofSeconds(2));
            returnedWithinBound = !caller.isAlive();
        } finally {
            container.releaseInspect.countDown();
            caller.join(Duration.ofSeconds(1));
        }

        assertTrue(
                returnedWithinBound,
                "hung Docker inspection exceeded the generous wall-clock bound");
        IllegalStateException failure = assertInstanceOf(
                IllegalStateException.class, result.get());
        assertTrue(failure.getMessage().contains("inspecting liveness for physical-1"));
    }

    @Test
    void interruptingFenceCallerCancelsDriverWaitAndPreservesInterruptStatus()
            throws Exception {
        RecordingGenericContainer container = new RecordingGenericContainer(false, true);
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean interruptedOnReturn = new AtomicBoolean();

        Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
            try {
                handle.isRunningWithin(fenceDeadline(Duration.ofSeconds(30)));
            } catch (Throwable failure) {
                result.set(failure);
                interruptedOnReturn.set(Thread.currentThread().isInterrupted());
            }
        });

        assertTrue(container.inspectEntered.await(1, TimeUnit.SECONDS));
        try {
            caller.interrupt();
            caller.join(Duration.ofSeconds(2));
        } finally {
            container.releaseInspect.countDown();
            caller.join(Duration.ofSeconds(1));
        }

        assertFalse(caller.isAlive());
        IllegalStateException failure = assertInstanceOf(
                IllegalStateException.class, result.get());
        assertNotNull(failure.getCause());
        assertTrue(failure.getMessage().contains("Interrupted while inspecting liveness"));
        assertTrue(interruptedOnReturn.get());
    }

    @Test
    void missingContainerIsTheOnlyInspectFailureTreatedAsStopped() {
        RecordingGenericContainer container = new RecordingGenericContainer();
        container.inspectFailure = new NotFoundException("container disappeared");
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);

        assertFalse(handle.isRunningWithin(fenceDeadline(Duration.ofSeconds(1))));
    }

    @Test
    void genericDockerInspectFailureCannotCertifyAStoppedProcess() {
        RecordingGenericContainer container = new RecordingGenericContainer();
        container.inspectFailure = new DockerException("daemon unavailable", 503);
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);

        DockerException failure = assertThrows(
                DockerException.class,
                () -> handle.isRunningWithin(fenceDeadline(Duration.ofSeconds(1))));

        assertEquals(503, failure.getHttpStatus());
        assertTrue(container.running);
    }

    @Test
    void cleanupDoesNotRaceAnInterruptIgnoringFenceDriverCall() throws Exception {
        RecordingGenericContainer container = new RecordingGenericContainer(false, true);
        TestcontainersContainerHandle.ContainerLifecycle handle = lifecycle(container);
        Thread fence = Thread.ofPlatform().daemon(true).start(() -> assertThrows(
                IllegalStateException.class,
                () -> handle.isRunningWithin(fenceDeadline(Duration.ofMillis(50)))));
        assertTrue(container.inspectEntered.await(1, TimeUnit.SECONDS));
        fence.join(Duration.ofSeconds(1));
        assertFalse(fence.isAlive());

        Thread cleanup = Thread.ofPlatform().daemon(true).start(handle::stop);
        try {
            assertFalse(container.stopEntered.await(100, TimeUnit.MILLISECONDS),
                    "cleanup reached GenericContainer.stop while inspect still owned the handle");
        } finally {
            container.releaseInspect.countDown();
            cleanup.join(Duration.ofSeconds(1));
        }

        assertFalse(cleanup.isAlive());
        assertEquals(List.of("stop"), container.events);
    }

    private static TestcontainersContainerHandle.ContainerLifecycle lifecycle(
            GenericContainer<?> container) {
        return new TestcontainersContainerHandle.ContainerLifecycle(container);
    }

    private static ContainerOperationDeadline fenceDeadline(Duration timeout) {
        return ContainerOperationDeadline.start(
                "establishing Flink process write fence", timeout, System::nanoTime);
    }

    private static ContainerOperationDeadline actionDeadline(Duration timeout) {
        return ContainerOperationDeadline.start(
                "performing TaskManager action", timeout, System::nanoTime);
    }

    private static final class RecordingGenericContainer
            extends GenericContainer<RecordingGenericContainer> {
        private final List<String> events = new ArrayList<>();
        private final DockerClient dockerClient;
        private final boolean hangKill;
        private final boolean hangInspect;
        private final boolean hangStart;
        private final CountDownLatch startEntered = new CountDownLatch(1);
        private final CountDownLatch killEntered = new CountDownLatch(1);
        private final CountDownLatch inspectEntered = new CountDownLatch(1);
        private final CountDownLatch stopEntered = new CountDownLatch(1);
        private final CountDownLatch releaseKill;
        private final CountDownLatch releaseInspect;
        private final CountDownLatch releaseStart;
        private RuntimeException inspectFailure;
        private boolean running = true;

        private RecordingGenericContainer() {
            this(false, false, false);
        }

        private RecordingGenericContainer(boolean hangKill, boolean hangInspect) {
            this(hangKill, hangInspect, false);
        }

        private RecordingGenericContainer(
                boolean hangKill,
                boolean hangInspect,
                boolean hangStart) {
            super(DockerImageName.parse("alpine:3.20"));
            this.hangKill = hangKill;
            this.hangInspect = hangInspect;
            this.hangStart = hangStart;
            this.releaseKill = new CountDownLatch(hangKill ? 1 : 0);
            this.releaseInspect = new CountDownLatch(hangInspect ? 1 : 0);
            this.releaseStart = new CountDownLatch(hangStart ? 1 : 0);
            KillContainerCmd killCommand = (KillContainerCmd) Proxy.newProxyInstance(
                    KillContainerCmd.class.getClassLoader(),
                    new Class<?>[]{KillContainerCmd.class},
                    (proxy, method, args) -> {
                        if ("exec".equals(method.getName())) {
                            killEntered.countDown();
                            if (hangKill) {
                                awaitUninterruptibly(releaseKill);
                            }
                            events.add("kill:physical-1");
                            running = false;
                            return null;
                        }
                        if (method.getReturnType().isInstance(proxy)) {
                            return proxy;
                        }
                        return defaultValue(method.getReturnType());
                    });
            InspectContainerCmd inspectCommand = (InspectContainerCmd) Proxy.newProxyInstance(
                    InspectContainerCmd.class.getClassLoader(),
                    new Class<?>[]{InspectContainerCmd.class},
                    (proxy, method, args) -> {
                        if ("exec".equals(method.getName())) {
                            inspectEntered.countDown();
                            if (hangInspect) {
                                awaitUninterruptibly(releaseInspect);
                            }
                            if (inspectFailure != null) {
                                throw inspectFailure;
                            }
                            return inspection(running);
                        }
                        if (method.getReturnType().isInstance(proxy)) {
                            return proxy;
                        }
                        return defaultValue(method.getReturnType());
                    });
            dockerClient = (DockerClient) Proxy.newProxyInstance(
                    DockerClient.class.getClassLoader(),
                    new Class<?>[]{DockerClient.class},
                    (proxy, method, args) -> {
                        if ("killContainerCmd".equals(method.getName())) {
                            assertEquals("physical-1", args[0]);
                            return killCommand;
                        }
                        if ("inspectContainerCmd".equals(method.getName())) {
                            assertEquals("physical-1", args[0]);
                            return inspectCommand;
                        }
                        return defaultValue(method.getReturnType());
                    });
        }

        @Override
        public void start() {
            startEntered.countDown();
            if (hangStart) {
                awaitUninterruptibly(releaseStart);
            }
            events.add("start");
            running = true;
        }

        @Override
        public void stop() {
            stopEntered.countDown();
            events.add("stop");
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public String getContainerId() {
            return "physical-1";
        }

        @Override
        public DockerClient getDockerClient() {
            return dockerClient;
        }

        private static void awaitUninterruptibly(CountDownLatch latch) {
            boolean interrupted = false;
            while (true) {
                try {
                    latch.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        private static InspectContainerResponse inspection(boolean running) {
            InspectContainerResponse response = new InspectContainerResponse();
            InspectContainerResponse.ContainerState state =
                    response.new ContainerState() {
                        @Override
                        public Boolean getRunning() {
                            return running;
                        }
                    };
            return new InspectContainerResponse() {
                @Override
                public ContainerState getState() {
                    return state;
                }
            };
        }

        private static Object defaultValue(Class<?> type) {
            if (!type.isPrimitive()) {
                return null;
            }
            if (type == boolean.class) {
                return false;
            }
            if (type == char.class) {
                return '\0';
            }
            if (type == byte.class) {
                return (byte) 0;
            }
            if (type == short.class) {
                return (short) 0;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            if (type == float.class) {
                return 0F;
            }
            return 0D;
        }
    }
}
