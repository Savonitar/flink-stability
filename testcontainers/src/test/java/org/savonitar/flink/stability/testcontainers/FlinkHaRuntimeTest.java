package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class FlinkHaRuntimeTest {
    private static final FlinkRuntimeTarget.HighAvailability CONFIGURATION =
            new FlinkRuntimeTarget.HighAvailability("zookeeper:3.9.3", Duration.ofSeconds(6));

    @Test
    void healthyRoutingCoalescesOnlyIdenticalSamplesAndKeepsBoundariesAndChanges() throws Exception {
        FakeHandle first = new FakeHandle("jobmanager-1-1", "first");
        FakeHandle second = new FakeHandle("jobmanager-2-1", "second");
        var current = new java.util.concurrent.atomic.AtomicReference<>(leadership(first));
        try (FlinkHaRuntime runtime = new FlinkHaRuntime(CONFIGURATION, System::nanoTime,
                ignored -> Optional.of(current.get()), Map.of())) {
            runtime.register("jobmanager-1", first);
            runtime.register("jobmanager-2", second);
            runtime.initialRestEndpoint(Duration.ofSeconds(1));
            for (int index = 0; index < 5_000; index++) runtime.restEndpoint(Duration.ofSeconds(1));
            current.set(leadership(second));
            runtime.restEndpoint(Duration.ofSeconds(1));
            runtime.observeBeforeFence(Duration.ofSeconds(1));
            var history = runtime.observations(List.of());
            assertFalse(history.overflow());
            assertEquals(4, history.leadership().size());
            assertEquals(FlinkHaControl.ObservationMoment.INITIAL, history.leadership().getFirst().moment());
            assertEquals(5_000, history.leadership().get(1).sampleCount());
            assertEquals(5_002, history.leadership().get(2).sequence());
            assertEquals("second", history.leadership().get(2).leadership().orElseThrow().resourceManager().runtimeId());
            assertEquals(FlinkHaControl.ObservationMoment.PRE_FENCE, history.leadership().getLast().moment());
        }
    }

    @Test
    void historyKeepsObservationFailuresAndReportsOverflow() throws Exception {
        FakeHandle first = new FakeHandle("jobmanager-1-1", "first");
        FakeHandle second = new FakeHandle("jobmanager-2-1", "second");
        AtomicInteger reads = new AtomicInteger();
        try (FlinkHaRuntime runtime = new FlinkHaRuntime(CONFIGURATION, System::nanoTime, ignored -> {
            int read = reads.getAndIncrement();
            if (read == 1) throw new IllegalStateException("observer-disconnected");
            return Optional.of(leadership(read % 2 == 0 ? first : second));
        }, Map.of())) {
            runtime.register("jobmanager-1", first);
            runtime.register("jobmanager-2", second);
            runtime.initialRestEndpoint(Duration.ofSeconds(1));
            assertThrows(IllegalStateException.class, () -> runtime.restEndpoint(Duration.ofSeconds(1)));
            assertTrue(runtime.observations(List.of()).leadership().getLast().error().orElseThrow()
                    .contains("observer-disconnected"));
            for (int index = 0; index < FlinkHaRuntime.MAX_LEADERSHIP_OBSERVATIONS; index++) {
                runtime.restEndpoint(Duration.ofSeconds(1));
            }
            assertEquals(FlinkHaRuntime.MAX_LEADERSHIP_OBSERVATIONS, runtime.observations(List.of()).leadership().size());
            assertTrue(runtime.observations(List.of()).overflow());
        }
    }

    @Test
    void unsuccessfulPreFenceObservationIsRetainedWithoutThrowing() throws Exception {
        try (FlinkHaRuntime runtime = new FlinkHaRuntime(CONFIGURATION, System::nanoTime,
                ignored -> { throw new IllegalStateException("no-pre-fence-leader"); }, Map.of())) {
            runtime.observeBeforeFence(Duration.ofSeconds(1));
            var last = runtime.observations(List.of()).leadership().getLast();
            assertEquals(FlinkHaControl.ObservationMoment.PRE_FENCE, last.moment());
            assertTrue(last.leadership().isEmpty());
            assertTrue(last.error().orElseThrow().contains("no-pre-fence-leader"));
        }
    }

    @Test
    void ensembleConfigurationExplicitlyPermitsTheSupportedFlinkSessionRange() {
        assertTrue(FlinkHaRuntime.ZOOKEEPER_CONFIGURATION.contains("minSessionTimeout=2000\n"));
        assertTrue(FlinkHaRuntime.ZOOKEEPER_CONFIGURATION.contains("maxSessionTimeout=60000\n"));
    }

    @Test
    void decodesFlinksActualLeaderFormatAndRejectsOtherSerializedTypes() throws Exception {
        UUID session = UUID.randomUUID();
        String address = "pekko.tcp://flink@jobmanager-2-3:6123/user/rpc/resourcemanager_0";
        assertEquals(Optional.of(new FlinkHaRuntime.AddressSession(address, session.toString())),
                FlinkHaRuntime.decodeLeader(encoded(address, session)));
        assertThrows(IOException.class,
                () -> FlinkHaRuntime.decodeLeader(encoded(address, new HashMap<>())));
        assertThrows(IOException.class,
                () -> FlinkHaRuntime.decodeLeader(encoded(address, "not-a-session")));
        assertThrows(IOException.class, () -> FlinkHaRuntime.decodeLeader(new byte[16_385]));
        assertThrows(IOException.class, () -> FlinkHaRuntime.decodeLeader(new byte[] {1}));
    }

    @Test
    void unsetLeaderRecordsKeepPollingUntilAnActualLeaderIsPublished() throws Exception {
        FakeHandle leader = new FakeHandle("jobmanager-1-1", "leader");
        UUID session = UUID.randomUUID();
        byte[] published = encoded("pekko.tcp://flink@jobmanager-1-1:6123/user/rpc/resourcemanager_0", session);
        AtomicInteger reads = new AtomicInteger();
        try (FlinkHaRuntime runtime = new FlinkHaRuntime(CONFIGURATION, System::nanoTime, ignored -> {
            byte[] data = switch (reads.getAndIncrement()) {
                case 0 -> null;
                case 1 -> new byte[0];
                default -> published;
            };
            try {
                return FlinkHaRuntime.decodeLeader(data).map(record -> {
                    assertEquals(session.toString(), record.sessionId());
                    return leadership(leader);
                });
            } catch (IOException | ClassNotFoundException failure) {
                throw new AssertionError(failure);
            }
        }, Map.of())) {
            runtime.register("jobmanager-1", leader);
            assertEquals("http://localhost:8081", runtime.initialRestEndpoint(Duration.ofSeconds(2)));
            assertEquals(3, reads.get());
            var observations = runtime.observations(List.of()).leadership();
            assertEquals(3, observations.size());
            assertTrue(observations.get(0).leadership().isEmpty());
            assertTrue(observations.get(1).leadership().isEmpty());
            assertTrue(observations.stream().allMatch(sample -> sample.error().isEmpty()));
            assertEquals("leader", observations.getLast().leadership().orElseThrow().resourceManager().runtimeId());
        }
    }

    @Test
    void anUnsetLeaderRecordCannotExtendTheObservationDeadline() throws Exception {
        AtomicLong clock = new AtomicLong();
        try (FlinkHaRuntime runtime = new FlinkHaRuntime(CONFIGURATION, clock::get, ignored -> {
            clock.set(Duration.ofSeconds(1).toNanos());
            try {
                return FlinkHaRuntime.decodeLeader(null).map(record -> {
                    throw new AssertionError("An unset record cannot identify a leader");
                });
            } catch (IOException | ClassNotFoundException failure) {
                throw new AssertionError(failure);
            }
        }, Map.of())) {
            assertThrows(ContainerOperationTimeoutException.class,
                    () -> runtime.initialRestEndpoint(Duration.ofMillis(100)));
            var observations = runtime.observations(List.of()).leadership();
            assertEquals(1, observations.size());
            assertTrue(observations.getFirst().leadership().isEmpty());
            assertTrue(observations.getFirst().error().isEmpty());
        }
    }

    @Test
    void isolationClosesExistingConnectionRejectsNewConnectionsAndCanHeal() throws Exception {
        try (var executor = Executors.newSingleThreadExecutor();
             ServerSocket backend = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
             FlinkHaRuntime.TcpGate gate = new FlinkHaRuntime.TcpGate(
                     backend.getInetAddress().getHostAddress(), backend.getLocalPort())) {
            backend.setSoTimeout(3_000);
            var echo = executor.submit(() -> {
                for (int i = 0; i < 2; i++) {
                    try (Socket socket = backend.accept()) {
                        socket.setSoTimeout(3_000);
                        socket.getOutputStream().write(socket.getInputStream().read());
                        try {
                            socket.getInputStream().read();
                        } catch (SocketException closed) {
                            // Isolation may reset an established transport instead of sending EOF.
                        }
                    }
                }
                return null;
            });
            try (Socket first = client(gate)) {
                first.getOutputStream().write(17);
                assertEquals(17, first.getInputStream().read());
                assertEquals(1, gate.block());
                assertTrue(gate.blocked());
                assertClosed(first);
                try (Socket rejected = client(gate)) {
                    assertClosed(rejected);
                }
                assertEquals(1, gate.rejected());
                gate.unblock();
                assertFalse(gate.blocked());
                try (Socket healed = client(gate)) {
                    healed.getOutputStream().write(23);
                    assertEquals(23, healed.getInputStream().read());
                }
            }
            echo.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void killEvidenceKeepsOriginalIdentityAndActualReplacementState() throws Exception {
        FakeHandle original = new FakeHandle("jobmanager-1-1", "old");
        FakeHandle replacement = new FakeHandle("jobmanager-1-2", "replacement");
        FakeHandle standby = new FakeHandle("jobmanager-2-1", "standby");
        try (FlinkHaRuntime runtime = recordedRuntime(original, standby)) {
            var evidence = runtime.fault(request(FlinkHaControl.Mode.KILL), new FlinkHaRuntime.JobManagerActions() {
                @Override
                public FlinkHaControl.ProcessState kill(String name, ContainerOperationDeadline deadline) {
                    assertEquals("jobmanager-1", name);
                    original.running = false;
                    return original.processState(deadline);
                }

                @Override
                public ContainerHandle restart(String name, ContainerOperationDeadline deadline) {
                    return replacement;
                }
            }, Optional.empty());
            assertTrue(evidence.applied());
            assertTrue(evidence.healed());
            assertEquals("old", evidence.target().orElseThrow().runtimeId());
            assertFalse(evidence.faultState().orElseThrow().running());
            assertEquals("replacement", evidence.healedState().orElseThrow().runtimeId());
            assertEquals("standby", evidence.after().orElseThrow().resourceManager().runtimeId());
            assertTrue(evidence.errors().isEmpty());
        }
    }

    @Test
    void partiallyAppliedPauseIsHealedAndOriginalFailureIsRetained() throws Exception {
        FakeHandle original = new FakeHandle("jobmanager-1-1", "old");
        original.failPauseAfterPausing = true;
        try (FlinkHaRuntime runtime = recordedRuntime(original, new FakeHandle("jobmanager-2-1", "standby"))) {
            var evidence = runtime.fault(request(FlinkHaControl.Mode.PAUSE), unusedActions(), Optional.empty());
            assertFalse(evidence.applied());
            assertTrue(evidence.healed());
            assertFalse(original.paused);
            assertTrue(evidence.errors().stream().anyMatch(value -> value.contains("pause-inspection-failed")));
            assertTrue(evidence.after().isEmpty());
            assertEquals("old", evidence.healedState().orElseThrow().runtimeId());
        }
    }

    @Test
    void pausedProcessRetainsItsPhysicalIdentityAfterHealing() throws Exception {
        FakeHandle original = new FakeHandle("jobmanager-1-1", "old");
        try (FlinkHaRuntime runtime = recordedRuntime(original, new FakeHandle("jobmanager-2-1", "standby"))) {
            var evidence = runtime.fault(request(FlinkHaControl.Mode.PAUSE), unusedActions(), Optional.empty());
            assertTrue(evidence.applied());
            assertTrue(evidence.faultState().orElseThrow().running());
            assertTrue(evidence.faultState().orElseThrow().paused());
            assertTrue(evidence.healed());
            assertFalse(evidence.healedState().orElseThrow().paused());
            assertEquals("old", evidence.healedState().orElseThrow().runtimeId());
        }
    }

    @Test
    void consumedBudgetIsNotReplenishedAndExpirationAfterPauseStillResumes() throws Exception {
        AtomicLong clock = new AtomicLong();
        FakeHandle original = new FakeHandle("jobmanager-1-1", "old");
        original.afterPause = () -> clock.set(Duration.ofSeconds(2).toNanos());
        try (FlinkHaRuntime runtime = recordedRuntime(original,
                new FakeHandle("jobmanager-2-1", "standby"), clock::get)) {
            var evidence = runtime.fault(request(FlinkHaControl.Mode.PAUSE), Duration.ofSeconds(1),
                    unusedActions(), Optional.empty());
            assertEquals(1, original.resumeCalls);
            assertFalse(original.paused);
            assertTrue(evidence.healed());
            assertFalse(evidence.applied());
            assertTrue(evidence.after().isEmpty());
            assertEquals(Duration.ofSeconds(10), evidence.request().timeout());
            assertTrue(evidence.errors().stream().anyMatch(value -> value.contains("ContainerOperationTimeoutException")));
            assertTrue(evidence.errors().stream().noneMatch(value -> value.contains("healing failed")));
        }
    }

    @Test
    void tokenHealingFailureCannotSkipProcessHealingAndEveryFailureRemainsVisible() throws Exception {
        for (boolean failProcessHealing : List.of(false, true)) {
            FakeHandle original = new FakeHandle("jobmanager-1-1", "old");
            original.failPauseAfterPausing = true;
            original.failResumeAfterResuming = failProcessHealing;
            TokenServiceControl service = new TokenServiceControl() {
                public long configure(Mode mode, Duration delay) {
                    if (mode == Mode.HEALTHY) { throw new IllegalStateException("token-heal-failed"); }
                    return 1;
                }
                public Snapshot snapshot() { return new Snapshot(List.of(), false, false, 0, 0); }
            };
            try (FlinkHaRuntime runtime = recordedRuntime(original, new FakeHandle("jobmanager-2-1", "standby"))) {
                var request = new FlinkHaControl.LeaderFaultRequest(FlinkHaControl.Mode.PAUSE,
                        Duration.ofNanos(1), Duration.ofSeconds(10), Optional.of(
                                new FlinkHaControl.TokenFault(TokenServiceControl.Mode.FAIL, Duration.ZERO)));
                var evidence = runtime.fault(request, unusedActions(), Optional.of(service));
                assertEquals(1, original.resumeCalls);
                assertFalse(original.paused);
                assertFalse(evidence.healed());
                assertTrue(evidence.errors().stream().anyMatch(value -> value.contains("pause-inspection-failed")));
                assertTrue(evidence.errors().stream().anyMatch(value -> value.contains("token-heal-failed")));
                assertEquals(failProcessHealing,
                        evidence.errors().stream().anyMatch(value -> value.contains("process-heal-failed")));
                assertEquals(!failProcessHealing, evidence.healedState().isPresent());
            }
        }
    }

    @Test
    void targetedTokenHoldWaitsForCompleteNewLeaderJobExposureAndAlwaysHeals() throws Exception {
        for (boolean complete : List.of(false, true)) {
            FakeHandle original = new FakeHandle("jobmanager-1-1", "old");
            String jobId = "a".repeat(32);
            var target = new TokenServiceControl.JobTarget(jobId, "eos-job");
            AtomicInteger snapshotsAfterResume = new AtomicInteger();
            AtomicInteger heals = new AtomicInteger();
            TokenServiceControl service = new TokenServiceControl() {
                boolean armed;
                public long configure(Mode mode, Duration delay, JobTarget actual) {
                    assertEquals(target, actual);
                    armed = true;
                    return 1;
                }
                public long configure(Mode mode, Duration delay) {
                    assertEquals(Mode.HEALTHY, mode);
                    assertFalse(original.paused, "process hold must heal before waiting for JOB exposure");
                    assertTrue(snapshotsAfterResume.get() >= 2);
                    heals.incrementAndGet();
                    armed = false;
                    return 2;
                }
                public Snapshot snapshot() {
                    if (!armed || original.resumeCalls == 0) return new Snapshot(List.of(), false, false, 0, 0);
                    int count = snapshotsAfterResume.incrementAndGet();
                    var registration = new RegistrationSnapshot("provider", "JOB", 1, jobId, "eos-job",
                            false, 0, List.of(new Lifecycle(1, "REGISTER", 1, jobId, "eos-job")));
                    var events = new java.util.ArrayList<Event>();
                    var kinds = List.of(Kind.REQUEST_STARTED, Kind.ISSUED, Kind.REQUEST_FINISHED);
                    for (int i = 0; i < (complete && count >= 2 ? 3 : 1); i++) {
                        events.add(new Event(i + 1, kinds.get(i), "jobmanager-2#1", "jobmanager", 1,
                                i * 5_000_000_000L, 1, 1, Mode.DELAY, java.util.OptionalLong.of(1), "",
                                Optional.of(registration), Optional.of("provider")));
                    }
                    return new Snapshot(events, false, false, events.size() == 1 ? 1 : 0, 1);
                }
            };
            try (var runtime = recordedRuntime(original, new FakeHandle("jobmanager-2-1", "standby"), System::nanoTime)) {
                var request = new FlinkHaControl.LeaderFaultRequest(FlinkHaControl.Mode.PAUSE,
                        Duration.ofNanos(1), Duration.ofMillis(400), Optional.of(
                                new FlinkHaControl.TokenFault(TokenServiceControl.Mode.DELAY, Duration.ofSeconds(5), true)));
                var evidence = runtime.fault(request, request.timeout(), unusedActions(), Optional.of(service), Optional.of(target));
                assertEquals(1, heals.get());
                assertEquals(1, original.resumeCalls);
                assertTrue(evidence.healed());
                assertEquals(complete, evidence.errors().isEmpty());
                assertEquals(complete, evidence.after().isPresent());
            }
        }
    }

    private static FlinkHaRuntime recordedRuntime(FakeHandle original, FakeHandle standby) throws IOException {
        AtomicLong clock = new AtomicLong();
        return recordedRuntime(original, standby, () -> clock.addAndGet(1_000));
    }

    private static FlinkHaRuntime recordedRuntime(FakeHandle original, FakeHandle standby,
                                                 LongSupplier clock) throws IOException {
        AtomicInteger reads = new AtomicInteger();
        FlinkHaRuntime runtime = new FlinkHaRuntime(CONFIGURATION, clock,
                unused -> Optional.of(leadership(reads.getAndIncrement() == 0 ? original : standby)),
                Map.of("jobmanager-1", new FlinkHaRuntime.TcpGate("127.0.0.1", 1)));
        runtime.register("jobmanager-1", original);
        runtime.register("jobmanager-2", standby);
        return runtime;
    }

    private static FlinkHaControl.Leadership leadership(FakeHandle handle) {
        var identity = new FlinkHaControl.LeaderIdentity(handle.alias.startsWith("jobmanager-1-")
                ? "jobmanager-1" : "jobmanager-2", handle.id, "http://" + handle.alias + ":8081", handle.id + "-session");
        return new FlinkHaControl.Leadership(identity, identity, identity);
    }

    private static FlinkHaControl.LeaderFaultRequest request(FlinkHaControl.Mode mode) {
        return new FlinkHaControl.LeaderFaultRequest(mode, Duration.ofNanos(1), Duration.ofSeconds(10), Optional.empty());
    }

    private static FlinkHaRuntime.JobManagerActions unusedActions() {
        return new FlinkHaRuntime.JobManagerActions() {
            public FlinkHaControl.ProcessState kill(String name, ContainerOperationDeadline deadline) { throw new AssertionError("Unexpected kill"); }
            public ContainerHandle restart(String name, ContainerOperationDeadline deadline) { throw new AssertionError("Unexpected restart"); }
        };
    }

    private static Socket client(FlinkHaRuntime.TcpGate gate) throws IOException {
        Socket socket = new Socket(InetAddress.getLoopbackAddress(), gate.port());
        socket.setSoTimeout(3_000);
        return socket;
    }

    private static void assertClosed(Socket socket) throws IOException {
        try {
            assertEquals(-1, socket.getInputStream().read());
        } catch (SocketException reset) {
            // A reset and EOF both prove that the gate closed this transport.
        }
    }

    private static byte[] encoded(String address, Object session) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeUTF(address);
            output.writeObject(session);
        }
        return bytes.toByteArray();
    }

    private static final class FakeHandle implements ContainerHandle {
        private final String alias, id;
        private boolean running = true, paused, failPauseAfterPausing, failResumeAfterResuming;
        private Runnable afterPause = () -> {};
        private int resumeCalls;
        private FakeHandle(String alias, String id) { this.alias = alias; this.id = id; }
        public void start() { running = true; }
        public void startWithin(ContainerOperationDeadline deadline) { start(); }
        public void stop() { running = false; }
        public FlinkHaControl.ProcessState killAndRemoveWithin(ContainerOperationDeadline deadline) {
            stop(); return processState(deadline);
        }
        public FlinkHaControl.ProcessState killProcessForWriteFence(ContainerOperationDeadline deadline) {
            stop(); return processState(deadline);
        }
        public boolean isRunningWithin(ContainerOperationDeadline deadline) { return running; }
        public boolean isRunning() { return running; }
        public int mappedPort(int port) { return port; }
        public String runtimeId() { return id; }
        public String advertisedAlias() { return alias; }
        public FlinkComponentProvisioningEvidence provisioningEvidence() { throw new UnsupportedOperationException(); }
        public FlinkHaControl.ProcessState processState(ContainerOperationDeadline deadline) {
            deadline.remaining("reading test process state");
            return new FlinkHaControl.ProcessState(id, running, paused);
        }
        public void pauseWithin(ContainerOperationDeadline deadline) {
            paused = true;
            afterPause.run();
            if (failPauseAfterPausing) { throw new IllegalStateException("pause-inspection-failed"); }
        }
        public void resumeWithin(ContainerOperationDeadline deadline) {
            deadline.remaining("resuming test process");
            resumeCalls++;
            paused = false;
            if (failResumeAfterResuming) { throw new IllegalStateException("process-heal-failed"); }
        }
    }
}
