package org.savonitar.flink.stability.testcontainers;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

/** Attempt-owned HA infrastructure. Flink's own ZooKeeper records identify the elected process. */
final class FlinkHaRuntime implements AutoCloseable {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private final FlinkRuntimeTarget.HighAvailability configuration;
    private final GenericContainer<?> zookeeper;
    private final String clusterId = "flink-stability-" + UUID.randomUUID();
    private final Map<String, TcpGate> gates = new LinkedHashMap<>();
    private final Map<String, Process> incarnations = new LinkedHashMap<>();
    private final LongSupplier nanoTime;
    private final Function<ContainerOperationDeadline, Optional<FlinkHaControl.Leadership>> leadershipReader;
    private ZooKeeper observer;

    FlinkHaRuntime(Network network, FlinkRuntimeTarget.HighAvailability configuration,
                   LongSupplier nanoTime) {
        this.configuration = configuration;
        this.nanoTime = nanoTime;
        leadershipReader = this::observeZooKeeper;
        zookeeper = new GenericContainer<>(DockerImageName.parse(configuration.zookeeperImage()))
                .withNetwork(network).withNetworkAliases("flink-zookeeper")
                .withExposedPorts(2181).waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /** Lifecycle tests use recorded leader observations and real local gates without Docker. */
    FlinkHaRuntime(FlinkRuntimeTarget.HighAvailability configuration, LongSupplier nanoTime,
                   Function<ContainerOperationDeadline, Optional<FlinkHaControl.Leadership>> reader,
                   Map<String, TcpGate> gates) {
        this.configuration = configuration;
        this.nanoTime = nanoTime;
        this.leadershipReader = reader;
        this.gates.putAll(gates);
        zookeeper = null;
    }

    void start() {
        if (zookeeper == null) {
            // The injected lifecycle fixture already owns its gates and leader observations.
            return;
        }
        try {
            zookeeper.start();
            for (int index = 1; index <= 2; index++) {
                TcpGate gate = new TcpGate(zookeeper.getHost(), zookeeper.getMappedPort(2181));
                gates.put("jobmanager-" + index, gate);
                Testcontainers.exposeHostPorts(gate.port());
            }
            CountDownLatch connected = new CountDownLatch(1);
            ZKClientConfig clientConfiguration = new ZKClientConfig();
            clientConfiguration.setProperty("zookeeper.sasl.client", "false");
            observer = new ZooKeeper(zookeeper.getHost() + ":" + zookeeper.getMappedPort(2181),
                    10_000, event -> {
                        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                            connected.countDown();
                        }
                    }, false, clientConfiguration);
            if (!connected.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("ZooKeeper observer did not connect within 30 seconds");
            }
        } catch (IOException failure) {
            throw new IllegalStateException("Could not start HA observation", failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting HA observation", interrupted);
        }
    }

    String properties(String role, String logicalName, String incarnationAlias) {
        String quorum = "flink-zookeeper:2181";
        if (role.equals("jobmanager")) {
            quorum = "host.testcontainers.internal:" + gates.get(logicalName).port();
        }
        String result = "\nhigh-availability.type: zookeeper"
                + "\nhigh-availability.cluster-id: /" + clusterId
                + "\nhigh-availability.storageDir: file:/flink/checkpoints/ha"
                + "\nhigh-availability.zookeeper.quorum: " + quorum
                + "\nhigh-availability.zookeeper.client.session-timeout: "
                + configuration.sessionTimeout().toMillis() + " ms";
        if (role.equals("jobmanager")) {
            result += "\njobmanager.rpc.address: " + incarnationAlias
                    + "\njobmanager.bind-host: 0.0.0.0"
                    + "\nrest.address: " + incarnationAlias + "\nrest.bind-address: 0.0.0.0";
        }
        return result;
    }

    void register(String logicalName, ContainerHandle handle) {
        Process process = new Process(logicalName, handle.runtimeId(), handle.advertisedAlias(),
                "http://localhost:" + handle.mappedPort(FlinkContainer.JOB_MANAGER_PORT), handle);
        if (incarnations.putIfAbsent(process.alias(), process) != null) {
            throw new IllegalStateException("JobManager incarnation alias was reused: " + process.alias());
        }
    }

    String restEndpoint(Duration timeout) {
        ContainerOperationDeadline deadline = deadline(timeout);
        FlinkHaControl.Leadership leadership = awaitLeadership(deadline, ignored -> true);
        return process(leadership.restServer()).restEndpoint();
    }

    private ContainerOperationDeadline deadline(Duration timeout) {
        return ContainerOperationDeadline.start("observing or changing Flink leadership", timeout, nanoTime);
    }

    private Process process(FlinkHaControl.LeaderIdentity identity) {
        return incarnations.values().stream()
                .filter(value -> value.runtimeId().equals(identity.runtimeId())).findFirst().orElseThrow();
    }

    private FlinkHaControl.Leadership awaitLeadership(
            ContainerOperationDeadline deadline, Predicate<FlinkHaControl.Leadership> accept) {
        while (true) {
            deadline.remaining("observing elected leadership");
            Optional<FlinkHaControl.Leadership> observed = observe(deadline);
            if (observed.isPresent() && accept.test(observed.orElseThrow())) {
                return observed.orElseThrow();
            }
            pause(deadline, POLL_INTERVAL);
        }
    }

    private Optional<FlinkHaControl.Leadership> observe(ContainerOperationDeadline deadline) {
        return leadershipReader.apply(deadline);
    }

    private Optional<FlinkHaControl.Leadership> observeZooKeeper(ContainerOperationDeadline deadline) {
        return ContainerDriverCallBoundary.call(deadline, "reading ZooKeeper leader records", () -> {
            if (!observer.getState().isConnected()) {
                return Optional.empty();
            }
            List<FlinkHaControl.LeaderIdentity> leaders = new ArrayList<>();
            for (String component : List.of("resource_manager", "dispatcher", "rest_server")) {
                byte[] data;
                try {
                    data = observer.getData("/flink/" + clusterId + "/leader/" + component
                            + "/connection_info", false, new Stat());
                } catch (KeeperException.NoNodeException | KeeperException.ConnectionLossException missing) {
                    return Optional.empty();
                }
                if (data.length == 0) {
                    return Optional.empty();
                }
                AddressSession decoded = decodeLeader(data);
                String alias = URI.create(decoded.address()).getHost();
                Process registered = incarnations.get(alias);
                if (registered == null) {
                    throw new IllegalStateException("Leader address is not a registered JobManager incarnation: " + decoded.address());
                }
                if (!registered.handle().isRunningWithin(deadline)) {
                    return Optional.empty();
                }
                leaders.add(new FlinkHaControl.LeaderIdentity(registered.logicalName(),
                        registered.runtimeId(), decoded.address(), decoded.sessionId()));
            }
            // One Flink process owns the shared election; never join a torn transition snapshot.
            if (leaders.stream().map(FlinkHaControl.LeaderIdentity::runtimeId).distinct().count() != 1
                    || leaders.stream().map(FlinkHaControl.LeaderIdentity::sessionId).distinct().count() != 1) {
                return Optional.empty();
            }
            return Optional.of(new FlinkHaControl.Leadership(leaders.get(0), leaders.get(1), leaders.get(2)));
        });
    }

    static AddressSession decodeLeader(byte[] data) throws IOException, ClassNotFoundException {
        if (data.length > 16_384) {
            throw new IOException("ZooKeeper leader record exceeds the bounded protocol size");
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(data))) {
            input.setObjectInputFilter(info -> {
                if (info.depth() > 2 || info.references() > 8 || info.streamBytes() > 16_384) {
                    return ObjectInputFilter.Status.REJECTED;
                }
                return info.serialClass() == null ? ObjectInputFilter.Status.UNDECIDED
                        : info.serialClass() == UUID.class ? ObjectInputFilter.Status.ALLOWED
                        : ObjectInputFilter.Status.REJECTED;
            });
            String address = input.readUTF();
            Object session = input.readObject();
            if (!(session instanceof UUID id) || address.isBlank()) {
                throw new IOException("ZooKeeper leader record has no address and UUID session");
            }
            return new AddressSession(address, id.toString());
        }
    }

    FlinkHaControl.LeaderFaultEvidence fault(
            FlinkHaControl.LeaderFaultRequest request, JobManagerActions actions,
            Optional<? extends TokenServiceControl> tokenService) {
        return fault(request, request.timeout(), actions, tokenService);
    }

    FlinkHaControl.LeaderFaultEvidence fault(
            FlinkHaControl.LeaderFaultRequest request, Duration remainingBudget,
            JobManagerActions actions, Optional<? extends TokenServiceControl> tokenService) {
        if (remainingBudget.isZero() || remainingBudget.isNegative()
                || remainingBudget.compareTo(request.timeout()) > 0) {
            throw new IllegalArgumentException("Remaining fault budget must be positive and no greater than timeout");
        }
        ContainerOperationDeadline deadline = deadline(remainingBudget);
        Optional<FlinkHaControl.Leadership> before = Optional.empty(), after = Optional.empty();
        Optional<FlinkHaControl.LeaderIdentity> target = Optional.empty();
        Optional<FlinkHaControl.ProcessState> faultState = Optional.empty(), healedState = Optional.empty();
        List<String> errors = new ArrayList<>();
        Optional<TokenServiceControl.Snapshot> tokensBefore = tokenSnapshot(tokenService, "before", errors);
        Optional<TokenServiceControl.Snapshot> tokensDuring = Optional.empty(), tokensAfter = Optional.empty();
        boolean applied = false, healed = false, healingRequired = false;
        long armedAt = 0, healedAt = 0, closed = 0, rejectedBefore = 0;
        Process original = null;
        TcpGate gate = null;
        try {
            before = Optional.of(awaitLeadership(deadline, ignored -> true));
            target = Optional.of(before.orElseThrow().resourceManager());
            original = process(target.orElseThrow());
            gate = gates.get(original.logicalName());
            rejectedBefore = gate.rejected();
            if (deadline.remaining("reserving the fault hold").compareTo(request.duration()) <= 0) {
                throw new IllegalStateException("Insufficient operation budget to hold the requested fault");
            }
            if (request.tokenFault().isPresent()) {
                FlinkHaControl.TokenFault fault = request.tokenFault().orElseThrow();
                tokenService.orElseThrow(() -> new IllegalStateException("Token fixture is not enabled"))
                        .configure(fault.mode(), fault.delay());
            }
            healingRequired = true;
            armedAt = System.currentTimeMillis();
            switch (request.mode()) {
                case KILL -> {
                    actions.kill(original.logicalName(), deadline);
                    faultState = Optional.of(new FlinkHaControl.ProcessState(original.runtimeId(), false, false));
                }
                case PAUSE -> {
                    original.handle().pauseWithin(deadline);
                    faultState = Optional.of(original.handle().processState(deadline));
                }
                case ISOLATE_ZOOKEEPER -> {
                    closed = gate.block();
                    faultState = Optional.of(original.handle().processState(deadline));
                }
            }
            applied = true;
            long start = nanoTime.getAsLong();
            while (nanoTime.getAsLong() - start < request.duration().toNanos()) {
                pause(deadline, Duration.ofNanos(Math.min(POLL_INTERVAL.toNanos(),
                        request.duration().toNanos() - (nanoTime.getAsLong() - start))));
            }
            tokensDuring = tokenSnapshot(tokenService, "during", errors);
        } catch (RuntimeException failure) {
            errors.add(diagnostic(failure));
        } finally {
            if (tokensDuring.isEmpty()) {
                tokensDuring = tokenSnapshot(tokenService, "during", errors);
            }
            // Interrupted or timed-out fault work must not skip independent, bounded healing.
            boolean restoreInterrupt = Thread.interrupted();
            boolean tokenHealed = tokenService.isEmpty();
            try {
                if (tokenService.isPresent()) {
                    TokenServiceControl service = tokenService.orElseThrow();
                    ContainerDriverCallBoundary.run(deadline(FlinkHaControl.TOKEN_HEAL_TIMEOUT),
                            "healing the synthetic token service", () ->
                                    service.configure(TokenServiceControl.Mode.HEALTHY, Duration.ZERO));
                    tokenHealed = true;
                }
            } catch (RuntimeException failure) {
                errors.add("Token service healing failed: " + diagnostic(failure));
            }
            restoreInterrupt |= Thread.interrupted();
            try {
                if (healingRequired && original != null) {
                    ContainerOperationDeadline healingDeadline = deadline(FlinkHaControl.PROCESS_HEAL_TIMEOUT);
                    ContainerHandle resumed = original.handle();
                    switch (request.mode()) {
                        case KILL -> {
                            resumed = actions.restart(original.logicalName(), healingDeadline);
                            register(original.logicalName(), resumed);
                        }
                        case PAUSE -> resumed.resumeWithin(healingDeadline);
                        case ISOLATE_ZOOKEEPER -> {
                            TcpGate heldGate = gate;
                            ContainerDriverCallBoundary.run(healingDeadline,
                                    "healing the ZooKeeper gate", heldGate::unblock);
                        }
                    }
                    healedState = Optional.of(resumed.processState(healingDeadline));
                    healed = tokenHealed && healedState.orElseThrow().running() && !healedState.orElseThrow().paused()
                            && !gate.blocked();
                    healedAt = System.currentTimeMillis();
                }
            } catch (RuntimeException failure) {
                errors.add("Process or gate healing failed: " + diagnostic(failure));
            } finally {
                if (restoreInterrupt) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (applied && healed && before.isPresent()) {
            FlinkHaControl.LeaderIdentity previous = before.orElseThrow().resourceManager();
            try {
                after = Optional.of(awaitLeadership(deadline, current ->
                        !current.resourceManager().logicalName().equals(previous.logicalName())
                                && !current.resourceManager().runtimeId().equals(previous.runtimeId())
                                && !current.resourceManager().sessionId().equals(previous.sessionId())));
            } catch (RuntimeException failure) {
                errors.add("Leadership transfer unconfirmed: " + diagnostic(failure));
            }
        }
        tokensAfter = tokenSnapshot(tokenService, "after", errors);
        return new FlinkHaControl.LeaderFaultEvidence(request, before, after, target, applied, healed,
                faultState, healedState, armedAt, healedAt, closed,
                gate == null ? 0 : gate.rejected() - rejectedBefore, gate != null && gate.blocked(),
                tokensBefore, tokensDuring, tokensAfter, errors);
    }

    private static Optional<TokenServiceControl.Snapshot> tokenSnapshot(
            Optional<? extends TokenServiceControl> service, String phase, List<String> errors) {
        try {
            return service.map(TokenServiceControl::snapshot);
        } catch (RuntimeException failure) {
            errors.add("Token snapshot " + phase + " failed: " + diagnostic(failure));
            return Optional.empty();
        }
    }

    private static String diagnostic(Throwable failure) {
        StringBuilder result = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (!result.isEmpty()) {
                result.append("; caused by ");
            }
            result.append(current.getClass().getSimpleName()).append(": ").append(current.getMessage());
        }
        return result.toString();
    }

    private static void pause(ContainerOperationDeadline deadline, Duration wanted) {
        Duration remaining = deadline.remaining("waiting for HA transition");
        long nanos = Math.min(remaining.toNanos(), Math.max(1, wanted.toNanos()));
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for HA transition", interrupted);
        }
    }

    @Override
    public void close() {
        gates.values().forEach(TcpGate::close);
        RuntimeException failure = null;
        if (observer != null) {
            try {
                observer.close(5_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failure = new IllegalStateException("Interrupted while closing HA observer", interrupted);
            }
        }
        try {
            if (zookeeper != null) {
                zookeeper.stop();
            }
        } catch (RuntimeException stopFailure) {
            if (failure == null) {
                failure = stopFailure;
            } else {
                failure.addSuppressed(stopFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    interface JobManagerActions {
        void kill(String logicalName, ContainerOperationDeadline deadline);
        ContainerHandle restart(String logicalName, ContainerOperationDeadline deadline);
    }

    record AddressSession(String address, String sessionId) {}
    private record Process(String logicalName, String runtimeId, String alias,
                           String restEndpoint, ContainerHandle handle) {}

    /** A gate owns every socket and closes established connections before reporting isolation. */
    static final class TcpGate implements AutoCloseable {
        private final ServerSocket listener;
        private final InetSocketAddress backend;
        private final ExecutorService workers = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "flink-zookeeper-gate");
            thread.setDaemon(true);
            return thread;
        });
        private final Set<Connection> connections = new LinkedHashSet<>();
        private boolean blocked, closed;
        private long rejected;

        TcpGate(String backendHost, int backendPort) throws IOException {
            backend = new InetSocketAddress(backendHost, backendPort);
            listener = new ServerSocket(0, 32, InetAddress.getLoopbackAddress());
            workers.execute(this::accept);
        }

        int port() { return listener.getLocalPort(); }
        synchronized boolean blocked() { return blocked || closed; }
        synchronized long rejected() { return rejected; }

        synchronized long block() {
            blocked = true;
            long count = connections.size();
            List.copyOf(connections).forEach(Connection::close);
            return count;
        }

        synchronized void unblock() {
            if (closed) {
                throw new IllegalStateException("ZooKeeper gate is closed; healing cannot be confirmed");
            }
            blocked = false;
        }

        private void accept() {
            while (!listener.isClosed()) {
                try {
                    Socket client = listener.accept();
                    synchronized (this) {
                        if (blocked || closed || connections.size() >= 32) {
                            rejected++;
                            client.close();
                            continue;
                        }
                        Connection connection = new Connection(client, new Socket());
                        connections.add(connection);
                        workers.execute(() -> connect(connection));
                    }
                } catch (IOException failure) {
                    if (!listener.isClosed()) {
                        close();
                    }
                }
            }
        }

        private void connect(Connection connection) {
            try {
                connection.server.connect(backend, 5_000);
                workers.execute(() -> forward(connection, connection.server, connection.client));
                forward(connection, connection.client, connection.server);
            } catch (IOException | RuntimeException failure) {
                connection.close();
            }
        }

        private void forward(Connection connection, Socket source, Socket target) {
            try {
                source.getInputStream().transferTo(target.getOutputStream());
            } catch (IOException ignored) {
                // Closing either end terminates both pumps, including deliberate isolation.
            } finally {
                connection.close();
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
            block();
            closeSocket(listener);
            workers.shutdownNow();
        }

        private static void closeSocket(AutoCloseable socket) {
            try {
                socket.close();
            } catch (Exception ignored) {
                // Socket close is best effort; ownership remains with this gate.
            }
        }

        private final class Connection {
            private final Socket client, server;
            private final AtomicBoolean stopped = new AtomicBoolean();
            private Connection(Socket client, Socket server) {
                this.client = client;
                this.server = server;
            }
            private void close() {
                if (stopped.compareAndSet(false, true)) {
                    closeSocket(client);
                    closeSocket(server);
                    synchronized (TcpGate.this) {
                        connections.remove(this);
                    }
                }
            }
        }
    }
}
