package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;
import org.savonitar.flink.stability.runtime.api.KafkaProxyEndpoint;
import org.savonitar.flink.stability.runtime.api.KafkaProxyTarget;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.Testcontainers;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Owns one isolated Kafka/Flink attempt and its deterministic logical component registry.
 * Checkpoint storage is retained after close as run evidence; the caller owns its eventual
 * deletion because container-created files may use a different host UID.
 */
public final class ClusterManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
    private static final String PRIMARY_JOB_MANAGER = "jobmanager-1";
    private static final Duration IDENTITY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration PRE_FENCE_TELEMETRY_LIMIT = Duration.ofSeconds(10);
    private static final Duration PRE_FENCE_LEADERSHIP_LIMIT = Duration.ofSeconds(5);

    private final Network network;
    private final boolean ownsNetwork;
    private final Path checkpointStorageRoot;
    private final FlinkFactoryProvider flinkFactoryProvider;
    private final KafkaRuntimeFactory kafkaRuntimeFactory;
    private final HighAvailabilityFactory highAvailabilityFactory;
    private final LongSupplier monotonicNanos;
    private final LinkedHashMap<String, ComponentSlot> jobManagers = new LinkedHashMap<>();
    private final LinkedHashMap<String, ComponentSlot> taskManagers = new LinkedHashMap<>();
    private final List<ComponentSlot> pendingCleanup = new ArrayList<>();
    private final List<FlinkComponentProvisioningEvidence> provisioningHistory =
            new ArrayList<>();
    private final List<FlinkProcessWriteFenceEvidence.Observation> processObservations = new ArrayList<>();
    private final List<FlinkProcessWriteFenceEvidence.Component> fencedProcesses = new ArrayList<>();
    private boolean processObservationOverflow;
    private final List<FlinkComponentFactory> flinkFactories = new ArrayList<>();

    private KafkaRuntimeCluster kafkaRuntime;
    private java.util.Optional<org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence> retainedKafkaRuntimeEvidence = java.util.Optional.empty();
    private KroxyliciousProxy kafkaProxy;
    private FlinkHaRuntime highAvailability;
    private SyntheticTokenService tokenService;
    private SyntheticTokenPlugin tokenPlugin;
    private String standaloneRestEndpoint;
    private boolean flinkProcessWriteFenceStarted;
    private boolean cleanupStarted;
    private boolean networkClosed;
    private boolean closed;

    /**
     * Uses one caller-owned attempt directory for every Flink process and leaves it intact on
     * close so the caller can retain or report its state artifacts. If the directory already
     * exists, the caller must make it writable by the Flink container UID; newly created attempt
     * directories are prepared for container writes by {@link FlinkContainer}.
     */
    public ClusterManager(Path checkpointStorageRoot) {
        this(checkpointStorageRoot, new ClassLoadLogs(checkpointStorageRoot));
    }

    private ClusterManager(Path checkpointStorageRoot, ClassLoadLogs classLoadLogs) {
        this(
                Network.newNetwork(),
                true,
                createFlinkFactoryProvider(classLoadLogs),
                (ownedNetwork, target) -> KafkaRuntimeTarget.GENERIC_KRAFT.equals(target.launchType())
                        ? new GenericKraftKafkaRuntime(ownedNetwork, target)
                        : target.brokers() == 3 ? new ThreeBrokerKafkaRuntime(ownedNetwork, target) : new ApacheKafkaRuntime(ownedNetwork, target),
                checkpointStorageRoot);
    }

    private static FlinkFactoryProvider createFlinkFactoryProvider(ClassLoadLogs classLoadLogs) {
        AtomicReference<String> imageId = new AtomicReference<>();
        return (target, network, storage) -> new FlinkContainer(
                target, network, storage, classLoadLogs, imageId);
    }

    ClusterManager(
            Network network,
            boolean ownsNetwork,
            FlinkFactoryProvider flinkFactoryProvider,
            KafkaRuntimeFactory kafkaRuntimeFactory,
            Path checkpointStorageRoot) {
        this(
                network,
                ownsNetwork,
                flinkFactoryProvider,
                kafkaRuntimeFactory,
                checkpointStorageRoot,
                System::nanoTime);
    }

    ClusterManager(
            Network network,
            boolean ownsNetwork,
            FlinkFactoryProvider flinkFactoryProvider,
            KafkaRuntimeFactory kafkaRuntimeFactory,
            Path checkpointStorageRoot,
            LongSupplier monotonicNanos) {
        this(network, ownsNetwork, flinkFactoryProvider, kafkaRuntimeFactory,
                checkpointStorageRoot, monotonicNanos, FlinkHaRuntime::new);
    }

    ClusterManager(
            Network network,
            boolean ownsNetwork,
            FlinkFactoryProvider flinkFactoryProvider,
            KafkaRuntimeFactory kafkaRuntimeFactory,
            Path checkpointStorageRoot,
            LongSupplier monotonicNanos,
            HighAvailabilityFactory highAvailabilityFactory) {
        this.network = Objects.requireNonNull(network, "network");
        this.ownsNetwork = ownsNetwork;
        this.checkpointStorageRoot = Objects.requireNonNull(
                        checkpointStorageRoot, "checkpointStorageRoot")
                .toAbsolutePath()
                .normalize();
        this.flinkFactoryProvider = Objects.requireNonNull(
                flinkFactoryProvider, "flinkFactoryProvider");
        this.kafkaRuntimeFactory = Objects.requireNonNull(
                kafkaRuntimeFactory, "kafkaRuntimeFactory");
        this.monotonicNanos = Objects.requireNonNull(monotonicNanos, "monotonicNanos");
        this.highAvailabilityFactory = Objects.requireNonNull(highAvailabilityFactory, "highAvailabilityFactory");
    }

    /** Starts Kafka without creating topics or producing input. */
    public synchronized KafkaRuntimeEndpoints startKafka(KafkaRuntimeTarget runtimeTarget) {
        ensureOpen();
        Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        if (kafkaRuntime != null) {
            throw new IllegalStateException("Kafka has already been started");
        }

        LOG.info("Starting Kafka runtime {}", runtimeTarget.imageReference());
        KafkaRuntimeCluster candidate = Objects.requireNonNull(
                kafkaRuntimeFactory.create(network, runtimeTarget),
                "Kafka runtime factory returned null");
        try {
            candidate.start();
            KafkaRuntimeEndpoints endpoints = Objects.requireNonNull(
                    candidate.endpoints(), "Kafka runtime returned null endpoints");
            validateKafkaRuntimeEndpoints(runtimeTarget, endpoints);
            kafkaRuntime = candidate;
            LOG.info(
                    "Kafka started: internal={}, host={}",
                    endpoints.internalBootstrapServers(),
                    endpoints.hostBootstrapServers());
            return endpoints;
        } catch (RuntimeException failure) {
            try {
                candidate.stop();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
                kafkaRuntime = candidate;
            }
            throw failure;
        } finally {
            retainedKafkaRuntimeEvidence = candidate.runtimeEvidence();
        }
    }

    public synchronized java.util.Optional<org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence> kafkaRuntimeEvidence() {
        return retainedKafkaRuntimeEvidence;
    }

    /** Starts the declared JobManagers and TaskManagers with one shared attempt namespace. */
    public synchronized String startFlink(FlinkRuntimeTarget runtimeTarget) {
        validateFlinkStart(runtimeTarget);
        LinkedHashMap<String, ComponentSlot> candidateJobManagers = new LinkedHashMap<>();
        LinkedHashMap<String, ComponentSlot> candidateTaskManagers = new LinkedHashMap<>();
        String candidateRestUrl;

        try {
            if (runtimeTarget.tokenProvider().isPresent()) {
                tokenPlugin = new SyntheticTokenPlugin();
                try {
                    tokenService = SyntheticTokenService.start(runtimeTarget.tokenProvider()
                            .orElseThrow().renewalInterval().multipliedBy(2));
                } catch (IOException failure) {
                    throw new IllegalStateException("Could not start synthetic token fixture", failure);
                }
                Testcontainers.exposeHostPorts(tokenService.port());
            }
            if (runtimeTarget.highAvailability().isPresent()) {
                highAvailability = highAvailabilityFactory.create(network,
                        runtimeTarget.highAvailability().orElseThrow(), monotonicNanos);
                highAvailability.start();
            }
            FlinkComponentFactory candidateFactory = createFlinkFactory(runtimeTarget);
            if (highAvailability != null) {
                candidateFactory.configureHighAvailability(highAvailability);
            }
            if (tokenService != null) {
                candidateFactory.configureTokenProvider(tokenPlugin, tokenService.port());
            }
            for (int index = 1; index <= runtimeTarget.jobManagers(); index++) {
                String name = "jobmanager-" + index;
                ComponentSlot jobManager = new ComponentSlot(name, FlinkComponentRole.JOB_MANAGER,
                        runtimeTarget, () -> candidateFactory.newJobManager(name));
                candidateJobManagers.put(name, jobManager);
                record(jobManager.start());
                if (highAvailability != null) {
                    highAvailability.register(name, jobManager.handle);
                }
            }

            for (int index = 0; index < runtimeTarget.taskManagers(); index++) {
                String name = "taskmanager-" + (index + 1);
                ComponentSlot taskManager = new ComponentSlot(
                        name,
                        FlinkComponentRole.TASK_MANAGER,
                        runtimeTarget,
                        () -> candidateFactory.newTaskManager(name));
                candidateTaskManagers.put(name, taskManager);
                record(taskManager.start());
            }

            int restPort = candidateJobManagers.get(PRIMARY_JOB_MANAGER)
                    .mappedPort(FlinkContainer.JOB_MANAGER_PORT);
            candidateRestUrl = highAvailability == null ? "http://localhost:" + restPort
                    : highAvailability.initialRestEndpoint(Duration.ofMinutes(2));
        } catch (RuntimeException failure) {
            cleanupSlots(candidateTaskManagers, failure);
            cleanupSlots(candidateJobManagers, failure);
            retainPendingCleanup(candidateTaskManagers);
            retainPendingCleanup(candidateJobManagers);
            throw failure;
        }

        jobManagers.putAll(candidateJobManagers);
        taskManagers.putAll(candidateTaskManagers);
        standaloneRestEndpoint = candidateRestUrl;
        LOG.info("Flink JobManager started at: {}", candidateRestUrl);
        LOG.info(
                "Flink components: JobManagers={}, TaskManagers={}",
                jobManagers.keySet(),
                taskManagers.keySet());
        return candidateRestUrl;
    }

    public synchronized String currentFlinkRestEndpoint(Duration timeout) {
        ensureOpen();
        ensureFlinkStarted();
        return highAvailability == null ? standaloneRestEndpoint : highAvailability.restEndpoint(timeout);
    }

    public synchronized FlinkHaControl.LeaderFaultEvidence faultLeader(
            FlinkHaControl.LeaderFaultRequest request) {
        return faultLeader(request, request.timeout());
    }

    public synchronized FlinkHaControl.LeaderFaultEvidence faultLeader(
            FlinkHaControl.LeaderFaultRequest request, Duration remainingBudget) {
        return faultLeader(request, remainingBudget, Optional.empty());
    }

    public synchronized FlinkHaControl.LeaderFaultEvidence faultLeader(
            FlinkHaControl.LeaderFaultRequest request, Duration remainingBudget,
            Optional<TokenServiceControl.JobTarget> target) {
        ensureOpen();
        ensureFlinkProcessStartAllowed();
        if (highAvailability == null) {
            throw new IllegalStateException("JobManager leadership faults require ZooKeeper HA");
        }
        return highAvailability.fault(request, remainingBudget, new FlinkHaRuntime.JobManagerActions() {
            @Override
            public FlinkHaControl.ProcessState kill(String name, ContainerOperationDeadline deadline) {
                return killDeclared(requireSlot(jobManagers, name), deadline);
            }

            @Override
            public ContainerHandle restart(String name, ContainerOperationDeadline deadline) {
                ComponentSlot slot = requireSlot(jobManagers, name);
                record(slot.startWithin(deadline));
                return slot.handle;
            }
        }, Optional.ofNullable(tokenService), target);
    }

    public synchronized Optional<String> tokenPluginSha256() {
        return Optional.ofNullable(tokenPlugin).map(SyntheticTokenPlugin::sha256);
    }

    public synchronized Optional<TokenServiceControl.Snapshot> tokenServiceEvidence() {
        return Optional.ofNullable(tokenService).map(SyntheticTokenService::snapshot);
    }

    /** Kills only the named TaskManager; restart remains an explicit later operation. */
    public synchronized String killTaskManager(String name, Duration timeout) {
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "performing TaskManager kill for " + name,
                timeout,
                monotonicNanos);
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(taskManagers, name);
        String runtimeId = killDeclared(slot, deadline).runtimeId();
        LOG.info("Killed {} ({})", name, runtimeId);
        return runtimeId;
    }

    /** Recreates one previously killed TaskManager with its original verified runtime target. */
    public synchronized String restartTaskManager(String name, Duration timeout) {
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "performing TaskManager restart for " + name,
                timeout,
                monotonicNanos);
        ensureFlinkProcessStartAllowed();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(taskManagers, name);
        if (slot.isRunningWithin(deadline)) {
            throw new IllegalStateException(
                    "TaskManager must be killed before it can be restarted: " + name);
        }
        record(slot.startWithin(deadline));
        String runtimeId = slot.runtimeIdWithin(deadline);
        LOG.info("Restarted {} as {}", name, runtimeId);
        return runtimeId;
    }

    /** Does not reuse an earlier incarnation's identity after kill or failed replacement. */
    public synchronized Optional<TaskManagerControl.Identity> taskManagerIdentity(String name) {
        return taskManagerIdentity(name, IDENTITY_TIMEOUT);
    }

    public synchronized Optional<TaskManagerControl.Identity> taskManagerIdentity(String name, Duration timeout) {
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "reading TaskManager identity for " + name, timeout, monotonicNanos);
        ComponentSlot slot = taskManagers.get(name);
        if (slot == null || !slot.hasHandle()) {
            return Optional.empty();
        }
        ContainerHandle handle = slot.handle;
        return ContainerDriverCallBoundary.call(deadline, "observing TaskManager " + name, () -> {
            if (!handle.isRunning()) {
                return Optional.empty();
            }
            Optional<TaskManagerControl.Identity> identity = handle.taskManagerIdentity();
            identity.ifPresent(value -> {
                if (!name.equals(value.logicalName())
                        || !handle.runtimeId().equals(value.runtimeId())) {
                    throw new IllegalStateException(
                            "TaskManager identity does not match its handle: " + name);
                }
            });
            return identity;
        });
    }

    /**
     * Append-only evidence for physical Flink containers that completed process start and
     * provisioning-evidence validation.
     */
    public synchronized List<FlinkComponentProvisioningEvidence> provisioningHistory() {
        return List.copyOf(provisioningHistory);
    }

    public synchronized Optional<FlinkHaControl.Observations> haObservations() {
        return highAvailability == null ? Optional.empty() : Optional.of(highAvailability.observations(
                flinkFactories.stream().flatMap(factory -> factory.haSessions().stream()).toList()));
    }

    public synchronized FlinkProcessWriteFenceEvidence.Observations processObservations() {
        return new FlinkProcessWriteFenceEvidence.Observations(
                processObservations, fencedProcesses, processObservationOverflow);
    }

    private FlinkHaControl.ProcessState killDeclared(ComponentSlot slot, ContainerOperationDeadline deadline) {
        return slot.kill(deadline, stopped -> recordProcessState(slot,
                FlinkProcessWriteFenceEvidence.Moment.AFTER_DECLARED_KILL, stopped));
    }

    private void recordProcessState(ComponentSlot slot, FlinkProcessWriteFenceEvidence.Moment moment,
                                   FlinkHaControl.ProcessState state) {
        recordProcessObservation(new FlinkProcessWriteFenceEvidence.Observation(slot.name(), slot.role(),
                Optional.of(state.runtimeId()), moment, Instant.now(), Optional.of(state), false, Optional.empty()));
    }

    private void recordProcessObservation(FlinkProcessWriteFenceEvidence.Observation observation) {
        if (processObservations.size() < FlinkProcessWriteFenceEvidence.Observations.LIMIT) {
            processObservations.add(observation);
        } else {
            processObservationOverflow = true;
        }
    }

    private void observeBeforeFence(ComponentSlot slot, ContainerOperationDeadline deadline) {
        Optional<String> id = slot.physicalRuntimeId();
        try {
            if (slot.handle == null && id.isPresent()) {
                recordProcessObservation(new FlinkProcessWriteFenceEvidence.Observation(slot.name(), slot.role(),
                        id, FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE, Instant.now(), Optional.empty(),
                        true, Optional.of("Physical container was removed after its declared kill")));
                return;
            }
            if (slot.handle == null) {
                throw new IllegalStateException("No physical handle remains for " + slot.name());
            }
            recordProcessState(slot, FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE,
                    slot.handle.processState(deadline));
        } catch (RuntimeException failure) {
            recordProcessObservation(new FlinkProcessWriteFenceEvidence.Observation(slot.name(), slot.role(),
                    id, FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE, Instant.now(), Optional.empty(),
                    failure instanceof com.github.dockerjava.api.exception.NotFoundException,
                    Optional.of(failure.toString())));
        }
    }

    synchronized java.util.List<org.savonitar.flink.stability.runtime.api.FlinkComponentLog> componentLogs() {
        return flinkFactories.stream().flatMap(factory -> factory.componentLogs().stream()).toList();
    }

    synchronized List<FlinkClassLoadLog> classLoadLogs() {
        return flinkFactories.stream().flatMap(factory -> factory.classLoadLogs().stream())
                .distinct()
                .toList();
    }

    Path checkpointStorageRoot() {
        return checkpointStorageRoot;
    }

    synchronized List<String> taskManagerNames() {
        return List.copyOf(taskManagers.keySet());
    }

    synchronized List<String> jobManagerNames() {
        return List.copyOf(jobManagers.keySet());
    }

    synchronized boolean isTaskManagerRunning(String name) {
        return requireSlot(taskManagers, name).isRunning();
    }

    synchronized boolean isJobManagerRunning(String name) {
        return requireSlot(jobManagers, name).isRunning();
    }

    /**
     * Establishes the process side of the terminal write fence without graceful Flink shutdown.
     * Every running TaskManager is SIGKILLed and confirmed first, followed by every JobManager.
     * Physical handles remain registered so normal attempt cleanup can remove them.
     *
     * <p>One monotonic deadline is shared by every component operation, so the confirmation budget
     * does not multiply with the number of processes. A successful return guarantees that no
     * registered or pending-cleanup Flink process is running.</p>
     */
    public synchronized FlinkProcessWriteFenceEvidence establishFlinkProcessWriteFence(
            Duration timeout) {
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "establishing Flink process write fence", timeout, monotonicNanos);
        ensureOpen();
        ensureFlinkProcessesKnown();
        flinkProcessWriteFenceStarted = true;
        List<ComponentSlot> taskManagerFenceSlots = fenceSlots(
                taskManagers, FlinkComponentRole.TASK_MANAGER);
        List<ComponentSlot> jobManagerFenceSlots = fenceSlots(
                jobManagers, FlinkComponentRole.JOB_MANAGER);
        List<FlinkProcessWriteFenceEvidence.Component> evidence = new ArrayList<>();
        // Telemetry shares at most a quarter of the remaining physical fence budget, capped
        // at ten seconds. Inspect processes first; leadership receives at most five seconds.
        // An unavailable observer must not consume the deadline needed to stop writers.
        Duration remaining = deadline.remaining("reserving pre-fence telemetry");
        Duration telemetryBudget = remaining.dividedBy(4);
        if (telemetryBudget.isZero()) telemetryBudget = Duration.ofNanos(1);
        if (telemetryBudget.compareTo(PRE_FENCE_TELEMETRY_LIMIT) > 0) {
            telemetryBudget = PRE_FENCE_TELEMETRY_LIMIT;
        }
        ContainerOperationDeadline telemetry = ContainerOperationDeadline.start(
                "observing pre-fence process and leadership state", telemetryBudget, monotonicNanos);
        taskManagerFenceSlots.forEach(slot -> observeBeforeFence(slot, telemetry));
        jobManagerFenceSlots.forEach(slot -> observeBeforeFence(slot, telemetry));
        if (highAvailability != null) {
            try {
                Duration leadershipBudget = telemetry.remaining("observing pre-fence leadership");
                highAvailability.observeBeforeFence(leadershipBudget.compareTo(PRE_FENCE_LEADERSHIP_LIMIT) > 0
                        ? PRE_FENCE_LEADERSHIP_LIMIT : leadershipBudget);
            } catch (RuntimeException unavailable) {
                highAvailability.preFenceObservationFailed(unavailable);
            }
        }
        RuntimeException failure = null;
        failure = killRunningSlotsForWriteFence(
                taskManagerFenceSlots, deadline, evidence, failure);
        failure = killRunningSlotsForWriteFence(
                jobManagerFenceSlots, deadline, evidence, failure);
        failure = confirmNoRunningSlots(taskManagerFenceSlots, deadline, failure);
        failure = confirmNoRunningSlots(jobManagerFenceSlots, deadline, failure);
        if (failure != null) {
            throw failure;
        }
        return new FlinkProcessWriteFenceEvidence(evidence, Instant.now());
    }

    /**
     * Starts the one Kafka proxy of this attempt after Kafka. Its control directory sits in the
     * attempt directory, next to the Flink state, and is left in place on close as evidence.
     */
    public synchronized KafkaProxyEndpoint startKafkaProxy(KafkaProxyTarget target) {
        ensureOpen();
        Objects.requireNonNull(target, "target");
        if (kafkaRuntime == null) {
            throw new IllegalStateException("Kafka must be started before its proxy");
        }
        if (kafkaProxy != null) {
            throw new IllegalStateException("A Kafka proxy has already been started");
        }
        KroxyliciousProxy candidate = new KroxyliciousProxy(
                network,
                target,
                checkpointStorageRoot.resolve("flink-stability-proxy-" + target.proxyAlias()));
        kafkaProxy = candidate;
        KafkaProxyEndpoint endpoint = candidate.start();
        LOG.info("Kafka proxy started: {} -> {}",
                endpoint.bootstrapServers(), target.upstreamBootstrapServers());
        return endpoint;
    }

    /** Idempotently attempts Flink, Kafka, and network cleanup without hiding later failures. */
    private synchronized void stopAll() {
        if (closed) {
            return;
        }
        cleanupStarted = true;
        LOG.info("ClusterManager stopping everything");

        RuntimeException failure = null;
        failure = stopFlink(failure);
        if (tokenService != null) {
            try {
                tokenService.close();
            } catch (RuntimeException tokenFailure) {
                failure = appendFailure(failure, tokenFailure);
            }
        }
        if (highAvailability != null) {
            try {
                highAvailability.close();
                highAvailability = null;
            } catch (RuntimeException haFailure) {
                failure = appendFailure(failure, haFailure);
            }
        }

        if (kafkaProxy != null) {
            try {
                kafkaProxy.stop();
                kafkaProxy = null;
            } catch (RuntimeException proxyFailure) {
                failure = appendFailure(failure, proxyFailure);
            }
        }

        if (kafkaRuntime != null) {
            try {
                kafkaRuntime.stop();
                kafkaRuntime = null;
            } catch (RuntimeException kafkaFailure) {
                failure = appendFailure(failure, kafkaFailure);
            }
        }

        if (ownsNetwork && !networkClosed) {
            try {
                network.close();
                networkClosed = true;
            } catch (RuntimeException networkFailure) {
                failure = appendFailure(failure, networkFailure);
            }
        }

        if (failure != null) {
            throw failure;
        }
        closed = true;
    }

    @Override
    public void close() {
        stopAll();
    }

    private RuntimeException stopFlink(RuntimeException failure) {
        RuntimeException result = stopSlots(taskManagers, failure);
        result = stopSlots(jobManagers, result);
        result = stopPendingCleanup(result);
        if (result == null) {
            taskManagers.clear();
            jobManagers.clear();
        }
        return result;
    }

    private FlinkComponentFactory createFlinkFactory(FlinkRuntimeTarget runtimeTarget) {
        Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        runtimeTarget.connectorBundle().classpathManifest().verifyHostFiles();
        FlinkRuntimeTarget effectiveTarget = runtimeTarget;
        if (!provisioningHistory.isEmpty()) {
            String imageId = provisioningHistory.getFirst().imageId();
            runtimeTarget.expectedImageId().ifPresent(expected -> {
                if (!expected.equals(imageId)) {
                    throw new IllegalStateException(
                            "Flink image identity changed within the attempt: expected "
                                    + expected + ", actual " + imageId);
                }
            });
            effectiveTarget = runtimeTarget.withExpectedImageId(imageId);
        }
        FlinkComponentFactory factory = Objects.requireNonNull(
                flinkFactoryProvider.create(effectiveTarget, network, checkpointStorageRoot),
                "Flink component factory returned null");
        flinkFactories.add(factory);
        return factory;
    }

    private void validateFlinkStart(FlinkRuntimeTarget runtimeTarget) {
        ensureFlinkProcessStartAllowed();
        Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        if (!jobManagers.isEmpty() || !taskManagers.isEmpty() || !pendingCleanup.isEmpty()
                || highAvailability != null || tokenService != null) {
            throw new IllegalStateException("Flink has already been started");
        }
    }

    private void record(FlinkComponentProvisioningEvidence evidence) {
        provisioningHistory.add(Objects.requireNonNull(evidence, "evidence"));
    }

    private static ComponentSlot requireSlot(Map<String, ComponentSlot> registry, String name) {
        ComponentSlot slot = registry.get(name);
        if (slot == null) {
            throw new IllegalArgumentException("Unknown component: " + name);
        }
        return slot;
    }

    private static void validateKafkaRuntimeEndpoints(
            KafkaRuntimeTarget target,
            KafkaRuntimeEndpoints endpoints) {
        if (!target.clusterAlias().equals(endpoints.clusterAlias())
                || !target.imageReference().equals(endpoints.imageReference())
                || !target.internalBootstrapServers().equals(
                        endpoints.internalBootstrapServers())) {
            throw new IllegalStateException(
                    "Kafka runtime endpoints do not match the requested target");
        }
    }

    private void ensureFlinkStarted() {
        if (jobManagers.isEmpty()) {
            throw new IllegalStateException("Flink has not been started");
        }
    }

    private void ensureFlinkProcessesKnown() {
        if (jobManagers.isEmpty() && taskManagers.isEmpty() && pendingCleanup.isEmpty()) {
            throw new IllegalStateException("No Flink processes are registered");
        }
    }

    private void ensureOpen() {
        if (closed || cleanupStarted) {
            throw new IllegalStateException("ClusterManager is closed");
        }
    }

    private void ensureFlinkProcessStartAllowed() {
        ensureOpen();
        if (flinkProcessWriteFenceStarted) {
            throw new IllegalStateException(
                    "Flink process write fence has started; process creation is disabled");
        }
    }

    private static void cleanupSlots(
            LinkedHashMap<String, ComponentSlot> slots, RuntimeException original) {
        for (ComponentSlot slot : reversed(slots)) {
            try {
                slot.stopForCleanup();
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
            }
        }
    }

    private void retainPendingCleanup(LinkedHashMap<String, ComponentSlot> slots) {
        for (ComponentSlot slot : reversed(slots)) {
            if (slot.hasHandle() && !pendingCleanup.contains(slot)) {
                pendingCleanup.add(slot);
            }
        }
    }

    private RuntimeException stopPendingCleanup(RuntimeException failure) {
        RuntimeException result = failure;
        List<ComponentSlot> remaining = new ArrayList<>();
        for (ComponentSlot slot : pendingCleanup) {
            try {
                slot.stopForCleanup();
            } catch (RuntimeException cleanupFailure) {
                result = appendFailure(result, cleanupFailure);
                remaining.add(slot);
            }
        }
        pendingCleanup.clear();
        pendingCleanup.addAll(remaining);
        return result;
    }

    private static RuntimeException stopSlots(
            LinkedHashMap<String, ComponentSlot> slots, RuntimeException failure) {
        RuntimeException result = failure;
        for (ComponentSlot slot : reversed(slots)) {
            try {
                slot.stopForCleanup();
            } catch (RuntimeException cleanupFailure) {
                result = appendFailure(result, cleanupFailure);
            }
        }
        return result;
    }

    private RuntimeException killRunningSlotsForWriteFence(
            List<ComponentSlot> slots,
            ContainerOperationDeadline deadline,
            List<FlinkProcessWriteFenceEvidence.Component> evidence,
            RuntimeException failure) {
        RuntimeException result = failure;
        for (ComponentSlot slot : slots) {
            try {
                FlinkProcessWriteFenceEvidence.Component component = slot.killProcessForWriteFence(deadline,
                        stopped -> recordProcessState(slot,
                                FlinkProcessWriteFenceEvidence.Moment.AFTER_FENCE_KILL, stopped));
                evidence.add(component);
                if (fencedProcesses.size() < FlinkProcessWriteFenceEvidence.Observations.LIMIT) {
                    fencedProcesses.add(component);
                } else {
                    processObservationOverflow = true;
                }
            } catch (RuntimeException componentFailure) {
                result = appendFailure(result, componentFailure);
            }
        }
        return result;
    }

    private static RuntimeException confirmNoRunningSlots(
            List<ComponentSlot> slots,
            ContainerOperationDeadline deadline,
            RuntimeException failure) {
        RuntimeException result = failure;
        for (ComponentSlot slot : slots) {
            try {
                if (slot.isRunningForWriteFence(deadline)) {
                    result = appendFailure(result, new IllegalStateException(
                            "Flink process remained running after write fence: " + slot.name()));
                }
            } catch (RuntimeException confirmationFailure) {
                result = appendFailure(result, confirmationFailure);
            }
        }
        return result;
    }

    private List<ComponentSlot> fenceSlots(
            LinkedHashMap<String, ComponentSlot> activeSlots,
            FlinkComponentRole role) {
        List<ComponentSlot> result = reversed(activeSlots);
        for (ComponentSlot slot : pendingCleanup) {
            if (slot.role() == role && !result.contains(slot)) {
                result.add(slot);
            }
        }
        return result;
    }

    private static List<ComponentSlot> reversed(LinkedHashMap<String, ComponentSlot> slots) {
        List<ComponentSlot> reversed = new ArrayList<>(slots.values());
        Collections.reverse(reversed);
        return reversed;
    }

    private static RuntimeException appendFailure(
            RuntimeException aggregate, RuntimeException next) {
        if (aggregate == null) {
            return next;
        }
        if (aggregate != next) {
            aggregate.addSuppressed(next);
        }
        return aggregate;
    }

    @FunctionalInterface
    interface FlinkFactoryProvider {
        FlinkComponentFactory create(
                FlinkRuntimeTarget runtimeTarget,
                Network network,
                Path checkpointStorageRoot);
    }

    @FunctionalInterface
    interface KafkaRuntimeFactory {
        KafkaRuntimeCluster create(Network network, KafkaRuntimeTarget runtimeTarget);
    }

    interface HighAvailabilityFactory {
        FlinkHaRuntime create(Network network, FlinkRuntimeTarget.HighAvailability configuration,
                              LongSupplier nanoTime);
    }

    private final class ComponentSlot {
        private final String name;
        private final FlinkComponentRole role;
        private final FlinkRuntimeTarget runtimeTarget;
        private final Supplier<ContainerHandle> factory;
        private ContainerHandle handle;

        private ComponentSlot(
                String name,
                FlinkComponentRole role,
                FlinkRuntimeTarget runtimeTarget,
                Supplier<ContainerHandle> factory) {
            this.name = Objects.requireNonNull(name, "name");
            this.role = Objects.requireNonNull(role, "role");
            this.runtimeTarget = Objects.requireNonNull(runtimeTarget, "runtimeTarget");
            this.factory = Objects.requireNonNull(factory, "factory");
        }

        String name() {
            return name;
        }

        FlinkComponentRole role() {
            return role;
        }

        FlinkComponentProvisioningEvidence start() {
            if (handle != null && handle.isRunning()) {
                throw new IllegalStateException("Component is already running: " + name);
            }
            if (handle != null) {
                stopAndRelease("while preparing restart");
            }

            ContainerHandle candidate = Objects.requireNonNull(
                    factory.get(), "component factory returned null");
            handle = candidate;
            try {
                candidate.start();
                if (!candidate.isRunning()) {
                    throw new IllegalStateException("Component did not remain running: " + name);
                }
                FlinkComponentProvisioningEvidence evidence = candidate.provisioningEvidence();
                validateEvidence(evidence, candidate.runtimeId());
                return evidence;
            } catch (RuntimeException failure) {
                try {
                    stopAndRelease("after failed start");
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        FlinkComponentProvisioningEvidence startWithin(ContainerOperationDeadline deadline) {
            Objects.requireNonNull(deadline, "deadline");
            if (handle != null && handle.isRunningWithin(deadline)) {
                throw new IllegalStateException("Component is already running: " + name);
            }
            if (handle != null) {
                stopAndReleaseWithin(deadline, "while preparing restart");
            }

            ContainerHandle candidate = Objects.requireNonNull(
                    factory.get(), "component factory returned null");
            handle = candidate;
            try {
                candidate.startWithin(deadline);
                if (!candidate.isRunningWithin(deadline)) {
                    throw new IllegalStateException("Component did not remain running: " + name);
                }
                FlinkComponentProvisioningEvidence evidence = runtimeTarget.expectedRuntimeJar().isPresent()
                        || !runtimeTarget.config().isEmpty()
                        || runtimeTarget.tokenProvider().isPresent()
                        || !runtimeTarget.connectorBundle().imageConnectors().isEmpty()
                        ? ContainerDriverCallBoundary.call(deadline, "verifying provisioning for " + name,
                                candidate::provisioningEvidence)
                        : candidate.provisioningEvidence();
                validateEvidence(evidence, candidate.runtimeId());
                return evidence;
            } catch (RuntimeException failure) {
                try {
                    stopAndReleaseWithin(deadline, "after failed start");
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        FlinkHaControl.ProcessState kill(ContainerOperationDeadline deadline,
                java.util.function.Consumer<FlinkHaControl.ProcessState> observed) {
            ContainerHandle running = requireRunningWithin(deadline);
            String runtimeId = running.runtimeId();
            FlinkHaControl.ProcessState stopped = running.killAndRemoveWithin(deadline, state -> {
                if (!runtimeId.equals(state.runtimeId()) || state.running()) {
                    throw new IllegalStateException("Kill returned no stopped state for " + runtimeId);
                }
                lastStoppedState = state;
                observed.accept(state);
            });
            if (!runtimeId.equals(stopped.runtimeId()) || stopped.running()) {
                throw new IllegalStateException("Kill returned no stopped state for " + runtimeId);
            }
            // killAndRemoveWithin returns only after SIGKILL termination is confirmed and the
            // physical container is removed. Testcontainers clears its runtime ID during that
            // removal, so querying liveness on the removed handle is both redundant and invalid.
            handle = null;
            deadline.remaining("completing TaskManager removal for " + name);
            return stopped;
        }

        private FlinkHaControl.ProcessState lastStoppedState;

        FlinkProcessWriteFenceEvidence.Component killProcessForWriteFence(
                ContainerOperationDeadline deadline,
                java.util.function.Consumer<FlinkHaControl.ProcessState> observed) {
            deadline.remaining("checking liveness for " + name);
            Optional<String> runtimeId = physicalRuntimeId();
            if (handle == null || !handle.isRunningWithin(deadline)) {
                return new FlinkProcessWriteFenceEvidence.Component(
                        name,
                        role,
                        runtimeId,
                        FlinkProcessWriteFenceEvidence.Outcome.ALREADY_STOPPED);
            }
            lastStoppedState = handle.killProcessForWriteFence(deadline);
            if (lastStoppedState.running() || !runtimeId.equals(Optional.of(lastStoppedState.runtimeId()))) {
                throw new IllegalStateException("Fence kill returned no matching stopped state for " + name);
            }
            observed.accept(lastStoppedState);
            deadline.remaining("confirming process termination for " + name);
            if (handle.isRunningWithin(deadline)) {
                throw new IllegalStateException(
                        "Component remained running after process write fence: " + name);
            }
            return new FlinkProcessWriteFenceEvidence.Component(
                    name,
                    role,
                    runtimeId,
                    FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED);
        }

        private Optional<String> physicalRuntimeId() {
            if (handle == null) {
                return Optional.ofNullable(lastStoppedState).map(FlinkHaControl.ProcessState::runtimeId);
            }
            try {
                String runtimeId = handle.runtimeId();
                return runtimeId == null || runtimeId.isBlank()
                        ? Optional.empty()
                        : Optional.of(runtimeId);
            } catch (RuntimeException unavailable) {
                return Optional.empty();
            }
        }

        void stopForCleanup() {
            if (handle != null) {
                stopAndRelease("after cleanup");
            }
        }

        boolean isRunning() {
            return handle != null && handle.isRunning();
        }

        boolean isRunningForWriteFence(ContainerOperationDeadline deadline) {
            deadline.remaining("confirming liveness for " + name);
            return handle != null && handle.isRunningWithin(deadline);
        }

        boolean isRunningWithin(ContainerOperationDeadline deadline) {
            Objects.requireNonNull(deadline, "deadline");
            return handle != null && handle.isRunningWithin(deadline);
        }

        boolean hasHandle() {
            return handle != null;
        }

        int mappedPort(int containerPort) {
            return requireRunning().mappedPort(containerPort);
        }

        String runtimeId() {
            return requireRunning().runtimeId();
        }

        String runtimeIdWithin(ContainerOperationDeadline deadline) {
            return requireRunningWithin(deadline).runtimeId();
        }

        private ContainerHandle requireRunning() {
            if (!isRunning()) {
                throw new IllegalStateException("Component is not running: " + name);
            }
            return handle;
        }

        private ContainerHandle requireRunningWithin(ContainerOperationDeadline deadline) {
            if (!isRunningWithin(deadline)) {
                throw new IllegalStateException("Component is not running: " + name);
            }
            return handle;
        }

        private void validateEvidence(
                FlinkComponentProvisioningEvidence evidence,
                String runtimeId) {
            Objects.requireNonNull(evidence, "provisioningEvidence");
            if (!name.equals(evidence.logicalName())
                    || role != evidence.role()
                    || !Objects.requireNonNull(runtimeId, "runtimeId").equals(
                            evidence.runtimeId())
                    || !runtimeTarget.imageReference().equals(evidence.imageReference())) {
                throw new ConnectorBundleProvisioningException(
                        "Flink component provisioning evidence does not match slot " + name);
            }

            FlinkConnectorBundleInstallation installation = runtimeTarget.connectorBundle();
            List<ProvisionedConnectorArtifact> expectedArtifacts =
                    installation.classpathManifest().entries().stream()
                            .map(entry -> new ProvisionedConnectorArtifact(
                                    entry.index(), entry.containerPath(), entry.sha256()))
                            .toList();
            if (!evidence.targetBindingSha256().equals(installation.targetBindingSha256())
                    || !evidence.classpathManifestSha256().equals(
                            installation.classpathManifest().manifestSha256())
                    || !evidence.connectorArtifacts().equals(expectedArtifacts)
                    || !evidence.imageConnectorArtifacts().equals(installation.imageConnectors())) {
                throw new ConnectorBundleProvisioningException(
                        "Connector bundle provisioning evidence does not match target for " + name);
            }
            if (!evidence.flinkConfig().equals(runtimeTarget.config())
                    || (!runtimeTarget.config().isEmpty() && evidence.effectiveConfiguration().filter(receipt ->
                            receipt.observedValues().equals(runtimeTarget.config())
                                    && receipt.sourceSha256().equals(receipt.observedSha256())).isEmpty())) {
                throw new IllegalStateException("Flink effective configuration evidence does not match target for " + name);
            }
            runtimeTarget.expectedImageId().ifPresent(expected -> {
                if (!expected.equals(evidence.imageId())) {
                    throw new IllegalStateException(
                            "Flink image ID mismatch for " + name + ": expected "
                                    + expected + ", actual " + evidence.imageId());
                }
            });
            runtimeTarget.expectedRuntimeJar().ifPresent(expected -> {
                if (evidence.runtimeJarEvidence().filter(observed -> expected.equals(observed.jar()))
                        .isEmpty()) {
                    throw new IllegalStateException(
                            "Flink runtime JAR evidence does not match target for " + name);
                }
            });
            if (!provisioningHistory.isEmpty()) {
                String expected = provisioningHistory.getFirst().imageId();
                if (!expected.equals(evidence.imageId())) {
                    throw new IllegalStateException(
                            "Flink image changed for " + name + ": expected "
                                    + expected + ", actual " + evidence.imageId());
                }
            }
        }

        private void stopAndRelease(String context) {
            handle.stop();
            if (handle.isRunning()) {
                throw new IllegalStateException(
                        "Component remained running " + context + ": " + name);
            }
            handle = null;
        }

        private void stopAndReleaseWithin(
                ContainerOperationDeadline deadline,
                String context) {
            ContainerHandle current = handle;
            ContainerDriverCallBoundary.run(
                    deadline,
                    "stopping " + name + " " + context,
                    current::stop);
            if (current.isRunningWithin(deadline)) {
                throw new IllegalStateException(
                        "Component remained running " + context + ": " + name);
            }
            handle = null;
        }
    }
    public synchronized org.savonitar.flink.stability.runtime.api.PacketFaultControl.Evidence packetFault(
            org.savonitar.flink.stability.runtime.api.PacketFaultControl.Request request) {
        ensureOpen(); ensureFlinkStarted();
        if (!(kafkaRuntime instanceof ThreeBrokerKafkaRuntime kafka))
            throw new IllegalStateException("Packet probe requires the owned three-broker runtime");
        var identity = taskManagerIdentity(request.taskManager(), IDENTITY_TIMEOUT)
                .orElseThrow(() -> new IllegalStateException("No running owned TaskManager for packet probe"));
        return kafka.packetFault(request, identity);
    }

    public java.util.List<org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence> brokerFault(
            org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Request request,
            java.util.List<org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Partition> partitions) {
        return kafkaRuntime.brokerFault(request, partitions);
    }

    public org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence brokerOperation(String name, boolean restart,
            List<KafkaLogCapture.Partition> partitions, Duration timeout) {
        if (kafkaRuntime == null) throw new IllegalStateException("Kafka owner unavailable");
        return kafkaRuntime.brokerOperation(name, restart, partitions, timeout);
    }

    public KafkaLogCapture captureKafkaLogs(
            List<KafkaLogCapture.Partition> partitions,
            Path directory, MonotonicDeadline deadline) {
        if (kafkaRuntime == null) throw new IllegalStateException("Kafka owner unavailable");
        return kafkaRuntime.captureKafkaLogs(partitions, directory, deadline);
    }
}
