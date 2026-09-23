package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

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
    private static final String PRIMARY_TASK_MANAGER = "taskmanager-1";

    private final Network network;
    private final boolean ownsNetwork;
    private final Path checkpointStorageRoot;
    private final FlinkFactoryProvider flinkFactoryProvider;
    private final KafkaRuntimeFactory kafkaRuntimeFactory;
    private final LongSupplier monotonicNanos;
    private final LinkedHashMap<String, ComponentSlot> jobManagers = new LinkedHashMap<>();
    private final LinkedHashMap<String, ComponentSlot> taskManagers = new LinkedHashMap<>();
    private final List<ComponentSlot> pendingCleanup = new ArrayList<>();
    private final List<FlinkComponentProvisioningEvidence> provisioningHistory =
            new ArrayList<>();

    private KafkaRuntimeCluster kafkaRuntime;
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
        this(
                Network.newNetwork(),
                true,
                FlinkContainer::new,
                ApacheKafkaRuntime::new,
                checkpointStorageRoot);
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
        }
    }

    /** Starts the fixed first runtime shape: one JobManager and one TaskManager. */
    public synchronized String startFlink(FlinkRuntimeTarget runtimeTarget) {
        validateFlinkStart(runtimeTarget);
        FlinkComponentFactory candidateFactory = createFlinkFactory(runtimeTarget);
        LinkedHashMap<String, ComponentSlot> candidateJobManagers = new LinkedHashMap<>();
        LinkedHashMap<String, ComponentSlot> candidateTaskManagers = new LinkedHashMap<>();
        String candidateRestUrl;

        try {
            ComponentSlot jobManager = new ComponentSlot(
                    PRIMARY_JOB_MANAGER,
                    FlinkComponentRole.JOB_MANAGER,
                    runtimeTarget,
                    () -> candidateFactory.newJobManager(PRIMARY_JOB_MANAGER));
            candidateJobManagers.put(PRIMARY_JOB_MANAGER, jobManager);
            record(jobManager.start());

            ComponentSlot taskManager = new ComponentSlot(
                    PRIMARY_TASK_MANAGER,
                    FlinkComponentRole.TASK_MANAGER,
                    runtimeTarget,
                    () -> candidateFactory.newTaskManager(PRIMARY_TASK_MANAGER));
            candidateTaskManagers.put(PRIMARY_TASK_MANAGER, taskManager);
            record(taskManager.start());

            int restPort = jobManager.mappedPort(FlinkContainer.JOB_MANAGER_PORT);
            candidateRestUrl = "http://localhost:" + restPort;
        } catch (RuntimeException failure) {
            cleanupSlots(candidateTaskManagers, failure);
            cleanupSlots(candidateJobManagers, failure);
            retainPendingCleanup(candidateTaskManagers);
            retainPendingCleanup(candidateJobManagers);
            throw failure;
        }

        jobManagers.putAll(candidateJobManagers);
        taskManagers.putAll(candidateTaskManagers);
        LOG.info("Flink JobManager started at: {}", candidateRestUrl);
        LOG.info(
                "Flink components: JobManagers={}, TaskManagers={}",
                jobManagers.keySet(),
                taskManagers.keySet());
        return candidateRestUrl;
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
        String runtimeId = slot.kill(deadline);
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

    /**
     * Append-only evidence for physical Flink containers that completed process start and
     * provisioning-evidence validation.
     */
    public synchronized List<FlinkComponentProvisioningEvidence> provisioningHistory() {
        return List.copyOf(provisioningHistory);
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

    /** Idempotently attempts Flink, Kafka, and network cleanup without hiding later failures. */
    private synchronized void stopAll() {
        if (closed) {
            return;
        }
        cleanupStarted = true;
        LOG.info("ClusterManager stopping everything");

        RuntimeException failure = null;
        failure = stopFlink(failure);

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
        return Objects.requireNonNull(
                flinkFactoryProvider.create(runtimeTarget, network, checkpointStorageRoot),
                "Flink component factory returned null");
    }

    private void validateFlinkStart(FlinkRuntimeTarget runtimeTarget) {
        ensureFlinkProcessStartAllowed();
        Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        if (!jobManagers.isEmpty() || !taskManagers.isEmpty() || !pendingCleanup.isEmpty()) {
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

    private static RuntimeException killRunningSlotsForWriteFence(
            List<ComponentSlot> slots,
            ContainerOperationDeadline deadline,
            List<FlinkProcessWriteFenceEvidence.Component> evidence,
            RuntimeException failure) {
        RuntimeException result = failure;
        for (ComponentSlot slot : slots) {
            try {
                evidence.add(slot.killProcessForWriteFence(deadline));
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

    private static final class ComponentSlot {
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
                FlinkComponentProvisioningEvidence evidence = candidate.provisioningEvidence();
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

        String kill(ContainerOperationDeadline deadline) {
            ContainerHandle running = requireRunningWithin(deadline);
            String runtimeId = running.runtimeId();
            running.killAndRemoveWithin(deadline);
            // killAndRemoveWithin returns only after SIGKILL termination is confirmed and the
            // physical container is removed. Testcontainers clears its runtime ID during that
            // removal, so querying liveness on the removed handle is both redundant and invalid.
            handle = null;
            deadline.remaining("completing TaskManager removal for " + name);
            return runtimeId;
        }

        FlinkProcessWriteFenceEvidence.Component killProcessForWriteFence(
                ContainerOperationDeadline deadline) {
            deadline.remaining("checking liveness for " + name);
            Optional<String> runtimeId = physicalRuntimeId();
            if (handle == null || !handle.isRunningWithin(deadline)) {
                return new FlinkProcessWriteFenceEvidence.Component(
                        name,
                        role,
                        runtimeId,
                        FlinkProcessWriteFenceEvidence.Outcome.ALREADY_STOPPED);
            }
            handle.killProcessForWriteFence(deadline);
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
                return Optional.empty();
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
                    || !evidence.connectorArtifacts().equals(expectedArtifacts)) {
                throw new ConnectorBundleProvisioningException(
                        "Connector bundle provisioning evidence does not match target for " + name);
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
}
