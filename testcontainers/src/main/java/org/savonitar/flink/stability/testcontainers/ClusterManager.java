package org.savonitar.flink.stability.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Owns one isolated Kafka/Flink runtime and its deterministic logical component registry.
 * Checkpoint storage is retained after close as run evidence; the caller (or repository clean)
 * owns its eventual deletion because container-created files may use a different host UID.
 */
public class ClusterManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
    private static final int CONSUMER_TIMEOUT_MS = 10_000;
    private static final String PRIMARY_JOB_MANAGER = "jobmanager-1";
    private static final String PRIMARY_TASK_MANAGER = "taskmanager-1";

    private final Network network;
    private final boolean ownsNetwork;
    private final Path checkpointStorageRoot;
    private final FlinkFactoryProvider flinkFactoryProvider;
    private final KafkaFactory kafkaFactory;
    private final LinkedHashMap<String, ComponentSlot> jobManagers = new LinkedHashMap<>();
    private final LinkedHashMap<String, ComponentSlot> taskManagers = new LinkedHashMap<>();
    private final List<ComponentSlot> pendingCleanup = new ArrayList<>();
    private final List<FlinkComponentProvisioningEvidence> provisioningHistory =
            new ArrayList<>();

    private KafkaCluster kafkaManager;
    private boolean kafkaReady;
    private FlinkComponentFactory flinkFactory;
    /** Target used for legacy creation of additional logical slots; each slot owns its desired target. */
    private FlinkRuntimeTarget defaultFlinkRuntimeTarget;
    private String jobManagerRestUrl;
    private String runningJobId;
    private int nextTaskManagerOrdinal = 1;
    private boolean cleanupStarted;
    private boolean networkClosed;
    private boolean closed;

    public ClusterManager() {
        this(defaultCheckpointStorageRoot());
    }

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
                KafkaManager::new,
                checkpointStorageRoot);
    }

    ClusterManager(
            Network network,
            boolean ownsNetwork,
            FlinkFactoryProvider flinkFactoryProvider,
            KafkaFactory kafkaFactory) {
        this(
                network,
                ownsNetwork,
                flinkFactoryProvider,
                kafkaFactory,
                Path.of("target", "test-checkpoints"));
    }

    ClusterManager(
            Network network,
            boolean ownsNetwork,
            FlinkFactoryProvider flinkFactoryProvider,
            KafkaFactory kafkaFactory,
            Path checkpointStorageRoot) {
        this.network = Objects.requireNonNull(network, "network");
        this.ownsNetwork = ownsNetwork;
        this.checkpointStorageRoot = Objects.requireNonNull(
                        checkpointStorageRoot, "checkpointStorageRoot")
                .toAbsolutePath()
                .normalize();
        this.flinkFactoryProvider = Objects.requireNonNull(
                flinkFactoryProvider, "flinkFactoryProvider");
        this.kafkaFactory = Objects.requireNonNull(kafkaFactory, "kafkaFactory");
    }

    public synchronized void checkKafkaUniqueIds(String topic, int expectedMessages) throws Exception {
        requireKafka().checkKafkaUniqueIds(topic, expectedMessages);
    }

    public synchronized void startKafka() throws IOException, InterruptedException {
        ensureOpen();
        if (kafkaManager != null) {
            throw new IllegalStateException("Kafka has already been started");
        }

        LOG.info("Starting Kafka");
        KafkaCluster candidate = Objects.requireNonNull(
                kafkaFactory.create(network, 1000), "Kafka factory returned null");
        try {
            candidate.start();
            candidate.createAndFillInInputTopic();
            String bootstrapServers = candidate.getBootstrapServers();
            kafkaManager = candidate;
            kafkaReady = true;
            LOG.info("Kafka started at: {}", bootstrapServers);
        } catch (RuntimeException | IOException | InterruptedException failure) {
            try {
                candidate.stop();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
                kafkaManager = candidate;
                kafkaReady = false;
            }
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw failure;
        }
    }

    /**
     * Legacy adapter: terminates only the primary TaskManager. Recovery is an explicit later
     * {@link #startNewTaskManager()} call; no replacement is created as a side effect of the kill.
     */
    @Deprecated
    public synchronized void simulateTaskManagerFailureAndRecovery() {
        killTaskManager(PRIMARY_TASK_MANAGER);
    }

    /**
     * Legacy adapter: restarts the lowest stopped logical slot, or creates one additional slot
     * when every existing slot is running.
     */
    @Deprecated
    public synchronized void startNewTaskManager() {
        ensureOpen();
        ensureFlinkStarted();
        for (ComponentSlot slot : taskManagers.values()) {
            if (!slot.isRunning()) {
                record(slot.start());
                LOG.info("TaskManager {} restarted as {}", slot.name(), slot.runtimeId());
                return;
            }
        }

        String name = taskManagerName(nextTaskManagerOrdinal);
        ComponentSlot slot = new ComponentSlot(
                name,
                FlinkComponentRole.TASK_MANAGER,
                defaultFlinkRuntimeTarget,
                () -> flinkFactory.newTaskManager(name));
        try {
            record(slot.start());
        } catch (RuntimeException failure) {
            if (slot.hasHandle() && !pendingCleanup.contains(slot)) {
                pendingCleanup.add(slot);
            }
            throw failure;
        }
        taskManagers.put(name, slot);
        nextTaskManagerOrdinal++;
        LOG.info("Additional TaskManager {} started as {}", name, slot.runtimeId());
    }

    public synchronized void startFlink(String version) throws InterruptedException, IOException {
        startFlink(legacyRuntimeTarget(version), 1, 1);
    }

    /** Starts one standalone JobManager and the requested deterministic TaskManager slots. */
    public synchronized void startFlink(
            String version, int jobManagerCount, int taskManagerCount)
            throws InterruptedException, IOException {
        startFlink(legacyRuntimeTarget(version), jobManagerCount, taskManagerCount);
    }

    /** Starts one standalone Flink cluster with one exact image/bundle runtime target. */
    public synchronized void startFlink(
            FlinkRuntimeTarget runtimeTarget,
            int jobManagerCount,
            int taskManagerCount)
            throws InterruptedException, IOException {
        validateFlinkStart(runtimeTarget, jobManagerCount, taskManagerCount);
        FlinkComponentFactory candidateFactory = createFlinkFactory(runtimeTarget);
        startFlink(runtimeTarget, candidateFactory, jobManagerCount, taskManagerCount);
    }

    private void startFlink(
            FlinkRuntimeTarget runtimeTarget,
            FlinkComponentFactory candidateFactory,
            int jobManagerCount,
            int taskManagerCount) {
        validateFlinkStart(runtimeTarget, jobManagerCount, taskManagerCount);
        Objects.requireNonNull(candidateFactory, "candidateFactory");

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

            for (int ordinal = 1; ordinal <= taskManagerCount; ordinal++) {
                String name = taskManagerName(ordinal);
                ComponentSlot taskManager = new ComponentSlot(
                        name,
                        FlinkComponentRole.TASK_MANAGER,
                        runtimeTarget,
                        () -> candidateFactory.newTaskManager(name));
                candidateTaskManagers.put(name, taskManager);
                record(taskManager.start());
            }

            int restPort = jobManager.mappedPort(FlinkContainer.JOB_MANAGER_PORT);
            candidateRestUrl = "http://localhost:" + restPort;
        } catch (RuntimeException failure) {
            cleanupSlots(candidateTaskManagers, failure);
            cleanupSlots(candidateJobManagers, failure);
            retainPendingCleanup(candidateTaskManagers);
            retainPendingCleanup(candidateJobManagers);
            throw failure;
        }

        flinkFactory = candidateFactory;
        defaultFlinkRuntimeTarget = runtimeTarget;
        jobManagers.putAll(candidateJobManagers);
        taskManagers.putAll(candidateTaskManagers);
        nextTaskManagerOrdinal = taskManagerCount + 1;
        jobManagerRestUrl = candidateRestUrl;

        LOG.info("Flink JobManager started at: {}", jobManagerRestUrl);
        LOG.info("Flink components: JobManagers={}, TaskManagers={}",
                jobManagers.keySet(), taskManagers.keySet());
    }

    public synchronized String killTaskManager(String name) {
        return terminate(taskManagers, name, Termination.KILL);
    }

    public synchronized String stopTaskManager(String name) {
        return terminate(taskManagers, name, Termination.STOP);
    }

    public synchronized String startTaskManager(String name) {
        return start(taskManagers, name);
    }

    public synchronized String killJobManager(String name) {
        return terminateJobManager(name, Termination.KILL);
    }

    public synchronized String stopJobManager(String name) {
        return terminateJobManager(name, Termination.STOP);
    }

    public synchronized String startJobManager(String name) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot jobManager = jobManagers.get(name);
        if (jobManager == null) {
            throw new IllegalArgumentException("Unknown component: " + name);
        }
        record(jobManager.start());
        try {
            int restPort = jobManager.mappedPort(FlinkContainer.JOB_MANAGER_PORT);
            jobManagerRestUrl = "http://localhost:" + restPort;
            String runtimeId = jobManager.runtimeId();
            LOG.info("Started {} as {}", name, runtimeId);
            return runtimeId;
        } catch (RuntimeException failure) {
            try {
                jobManager.stopForCleanup();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /** Restarts a uniform Flink cluster with the same desired runtime target. */
    public synchronized void restartFlink() throws InterruptedException, IOException {
        ensureOpen();
        ensureFlinkStarted();
        List<ComponentSlot> slots = new ArrayList<>(jobManagers.values());
        slots.addAll(taskManagers.values());
        Set<RuntimeTargetIdentity> identities = slots.stream()
                .map(ComponentSlot::desiredTarget)
                .map(ClusterManager::runtimeTargetIdentity)
                .collect(java.util.stream.Collectors.toCollection(
                        java.util.LinkedHashSet::new));
        if (identities.size() != 1) {
            throw new IllegalStateException(
                    "Flink restart without an image is ambiguous across desired runtime targets");
        }
        restartFlink(slots.getFirst().desiredTarget());
    }

    /** Restarts every Flink process with an explicit target while retaining checkpoint storage. */
    public synchronized void restartFlink(FlinkRuntimeTarget runtimeTarget)
            throws InterruptedException, IOException {
        ensureOpen();
        ensureFlinkStarted();
        FlinkComponentFactory candidateFactory = createFlinkFactory(runtimeTarget);
        int jobManagerCount = jobManagers.size();
        int taskManagerCount = taskManagers.size();
        stopFlink();
        startFlink(runtimeTarget, candidateFactory, jobManagerCount, taskManagerCount);
    }

    /**
     * Recreates one TaskManager with its desired target. If an earlier explicit retarget failed
     * after stopping the old process, this retries that candidate; it never restores the old
     * target implicitly.
     */
    public synchronized String restartTaskManager(String name) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(taskManagers, name);
        if (!slot.isRunning()) {
            return start(taskManagers, name);
        }
        return restartTaskManager(name, slot.desiredTarget());
    }

    /**
     * Retargets one running TaskManager explicitly. Candidate preflight occurs while the old
     * process remains running. Once the old process stops successfully, {@code candidateTarget}
     * becomes the slot's desired target. Candidate creation, start, or evidence-validation
     * failure then leaves the slot stopped on that desired target; {@link #startTaskManager}
     * or {@link #restartTaskManager(String)} retries it. The old target is never recreated
     * automatically.
     */
    public synchronized String restartTaskManager(
            String name, FlinkRuntimeTarget candidateTarget) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(taskManagers, name);
        if (!slot.isRunning()) {
            throw new IllegalStateException("Component is not running: " + name);
        }
        FlinkComponentFactory candidateFactory = createFlinkFactory(candidateTarget);
        ensureManifestCompatible(candidateTarget, slot);
        stopTaskManager(name);
        slot.retarget(candidateTarget, () -> candidateFactory.newTaskManager(name));
        record(slot.start());
        LOG.info("Restarted {} as {} with {}", name, slot.runtimeId(), candidateTarget);
        return slot.runtimeId();
    }

    /**
     * Recreates one JobManager with its desired target. If an earlier explicit retarget failed,
     * including while publishing its REST endpoint, this retries that candidate and never
     * restores the old target implicitly.
     */
    public synchronized String restartJobManager(String name) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(jobManagers, name);
        if (!slot.isRunning()) {
            return startJobManager(name);
        }
        return restartJobManager(name, slot.desiredTarget());
    }

    /**
     * Retargets one running JobManager explicitly. Candidate preflight occurs while the old
     * process remains running. Once the old process stops successfully, {@code candidateTarget}
     * becomes the slot's desired target. Candidate creation, start, evidence-validation, or REST
     * endpoint-publication failure then leaves the slot stopped on that target;
     * {@link #startJobManager} or {@link #restartJobManager(String)} retries it. Provisioning
     * evidence remains recorded when only endpoint publication fails. The old target is never
     * recreated automatically.
     */
    public synchronized String restartJobManager(
            String name, FlinkRuntimeTarget candidateTarget) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(jobManagers, name);
        if (!slot.isRunning()) {
            throw new IllegalStateException("Component is not running: " + name);
        }
        FlinkComponentFactory candidateFactory = createFlinkFactory(candidateTarget);
        ensureManifestCompatible(candidateTarget, slot);
        stopJobManager(name);
        slot.retarget(candidateTarget, () -> candidateFactory.newJobManager(name));
        record(slot.start());
        try {
            int restPort = slot.mappedPort(FlinkContainer.JOB_MANAGER_PORT);
            jobManagerRestUrl = "http://localhost:" + restPort;
            LOG.info("Restarted {} as {} with {}", name, slot.runtimeId(), candidateTarget);
            return slot.runtimeId();
        } catch (RuntimeException failure) {
            try {
                slot.stopForCleanup();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    public synchronized List<String> taskManagerNames() {
        return List.copyOf(taskManagers.keySet());
    }

    public synchronized List<String> jobManagerNames() {
        return List.copyOf(jobManagers.keySet());
    }

    /**
     * Append-only evidence for physical Flink containers that completed process start and
     * provisioning-evidence validation. A later JobManager endpoint-publication failure retains
     * its entry; creation, process-start, or evidence-validation failures add no entry.
     */
    public synchronized List<FlinkComponentProvisioningEvidence> provisioningHistory() {
        return List.copyOf(provisioningHistory);
    }

    public Path checkpointStorageRoot() {
        return checkpointStorageRoot;
    }

    public synchronized boolean isTaskManagerRunning(String name) {
        return requireSlot(taskManagers, name).isRunning();
    }

    public synchronized boolean isJobManagerRunning(String name) {
        return requireSlot(jobManagers, name).isRunning();
    }

    public synchronized boolean waitForAndValidateKafkaOutput(
            String topic, int expectedMessages) throws Exception {
        return requireKafka().waitForAndValidateKafkaOutput(topic, expectedMessages);
    }

    public synchronized boolean performFinalValidation(
            String topic, int expectedMessages) throws Exception {
        return requireKafka().performFinalValidation(topic, expectedMessages);
    }

    public synchronized void printKafka() throws InterruptedException, IOException {
        requireKafka().printMessages("flink-output", CONSUMER_TIMEOUT_MS, 1000);
    }

    /** Stops every Flink component, attempting all TaskManagers before all JobManagers. */
    public synchronized void stopFlink() {
        RuntimeException failure = null;
        failure = stopSlots(taskManagers, failure);
        failure = stopSlots(jobManagers, failure);
        failure = stopPendingCleanup(failure);
        jobManagerRestUrl = null;
        runningJobId = null;

        if (failure == null) {
            taskManagers.clear();
            jobManagers.clear();
            flinkFactory = null;
            defaultFlinkRuntimeTarget = null;
            nextTaskManagerOrdinal = 1;
            return;
        }
        throw failure;
    }

    /** Idempotently attempts Flink, Kafka, and network cleanup without hiding later failures. */
    public synchronized void stopAll() {
        if (closed) {
            return;
        }
        cleanupStarted = true;
        LOG.info("ClusterManager stopping everything");

        RuntimeException failure = null;
        try {
            stopFlink();
        } catch (RuntimeException flinkFailure) {
            failure = appendFailure(failure, flinkFailure);
        }

        if (kafkaManager != null) {
            try {
                kafkaManager.stop();
                kafkaManager = null;
                kafkaReady = false;
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

    public synchronized String getJobManagerRestUrl() {
        if (jobManagerRestUrl == null) {
            throw new IllegalStateException("Flink JobManager is not running");
        }
        return jobManagerRestUrl;
    }

    public synchronized void setRunningJobId(String jobId) {
        runningJobId = Objects.requireNonNull(jobId, "jobId");
    }

    public synchronized String getRunningJobId() {
        if (runningJobId == null) {
            throw new IllegalStateException("No Flink job has been recorded");
        }
        return runningJobId;
    }

    private static String taskManagerName(int ordinal) {
        return "taskmanager-" + ordinal;
    }

    private static Path defaultCheckpointStorageRoot() {
        return Path.of("checkpoints", "attempt-" + UUID.randomUUID());
    }

    private String terminate(
            Map<String, ComponentSlot> registry, String name, Termination termination) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(registry, name);
        String runtimeId = termination == Termination.KILL ? slot.kill() : slot.stop();
        LOG.info("{} {} ({})", termination.pastTense(), name, runtimeId);
        return runtimeId;
    }

    private String terminateJobManager(String name, Termination termination) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(jobManagers, name);
        if (!slot.isRunning()) {
            throw new IllegalStateException("Component is not running: " + name);
        }
        try {
            String runtimeId = termination == Termination.KILL ? slot.kill() : slot.stop();
            LOG.info("{} {} ({})", termination.pastTense(), name, runtimeId);
            return runtimeId;
        } finally {
            // Once termination is attempted, liveness may be uncertain on failure. Do not expose
            // a possibly stale endpoint or job identity.
            jobManagerRestUrl = null;
            runningJobId = null;
        }
    }

    private String start(Map<String, ComponentSlot> registry, String name) {
        ensureOpen();
        ensureFlinkStarted();
        ComponentSlot slot = requireSlot(registry, name);
        record(slot.start());
        LOG.info("Started {} as {}", name, slot.runtimeId());
        return slot.runtimeId();
    }

    private FlinkComponentFactory createFlinkFactory(FlinkRuntimeTarget runtimeTarget) {
        Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        runtimeTarget.connectorBundle().ifPresent(
                installation -> installation.classpathManifest().verifyHostFiles());
        return Objects.requireNonNull(
                flinkFactoryProvider.create(runtimeTarget, network, checkpointStorageRoot),
                "Flink component factory returned null");
    }

    private void validateFlinkStart(
            FlinkRuntimeTarget runtimeTarget,
            int jobManagerCount,
            int taskManagerCount) {
        ensureOpen();
        Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        if (!jobManagers.isEmpty() || !taskManagers.isEmpty() || flinkFactory != null
                || !pendingCleanup.isEmpty()) {
            throw new IllegalStateException("Flink has already been started");
        }
        if (jobManagerCount != 1) {
            throw new IllegalArgumentException(
                    "The standalone runtime supports exactly one JobManager");
        }
        if (taskManagerCount < 1) {
            throw new IllegalArgumentException("At least one TaskManager is required");
        }
    }

    private void ensureManifestCompatible(
            FlinkRuntimeTarget candidate, ComponentSlot replacedSlot) {
        Optional<String> candidateManifest = manifestIdentity(candidate);
        for (ComponentSlot slot : jobManagers.values()) {
            requireSameManifest(candidateManifest, slot, replacedSlot);
        }
        for (ComponentSlot slot : taskManagers.values()) {
            requireSameManifest(candidateManifest, slot, replacedSlot);
        }
    }

    private static void requireSameManifest(
            Optional<String> candidateManifest,
            ComponentSlot slot,
            ComponentSlot replacedSlot) {
        if (slot == replacedSlot) {
            return;
        }
        Optional<String> existingManifest = manifestIdentity(slot.desiredTarget());
        if (!candidateManifest.equals(existingManifest)) {
            throw new ConnectorBundleProvisioningException(
                    "A component restart would create different connector classpath manifests: "
                            + candidateManifest.orElse("legacy-disabled") + " versus "
                            + existingManifest.orElse("legacy-disabled") + " on " + slot.name());
        }
    }

    private static Optional<String> manifestIdentity(FlinkRuntimeTarget runtimeTarget) {
        return runtimeTarget.connectorBundle()
                .map(FlinkConnectorBundleInstallation::classpathManifest)
                .map(ConnectorClasspathManifest::manifestSha256);
    }

    private static RuntimeTargetIdentity runtimeTargetIdentity(
            FlinkRuntimeTarget runtimeTarget) {
        Optional<FlinkConnectorBundleInstallation> installation =
                runtimeTarget.connectorBundle();
        return new RuntimeTargetIdentity(
                runtimeTarget.imageReference(),
                installation.map(FlinkConnectorBundleInstallation::targetBindingSha256),
                installation.map(FlinkConnectorBundleInstallation::classpathManifest)
                        .map(ConnectorClasspathManifest::manifestSha256));
    }

    private static FlinkRuntimeTarget legacyRuntimeTarget(String imageReference) {
        if (imageReference == null || imageReference.isBlank()) {
            throw new IllegalArgumentException("Flink image cannot be null or blank");
        }
        return FlinkRuntimeTarget.legacy(imageReference);
    }

    private void record(FlinkComponentProvisioningEvidence evidence) {
        provisioningHistory.add(Objects.requireNonNull(evidence, "evidence"));
    }

    private record RuntimeTargetIdentity(
            String imageReference,
            Optional<String> targetBindingSha256,
            Optional<String> classpathManifestSha256) {}

    private static ComponentSlot requireSlot(Map<String, ComponentSlot> registry, String name) {
        ComponentSlot slot = registry.get(name);
        if (slot == null) {
            throw new IllegalArgumentException("Unknown component: " + name);
        }
        return slot;
    }

    private KafkaCluster requireKafka() {
        ensureOpen();
        if (kafkaManager == null || !kafkaReady) {
            throw new IllegalStateException("Kafka has not been started");
        }
        return kafkaManager;
    }

    private void ensureFlinkStarted() {
        if (flinkFactory == null || jobManagers.isEmpty()) {
            throw new IllegalStateException("Flink has not been started");
        }
    }

    private void ensureOpen() {
        if (closed || cleanupStarted) {
            throw new IllegalStateException("ClusterManager is closed");
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
    interface KafkaFactory {
        KafkaCluster create(Network network, int messages);
    }

    private enum Termination {
        KILL("Killed"),
        STOP("Stopped");

        private final String pastTense;

        Termination(String pastTense) {
            this.pastTense = pastTense;
        }

        String pastTense() {
            return pastTense;
        }
    }

    private static final class ComponentSlot {
        private final String name;
        private final FlinkComponentRole role;
        private FlinkRuntimeTarget desiredTarget;
        private Supplier<ContainerHandle> factory;
        private ContainerHandle handle;

        private ComponentSlot(
                String name,
                FlinkComponentRole role,
                FlinkRuntimeTarget desiredTarget,
                Supplier<ContainerHandle> factory) {
            this.name = Objects.requireNonNull(name, "name");
            this.role = Objects.requireNonNull(role, "role");
            this.desiredTarget = Objects.requireNonNull(desiredTarget, "desiredTarget");
            this.factory = Objects.requireNonNull(factory, "factory");
        }

        String name() {
            return name;
        }

        FlinkRuntimeTarget desiredTarget() {
            return desiredTarget;
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
                FlinkComponentProvisioningEvidence evidence = candidate.provisioningEvidence()
                        .orElseGet(() -> legacyEvidence(candidate));
                validateEvidence(evidence);
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

        void retarget(
                FlinkRuntimeTarget desiredTarget,
                Supplier<ContainerHandle> factory) {
            if (handle != null) {
                throw new IllegalStateException(
                        "A component must be stopped before changing its runtime target: " + name);
            }
            this.desiredTarget = Objects.requireNonNull(desiredTarget, "desiredTarget");
            this.factory = Objects.requireNonNull(factory, "factory");
        }

        String kill() {
            ContainerHandle running = requireRunning();
            String runtimeId = running.runtimeId();
            running.kill();
            if (running.isRunning()) {
                throw new IllegalStateException("Component remained running after kill: " + name);
            }
            handle = null;
            return runtimeId;
        }

        String stop() {
            ContainerHandle running = requireRunning();
            String runtimeId = running.runtimeId();
            running.stop();
            if (running.isRunning()) {
                throw new IllegalStateException("Component remained running after stop: " + name);
            }
            handle = null;
            return runtimeId;
        }

        void stopForCleanup() {
            if (handle == null) {
                return;
            }
            stopAndRelease("after cleanup");
        }

        boolean isRunning() {
            return handle != null && handle.isRunning();
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

        private ContainerHandle requireRunning() {
            if (!isRunning()) {
                throw new IllegalStateException("Component is not running: " + name);
            }
            return handle;
        }

        private FlinkComponentProvisioningEvidence legacyEvidence(ContainerHandle candidate) {
            if (desiredTarget.connectorBundleEnabled()) {
                throw new ConnectorBundleProvisioningException(
                        "Bundle-aware container returned no provisioning evidence: " + name);
            }
            return FlinkComponentProvisioningEvidence.legacy(
                    name, role, candidate.runtimeId(), desiredTarget.imageReference());
        }

        private void validateEvidence(FlinkComponentProvisioningEvidence evidence) {
            if (!name.equals(evidence.logicalName())
                    || role != evidence.role()
                    || !runtimeId().equals(evidence.runtimeId())
                    || !desiredTarget.imageReference().equals(evidence.imageReference())) {
                throw new ConnectorBundleProvisioningException(
                        "Flink component provisioning evidence does not match slot " + name);
            }
            Optional<FlinkConnectorBundleInstallation> expected =
                    desiredTarget.connectorBundle();
            if (expected.isEmpty()) {
                if (evidence.targetBindingSha256().isPresent()
                        || evidence.classpathManifestSha256().isPresent()
                        || !evidence.connectorArtifacts().isEmpty()
                        || evidence.verifiedBeforeProcessStart()) {
                    throw new ConnectorBundleProvisioningException(
                            "Legacy component unexpectedly reported connector bundle evidence: "
                                    + name);
                }
                return;
            }

            FlinkConnectorBundleInstallation installation = expected.get();
            List<ProvisionedConnectorArtifact> expectedArtifacts =
                    installation.classpathManifest().entries().stream()
                            .map(entry -> new ProvisionedConnectorArtifact(
                                    entry.index(), entry.containerPath(), entry.sha256()))
                            .toList();
            if (!evidence.targetBindingSha256().equals(
                            Optional.of(installation.targetBindingSha256()))
                    || !evidence.classpathManifestSha256().equals(Optional.of(
                            installation.classpathManifest().manifestSha256()))
                    || !evidence.connectorArtifacts().equals(expectedArtifacts)
                    || !evidence.verifiedBeforeProcessStart()) {
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
    }
}
