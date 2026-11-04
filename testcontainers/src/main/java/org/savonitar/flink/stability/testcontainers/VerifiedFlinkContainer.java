package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.ConnectorClasspathManifest;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.ImageConnectorArtifact;
import org.savonitar.flink.stability.runtime.api.ImageIdentityEvidence;
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

/** Verifies image identity and connector bytes while the created container is still stopped. */
final class VerifiedFlinkContainer extends GenericContainer<VerifiedFlinkContainer> {
    private static final int READ_ONLY_FILE_MODE = 0444;

    private final FlinkRuntimeTarget runtimeTarget;
    private final Consumer<String> imageIdVerifier;
    private final FlinkClassLoadLog classLoadLog;
    private final String resourceId;
    private final List<String> configuredBundleTargets = new ArrayList<>();
    private ConnectorBundleVerification verification;
    private volatile VerifiedImageIdentity verifiedImageIdentity;
    private String runtimeJarContainerId;
    private String imageConnectorContainerId;
    private List<ImageConnectorArtifact> imageConnectorArtifacts = List.of();
    private Map<String, String> observedConfig = Map.of();
    private String configurationContainerId;
    private byte[] preparedConfiguration;
    private List<String> configurationLauncher = List.of();
    private SyntheticTokenPlugin tokenPlugin;
    private String tokenPluginContainerId;
    private FlinkSessionLog sessionLog;
    private final RetainedFlinkLog retainedLog;
    private FuseCheckpointStorageRuntime storage;
    private FuseCheckpointStorageRuntime.Mount storageMount;

    void configureStorage(FuseCheckpointStorageRuntime storage, FuseCheckpointStorageRuntime.Mount mount) {
        this.storage = storage; this.storageMount = mount;
    }

    void storageProcessDead(FlinkHaControl.ProcessState proof, ContainerOperationDeadline deadline) {
        if (storage != null) storage.processDead(storageMount, proof, deadline);
    }

    void verifyStorageProcess() { if (storage != null) storage.verifyFlinkProcess(storageMount); }

    Optional<FlinkHaControl.ProcessState> capturedStorageProcessState(ContainerOperationDeadline deadline) {
        return storage == null ? Optional.empty() : Optional.of(storage.inspectProcess(storageMount, deadline));
    }

    private boolean literalConfiguration() { return !runtimeTarget.effectiveConfig().isEmpty(); }

    private Map<String, String> requestedConfiguration() { return runtimeTarget.effectiveConfig(); }

    @Override protected void containerIsStarting(com.github.dockerjava.api.command.InspectContainerResponse info) {
        super.containerIsStarting(info);
        if (storage != null) storage.start(storageMount);
    }

    VerifiedFlinkContainer(
            DockerImageName image,
            FlinkRuntimeTarget runtimeTarget,
            Consumer<String> imageIdVerifier,
            FlinkClassLoadLog classLoadLog) {
        super(Objects.requireNonNull(image, "image"));
        this.runtimeTarget = Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        this.imageIdVerifier = Objects.requireNonNull(imageIdVerifier, "imageIdVerifier");
        this.classLoadLog = Objects.requireNonNull(classLoadLog, "classLoadLog");
        retainedLog = new RetainedFlinkLog(classLoadLog);
        withLogConsumer(frame -> retainedLog.accept(frame.getBytes()));
        this.resourceId = "flink-stability-" + classLoadLog.process().replaceAll("[^A-Za-z0-9-]", "-")
                + "-" + UUID.randomUUID();
        ConnectorClasspathManifest manifest = runtimeTarget.connectorBundle().classpathManifest();
        manifest.verifyHostFiles();
        for (ConnectorClasspathManifest.Entry entry : manifest.entries()) {
            withCopyFileToContainer(
                    MountableFile.forHostPath(entry.preparedPath(), READ_ONLY_FILE_MODE),
                    entry.containerPath());
            configuredBundleTargets.add(entry.containerPath());
        }
        withCopyToContainer(
                Transferable.of(manifest.canonicalBytes(), READ_ONLY_FILE_MODE),
                ConnectorClasspathManifest.CONTAINER_MANIFEST_PATH);
        configuredBundleTargets.add(ConnectorClasspathManifest.CONTAINER_MANIFEST_PATH);
    }

    @Override public void start() {
        try { super.start(); }
        catch (RuntimeException | Error failure) {
            retainedLog.close();
            throw failure;
        }
    }

    @Override public void stop() {
        // Release the local descriptor even if Docker cleanup blocks or fails.
        retainedLog.close();
        super.stop();
    }

    /** Unique for this handle, including replacements and separate run attempts. */
    String resourceId() {
        return resourceId;
    }

    String advertisedAlias() {
        return classLoadLog.process().replace('#', '-');
    }

    org.savonitar.flink.stability.runtime.api.FlinkComponentLog componentLog() {
        return retainedLog.snapshot();
    }

    void observeHaSessions(String logicalName, FlinkComponentRole role) {
        runtimeTarget.highAvailability().ifPresent(ha -> {
            sessionLog = new FlinkSessionLog(logicalName, role, classLoadLog.process(),
                    ha.sessionTimeout().toMillis());
            withLogConsumer(frame -> sessionLog.accept(frame.getUtf8String()));
        });
    }

    Optional<FlinkHaControl.SessionEvidence> haSessionEvidence() {
        return sessionLog == null ? Optional.empty() : sessionLog.snapshot();
    }

    void installTokenPlugin(SyntheticTokenPlugin plugin) {
        tokenPlugin = Objects.requireNonNull(plugin, "plugin");
        withCopyToContainer(Transferable.of(plugin.bytes(), READ_ONLY_FILE_MODE),
                SyntheticTokenPlugin.CONTAINER_PATH);
    }

    /** Opt-in image contract; no image-specific entrypoint is allowed to reinterpret values. */
    void useLiteralConfigurationLauncher(String role) {
        if (!literalConfiguration()) return;
        configurationLauncher = FlinkProcessConfiguration.launcher(role);
        withCopyToContainer(Transferable.of(FlinkProcessConfiguration.launcherBytes(), READ_ONLY_FILE_MODE),
                FlinkProcessConfiguration.LAUNCHER_PATH);
        withEnv("FLINK_HOME", "/opt/flink");
        withEnv("FLINK_CONF_DIR", "/opt/flink/conf");
        withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/bin/bash", FlinkProcessConfiguration.LAUNCHER_PATH));
    }

    void verifyTokenPluginAfterStart(String containerId) {
        if (tokenPlugin != null) {
            if (!containerId.equals(tokenPluginContainerId)) {
                throw new IllegalStateException("Token plugin has no stopped-container verification");
            }
            tokenPlugin.verify(this::readJarSha256);
        }
    }

    @Override
    protected void containerIsCreated(String containerId) {
        super.containerIsCreated(containerId);
        if (sessionLog != null) sessionLog.created(containerId);
        retainedLog.created(containerId);
        // Testcontainers 1.21.4 invokes this hook after its configured archive copies and before
        // Docker's startContainer command. A mismatch therefore prevents the Flink entrypoint.
        var imageDeadline = ContainerOperationDeadline.start("Flink image identity", Duration.ofSeconds(30), System::nanoTime);
        verifyImageIdentity(containerId, id -> ImageIdentityVerifier.capture(getDockerClient(), id, imageDeadline));
        verifyRuntimeJarBeforeStart(containerId, this::readJarSha256);
        verifyCopiedBundle(runtimeTarget.connectorBundle());
        verifyImageConnectorsBeforeStart(containerId);
        if (literalConfiguration()) {
            var config = getDockerClient().inspectContainerCmd(containerId).exec().getConfig();
            if (storage == null) verifyConfigurationLaunch(Arrays.asList(config.getEnv()), Arrays.asList(config.getEntrypoint()),
                    Arrays.asList(config.getCmd()));
            else {
                verifyStorageEnvironment(Arrays.asList(config.getEnv()));
                storage.verifyCreated(storageMount);
            }
            prepareProcessConfiguration(containerId, this::readConfiguration,
                    (path, bytes) -> copyFileToContainer(Transferable.of(bytes, READ_ONLY_FILE_MODE), path));
        }
        tokenPluginContainerId = null;
        if (tokenPlugin != null) {
            tokenPlugin.verify(this::readJarSha256);
            tokenPluginContainerId = containerId;
        }
    }

    private void verifyStorageEnvironment(List<String> environment) {
        for (String key : List.of("FLINK_HOME", "FLINK_CONF_DIR", "FLINK_PROPERTIES", "JOB_MANAGER_RPC_ADDRESS")) {
            String expected = key + "=" + getEnvMap().get(key);
            if (!environment.stream().filter(value -> value.startsWith(key + "=")).toList().equals(List.of(expected)))
                throw new IllegalStateException("FUSE Flink launch environment differs at " + key);
        }
    }

    void verifyConfigurationLaunch(List<String> environment, List<String> entrypoint, List<String> command) {
        if (!literalConfiguration()) return;
        String expected = "FLINK_PROPERTIES=" + getEnvMap().get("FLINK_PROPERTIES");
        List<String> actual = environment.stream()
                .filter(value -> value.startsWith("FLINK_PROPERTIES=")).toList();
        if (!actual.equals(List.of(expected)) || !entrypoint.equals(configurationLauncher.subList(0, 2))
                || !command.equals(configurationLauncher.subList(2, 3))) {
            throw new IllegalStateException("Flink configuration environment or launcher differs from the declared plan");
        }
    }

    /** Each stopped physical container receives and verifies its own full standard YAML file. */
    void prepareProcessConfiguration(String containerId, Function<String, byte[]> reader,
                                     java.util.function.BiConsumer<String, byte[]> writer) {
        configurationContainerId = null;
        preparedConfiguration = null;
        observedConfig = Map.of();
        if (!literalConfiguration()) return;
        if (configurationLauncher.isEmpty()) throw new IllegalStateException("No literal Flink configuration launcher");
        byte[] merged = FlinkProcessConfiguration.merge(reader.apply(FlinkProcessConfiguration.PATH),
                getEnvMap().get("FLINK_PROPERTIES"), requestedConfiguration(),
                getEnvMap().get("JOB_MANAGER_RPC_ADDRESS"));
        writer.accept(FlinkProcessConfiguration.PATH, merged.clone());
        if (!Arrays.equals(merged, reader.apply(FlinkProcessConfiguration.PATH))) {
            throw new IllegalStateException("Flink stopped-container configuration copy differs from the prepared file");
        }
        preparedConfiguration = merged;
        configurationContainerId = Objects.requireNonNull(containerId, "containerId");
    }

    Optional<FlinkComponentProvisioningEvidence.EffectiveConfigurationEvidence> effectiveConfigurationEvidence(
            String containerId) {
        if (!literalConfiguration()) return Optional.empty();
        return effectiveConfigurationEvidence(containerId, this::readConfiguration, getLogs());
    }

    Optional<FlinkComponentProvisioningEvidence.EffectiveConfigurationEvidence> effectiveConfigurationEvidence(
            String containerId, Function<String, byte[]> reader, String startupLog) {
        if (!literalConfiguration()) return Optional.empty();
        if (!Objects.equals(configurationContainerId, containerId) || preparedConfiguration == null) {
            throw new IllegalStateException("Flink configuration has no stopped-container verification for " + containerId);
        }
        byte[] actual = reader.apply(FlinkProcessConfiguration.PATH);
        observedConfig = FlinkProcessConfiguration.observe(actual, preparedConfiguration, requestedConfiguration(), startupLog);
        return Optional.of(new FlinkComponentProvisioningEvidence.EffectiveConfigurationEvidence(
                FlinkProcessConfiguration.PATH, Digests.sha256(preparedConfiguration), Digests.sha256(actual),
                configurationLauncher, observedConfig));
    }

    private byte[] readConfiguration(String path) {
        return copyFileFromContainer(path, input -> {
            byte[] bytes = input.readNBytes(FlinkProcessConfiguration.MAX_BYTES + 1);
            if (bytes.length > FlinkProcessConfiguration.MAX_BYTES) {
                throw new IllegalStateException("Flink configuration exceeds the 2 MiB evidence limit");
            }
            return bytes;
        });
    }

    Map<String, String> observedConfig() { return observedConfig; }
    String classLoadProcess() { return classLoadLog.process(); }

    private void verifyImageConnectorsBeforeStart(String containerId) {
        imageConnectorContainerId = null;
        imageConnectorArtifacts = verifyImageConnectors();
        imageConnectorContainerId = containerId;
    }

    List<ImageConnectorArtifact> imageConnectorEvidence(String containerId) {
        if (runtimeTarget.connectorBundle().imageConnectors().isEmpty()) return List.of();
        if (!Objects.equals(imageConnectorContainerId, containerId)) {
            throw new ConnectorBundleProvisioningException("Image connector has no pre-start observation for " + containerId);
        }
        // Entrypoints may replace JARs. Re-read and rescan before accepting readiness evidence.
        return verifyImageConnectors();
    }

    private List<ImageConnectorArtifact> verifyImageConnectors() {
        List<ImageConnectorArtifact> expected = runtimeTarget.connectorBundle().imageConnectors();
        if (expected.isEmpty()) return List.of();
        try {
            List<ImageConnectorArtifact> observed = new ArrayList<>();
            for (ImageConnectorArtifact connector : expected) {
                observed.add(copyFileFromContainer(connector.containerPath(),
                        input -> ImageConnectorVerification.verifyPrimary(connector, input)));
            }
            try (var archive = getDockerClient().copyArchiveFromContainerCmd(getContainerId(), "/opt/flink/lib").exec()) {
                ImageConnectorVerification.verifyLibraryArchive(archive, expected);
            }
            return List.copyOf(observed);
        } catch (java.io.IOException | RuntimeException failure) {
            if (failure instanceof ConnectorBundleProvisioningException problem) throw problem;
            throw new ConnectorBundleProvisioningException("Cannot verify image-supplied connector inventory", failure);
        }
    }

    /** Every create/retry must establish its own stopped-container observation. */
    void verifyRuntimeJarBeforeStart(String containerId, Function<String, String> reader) {
        runtimeJarContainerId = null;
        if (runtimeTarget.expectedRuntimeJar().isPresent()) {
            readRuntimeJar(reader);
            runtimeJarContainerId = Objects.requireNonNull(containerId, "containerId");
        }
    }

    Optional<FlinkComponentProvisioningEvidence.RuntimeJarEvidence> runtimeJarEvidence(
            String containerId) {
        return runtimeJarEvidence(containerId, this::readJarSha256);
    }

    /** Re-read after the entrypoint ran; a replacement cannot inherit an earlier observation. */
    Optional<FlinkComponentProvisioningEvidence.RuntimeJarEvidence> runtimeJarEvidence(
            String containerId, Function<String, String> reader) {
        if (runtimeTarget.expectedRuntimeJar().isEmpty()) {
            return Optional.empty();
        }
        if (runtimeJarContainerId == null || !runtimeJarContainerId.equals(containerId)) {
            throw new IllegalStateException(
                    "Flink runtime JAR has no pre-start verification for container " + containerId);
        }
        return Optional.of(new FlinkComponentProvisioningEvidence.RuntimeJarEvidence(
                readRuntimeJar(reader), classLoadLog.process()));
    }

    private String readJarSha256(String path) {
        return copyFileFromContainer(path, Digests::sha256);
    }

    private FlinkRuntimeTarget.RuntimeJar readRuntimeJar(Function<String, String> reader) {
        FlinkRuntimeTarget.RuntimeJar expected = runtimeTarget.expectedRuntimeJar().orElseThrow();
        String actual;
        try {
            actual = reader.apply(expected.containerPath());
        } catch (RuntimeException failure) {
            throw new IllegalStateException(
                    "Could not read Flink runtime JAR " + expected.containerPath()
                            + ": expected " + expected.sha256() + ", actual unavailable", failure);
        }
        if (!expected.sha256().equals(actual)) {
            throw new IllegalStateException(
                    "Flink runtime JAR checksum mismatch at " + expected.containerPath()
                            + ": expected " + expected.sha256() + ", actual " + actual);
        }
        return new FlinkRuntimeTarget.RuntimeJar(expected.containerPath(), actual);
    }

    /** The stopped-container inspection boundary; tests supply an inspector without Docker. */
    void verifyImageIdentity(String containerId, Function<String, ImageIdentityVerifier.Observation> inspector) {
        verifiedImageIdentity = null;
        final ImageIdentityVerifier.Observation actual;
        try {
            actual = inspector.apply(containerId);
        } catch (RuntimeException failure) {
            throw new IllegalStateException(
                    "Could not inspect Docker image ID for container " + containerId
                            + " using " + runtimeTarget.imageReference()
                            + ": expected " + runtimeTarget.expectedImageId().orElse("first observed image")
                            + ", actual unavailable", failure);
        }
        ImageIdentityEvidence receipt = ImageIdentityVerifier.verify(
                runtimeTarget.expectedImageId(), runtimeTarget.expectedImageDigest(), actual);
        if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Image verification was interrupted");
        imageIdVerifier.accept(receipt.containerImageId());
        verifiedImageIdentity = new VerifiedImageIdentity(Objects.requireNonNull(containerId), receipt);
    }

    String verifiedImageId() {
        var observed = verifiedImageIdentity;
        if (observed == null) {
            throw new IllegalStateException("Flink container has no verified Docker image ID");
        }
        return observed.evidence().containerImageId();
    }

    ImageIdentityEvidence imageIdentityEvidence(String containerId) {
        var observed = verifiedImageIdentity;
        if (observed == null || !observed.containerId().equals(containerId))
            throw new IllegalStateException("Flink container has no image identity receipt for " + containerId);
        return observed.evidence();
    }

    private record VerifiedImageIdentity(String containerId, ImageIdentityEvidence evidence) {}

    List<String> configuredBundleTargets() {
        return List.copyOf(configuredBundleTargets);
    }

    ConnectorBundleVerification connectorBundleVerification() {
        return verification;
    }

    WaitStrategy configuredWaitStrategy() {
        return getWaitStrategy();
    }

    private void verifyCopiedBundle(FlinkConnectorBundleInstallation installation) {
        ConnectorClasspathManifest manifest = installation.classpathManifest();
        byte[] expectedManifest = manifest.canonicalBytes();
        byte[] observedManifest;
        try {
            observedManifest = copyFileFromContainer(
                    ConnectorClasspathManifest.CONTAINER_MANIFEST_PATH,
                    input -> input.readAllBytes());
        } catch (RuntimeException exception) {
            throw new ConnectorBundleProvisioningException(
                    "Could not read connector classpath manifest from created container",
                    exception);
        }
        if (!Arrays.equals(expectedManifest, observedManifest)) {
            throw new ConnectorBundleProvisioningException(
                    "Connector classpath manifest bytes changed during container copy");
        }

        List<ProvisionedConnectorArtifact> observedArtifacts = new ArrayList<>();
        for (ConnectorClasspathManifest.Entry entry : manifest.entries()) {
            String observed;
            try {
                observed = copyFileFromContainer(
                        entry.containerPath(), Digests::sha256);
            } catch (RuntimeException exception) {
                throw new ConnectorBundleProvisioningException(
                        "Could not read copied connector JAR " + entry.containerPath(),
                        exception);
            }
            if (!entry.sha256().equals(observed)) {
                throw new ConnectorBundleProvisioningException(
                        "Copied connector JAR checksum mismatch at " + entry.containerPath()
                                + ": expected " + entry.sha256() + " but found " + observed);
            }
            observedArtifacts.add(new ProvisionedConnectorArtifact(
                    entry.index(), entry.containerPath(), observed));
        }
        verification = new ConnectorBundleVerification(
                manifest.manifestSha256(), observedArtifacts);
    }

    record ConnectorBundleVerification(
            String classpathManifestSha256,
            List<ProvisionedConnectorArtifact> connectorArtifacts) {
        ConnectorBundleVerification {
            Objects.requireNonNull(classpathManifestSha256, "classpathManifestSha256");
            connectorArtifacts = List.copyOf(Objects.requireNonNull(
                    connectorArtifacts, "connectorArtifacts"));
        }
    }
}
