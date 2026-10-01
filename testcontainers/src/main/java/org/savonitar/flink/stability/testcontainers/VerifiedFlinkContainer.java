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
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.savonitar.flink.stability.runtime.api.Checks.requireDockerImageId;

/** Verifies image identity and connector bytes while the created container is still stopped. */
final class VerifiedFlinkContainer extends GenericContainer<VerifiedFlinkContainer> {
    private static final int READ_ONLY_FILE_MODE = 0444;

    private final FlinkRuntimeTarget runtimeTarget;
    private final Consumer<String> imageIdVerifier;
    private final FlinkClassLoadLog classLoadLog;
    private final String resourceId;
    private final List<String> configuredBundleTargets = new ArrayList<>();
    private ConnectorBundleVerification verification;
    private String verifiedImageId;
    private String runtimeJarContainerId;
    private SyntheticTokenPlugin tokenPlugin;
    private String tokenPluginContainerId;
    private FlinkSessionLog sessionLog;

    VerifiedFlinkContainer(
            DockerImageName image,
            FlinkRuntimeTarget runtimeTarget,
            Consumer<String> imageIdVerifier,
            FlinkClassLoadLog classLoadLog) {
        super(Objects.requireNonNull(image, "image"));
        this.runtimeTarget = Objects.requireNonNull(runtimeTarget, "runtimeTarget");
        this.imageIdVerifier = Objects.requireNonNull(imageIdVerifier, "imageIdVerifier");
        this.classLoadLog = Objects.requireNonNull(classLoadLog, "classLoadLog");
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

    /** Unique for this handle, including replacements and separate run attempts. */
    String resourceId() {
        return resourceId;
    }

    String advertisedAlias() {
        return classLoadLog.process().replace('#', '-');
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
        // Testcontainers 1.21.4 invokes this hook after its configured archive copies and before
        // Docker's startContainer command. A mismatch therefore prevents the Flink entrypoint.
        verifyImageIdentity(containerId, id -> getDockerClient()
                .inspectContainerCmd(id).exec().getImageId());
        verifyRuntimeJarBeforeStart(containerId, this::readJarSha256);
        verifyCopiedBundle(runtimeTarget.connectorBundle());
        tokenPluginContainerId = null;
        if (tokenPlugin != null) {
            tokenPlugin.verify(this::readJarSha256);
            tokenPluginContainerId = containerId;
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
    void verifyImageIdentity(String containerId, Function<String, String> inspector) {
        verifiedImageId = null;
        final String actual;
        try {
            actual = inspector.apply(containerId);
        } catch (RuntimeException failure) {
            throw new IllegalStateException(
                    "Could not inspect Docker image ID for container " + containerId
                            + " using " + runtimeTarget.imageReference()
                            + ": expected " + runtimeTarget.expectedImageId().orElse("first observed image")
                            + ", actual unavailable", failure);
        }
        try {
            requireDockerImageId(actual, "observed imageId");
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IllegalStateException(
                    "Invalid Docker image ID for container " + containerId
                            + ": expected " + runtimeTarget.expectedImageId()
                                    .orElse("sha256:<64 lowercase hexadecimal characters>")
                            + ", actual " + actual, failure);
        }
        runtimeTarget.expectedImageId().ifPresent(expected -> {
            if (!expected.equals(actual)) {
                throw new IllegalStateException(
                        "Docker image ID mismatch for container " + containerId
                                + ": expected " + expected + ", actual " + actual);
            }
        });
        imageIdVerifier.accept(actual);
        verifiedImageId = actual;
    }

    String verifiedImageId() {
        if (verifiedImageId == null) {
            throw new IllegalStateException("Flink container has no verified Docker image ID");
        }
        return verifiedImageId;
    }

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
