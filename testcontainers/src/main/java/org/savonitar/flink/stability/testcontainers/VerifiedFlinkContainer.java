package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.ConnectorClasspathManifest;
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

/** Copies and verifies connector bytes while the Docker container is created but still stopped. */
final class VerifiedFlinkContainer extends GenericContainer<VerifiedFlinkContainer> {
    private static final int READ_ONLY_FILE_MODE = 0444;

    private final FlinkRuntimeTarget runtimeTarget;
    private final List<String> configuredBundleTargets = new ArrayList<>();
    private ConnectorBundleVerification verification;

    VerifiedFlinkContainer(DockerImageName image, FlinkRuntimeTarget runtimeTarget) {
        super(Objects.requireNonNull(image, "image"));
        this.runtimeTarget = Objects.requireNonNull(runtimeTarget, "runtimeTarget");
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

    @Override
    protected void containerIsCreated(String containerId) {
        super.containerIsCreated(containerId);
        // Testcontainers 1.21 invokes this hook after its configured archive copies and before
        // Docker's startContainer command. A mismatch therefore prevents the Flink entrypoint.
        verifyCopiedBundle(runtimeTarget.connectorBundle());
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
                        entry.containerPath(), ConnectorClasspathManifest::sha256);
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
