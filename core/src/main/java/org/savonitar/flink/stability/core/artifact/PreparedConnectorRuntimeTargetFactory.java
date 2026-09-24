package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.runtime.api.ConnectorClasspathManifest;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Trusted adapter from one verified core bundle to its exact Testcontainers runtime target. */
public final class PreparedConnectorRuntimeTargetFactory {

    /**
     * Revalidates canonical identities and staged bytes before constructing a runtime target.
     * This is the canonical v1 boundary; callers do not pair image, binding, and bytes manually.
     */
    public FlinkRuntimeTarget create(PreparedConnectorBundle bundle) {
        Objects.requireNonNull(bundle, "bundle");
        verifyClosureLocks(bundle.closureLocks());
        verifyDeterministicMerge(bundle);

        String manifestJson = ConnectorClusterBundleBuilder.canonicalClasspathManifest(
                bundle.entries());
        String manifestSha256 = Digests.sha256(manifestJson);
        if (!manifestJson.equals(bundle.classpathManifestJson())
                || !manifestSha256.equals(bundle.classpathManifestSha256())) {
            throw new IllegalStateException(
                    "Prepared connector classpath manifest does not match its entries/hash: "
                            + "recorded " + bundle.classpathManifestSha256()
                            + ", recomputed " + manifestSha256);
        }

        String targetBindingJson = ConnectorClusterBundleBuilder.canonicalTargetBinding(
                bundle.targetFlinkImageReference(),
                bundle.closureLocks(),
                manifestSha256);
        String targetBindingSha256 = Digests.sha256(targetBindingJson);
        if (!targetBindingJson.equals(bundle.canonicalTargetBindingJson())
                || !targetBindingSha256.equals(bundle.targetBindingSha256())) {
            throw new IllegalStateException(
                    "Prepared connector target binding does not match its locks/manifest: "
                            + "recorded " + bundle.targetBindingSha256()
                            + ", recomputed " + targetBindingSha256);
        }

        List<ConnectorClasspathManifest.Entry> runtimeEntries = new ArrayList<>(
                bundle.entries().size());
        for (PreparedConnectorBundleEntry entry : bundle.entries()) {
            verifyStagedBytes(entry);
            ConnectorClasspathManifest.Entry runtimeEntry =
                    new ConnectorClasspathManifest.Entry(
                            entry.globalClasspathIndex(),
                            entry.stagedPath(),
                            entry.sha256());
            if (!runtimeEntry.containerFilename().equals(entry.fileName())
                    || !runtimeEntry.containerPath().equals(entry.containerPath())) {
                throw new IllegalStateException(
                        "Core and Testcontainers connector paths disagree at classpath index "
                                + entry.globalClasspathIndex() + ": core="
                                + entry.containerPath() + ", runtime="
                                + runtimeEntry.containerPath());
            }
            runtimeEntries.add(runtimeEntry);
        }

        ConnectorClasspathManifest runtimeManifest =
                new ConnectorClasspathManifest(runtimeEntries);
        if (!Arrays.equals(runtimeManifest.canonicalBytes(), bundle.classpathManifestBytes())
                || !runtimeManifest.manifestSha256().equals(manifestSha256)) {
            throw new IllegalStateException(
                    "Core and Testcontainers connector classpath manifests disagree: core="
                            + manifestSha256 + ", runtime="
                            + runtimeManifest.manifestSha256());
        }
        List<FlinkConnectorBundleInstallation.ClosureLockHash> runtimeClosureLocks =
                bundle.closureLocks().stream()
                        .map(lock -> new FlinkConnectorBundleInstallation.ClosureLockHash(
                                lock.alias(), lock.closureSha256()))
                        .toList();
        FlinkConnectorBundleInstallation installation =
                new FlinkConnectorBundleInstallation(
                        bundle.targetFlinkImageReference(),
                        runtimeClosureLocks,
                        runtimeManifest);
        if (!Arrays.equals(
                        installation.canonicalTargetBindingBytes(),
                        bundle.canonicalTargetBindingBytes())
                || !installation.targetBindingSha256().equals(targetBindingSha256)) {
            throw new IllegalStateException(
                    "Core and Testcontainers connector target bindings disagree: core="
                            + targetBindingSha256 + ", runtime="
                            + installation.targetBindingSha256());
        }
        return FlinkRuntimeTarget.withConnectorBundle(
                bundle.targetFlinkImageReference(), installation);
    }

    private static void verifyClosureLocks(List<ConnectorClosureLock> locks) {
        for (ConnectorClosureLock lock : locks) {
            String projection = ConnectorClosureLockFactory.canonicalProjection(lock);
            String sha256 = Digests.sha256(projection);
            if (!projection.equals(lock.canonicalProjectionJson())
                    || !sha256.equals(lock.closureSha256())) {
                throw new IllegalStateException(
                        "Prepared connector lock '" + lock.alias()
                                + "' does not match its canonical projection: recorded "
                                + lock.closureSha256() + ", recomputed " + sha256);
            }
        }
    }

    private static void verifyDeterministicMerge(PreparedConnectorBundle bundle) {
        Map<String, List<ConnectorBundleContribution>> byDigest = new LinkedHashMap<>();
        Map<String, String> digestByMavenConflictKey = new HashMap<>();
        for (ConnectorClosureLock lock : bundle.closureLocks()) {
            for (ConnectorClosureLockEntry entry : lock.classpath()) {
                entry.mavenIdentity().ifPresent(identity -> {
                    String first = digestByMavenConflictKey.putIfAbsent(
                            identity.conflictKey(), entry.sha256());
                    if (first != null && !first.equals(entry.sha256())) {
                        throw new IllegalStateException(
                                "Prepared connector locks contain distinct bytes for Maven "
                                        + "conflict key " + identity.conflictKey());
                    }
                });
                byDigest.computeIfAbsent(entry.sha256(), ignored -> new ArrayList<>())
                        .add(new ConnectorBundleContribution(
                                lock.alias(), entry.classpathIndex(), entry));
            }
        }
        if (byDigest.size() != bundle.entries().size()) {
            throw new IllegalStateException(
                    "Prepared connector bundle does not contain the deterministic lock union");
        }
        int index = 0;
        for (Map.Entry<String, List<ConnectorBundleContribution>> expected
                : byDigest.entrySet()) {
            PreparedConnectorBundleEntry actual = bundle.entries().get(index);
            ConnectorClosureLockEntry first = expected.getValue().getFirst().closureEntry();
            if (actual.globalClasspathIndex() != index
                    || !actual.sha256().equals(expected.getKey())
                    || !actual.stagedPath().equals(first.stagedPath())
                    || actual.contributions().size() != expected.getValue().size()) {
                throw new IllegalStateException(
                        "Prepared connector bundle diverges from its lock union at global index "
                                + index);
            }
            for (int contributionIndex = 0;
                    contributionIndex < expected.getValue().size();
                    contributionIndex++) {
                ConnectorBundleContribution expectedContribution =
                        expected.getValue().get(contributionIndex);
                ConnectorBundleContribution actualContribution =
                        actual.contributions().get(contributionIndex);
                if (!actualContribution.alias().equals(expectedContribution.alias())
                        || actualContribution.closureClasspathIndex()
                                != expectedContribution.closureClasspathIndex()
                        || actualContribution.closureEntry()
                                != expectedContribution.closureEntry()) {
                    throw new IllegalStateException(
                            "Prepared connector bundle contributor evidence diverges from its "
                                    + "lock union at global index " + index);
                }
            }
            index++;
        }
    }

    private static void verifyStagedBytes(PreparedConnectorBundleEntry entry) {
        Path path = entry.stagedPath();
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || !Files.isReadable(path)) {
            throw new IllegalStateException(
                    "Prepared connector JAR is unavailable at classpath index "
                            + entry.globalClasspathIndex() + ": " + path
                            + "; expected SHA-256 " + entry.sha256());
        }
        String actual;
        try {
            actual = Digests.sha256(path);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Could not revalidate prepared connector JAR at classpath index "
                            + entry.globalClasspathIndex() + ": " + path
                            + "; expected SHA-256 " + entry.sha256(),
                    exception);
        }
        if (!entry.sha256().equals(actual)) {
            throw new IllegalStateException(
                    "Prepared connector JAR changed at classpath index "
                            + entry.globalClasspathIndex() + ": " + path
                            + "; expected SHA-256 " + entry.sha256()
                            + ", actual " + actual);
        }
    }
}
