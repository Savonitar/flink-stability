package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorClasspathManifestTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void canonicalManifestContainsOnlyOrderedByteDeploymentFields() throws Exception {
        Path first = Files.writeString(temporaryDirectory.resolve("first.jar"), "first");
        Path second = Files.writeString(temporaryDirectory.resolve("second.jar"), "second");
        String firstHash = hash(first);
        String secondHash = hash(second);

        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(0, first, firstHash),
                new ConnectorClasspathManifest.Entry(1, second, secondHash)));

        String expected = "{\"entries\":["
                + "{\"filename\":\"flink-stability-connector-00000000-" + firstHash
                + ".jar\",\"index\":0,\"sha256\":\"" + firstHash + "\"},"
                + "{\"filename\":\"flink-stability-connector-00000001-" + secondHash
                + ".jar\",\"index\":1,\"sha256\":\"" + secondHash + "\"}"
                + "],\"format\":\"flink-stability-connector-classpath-v1\"}";

        assertEquals(expected, new String(manifest.canonicalBytes(), StandardCharsets.UTF_8));
        assertEquals(ConnectorClasspathManifest.sha256(
                expected.getBytes(StandardCharsets.UTF_8)), manifest.manifestSha256());
        assertEquals(
                "/opt/flink/lib/flink-stability-connector-00000001-"
                        + secondHash + ".jar",
                manifest.entries().get(1).containerPath());
        assertFalse(expected.contains(temporaryDirectory.toString()));
        assertFalse(expected.contains("image"));
        assertFalse(expected.contains("binding"));
    }

    @Test
    void targetBindingsCanDifferWhileTheCopiedManifestRemainsIdentical() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("connector.jar"), "bytes");
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(0, jar, hash(jar))));

        FlinkRuntimeTarget from = FlinkRuntimeTarget.withConnectorBundle(
                "flink:2.2.0",
                installation("flink:2.2.0", "connector", "a", manifest));
        FlinkRuntimeTarget to = FlinkRuntimeTarget.withConnectorBundle(
                "flink:2.2.1",
                installation("flink:2.2.1", "connector", "b", manifest));

        assertNotEquals(from, to);
        assertEquals(
                from.connectorBundle().classpathManifest().manifestSha256(),
                to.connectorBundle().classpathManifest().manifestSha256());
        assertArrayEquals(
                from.connectorBundle().classpathManifest().canonicalBytes(),
                to.connectorBundle().classpathManifest().canonicalBytes());
    }

    @Test
    void mutationIsDetectedBeforeContainerConfiguration() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("connector.jar"), "before");
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(0, jar, hash(jar))));
        Files.writeString(jar, "after");

        ConnectorBundleProvisioningException failure = assertThrows(
                ConnectorBundleProvisioningException.class,
                manifest::verifyHostFiles);

        assertTrue(failure.getMessage().contains("changed before provisioning"));
    }

    @Test
    void emptyVerifiedBundleRemainsAnExplicitRuntimeRequirement() {
        ConnectorClasspathManifest empty = new ConnectorClasspathManifest(List.of());
        FlinkRuntimeTarget v1 = FlinkRuntimeTarget.withConnectorBundle(
                "flink:2.2.0",
                installation("flink:2.2.0", "connector", "a", empty));
        assertTrue(v1.connectorBundle().classpathManifest().entries().isEmpty());
    }

    @Test
    void rejectsNonContiguousIndexesAndDuplicateBytes() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("connector.jar"), "bytes");
        String hash = hash(jar);

        assertThrows(IllegalArgumentException.class, () -> new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(1, jar, hash))));
        assertThrows(IllegalArgumentException.class, () -> new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(0, jar, hash),
                new ConnectorClasspathManifest.Entry(1, jar, hash))));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ConnectorClasspathManifest.Entry(100_000_000, jar, hash));
    }

    @Test
    void returnedBytesAndCollectionsAreImmutable() throws Exception {
        Path jar = Files.writeString(temporaryDirectory.resolve("connector.jar"), "bytes");
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of(
                new ConnectorClasspathManifest.Entry(0, jar, hash(jar))));
        byte[] returned = manifest.canonicalBytes();
        byte original = returned[0];
        returned[0] = (byte) (original + 1);

        assertEquals(original, manifest.canonicalBytes()[0]);
        assertThrows(UnsupportedOperationException.class, () -> manifest.entries().clear());
    }

    @Test
    void targetBindingIsCanonicalDerivedAndAliasSorted() {
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of());
        FlinkConnectorBundleInstallation installation = new FlinkConnectorBundleInstallation(
                "flink:2.2.0",
                List.of(
                        new FlinkConnectorBundleInstallation.ClosureLockHash(
                                "zeta", "b".repeat(64)),
                        new FlinkConnectorBundleInstallation.ClosureLockHash(
                                "alpha", "a".repeat(64))),
                manifest);
        String expected = "{\"classpath_manifest_sha256\":\""
                + manifest.manifestSha256()
                + "\",\"closures\":[{\"alias\":\"alpha\",\"closure_sha256\":\""
                + "a".repeat(64)
                + "\"},{\"alias\":\"zeta\",\"closure_sha256\":\""
                + "b".repeat(64)
                + "\"}],\"format\":\"flink-stability.connector-cluster-bundle/v1\","
                + "\"target_flink_image_reference\":\"flink:2.2.0\"}";

        assertEquals(
                expected,
                new String(
                        installation.canonicalTargetBindingBytes(),
                        StandardCharsets.UTF_8));
        assertEquals(
                ConnectorClasspathManifest.sha256(
                        expected.getBytes(StandardCharsets.UTF_8)),
                installation.targetBindingSha256());
        assertEquals(List.of("alpha", "zeta"), installation.closureLocks().stream()
                .map(FlinkConnectorBundleInstallation.ClosureLockHash::alias)
                .toList());
        assertThrows(
                UnsupportedOperationException.class,
                () -> installation.closureLocks().clear());
    }

    @Test
    void targetBindingRejectsImageMismatchAndDuplicateAlias() {
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of());
        FlinkConnectorBundleInstallation installation =
                installation("flink:2.2.0", "connector", "a", manifest);

        IllegalArgumentException mismatch = assertThrows(
                IllegalArgumentException.class,
                () -> FlinkRuntimeTarget.withConnectorBundle("flink:2.2.1", installation));
        assertTrue(mismatch.getMessage().contains("exactly match"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new FlinkConnectorBundleInstallation(
                        "flink:2.2.0",
                        List.of(
                                new FlinkConnectorBundleInstallation.ClosureLockHash(
                                        "connector", "a".repeat(64)),
                                new FlinkConnectorBundleInstallation.ClosureLockHash(
                                        "connector", "b".repeat(64))),
                        manifest));
    }

    @Test
    void targetBindingInputsAndReturnedBytesAreImmutable() {
        ConnectorClasspathManifest manifest = new ConnectorClasspathManifest(List.of());
        List<FlinkConnectorBundleInstallation.ClosureLockHash> callerLocks =
                new java.util.ArrayList<>(List.of(
                        new FlinkConnectorBundleInstallation.ClosureLockHash(
                                "connector", "a".repeat(64))));
        FlinkConnectorBundleInstallation installation = new FlinkConnectorBundleInstallation(
                "flink:2.2.0", callerLocks, manifest);
        String originalHash = installation.targetBindingSha256();
        byte[] returned = installation.canonicalTargetBindingBytes();
        returned[0]++;
        callerLocks.clear();

        assertEquals(originalHash, installation.targetBindingSha256());
        assertEquals(1, installation.closureLocks().size());
        assertEquals('{', installation.canonicalTargetBindingBytes()[0]);
    }

    private static FlinkConnectorBundleInstallation installation(
            String image,
            String alias,
            String closureHashCharacter,
            ConnectorClasspathManifest manifest) {
        return new FlinkConnectorBundleInstallation(
                image,
                List.of(new FlinkConnectorBundleInstallation.ClosureLockHash(
                        alias, closureHashCharacter.repeat(64))),
                manifest);
    }

    private static String hash(Path path) throws Exception {
        return ConnectorClasspathManifest.sha256(Files.readAllBytes(path));
    }
}
