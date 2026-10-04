package org.savonitar.flink.stability.testcontainers;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.ImageConnectorArtifact;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import static org.junit.jupiter.api.Assertions.*;

class ImageConnectorVerificationTest {
    @Test void verifiesPrimaryBytesAndRejectsMissingClassesOrWrongHash() throws Exception {
        byte[] jar = jar(false, true);
        var expected = subject(jar);
        assertEquals(expected, ImageConnectorVerification.verifyPrimary(expected, new ByteArrayInputStream(jar)));
        assertThrows(ConnectorBundleProvisioningException.class, () -> ImageConnectorVerification.verifyPrimary(
                new ImageConnectorArtifact("kafka", expected.containerPath(), "a".repeat(64)), new ByteArrayInputStream(jar)));
        byte[] incomplete = jar(false, false);
        assertThrows(ConnectorBundleProvisioningException.class, () -> ImageConnectorVerification.verifyPrimary(
                subject(incomplete), new ByteArrayInputStream(incomplete)));
        byte[] versioned = jar(true, true);
        assertThrows(ConnectorBundleProvisioningException.class, () -> ImageConnectorVerification.verifyPrimary(
                subject(versioned), new ByteArrayInputStream(versioned)));
    }

    @Test void inventoriesEveryLibraryJarAndRejectsSecondCopyMissingPrimaryOrLinks() throws Exception {
        byte[] jar = jar(false, true);
        var expected = subject(jar);
        assertDoesNotThrow(() -> ImageConnectorVerification.verifyLibraryArchive(
                archive(Map.of("lib/subject.jar", jar), false), List.of(expected)));
        assertThrows(ConnectorBundleProvisioningException.class, () -> ImageConnectorVerification.verifyLibraryArchive(
                archive(Map.of("lib/subject.jar", jar, "lib/second.jar", jar), false), List.of(expected)));
        assertThrows(ConnectorBundleProvisioningException.class, () -> ImageConnectorVerification.verifyLibraryArchive(
                archive(Map.of(), false), List.of(expected)));
        assertThrows(ConnectorBundleProvisioningException.class, () -> ImageConnectorVerification.verifyLibraryArchive(
                archive(Map.of("lib/subject.jar", jar), true), List.of(expected)));
    }

    @Test void internalOnlyAndLegacyConnectorCopiesAreConflicts() throws Exception {
        byte[] primary = jar(false, true);
        for (String name : List.of("org/apache/flink/connector/kafka/sink/internal/KafkaCommitter.class",
                "org/apache/flink/streaming/connectors/kafka/FlinkKafkaProducer.class")) {
            var bytes = new ByteArrayOutputStream();
            try (var jar = new JarOutputStream(bytes)) {
                jar.putNextEntry(new JarEntry(name)); jar.write(new byte[] {1, 2, 3}); jar.closeEntry();
            }
            assertThrows(ConnectorBundleProvisioningException.class, () -> ImageConnectorVerification.verifyLibraryArchive(
                    archive(Map.of("lib/subject.jar", primary, "lib/internal-copy.jar", bytes.toByteArray()), false),
                    List.of(subject(primary))), name);
        }
    }

    @Test void unrelatedMultiReleaseJarDoesNotConflictWithTheSubject() throws Exception {
        byte[] primary = jar(false, true);
        var bytes = new ByteArrayOutputStream();
        var manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Multi-Release", "true");
        try (var jar = new JarOutputStream(bytes, manifest)) {
            jar.putNextEntry(new JarEntry("META-INF/versions/21/example/Helper.class"));
            jar.write(new byte[] {1, 2, 3});
            jar.closeEntry();
        }
        assertDoesNotThrow(() -> ImageConnectorVerification.verifyLibraryArchive(
                archive(Map.of("lib/subject.jar", primary, "lib/unrelated.jar", bytes.toByteArray()), false),
                List.of(subject(primary))));
    }

    private static ImageConnectorArtifact subject(byte[] jar) {
        return new ImageConnectorArtifact("kafka", "/opt/flink/lib/subject.jar", Digests.sha256(jar));
    }

    private static byte[] jar(boolean versioned, boolean complete) throws Exception {
        var bytes = new ByteArrayOutputStream();
        var manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        if (versioned) manifest.getMainAttributes().putValue("Multi-Release", "true");
        try (var jar = new JarOutputStream(bytes, manifest)) {
            for (String name : ImageConnectorArtifact.ENTRY_CLASSES.subList(0, complete ? 2 : 1)) {
                jar.putNextEntry(new JarEntry((versioned ? "META-INF/versions/21/" : "")
                        + name.replace('.', '/') + ".class"));
                jar.write(new byte[] {1, 2, 3});
                jar.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static ByteArrayInputStream archive(Map<String, byte[]> files, boolean link) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var tar = new TarArchiveOutputStream(bytes)) {
            for (var file : files.entrySet()) {
                var entry = link ? new TarArchiveEntry(file.getKey(), TarConstants.LF_SYMLINK)
                        : new TarArchiveEntry(file.getKey());
                if (link) entry.setLinkName("/tmp/subject.jar");
                else entry.setSize(file.getValue().length);
                tar.putArchiveEntry(entry);
                if (!link) tar.write(file.getValue());
                tar.closeArchiveEntry();
            }
        }
        return new ByteArrayInputStream(bytes.toByteArray());
    }
}
