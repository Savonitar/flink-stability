package org.savonitar.flink.stability.testcontainers;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.savonitar.flink.stability.runtime.api.ConnectorBundleProvisioningException;
import org.savonitar.flink.stability.runtime.api.ImageConnectorArtifact;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

/** Reads Docker archives without extracting paths to the host or executing container commands. */
final class ImageConnectorVerification {
    private ImageConnectorVerification() {}

    static ImageConnectorArtifact verifyPrimary(ImageConnectorArtifact expected, InputStream bytes)
            throws IOException {
        // The bytes are read only once; hashing covers the complete archive, including trailers.
        java.security.MessageDigest digest;
        try { digest = java.security.MessageDigest.getInstance("SHA-256"); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
        try (var input = new java.security.DigestInputStream(bytes, digest)) {
            Set<String> found = classesInJar(input);
            input.transferTo(java.io.OutputStream.nullOutputStream());
            String actual = java.util.HexFormat.of().formatHex(digest.digest());
            if (!actual.equals(expected.sha256())) {
                throw failure("Image connector checksum mismatch at " + expected.containerPath()
                        + ": expected " + expected.sha256() + ", observed " + actual);
            }
            if (!found.containsAll(ImageConnectorArtifact.ENTRY_CLASSES)) {
                throw failure("Image connector lacks subject entry classes at " + expected.containerPath());
            }
            return new ImageConnectorArtifact(expected.alias(), expected.containerPath(), actual);
        }
    }

    static void verifyLibraryArchive(InputStream archive, List<ImageConnectorArtifact> subjects)
            throws IOException {
        try (TarArchiveInputStream tar = new TarArchiveInputStream(archive)) {
            Set<String> seenPaths = new HashSet<>();
            Set<String> seenSubjects = new HashSet<>();
            for (TarArchiveEntry file; (file = tar.getNextTarEntry()) != null;) {
                String name = file.getName();
                // Docker's archive of /opt/flink/lib is rooted at lib/.
                if (!name.startsWith("lib/") || name.contains("/../") || name.contains("/./")) {
                    if (file.isDirectory() && name.equals("lib")) continue;
                    throw failure("Unexpected path in Flink library archive: " + name);
                }
                if (file.isDirectory()) continue;
                String path = "/opt/flink/" + name;
                if (!name.endsWith(".jar")) continue;
                if (!file.isFile() || !seenPaths.add(path)) {
                    throw failure("Flink library JAR is a link, special file or duplicate path: " + path);
                }
                Set<String> classes = classesInJar(tar);
                if (classes.isEmpty()) continue;
                List<ImageConnectorArtifact> owners = subjects.stream()
                        .filter(subject -> subject.containerPath().equals(path)).toList();
                if (owners.size() != 1) {
                    throw failure("Second copy of subject connector classes in " + path + ": " + classes);
                }
                seenSubjects.add(path);
            }
            for (ImageConnectorArtifact subject : subjects) {
                if (subject.containerPath().startsWith("/opt/flink/lib/")
                        && !seenSubjects.contains(subject.containerPath())) {
                    throw failure("Declared image connector is absent from Flink library inventory: "
                            + subject.containerPath());
                }
            }
        }
    }

    private static Set<String> classesInJar(InputStream bytes) throws IOException {
        Set<String> found = new HashSet<>();
        // Closing the nested JAR must not close the enclosing tar/primary digest stream.
        try (JarInputStream jar = new JarInputStream(new FilterInputStream(bytes) {
            @Override public void close() {}
        })) {
            boolean multiRelease = jar.getManifest() != null && Boolean.parseBoolean(
                    jar.getManifest().getMainAttributes().getValue("Multi-Release"));
            for (ZipEntry entry; (entry = jar.getNextEntry()) != null;) {
                String name = entry.getName();
                if (ImageConnectorArtifact.connectorClassEntry(name)) {
                    found.add(name.substring(0, name.length() - ".class".length()).replace('/', '.'));
                }
                if (multiRelease && name.matches("META-INF/versions/[0-9]+/.*")
                        && ImageConnectorArtifact.connectorClassEntry(name.replaceFirst("META-INF/versions/[0-9]+/", ""))) {
                    throw failure("Versioned subject connector classes are unsupported in an image connector");
                }
            }
        }
        return found;
    }

    private static ConnectorBundleProvisioningException failure(String message) {
        return new ConnectorBundleProvisioningException(message);
    }
}
