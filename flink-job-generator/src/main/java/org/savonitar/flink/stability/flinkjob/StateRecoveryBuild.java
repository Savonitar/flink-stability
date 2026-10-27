package org.savonitar.flink.stability.flinkjob;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** The immutable build identity in this class's own artifact, never an ambient runtime setting. */
final class StateRecoveryBuild {
    static final String RESOURCE = "META-INF/flink-stability/keyed-prefix-build.properties";

    enum Mode {
        CORRECT("correct"), RESET_STATE_ON_RESTORE("reset-state-on-restore-v1");
        final String wireValue;
        Mode(String wireValue) { this.wireValue = wireValue; }
    }

    private StateRecoveryBuild() {}

    static Mode mode() {
        try {
            CodeSource source = StateRecoveryBuild.class.getProtectionDomain().getCodeSource();
            if (source == null || !"file".equals(source.getLocation().getProtocol()))
                throw new IOException("Workload build identity requires a local code source");
            Path location = Path.of(source.getLocation().toURI());
            if (Files.isDirectory(location, LinkOption.NOFOLLOW_LINKS)) {
                // Maven tests execute the same resource from their own classes directory.
                Path resource = location.resolve(RESOURCE);
                if (!Files.isRegularFile(resource, LinkOption.NOFOLLOW_LINKS))
                    throw new IOException("Workload build identity is missing");
                try (InputStream input = Files.newInputStream(resource, LinkOption.NOFOLLOW_LINKS)) {
                    return parse(input);
                }
            }
            return readJar(location);
        } catch (Exception failure) {
            throw new IllegalStateException("Cannot establish workload build identity", failure);
        }
    }

    static Mode readJar(Path location) throws IOException {
        if (!Files.isRegularFile(location, LinkOption.NOFOLLOW_LINKS))
            throw new IOException("Workload code source is not a regular JAR");
        try (JarFile jar = new JarFile(location.toFile(), false)) {
            JarEntry identity = null;
            for (Enumeration<JarEntry> entries = jar.entries(); entries.hasMoreElements();) {
                JarEntry entry = entries.nextElement();
                if (RESOURCE.equals(entry.getName())) {
                    if (identity != null) throw new IOException("Duplicate workload build identity");
                    identity = entry;
                }
            }
            if (identity == null || identity.isDirectory())
                throw new IOException("Workload build identity is missing");
            try (InputStream input = jar.getInputStream(identity)) {
                return parse(input);
            }
        }
    }

    static Mode parse(InputStream input) throws IOException {
        byte[] bytes = input.readNBytes(129);
        if (bytes.length > 128) throw new IOException("Workload build identity is oversized");
        String value = new String(bytes, StandardCharsets.US_ASCII);
        for (Mode mode : Mode.values()) {
            if (value.equals("mode=" + mode.wireValue + "\n")) return mode;
        }
        throw new IOException("Unknown or malformed workload build identity");
    }
}
