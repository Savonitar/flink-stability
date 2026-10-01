package org.savonitar.flink.stability.core.artifact;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Checksums for small, isolated file-repository fixtures; no remote requests. */
final class MavenRepositoryFixture {
    private MavenRepositoryFixture() {}

    static void checksum(Path file) throws IOException {
        try {
            Files.writeString(file.resolveSibling(file.getFileName() + ".sha1"),
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-1").digest(Files.readAllBytes(file))));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
