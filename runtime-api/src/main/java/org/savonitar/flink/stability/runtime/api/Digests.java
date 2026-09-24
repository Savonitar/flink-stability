package org.savonitar.flink.stability.runtime.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** SHA-256 digests in lowercase hex, the form of every checksum the harness records. */
public final class Digests {
    private Digests() {}

    public static String sha256(byte[] bytes) {
        return HexFormat.of().formatHex(newSha256().digest(bytes));
    }

    /** The digest of the text's UTF-8 bytes. */
    public static String sha256(String text) {
        return sha256(text.getBytes(StandardCharsets.UTF_8));
    }

    /** Reads the stream to its end; the caller closes it. */
    public static String sha256(InputStream input) throws IOException {
        MessageDigest digest = newSha256();
        byte[] buffer = new byte[8192];
        for (int read; (read = input.read(buffer)) >= 0;) {
            digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** The digest of a file; pass {@code NOFOLLOW_LINKS} to refuse a symbolic link. */
    public static String sha256(Path file, LinkOption... options) throws IOException {
        try (InputStream input = Files.newInputStream(file, options)) {
            return sha256(input);
        }
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java must provide SHA-256", impossible);
        }
    }
}
