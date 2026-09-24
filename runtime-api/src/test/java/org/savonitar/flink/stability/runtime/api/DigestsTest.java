package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DigestsTest {
    /** FIPS 180-2, appendix B.1. */
    private static final String ABC_SHA256 =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    @TempDir
    Path directory;

    @Test
    void everyInputFormDigestsTheSameBytes() throws IOException {
        byte[] abc = "abc".getBytes(StandardCharsets.UTF_8);
        Path file = Files.write(directory.resolve("abc.txt"), abc);

        assertEquals(ABC_SHA256, Digests.sha256(abc));
        assertEquals(ABC_SHA256, Digests.sha256("abc"));
        assertEquals(ABC_SHA256, Digests.sha256(new ByteArrayInputStream(abc)));
        assertEquals(ABC_SHA256, Digests.sha256(file));
    }

    @Test
    void refusesASymbolicLinkWhenAskedNotToFollowIt() throws IOException {
        Path target = Files.writeString(directory.resolve("target.txt"), "abc");
        Path link = Files.createSymbolicLink(directory.resolve("link.txt"), target);

        assertEquals(ABC_SHA256, Digests.sha256(link));
        assertThrows(IOException.class,
                () -> Digests.sha256(link, LinkOption.NOFOLLOW_LINKS));
    }
}
