package org.savonitar.flink.stability.flinkjob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class StateRecoveryBuildTest {
    @TempDir Path temporary;

    @Test
    void ownPackagedIdentityIsAvailableInEitherBuildAndIgnoresRuntimeProperties() {
        StateRecoveryBuild.Mode expected = StateRecoveryBuild.mode();
        String previous = System.getProperty("workload.build.mode");
        try {
            System.setProperty("workload.build.mode", expected == StateRecoveryBuild.Mode.CORRECT
                    ? "reset-state-on-restore-v1" : "correct");
            assertSame(expected, StateRecoveryBuild.mode());
        } finally {
            if (previous == null) System.clearProperty("workload.build.mode");
            else System.setProperty("workload.build.mode", previous);
        }
    }

    @Test
    void rejectsMissingDuplicateOversizedAndMalformedIdentities() throws Exception {
        for (StateRecoveryBuild.Mode mode : StateRecoveryBuild.Mode.values())
            assertSame(mode, StateRecoveryBuild.parse(bytes("mode=" + mode.wireValue + "\n")));
        for (String invalid : new String[]{"", "mode=true\n", "mode=correct", "mode=correct\nmode=correct\n",
                "mode=correct\r\n", "mode=correct\n#comment", "x".repeat(129)})
            assertThrows(IOException.class, () -> StateRecoveryBuild.parse(bytes(invalid)), invalid);
        Path missing = jar("missing.jar", "other", "mode=correct\n");
        assertThrows(IOException.class, () -> StateRecoveryBuild.readJar(missing));
        Path valid = jar("valid.jar", StateRecoveryBuild.RESOURCE, "mode=correct\n");
        assertSame(StateRecoveryBuild.Mode.CORRECT, StateRecoveryBuild.readJar(valid));

        // ZIP permits duplicate entries although JarOutputStream does not. Rename an equal-length
        // second entry in both its local and central headers, leaving content and CRCs untouched.
        Path duplicate = temporary.resolve("duplicate.jar");
        String alternate = StateRecoveryBuild.RESOURCE.substring(0, StateRecoveryBuild.RESOURCE.length() - 1) + "x";
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(duplicate))) {
            entry(jar, StateRecoveryBuild.RESOURCE, "mode=correct\n");
            entry(jar, alternate, "mode=correct\n");
        }
        byte[] data = Files.readAllBytes(duplicate);
        byte[] from = alternate.getBytes(StandardCharsets.US_ASCII);
        byte[] to = StateRecoveryBuild.RESOURCE.getBytes(StandardCharsets.US_ASCII);
        int replaced = 0;
        for (int offset = 0; offset <= data.length - from.length; offset++) {
            boolean match = true;
            for (int index = 0; index < from.length; index++)
                if (data[offset + index] != from[index]) { match = false; break; }
            if (match) { System.arraycopy(to, 0, data, offset, to.length); replaced++; }
        }
        assertEquals(2, replaced);
        Files.write(duplicate, data);
        assertThrows(IOException.class, () -> StateRecoveryBuild.readJar(duplicate));
    }

    private Path jar(String name, String resource, String value) throws IOException {
        Path result = temporary.resolve(name);
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(result))) {
            entry(jar, resource, value);
        }
        return result;
    }

    private static void entry(JarOutputStream jar, String name, String value) throws IOException {
        jar.putNextEntry(new JarEntry(name));
        jar.write(value.getBytes(StandardCharsets.US_ASCII));
        jar.closeEntry();
    }

    private static ByteArrayInputStream bytes(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.US_ASCII));
    }
}
