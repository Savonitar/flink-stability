package org.savonitar.flink.stability.core.artifact;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkloadProtocolArtifactValidatorTest {
    @TempDir
    Path temporaryDirectory;
    private int jarSequence;

    @Test
    void acceptsExactlyOneContinuationAwareCaseInsensitiveV1Marker() throws IOException {
        Path jar = rawManifestJar(List.of(
                "Manifest-Version: 1.0",
                "Main-Class: example.Main",
                "flink-stability-workload-protocol: v",
                " 1"));

        assertDoesNotThrow(() -> WorkloadProtocolArtifactValidator.validate(jar));
    }

    @Test
    void classifiesMissingBlankUnsupportedDuplicateAndMalformedMarkers() throws IOException {
        assertKind(
                WorkloadProtocolArtifactValidator.FailureKind.MISSING,
                List.of("Manifest-Version: 1.0", "Main-Class: example.Main"));
        assertKind(
                WorkloadProtocolArtifactValidator.FailureKind.MISSING,
                List.of(
                        "Manifest-Version: 1.0",
                        "Main-Class: example.Main",
                        "Flink-Stability-Workload-Protocol: "));
        assertKind(
                WorkloadProtocolArtifactValidator.FailureKind.UNSUPPORTED,
                List.of(
                        "Manifest-Version: 1.0",
                        "Main-Class: example.Main",
                        "Flink-Stability-Workload-Protocol: v2"));
        assertKind(
                WorkloadProtocolArtifactValidator.FailureKind.DUPLICATE,
                List.of(
                        "Manifest-Version: 1.0",
                        "Main-Class: example.Main",
                        "Flink-Stability-Workload-Protocol: v1",
                        "flink-stability-workload-protocol: v1"));
        assertKind(
                WorkloadProtocolArtifactValidator.FailureKind.UNREADABLE,
                List.of(
                        "Manifest-Version: 1.0",
                        "Main-Class: example.Main",
                        "malformed-main-attribute"));
    }

    private void assertKind(
            WorkloadProtocolArtifactValidator.FailureKind expected,
            List<String> manifestLines) throws IOException {
        WorkloadProtocolArtifactValidator.ValidationException failure = assertThrows(
                WorkloadProtocolArtifactValidator.ValidationException.class,
                () -> WorkloadProtocolArtifactValidator.validate(rawManifestJar(manifestLines)));
        assertEquals(expected, failure.kind());
    }

    private Path rawManifestJar(List<String> lines) throws IOException {
        Path jar = temporaryDirectory.resolve("workload-" + jarSequence++ + ".jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry(JarFileNames.MANIFEST));
            output.write((String.join("\r\n", lines) + "\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
            output.putNextEntry(new JarEntry("example/Main.class"));
            output.write(new byte[] {0, 1, 2, 3});
            output.closeEntry();
        }
        return jar;
    }

    private static final class JarFileNames {
        private static final String MANIFEST = "META-INF/MANIFEST.MF";

        private JarFileNames() {
        }
    }
}
