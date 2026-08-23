package org.savonitar.flink.stability.core.spec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CentralMavenArtifactLookupTest {
    @TempDir
    Path root;

    @Test
    void resolvesFromAFileRepositoryThenReusesTheIsolatedCacheOffline() throws Exception {
        Path remote = root.resolve("remote");
        Path repositoryArtifact = remote.resolve(
                "org/example/test-connector/1.2.3/test-connector-1.2.3.jar");
        createJar(repositoryArtifact);
        Path local = root.resolve("local");
        CentralMavenArtifactLookup lookup = new CentralMavenArtifactLookup(
                local, List.of(remote.toUri()));
        MavenCoordinate coordinate = MavenCoordinate.parse(
                "maven:org.example:test-connector:1.2.3");

        Path online = lookup.resolve(coordinate, false);
        Path offline = lookup.resolve(coordinate, true);

        assertEquals(local.resolve(
                        "org/example/test-connector/1.2.3/test-connector-1.2.3.jar")
                        .toAbsolutePath().normalize(),
                online);
        assertEquals(online, offline);
        assertArrayEquals(Files.readAllBytes(repositoryArtifact), Files.readAllBytes(online));
    }

    @Test
    void reportsOfflineMissWithoutReadingOrPopulatingAnAvailableRemoteArtifact()
            throws IOException {
        Path remote = root.resolve("remote");
        Path repositoryArtifact = remote.resolve(
                "org/example/remote-only/9.9.9/remote-only-9.9.9.jar");
        createJar(repositoryArtifact);
        Path local = root.resolve("empty-local");
        CentralMavenArtifactLookup lookup = new CentralMavenArtifactLookup(
                local, List.of(remote.toUri()));

        MavenArtifactLookupException exception = assertThrows(
                MavenArtifactLookupException.class,
                () -> lookup.resolve(
                        MavenCoordinate.parse("maven:org.example:remote-only:9.9.9"), true));

        assertEquals(MavenArtifactLookupException.Kind.OFFLINE_MISS, exception.kind());
        assertFalse(Files.exists(local.resolve(
                "org/example/remote-only/9.9.9/remote-only-9.9.9.jar")));
    }

    @Test
    void reusesAConventionalMavenCentralCacheEntryOffline() throws Exception {
        Path local = root.resolve("local");
        Path cached = local.resolve(
                "org/example/central-cached/1.2.3/central-cached-1.2.3.jar");
        createJar(cached);
        Files.writeString(
                cached.resolveSibling("_remote.repositories"),
                "central-cached-1.2.3.jar>central=\n");
        CentralMavenArtifactLookup lookup = new CentralMavenArtifactLookup(
                local,
                List.of(URI.create("https://repo.maven.apache.org/maven2/")));

        Path resolved = lookup.resolve(
                MavenCoordinate.parse("maven:org.example:central-cached:1.2.3"),
                true);

        assertEquals(cached.toAbsolutePath().normalize(), resolved);
    }

    @Test
    void classifiesAMissingOnlineFileRepositoryCoordinateAsNotFound() throws IOException {
        Path remote = Files.createDirectories(root.resolve("remote"));
        CentralMavenArtifactLookup lookup = new CentralMavenArtifactLookup(
                root.resolve("local"), List.of(remote.toUri()));

        MavenArtifactLookupException exception = assertThrows(
                MavenArtifactLookupException.class,
                () -> lookup.resolve(
                        MavenCoordinate.parse("maven:org.example:missing:1.2.3"), false));

        assertEquals(MavenArtifactLookupException.Kind.NOT_FOUND, exception.kind());
    }

    @Test
    void classifiesAnUnusableLocalRepositorySetupAsRepositoryUnavailable()
            throws IOException {
        Path unusableLocal = Files.writeString(root.resolve("local-is-a-file"), "blocked");
        Path remote = Files.createDirectories(root.resolve("remote"));
        CentralMavenArtifactLookup lookup = new CentralMavenArtifactLookup(
                unusableLocal, List.of(remote.toUri()));

        for (boolean offline : List.of(false, true)) {
            MavenArtifactLookupException exception = assertThrows(
                    MavenArtifactLookupException.class,
                    () -> lookup.resolve(
                            MavenCoordinate.parse("maven:org.example:missing:1.2.3"), offline));

            assertEquals(MavenArtifactLookupException.Kind.REPOSITORY_UNAVAILABLE,
                    exception.kind(), "offline=" + offline);
        }
    }

    @Test
    void rejectsMutableAndNonCanonicalCoordinatesBeforeRepositoryResolution() {
        List<String> invalid = List.of(
                "maven:org.example:connector:SNAPSHOT",
                "maven:org.example:connector:1.0-SNAPSHOT",
                "maven:org.example:connector:1.0.SNAPSHOT",
                "maven:org.example:connector:1.0-20260823.123456-1",
                "maven:org.example:connector:LATEST",
                "maven:org.example:connector:RELEASE",
                "maven:org.example:connector:jar:1.0",
                "maven:org/example:connector:1.0",
                "maven:org.example:connector:");

        for (String reference : invalid) {
            assertThrows(IllegalArgumentException.class,
                    () -> MavenCoordinate.parse(reference), reference);
        }
    }

    private static void createJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream ignored = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            // The connector role requires a valid primary JAR, not a Main-Class.
        }
    }
}
