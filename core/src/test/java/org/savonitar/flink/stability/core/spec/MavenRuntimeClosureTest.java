package org.savonitar.flink.stability.core.spec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MavenRuntimeClosureTest {
    @TempDir
    Path root;

    @Test
    void selectsStrictRuntimeScopesAndExclusionsInBreadthFirstOrder() throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        Path systemJar = root.resolve("system-only.jar");
        createJar(systemJar);

        install(remote, "org.example", "compile-transitive", "1.0", "");
        install(remote, "org.example", "compile-dep", "1.0", dependencies(
                dependency("org.example", "compile-transitive", "1.0", "", "")));
        install(remote, "org.example", "runtime-dep", "1.0", "");
        install(remote, "org.example", "provided-dep", "1.0", "");
        install(remote, "org.example", "test-dep", "1.0", "");
        install(remote, "org.example", "optional-dep", "1.0", "");
        install(remote, "org.example", "excluded-dep", "1.0", "");
        install(remote, "org.example", "branch", "1.0", dependencies(
                dependency("org.example", "excluded-dep", "1.0", "", "")));
        install(remote, "org.example", "runtime-root", "1.0", dependencies(
                dependency("org.example", "compile-dep", "1.0", "", ""),
                dependency("org.example", "runtime-dep", "1.0", "runtime", ""),
                dependency("org.example", "provided-dep", "1.0", "provided", ""),
                dependency("org.example", "test-dep", "1.0", "test", ""),
                systemDependency(systemJar),
                dependency("org.example", "optional-dep", "1.0", "", "<optional>true</optional>"),
                dependency(
                        "org.example",
                        "branch",
                        "1.0",
                        "",
                        """
                                <exclusions>
                                  <exclusion>
                                    <groupId>org.example</groupId>
                                    <artifactId>excluded-dep</artifactId>
                                  </exclusion>
                                </exclusions>
                                """)));

        CentralMavenArtifactLookup lookup = lookup(remote, "local");
        MavenRuntimeClosure closure = lookup.resolveRuntimeClosure(
                List.of(root(4, "runtime-root", "1.0")), false);

        assertEquals(
                List.of(
                        "runtime-root",
                        "compile-dep",
                        "runtime-dep",
                        "branch",
                        "compile-transitive"),
                artifactIds(closure));
        assertEquals(List.of(0, 1, 1, 1, 2), closure.classpath().stream()
                .map(ResolvedMavenJar::depth)
                .toList());
        assertTrue(closure.classpath().stream()
                .allMatch(artifact -> artifact.originRootIndex() == 4));
        assertTrue(closure.classpath().stream()
                .allMatch(artifact -> Set.of("compile", "runtime")
                        .contains(artifact.effectiveScope())));
        assertFalse(artifactIds(closure).contains("provided-dep"));
        assertFalse(artifactIds(closure).contains("test-dep"));
        assertFalse(artifactIds(closure).contains("optional-dep"));
        assertFalse(artifactIds(closure).contains("excluded-dep"));

        List<MavenArtifactIdentity> evidenceIdentities = closure.consultedPoms().stream()
                .map(MavenPomEvidence::identity)
                .toList();
        assertTrue(evidenceIdentities.stream()
                .anyMatch(identity -> identity.artifactId().equals("runtime-root")));
        assertTrue(evidenceIdentities.stream()
                .anyMatch(identity -> identity.artifactId().equals("compile-transitive")));
        List<MavenArtifactIdentity> sortedEvidence = new ArrayList<>(evidenceIdentities);
        sortedEvidence.sort(null);
        assertEquals(sortedEvidence, evidenceIdentities);
        for (MavenPomEvidence evidence : closure.consultedPoms()) {
            assertEquals(sha256(evidence.sourcePath()), evidence.sha256());
        }
        for (ResolvedMavenJar artifact : closure.classpath()) {
            assertEquals(sha256(artifact.sourcePath()), artifact.sha256());
        }

        assertThrows(UnsupportedOperationException.class, closure.classpath()::clear);
        assertThrows(UnsupportedOperationException.class,
                closure.classpath().get(0).dependencyPath()::clear);
        assertThrows(UnsupportedOperationException.class, closure.consultedPoms()::clear);
    }

    @Test
    void equalDepthConflictUsesFirstBreadthFirstRootAndReportsEveryDecision()
            throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        install(remote, "org.example", "first-child", "1.0", "");
        install(remote, "org.example", "second-child", "1.0", "");
        install(remote, "org.example", "other-child", "1.0", "");
        install(remote, "org.example", "same", "1.0", dependencies(
                dependency("org.example", "first-child", "1.0", "", "")));
        install(remote, "org.example", "same", "2.0", dependencies(
                dependency("org.example", "second-child", "1.0", "", "")));
        install(remote, "org.example", "same", "3.0", "");
        install(remote, "org.example", "other", "1.0", dependencies(
                dependency("org.example", "other-child", "1.0", "", "")));

        MavenRuntimeClosure first = lookup(remote, "first-local").resolveRuntimeClosure(
                List.of(
                        root(7, "same", "1.0"),
                        root(8, "same", "2.0"),
                        root(9, "same", "3.0"),
                        root(10, "other", "1.0")),
                false);

        assertEquals(
                List.of("same:1.0", "other:1.0", "first-child:1.0", "other-child:1.0"),
                artifactVersions(first));
        assertEquals(2, first.conflicts().size());
        MavenConflictDecision conflict = first.conflicts().get(0);
        assertEquals("same", conflict.winner().artifactId());
        assertEquals("1.0", conflict.winner().version());
        assertEquals("2.0", conflict.omitted().version());
        assertEquals(MavenConflictDecision.Reason.FIRST_BREADTH_FIRST, conflict.reason());
        assertEquals(0, conflict.winnerDepth());
        assertEquals(0, conflict.omittedDepth());
        assertEquals(7, conflict.winnerOriginRootIndex());
        assertEquals(8, conflict.omittedOriginRootIndex());
        assertEquals(List.of(conflict.winner()), conflict.winnerPath());
        assertEquals(List.of(conflict.omitted()), conflict.omittedPath());
        assertEquals(
                List.of("2.0", "3.0"),
                first.conflicts().stream()
                        .map(decision -> decision.omitted().version())
                        .toList());

        MavenRuntimeClosure reversed = lookup(remote, "reversed-local").resolveRuntimeClosure(
                List.of(root(8, "same", "2.0"), root(7, "same", "1.0")),
                false);
        assertEquals(
                List.of("same:2.0", "second-child:1.0"),
                artifactVersions(reversed));
        assertEquals("1.0", reversed.conflicts().get(0).omitted().version());
        assertEquals("2.0", reversed.conflicts().get(0).winner().version());
    }

    @Test
    void fullConflictEvidenceDoesNotInventDecisionsForPrunedDescendants() throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        install(remote, "org.example", "nested", "1.0", "");
        install(remote, "org.example", "nested", "2.0", "");
        install(remote, "org.example", "parent", "1.0", dependencies(
                dependency("org.example", "nested", "1.0", "", "")));
        install(remote, "org.example", "parent", "2.0", dependencies(
                dependency("org.example", "nested", "2.0", "", "")));

        MavenRuntimeClosure closure = lookup(remote, "local").resolveRuntimeClosure(
                List.of(root(0, "parent", "1.0"), root(1, "parent", "2.0")),
                false);

        assertEquals(List.of("parent:1.0", "nested:1.0"), artifactVersions(closure));
        assertEquals(
                List.of("parent:2.0"),
                closure.conflicts().stream()
                        .map(decision -> decision.omitted().artifactId()
                                + ":" + decision.omitted().version())
                        .toList());
        assertEquals(
                List.of("parent:1.0"),
                closure.conflicts().stream()
                        .map(decision -> decision.winner().artifactId()
                                + ":" + decision.winner().version())
                        .toList());
    }

    @Test
    void recordsParentAndImportedBomPomsAsHashedConsultationEvidence() throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        writePom(
                remote,
                "org.example",
                "evidence-parent",
                "1.0",
                """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <project xmlns="http://maven.apache.org/POM/4.0.0">
                          <modelVersion>4.0.0</modelVersion>
                          <groupId>org.example</groupId>
                          <artifactId>evidence-parent</artifactId>
                          <version>1.0</version>
                          <packaging>pom</packaging>
                        </project>
                        """);
        writePom(
                remote,
                "org.example",
                "evidence-bom",
                "1.0",
                """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <project xmlns="http://maven.apache.org/POM/4.0.0">
                          <modelVersion>4.0.0</modelVersion>
                          <groupId>org.example</groupId>
                          <artifactId>evidence-bom</artifactId>
                          <version>1.0</version>
                          <packaging>pom</packaging>
                          <dependencyManagement>
                            <dependencies>
                              <dependency>
                                <groupId>org.example</groupId>
                                <artifactId>managed-dep</artifactId>
                                <version>3.0</version>
                              </dependency>
                            </dependencies>
                          </dependencyManagement>
                        </project>
                        """);
        install(remote, "org.example", "managed-dep", "3.0", "");
        createJar(artifactPath(
                remote, "org.example", "evidence-root", "1.0", "jar"));
        writePom(
                remote,
                "org.example",
                "evidence-root",
                "1.0",
                """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <project xmlns="http://maven.apache.org/POM/4.0.0">
                          <modelVersion>4.0.0</modelVersion>
                          <parent>
                            <groupId>org.example</groupId>
                            <artifactId>evidence-parent</artifactId>
                            <version>1.0</version>
                          </parent>
                          <artifactId>evidence-root</artifactId>
                          <version>1.0</version>
                          <dependencyManagement>
                            <dependencies>
                              <dependency>
                                <groupId>org.example</groupId>
                                <artifactId>evidence-bom</artifactId>
                                <version>1.0</version>
                                <type>pom</type>
                                <scope>import</scope>
                              </dependency>
                            </dependencies>
                          </dependencyManagement>
                          <dependencies>
                            <dependency>
                              <groupId>org.example</groupId>
                              <artifactId>managed-dep</artifactId>
                            </dependency>
                          </dependencies>
                        </project>
                        """);

        MavenRuntimeClosure closure = lookup(remote, "local").resolveRuntimeClosure(
                List.of(root(0, "evidence-root", "1.0")), false);

        assertEquals(
                List.of("evidence-root:1.0", "managed-dep:3.0"),
                artifactVersions(closure));
        assertEquals(
                Set.of("evidence-parent", "evidence-bom", "evidence-root", "managed-dep"),
                closure.consultedPoms().stream()
                        .map(evidence -> evidence.identity().artifactId())
                        .collect(java.util.stream.Collectors.toSet()));
        assertTrue(closure.consultedPoms().stream()
                .allMatch(evidence -> evidence.sha256().matches("[0-9a-f]{64}")));
    }

    @Test
    void nearerVersionWinsEvenWhenTheFartherVersionIsEncounteredFirst() throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        install(remote, "org.example", "shared", "1.0", "");
        install(remote, "org.example", "shared", "2.0", "");
        install(remote, "org.example", "branch", "1.0", dependencies(
                dependency("org.example", "shared", "1.0", "", "")));
        install(remote, "org.example", "nearest-root", "1.0", dependencies(
                dependency("org.example", "branch", "1.0", "", ""),
                dependency("org.example", "shared", "2.0", "", "")));

        MavenRuntimeClosure closure = lookup(remote, "local").resolveRuntimeClosure(
                List.of(root(2, "nearest-root", "1.0")), false);

        assertEquals(
                List.of("nearest-root:1.0", "branch:1.0", "shared:2.0"),
                artifactVersions(closure));
        assertEquals(1, closure.conflicts().size());
        MavenConflictDecision conflict = closure.conflicts().get(0);
        assertEquals(MavenConflictDecision.Reason.NEAREST, conflict.reason());
        assertEquals("1.0", conflict.omitted().version());
        assertEquals("2.0", conflict.winner().version());
        assertEquals(2, conflict.omittedDepth());
        assertEquals(1, conflict.winnerDepth());
    }

    @Test
    void rejectsAVersionRangeEvenWhenItsConflictCandidateIsOmitted() throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        install(remote, "org.example", "ranged", "1.0", "");
        install(remote, "org.example", "ranged", "2.0", "");
        writeMetadata(remote, "org.example", "ranged", List.of("1.0", "2.0"));
        install(remote, "org.example", "fixed-root", "1.0", dependencies(
                dependency("org.example", "ranged", "1.0", "", "")));
        install(remote, "org.example", "range-root", "1.0", dependencies(
                dependency("org.example", "ranged", "[1.0,2.0]", "", "")));

        MavenArtifactLookupException exception = assertThrows(
                MavenArtifactLookupException.class,
                () -> lookup(remote, "local").resolveRuntimeClosure(
                        List.of(
                                root(0, "fixed-root", "1.0"),
                                root(1, "range-root", "1.0")),
                        false));

        assertEquals(MavenArtifactLookupException.Kind.INVALID_CLOSURE, exception.kind());
        assertTrue(exception.getMessage().contains("version range"), exception.getMessage());
        assertTrue(exception.getMessage().contains("ranged"), exception.getMessage());
    }

    @Test
    void rejectsASelectedNonJarWithItsDependencyPathBeforeArtifactDownload()
            throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        install(remote, "org.example", "zip-dep", "1.0", "");
        install(remote, "org.example", "jar-only-root", "1.0", dependencies(
                dependency(
                        "org.example",
                        "zip-dep",
                        "1.0",
                        "runtime",
                        "<type>zip</type>")));

        MavenArtifactLookupException exception = assertThrows(
                MavenArtifactLookupException.class,
                () -> lookup(remote, "local").resolveRuntimeClosure(
                        List.of(root(0, "jar-only-root", "1.0")), false));

        assertEquals(MavenArtifactLookupException.Kind.INVALID_CLOSURE, exception.kind());
        assertTrue(exception.getMessage().contains(
                "org.example:jar-only-root:jar:1.0 -> org.example:zip-dep:zip:1.0"),
                exception.getMessage());
        assertFalse(Files.exists(artifactPath(
                root.resolve("local"), "org.example", "zip-dep", "1.0", "zip")));
    }

    @Test
    void classifiesMalformedPomAsInvalidClosureOnlineAndFromTheOfflineCache()
            throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        createJar(artifactPath(
                remote, "org.example", "malformed-root", "1.0", "jar"));
        writePom(
                remote,
                "org.example",
                "malformed-root",
                "1.0",
                "<project><modelVersion>4.0.0</modelVersion><broken>");
        CentralMavenArtifactLookup lookup = lookup(remote, "local");

        for (boolean offline : List.of(false, true)) {
            MavenArtifactLookupException exception = assertThrows(
                    MavenArtifactLookupException.class,
                    () -> lookup.resolveRuntimeClosure(
                            List.of(root(0, "malformed-root", "1.0")), offline));

            assertEquals(MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                    exception.kind(), "offline=" + offline);
            assertTrue(exception.getMessage().contains("malformed-root"),
                    exception.getMessage());
        }
        assertTrue(Files.isRegularFile(artifactPath(
                root.resolve("local"), "org.example", "malformed-root", "1.0", "pom")));
    }

    @Test
    void offlineMissingPomIsATypedDescriptorMissEvenWhenTheRootJarIsCached()
            throws Exception {
        Path remote = Files.createDirectories(root.resolve("remote"));
        install(remote, "org.example", "offline-root", "1.0", "");
        Path local = root.resolve("local");
        Path remoteJar = artifactPath(
                remote, "org.example", "offline-root", "1.0", "jar");
        Path localJar = artifactPath(
                local, "org.example", "offline-root", "1.0", "jar");
        Files.createDirectories(localJar.getParent());
        Files.copy(remoteJar, localJar);
        String repositoryId = fixedRepositoryId(remote.toUri(), 0);
        Files.writeString(
                localJar.resolveSibling("_remote.repositories"),
                localJar.getFileName() + ">" + repositoryId + "=\n");

        MavenArtifactLookupException exception = assertThrows(
                MavenArtifactLookupException.class,
                () -> new CentralMavenArtifactLookup(local, List.of(remote.toUri()))
                        .resolveRuntimeClosure(
                                List.of(root(0, "offline-root", "1.0")), true));

        assertEquals(MavenArtifactLookupException.Kind.OFFLINE_MISS, exception.kind());
        assertTrue(exception.getMessage().contains("offline-root:pom:1.0"),
                exception.getMessage());
        assertFalse(Files.exists(artifactPath(
                local, "org.example", "offline-root", "1.0", "pom")));
    }

    @Test
    void ignoresRepositoriesDeclaredByPomAndUsesOnlyConfiguredFixedRepositories()
            throws Exception {
        Path trusted = Files.createDirectories(root.resolve("trusted"));
        Path untrusted = Files.createDirectories(root.resolve("untrusted"));
        install(untrusted, "org.example", "untrusted-only", "1.0", "");
        install(
                trusted,
                "org.example",
                "fixed-root",
                "1.0",
                dependencies(dependency(
                        "org.example", "untrusted-only", "1.0", "", "")),
                """
                        <repositories>
                          <repository>
                            <id>pom-declared</id>
                            <url>%s</url>
                          </repository>
                        </repositories>
                        """.formatted(untrusted.toUri()));

        MavenArtifactLookupException exception = assertThrows(
                MavenArtifactLookupException.class,
                () -> lookup(trusted, "local").resolveRuntimeClosure(
                        List.of(root(0, "fixed-root", "1.0")), false));

        assertEquals(MavenArtifactLookupException.Kind.NOT_FOUND, exception.kind());
        assertTrue(exception.getMessage().contains("untrusted-only"), exception.getMessage());
    }

    @Test
    void immutableModelsRejectContradictoryDepthAndMediationEvidence() {
        MavenArtifactIdentity versionOne = new MavenArtifactIdentity(
                "org.example", "identity", "jar", "", "1.0");
        MavenArtifactIdentity versionTwo = new MavenArtifactIdentity(
                "org.example", "identity", "jar", "", "2.0");

        assertThrows(IllegalArgumentException.class, () -> new ResolvedMavenJar(
                versionOne,
                1,
                0,
                "runtime",
                List.of(versionOne),
                root.resolve("identity.jar"),
                "0".repeat(64)));
        assertThrows(IllegalArgumentException.class, () -> new MavenConflictDecision(
                versionTwo,
                versionOne,
                MavenConflictDecision.Reason.NEAREST,
                0,
                0,
                1,
                0,
                List.of(versionTwo),
                List.of(versionOne)));
        assertThrows(IllegalArgumentException.class, () -> new MavenConflictDecision(
                versionTwo,
                versionOne,
                MavenConflictDecision.Reason.FIRST_BREADTH_FIRST,
                1,
                1,
                1,
                0,
                List.of(versionTwo),
                List.of(versionOne)));
    }

    private CentralMavenArtifactLookup lookup(Path remote, String localName) {
        return new CentralMavenArtifactLookup(
                root.resolve(localName), List.of(remote.toUri()));
    }

    private static MavenRuntimeRoot root(int index, String artifactId, String version) {
        return new MavenRuntimeRoot(
                index,
                MavenCoordinate.parse("maven:org.example:" + artifactId + ":" + version));
    }

    private static List<String> artifactIds(MavenRuntimeClosure closure) {
        return closure.classpath().stream()
                .map(artifact -> artifact.identity().artifactId())
                .toList();
    }

    private static List<String> artifactVersions(MavenRuntimeClosure closure) {
        return closure.classpath().stream()
                .map(artifact -> artifact.identity().artifactId()
                        + ":" + artifact.identity().version())
                .toList();
    }

    private static String dependencies(String... dependencies) {
        return "<dependencies>" + String.join("", dependencies) + "</dependencies>";
    }

    private static String dependency(
            String groupId,
            String artifactId,
            String version,
            String scope,
            String extra) {
        return """
                <dependency>
                  <groupId>%s</groupId>
                  <artifactId>%s</artifactId>
                  <version>%s</version>
                  %s
                  %s
                </dependency>
                """.formatted(
                groupId,
                artifactId,
                version,
                scope.isBlank() ? "" : "<scope>" + scope + "</scope>",
                extra);
    }

    private static String systemDependency(Path systemJar) {
        return """
                <dependency>
                  <groupId>org.example</groupId>
                  <artifactId>system-only</artifactId>
                  <version>1.0</version>
                  <scope>system</scope>
                  <systemPath>%s</systemPath>
                </dependency>
                """.formatted(xml(systemJar.toAbsolutePath().normalize().toString()));
    }

    private static String xml(String value) {
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private static void install(
            Path repository,
            String groupId,
            String artifactId,
            String version,
            String dependencyXml) throws IOException {
        install(repository, groupId, artifactId, version, dependencyXml, "");
    }

    private static void install(
            Path repository,
            String groupId,
            String artifactId,
            String version,
            String dependencyXml,
        String extraProjectXml) throws IOException {
        createJar(artifactPath(repository, groupId, artifactId, version, "jar"));
        writePom(repository, groupId, artifactId, version, """
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <modelVersion>4.0.0</modelVersion>
                  <groupId>%s</groupId>
                  <artifactId>%s</artifactId>
                  <version>%s</version>
                  <packaging>jar</packaging>
                  %s
                  %s
                </project>
                """.formatted(
                groupId, artifactId, version, dependencyXml, extraProjectXml));
    }

    private static void writePom(
            Path repository,
            String groupId,
            String artifactId,
            String version,
            String content) throws IOException {
        Path pom = artifactPath(repository, groupId, artifactId, version, "pom");
        Files.createDirectories(pom.getParent());
        Files.writeString(pom, content);
    }

    private static void writeMetadata(
            Path repository,
            String groupId,
            String artifactId,
            List<String> versions) throws IOException {
        Path metadata = repository.resolve(groupId.replace('.', '/'))
                .resolve(artifactId)
                .resolve("maven-metadata.xml");
        Files.createDirectories(metadata.getParent());
        String versionElements = versions.stream()
                .map(version -> "<version>" + version + "</version>")
                .reduce("", String::concat);
        Files.writeString(metadata, """
                <?xml version="1.0" encoding="UTF-8"?>
                <metadata>
                  <groupId>%s</groupId>
                  <artifactId>%s</artifactId>
                  <versioning>
                    <latest>%s</latest>
                    <release>%s</release>
                    <versions>%s</versions>
                  </versioning>
                </metadata>
                """.formatted(
                groupId,
                artifactId,
                versions.getLast(),
                versions.getLast(),
                versionElements));
    }

    private static Path artifactPath(
            Path repository,
            String groupId,
            String artifactId,
            String version,
            String extension) {
        return repository.resolve(groupId.replace('.', '/'))
                .resolve(artifactId)
                .resolve(version)
                .resolve(artifactId + "-" + version + "." + extension);
    }

    private static void createJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream ignored = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            // A valid empty test JAR is sufficient for artifact resolution.
        }
    }

    private static String sha256(Path path) throws IOException {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String fixedRepositoryId(URI uri, int index) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    uri.toASCIIString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return "v1-repository-" + index + "-" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
