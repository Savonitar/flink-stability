package org.savonitar.flink.stability.core.artifact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FuseCheckpointStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FuseCheckpointBundlePreparerTest {
    @TempDir Path root;
    private final ObjectMapper json = new ObjectMapper();

    @Test void snapshotsPinnedManifestAndEveryHelperWithoutDependingOnOriginalFiles() throws Exception {
        var fixture = fixture();
        var prepared = prepare(fixture, fixture.hash());
        assertNotEquals(fixture.bundle(), prepared.bundleRoot());
        assertTrue(prepared.bundleRoot().startsWith(fixture.preparation()));
        assertEquals(fixture.hash(), prepared.manifestSha256());
        assertEquals(FuseCheckpointStorage.REQUIRED_FILES, prepared.files().keySet());
        for (String name : prepared.files().keySet()) {
            Path snapshot = prepared.bundleRoot().resolve("bundle").resolve(name);
            assertEquals(prepared.files().get(name), Digests.sha256(snapshot));
            Files.writeString(fixture.bundle().resolve("bundle").resolve(name), "changed original");
            assertEquals(prepared.files().get(name), Digests.sha256(snapshot));
        }
        Files.writeString(fixture.bundle().resolve("build-evidence.json"), "changed manifest");
        assertEquals(fixture.hash(), Digests.sha256(prepared.bundleRoot().resolve("build-evidence.json")));
    }

    @Test void rejectsWrongManifestPinBeforeCopying() throws Exception {
        var fixture = fixture();
        rejected(() -> prepare(fixture, "0".repeat(64)));
        try (var files = Files.list(fixture.preparation())) { assertEquals(0, files.count()); }
    }

    @Test void rejectsTamperedHelperEvenWhenManifestRemainsPinned() throws Exception {
        var fixture = fixture();
        Files.writeString(fixture.bundle().resolve("bundle/checkpoint-fuse"), "tampered helper");
        rejected(() -> prepare(fixture, fixture.hash()));
    }

    @Test void rejectsIncompleteBuildCleanupAndWrongRuntimeIdentity() throws Exception {
        for (String change : List.of("status", "cleanup", "image", "platform", "network", "java", "fuse")) {
            var fixture = fixture();
            var manifest = fixture.manifest();
            switch (change) {
                case "status" -> manifest.put("status", "failed");
                case "cleanup" -> manifest.putArray("cleanup").addObject().put("status", "unknown");
                case "image" -> manifest.put("imageId", "sha256:" + "a".repeat(64));
                case "platform" -> manifest.put("platform", "linux/amd64");
                case "network" -> manifest.put("networkDisconnectedBeforeSourceCopy", false);
                case "java" -> manifest.put("javaRelease", 21);
                case "fuse" -> manifest.put("libfuseVersion", "unknown");
            }
            String pin = writeManifest(fixture.bundle(), manifest);
            rejected(() -> prepare(fixture, pin));
        }
    }

    @Test void rejectsMissingOrUnexpectedFilesAndPins() throws Exception {
        for (String change : List.of("missing", "extra", "pin", "unlisted")) {
            var fixture = fixture();
            String hash = fixture.hash();
            switch (change) {
                case "missing" -> Files.delete(fixture.bundle().resolve("bundle/probe.jar"));
                case "extra" -> Files.writeString(fixture.bundle().resolve("bundle/untrusted-helper"), "extra");
                case "pin" -> ((ObjectNode) fixture.manifest().path("files")).put("probe.jar", "not-a-hash");
                case "unlisted" -> ((ObjectNode) fixture.manifest().path("files")).remove("probe.jar");
            }
            if (change.equals("pin") || change.equals("unlisted")) hash = writeManifest(fixture.bundle(), fixture.manifest());
            String pin = hash;
            rejected(() -> prepare(fixture, pin));
        }
    }

    @Test void rejectsSymlinkedHelperManifestAndBundleDirectory() throws Exception {
        for (String change : List.of("helper", "manifest", "directory")) {
            var fixture = fixture();
            Path original = change.equals("helper") ? fixture.bundle().resolve("bundle/checkpoint-fuse")
                    : change.equals("manifest") ? fixture.bundle().resolve("build-evidence.json") : fixture.bundle().resolve("bundle");
            Path moved = original.resolveSibling(original.getFileName() + "-real");
            Files.move(original, moved);
            Files.createSymbolicLink(original, moved.getFileName());
            rejected(() -> prepare(fixture, fixture.hash()));
        }
    }

    @Test void rejectsAbsoluteAndTraversingBundleRoots() throws Exception {
        var fixture = fixture();
        for (String declared : List.of(fixture.bundle().toString(), "../elsewhere", "file:helper", "a\\b")) {
            rejected(() -> FuseCheckpointBundlePreparer.prepare(root, fixture.preparation(), root.resolve("scenario.yaml"),
                    declared, fixture.hash(), FuseCheckpointStorage.SUPPORTED_IMAGE_ID, "linux/arm64"));
        }
    }

    @Test void rejectsDuplicateJsonFieldsWithoutPrintingManifestContent() throws Exception {
        var fixture = fixture();
        String text = json.writeValueAsString(fixture.manifest()).replace("\"status\":\"built\"", "\"status\":\"built\",\"status\":\"built\"");
        Files.writeString(fixture.bundle().resolve("build-evidence.json"), text);
        var error = assertThrows(SpecificationException.class, () -> prepare(fixture, Digests.sha256(text.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        assertTrue(error.getMessage().contains("invalid JSON manifest"));
        assertFalse(error.getMessage().contains("networkDisconnectedBeforeSourceCopy"));
    }

    @Test void boundsManifestAndBinaryReads() throws Exception {
        var manifestFixture = fixture();
        Files.writeString(manifestFixture.bundle().resolve("build-evidence.json"), " ".repeat(1024 * 1024 + 1));
        rejected(() -> prepare(manifestFixture, manifestFixture.hash()));
        var binaryFixture = fixture();
        try (var file = new java.io.RandomAccessFile(binaryFixture.bundle().resolve("bundle/checkpoint-fuse").toFile(), "rw")) {
            file.setLength(32L * 1024 * 1024 + 1);
        }
        rejected(() -> prepare(binaryFixture, binaryFixture.hash()));
    }

    private FuseCheckpointStorage prepare(Fixture fixture, String hash) {
        return FuseCheckpointBundlePreparer.prepare(root, fixture.preparation(), root.resolve("scenario.yaml"),
                root.relativize(fixture.bundle()).toString(), hash, FuseCheckpointStorage.SUPPORTED_IMAGE_ID, "linux/arm64");
    }

    private void rejected(Runnable action) {
        var failure = assertThrows(SpecificationException.class, action::run);
        assertEquals(SpecificationException.Stage.ARTIFACT, failure.stage());
        assertEquals("artifact.checkpoint-storage.bundle-invalid", failure.diagnostics().getFirst().code());
    }

    private Fixture fixture() throws IOException {
        Path bundle = Files.createTempDirectory(root, "helper-");
        Path files = Files.createDirectory(bundle.resolve("bundle"));
        ObjectNode manifest = json.createObjectNode().put("schemaVersion", 1).put("status", "built")
                .put("networkDisconnectedBeforeSourceCopy", true).put("javaRelease", 17).put("libfuseVersion", "3.14.0")
                .put("imageId", FuseCheckpointStorage.SUPPORTED_IMAGE_ID).put("platform", "linux/arm64");
        manifest.putArray("cleanup").addObject().put("status", "removed");
        var pins = manifest.putObject("files");
        for (String name : FuseCheckpointStorage.REQUIRED_FILES) {
            Path file = Files.writeString(files.resolve(name), "synthetic test helper " + name);
            pins.put(name, Digests.sha256(file));
        }
        return new Fixture(bundle, Files.createTempDirectory(root, "prepared-"), manifest, writeManifest(bundle, manifest));
    }

    private String writeManifest(Path bundle, ObjectNode manifest) throws IOException {
        byte[] bytes = json.writeValueAsBytes(manifest);
        Files.write(bundle.resolve("build-evidence.json"), bytes);
        return Digests.sha256(bytes);
    }
    private record Fixture(Path bundle, Path preparation, ObjectNode manifest, String hash) {}
}
