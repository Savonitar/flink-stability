package org.savonitar.flink.stability.core.artifact;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FuseCheckpointStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Snapshots the explicitly pinned local PR3 helper bundle before any container can start. */
public final class FuseCheckpointBundlePreparer {
    private static final int MANIFEST_LIMIT = 1024 * 1024;
    private static final int FILE_LIMIT = 32 * 1024 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private FuseCheckpointBundlePreparer() {}

    public static FuseCheckpointStorage prepare(PreparedScenarioPlan prepared, String bundle,
            String manifestSha256, String imageId, String platform) {
        return prepare(prepared.artifactRoot(), prepared.preparationRoot(),
                prepared.scenarioPlan().scenario().template().source(), bundle, manifestSha256, imageId, platform);
    }

    static FuseCheckpointStorage prepare(Path artifactRoot, Path preparationRoot, Path source,
            String bundle, String manifestSha256, String imageId, String platform) {
        try {
            if (!manifestSha256.matches("[0-9a-f]{64}")
                    || !FuseCheckpointStorage.SUPPORTED_IMAGE_ID.equals(imageId)
                    || !FuseCheckpointStorage.SUPPORTED_PLATFORM.equals(platform))
                throw new IOException("Unsupported or missing helper/runtime identity");
            Path declared = Path.of(bundle);
            if (declared.isAbsolute() || bundle.contains(":") || bundle.contains("\\"))
                throw new IOException("Helper bundle must be relative to the artifact root");
            for (Path part : declared) if (part.toString().equals(".."))
                throw new IOException("Helper bundle traversal is forbidden");
            Path root = checked(artifactRoot, artifactRoot.resolve(declared).normalize(), true);
            Path manifest = checked(artifactRoot, root.resolve("build-evidence.json"), false);
            byte[] manifestBytes = readBounded(manifest, MANIFEST_LIMIT);
            if (!Digests.sha256(manifestBytes).equals(manifestSha256))
                throw new IOException("Helper manifest SHA-256 does not match its declared pin");
            JsonNode receipt = JSON.readTree(manifestBytes);
            if (receipt == null || !receipt.isObject() || !receipt.path("schemaVersion").isIntegralNumber()
                    || receipt.path("schemaVersion").asInt() != 1
                    || !"built".equals(receipt.path("status").asText())
                    || !receipt.path("networkDisconnectedBeforeSourceCopy").isBoolean()
                    || !receipt.path("networkDisconnectedBeforeSourceCopy").booleanValue()
                    || !receipt.path("javaRelease").isIntegralNumber() || receipt.path("javaRelease").asInt() != 17
                    || !"3.14.0".equals(receipt.path("libfuseVersion").asText())
                    || !imageId.equals(receipt.path("imageId").asText())
                    || !platform.equals(receipt.path("platform").asText())
                    || !receipt.path("cleanup").isArray() || receipt.path("cleanup").isEmpty())
                throw new IOException("Helper build evidence is incomplete or differs from the runtime pin");
            for (JsonNode cleanup : receipt.path("cleanup"))
                if (!"removed".equals(cleanup.path("status").asText()))
                    throw new IOException("Helper build cleanup was not confirmed");
            Map<String, String> pins = new TreeMap<>();
            if (!receipt.path("files").isObject()) throw new IOException("Helper file pins are missing");
            var entries = receipt.path("files").fields();
            while (entries.hasNext()) {
                var entry = entries.next();
                if (!entry.getValue().isTextual()) throw new IOException("Helper file pins must be strings");
                pins.put(entry.getKey(), entry.getValue().textValue());
            }
            if (!pins.keySet().equals(FuseCheckpointStorage.REQUIRED_FILES)
                    || pins.values().stream().anyMatch(value -> !value.matches("[0-9a-f]{64}")))
                throw new IOException("Helper bundle has missing, unexpected or malformed file pins");
            Path files = checked(artifactRoot, root.resolve("bundle"), true);
            try (var fileEntries = Files.list(files)) {
                if (!fileEntries.limit(4).map(path -> path.getFileName().toString()).collect(java.util.stream.Collectors.toSet())
                        .equals(FuseCheckpointStorage.REQUIRED_FILES))
                    throw new IOException("Helper bundle contains missing or unexpected files");
            }
            Path snapshot = Files.createTempDirectory(preparationRoot, "fuse-");
            Files.setPosixFilePermissions(snapshot, PosixFilePermissions.fromString("rwx------"));
            Files.write(snapshot.resolve("build-evidence.json"), manifestBytes, StandardOpenOption.CREATE_NEW);
            Files.createDirectory(snapshot.resolve("bundle"));
            for (var pin : pins.entrySet()) {
                Path file = checked(artifactRoot, files.resolve(pin.getKey()), false);
                byte[] bytes = readBounded(file, FILE_LIMIT);
                if (!Digests.sha256(bytes).equals(pin.getValue()))
                    throw new IOException("Helper file SHA-256 differs from pinned manifest: " + pin.getKey());
                Path target = snapshot.resolve("bundle").resolve(pin.getKey());
                Files.write(target, bytes, StandardOpenOption.CREATE_NEW);
                Files.setPosixFilePermissions(target, PosixFilePermissions.fromString(
                        pin.getKey().endsWith(".jar") ? "rw-r--r--" : "rwxr-xr-x"));
            }
            return new FuseCheckpointStorage(snapshot.toAbsolutePath().normalize(), manifestSha256, imageId, platform, pins);
        } catch (IOException | RuntimeException failure) {
            throw new SpecificationException(SpecificationException.Stage.ARTIFACT, List.of(new Diagnostic(
                    source, ResolutionScope.SINGLE, "artifact.checkpoint-storage.bundle-invalid",
                    "$/workload/jobs/0/checkpointing/storage/fuse",
                    "Cannot prepare the pinned FUSE helper bundle: " + safeMessage(failure))));
        }
    }

    private static Path checked(Path base, Path candidate, boolean directory) throws IOException {
        Path root = base.toAbsolutePath().normalize();
        Path value = candidate.toAbsolutePath().normalize();
        if (!value.startsWith(root) || value.equals(root)) throw new IOException("Helper path must stay inside artifact root");
        Path current = root;
        if (Files.isSymbolicLink(current)) throw new IOException("Artifact root is a symlink");
        for (Path part : root.relativize(value)) {
            current = current.resolve(part);
            if (Files.isSymbolicLink(current)) throw new IOException("Helper paths must not contain symlinks");
        }
        if (directory ? !Files.isDirectory(value, LinkOption.NOFOLLOW_LINKS)
                : !Files.isRegularFile(value, LinkOption.NOFOLLOW_LINKS))
            throw new IOException("Helper path has the wrong file type");
        return value;
    }

    private static byte[] readBounded(Path path, int limit) throws IOException {
        if (Files.size(path) > limit) throw new IOException("Helper file exceeds preparation limit");
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] bytes = input.readNBytes(limit + 1);
            if (bytes.length > limit) throw new IOException("Helper file exceeds preparation limit");
            return bytes;
        }
    }

    private static String safeMessage(Exception failure) {
        // JSON parse errors may quote a source line; only filesystem/contract errors are rendered.
        return failure instanceof com.fasterxml.jackson.core.JacksonException
                ? "invalid JSON manifest" : String.valueOf(failure.getMessage());
    }
}
