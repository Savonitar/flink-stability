package org.savonitar.flink.stability.testcontainers.storage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Read-only, JDK-only launcher; the owned Flink distribution supplies metadata classes at runtime. */
public final class CheckpointStateInspector {
    static final String OPERATOR_UID = "flink-stability-keyed-prefix-v1";
    static final int MAX_METADATA_BYTES = 16 * 1024 * 1024;
    static final long MAX_TOTAL_STATE_BYTES = 64L * 1024 * 1024;
    static final int MAX_FILES = 128;
    private static final String KEY_GROUPS = "org.apache.flink.runtime.state.KeyGroupsStateHandle";
    private static final Set<String> FILE_HANDLES = Set.of(
            "org.apache.flink.runtime.state.filesystem.FileStateHandle",
            "org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle");

    private CheckpointStateInspector() {}

    public static void main(String[] args) {
        try {
            Arguments parsed = arguments(args);
            Receipt receipt = inspect(parsed.root(), parsed.jobId(), parsed.checkpointId(), CheckpointStateInspector::readMetadata);
            System.out.println(receipt.toJson());
        } catch (Exception failure) {
            // Do not print reflected exception bodies or the metadata's serialized values.
            System.err.println("Checkpoint state inspection failed: " + failure.getClass().getSimpleName());
            System.exit(1);
        }
    }

    record Arguments(Path root, String jobId, long checkpointId) {}
    record FileEvidence(String path, long size, String sha256) {
        String toJson() { return "{\"path\":" + quote(path) + ",\"size\":" + size + ",\"sha256\":" + quote(sha256) + "}"; }
    }
    record Receipt(String jobId, long checkpointId, String operatorUid, FileEvidence metadata, List<FileEvidence> files) {
        Receipt { files = List.copyOf(files); }
        String toJson() {
            return "{\"version\":1,\"jobId\":" + quote(jobId) + ",\"checkpointId\":" + checkpointId
                    + ",\"operatorUid\":" + quote(operatorUid) + ",\"metadata\":" + metadata.toJson()
                    + ",\"files\":[" + String.join(",", files.stream().map(FileEvidence::toJson).toList()) + "]}";
        }
    }
    record ManagedFile(URI path, long size) {}
    record Metadata(long checkpointId, String operatorUid, List<ManagedFile> files) {
        Metadata { files = List.copyOf(files); }
    }
    @FunctionalInterface
    interface MetadataReader { Metadata read(byte[] metadata, String externalPointer) throws Exception; }

    static Arguments arguments(String[] args) {
        if (args.length != 6) throw new IllegalArgumentException("Exactly three arguments required");
        Map<String, String> values = new java.util.HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!Set.of("--root", "--job-id", "--checkpoint-id").contains(args[i])
                    || values.putIfAbsent(args[i], args[i + 1]) != null) {
                throw new IllegalArgumentException("Unknown or repeated inspector argument");
            }
        }
        Path root = Path.of(values.get("--root"));
        String jobId = values.get("--job-id");
        String checkpointId = values.get("--checkpoint-id");
        if (!root.isAbsolute() || !root.equals(root.normalize()) || root.toString().length() > 2048
                || !jobId.matches("[0-9a-f]{32}") || !checkpointId.matches("[1-9][0-9]{0,18}")) {
            throw new IllegalArgumentException("Invalid inspector identity/path");
        }
        return new Arguments(root, jobId, Long.parseLong(checkpointId));
    }

    static Receipt inspect(Path root, String jobId, long checkpointId, MetadataReader reader) throws Exception {
        // Revalidate even callers of the testable API; no ambient filesystem path may become a fallback.
        Arguments parsed = arguments(new String[]{"--root", root.toString(), "--job-id", jobId,
                "--checkpoint-id", Long.toString(checkpointId)});
        Path directory = parsed.root().resolve(jobId).resolve("chk-" + checkpointId);
        requireNoSymlinks(directory, true);
        Path metadataPath = directory.resolve("_metadata");
        requireNoSymlinks(metadataPath, false);
        long metadataSize = Files.size(metadataPath);
        if (metadataSize < 1 || metadataSize > MAX_METADATA_BYTES) throw new IOException("Metadata size outside bound");
        byte[] metadataBytes;
        try (var input = Files.newInputStream(metadataPath, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            metadataBytes = input.readNBytes(MAX_METADATA_BYTES + 1);
        }
        if (metadataBytes.length != metadataSize || Files.size(metadataPath) != metadataSize) {
            throw new IOException("Metadata changed or exceeded bound");
        }
        Metadata metadata = reader.read(metadataBytes, directory.toUri().toString());
        if (metadata.checkpointId() != checkpointId || !OPERATOR_UID.equals(metadata.operatorUid())
                || metadata.files().isEmpty() || metadata.files().size() > MAX_FILES) {
            throw new IOException("Metadata checkpoint/operator/handle identity differs");
        }
        Set<Path> seen = new HashSet<>();
        List<FileEvidence> files = new ArrayList<>();
        long total = 0;
        for (ManagedFile handle : metadata.files()) {
            URI uri = handle.path();
            if (!"file".equals(uri.getScheme()) || uri.getAuthority() != null
                    || uri.getQuery() != null || uri.getFragment() != null) throw new IOException("Non-local state handle");
            Path path = Path.of(uri);
            if (!path.isAbsolute() || !path.equals(path.normalize()) || !path.startsWith(directory)
                    || path.equals(metadataPath) || path.toString().length() > 4096 || !seen.add(path)) {
                throw new IOException("State handle escapes exact checkpoint or repeats a file");
            }
            if (handle.size() <= 0 || handle.size() > MAX_TOTAL_STATE_BYTES - total) throw new IOException("State size outside bound");
            total += handle.size();
            requireNoSymlinks(path, false);
            if (Files.size(path) != handle.size()) throw new IOException("State file length differs from handle");
            MessageDigest digest = sha256();
            long read = 0;
            try (SeekableByteChannel input = Files.newByteChannel(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                ByteBuffer buffer = ByteBuffer.allocate(8192);
                int count;
                while ((count = input.read(buffer)) != -1) {
                    read += count;
                    if (read > handle.size()) throw new IOException("State file grew during inspection");
                    buffer.flip();
                    digest.update(buffer);
                    buffer.clear();
                }
            }
            if (read != handle.size() || Files.size(path) != read) throw new IOException("State file changed during inspection");
            files.add(new FileEvidence(path.toString(), read, HexFormat.of().formatHex(digest.digest())));
        }
        files.sort(java.util.Comparator.comparing(FileEvidence::path));
        return new Receipt(jobId, checkpointId, OPERATOR_UID,
                new FileEvidence(metadataPath.toString(), metadataBytes.length, HexFormat.of().formatHex(sha256().digest(metadataBytes))), files);
    }

    static Metadata readMetadata(byte[] bytes, String externalPointer) throws Exception {
        ClassLoader loader = CheckpointStateInspector.class.getClassLoader();
        Class<?> checkpoints = Class.forName("org.apache.flink.runtime.checkpoint.Checkpoints", true, loader);
        Object metadata;
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            metadata = checkpoints.getMethod("loadCheckpointMetadata", DataInputStream.class, ClassLoader.class, String.class)
                    .invoke(null, input, loader, externalPointer);
            if (input.available() != 0) throw new IOException("Trailing checkpoint metadata bytes");
        }
        return metadataModel(metadata);
    }

    static Metadata metadataModel(Object metadata) throws Exception {
        long id = longValue(invoke(metadata, "getCheckpointId"));
        Collection<?> operators = collection(invoke(metadata, "getOperatorStates"), 1024);
        Object selected = null;
        for (Object operator : operators) {
            Object uid = invoke(operator, "getOperatorUid");
            if (!(uid instanceof Optional<?> optional)) throw new IOException("Unknown operator UID representation");
            if (optional.isPresent() && OPERATOR_UID.equals(optional.orElseThrow())) {
                if (selected != null) throw new IOException("Duplicate keyed operator UID");
                selected = operator;
            }
        }
        if (selected == null || longValue(invoke(selected, "getParallelism")) != 1) {
            throw new IOException("Missing or unsupported keyed operator");
        }
        Object subtasksObject = invoke(selected, "getSubtaskStates");
        if (!(subtasksObject instanceof Map<?, ?> subtasks) || subtasks.size() != 1 || !subtasks.containsKey(0)) {
            throw new IOException("Unexpected keyed operator subtasks");
        }
        Collection<?> handles = collection(invoke(subtasks.get(0), "getManagedKeyedState"), MAX_FILES);
        if (handles.isEmpty()) throw new IOException("No managed keyed state");
        List<ManagedFile> files = new ArrayList<>();
        for (Object handle : handles) {
            if (handle == null || !KEY_GROUPS.equals(handle.getClass().getName())) throw new IOException("Unknown keyed handle type");
            Object delegate = invoke(handle, "getDelegateStateHandle");
            if (delegate == null || !FILE_HANDLES.contains(delegate.getClass().getName())) {
                throw new IOException("Inline or unsupported keyed stream handle");
            }
            Object path = invoke(delegate, "getFilePath");
            Object uri = invoke(path, "toUri");
            if (!(uri instanceof URI fileUri)) throw new IOException("Unknown file URI representation");
            files.add(new ManagedFile(fileUri, longValue(invoke(delegate, "getStateSize"))));
        }
        return new Metadata(id, OPERATOR_UID, files);
    }

    private static Object invoke(Object receiver, String name) throws Exception {
        if (receiver == null) throw new IOException("Missing metadata object");
        try { return receiver.getClass().getMethod(name).invoke(receiver); }
        catch (InvocationTargetException failure) { throw new IOException("Metadata accessor failed", failure.getCause()); }
    }

    private static long longValue(Object value) throws IOException {
        if (value instanceof Long result) return result;
        if (value instanceof Integer result) return result;
        throw new IOException("Unknown metadata integer representation");
    }

    private static Collection<?> collection(Object value, int maximum) throws IOException {
        if (!(value instanceof Collection<?> result) || result.size() > maximum) throw new IOException("Metadata collection exceeds bound");
        return result;
    }

    static void requireNoSymlinks(Path path, boolean directory) throws IOException {
        Path current = path.getRoot();
        for (Path segment : path) {
            current = current.resolve(segment);
            if (Files.isSymbolicLink(current)) throw new IOException("Symlink in inspection path");
            boolean last = current.equals(path);
            if (!last || directory) {
                if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Missing checkpoint directory");
            } else if (!Files.isRegularFile(current, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Missing regular checkpoint file");
        }
    }

    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static String quote(String value) {
        StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '"' || ch == '\\') result.append('\\').append(ch);
            else if (ch < 0x20 || ch > 0x7e) result.append(String.format("\\u%04x", (int) ch));
            else result.append(ch);
        }
        return result.append('"').toString();
    }
}
