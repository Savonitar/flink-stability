package org.savonitar.flink.stability.testcontainers.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Standalone JDK-only filesystem observer, compilable with javac --release 17.
 * It never changes a fault rule and never retries an operation through a raw path.
 * Requests are bounded ASCII TSV; every completed command emits one JSON receipt.
 */
public final class CheckpointStorageIoProbe implements AutoCloseable {
    public static final int MAX_COMMAND_BYTES = 8192;
    public static final int MAX_PAYLOAD_BYTES = 4096;
    public static final int MAX_READ_BYTES = 16384;
    public static final int MAX_COMMANDS = 256;
    public static final int MAX_HANDLES = 8;
    public static final int MAX_DIRECTORY_ENTRIES = 128;
    public static final int WATCHDOG_EXIT_CODE = 124;
    private static final int MAX_PATH_LENGTH = 256;
    private final Path root;
    private final Path rawRoot;
    private final Path evidenceRoot;
    private final int timeoutSeconds;
    private final Map<String, HeldFile> handles = new HashMap<>();
    private long sequence;
    private long previousRequestId;
    private boolean quit;

    CheckpointStorageIoProbe(Path root, Path rawRoot, Path evidenceRoot, int timeoutSeconds) throws IOException {
        if (timeoutSeconds < 1 || timeoutSeconds > 120)
            throw new IllegalArgumentException("timeout-seconds must be in 1..120");
        this.root = root.toAbsolutePath().normalize();
        this.rawRoot = rawRoot.toAbsolutePath().normalize();
        this.evidenceRoot = evidenceRoot.toAbsolutePath().normalize();
        this.timeoutSeconds = timeoutSeconds;
        if (this.root.startsWith(this.rawRoot) || this.rawRoot.startsWith(this.root))
            throw new IllegalArgumentException("View and raw namespace roots must be disjoint");
        if (this.evidenceRoot.startsWith(this.root) || this.root.startsWith(this.evidenceRoot)
                || this.evidenceRoot.startsWith(this.rawRoot) || this.rawRoot.startsWith(this.evidenceRoot))
            throw new IllegalArgumentException("Evidence root must be outside view and raw roots");
        rejectSymlinkComponents(this.root);
        rejectSymlinkComponents(this.evidenceRoot);
        if (!Files.isDirectory(this.root, LinkOption.NOFOLLOW_LINKS))
            throw new IllegalArgumentException("View namespace root must be an existing directory");
        if (!Files.isDirectory(this.evidenceRoot, LinkOption.NOFOLLOW_LINKS))
            throw new IllegalArgumentException("Evidence root must be an existing directory");
        // The raw root intentionally need not be inspectable by the unprivileged JVM.
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = options(args);
        int timeout = Integer.parseInt(options.getOrDefault("--timeout-seconds", "120"));
        if (timeout < 1 || timeout > 120) throw new IllegalArgumentException("timeout-seconds must be in 1..120");
        Thread watchdog = new Thread(() -> {
            try {
                Thread.sleep(timeout * 1000L);
                // Do not wait for a blocked filesystem call, stdout pipe, or close hook.
                Runtime.getRuntime().halt(WATCHDOG_EXIT_CODE);
            } catch (InterruptedException completed) {
                Thread.currentThread().interrupt();
            }
        }, "checkpoint-storage-probe-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
        try (CheckpointStorageIoProbe probe = new CheckpointStorageIoProbe(
                Path.of(options.get("--root")), Path.of(options.get("--raw-root")),
                Path.of(options.get("--evidence-root")), timeout)) {
            probe.serve(System.in, System.out);
        } finally {
            watchdog.interrupt();
        }
    }

    void serve(InputStream input, OutputStream output) throws IOException {
        for (int commands = 0; commands < MAX_COMMANDS && !quit; commands++) {
            String line;
            try {
                line = readCommand(input);
            } catch (IllegalArgumentException invalid) {
                writeReceipt(output, failure(0, "protocol", "rejected", invalid, Map.of()));
                return;
            }
            if (line == null) return;
            writeReceipt(output, execute(line));
        }
        if (!quit) writeReceipt(output, failure(0, "protocol", "rejected",
                new IllegalArgumentException("Command limit exceeded"), Map.of()));
    }

    Map<String, Object> execute(String command) {
        long id = 0;
        String operation = "protocol";
        Map<String, Object> detail = new LinkedHashMap<>();
        try {
            if (quit) throw new IllegalArgumentException("Session already quit");
            if (sequence >= MAX_COMMANDS) throw new IllegalArgumentException("Command limit exceeded");
            if (command.length() > MAX_COMMAND_BYTES || !command.chars().allMatch(value ->
                    value == '\t' || value >= 32 && value <= 126))
                throw new IllegalArgumentException("Commands must be bounded ASCII TSV");
            String[] fields = command.split("\t", -1);
            if (fields.length < 2 || !fields[0].matches("[1-9][0-9]{0,8}"))
                throw new IllegalArgumentException("Request ID must be a positive canonical integer");
            id = Long.parseLong(fields[0]);
            operation = fields[1];
            if (id != previousRequestId + 1) throw new IllegalArgumentException("Request IDs must increase by exactly one");
            previousRequestId = id;
            detail.put("phase", operation);
            switch (operation) {
                case "hello":
                    arity(fields, 2);
                    hello(detail);
                    break;
                case "mkdir":
                    arity(fields, 3);
                    detail.put("path", fields[2]);
                    Files.createDirectory(resolve(fields[2], false));
                    break;
                case "create":
                    arity(fields, 4);
                    detail.put("path", fields[2]);
                    create(resolve(fields[2], false), payload(fields[3]), detail);
                    break;
                case "read":
                    arity(fields, 3);
                    detail.put("path", fields[2]);
                    read(resolve(fields[2], false), detail);
                    break;
                case "open":
                    arity(fields, 4);
                    requireHandleName(fields[2]);
                    if (handles.containsKey(fields[2])) throw new IllegalArgumentException("Handle already open");
                    if (handles.size() >= MAX_HANDLES) throw new IllegalArgumentException("Open handle limit exceeded");
                    detail.put("handle", fields[2]); detail.put("path", fields[3]);
                    detail.put("phase", "open");
                    FileChannel channel = FileChannel.open(resolve(fields[3], false), StandardOpenOption.READ,
                            StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                    try {
                        detail.put("phase", "position");
                        long position = channel.size();
                        channel.position(position);
                        handles.put(fields[2], new HeldFile(fields[3], channel));
                        detail.put("position", position);
                    } catch (IOException failure) {
                        try { channel.close(); } catch (IOException close) { failure.addSuppressed(close); }
                        throw failure;
                    }
                    break;
                case "write":
                    arity(fields, 4);
                    HeldFile held = held(fields[2]);
                    byte[] value = payload(fields[3]);
                    detail.put("handle", fields[2]); detail.put("path", held.path);
                    detail.put("phase", "position");
                    detail.put("positionBefore", held.channel.position());
                    detail.put("requestedBytes", value.length);
                    detail.put("phase", "write");
                    detail.put("writtenBytes", held.channel.write(ByteBuffer.wrap(value)));
                    detail.put("phase", "position");
                    detail.put("positionAfter", held.channel.position());
                    break;
                case "force":
                    arity(fields, 4);
                    if (!Set.of("true", "false").contains(fields[3]))
                        throw new IllegalArgumentException("force metadata must be true or false");
                    HeldFile syncing = held(fields[2]);
                    detail.put("handle", fields[2]); detail.put("path", syncing.path);
                    detail.put("metadata", Boolean.parseBoolean(fields[3]));
                    syncing.channel.force(Boolean.parseBoolean(fields[3]));
                    break;
                case "close":
                    arity(fields, 3);
                    HeldFile closing = held(fields[2]);
                    detail.put("handle", fields[2]); detail.put("path", closing.path);
                    handles.remove(fields[2]);
                    closing.channel.close();
                    break;
                case "rename":
                    arity(fields, 4);
                    detail.put("path", fields[2]); detail.put("target", fields[3]);
                    Path from = resolve(fields[2], false), to = resolve(fields[3], false);
                    if (Files.exists(to, LinkOption.NOFOLLOW_LINKS))
                        throw new IllegalArgumentException("Rename target must be fresh");
                    Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
                    break;
                case "list":
                    arity(fields, 3);
                    detail.put("path", fields[2]);
                    var names = new ArrayList<String>();
                    try (DirectoryStream<Path> entries = Files.newDirectoryStream(resolve(fields[2], true))) {
                        for (Path entry : entries) {
                            if (names.size() >= MAX_DIRECTORY_ENTRIES)
                                throw new IllegalArgumentException("Directory entry limit exceeded");
                            String name = entry.getFileName().toString();
                            if (name.length() > MAX_PATH_LENGTH)
                                throw new IllegalArgumentException("Directory entry name limit exceeded");
                            names.add(name);
                        }
                    }
                    names.sort(String::compareTo);
                    detail.put("entries", names);
                    break;
                case "stat":
                    arity(fields, 3);
                    detail.put("path", fields[2]);
                    BasicFileAttributes attributes = Files.readAttributes(resolve(fields[2], true),
                            BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    detail.put("size", attributes.size()); detail.put("regularFile", attributes.isRegularFile());
                    detail.put("directory", attributes.isDirectory());
                    break;
                case "delete":
                    arity(fields, 3);
                    detail.put("path", fields[2]);
                    Files.delete(resolve(fields[2], false));
                    break;
                case "raw-read":
                    arity(fields, 3);
                    detail.put("path", fields[2]); detail.put("rawAccessProbe", true);
                    read(resolveAt(rawRoot, fields[2], false), detail);
                    break;
                case "raw-write":
                    arity(fields, 4);
                    detail.put("path", fields[2]); detail.put("rawAccessProbe", true);
                    create(resolveAt(rawRoot, fields[2], false), payload(fields[3]), detail);
                    break;
                case "evidence-write":
                    arity(fields, 4);
                    detail.put("path", fields[2]); detail.put("evidenceAccessProbe", true);
                    create(resolveAt(evidenceRoot, fields[2], false), payload(fields[3]), detail);
                    break;
                case "quit":
                    arity(fields, 2);
                    quit = true;
                    close();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation");
            }
            var receipt = receipt(id, operation, "ok");
            detail.put("phase", "complete");
            receipt.putAll(detail);
            return receipt;
        } catch (IOException failure) {
            return failure(id, operation, "io-exception", failure, detail);
        } catch (IllegalArgumentException failure) {
            return failure(id, operation, "rejected", failure, detail);
        } catch (RuntimeException failure) {
            return failure(id, operation, "error", failure, detail);
        }
    }

    private void hello(Map<String, Object> detail) throws IOException {
        detail.put("protocol", "checkpoint-storage-io-v1");
        detail.put("pid", ProcessHandle.current().pid());
        detail.put("javaVersion", System.getProperty("java.version"));
        detail.put("javaVendor", System.getProperty("java.vendor"));
        detail.put("osName", System.getProperty("os.name"));
        detail.put("osArch", System.getProperty("os.arch"));
        detail.put("osVersion", System.getProperty("os.version"));
        detail.put("timeoutSeconds", timeoutSeconds);
        detail.put("watchdogExitCode", WATCHDOG_EXIT_CODE);
        detail.put("maximumPayloadBytes", MAX_PAYLOAD_BYTES);
        Path status = Path.of("/proc/self/status");
        detail.put("linuxIdentityAvailable", Files.isRegularFile(status, LinkOption.NOFOLLOW_LINKS));
        if (Files.isRegularFile(status, LinkOption.NOFOLLOW_LINKS)) {
            byte[] bytes;
            try (InputStream input = Files.newInputStream(status, LinkOption.NOFOLLOW_LINKS)) {
                bytes = input.readNBytes(4097);
            }
            if (bytes.length > 4096) throw new IOException("Linux process status exceeds 4 KiB");
            for (String line : new String(bytes, StandardCharsets.US_ASCII).split("\n")) {
                if (line.startsWith("Uid:") || line.startsWith("Gid:")) {
                    String[] ids = line.substring(4).strip().split("\\s+");
                    if (ids.length != 4) throw new IOException("Unexpected Linux process identity shape");
                    var parsed = new ArrayList<Long>();
                    for (String id : ids) parsed.add(Long.parseLong(id));
                    detail.put(line.startsWith("Uid:") ? "uid" : "gid", parsed);
                } else if (line.startsWith("CapEff:")) detail.put("capEff", line.substring(7).strip());
            }
            if (!detail.containsKey("uid") || !detail.containsKey("gid") || !detail.containsKey("capEff"))
                throw new IOException("Linux process identity is incomplete");
        }
    }

    private static void create(Path path, byte[] value, Map<String, Object> detail) throws IOException {
        detail.put("requestedBytes", value.length);
        detail.put("phase", "open");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            detail.put("phase", "write");
            detail.put("writtenBytes", channel.write(ByteBuffer.wrap(value)));
            detail.put("phase", "close");
        }
    }

    private static void read(Path path, Map<String, Object> detail) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        detail.put("phase", "open");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            detail.put("phase", "read");
            ByteBuffer buffer = ByteBuffer.allocate(MAX_READ_BYTES + 1);
            int stalled = 0;
            while (buffer.hasRemaining()) {
                int count = channel.read(buffer);
                if (count < 0) break;
                if (count == 0 && ++stalled > 16) throw new IOException("Read made no progress");
            }
            if (!buffer.hasRemaining()) throw new IllegalArgumentException("Read byte limit exceeded");
            result.write(buffer.array(), 0, buffer.position());
            detail.put("phase", "close");
        }
        byte[] bytes = result.toByteArray();
        detail.put("readBytes", bytes.length);
        detail.put("sha256", sha256(bytes));
        detail.put("base64", Base64.getEncoder().encodeToString(bytes));
    }

    private Path resolve(String value, boolean allowRoot) throws IOException {
        return resolveAt(root, value, allowRoot);
    }

    private static Path resolveAt(Path root, String value, boolean allowRoot) throws IOException {
        Path path = root.resolve(relative(value, allowRoot));
        rejectSymlinkComponents(path);
        return path;
    }

    private static Path relative(String value, boolean allowRoot) {
        if (allowRoot && value.equals("@")) return Path.of("");
        if (value.isEmpty() || value.length() > MAX_PATH_LENGTH || !value.matches("[A-Za-z0-9._/-]+"))
            throw new IllegalArgumentException("Invalid relative namespace path");
        Path path = Path.of(value);
        if (path.isAbsolute()) throw new IllegalArgumentException("Absolute paths are forbidden");
        for (String component : value.split("/", -1)) {
            if (component.isEmpty() || component.equals(".") || component.equals(".."))
                throw new IllegalArgumentException("Empty, dot and parent path components are forbidden");
        }
        return path;
    }

    private static void rejectSymlinkComponents(Path path) throws IOException {
        Path current = path.getRoot();
        for (Path name : path) {
            current = current.resolve(name);
            if (Files.isSymbolicLink(current)) throw new IllegalArgumentException("Symbolic links are forbidden");
        }
    }

    private HeldFile held(String handle) {
        requireHandleName(handle);
        HeldFile file = handles.get(handle);
        if (file == null) throw new IllegalArgumentException("Unknown open handle");
        return file;
    }

    private static void requireHandleName(String handle) {
        if (!handle.matches("[a-z][a-z0-9-]{0,31}")) throw new IllegalArgumentException("Invalid handle name");
    }

    private static byte[] payload(String encoded) {
        if (encoded.length() > ((MAX_PAYLOAD_BYTES + 2) / 3) * 4)
            throw new IllegalArgumentException("Payload byte limit exceeded");
        byte[] value = Base64.getDecoder().decode(encoded);
        if (value.length == 0 || value.length > MAX_PAYLOAD_BYTES
                || !Base64.getEncoder().encodeToString(value).equals(encoded))
            throw new IllegalArgumentException("Payload must be canonical base64 for 1..4096 bytes");
        return value;
    }

    private static void arity(String[] fields, int length) {
        if (fields.length != length) throw new IllegalArgumentException("Wrong number of command fields");
    }

    private Map<String, Object> receipt(long id, String operation, String status) {
        var result = new LinkedHashMap<String, Object>();
        result.put("protocol", "checkpoint-storage-io-v1");
        result.put("sequence", ++sequence);
        result.put("requestId", id);
        result.put("operation", operation);
        result.put("status", status);
        result.put("timestamp", Instant.now().toString());
        return result;
    }

    private Map<String, Object> failure(long id, String operation, String status, Exception failure,
                                       Map<String, Object> detail) {
        var result = receipt(id, operation, status);
        result.putAll(detail);
        result.put("exceptionClass", failure.getClass().getName());
        String message = failure.getMessage();
        result.put("exceptionMessage", message == null ? null : message.substring(0, Math.min(1024, message.length())));
        result.put("exceptionMessageTruncated", message != null && message.length() > 1024);
        return result;
    }

    private static String readCommand(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int value; (value = input.read()) >= 0;) {
            if (value == '\n') {
                byte[] line = bytes.toByteArray();
                int length = line.length > 0 && line[line.length - 1] == '\r' ? line.length - 1 : line.length;
                return new String(line, 0, length, StandardCharsets.US_ASCII);
            }
            if (bytes.size() >= MAX_COMMAND_BYTES) throw new IllegalArgumentException("Command byte limit exceeded");
            if (value != '\t' && value != '\r' && (value < 32 || value > 126))
                throw new IllegalArgumentException("Commands must be ASCII TSV");
            bytes.write(value);
        }
        if (bytes.size() != 0) throw new IllegalArgumentException("Incomplete final command line");
        return null;
    }

    private static void writeReceipt(OutputStream output, Map<String, Object> receipt) throws IOException {
        output.write((json(receipt) + "\n").getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?>) {
            var parts = new ArrayList<String>();
            for (var entry : ((Map<?, ?>) value).entrySet()) parts.add(json(entry.getKey()) + ":" + json(entry.getValue()));
            return "{" + String.join(",", parts) + "}";
        }
        if (value instanceof List<?>) {
            var parts = new ArrayList<String>();
            for (Object item : (List<?>) value) parts.add(json(item));
            return "[" + String.join(",", parts) + "]";
        }
        String text = value.toString();
        StringBuilder escaped = new StringBuilder("\"");
        for (int index = 0; index < text.length(); index++) {
            char character = text.charAt(index);
            if (character == '\\' || character == '"') escaped.append('\\').append(character);
            else if (character < 32) escaped.append(String.format("\\u%04x", (int) character));
            else escaped.append(character);
        }
        return escaped.append('"').toString();
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder result = new StringBuilder();
            for (byte value : hash) result.append(String.format("%02x", value & 255));
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static Map<String, String> options(String[] args) {
        var result = new HashMap<String, String>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 == args.length || !Set.of("--root", "--raw-root", "--evidence-root", "--timeout-seconds").contains(args[index])
                    || result.putIfAbsent(args[index], args[index + 1]) != null)
                throw new IllegalArgumentException("Expected unique --root, --raw-root, --evidence-root and optional --timeout-seconds arguments");
        }
        if (!result.containsKey("--root") || !result.containsKey("--raw-root") || !result.containsKey("--evidence-root"))
            throw new IllegalArgumentException("--root, --raw-root and --evidence-root are required");
        return result;
    }

    @Override public void close() throws IOException {
        IOException failure = null;
        for (HeldFile file : handles.values()) {
            try { file.channel.close(); }
            catch (IOException closeFailure) {
                if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
            }
        }
        handles.clear();
        if (failure != null) throw failure;
    }

    private static final class HeldFile {
        private final String path;
        private final FileChannel channel;
        private HeldFile(String path, FileChannel channel) { this.path = path; this.channel = channel; }
    }
}
