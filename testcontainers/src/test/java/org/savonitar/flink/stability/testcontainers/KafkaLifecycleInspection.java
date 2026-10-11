package org.savonitar.flink.stability.testcontainers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.containers.Container;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

/** Test-only evidence collection: persist observations before validating their contents. */
final class KafkaLifecycleInspection {
    private static final ObjectMapper JSON = new ObjectMapper();
    private KafkaLifecycleInspection() {}

    @FunctionalInterface interface Transfer { void copy(OutputStream output) throws Exception; }

    static void record(Path output, String name, Map<String, ?> values) throws IOException {
        var result = new LinkedHashMap<String, Object>();
        result.put("wall", Instant.now().toString()); result.put("monotonicNanos", System.nanoTime()); result.putAll(values);
        Path partial = output.resolve(name + ".json.partial");
        JSON.writerWithDefaultPrettyPrinter().writeValue(partial.toFile(), result);
        Files.move(partial, output.resolve(name + ".json"), StandardCopyOption.ATOMIC_MOVE);
    }

    /** Preserve exact entrypoint representation: null and an empty array are distinct observations. */
    static void launch(Path output, String[] expectedEntrypoint, String[] observedEntrypoint,
                       String[] command, Map<String, String> ownedEnvironment) throws IOException {
        var receipt = new LinkedHashMap<String, Object>();
        receipt.put("expectedEntrypoint", expectedEntrypoint); receipt.put("entrypoint", observedEntrypoint);
        receipt.put("entrypointPolicy", "exact-null-and-empty-distinct");
        receipt.put("command", command); receipt.put("ownedEnvironment", ownedEnvironment);
        record(output, "created-launch", receipt);
        if (!Arrays.equals(expectedEntrypoint, observedEntrypoint))
            throw new IOException("Created container entrypoint differs from the selected launch configuration");
    }

    static byte[] transfer(Path output, String name, String artifact, String source, Transfer transfer) throws Exception {
        Path raw = output.resolve(artifact);
        try {
            try (var sink = Files.newOutputStream(raw, StandardOpenOption.CREATE_NEW)) { transfer.copy(sink); }
        } catch (Exception | AssertionError failure) {
            var receipt = failure(failure);
            receipt.put("source", source);
            receipt.put("bytesCaptured", Files.exists(raw) ? Files.size(raw) : 0);
            record(output, name + ".transfer", receipt);
            throw failure;
        }
        record(output, name + ".transfer", Map.of(
                "status", "completed", "source", source, "bytesCaptured", Files.size(raw)));
        return Files.readAllBytes(raw);
    }

    static <T> T observation(Path output, String name, Callable<T> call,
                             Function<T, Map<String, ?>> details) throws Exception {
        T value;
        try { value = call.call(); }
        catch (Exception | AssertionError failure) {
            record(output, name, failure(failure));
            throw failure;
        }
        var receipt = new LinkedHashMap<String, Object>();
        receipt.put("status", "completed"); receipt.putAll(details.apply(value));
        record(output, name, receipt);
        return value;
    }

    static String command(Path output, String name, List<String> command, Callable<Container.ExecResult> call) throws Exception {
        Container.ExecResult result;
        try { result = call.call(); }
        catch (Exception | AssertionError failure) {
            var receipt = failure(failure); receipt.put("command", command);
            record(output, name + ".command", receipt);
            throw failure;
        }
        record(output, name + ".command", Map.of(
                "status", "completed", "command", command, "exitCode", result.getExitCode()));
        Files.writeString(output.resolve(name + ".stdout"), result.getStdout(), StandardOpenOption.CREATE_NEW);
        Files.writeString(output.resolve(name + ".stderr"), result.getStderr(), StandardOpenOption.CREATE_NEW);
        if (result.getExitCode() != 0) throw new IOException(name + " exited " + result.getExitCode());
        return result.getStdout();
    }

    static Properties brokerProperties(byte[] raw, Set<String> selected) throws IOException {
        var actual = new Properties(); actual.load(new ByteArrayInputStream(raw));
        var result = new Properties();
        for (String key : selected) if (actual.containsKey(key)) result.setProperty(key, actual.getProperty(key));
        return result;
    }

    static String processIdentity(String status, String stat, String command, String namespace) throws IOException {
        var values = new LinkedHashMap<String, String>();
        for (String line : status.lines().toList()) {
            int colon = line.indexOf(':');
            if (colon > 0) values.put(line.substring(0, colon), line.substring(colon + 1).trim());
        }
        if (!"java".equals(values.get("Name")) || !"1".equals(values.get("Pid")))
            throw new IOException("Namespace PID 1 is not Java");
        // /proc stat's parenthesized comm may itself contain spaces and parentheses.
        int close = stat.lastIndexOf(')');
        if (!stat.startsWith("1 (") || close < 3) throw new IOException("Malformed PID 1 stat");
        String[] fields = stat.substring(close + 1).trim().split("\\s+");
        if (fields.length < 20 || !fields[19].matches("[0-9]+")) throw new IOException("Missing process start time");
        String pidNamespace = namespace.strip();
        if (!pidNamespace.matches("pid:\\[[0-9]+]")) throw new IOException("Malformed PID namespace identity");
        String commandLine = command.replace('\0', ' ');
        if (!commandLine.contains("kafka.Kafka")) throw new IOException("PID 1 command is not Kafka");
        StringBuilder result = new StringBuilder("namespace_pid=1\n");
        for (String key : List.of("Name", "State", "Pid", "PPid", "NSpid", "SigIgn", "SigCgt"))
            if (values.containsKey(key)) result.append(key).append(":\t").append(values.get(key)).append('\n');
        return result.append("starttime_ticks=").append(fields[19]).append('\n')
                .append("pid_namespace=").append(pidNamespace).append('\n')
                .append("cmdline=").append(commandLine).append('\n').toString();
    }

    private static Map<String, Object> failure(Throwable failure) {
        var result = new LinkedHashMap<String, Object>();
        result.put("status", "failed"); result.put("errorType", failure.getClass().getName());
        result.put("errorMessage", String.valueOf(failure.getMessage()));
        var trace = new StringWriter(); failure.printStackTrace(new PrintWriter(trace));
        result.put("error", trace.toString());
        return result;
    }
}
