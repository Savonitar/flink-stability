package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.ImageConnectorArtifact;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Which JAR each Flink process loaded the subject connector's entry classes from, read from the
 * JVM class-load logs after the process fence (SPEC-001 R5.6d). This is the runtime proof that
 * the job ran the subject's code, not only that its bytes were installed.
 */
public record SubjectClassOrigins(
        String expectedSource,
        List<ProcessOrigin> processes,
        Optional<String> failure) {
    private static final Pattern CLASS_LOAD = Pattern.compile(
            "\\[class,load\\] (\\S+) source: (\\S+)");
    private static final Pattern HIDDEN_CLASS = Pattern.compile("[^/]+/0x[0-9a-fA-F]+");
    private static final Pattern HIDDEN_LAMBDA = Pattern.compile("(.+)\\$\\$Lambda(?:\\$[0-9]+)?/0x[0-9a-fA-F]+");
    private static final String FILE_URL = "file:";

    public SubjectClassOrigins {
        Objects.requireNonNull(expectedSource, "expectedSource");
        processes = List.copyOf(Objects.requireNonNull(processes, "processes"));
        Objects.requireNonNull(failure, "failure");
    }

    public enum Outcome {
        /** Every load came from the subject primary, and a TaskManager loaded every class. */
        CONFIRMED,
        /** Some process loaded an entry class from anywhere but the subject primary. */
        MISMATCH,
        /** The logs are unreadable, or no TaskManager loaded the entry classes. */
        UNCONFIRMED
    }

    /** One process incarnation and the sources it loaded each entry class from. */
    public record ProcessOrigin(String process, Map<String, List<String>> sources) {
        public ProcessOrigin {
            Objects.requireNonNull(process, "process");
            Map<String, List<String>> copy = new LinkedHashMap<>();
            Objects.requireNonNull(sources, "sources")
                    .forEach((entryClass, found) -> copy.put(entryClass, List.copyOf(found)));
            sources = java.util.Collections.unmodifiableMap(copy);
        }

        private boolean taskManager() {
            return process.startsWith("taskmanager-");
        }
    }

    /** Reads the entry-class lines of every log; a read failure becomes evidence. */
    public static SubjectClassOrigins read(
            List<FlinkClassLoadLog> logs,
            List<String> entryClasses,
            String expectedSource) {
        return read(logs, entryClasses, expectedSource, false);
    }

    /**
     * Image subjects must not mix a verified entry point with implementation classes from an
     * auxiliary classpath. Retain every observed current/legacy connector class, while callers
     * still require each TaskManager to have loaded both protocol entry classes.
     */
    public static SubjectClassOrigins readImageSubject(
            List<FlinkClassLoadLog> logs,
            List<String> entryClasses,
            String expectedSource) {
        return read(logs, entryClasses, expectedSource, true);
    }

    private static SubjectClassOrigins read(
            List<FlinkClassLoadLog> logs,
            List<String> entryClasses,
            String expectedSource,
            boolean includeConnectorImplementations) {
        Objects.requireNonNull(logs, "logs");
        Set<String> wanted = Set.copyOf(entryClasses);
        List<ProcessOrigin> processes = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        for (FlinkClassLoadLog log : logs) {
            try {
                processes.add(new ProcessOrigin(log.process(),
                        sources(log, wanted, includeConnectorImplementations)));
            } catch (IOException | UncheckedIOException unreadable) {
                failures.add(log.process() + ": " + unreadable.getMessage());
            }
        }
        return new SubjectClassOrigins(expectedSource, processes, failures.isEmpty()
                ? Optional.empty()
                : Optional.of("Cannot read Flink class-load logs: " + String.join("; ", failures)));
    }

    public Outcome outcome(List<String> entryClasses) {
        if (failure.isPresent()) {
            return Outcome.UNCONFIRMED;
        }
        boolean foreignSource = processes.stream()
                .flatMap(process -> process.sources().values().stream())
                .flatMap(List::stream)
                .anyMatch(source -> !source.equals(expectedSource));
        if (foreignSource) {
            return Outcome.MISMATCH;
        }
        boolean ranOnTaskManager = processes.stream().anyMatch(process -> process.taskManager()
                && entryClasses.stream().allMatch(entryClass ->
                        !process.sources().getOrDefault(entryClass, List.of()).isEmpty()));
        return ranOnTaskManager ? Outcome.CONFIRMED : Outcome.UNCONFIRMED;
    }

    /** Image subjects require proof from every provisioned TaskManager incarnation, including replacements. */
    public SubjectClassOrigins requireEveryTaskManager(
            List<FlinkComponentProvisioningEvidence> provisioning,
            List<String> entryClasses,
            ImageConnectorArtifact expected) {
        if (failure.isPresent()) return this;
        Map<String, ProcessOrigin> byProcess = new LinkedHashMap<>();
        for (ProcessOrigin process : processes) {
            if (byProcess.putIfAbsent(process.process(), process) != null) {
                return missingIncarnation("Duplicate class-load process " + process.process());
            }
        }
        Set<String> required = new LinkedHashSet<>();
        for (var component : provisioning) {
            if (component.role() != FlinkComponentRole.TASK_MANAGER) continue;
            if (!component.imageConnectorArtifacts().contains(expected) || component.classLoadProcess().isEmpty()) {
                return missingIncarnation("TaskManager " + component.runtimeId() + " lacks verified image connector bytes or log binding");
            }
            String process = component.classLoadProcess().orElseThrow();
            if (!required.add(process)) return missingIncarnation("Several TaskManagers claim class-load process " + process);
            ProcessOrigin observed = byProcess.get(process);
            if (observed == null || entryClasses.stream().anyMatch(name ->
                    observed.sources().getOrDefault(name, List.of()).isEmpty())) {
                return missingIncarnation("TaskManager incarnation " + process + " did not show every subject entry class");
            }
        }
        if (required.isEmpty()) return missingIncarnation("No provisioned TaskManager establishes image connector use");
        if (processes.stream().filter(ProcessOrigin::taskManager).anyMatch(process -> !required.contains(process.process()))) {
            return missingIncarnation("TaskManager class-load log has no provisioning identity");
        }
        return this;
    }

    private SubjectClassOrigins missingIncarnation(String message) {
        return new SubjectClassOrigins(expectedSource, processes, Optional.of(message));
    }

    /** One sentence explaining the outcome, for the attempt message. */
    public String detail(List<String> entryClasses) {
        return switch (outcome(entryClasses)) {
            case CONFIRMED -> "every observed entry-class load came from " + expectedSource
                    + " and one TaskManager loaded every entry class";
            case MISMATCH -> "a Flink process loaded a subject entry class from "
                    + processes.stream()
                            .flatMap(process -> process.sources().entrySet().stream()
                                    .flatMap(entry -> entry.getValue().stream()
                                            .filter(source -> !source.equals(expectedSource))
                                            .map(source -> process.process() + " loaded "
                                                    + entry.getKey() + " from " + source)))
                            .findFirst()
                            .orElseThrow()
                    + " instead of " + expectedSource;
            case UNCONFIRMED -> failure.orElse(
                    "no single TaskManager log shows every subject entry class being loaded ("
                            + processes.size() + " class-load logs)");
        };
    }

    private static Map<String, List<String>> sources(
            FlinkClassLoadLog log,
            Set<String> wanted,
            boolean includeConnectorImplementations) throws IOException {
        Map<String, Set<String>> found = new LinkedHashMap<>();
        Set<String> requiredLambdaHosts = new LinkedHashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(
                log.hostPath(), StandardCharsets.UTF_8)) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                Matcher matcher = CLASS_LOAD.matcher(line);
                if (matcher.find() && (wanted.contains(matcher.group(1))
                        || (includeConnectorImplementations && ImageConnectorArtifact.connectorClassEntry(
                                matcher.group(1).replace('.', '/') + ".class")))) {
                    String source = matcher.group(2);
                    if (includeConnectorImplementations && hiddenDefinition(
                            matcher.group(1), source, requiredLambdaHosts)) {
                        continue;
                    }
                    found.computeIfAbsent(matcher.group(1), ignored -> new LinkedHashSet<>())
                            .add(source.startsWith(FILE_URL)
                                    ? source.substring(FILE_URL.length())
                                    : source);
                }
            }
        }
        for (String host : requiredLambdaHosts) {
            if (!found.containsKey(host)) {
                throw new IOException("Hidden connector lambda has no declaring-class origin: " + host);
            }
        }
        Map<String, List<String>> sources = new LinkedHashMap<>();
        found.forEach((entryClass, paths) -> sources.put(entryClass, List.copyOf(paths)));
        return sources;
    }

    /** Generated hidden bytes have no JAR origin; their concrete declaring classes still do. */
    private static boolean hiddenDefinition(String name, String source, Set<String> requiredHosts) {
        Matcher lambda = HIDDEN_LAMBDA.matcher(name);
        if (lambda.matches() && ("__JVM_LookupDefineClass__".equals(source)
                || lambda.group(1).equals(source) || lambda.group(1).startsWith(source + "$"))) {
            requiredHosts.add(lambda.group(1));
            if (!"__JVM_LookupDefineClass__".equals(source)) requiredHosts.add(source);
            return true;
        }
        return "__JVM_LookupDefineClass__".equals(source) && HIDDEN_CLASS.matcher(name).matches();
    }
}
