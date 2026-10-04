package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

/** First matching text (or explicit absence) for each marker and physical process incarnation. */
public record FlinkLogMarkerEvidence(List<FlinkLogMarker> declarations, List<Observation> observations,
                                     List<String> diagnostics, boolean requiredConfirmed) {
    public static final String MISSING = "subject.flink.log-marker-missing";
    public FlinkLogMarkerEvidence {
        declarations = FlinkLogMarker.validate(declarations);
        observations = List.copyOf(observations);
        diagnostics = List.copyOf(diagnostics);
    }
    public record Observation(String name, String logicalName, String runtimeId, String process,
                              String logPath, String firstMatch, String matchedText, String absence, boolean required) {}

    public static FlinkLogMarkerEvidence collect(List<FlinkLogMarker> markers,
            List<FlinkComponentProvisioningEvidence> provisioning, List<FlinkComponentLog> logs, boolean fenced) {
        var observations = new ArrayList<Observation>();
        var diagnostics = new ArrayList<String>();
        var byProcess = new HashMap<String, List<FlinkComponentLog>>();
        for (var log : logs) byProcess.computeIfAbsent(log.process(), unused -> new ArrayList<>()).add(log);
        var sharedOutputProcesses = sharedOutputProcesses(logs, fenced);
        boolean confirmed = true;
        var bindings = new HashMap<String, Integer>();
        for (var component : provisioning) component.classLoadProcess().or(() -> component.runtimeJarEvidence()
                .map(FlinkComponentProvisioningEvidence.RuntimeJarEvidence::classLoadProcess))
                .ifPresent(process -> bindings.merge(process, 1, Integer::sum));
        long bytesLeft = 32L * 1024 * 1024;
        for (var component : provisioning) {
            List<FlinkLogMarker> selected = markers.stream().filter(m -> m.role() == component.role()).toList();
            if (selected.isEmpty()) continue;
            String process = component.classLoadProcess().orElseGet(() -> component.runtimeJarEvidence()
                    .map(FlinkComponentProvisioningEvidence.RuntimeJarEvidence::classLoadProcess).orElse(null));
            var matching = byProcess.getOrDefault(process, List.of());
            String path = matching.size() == 1 ? matching.getFirst().path().toString() : null;
            String text = null;
            String absence = !fenced ? "Process fence is unconfirmed" : process == null ? "Missing registered process log key"
                    : bindings.getOrDefault(process, 0) != 1 ? "Several incarnations claim one process output"
                    : sharedOutputProcesses.contains(process) ? "Several process keys share one retained output file"
                    : matching.size() != 1 ? "Missing or duplicate registered process output" : null;
            if (absence == null) {
                var log = matching.getFirst();
                try {
                    if (!Files.isRegularFile(log.path(), LinkOption.NOFOLLOW_LINKS)) throw new java.io.IOException("Not a regular retained log");
                    int limit = (int)Math.min(bytesLeft, FlinkComponentLog.MAX_BYTES);
                    try (var input = Files.newInputStream(log.path(), LinkOption.NOFOLLOW_LINKS)) {
                        byte[] bytes = input.readNBytes(limit + 1);
                        bytesLeft -= Math.min(limit, bytes.length);
                        if (bytes.length > limit) throw new java.io.IOException("Log read budget exceeded");
                        text = new String(bytes, StandardCharsets.UTF_8);
                    }
                    if (log.error().isPresent()) { absence = "Log capture error: " + log.error().orElseThrow(); text = null; }
                    else if (log.truncated()) {
                        absence = "Log capture was truncated";
                        diagnostics.add(process + ": matching is limited to complete lines in the retained prefix");
                        if (!text.endsWith("\n")) text = text.substring(0, text.lastIndexOf('\n') + 1);
                    }
                } catch (java.io.IOException | RuntimeException failure) { absence = "Output unavailable: " + failure; }
            }
            for (var marker : selected) {
                String match = null;
                String matchedText = null;
                String missing = absence;
                if (text != null) {
                    try {
                        var pattern = Pattern.compile(marker.regex());
                        long deadline = System.nanoTime() + 1_000_000_000L;
                        var lines = text.lines().iterator();
                        while (lines.hasNext()) {
                            String line = lines.next();
                            var matcher = pattern.matcher(new BoundedText(line, deadline));
                            if (matcher.find()) { match = line; matchedText = matcher.group(); missing = null; break; }
                        }
                        if (match == null && missing == null) missing = "No match";
                    } catch (RuntimeException | StackOverflowError failure) { missing = "Regex evaluation unavailable: " + failure.getClass().getSimpleName(); }
                }
                if (marker.required() && match == null) confirmed = false;
                observations.add(new Observation(marker.name(), component.logicalName(), component.runtimeId(),
                        process, path, match, matchedText, missing, marker.required()));
            }
        }
        for (var log : logs) if (!bindings.containsKey(log.process())) {
            for (var marker : markers) if (log.process().startsWith(marker.scope() + "-")) {
                observations.add(new Observation(marker.name(), null, null, log.process(), log.path().toString(), null, null,
                        "Registered process output has no successful provisioning identity", marker.required()));
                if (marker.required()) confirmed = false;
            }
        }
        for (var marker : markers) {
            if (provisioning.stream().noneMatch(component -> component.role() == marker.role())) {
                diagnostics.add(marker.name() + ": no provisioned " + marker.scope() + " incarnation");
                if (marker.required()) confirmed = false;
            }
        }
        return new FlinkLogMarkerEvidence(markers, observations, diagnostics, confirmed);
    }

    /** A replacement must prove its own output, even when registrations use aliases for one file. */
    private static Set<String> sharedOutputProcesses(List<FlinkComponentLog> logs, boolean fenced) {
        var byPath = new HashMap<Path, Set<String>>();
        var byFileIdentity = new HashMap<Object, Set<String>>();
        for (var log : logs) {
            byPath.computeIfAbsent(log.path().toAbsolutePath().normalize(), unused -> new HashSet<>()).add(log.process());
            if (fenced) {
                try {
                    var attributes = Files.readAttributes(log.path(), BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (attributes.isRegularFile() && attributes.fileKey() != null)
                        byFileIdentity.computeIfAbsent(attributes.fileKey(), unused -> new HashSet<>()).add(log.process());
                } catch (java.io.IOException | RuntimeException ignored) {
                    // The normal read path records unavailable output; it cannot establish a match.
                }
            }
        }
        var shared = new HashSet<String>();
        for (var processes : byPath.values()) if (processes.size() > 1) shared.addAll(processes);
        for (var processes : byFileIdentity.values()) if (processes.size() > 1) shared.addAll(processes);
        return shared;
    }

    /** Java regex does not honor interruption; its input enforces a bounded matching budget. */
    private record BoundedText(String text, long deadline) implements CharSequence {
        private void check() { if (System.nanoTime() > deadline) throw new IllegalStateException("Regex time budget exceeded"); }
        public int length() { check(); return text.length(); }
        public char charAt(int index) { check(); return text.charAt(index); }
        public CharSequence subSequence(int start, int end) { check(); return text.substring(start, end); }
        @Override public String toString() { check(); return text; }
    }
}
