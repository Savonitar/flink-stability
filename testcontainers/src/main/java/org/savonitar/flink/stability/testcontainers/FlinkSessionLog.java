package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/** Bounded session messages from one Flink container's stdout, never the harness observer. */
final class FlinkSessionLog {
    static final int MAX_SESSIONS = 128;
    static final int MAX_LINE_CHARS = 8192;
    private static final Pattern NEGOTIATED = Pattern.compile(
            "Session establishment complete on server .*?, session id = (0x[0-9a-fA-F]+), negotiated timeout = ([0-9]+)");
    private final String logicalName;
    private final FlinkComponentRole role;
    private final String process;
    private final long requested;
    private final List<FlinkHaControl.NegotiatedSession> sessions = new ArrayList<>();
    private final StringBuilder pending = new StringBuilder();
    private String runtimeId;
    private boolean overflow;
    private boolean discardLine;
    private boolean ambiguousContainer;

    FlinkSessionLog(String logicalName, FlinkComponentRole role, String process, long requested) {
        this.logicalName = logicalName;
        this.role = role;
        this.process = process;
        this.requested = requested;
    }

    synchronized void created(String id) {
        if (runtimeId != null && !runtimeId.equals(id)) {
            // A retried physical creation must not inherit an earlier consumer's attribution.
            ambiguousContainer = true;
            overflow = true;
        } else {
            runtimeId = id;
        }
    }

    synchronized void accept(String text) {
        if (text == null || ambiguousContainer) return;
        for (int index = 0; index < text.length(); index++) {
            char character = text.charAt(index);
            if (character == '\n') {
                if (!discardLine) line(pending.toString());
                pending.setLength(0);
                discardLine = false;
            } else if (!discardLine && character != '\r') {
                if (pending.length() == MAX_LINE_CHARS) {
                    overflow = true;
                    discardLine = true;
                    pending.setLength(0);
                } else pending.append(character);
            }
        }
    }

    private void line(String line) {
        if (!line.contains("org.apache.flink.shaded.zookeeper") || !line.contains("ClientCnxn")) return;
        var match = NEGOTIATED.matcher(line);
        if (!match.find()) return;
        if (sessions.size() == MAX_SESSIONS || runtimeId == null) {
            overflow = true;
            return;
        }
        try {
            sessions.add(new FlinkHaControl.NegotiatedSession(match.group(1),
                    Long.parseLong(match.group(2)), line));
        } catch (IllegalArgumentException invalid) {
            overflow = true;
        }
    }

    synchronized Optional<FlinkHaControl.SessionEvidence> snapshot() {
        return runtimeId == null ? Optional.empty() : Optional.of(new FlinkHaControl.SessionEvidence(
                logicalName, role, runtimeId, process, requested, sessions, overflow));
    }
}
