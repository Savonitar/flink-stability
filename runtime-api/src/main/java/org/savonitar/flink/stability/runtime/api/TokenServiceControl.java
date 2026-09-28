package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Controls only the local synthetic credential fixture; never a real token service. */
public interface TokenServiceControl {
    enum Mode { HEALTHY, DELAY, FAIL, LINKAGE_ERROR }
    enum Kind { MODE_CHANGED, PROVIDER_INITIALIZED, RECEIVER_INITIALIZED,
        REQUEST_STARTED, ISSUED, FAILED, FAULT_OBSERVED, REQUEST_FINISHED, RECEIVED, REJECTED }

    /** Returns the revision that subsequent acquisition requests observe. */
    long configure(Mode mode, Duration delay);

    Snapshot snapshot();

    record Event(long sequence, Kind kind, String process, String role,
                 long timestampMillis, long monotonicNanos, long requestId,
                 long revision, Mode mode, OptionalLong tokenSequence, String detail) {
        public Event {
            if (sequence < 1 || requestId < 0 || revision < 0) {
                throw new IllegalArgumentException("Invalid synthetic token event identity");
            }
            Objects.requireNonNull(kind, "kind");
            Checks.requireNonBlank(process, "process");
            Checks.requireNonBlank(role, "role");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(tokenSequence, "tokenSequence");
            Objects.requireNonNull(detail, "detail");
        }
    }

    /** Overflow or saturation invalidates evidence; events are never silently overwritten. */
    record Snapshot(List<Event> events, boolean overflow, boolean saturated,
                    int activeRequests, int maxConcurrentRequests) {
        public Snapshot {
            events = List.copyOf(Objects.requireNonNull(events, "events"));
            if (activeRequests < 0 || maxConcurrentRequests < activeRequests) {
                throw new IllegalArgumentException("Invalid synthetic token concurrency counters");
            }
        }
    }
}
