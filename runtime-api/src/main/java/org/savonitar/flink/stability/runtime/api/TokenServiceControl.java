package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Controls only the local synthetic credential fixture; never a real token service. */
public interface TokenServiceControl {
    enum Mode { HEALTHY, DELAY, FAIL, LINKAGE_ERROR }
    enum Kind { MODE_CHANGED, PROVIDER_INITIALIZED, RECEIVER_INITIALIZED,
        REQUEST_STARTED, ISSUED, FAILED, FAULT_OBSERVED, REQUEST_FINISHED, RECEIVED, REJECTED }

    /** Returns the revision that subsequent acquisition requests observe. */
    long configure(Mode mode, Duration delay);

    default long configure(Mode mode, Duration delay, JobTarget target) {
        throw new UnsupportedOperationException("Submitted-job token faults are unsupported");
    }

    record JobTarget(String jobId, String jobAlias) {
        public JobTarget {
            if (jobId == null || !jobId.matches("[0-9a-f]{32}")) {
                throw new IllegalArgumentException("Require the submitted Flink JobID");
            }
            Checks.requireNonBlank(jobAlias, "jobAlias");
        }

        public boolean matches(Event event) {
            return event.registration().filter(context -> "JOB".equals(context.scope())
                    && jobId.equals(context.jobId()) && jobAlias.equals(context.jobAlias())
                    && !context.coverageInvalid()).isPresent();
        }
    }

    Snapshot snapshot();

    record Event(long sequence, Kind kind, String process, String role,
                 long timestampMillis, long monotonicNanos, long requestId,
                 long revision, Mode mode, OptionalLong tokenSequence, String detail,
                 Optional<RegistrationSnapshot> registration, Optional<String> participantInstance) {
        /** Existing service-wide traces remain meaningful when per-job context is absent. */
        public Event(long sequence, Kind kind, String process, String role,
                     long timestampMillis, long monotonicNanos, long requestId,
                     long revision, Mode mode, OptionalLong tokenSequence, String detail) {
            this(sequence, kind, process, role, timestampMillis, monotonicNanos, requestId,
                    revision, mode, tokenSequence, detail, Optional.empty(), Optional.empty());
        }

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
            Objects.requireNonNull(registration, "registration");
            Objects.requireNonNull(participantInstance, "participantInstance");
            participantInstance.ifPresent(value -> Checks.requireNonBlank(value, "participantInstance"));
        }
    }

    /** Provider-local occurrence order; service ingestion time is deliberately separate. */
    record Lifecycle(long sequence, String kind, long generation, String jobId, String jobAlias) {
        public Lifecycle {
            if (sequence < 1 || generation < 0
                    || !List.of("REGISTER", "UNREGISTER", "CLOSE", "INVALID").contains(kind)) {
                throw new IllegalArgumentException("Invalid lifecycle record");
            }
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(jobAlias, "jobAlias");
        }
    }

    /** Immutable state captured before a request's delay/failure is selected. */
    record RegistrationSnapshot(String providerInstance, String scope, long generation,
                                String jobId, String jobAlias, boolean coverageInvalid,
                                long acknowledgedSequence, List<Lifecycle> journal) {
        public RegistrationSnapshot {
            Objects.requireNonNull(providerInstance, "providerInstance");
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(jobAlias, "jobAlias");
            journal = List.copyOf(journal);
            if (generation < 0 || acknowledgedSequence < 0 || journal.size() > 32) {
                throw new IllegalArgumentException("Invalid registration snapshot");
            }
            long expected = acknowledgedSequence;
            for (Lifecycle record : journal) {
                if (expected == Long.MAX_VALUE || record.sequence() != ++expected) {
                    throw new IllegalArgumentException("Lifecycle journal is not a contiguous prefix");
                }
            }
        }

        public long sentThrough() {
            return journal.isEmpty() ? acknowledgedSequence : journal.get(journal.size() - 1).sequence();
        }
    }

    /** One complete request, shared by the online fault hold and final evidence check. */
    static boolean completedFault(List<Event> events, Event start, Mode mode, Duration delay) {
        if (mode == Mode.HEALTHY) return false;
        return events.stream().anyMatch(outcome -> sameRequest(start, outcome)
                && outcome.sequence() > start.sequence()
                && (mode == Mode.DELAY ? outcome.kind() == Kind.ISSUED
                    && outcome.monotonicNanos() - start.monotonicNanos() >= delay.toNanos()
                    : outcome.kind() == Kind.FAILED
                        && outcome.detail().equals(mode == Mode.FAIL ? "HTTP 503" : "HTTP 598"))
                && events.stream().noneMatch(event -> sameRequest(start, event)
                    && event.kind() == Kind.FAILED && !event.detail().equals(outcome.detail()))
                && (mode == Mode.DELAY || events.stream().anyMatch(ack -> sameRequest(start, ack)
                    && ack.kind() == Kind.FAULT_OBSERVED && ack.sequence() > outcome.sequence()
                    && ack.detail().equals(outcome.detail())))
                && events.stream().anyMatch(finish -> sameRequest(start, finish)
                    && finish.kind() == Kind.REQUEST_FINISHED && finish.sequence() > outcome.sequence()));
    }

    static boolean sameRequest(Event first, Event other) {
        return first.requestId() > 0 && first.requestId() == other.requestId()
                && first.revision() == other.revision() && first.mode() == other.mode()
                && first.process().equals(other.process()) && first.role().equals(other.role())
                && first.registration().equals(other.registration())
                && first.participantInstance().equals(other.participantInstance());
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
