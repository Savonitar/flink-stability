package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Shared request-scope and exact-receiver proof for controls, faults and recovery barriers. */
public final class TokenScopeProof {
    private TokenScopeProof() {}

    /** Bound from the actual submit response and a subsequent append-only trace watermark. */
    public record Submission(String jobId, String jobAlias, TokenServiceControl.Snapshot snapshot) {
        public Submission {
            jobId = requireNonBlank(jobId, "jobId");
            jobAlias = requireNonBlank(jobAlias, "jobAlias");
            Objects.requireNonNull(snapshot, "snapshot");
        }

        public long afterSequence() { return snapshot.events().size(); }
    }

    /** Missing submission or live receiver identities cannot satisfy an explicit scope. */
    public record Requirement(FlinkRuntimeTarget.TokenProofScope scope, Optional<Submission> submission,
                              List<TokenCheckpointBarrier.Receiver> receivers) {
        public Requirement {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(submission, "submission");
            receivers = List.copyOf(receivers);
        }

        public Requirement(FlinkRuntimeTarget.TokenProofScope scope) {
            this(scope, Optional.empty(), List.of());
        }

        Requirement submitted(String jobId, String alias, TokenServiceControl.Snapshot snapshot) {
            return new Requirement(scope, Optional.of(new Submission(jobId, alias, snapshot)), receivers);
        }

        Requirement withReceivers(List<TokenCheckpointBarrier.Receiver> observed) {
            return new Requirement(scope, submission, observed);
        }
    }

    static boolean sameRequest(TokenServiceControl.Event first, TokenServiceControl.Event other) {
        return TokenServiceControl.sameRequest(first, other);
    }

    static boolean request(List<TokenServiceControl.Event> events, TokenServiceControl.Event start,
                           Optional<Requirement> requirement) {
        return requirement.isEmpty() || request(events, start, requirement.orElseThrow(), inspect(events));
    }

    private static boolean request(List<TokenServiceControl.Event> events, TokenServiceControl.Event start,
                                   Requirement expected, Contexts contexts) {
        if (!contexts.valid() || !contexts.requests().getOrDefault(start.sequence(), false)
                || expected.submission().isEmpty() || start.registration().isEmpty()) return false;
        var submitted = expected.submission().orElseThrow();
        if (!FlinkHaEvidence.completeTrace(submitted.snapshot()) || start.sequence() <= submitted.afterSequence()
                || submitted.afterSequence() > events.size()
                || !events.subList(0, submitted.snapshot().events().size()).equals(submitted.snapshot().events())) return false;
        var context = start.registration().orElseThrow();
        return expected.scope() == FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB
                ? "JOB".equals(context.scope()) && submitted.jobId().equals(context.jobId())
                    && submitted.jobAlias().equals(context.jobAlias())
                : "BOOTSTRAP".equals(context.scope()) && "-".equals(context.jobId()) && "-".equals(context.jobAlias());
    }

    private record Contexts(Map<Long, Boolean> requests,
                            Map<String, java.util.NavigableMap<Long, State>> states, boolean valid) {
        boolean current(TokenServiceControl.Event issued, long sequence) {
            if (issued.registration().isEmpty()) return false;
            var context = issued.registration().orElseThrow();
            var history = states.get(context.providerInstance());
            var state = history == null ? null : history.floorEntry(sequence);
            return state != null && state.getValue().equals(new State(context.generation(), context.jobId(), context.jobAlias()));
        }
    }
    private record State(long generation, String jobId, String alias) {
        State apply(TokenServiceControl.Lifecycle occurrence) {
            if ("REGISTER".equals(occurrence.kind())) {
                if ("-".equals(occurrence.jobId()) || "-".equals(occurrence.jobAlias())) return null;
                if ("-".equals(jobId) && occurrence.generation() == generation + 1) {
                    return new State(occurrence.generation(), occurrence.jobId(), occurrence.jobAlias());
                }
                if (occurrence.generation() == generation && jobId.equals(occurrence.jobId())
                        && alias.equals(occurrence.jobAlias())) return this;
            } else if ("UNREGISTER".equals(occurrence.kind()) && !"-".equals(jobId)
                    && jobId.equals(occurrence.jobId()) && alias.equals(occurrence.jobAlias())
                    && occurrence.generation() == generation + 1) {
                return new State(occurrence.generation(), "-", "-");
            }
            return null;
        }
    }

    private static final class Provider {
        final String process;
        final Map<Long, TokenServiceControl.Lifecycle> records = new HashMap<>();
        final Map<Long, State> states = new HashMap<>();
        Provider(String process) {
            this.process = process;
            states.put(0L, new State(0, "-", "-"));
        }
    }

    /** Exact successful ACK endpoints, including responses completed after a subsequent request started. */
    private static Map<String, Map<Long, Long>> acknowledgments(List<TokenServiceControl.Event> events) {
        Map<Long, TokenServiceControl.Event> starts = new HashMap<>(), issued = new HashMap<>(), finished = new HashMap<>();
        java.util.Set<Long> failed = new java.util.HashSet<>();
        for (var event : events) {
            switch (event.kind()) {
                case REQUEST_STARTED -> starts.put(event.requestId(), event);
                case ISSUED -> issued.put(event.requestId(), event);
                case FAILED -> failed.add(event.requestId());
                case REQUEST_FINISHED -> finished.put(event.requestId(), event);
                default -> { }
            }
        }
        Map<String, Map<Long, Long>> acknowledged = new HashMap<>();
        for (var token : issued.values()) {
            var start = starts.get(token.requestId());
            var finish = finished.get(token.requestId());
            if (start == null || finish == null || failed.contains(token.requestId()) || token.registration().isEmpty()
                    || token.tokenSequence().isEmpty() || !sameRequest(start, token) || !sameRequest(start, finish)
                    || start.sequence() >= token.sequence() || token.sequence() >= finish.sequence()) continue;
            var context = token.registration().orElseThrow();
            acknowledged.computeIfAbsent(context.providerInstance(), ignored -> new HashMap<>())
                    .merge(context.sentThrough(), token.sequence(), Math::min);
        }
        return acknowledged;
    }

    /** Bounded per-provider journals; old immutable captures do not roll back the current state. */
    private static Contexts inspect(List<TokenServiceControl.Event> events) {
        Map<String, Provider> providers = new HashMap<>();
        Map<Long, Boolean> validRequests = new HashMap<>();
        Map<String, java.util.NavigableMap<Long, State>> states = new HashMap<>();
        var acknowledged = acknowledgments(events);
        Map<Long, TokenServiceControl.Event> issued = new HashMap<>();
        java.util.Set<Long> issuedRequests = new java.util.HashSet<>();
        boolean valid = true;
        for (var event : events) {
            if (event.kind() == TokenServiceControl.Kind.REJECTED || event.registration().filter(context ->
                    context.coverageInvalid()).isPresent()) valid = false;
            if (event.kind() == TokenServiceControl.Kind.REQUEST_STARTED) {
                if (event.registration().isEmpty() || event.participantInstance().isEmpty()
                        || !"jobmanager".equals(event.role())
                        || !FlinkHaEvidence.initialized(events, event, TokenServiceControl.Kind.PROVIDER_INITIALIZED)) {
                    valid = false;
                    continue;
                }
                var context = event.registration().orElseThrow();
                var provider = providers.computeIfAbsent(context.providerInstance(), ignored -> new Provider(event.process()));
                long before = provider.records.size();
                long ackAt = acknowledged.getOrDefault(context.providerInstance(), Map.of())
                        .getOrDefault(context.acknowledgedSequence(), Long.MAX_VALUE);
                if (providers.size() > 128 || !provider.process.equals(event.process())
                        || !event.participantInstance().equals(Optional.of(context.providerInstance()))
                        || context.sentThrough() > 10_000
                        || context.acknowledgedSequence() > 0 && ackAt >= event.sequence()) {
                    valid = false;
                    continue;
                }
                for (var occurrence : context.journal()) {
                    var previous = provider.records.get(occurrence.sequence());
                    if (previous != null) {
                        if (!previous.equals(occurrence)) valid = false;
                        continue;
                    }
                    var state = provider.states.get(occurrence.sequence() - 1);
                    var next = state == null ? null : state.apply(occurrence);
                    if (occurrence.sequence() != provider.records.size() + 1 || next == null) {
                        valid = false;
                        continue;
                    }
                    provider.records.put(occurrence.sequence(), occurrence);
                    provider.states.put(occurrence.sequence(), next);
                }
                var state = provider.states.get(context.sentThrough());
                boolean matches = state != null && state.generation() == context.generation()
                        && state.jobId().equals(context.jobId()) && state.alias().equals(context.jobAlias())
                        && ("-".equals(state.jobId()) ? "BOOTSTRAP" : "JOB").equals(context.scope());
                if (!matches) valid = false;
                states.computeIfAbsent(context.providerInstance(), ignored -> new java.util.TreeMap<>())
                        .put(event.sequence(), provider.states.get((long) provider.records.size()));
                validRequests.put(event.sequence(), matches && context.sentThrough() >= before);
            } else if (event.kind() == TokenServiceControl.Kind.ISSUED) {
                if (!issuedRequests.add(event.requestId()) || event.tokenSequence().isEmpty()
                        || issued.putIfAbsent(event.tokenSequence().orElseThrow(), event) != null) valid = false;
            } else if (event.kind() == TokenServiceControl.Kind.RECEIVED) {
                var token = event.tokenSequence().isPresent() ? issued.get(event.tokenSequence().orElseThrow()) : null;
                if (token == null || token.sequence() >= event.sequence() || token.registration().isEmpty()
                        || !event.registration().equals(token.registration()) || event.participantInstance().isEmpty()
                        || event.participantInstance().equals(token.participantInstance())
                        || !FlinkHaEvidence.initialized(events, event, TokenServiceControl.Kind.RECEIVER_INITIALIZED)) valid = false;
            }
        }
        return new Contexts(validRequests, states, valid);
    }

    static boolean coverageValid(TokenServiceControl.Snapshot snapshot) {
        return inspect(snapshot.events()).valid();
    }

    static Optional<TokenServiceControl.Event> issuance(TokenServiceControl.Snapshot snapshot, String issuer,
            List<TokenCheckpointBarrier.Receiver> receivers, long boundary, Optional<Requirement> requirement) {
        return issuance(snapshot, issuer, receivers, boundary, requirement, false);
    }

    static Optional<TokenServiceControl.Event> issuance(TokenServiceControl.Snapshot snapshot, String issuer,
            List<TokenCheckpointBarrier.Receiver> receivers, long boundary, Optional<Requirement> requirement,
            boolean requireCurrentState) {
        if (receivers.isEmpty() || !FlinkHaEvidence.completeTrace(snapshot)) return Optional.empty();
        var events = snapshot.events();
        var contexts = requirement.isPresent() ? inspect(events) : new Contexts(Map.of(), Map.of(), true);
        if (!contexts.valid()) return Optional.empty();
        long currentRevision = events.stream().filter(event -> event.kind() == TokenServiceControl.Kind.MODE_CHANGED)
                .mapToLong(TokenServiceControl.Event::revision).max().orElse(0);
        return events.stream().filter(issued -> issued.sequence() > boundary
                && issued.kind() == TokenServiceControl.Kind.ISSUED && issuer.equals(issued.process())
                && "jobmanager".equals(issued.role()) && issued.mode() == TokenServiceControl.Mode.HEALTHY
                && issued.revision() == currentRevision && issued.tokenSequence().isPresent()
                && (requirement.isEmpty() || !requireCurrentState || contexts.current(issued, events.size()))
                && events.stream().anyMatch(start -> sameRequest(start, issued)
                    && start.kind() == TokenServiceControl.Kind.REQUEST_STARTED && start.sequence() > boundary
                    && start.sequence() < issued.sequence()
                    && (requirement.isEmpty() || request(events, start, requirement.orElseThrow(), contexts))
                    && FlinkHaEvidence.initialized(events, start, TokenServiceControl.Kind.PROVIDER_INITIALIZED))
                && events.stream().noneMatch(event -> sameRequest(issued, event)
                    && event.kind() == TokenServiceControl.Kind.FAILED)
                && events.stream().anyMatch(event -> sameRequest(issued, event)
                    && event.kind() == TokenServiceControl.Kind.REQUEST_FINISHED && event.sequence() > issued.sequence())
                && receivers.stream().allMatch(receiver -> events.stream().anyMatch(received ->
                    received.sequence() > issued.sequence() && received.kind() == TokenServiceControl.Kind.RECEIVED
                    && "taskmanager".equals(received.role()) && receiver.classLoadProcess().equals(received.process())
                    && received.tokenSequence().equals(issued.tokenSequence())
                    && (requirement.isEmpty() || received.registration().equals(issued.registration())
                        && received.participantInstance().isPresent()
                        && !received.participantInstance().equals(issued.participantInstance())
                        && contexts.current(issued, received.sequence()))
                    && FlinkHaEvidence.initialized(events, received, TokenServiceControl.Kind.RECEIVER_INITIALIZED))))
                .findFirst();
    }
}
