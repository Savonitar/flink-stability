package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.execution.FlinkHaEvidence;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.util.List;
import java.util.Locale;
import java.util.LinkedHashMap;
import java.util.Map;

/** Retains the raw observations used to judge a leadership or synthetic token fault. */
final class FlinkHaEvidenceRenderer {
    private FlinkHaEvidenceRenderer() {}

    static void render(ObjectNode node, FlinkHaEvidence evidence,
                       List<PhaseExecutionEvidence.LeaderFault> faults) {
        TokenTrace trace = new TokenTrace(node.putArray("tokenEvents"));
        node.put("status", !evidence.expected().haRequired() && evidence.expected().faults().isEmpty()
                && !evidence.expected().tokenProviderRequired() ? "not-requested"
                : evidence.outcome().name().toLowerCase(Locale.ROOT));
        node.put("detail", evidence.detail());
        node.put("haRequired", evidence.expected().haRequired());
        node.put("tokenProviderRequired", evidence.expected().tokenProviderRequired());
        evidence.observations().ifPresent(value -> observations(node.putObject("observations"), value));
        ArrayNode requests = node.putArray("requestedFaults");
        evidence.expected().faults().forEach(fault -> {
            ObjectNode requested = requests.addObject();
            location(requested, fault.path(), fault.loopIterations());
            request(requested.putObject("request"), fault.request());
        });
        ArrayNode observations = node.putArray("leaderFaults");
        faults.forEach(fault -> {
            ObjectNode observed = observations.addObject();
            location(observed, fault.path(), fault.loopIterations());
            observed.put("jobId", fault.jobId());
            job(observed.putObject("jobBefore"), fault.jobBefore());
            job(observed.putObject("jobAfter"), fault.jobAfter());
            ArrayNode observationErrors = observed.putArray("observationErrors");
            fault.observationErrors().forEach(observationErrors::add);
            var raw = fault.raw();
            request(observed.putObject("request"), raw.request());
            raw.before().ifPresent(value -> leadership(observed.putObject("before"), value));
            raw.after().ifPresent(value -> leadership(observed.putObject("after"), value));
            raw.target().ifPresent(value -> identity(observed.putObject("target"), value));
            raw.faultState().ifPresent(value -> state(observed.putObject("faultState"), value));
            raw.healedState().ifPresent(value -> state(observed.putObject("healedState"), value));
            observed.put("applied", raw.applied());
            observed.put("healed", raw.healed());
            observed.put("armedAtMillis", raw.armedAtMillis());
            observed.put("healedAtMillis", raw.healedAtMillis());
            observed.put("closedConnections", raw.closedConnections());
            observed.put("rejectedConnections", raw.rejectedConnections());
            observed.put("isolationActiveAfterHeal", raw.isolationActiveAfterHeal());
            raw.tokensBefore().ifPresent(value -> trace.snapshot(observed.putObject("tokensBefore"), value));
            raw.tokensDuring().ifPresent(value -> trace.snapshot(observed.putObject("tokensDuring"), value));
            raw.tokensAfter().ifPresent(value -> trace.snapshot(observed.putObject("tokensAfter"), value));
            ArrayNode errors = observed.putArray("errors");
            raw.errors().forEach(errors::add);
        });
        evidence.tokens().ifPresent(tokens -> {
            ObjectNode rendered = node.putObject("tokens");
            tokens.pluginSha256().ifPresent(value -> rendered.put("pluginSha256", value));
            rendered.put("containerPath", FlinkHaEvidence.TOKEN_CONTAINER_PATH);
            tokens.snapshot().ifPresent(value -> trace.snapshot(rendered.putObject("snapshot"), value));
            tokens.origins().ifPresent(origins -> {
                rendered.put("expectedSource", origins.expectedSource());
                origins.failure().ifPresent(value -> rendered.put("classLoadFailure", value));
                ArrayNode classes = rendered.putArray("classes");
                origins.processes().forEach(process -> {
                    ObjectNode item = classes.addObject();
                    item.put("process", process.process());
                    ObjectNode sources = item.putObject("sources");
                    process.sources().forEach((name, paths) -> {
                        ArrayNode found = sources.putArray(name);
                        paths.forEach(found::add);
                    });
                });
            });
            ArrayNode errors = rendered.putArray("errors");
            tokens.errors().forEach(errors::add);
        });
    }

    private static void location(ObjectNode node, String path,
                                 List<PhaseExecutionEvidence.LoopIteration> iterations) {
        node.put("path", path);
        ArrayNode loops = node.putArray("loopIterations");
        iterations.forEach(iteration -> loops.addObject()
                .put("loopPath", iteration.loopPath()).put("iteration", iteration.iteration())
                .put("totalIterations", iteration.totalIterations()));
    }

    private static void request(ObjectNode node, FlinkHaControl.LeaderFaultRequest request) {
        node.put("mode", request.mode().name().toLowerCase(Locale.ROOT));
        node.put("duration", request.duration().toString());
        node.put("timeout", request.timeout().toString());
        request.tokenFault().ifPresent(fault -> node.putObject("tokenFault")
                .put("mode", fault.mode().name().toLowerCase(Locale.ROOT))
                .put("delay", fault.delay().toString()));
    }

    private static void leadership(ObjectNode node, FlinkHaControl.Leadership leadership) {
        identity(node.putObject("resourceManager"), leadership.resourceManager());
        identity(node.putObject("dispatcher"), leadership.dispatcher());
        identity(node.putObject("restServer"), leadership.restServer());
    }

    private static void identity(ObjectNode node, FlinkHaControl.LeaderIdentity identity) {
        node.put("logicalName", identity.logicalName());
        node.put("runtimeId", identity.runtimeId());
        node.put("address", identity.address());
        node.put("sessionId", identity.sessionId());
    }

    static void state(ObjectNode node, FlinkHaControl.ProcessState state) {
        node.put("runtimeId", state.runtimeId());
        node.put("running", state.running());
        node.put("paused", state.paused());
        state.exitCode().ifPresent(value -> node.put("exitCode", value));
        state.oomKilled().ifPresent(value -> node.put("oomKilled", value));
        state.finishedAt().ifPresent(value -> node.put("finishedAt", value));
    }

    private static void observations(ObjectNode node, FlinkHaControl.Observations observations) {
        node.put("coverage", "sampled");
        node.put("overflow", observations.overflow());
        ArrayNode history = node.putArray("leadership");
        observations.leadership().forEach(sample -> {
            ObjectNode item = history.addObject();
            item.put("sequence", sample.sequence());
            item.put("sampleCount", sample.sampleCount());
            item.put("firstObservedAtMillis", sample.firstObservedAtMillis());
            item.put("lastObservedAtMillis", sample.lastObservedAtMillis());
            item.put("moment", sample.moment().name().toLowerCase(Locale.ROOT).replace('_', '-'));
            sample.leadership().ifPresent(value -> leadership(item.putObject("leadership"), value));
            sample.error().ifPresent(value -> item.put("error", value));
        });
        ArrayNode sessions = node.putArray("sessions");
        observations.sessions().forEach(process -> {
            ObjectNode item = sessions.addObject();
            item.put("logicalName", process.logicalName());
            item.put("role", process.role().name().toLowerCase(Locale.ROOT));
            item.put("runtimeId", process.runtimeId());
            item.put("classLoadProcess", process.classLoadProcess());
            item.put("requestedTimeoutMillis", process.requestedTimeoutMillis());
            item.put("overflow", process.overflow());
            ArrayNode negotiated = item.putArray("negotiated");
            process.negotiated().forEach(session -> negotiated.addObject()
                    .put("sessionId", session.sessionId())
                    .put("timeoutMillis", session.timeoutMillis())
                    .put("logLine", session.logLine()));
        });
    }

    private static void job(ObjectNode node, FlinkJobObservation.Attempt attempt) {
        attempt.failure().ifPresent(value -> node.put("failure", value));
        attempt.observation().ifPresent(observed -> {
            node.put("jobManagerTimeMillis", observed.jobManagerTimeMillis());
            node.put("state", observed.state().name());
            node.put("completedCheckpoints", observed.completedCheckpoints());
            node.put("restoredCheckpoints", observed.restoredCheckpoints());
            observed.latestRestore().ifPresent(restore -> node.putObject("latestRestore")
                    .put("checkpointId", restore.checkpointId())
                    .put("restoredAtMillis", restore.restoredAtMillis()));
            ArrayNode failures = node.putArray("failures");
            observed.failures().forEach(failure -> failures.addObject()
                    .put("timestampMillis", failure.timestampMillis())
                    .put("exceptionName", failure.exceptionName()).put("rootCause", failure.rootCause())
                    .put("taskManagerId", failure.taskManagerId().orElse(null)));
            ArrayNode tasks = node.putArray("subtasks");
            observed.subtasks().forEach(task -> tasks.addObject().put("vertexName", task.vertexName())
                    .put("index", task.index()).put("attempt", task.attempt()).put("status", task.status())
                    .put("taskManagerId", task.taskManagerId().orElse(null)));
        });
    }

    /** Full-value interning preserves conflicting events with the same sequence number. */
    private static final class TokenTrace {
        private final ArrayNode events;
        private final Map<TokenServiceControl.Event, Integer> indices = new LinkedHashMap<>();

        private TokenTrace(ArrayNode events) {
            this.events = events;
        }

        private void snapshot(ObjectNode node, TokenServiceControl.Snapshot snapshot) {
            node.put("overflow", snapshot.overflow());
            node.put("saturated", snapshot.saturated());
            node.put("activeRequests", snapshot.activeRequests());
            node.put("maxConcurrentRequests", snapshot.maxConcurrentRequests());
            node.put("eventCount", snapshot.events().size());
            ArrayNode ranges = node.putArray("eventRanges");
            ArrayNode range = null;
            int previous = -2;
            for (TokenServiceControl.Event event : snapshot.events()) {
                int index = indices.computeIfAbsent(event, value -> {
                    int next = events.size();
                    event(events.addObject(), value);
                    return next;
                });
                if (range == null || index != previous + 1) {
                    range = ranges.addArray().add(index).add(index + 1);
                } else {
                    range.set(1, node.numberNode(index + 1));
                }
                previous = index;
            }
        }

        private static void event(ObjectNode item, TokenServiceControl.Event event) {
            item.put("sequence", event.sequence());
            item.put("kind", event.kind().name().toLowerCase(Locale.ROOT));
            item.put("process", event.process());
            item.put("role", event.role());
            item.put("timestampMillis", event.timestampMillis());
            item.put("monotonicNanos", event.monotonicNanos());
            item.put("requestId", event.requestId());
            item.put("revision", event.revision());
            item.put("mode", event.mode().name().toLowerCase(Locale.ROOT));
            event.tokenSequence().ifPresent(value -> item.put("tokenSequence", value));
            item.put("detail", event.detail());
        }
    }
}
