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

/** Retains the raw observations used to judge a leadership or synthetic token fault. */
final class FlinkHaEvidenceRenderer {
    private FlinkHaEvidenceRenderer() {}

    static void render(ObjectNode node, FlinkHaEvidence evidence,
                       List<PhaseExecutionEvidence.LeaderFault> faults) {
        node.put("status", evidence.expected().faults().isEmpty()
                && !evidence.expected().tokenProviderRequired() ? "not-requested"
                : evidence.outcome().name().toLowerCase(Locale.ROOT));
        node.put("detail", evidence.detail());
        node.put("tokenProviderRequired", evidence.expected().tokenProviderRequired());
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
            raw.tokensBefore().ifPresent(value -> snapshot(observed.putObject("tokensBefore"), value));
            raw.tokensDuring().ifPresent(value -> snapshot(observed.putObject("tokensDuring"), value));
            raw.tokensAfter().ifPresent(value -> snapshot(observed.putObject("tokensAfter"), value));
            ArrayNode errors = observed.putArray("errors");
            raw.errors().forEach(errors::add);
        });
        evidence.tokens().ifPresent(tokens -> {
            ObjectNode rendered = node.putObject("tokens");
            tokens.pluginSha256().ifPresent(value -> rendered.put("pluginSha256", value));
            rendered.put("containerPath", FlinkHaEvidence.TOKEN_CONTAINER_PATH);
            tokens.snapshot().ifPresent(value -> snapshot(rendered.putObject("snapshot"), value));
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

    private static void state(ObjectNode node, FlinkHaControl.ProcessState state) {
        node.put("runtimeId", state.runtimeId());
        node.put("running", state.running());
        node.put("paused", state.paused());
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

    private static void snapshot(ObjectNode node, TokenServiceControl.Snapshot snapshot) {
        node.put("overflow", snapshot.overflow());
        node.put("saturated", snapshot.saturated());
        node.put("activeRequests", snapshot.activeRequests());
        node.put("maxConcurrentRequests", snapshot.maxConcurrentRequests());
        ArrayNode events = node.putArray("events");
        snapshot.events().forEach(event -> {
            ObjectNode item = events.addObject();
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
        });
    }
}
