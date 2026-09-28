package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Separates observed process exits from the provenance and terminal data checks. */
public record FlinkProcessHealth(Outcome outcome, String reason, String detail) {
    public static final String UNEXPECTED_EXIT = "flink.process.unexpected-exit";
    public static final String UNCONFIRMED = "flink.process.state-unconfirmed";
    public enum Outcome { HEALTHY, UNEXPECTED_EXIT, UNCONFIRMED }

    public static FlinkProcessHealth evaluate(
            FlinkRuntimeIdentity.ExpectedTarget expected,
            Optional<FlinkProcessWriteFenceEvidence> fence,
            Optional<FlinkProcessWriteFenceEvidence.Observations> snapshot) {
        if (snapshot.isEmpty()) {
            return unknown("No process observations were retained");
        }
        var evidence = snapshot.orElseThrow();
        Map<String, FlinkProcessWriteFenceEvidence.Observation> before = new HashMap<>();
        boolean uncertain = evidence.overflow();
        for (var observation : evidence.observations()) {
            if (observation.state().flatMap(state -> state.oomKilled()).orElse(false)) {
                return new FlinkProcessHealth(Outcome.UNEXPECTED_EXIT, UNEXPECTED_EXIT,
                        "Docker observed an OOM kill, including when a termination was requested: "
                                + observation.logicalName() + " " + observation.runtimeId().orElse("unknown"));
            }
            if (observation.moment() != FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE) {
                continue;
            }
            if (observation.runtimeId().isEmpty()
                    || expected.components().get(observation.logicalName()) != observation.role()
                    || before.putIfAbsent(observation.runtimeId().orElseThrow(), observation) != null) {
                uncertain = true;
            }
            boolean declaredKill = evidence.observations().stream().anyMatch(kill ->
                    kill.moment() == FlinkProcessWriteFenceEvidence.Moment.AFTER_DECLARED_KILL
                            && kill.logicalName().equals(observation.logicalName())
                            && kill.role() == observation.role()
                            && kill.runtimeId().equals(observation.runtimeId())
                            && kill.state().filter(state -> !state.running()).isPresent()
                            && !kill.observedAt().isAfter(observation.observedAt()));
            boolean exited = observation.state().filter(state -> !state.running()).isPresent()
                    || (observation.missing() && observation.runtimeId().isPresent());
            if (exited && !declaredKill) {
                return new FlinkProcessHealth(Outcome.UNEXPECTED_EXIT, UNEXPECTED_EXIT,
                        "Process was stopped or missing before the terminal fence without a matching declared kill: "
                                + observation.logicalName() + " " + observation.runtimeId().orElse("unknown"));
            }
            if ((!declaredKill && observation.state().isEmpty())
                    || observation.state().filter(state -> state.paused()).isPresent()) {
                uncertain = true;
            }
        }
        if (fence.isEmpty()) {
            return unknown("Process fence is incomplete; partial observations are retained");
        }
        var completed = fence.orElseThrow();
        if (!evidence.fenced().equals(completed.components())
                || completed.components().size() != expected.components().size()) {
            uncertain = true;
        }
        var slots = new java.util.HashSet<String>();
        for (var component : completed.components()) {
            var observation = component.runtimeId().map(before::get).orElse(null);
            if (observation == null || !component.logicalName().equals(observation.logicalName())
                    || component.role() != observation.role()
                    || !slots.add(component.logicalName())
                    || observation.observedAt().isAfter(completed.completedAt())) {
                uncertain = true;
            }
            if (component.outcome() == FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED
                    && evidence.observations().stream().noneMatch(stopped ->
                    stopped.moment() == FlinkProcessWriteFenceEvidence.Moment.AFTER_FENCE_KILL
                            && component.logicalName().equals(stopped.logicalName())
                            && component.role() == stopped.role()
                            && component.runtimeId().equals(stopped.runtimeId())
                            && stopped.state().filter(state -> !state.running()).isPresent()
                            && observation != null && !stopped.observedAt().isBefore(observation.observedAt())
                            && !stopped.observedAt().isAfter(completed.completedAt()))) {
                uncertain = true;
            }
            if (component.outcome() == FlinkProcessWriteFenceEvidence.Outcome.ALREADY_STOPPED
                    && (observation == null || observation.state().filter(state -> state.running()).isPresent())) {
                // A peer may stop after the initial snapshot. Keep this causal gap visible.
                uncertain = true;
            }
        }
        if (uncertain || !slots.equals(expected.components().keySet())) {
            return unknown("Process observations do not completely identify every pre-fence process state");
        }
        return new FlinkProcessHealth(Outcome.HEALTHY, "flink.process.healthy",
                "Every fenced process was observed live before the fence or has an exact declared-kill observation");
    }

    private static FlinkProcessHealth unknown(String detail) {
        return new FlinkProcessHealth(Outcome.UNCONFIRMED, UNCONFIRMED, detail);
    }
}
