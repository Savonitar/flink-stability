package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.nio.file.Path;
import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.*;

/** The executable two-JobManager HA subset and its bounded, automatically healed faults. */
final class HighAvailabilityPlanCompiler {
    private HighAvailabilityPlanCompiler() {}

    static void validate(Path source, ObjectNode document, List<Diagnostic> issues) {
        JsonNode flink = document.at("/setup/flink");
        boolean ha = flink.has("high_availability");
        int required = ha ? 2 : 1;
        if (!flink.path("jobmanagers").canConvertToInt()
                || flink.path("jobmanagers").intValue() != required) {
            issues.add(issue(source, "runner.flink.jobmanager-count-unsupported",
                    "$/setup/flink/jobmanagers", "Require " + required + " JobManager(s) for this HA mode"));
        }
        if (ha) {
            JsonNode config = flink.path("high_availability");
            if (!config.path("zookeeper_image").asText().matches("(?:docker\\.io/)?zookeeper:3\\.9\\.[0-9]+")) {
                issues.add(issue(source, "runner.flink.zookeeper-image-unsupported",
                        "$/setup/flink/high_availability/zookeeper_image",
                        "HA requires an official zookeeper:3.9.x image"));
            }
            bounded(source, config.path("session_timeout"),
                    "$/setup/flink/high_availability/session_timeout", Duration.ofSeconds(2),
                    Duration.ofSeconds(60), issues);
            if (!"filesystem".equals(document.at("/workload/jobs/0/checkpointing/storage/type").asText())) {
                issues.add(issue(source, "runner.flink.ha-checkpoint-storage-required",
                        "$/workload/jobs/0/checkpointing/storage",
                        "HA requires shared filesystem checkpoint storage; JobManager memory is not durable"));
            }
        }
        if (flink.has("token_provider")) {
            bounded(source, flink.at("/token_provider/renewal_interval"),
                    "$/setup/flink/token_provider/renewal_interval", Duration.ofSeconds(1),
                    Duration.ofMinutes(5), issues);
            if (flink.path("token_provider").has("retry_backoff")) {
                bounded(source, flink.at("/token_provider/retry_backoff"),
                        "$/setup/flink/token_provider/retry_backoff", Duration.ofSeconds(1),
                        Duration.ofMinutes(5), issues);
            }
        }
        for (int index = 0; index < document.path("phases").size(); index++) {
            validateSteps(source, document.path("phases").get(index).path("steps"),
                    "$/phases/" + index + "/steps", ha, flink.path("token_provider"), issues);
        }
        boolean barrier = false;
        boolean ordinary = false;
        for (JsonNode phase : document.path("phases")) {
            barrier |= hasBarrierChoice(phase.path("steps"), true);
            ordinary |= hasBarrierChoice(phase.path("steps"), false);
        }
        if (barrier && ordinary) issues.add(issue(source, "runner.phase.mixed-recovery-barriers", "$/phases",
                "When token-checkpoint synchronization is requested, every leader fault must declare it"));
        BigInteger count = BigInteger.ZERO;
        for (JsonNode phase : document.path("phases")) count = count.add(faultCount(phase.path("steps")));
        if (count.compareTo(BigInteger.valueOf(100)) > 0) {
            issues.add(issue(source, "runner.phase.ha-fault-count-unsupported", "$/phases",
                    "An invocation supports at most 100 expanded leader faults"));
        }
    }

    private static boolean hasBarrierChoice(JsonNode steps, boolean present) {
        for (JsonNode step : steps) {
            if (step.has("leader_fault") && step.path("leader_fault").has("recovery_barrier") == present) return true;
            if (step.has("loop") && hasBarrierChoice(step.at("/loop/steps"), present)) return true;
        }
        return false;
    }

    private static BigInteger faultCount(JsonNode steps) {
        BigInteger count = BigInteger.ZERO;
        for (JsonNode step : steps) {
            if (step.has("leader_fault")) count = count.add(BigInteger.ONE);
            else if (step.has("loop")) count = count.add(faultCount(step.at("/loop/steps"))
                    .multiply(step.at("/loop/times").bigIntegerValue()));
        }
        return count;
    }

    private static void validateSteps(Path source, JsonNode steps, String path, boolean ha,
                                      JsonNode provider, List<Diagnostic> issues) {
        for (int index = 0; index < steps.size(); index++) {
            JsonNode step = steps.get(index);
            String stepPath = path + "/" + index;
            if (step.has("leader_fault")) {
                JsonNode fault = step.path("leader_fault");
                String field = stepPath + "/leader_fault";
                if (!ha) issues.add(issue(source, "runner.phase.ha-required", field,
                        "A leader fault requires the two-JobManager HA topology"));
                Optional<Duration> hold = bounded(source, fault.path("duration"), field + "/duration",
                        Duration.ofMillis(1), Duration.ofMinutes(2), issues);
                Optional<Duration> timeout = bounded(source, fault.path("timeout"), field + "/timeout",
                        Duration.ofMillis(1), Duration.ofMinutes(5), issues);
                if (hold.isPresent() && timeout.isPresent() && hold.get().compareTo(timeout.get()) >= 0) {
                    issues.add(issue(source, "runner.phase.ha-timeout-invalid", field + "/timeout",
                            "The action timeout must exceed its fault hold duration"));
                }
                if (fault.has("recovery_barrier") && provider.isMissingNode()) {
                    issues.add(issue(source, "runner.phase.token-provider-required", field,
                            "The token-checkpoint recovery barrier requires setup.flink.token_provider"));
                }
                if (fault.has("token_fault")) {
                    if (provider.isMissingNode()) issues.add(issue(source, "runner.phase.token-provider-required", field,
                            "A token fault requires setup.flink.token_provider"));
                    JsonNode token = fault.path("token_fault");
                    if (token.has("target") && (!"submitted-job".equals(provider.path("proof_scope").asText())
                            || "linkage-error".equals(token.path("mode").asText()))) {
                        issues.add(issue(source, "runner.phase.token-target-invalid", field + "/token_fault/target",
                                "Submitted-job targeting requires submitted-job proof and delay or fail mode"));
                    }
                    boolean delay = "delay".equals(token.path("mode").asText());
                    if (delay != token.has("delay")) issues.add(issue(source,
                            "runner.phase.token-delay-invalid", field + "/token_fault",
                            "Only delay mode requires a delay value"));
                    if (token.has("delay")) bounded(source, token.path("delay"),
                            field + "/token_fault/delay", Duration.ofMillis(1), Duration.ofSeconds(30), issues);
                }
            } else if (step.has("loop")) {
                validateSteps(source, step.at("/loop/steps"), stepPath + "/loop/steps", ha, provider, issues);
            }
        }
    }

    private static Optional<Duration> bounded(Path source, JsonNode value, String path,
                                               Duration minimum, Duration maximum, List<Diagnostic> issues) {
        int prior = issues.size();
        requireDuration(source, value, path, issues);
        if (issues.size() != prior) return Optional.empty();
        Duration duration = parseDuration(value.asText());
        if (duration.compareTo(minimum) < 0 || duration.compareTo(maximum) > 0) {
            issues.add(issue(source, "runner.duration.out-of-range", path,
                    "Duration must be between " + minimum + " and " + maximum));
        }
        return Optional.of(duration);
    }

    static Optional<FlinkRuntimeTarget.HighAvailability> highAvailability(JsonNode flink) {
        return Optional.ofNullable(flink.get("high_availability")).map(value ->
                new FlinkRuntimeTarget.HighAvailability(value.path("zookeeper_image").asText(),
                        parseDuration(value.path("session_timeout").asText())));
    }

    static Optional<FlinkRuntimeTarget.TokenProvider> tokenProvider(JsonNode flink) {
        return Optional.ofNullable(flink.get("token_provider")).map(value ->
                new FlinkRuntimeTarget.TokenProvider(parseDuration(value.path("renewal_interval").asText()),
                        Optional.ofNullable(value.get("retry_backoff"))
                                .map(backoff -> parseDuration(backoff.asText())),
                        Optional.ofNullable(value.get("proof_scope"))
                                .map(scope -> FlinkRuntimeTarget.TokenProofScope.valueOf(
                                        scope.asText().replace('-', '_').toUpperCase(Locale.ROOT)))));
    }

    static ExecutableScenarioPlan.LeaderFault fault(JsonNode value) {
        Optional<FlinkHaControl.TokenFault> token = Optional.ofNullable(value.get("token_fault"))
                .map(node -> new FlinkHaControl.TokenFault(TokenServiceControl.Mode.valueOf(
                        node.path("mode").asText().replace('-', '_').toUpperCase(Locale.ROOT)),
                        node.has("delay") ? parseDuration(node.path("delay").asText()) : Duration.ZERO, node.has("target")));
        return new ExecutableScenarioPlan.LeaderFault(new FlinkHaControl.LeaderFaultRequest(
                FlinkHaControl.Mode.valueOf(value.path("mode").asText().replace('-', '_').toUpperCase(Locale.ROOT)),
                parseDuration(value.path("duration").asText()),
                parseDuration(value.path("timeout").asText()), token),
                value.has("recovery_barrier")
                        ? Optional.of(ExecutableScenarioPlan.RecoveryBarrier.TOKEN_CHECKPOINT) : Optional.empty());
    }
}
