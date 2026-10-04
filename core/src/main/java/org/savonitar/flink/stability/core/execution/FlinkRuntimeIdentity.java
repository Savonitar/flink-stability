package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Observed image and optional runtime JAR identities across the runner's process incarnations. */
public record FlinkRuntimeIdentity(Outcome outcome, String detail, String reason) {
    public static final String MISMATCH = "subject.flink.image-id-mismatch";
    public static final String UNCONFIRMED = "subject.flink.image-id-unconfirmed";
    public static final String RUNTIME_JAR_MISMATCH = "subject.flink.runtime-jar-mismatch";
    public static final String RUNTIME_JAR_UNCONFIRMED = "subject.flink.runtime-jar-unconfirmed";
    public static final String CONFIG_MISMATCH = "subject.flink.config-mismatch";
    public static final String CONFIG_UNCONFIRMED = "subject.flink.config-unconfirmed";
    public static final String RESOURCE_MANAGER_CLASS =
            "org.apache.flink.runtime.resourcemanager.ResourceManager";
    public static final String TASK_EXECUTOR_CLASS =
            "org.apache.flink.runtime.taskexecutor.TaskExecutor";
    public static final List<String> RUNTIME_CLASSES = List.of(
            RESOURCE_MANAGER_CLASS, TASK_EXECUTOR_CLASS);

    public enum Outcome { CONFIRMED, MISMATCH, UNCONFIRMED }

    /** Expected composition is supplied by the executable plan, never inferred from observations. */
    public record ExpectedTarget(
            Optional<String> imageId,
            Map<String, FlinkComponentRole> components,
            Optional<FlinkRuntimeTarget.RuntimeJar> runtimeJar,
            Optional<String> declaredLine,
            Optional<String> imageReference,
            Map<String, String> config) {
        public ExpectedTarget(Optional<String> imageId, Map<String, FlinkComponentRole> components,
                              Optional<FlinkRuntimeTarget.RuntimeJar> runtimeJar) {
            this(imageId, components, runtimeJar, Optional.empty(), Optional.empty(), Map.of());
        }
        public ExpectedTarget {
            Objects.requireNonNull(declaredLine, "declaredLine");
            Objects.requireNonNull(imageReference, "imageReference");
            config = org.savonitar.flink.stability.runtime.api.FlinkConfiguration.validate(config);
            imageId = Objects.requireNonNull(imageId, "imageId");
            runtimeJar = Objects.requireNonNull(runtimeJar, "runtimeJar");
            imageId.ifPresent(id -> org.savonitar.flink.stability.runtime.api.Checks
                    .requireDockerImageId(id, "imageId"));
            components = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(
                    Objects.requireNonNull(components, "components")));
            if (components.isEmpty()) {
                throw new IllegalArgumentException("Expected Flink components must not be empty");
            }
            components.forEach((name, role) -> {
                requireNonBlank(name, "logicalName");
                Objects.requireNonNull(role, "role");
            });
        }

        public ExpectedTarget(Optional<String> imageId, Map<String, FlinkComponentRole> components) {
            this(imageId, components, Optional.empty());
        }
    }

    public FlinkRuntimeIdentity(Outcome outcome, String detail) {
        this(outcome, detail, outcome == Outcome.MISMATCH ? MISMATCH : UNCONFIRMED);
    }

    public static FlinkRuntimeIdentity evaluate(
            ExpectedTarget expected,
            List<FlinkComponentProvisioningEvidence> provisioning,
            Optional<FlinkProcessWriteFenceEvidence> fence,
            Optional<PhaseExecutionEvidence> phases) {
        if (provisioning.isEmpty()) {
            return unconfirmed("No Flink process has observed image identity evidence");
        }
        if (!expected.config().isEmpty()) {
            if (provisioning.stream().anyMatch(component -> component.effectiveConfiguration().isEmpty()
                    || component.classLoadProcess().isEmpty())) {
                return new FlinkRuntimeIdentity(Outcome.UNCONFIRMED,
                        "Every Flink incarnation requires verified effective configuration and its registered process log",
                        CONFIG_UNCONFIRMED);
            }
            if (provisioning.stream().map(component -> component.effectiveConfiguration().orElseThrow())
                    .anyMatch(receipt -> !receipt.observedValues().equals(expected.config())
                            || !receipt.sourceSha256().equals(receipt.observedSha256()))) {
                return new FlinkRuntimeIdentity(Outcome.MISMATCH,
                        "Effective Flink configuration differs from the staged bytes or resolved custom values",
                        CONFIG_MISMATCH);
            }
        }
        if (provisioning.stream().anyMatch(component -> !expected.config().equals(component.flinkConfig()))) {
            return new FlinkRuntimeIdentity(Outcome.MISMATCH,
                    "Flink process configuration differs from the resolved custom configuration", CONFIG_MISMATCH);
        }
        String observedImageId = provisioning.getFirst().imageId();
        if (provisioning.stream().anyMatch(component ->
                !observedImageId.equals(component.imageId())
                        || expected.imageId().filter(id -> !id.equals(component.imageId())).isPresent())) {
            return new FlinkRuntimeIdentity(Outcome.MISMATCH,
                    "Flink process image IDs differ across incarnations or from the declared image ID");
        }
        Map<String, FlinkComponentProvisioningEvidence> byRuntimeId = new HashMap<>();
        Map<String, Long> incarnations = new HashMap<>();
        Map<String, String> latestRuntimeIds = new HashMap<>();
        Map<String, Integer> provisionOrder = new HashMap<>();
        for (FlinkComponentProvisioningEvidence component : provisioning) {
            if (expected.components().get(component.logicalName()) != component.role()) {
                return unconfirmed("A provisioned Flink process does not match an expected slot: "
                        + component.logicalName() + " " + component.role());
            }
            if (byRuntimeId.putIfAbsent(component.runtimeId(), component) != null) {
                return unconfirmed("Duplicate provisioning evidence for Flink process "
                        + component.runtimeId());
            }
            incarnations.merge(component.logicalName(), 1L, Long::sum);
            latestRuntimeIds.putIfAbsent(component.logicalName(), component.runtimeId());
            provisionOrder.put(component.runtimeId(), provisionOrder.size());
        }
        if (phases.isEmpty()) {
            return unconfirmed("No phase evidence establishes the required Flink replacements");
        }
        PhaseExecutionEvidence phase = phases.orElseThrow();
        List<PhaseExecutionEvidence.StepEvidence> restartSteps = phase.steps().stream()
                .filter(step -> step.kind() == PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER
                        && step.status() == PhaseExecutionEvidence.StepStatus.SUCCEEDED)
                .toList();
        if (restartSteps.size() != phase.taskManagerRestarts().size()) {
            return unconfirmed("Every successful TaskManager restart requires targeted identity evidence");
        }
        if (!restartSteps.isEmpty() && !restartsMatchPrecedingKills(phase)) {
            return unconfirmed("TaskManager restart predecessor does not match its preceding unhealed kill");
        }
        Map<String, Long> restartsBySlot = new HashMap<>();
        Set<StepIdentity> restartLocations = new HashSet<>();
        Set<String> replacementIds = new HashSet<>();
        Map<String, String> latestResourceIds = new HashMap<>();
        Set<String> resourceIds = new HashSet<>();
        for (int index = 0; index < restartSteps.size(); index++) {
            PhaseExecutionEvidence.StepEvidence step = restartSteps.get(index);
            PhaseExecutionEvidence.TaskManagerRestart restart = phase.taskManagerRestarts().get(index);
            StepIdentity location = new StepIdentity(step.path(), step.loopIterations());
            if (!restartLocations.add(location)
                    || !location.equals(new StepIdentity(restart.path(), restart.loopIterations()))
                    || expected.components().get(restart.target()) != FlinkComponentRole.TASK_MANAGER
                    || restart.previousIdentity().isEmpty() || restart.replacementIdentity().isEmpty()) {
                return unconfirmed("TaskManager restart has missing, duplicate, or mismatched target evidence");
            }
            var previous = restart.previousIdentity().orElseThrow();
            var replacement = restart.replacementIdentity().orElseThrow();
            FlinkComponentProvisioningEvidence oldProcess = byRuntimeId.get(previous.runtimeId());
            FlinkComponentProvisioningEvidence newProcess = byRuntimeId.get(replacement.runtimeId());
            if (!restart.target().equals(previous.logicalName())
                    || !restart.target().equals(replacement.logicalName())
                    || oldProcess == null || newProcess == null
                    || !restart.target().equals(oldProcess.logicalName())
                    || !restart.target().equals(newProcess.logicalName())
                    || !previous.runtimeId().equals(latestRuntimeIds.get(restart.target()))
                    || previous.runtimeId().equals(replacement.runtimeId())
                    || previous.resourceId().equals(replacement.resourceId())
                    || provisionOrder.get(previous.runtimeId()) >= provisionOrder.get(replacement.runtimeId())
                    || !replacementIds.add(replacement.runtimeId())) {
                return unconfirmed("TaskManager restart does not replace the observed incarnation of "
                        + restart.target());
            }
            String lastResource = latestResourceIds.get(restart.target());
            if ((lastResource != null && !lastResource.equals(previous.resourceId()))
                    || (lastResource == null && !resourceIds.add(previous.resourceId()))
                    || !resourceIds.add(replacement.resourceId())) {
                return unconfirmed("TaskManager restart reuses or misidentifies a Flink resource identity");
            }
            latestRuntimeIds.put(restart.target(), replacement.runtimeId());
            latestResourceIds.put(restart.target(), replacement.resourceId());
            restartsBySlot.merge(restart.target(), 1L, Long::sum);
        }
        for (PhaseExecutionEvidence.TaskManagerKill kill : phase.taskManagerKills()) {
            if (kill.identity().isEmpty()) {
                return unconfirmed("A killed TaskManager has no observed physical identity");
            }
            var identity = kill.identity().orElseThrow();
            FlinkComponentProvisioningEvidence killed = byRuntimeId.get(identity.runtimeId());
            if (!kill.target().equals(identity.logicalName()) || killed == null
                    || !kill.target().equals(killed.logicalName())
                    || killed.role() != FlinkComponentRole.TASK_MANAGER) {
                return unconfirmed("A killed TaskManager has no matching provisioning identity");
            }
        }
        List<PhaseExecutionEvidence.StepEvidence> leaderSteps = phase.steps().stream()
                .filter(step -> step.kind() == PhaseExecutionEvidence.StepKind.LEADER_FAULT)
                .toList();
        Set<StepIdentity> leaderLocations = new HashSet<>();
        int previousLeaderStep = -1;
        for (var fault : phase.leaderFaults()) {
            var raw = fault.raw();
            StepIdentity location = new StepIdentity(fault.path(), fault.loopIterations());
            int matchingStep = -1;
            for (int index = 0; index < leaderSteps.size(); index++) {
                var step = leaderSteps.get(index);
                if (location.equals(new StepIdentity(step.path(), step.loopIterations()))) {
                    matchingStep = index;
                    break;
                }
            }
            if (!leaderLocations.add(location) || matchingStep <= previousLeaderStep) {
                return unconfirmed("A leader fault has no correlated ordered declared operation");
            }
            previousLeaderStep = matchingStep;
            // Election, recovery, token, and healing success are HA evidence, not image identity.
            // Only physical incarnation changes belong in this accounting.
            if (raw.target().isEmpty()) {
                if (raw.healedState().isPresent()) {
                    return unconfirmed("A replacement process has no physical fault target");
                }
                continue;
            }
            var target = raw.target().orElseThrow();
            FlinkComponentProvisioningEvidence oldProcess = byRuntimeId.get(target.runtimeId());
            if (expected.components().get(target.logicalName()) != FlinkComponentRole.JOB_MANAGER
                    || oldProcess == null || !target.logicalName().equals(oldProcess.logicalName())
                    || !target.runtimeId().equals(latestRuntimeIds.get(target.logicalName()))) {
                return unconfirmed("Leader fault target does not match the current provisioned JobManager");
            }
            if (raw.healedState().isPresent()) {
                var healed = raw.healedState().orElseThrow();
                FlinkComponentProvisioningEvidence replacement = byRuntimeId.get(healed.runtimeId());
                if (replacement == null || !target.logicalName().equals(replacement.logicalName())) {
                    return unconfirmed("Leader fault replacement has no matching provisioning identity");
                }
                if (raw.request().mode()
                        == org.savonitar.flink.stability.runtime.api.FlinkHaControl.Mode.KILL) {
                    if (provisionOrder.get(target.runtimeId()) >= provisionOrder.get(healed.runtimeId())
                            || !replacementIds.add(healed.runtimeId())) {
                        return unconfirmed("A killed JobManager has no distinct subsequent replacement");
                    }
                    latestRuntimeIds.put(target.logicalName(), healed.runtimeId());
                    restartsBySlot.merge(target.logicalName(), 1L, Long::sum);
                } else if (!target.runtimeId().equals(healed.runtimeId())) {
                    return unconfirmed("A non-kill leader fault changed its physical process identity");
                }
            }
            if (raw.after().isPresent()) {
                var successor = raw.after().orElseThrow().resourceManager();
                if (expected.components().get(successor.logicalName()) != FlinkComponentRole.JOB_MANAGER
                        || !successor.runtimeId().equals(latestRuntimeIds.get(successor.logicalName()))) {
                    return unconfirmed("Observed leader has no current provisioned JobManager identity");
                }
            }
        }
        for (Map.Entry<String, FlinkComponentRole> slot : expected.components().entrySet()) {
            long required = 1 + restartsBySlot.getOrDefault(slot.getKey(), 0L);
            if (incarnations.getOrDefault(slot.getKey(), 0L) != required) {
                return unconfirmed("Provisioning evidence does not match initial and replacement processes for "
                        + slot.getKey());
            }
        }
        if (fence.isEmpty() || fence.orElseThrow().components().isEmpty()) {
            return unconfirmed("No process fence identifies the Flink processes that ran");
        }
        Set<String> fencedSlots = new HashSet<>();
        for (FlinkProcessWriteFenceEvidence.Component component : fence.orElseThrow().components()) {
            FlinkComponentProvisioningEvidence observed = component.runtimeId()
                    .map(byRuntimeId::get).orElse(null);
            if (observed == null || observed.role() != component.role()
                    || !observed.logicalName().equals(component.logicalName())
                    || !observed.runtimeId().equals(latestRuntimeIds.get(component.logicalName()))
                    || !fencedSlots.add(component.logicalName())) {
                return unconfirmed("A fenced Flink process has no matching provisioning identity: "
                        + component.logicalName() + " " + component.runtimeId().orElse("unknown"));
            }
        }
        if (!fencedSlots.containsAll(expected.components().keySet())) {
            return unconfirmed("The process fence does not identify every expected Flink slot");
        }
        return new FlinkRuntimeIdentity(Outcome.CONFIRMED,
                "Every recorded Flink incarnation used local Docker image " + observedImageId
                        + "; initial, replacement, and fenced process identities are covered");
    }

    private record StepIdentity(String path, List<PhaseExecutionEvidence.LoopIteration> iterations) {}

    private static boolean restartsMatchPrecedingKills(PhaseExecutionEvidence phase) {
        Map<String, PhaseExecutionEvidence.TaskManagerKill> unhealed = new HashMap<>();
        Set<StepIdentity> locations = new HashSet<>();
        int killIndex = 0;
        int restartIndex = 0;
        for (PhaseExecutionEvidence.StepEvidence step : phase.steps()) {
            if (step.status() != PhaseExecutionEvidence.StepStatus.SUCCEEDED
                    || (step.kind() != PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER
                    && step.kind() != PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER)) {
                continue;
            }
            StepIdentity location = new StepIdentity(step.path(), step.loopIterations());
            if (!locations.add(location)) {
                return false;
            }
            if (step.kind() == PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER) {
                if (killIndex >= phase.taskManagerKills().size()) {
                    return false;
                }
                var kill = phase.taskManagerKills().get(killIndex++);
                if (!location.equals(new StepIdentity(kill.path(), kill.loopIterations()))
                        || unhealed.putIfAbsent(kill.target(), kill) != null) {
                    return false;
                }
            } else {
                if (restartIndex >= phase.taskManagerRestarts().size()) {
                    return false;
                }
                var restart = phase.taskManagerRestarts().get(restartIndex++);
                var kill = unhealed.remove(restart.target());
                if (!location.equals(new StepIdentity(restart.path(), restart.loopIterations()))
                        || kill == null || !kill.identity().equals(restart.previousIdentity())) {
                    return false;
                }
            }
        }
        return killIndex == phase.taskManagerKills().size()
                && restartIndex == phase.taskManagerRestarts().size();
    }

    private static FlinkRuntimeIdentity unconfirmed(String detail) {
        return new FlinkRuntimeIdentity(Outcome.UNCONFIRMED, detail);
    }

    /** Each registered log must belong to exactly one observed physical incarnation. */
    public static Optional<FlinkRuntimeIdentity> evaluateRuntimeJar(
            ExpectedTarget expected,
            List<FlinkComponentProvisioningEvidence> provisioning,
            Optional<SubjectClassOrigins> origins) {
        return expected.runtimeJar().map(jar -> runtimeJarIdentity(expected, jar, provisioning, origins));
    }

    private static FlinkRuntimeIdentity runtimeJarIdentity(
            ExpectedTarget expected,
            FlinkRuntimeTarget.RuntimeJar jar,
            List<FlinkComponentProvisioningEvidence> provisioning,
            Optional<SubjectClassOrigins> origins) {
        if (provisioning.isEmpty()) {
            return jarUnconfirmed("No Flink process has runtime JAR evidence");
        }
        Map<String, FlinkComponentProvisioningEvidence> bindings = new HashMap<>();
        Set<String> runtimeIds = new HashSet<>();
        Set<String> observedSlots = new HashSet<>();
        for (FlinkComponentProvisioningEvidence component : provisioning) {
            if (expected.components().get(component.logicalName()) != component.role()
                    || !runtimeIds.add(component.runtimeId())) {
                return jarUnconfirmed("Runtime JAR evidence has an unexpected or duplicate Flink process");
            }
            observedSlots.add(component.logicalName());
            if (component.runtimeJarEvidence().isEmpty()) {
                return jarUnconfirmed("Missing runtime JAR observation for " + component.runtimeId());
            }
            var observed = component.runtimeJarEvidence().orElseThrow();
            if (!jar.equals(observed.jar())) {
                return jarMismatch("Runtime JAR bytes or path differ for " + component.runtimeId()
                        + ": expected " + jar + ", observed " + observed.jar());
            }
            if (bindings.putIfAbsent(observed.classLoadProcess(), component) != null) {
                return jarUnconfirmed("Several Flink incarnations claim class-load log "
                        + observed.classLoadProcess());
            }
        }
        if (!observedSlots.containsAll(expected.components().keySet())) {
            return jarUnconfirmed("Runtime JAR observations omit an expected Flink slot");
        }
        if (origins.isEmpty()) {
            return jarUnconfirmed("No runtime class-load observations were retained");
        }
        SubjectClassOrigins loaded = origins.orElseThrow();
        if (!jar.containerPath().equals(loaded.expectedSource())) {
            return jarUnconfirmed("Runtime class-load observations were checked against another JAR path");
        }
        Set<String> observedLogs = new HashSet<>();
        for (SubjectClassOrigins.ProcessOrigin process : loaded.processes()) {
            FlinkComponentProvisioningEvidence component = bindings.get(process.process());
            if (component == null || !observedLogs.add(process.process())) {
                return jarUnconfirmed("Runtime class-load log has an unknown or duplicate binding: "
                        + process.process());
            }
            for (String runtimeClass : RUNTIME_CLASSES) {
                if (process.sources().getOrDefault(runtimeClass, List.of()).stream()
                        .anyMatch(source -> !jar.containerPath().equals(source))) {
                    return jarMismatch(process.process() + " loaded " + runtimeClass
                            + " from outside the verified runtime JAR");
                }
            }
            String required = component.role() == FlinkComponentRole.JOB_MANAGER
                    ? RESOURCE_MANAGER_CLASS : TASK_EXECUTOR_CLASS;
            if (process.sources().getOrDefault(required, List.of()).isEmpty()) {
                return jarUnconfirmed(process.process() + " did not show a load of " + required);
            }
        }
        if (loaded.failure().isPresent() || !observedLogs.equals(bindings.keySet())) {
            return jarUnconfirmed(loaded.failure().orElse(
                    "A provisioned Flink incarnation has no runtime class-load log"));
        }
        return new FlinkRuntimeIdentity(Outcome.CONFIRMED,
                "Every Flink incarnation loaded its required runtime class from "
                        + jar.containerPath() + " with observed SHA-256 " + jar.sha256(),
                "subject.flink.runtime-jar-confirmed");
    }

    private static FlinkRuntimeIdentity jarUnconfirmed(String detail) {
        return new FlinkRuntimeIdentity(Outcome.UNCONFIRMED, detail, RUNTIME_JAR_UNCONFIRMED);
    }

    private static FlinkRuntimeIdentity jarMismatch(String detail) {
        return new FlinkRuntimeIdentity(Outcome.MISMATCH, detail, RUNTIME_JAR_MISMATCH);
    }
}
