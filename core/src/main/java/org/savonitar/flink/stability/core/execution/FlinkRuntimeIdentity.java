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
            Optional<FlinkRuntimeTarget.RuntimeJar> runtimeJar) {
        public ExpectedTarget {
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
        String observedImageId = provisioning.getFirst().imageId();
        if (provisioning.stream().anyMatch(component ->
                !observedImageId.equals(component.imageId())
                        || expected.imageId().filter(id -> !id.equals(component.imageId())).isPresent())) {
            return new FlinkRuntimeIdentity(Outcome.MISMATCH,
                    "Flink process image IDs differ across incarnations or from the declared image ID");
        }
        Map<String, FlinkComponentProvisioningEvidence> byRuntimeId = new HashMap<>();
        Map<String, Long> incarnations = new HashMap<>();
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
        }
        if (phases.isEmpty()) {
            return unconfirmed("No phase evidence establishes the required Flink replacements");
        }
        long restarts = phases.stream().flatMap(phase -> phase.steps().stream())
                .filter(step -> step.kind() == PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER
                        && step.status() == PhaseExecutionEvidence.StepStatus.SUCCEEDED)
                .count();
        long taskManagerSlots = expected.components().values().stream()
                .filter(role -> role == FlinkComponentRole.TASK_MANAGER).count();
        // Current restart evidence is untargeted because the executable runner permits one TM.
        if (restarts > 0 && taskManagerSlots != 1) {
            return unconfirmed("Untargeted restart evidence cannot identify a TaskManager slot");
        }
        for (Map.Entry<String, FlinkComponentRole> slot : expected.components().entrySet()) {
            long required = 1 + (slot.getValue() == FlinkComponentRole.TASK_MANAGER ? restarts : 0);
            if (incarnations.getOrDefault(slot.getKey(), 0L) < required) {
                return unconfirmed("Provisioning evidence omits an initial or replacement Flink process: "
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
                    || !observed.logicalName().equals(component.logicalName())) {
                return unconfirmed("A fenced Flink process has no matching provisioning identity: "
                        + component.logicalName() + " " + component.runtimeId().orElse("unknown"));
            }
            fencedSlots.add(component.logicalName());
        }
        if (!fencedSlots.containsAll(expected.components().keySet())) {
            return unconfirmed("The process fence does not identify every expected Flink slot");
        }
        return new FlinkRuntimeIdentity(Outcome.CONFIRMED,
                "Every recorded Flink incarnation used local Docker image " + observedImageId
                        + "; initial, replacement, and fenced process identities are covered");
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
