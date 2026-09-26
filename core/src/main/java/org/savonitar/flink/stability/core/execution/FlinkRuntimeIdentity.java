package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Observed local Docker image identity across the first runner's process incarnations. */
public record FlinkRuntimeIdentity(Outcome outcome, String detail) {
    public static final String MISMATCH = "subject.flink.image-id-mismatch";
    public static final String UNCONFIRMED = "subject.flink.image-id-unconfirmed";

    public enum Outcome { CONFIRMED, MISMATCH, UNCONFIRMED }

    /** Expected composition is supplied by the executable plan, never inferred from observations. */
    public record ExpectedTarget(
            Optional<String> imageId,
            Map<String, FlinkComponentRole> components) {
        public ExpectedTarget {
            imageId = Objects.requireNonNull(imageId, "imageId");
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
    }

    public String reason() {
        return outcome == Outcome.MISMATCH ? MISMATCH : UNCONFIRMED;
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
}
