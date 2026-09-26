package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Observed local Docker image identity across the first runner's process incarnations. */
public record FlinkRuntimeIdentity(Outcome outcome, String detail) {
    public static final String MISMATCH = "subject.flink.image-id-mismatch";
    public static final String UNCONFIRMED = "subject.flink.image-id-unconfirmed";

    public enum Outcome { CONFIRMED, MISMATCH, UNCONFIRMED }

    public String reason() {
        return outcome == Outcome.MISMATCH ? MISMATCH : UNCONFIRMED;
    }

    public static FlinkRuntimeIdentity evaluate(
            Optional<String> expectedImageId,
            List<FlinkComponentProvisioningEvidence> provisioning,
            Optional<FlinkProcessWriteFenceEvidence> fence,
            Optional<PhaseExecutionEvidence> phases) {
        if (provisioning.isEmpty()) {
            return unconfirmed("No Flink process has observed image identity evidence");
        }
        String observedImageId = provisioning.getFirst().imageId();
        if (provisioning.stream().anyMatch(component ->
                !observedImageId.equals(component.imageId())
                        || expectedImageId.filter(id -> !id.equals(component.imageId())).isPresent())) {
            return new FlinkRuntimeIdentity(Outcome.MISMATCH,
                    "Flink process image IDs differ across incarnations or from the declared image ID");
        }
        Map<String, FlinkComponentProvisioningEvidence> byRuntimeId = new HashMap<>();
        for (FlinkComponentProvisioningEvidence component : provisioning) {
            if (byRuntimeId.putIfAbsent(component.runtimeId(), component) != null) {
                return unconfirmed("Duplicate provisioning evidence for Flink process "
                        + component.runtimeId());
            }
        }
        if (phases.isEmpty()) {
            return unconfirmed("No phase evidence establishes the required Flink replacements");
        }
        long restarts = phases.stream().flatMap(phase -> phase.steps().stream())
                .filter(step -> step.kind() == PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER
                        && step.status() == PhaseExecutionEvidence.StepStatus.SUCCEEDED)
                .count();
        long taskManagers = provisioning.stream().filter(component ->
                component.role() == FlinkComponentRole.TASK_MANAGER
                        && component.logicalName().equals("taskmanager-1")).count();
        boolean jobManager = provisioning.stream().anyMatch(component ->
                component.role() == FlinkComponentRole.JOB_MANAGER
                        && component.logicalName().equals("jobmanager-1"));
        // The first runner has one slot per role, and a successful restart creates a new TM.
        if (!jobManager || taskManagers < 1 + restarts) {
            return unconfirmed("Provisioning evidence omits an initial or replacement Flink process");
        }
        if (fence.isEmpty() || fence.orElseThrow().components().isEmpty()) {
            return unconfirmed("No process fence identifies the Flink processes that ran");
        }
        boolean fencedJobManager = false;
        boolean fencedTaskManager = false;
        for (FlinkProcessWriteFenceEvidence.Component component : fence.orElseThrow().components()) {
            FlinkComponentProvisioningEvidence observed = component.runtimeId()
                    .map(byRuntimeId::get).orElse(null);
            if (observed == null || observed.role() != component.role()
                    || !observed.logicalName().equals(component.logicalName())) {
                return unconfirmed("A fenced Flink process has no matching provisioning identity: "
                        + component.logicalName() + " " + component.runtimeId().orElse("unknown"));
            }
            fencedJobManager |= component.role() == FlinkComponentRole.JOB_MANAGER;
            fencedTaskManager |= component.role() == FlinkComponentRole.TASK_MANAGER;
        }
        if (!fencedJobManager || !fencedTaskManager) {
            return unconfirmed("The process fence does not identify both Flink roles");
        }
        return new FlinkRuntimeIdentity(Outcome.CONFIRMED,
                "Every recorded Flink incarnation used local Docker image " + observedImageId
                        + "; initial, replacement, and fenced process identities are covered");
    }

    private static FlinkRuntimeIdentity unconfirmed(String detail) {
        return new FlinkRuntimeIdentity(Outcome.UNCONFIRMED, detail);
    }
}
