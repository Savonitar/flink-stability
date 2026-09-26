package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlinkRuntimeIdentityTest {
    static final String IMAGE_ID = "sha256:" + "a".repeat(64);
    static final String OTHER_IMAGE_ID = "sha256:" + "b".repeat(64);

    @Test
    void absentPhasesCannotEstablishReplacementCoverage() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(Optional.empty(), provisioning(1),
                        Optional.of(fence(1)), Optional.empty()).outcome());
    }

    @Test
    void confirmsInitialAndReplacementContainersAgainstTheExpectedLocalImage() {
        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                evaluate(provisioning(2), Optional.of(IMAGE_ID), fence(2), restarted()).outcome());
    }

    @Test
    void missingOrMixedImageEvidenceCannotConfirmAnUnpinnedTag() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(List.of(), Optional.empty(), fence(1), noPhases()).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.MISMATCH,
                evaluate(List.of(component("jobmanager-1", "jm", IMAGE_ID),
                                component("taskmanager-1", "tm-1", OTHER_IMAGE_ID)),
                        Optional.empty(), fence(1), noPhases()).outcome());
    }

    @Test
    void matchingObservedIdsDoNotOverrideTheDeclaredImageId() {
        assertEquals(FlinkRuntimeIdentity.Outcome.MISMATCH,
                evaluate(provisioning(1), Optional.of(OTHER_IMAGE_ID),
                        fence(1), noPhases()).outcome());
    }

    @Test
    void theSameTagCannotHideAChangedReplacementImage() {
        assertEquals(FlinkRuntimeIdentity.Outcome.MISMATCH,
                evaluate(List.of(component("jobmanager-1", "jm", IMAGE_ID),
                                component("taskmanager-1", "tm-1", IMAGE_ID),
                                component("taskmanager-1", "tm-2", OTHER_IMAGE_ID)),
                        Optional.empty(), fence(2), restarted()).outcome());
    }

    @Test
    void fenceIdsAndSuccessfulRestartsBothRequireProvisioningEvidence() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(provisioning(1), Optional.empty(), fence(2), noPhases()).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(List.of(component("jobmanager-1", "jm", IMAGE_ID),
                                component("taskmanager-1", "tm-2", IMAGE_ID)),
                        Optional.empty(), fence(2), restarted()).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(List.of(component("jobmanager-1", "jm", IMAGE_ID),
                                component("taskmanager-1", "tm-2", IMAGE_ID),
                                component("taskmanager-1", "tm-2", IMAGE_ID)),
                        Optional.empty(), fence(2), restarted()).outcome());
    }

    @Test
    void missingRoleOrUnknownPhysicalProcessCannotConfirm() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(List.of(component("taskmanager-1", "tm-1", IMAGE_ID)),
                        Optional.empty(), fence(1), noPhases()).outcome());
        FlinkProcessWriteFenceEvidence unknown = new FlinkProcessWriteFenceEvidence(List.of(
                new FlinkProcessWriteFenceEvidence.Component("jobmanager-1",
                        FlinkComponentRole.JOB_MANAGER, Optional.empty(),
                        FlinkProcessWriteFenceEvidence.Outcome.ALREADY_STOPPED)), Instant.EPOCH);
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(provisioning(1), Optional.empty(), unknown, noPhases()).outcome());
    }

    private static FlinkRuntimeIdentity evaluate(
            List<FlinkComponentProvisioningEvidence> components, Optional<String> expected,
            FlinkProcessWriteFenceEvidence fence, PhaseExecutionEvidence phases) {
        return FlinkRuntimeIdentity.evaluate(expected, components, Optional.of(fence), Optional.of(phases));
    }

    static List<FlinkComponentProvisioningEvidence> provisioning(int taskManagers) {
        java.util.ArrayList<FlinkComponentProvisioningEvidence> components = new java.util.ArrayList<>();
        components.add(component("jobmanager-1", "jm", IMAGE_ID));
        for (int index = 1; index <= taskManagers; index++) {
            components.add(component("taskmanager-1", "tm-" + index, IMAGE_ID));
        }
        return List.copyOf(components);
    }

    static FlinkComponentProvisioningEvidence component(String name, String id, String imageId) {
        return FlinkComponentProvisioningEvidence.verified(name,
                name.startsWith("jobmanager") ? FlinkComponentRole.JOB_MANAGER
                        : FlinkComponentRole.TASK_MANAGER,
                id, "flink:2.2.0", imageId, "0".repeat(64), "1".repeat(64), List.of());
    }

    static FlinkProcessWriteFenceEvidence fence(int incarnation) {
        return new FlinkProcessWriteFenceEvidence(List.of(
                new FlinkProcessWriteFenceEvidence.Component("jobmanager-1", FlinkComponentRole.JOB_MANAGER,
                        Optional.of("jm"), FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED),
                new FlinkProcessWriteFenceEvidence.Component("taskmanager-1", FlinkComponentRole.TASK_MANAGER,
                        Optional.of("tm-" + incarnation), FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED)),
                Instant.EPOCH);
    }

    private static PhaseExecutionEvidence restarted() {
        return new PhaseExecutionEvidence(List.of(new PhaseExecutionEvidence.StepEvidence(0,
                "restore", "$/phases/0/steps/0", List.of(),
                PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER,
                PhaseExecutionEvidence.StepStatus.SUCCEEDED, "restarted")));
    }

    private static PhaseExecutionEvidence noPhases() {
        return new PhaseExecutionEvidence(List.of());
    }
}
