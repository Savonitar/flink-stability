package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlinkRuntimeIdentityTest {
    static final String IMAGE_ID = "sha256:" + "a".repeat(64);
    static final String OTHER_IMAGE_ID = "sha256:" + "b".repeat(64);

    @Test
    void renamedExpectedSlotsStillConfirmIncludingAReplacement() {
        FlinkRuntimeIdentity.ExpectedTarget expected = new FlinkRuntimeIdentity.ExpectedTarget(
                Optional.of(IMAGE_ID), Map.of("coordinator", FlinkComponentRole.JOB_MANAGER,
                        "worker-blue", FlinkComponentRole.TASK_MANAGER));
        FlinkComponentProvisioningEvidence coordinator = component(
                "coordinator", FlinkComponentRole.JOB_MANAGER, "jm", IMAGE_ID);
        FlinkComponentProvisioningEvidence first = component(
                "worker-blue", FlinkComponentRole.TASK_MANAGER, "tm-old", IMAGE_ID);
        FlinkComponentProvisioningEvidence replacement = component(
                "worker-blue", FlinkComponentRole.TASK_MANAGER, "tm-new", IMAGE_ID);

        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                FlinkRuntimeIdentity.evaluate(expected, List.of(coordinator, first, replacement),
                        Optional.of(fenceFor(List.of(coordinator, replacement))),
                        Optional.of(restarted())).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(expected, List.of(coordinator, replacement),
                        Optional.of(fenceFor(List.of(coordinator, replacement))),
                        Optional.of(restarted())).outcome());
    }

    @Test
    void omissionFromBothObservationsCannotHideAnExpectedSlot() {
        FlinkRuntimeIdentity.ExpectedTarget expected = twoTaskManagerSlots();
        // Only the evaluator accepts this expectation; the executable plan still permits one TM.
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(expected, provisioning(1), Optional.of(fence(1)),
                        Optional.of(noPhases())).outcome());
    }

    @Test
    void twoIncarnationsOfOneSlotCannotStandInForAnotherExpectedSlot() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(twoTaskManagerSlots(), provisioning(2),
                        Optional.of(fence(2)), Optional.of(noPhases())).outcome());
    }

    @Test
    void everyExpectedSlotMustAlsoAppearInTheFence() {
        java.util.ArrayList<FlinkComponentProvisioningEvidence> components =
                new java.util.ArrayList<>(provisioning(1));
        components.add(component("taskmanager-2", "tm-other", IMAGE_ID));
        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                FlinkRuntimeIdentity.evaluate(twoTaskManagerSlots(), components,
                        Optional.of(fenceFor(components)), Optional.of(noPhases())).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(twoTaskManagerSlots(), components,
                        Optional.of(fence(1)), Optional.of(noPhases())).outcome());
    }

    @Test
    void wrongRolesAndUnexpectedSlotsCannotSubstituteForPlannedComponents() {
        for (FlinkComponentProvisioningEvidence foreign : List.of(
                component("taskmanager-1", FlinkComponentRole.JOB_MANAGER, "tm-1", IMAGE_ID),
                component("unplanned-worker", FlinkComponentRole.TASK_MANAGER, "tm-1", IMAGE_ID))) {
            List<FlinkComponentProvisioningEvidence> components = List.of(
                    component("jobmanager-1", "jm", IMAGE_ID), foreign);
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    FlinkRuntimeIdentity.evaluate(expected(Optional.empty()), components,
                            Optional.of(fenceFor(components)), Optional.of(noPhases())).outcome());
        }
    }

    @Test
    void untargetedRestartEvidenceCannotBeSpreadAcrossMultipleExpectedSlots() {
        java.util.ArrayList<FlinkComponentProvisioningEvidence> components =
                new java.util.ArrayList<>(provisioning(2));
        components.add(component("taskmanager-2", "tm-other", IMAGE_ID));
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(twoTaskManagerSlots(), components,
                        Optional.of(fenceFor(components)), Optional.of(restarted())).outcome());
    }

    @Test
    void absentPhasesCannotEstablishReplacementCoverage() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(expected(Optional.empty()), provisioning(1),
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
        return FlinkRuntimeIdentity.evaluate(expected(expected), components,
                Optional.of(fence), Optional.of(phases));
    }

    static FlinkRuntimeIdentity.ExpectedTarget expected(Optional<String> imageId) {
        ExecutableScenarioPlan.FlinkCluster plan = new ExecutableScenarioPlan.FlinkCluster(
                "flink:2.2.0", imageId, 1, 1);
        return new FlinkRuntimeIdentity.ExpectedTarget(plan.expectedImageId(), plan.expectedComponents());
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
        return component(name, name.startsWith("jobmanager") ? FlinkComponentRole.JOB_MANAGER
                : FlinkComponentRole.TASK_MANAGER, id, imageId);
    }

    private static FlinkComponentProvisioningEvidence component(
            String name, FlinkComponentRole role, String id, String imageId) {
        return FlinkComponentProvisioningEvidence.verified(name, role,
                id, "flink:2.2.0", imageId, "0".repeat(64), "1".repeat(64), List.of());
    }

    private static FlinkProcessWriteFenceEvidence fenceFor(
            List<FlinkComponentProvisioningEvidence> components) {
        return new FlinkProcessWriteFenceEvidence(components.stream().map(component ->
                new FlinkProcessWriteFenceEvidence.Component(component.logicalName(), component.role(),
                        Optional.of(component.runtimeId()), FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED))
                .toList(), Instant.EPOCH);
    }

    private static FlinkRuntimeIdentity.ExpectedTarget twoTaskManagerSlots() {
        return new FlinkRuntimeIdentity.ExpectedTarget(Optional.empty(), Map.of(
                "jobmanager-1", FlinkComponentRole.JOB_MANAGER,
                "taskmanager-1", FlinkComponentRole.TASK_MANAGER,
                "taskmanager-2", FlinkComponentRole.TASK_MANAGER));
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
