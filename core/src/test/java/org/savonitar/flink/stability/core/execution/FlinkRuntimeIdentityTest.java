package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;

import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

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
                        Optional.of(restarted("worker-blue", "tm-old", "tm-new"))).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(expected, List.of(coordinator, replacement),
                        Optional.of(fenceFor(List.of(coordinator, replacement))),
                        Optional.of(restarted("worker-blue", "tm-old", "tm-new"))).outcome());
    }

    @Test
    void omissionFromBothObservationsCannotHideAnExpectedSlot() {
        FlinkRuntimeIdentity.ExpectedTarget expected = twoTaskManagerSlots();
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
        FlinkComponentProvisioningEvidence other = component("taskmanager-2", "tm-other", IMAGE_ID);
        components.add(other);
        PhaseExecutionEvidence untargeted = new PhaseExecutionEvidence(restarted().steps());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                FlinkRuntimeIdentity.evaluate(twoTaskManagerSlots(), components,
                        Optional.of(fenceFor(List.of(components.getFirst(), components.get(2), other))),
                        Optional.of(untargeted)).outcome());
    }

    @Test
    void independentReplacementsCoverEachTaskManagerSlotAndItsLatestFence() {
        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                evaluateDistributed(distributedProvisioning(), distributedFence(),
                        distributedRestarts()).outcome());
    }

    @Test
    void aReplacementCannotBorrowAnotherLogicalTargetsObservedContainer() {
        List<PhaseExecutionEvidence.TaskManagerRestart> restarts = distributedRestarts();
        PhaseExecutionEvidence.TaskManagerRestart first = restarts.getFirst();
        var wrongTarget = new PhaseExecutionEvidence.TaskManagerRestart(
                first.path(), first.loopIterations(), "taskmanager-1",
                first.previousIdentity(), first.replacementIdentity());
        var borrowedContainer = new PhaseExecutionEvidence.TaskManagerRestart(
                first.path(), first.loopIterations(), first.target(), first.previousIdentity(),
                Optional.of(identity("taskmanager-2", "tm-a-new")));
        for (var invalid : List.of(wrongTarget, borrowedContainer)) {
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluateDistributed(distributedProvisioning(), distributedFence(),
                            List.of(invalid, restarts.get(1))).outcome());
        }
    }

    @Test
    void aNamedRestartCannotConfirmAnUnobservedReplacement() {
        List<FlinkComponentProvisioningEvidence> missingReplacement = distributedProvisioning().stream()
                .filter(component -> !component.runtimeId().equals("tm-b-new"))
                .toList();
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluateDistributed(missingReplacement, distributedFence(),
                        distributedRestarts()).outcome());
    }

    @Test
    void replacementsCannotReusePhysicalOrResourceIdentities() {
        List<PhaseExecutionEvidence.TaskManagerRestart> restarts = distributedRestarts();
        PhaseExecutionEvidence.TaskManagerRestart first = restarts.getFirst();
        for (TaskManagerControl.Identity reused : List.of(
                new TaskManagerControl.Identity("taskmanager-2", "tm-b-old", "resource-tm-b-new"),
                new TaskManagerControl.Identity("taskmanager-2", "tm-b-new", "resource-tm-b-old"),
                new TaskManagerControl.Identity("taskmanager-2", "tm-b-new", "resource-tm-a-old"))) {
            var invalid = new PhaseExecutionEvidence.TaskManagerRestart(
                    first.path(), first.loopIterations(), first.target(), first.previousIdentity(),
                    Optional.of(reused));
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluateDistributed(distributedProvisioning(), distributedFence(),
                            List.of(invalid, restarts.get(1))).outcome());
        }
    }

    @Test
    void aFenceForAnOldIncarnationCannotStandInForItsReplacement() {
        List<FlinkComponentProvisioningEvidence> components = distributedProvisioning();
        FlinkProcessWriteFenceEvidence oldFence = fenceFor(
                List.of(components.getFirst(), components.get(4), components.get(2)));
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluateDistributed(components, oldFence, distributedRestarts()).outcome());
    }

    @Test
    void repeatedRestartsMustContinueTheSameIncarnationChain() {
        var first = restart(0, "taskmanager-1", "tm-1", "tm-2");
        var second = restart(1, "taskmanager-1", "tm-2", "tm-3");
        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                evaluate(provisioning(3), Optional.of(IMAGE_ID), fence(3),
                        phasesWithRestarts(List.of(first, second))).outcome());
        for (var previous : List.of(
                identity("taskmanager-1", "tm-1"),
                new TaskManagerControl.Identity("taskmanager-1", "tm-2", "unrelated-resource"))) {
            var brokenChain = new PhaseExecutionEvidence.TaskManagerRestart(
                    second.path(), second.loopIterations(), second.target(), Optional.of(previous),
                    second.replacementIdentity());
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluate(provisioning(3), Optional.of(IMAGE_ID), fence(3),
                            phasesWithRestarts(List.of(first, brokenChain))).outcome());
        }
    }

    @Test
    void restartRecordsMustMatchSuccessfulStepLocationsIncludingLoopIterations() {
        PhaseExecutionEvidence original = restarted();
        PhaseExecutionEvidence.TaskManagerRestart restart = original.taskManagerRestarts().getFirst();
        for (var mismatched : List.of(
                new PhaseExecutionEvidence.TaskManagerRestart(
                        "$/phases/0/steps/99", restart.loopIterations(), restart.target(),
                        restart.previousIdentity(), restart.replacementIdentity()),
                new PhaseExecutionEvidence.TaskManagerRestart(
                        restart.path(), List.of(new PhaseExecutionEvidence.LoopIteration(
                                "$/phases/0/steps/0", 2, 2)), restart.target(),
                        restart.previousIdentity(), restart.replacementIdentity()))) {
            PhaseExecutionEvidence phases = new PhaseExecutionEvidence(
                    original.steps(), original.taskManagerKills(), List.of(), List.of(mismatched));
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluate(provisioning(2), Optional.of(IMAGE_ID), fence(2), phases).outcome());
        }
    }

    @Test
    void restartPredecessorMustEqualTheActualKillIncludingItsResourceIdentity() {
        PhaseExecutionEvidence original = phasesWithRestarts(List.of(
                restart(0, "taskmanager-1", "tm-1", "tm-2"),
                restart(1, "taskmanager-1", "tm-2", "tm-3")));
        var secondKill = original.taskManagerKills().get(1);
        for (var contradictory : List.of(
                identity("taskmanager-1", "tm-1"),
                new TaskManagerControl.Identity("taskmanager-1", "tm-2", "unrelated-resource"))) {
            var mutatedKill = new PhaseExecutionEvidence.TaskManagerKill(
                    secondKill.path(), secondKill.loopIterations(), secondKill.target(),
                    secondKill.jobBeforeKill(), secondKill.jobManagerTimeBeforeKill(),
                    secondKill.jobManagerTimeAfterKill(),
                    Optional.of(contradictory));
            PhaseExecutionEvidence phases = new PhaseExecutionEvidence(original.steps(),
                    List.of(original.taskManagerKills().getFirst(), mutatedKill),
                    List.of(), original.taskManagerRestarts());
            FlinkRuntimeIdentity result = evaluate(provisioning(3), Optional.of(IMAGE_ID), fence(3), phases);
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED, result.outcome());
            assertEquals("TaskManager restart predecessor does not match its preceding unhealed kill",
                    result.detail());
        }
    }

    @Test
    void killAndRestartPairingRequiresMatchingSuccessfulStepOrderAndLoopLocation() {
        PhaseExecutionEvidence original = restarted();
        var kill = original.taskManagerKills().getFirst();
        var wrongLoopKill = new PhaseExecutionEvidence.TaskManagerKill(
                kill.path(), List.of(new PhaseExecutionEvidence.LoopIteration(
                        "$/phases/0/steps/0", 2, 2)), kill.target(),
                kill.jobBeforeKill(), kill.jobManagerTimeBeforeKill(),
                kill.jobManagerTimeAfterKill(), kill.identity());
        for (var phases : List.of(
                new PhaseExecutionEvidence(original.steps().reversed(), original.taskManagerKills(),
                        List.of(), original.taskManagerRestarts()),
                new PhaseExecutionEvidence(original.steps(), List.of(wrongLoopKill),
                        List.of(), original.taskManagerRestarts()),
                new PhaseExecutionEvidence(List.of(original.steps().getLast()), List.of(),
                        List.of(), original.taskManagerRestarts()))) {
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluate(provisioning(2), Optional.of(IMAGE_ID), fence(2), phases).outcome());
        }
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

    @Test
    void eachKilledJobManagerAddsOneIncarnationToItsOwnLogicalSlot() {
        var components = haProvisioning();
        var faults = List.of(haKill(0, "jobmanager-1", "jm-a", "jm-a-new", "jobmanager-2", "jm-b"),
                haKill(1, "jobmanager-2", "jm-b", "jm-b-new", "jobmanager-1", "jm-a-new"));
        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                evaluateHa(components, fenceFor(List.of(components.get(2), components.get(3), components.get(4))),
                        haPhases(faults)).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluateHa(components, fenceFor(List.of(components.get(0), components.get(1), components.get(2))),
                        haPhases(faults)).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluateHa(components.subList(0, 4), fenceFor(List.of(components.get(2), components.get(3), components.get(4))),
                        haPhases(faults)).outcome());
    }

    @Test
    void leaderReplacementCannotBorrowAnotherSlotOrReuseItsKilledProcess() {
        var components = haProvisioning().subList(0, 4);
        var fence = fenceFor(List.of(components.get(1), components.get(2), components.get(3)));
        for (String replacement : List.of("jm-b", "jm-a", "unobserved")) {
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluateHa(components, fence, haPhases(List.of(haKill(0,
                            "jobmanager-1", "jm-a", replacement, "jobmanager-2", "jm-b")))).outcome());
        }
    }

    @Test
    void leaderFaultMustMatchSuccessfulPhaseLocationAndOrder() {
        var components = haProvisioning();
        var faults = List.of(haKill(0, "jobmanager-1", "jm-a", "jm-a-new", "jobmanager-2", "jm-b"),
                haKill(1, "jobmanager-2", "jm-b", "jm-b-new", "jobmanager-1", "jm-a-new"));
        var original = haPhases(faults);
        var fence = fenceFor(List.of(components.get(2), components.get(3), components.get(4)));
        for (var incomplete : List.of(
                new PhaseExecutionEvidence(original.steps()),
                new PhaseExecutionEvidence(original.steps().reversed(), List.of(), List.of(), List.of(), faults),
                new PhaseExecutionEvidence(List.of(), List.of(), List.of(), List.of(), faults))) {
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluateHa(components, fence, incomplete).outcome());
        }
        var staleSuccessor = haKill(1, "jobmanager-2", "jm-b", "jm-b-new", "jobmanager-1", "jm-a");
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluateHa(components, fence, haPhases(List.of(faults.getFirst(), staleSuccessor))).outcome());
    }

    @Test
    void failedLeadershipEffectDoesNotInvalidateKnownPhysicalIdentities() {
        for (var mode : List.of(FlinkHaControl.Mode.PAUSE, FlinkHaControl.Mode.KILL)) {
            var killed = haKill(0, "jobmanager-1", "jm-a", "jm-a-new", "jobmanager-2", "jm-b");
            var original = killed.raw();
            var raw = new FlinkHaControl.LeaderFaultEvidence(new FlinkHaControl.LeaderFaultRequest(
                    mode, Duration.ofSeconds(1), Duration.ofSeconds(10), Optional.empty()),
                    original.before(), Optional.empty(), original.target(), true, false,
                    original.faultState(), mode == FlinkHaControl.Mode.KILL
                            ? original.healedState() : Optional.empty(),
                    1_000, 2_000, 0, 0, false, Optional.empty(), Optional.empty(), Optional.empty(),
                    List.of("New leadership was not confirmed"));
            var fault = new PhaseExecutionEvidence.LeaderFault(killed.path(), List.of(), "job",
                    killed.jobBefore(), killed.jobAfter(), raw);
            var normal = haPhases(List.of(fault));
            var step = normal.steps().getFirst();
            var failed = new PhaseExecutionEvidence(List.of(new PhaseExecutionEvidence.StepEvidence(
                    step.phaseIndex(), step.phaseName(), step.path(), step.loopIterations(), step.kind(),
                    PhaseExecutionEvidence.StepStatus.FAILED, "HA effect unconfirmed")),
                    List.of(), List.of(), List.of(), List.of(fault));
            var components = haProvisioning().subList(0, mode == FlinkHaControl.Mode.KILL ? 4 : 3);
            var fenced = mode == FlinkHaControl.Mode.KILL
                    ? List.of(components.get(1), components.get(2), components.get(3)) : components;
            assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                    evaluateHa(components, fenceFor(fenced), failed).outcome());
            org.junit.jupiter.api.Assertions.assertTrue(FlinkHaEvidence.faultFailure(raw).isPresent());
        }
    }

    @Test
    void pauseDoesNotAuthorizeAnExtraJobManagerIncarnation() {
        var killed = haKill(0, "jobmanager-1", "jm-a", "jm-a-new", "jobmanager-2", "jm-b");
        var raw = killed.raw();
        var paused = new FlinkHaControl.LeaderFaultEvidence(new FlinkHaControl.LeaderFaultRequest(
                FlinkHaControl.Mode.PAUSE, Duration.ofSeconds(1), Duration.ofSeconds(10), Optional.empty()),
                raw.before(), raw.after(), raw.target(), true, true,
                Optional.of(new FlinkHaControl.ProcessState("jm-a", true, true)),
                Optional.of(new FlinkHaControl.ProcessState("jm-a", true, false)),
                1_000, 2_000, 0, 0, false, Optional.empty(), Optional.empty(), Optional.empty(), List.of());
        var fault = new PhaseExecutionEvidence.LeaderFault(killed.path(), List.of(), "job",
                killed.jobBefore(), killed.jobAfter(), paused);
        var initial = haProvisioning().subList(0, 3);
        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED,
                evaluateHa(initial, fenceFor(initial), haPhases(List.of(fault))).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluateHa(haProvisioning().subList(0, 4), fenceFor(initial), haPhases(List.of(fault))).outcome());
    }

    private static List<FlinkComponentProvisioningEvidence> haProvisioning() {
        return List.of(component("jobmanager-1", "jm-a", IMAGE_ID),
                component("jobmanager-2", "jm-b", IMAGE_ID), component("taskmanager-1", "tm", IMAGE_ID),
                component("jobmanager-1", "jm-a-new", IMAGE_ID), component("jobmanager-2", "jm-b-new", IMAGE_ID));
    }

    private static FlinkRuntimeIdentity evaluateHa(List<FlinkComponentProvisioningEvidence> components,
            FlinkProcessWriteFenceEvidence fence, PhaseExecutionEvidence phases) {
        return FlinkRuntimeIdentity.evaluate(new FlinkRuntimeIdentity.ExpectedTarget(Optional.of(IMAGE_ID), Map.of(
                "jobmanager-1", FlinkComponentRole.JOB_MANAGER,
                "jobmanager-2", FlinkComponentRole.JOB_MANAGER,
                "taskmanager-1", FlinkComponentRole.TASK_MANAGER)), components, Optional.of(fence), Optional.of(phases));
    }

    private static PhaseExecutionEvidence.LeaderFault haKill(int index, String logical,
            String oldRuntime, String newRuntime, String otherLogical, String otherRuntime) {
        var before = FlinkHaEvidenceTest.leadership(logical, oldRuntime, "before-" + index);
        var after = FlinkHaEvidenceTest.leadership(otherLogical, otherRuntime, "after-" + index);
        var request = new FlinkHaControl.LeaderFaultRequest(FlinkHaControl.Mode.KILL,
                Duration.ofSeconds(1), Duration.ofSeconds(10), Optional.empty());
        var raw = new FlinkHaControl.LeaderFaultEvidence(request, Optional.of(before), Optional.of(after),
                Optional.of(before.resourceManager()), true, true,
                Optional.of(new FlinkHaControl.ProcessState(oldRuntime, false, false)),
                Optional.of(new FlinkHaControl.ProcessState(newRuntime, true, false)),
                1_000, 2_000, 0, 0, false, Optional.empty(), Optional.empty(), Optional.empty(), List.of());
        var unsampled = new FlinkJobObservation.Attempt(Optional.empty(), Optional.of("outside identity check"));
        return new PhaseExecutionEvidence.LeaderFault("$/phases/0/steps/" + index,
                List.of(), "job", unsampled, unsampled, raw);
    }

    private static PhaseExecutionEvidence haPhases(List<PhaseExecutionEvidence.LeaderFault> faults) {
        var steps = faults.stream().map(fault -> new PhaseExecutionEvidence.StepEvidence(0, "ha",
                fault.path(), fault.loopIterations(), PhaseExecutionEvidence.StepKind.LEADER_FAULT,
                PhaseExecutionEvidence.StepStatus.SUCCEEDED, "faulted and healed")).toList();
        return new PhaseExecutionEvidence(steps, List.of(), List.of(), List.of(), faults);
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

    static FlinkProcessWriteFenceEvidence.Observations healthyProcesses(FlinkProcessWriteFenceEvidence fence) {
        var events = new java.util.ArrayList<FlinkProcessWriteFenceEvidence.Observation>();
        for (var component : fence.components()) {
            for (var moment : List.of(FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE,
                    FlinkProcessWriteFenceEvidence.Moment.AFTER_FENCE_KILL)) {
                events.add(new FlinkProcessWriteFenceEvidence.Observation(component.logicalName(),
                        component.role(), component.runtimeId(), moment, fence.completedAt(),
                        Optional.of(new FlinkHaControl.ProcessState(component.runtimeId().orElseThrow(),
                                moment == FlinkProcessWriteFenceEvidence.Moment.BEFORE_FENCE, false)),
                        false, Optional.empty()));
            }
        }
        return new FlinkProcessWriteFenceEvidence.Observations(events, fence.components(), false);
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
        return restarted("taskmanager-1", "tm-1", "tm-2");
    }

    private static PhaseExecutionEvidence restarted(String target, String previous, String replacement) {
        return phasesWithRestarts(List.of(restart(0, target, previous, replacement)));
    }

    private static TaskManagerControl.Identity identity(String target, String runtimeId) {
        return new TaskManagerControl.Identity(target, runtimeId, "resource-" + runtimeId);
    }

    private static PhaseExecutionEvidence.TaskManagerRestart restart(
            int ordinal, String target, String previous, String replacement) {
        return new PhaseExecutionEvidence.TaskManagerRestart(
                "$/phases/0/steps/" + (ordinal * 2 + 1), List.of(), target,
                Optional.of(identity(target, previous)), Optional.of(identity(target, replacement)));
    }

    private static PhaseExecutionEvidence phasesWithRestarts(
            List<PhaseExecutionEvidence.TaskManagerRestart> restarts) {
        List<PhaseExecutionEvidence.StepEvidence> steps = new ArrayList<>();
        List<PhaseExecutionEvidence.TaskManagerKill> kills = new ArrayList<>();
        for (int index = 0; index < restarts.size(); index++) {
            PhaseExecutionEvidence.TaskManagerRestart restart = restarts.get(index);
            String killPath = "$/phases/0/steps/" + (index * 2);
            kills.add(new PhaseExecutionEvidence.TaskManagerKill(
                    killPath, restart.loopIterations(), restart.target(),
                    new FlinkJobObservation.Attempt(Optional.empty(), Optional.of("not sampled")),
                    OptionalLong.empty(), OptionalLong.empty(), restart.previousIdentity()));
            steps.add(new PhaseExecutionEvidence.StepEvidence(0, "restore", killPath,
                    restart.loopIterations(), PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER,
                    PhaseExecutionEvidence.StepStatus.SUCCEEDED, "killed"));
            steps.add(new PhaseExecutionEvidence.StepEvidence(0, "restore", restart.path(),
                    restart.loopIterations(), PhaseExecutionEvidence.StepKind.RESTART_TASKMANAGER,
                    PhaseExecutionEvidence.StepStatus.SUCCEEDED, "restarted"));
        }
        return new PhaseExecutionEvidence(steps, kills, List.of(), restarts);
    }

    private static List<FlinkComponentProvisioningEvidence> distributedProvisioning() {
        return List.of(
                component("jobmanager-1", "jm", IMAGE_ID),
                component("taskmanager-1", "tm-a-old", IMAGE_ID),
                component("taskmanager-2", "tm-b-old", IMAGE_ID),
                component("taskmanager-2", "tm-b-new", IMAGE_ID),
                component("taskmanager-1", "tm-a-new", IMAGE_ID));
    }

    private static FlinkProcessWriteFenceEvidence distributedFence() {
        List<FlinkComponentProvisioningEvidence> components = distributedProvisioning();
        return fenceFor(List.of(components.getFirst(), components.get(4), components.get(3)));
    }

    private static List<PhaseExecutionEvidence.TaskManagerRestart> distributedRestarts() {
        // Reverse target order proves that replacements are attributed by identity, not position.
        return List.of(restart(0, "taskmanager-2", "tm-b-old", "tm-b-new"),
                restart(1, "taskmanager-1", "tm-a-old", "tm-a-new"));
    }

    private static FlinkRuntimeIdentity evaluateDistributed(
            List<FlinkComponentProvisioningEvidence> components,
            FlinkProcessWriteFenceEvidence fence,
            List<PhaseExecutionEvidence.TaskManagerRestart> restarts) {
        return FlinkRuntimeIdentity.evaluate(twoTaskManagerSlots(), components,
                Optional.of(fence), Optional.of(phasesWithRestarts(restarts)));
    }

    private static PhaseExecutionEvidence noPhases() {
        return new PhaseExecutionEvidence(List.of());
    }
}
