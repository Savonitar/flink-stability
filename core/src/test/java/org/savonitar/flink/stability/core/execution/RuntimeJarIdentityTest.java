package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeJarIdentityTest {
    static final FlinkRuntimeTarget.RuntimeJar JAR = new FlinkRuntimeTarget.RuntimeJar(
            "/opt/flink/lib/flink-dist-2.2.0.jar", "c".repeat(64));
    private static final String JM = "coordinator-log";
    private static final String OLD_TM = "old-worker-log";
    private static final String NEW_TM = "replacement-log";

    @Test
    void provenanceIsNotClaimedWithoutAnExplicitRequest() {
        assertTrue(FlinkRuntimeIdentity.evaluateRuntimeJar(
                FlinkRuntimeIdentityTest.expected(Optional.empty()), provisioning(),
                Optional.of(origins())).isEmpty());
    }

    @Test
    void confirmsAllThreeIncarnationsWithoutJoiningByListOrder() {
        FlinkRuntimeIdentity identity = evaluate(provisioning(), Optional.of(origins()));

        assertEquals(FlinkRuntimeIdentity.Outcome.CONFIRMED, identity.outcome());
        assertEquals("subject.flink.runtime-jar-confirmed", identity.reason());
    }

    @Test
    void requestedProvenanceRequiresEachObservedJarAndItsExpectedHash() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(FlinkRuntimeIdentityTest.provisioning(2), Optional.of(origins())).outcome());
        List<FlinkComponentProvisioningEvidence> wrong = new ArrayList<>(provisioning());
        wrong.set(1, wrong.get(1).withRuntimeJarEvidence(new FlinkRuntimeTarget.RuntimeJar(
                JAR.containerPath(), "d".repeat(64)), OLD_TM));
        FlinkRuntimeIdentity mismatch = evaluate(wrong, Optional.of(origins()));

        assertEquals(FlinkRuntimeIdentity.Outcome.MISMATCH, mismatch.outcome());
        assertEquals(FlinkRuntimeIdentity.RUNTIME_JAR_MISMATCH, mismatch.reason());
    }

    @Test
    void everyObservedLoadOfEitherRuntimeClassMustUseTheVerifiedPath() {
        SubjectClassOrigins foreign = new SubjectClassOrigins(JAR.containerPath(), List.of(
                origin(JM, FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS),
                origin(OLD_TM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS),
                new SubjectClassOrigins.ProcessOrigin(NEW_TM, Map.of(
                        FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS, List.of(JAR.containerPath()),
                        FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS, List.of("/other.jar")))),
                Optional.empty());

        assertEquals(FlinkRuntimeIdentity.Outcome.MISMATCH,
                evaluate(provisioning(), Optional.of(foreign)).outcome());
    }

    @Test
    void aReplacementCannotHideTheMissingPredecessorOrItsMissingRuntimeClass() {
        for (List<SubjectClassOrigins.ProcessOrigin> logs : List.of(
                List.of(origin(JM, FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS),
                        origin(NEW_TM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS)),
                List.of(origin(JM, FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS),
                        new SubjectClassOrigins.ProcessOrigin(OLD_TM, Map.of()),
                        origin(NEW_TM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS)))) {
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluate(provisioning(), Optional.of(new SubjectClassOrigins(
                            JAR.containerPath(), logs, Optional.empty()))).outcome());
        }
    }

    @Test
    void missingUnreadableDuplicateAndUnknownLogsFailClosed() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(provisioning(), Optional.empty()).outcome());
        List<SubjectClassOrigins.ProcessOrigin> duplicate = new ArrayList<>(origins().processes());
        duplicate.add(duplicate.getFirst());
        List<SubjectClassOrigins.ProcessOrigin> unknown = new ArrayList<>(origins().processes());
        unknown.add(origin("unregistered-process", FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS));
        for (SubjectClassOrigins invalid : List.of(
                new SubjectClassOrigins(JAR.containerPath(), origins().processes(),
                        Optional.of("Unreadable old process log")),
                new SubjectClassOrigins(JAR.containerPath(), duplicate, Optional.empty()),
                new SubjectClassOrigins(JAR.containerPath(), unknown, Optional.empty()))) {
            assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                    evaluate(provisioning(), Optional.of(invalid)).outcome());
        }
    }

    @Test
    void twoPhysicalProcessesCannotClaimTheSameLog() {
        List<FlinkComponentProvisioningEvidence> duplicate = new ArrayList<>(provisioning());
        duplicate.set(2, duplicate.get(2).withRuntimeJarEvidence(JAR, OLD_TM));

        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(duplicate, Optional.of(origins())).outcome());
    }

    @Test
    void runtimeClassesCannotBeSplitAcrossTheWrongRoles() {
        SubjectClassOrigins wrongRoles = new SubjectClassOrigins(JAR.containerPath(), List.of(
                origin(JM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS),
                origin(OLD_TM, FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS),
                origin(NEW_TM, FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS)), Optional.empty());

        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(provisioning(), Optional.of(wrongRoles)).outcome());
    }

    @Test
    void omittingAnExpectedSlotFromBothInventoriesOrChangingTheExpectedPathCannotConfirm() {
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(provisioning().subList(1, 3), Optional.of(new SubjectClassOrigins(
                        JAR.containerPath(), List.of(
                                origin(OLD_TM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS),
                                origin(NEW_TM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS)),
                        Optional.empty()))).outcome());
        assertEquals(FlinkRuntimeIdentity.Outcome.UNCONFIRMED,
                evaluate(provisioning(), Optional.of(new SubjectClassOrigins(
                        "/other.jar", origins().processes(), Optional.empty()))).outcome());
    }

    private static FlinkRuntimeIdentity evaluate(
            List<FlinkComponentProvisioningEvidence> components, Optional<SubjectClassOrigins> origins) {
        return FlinkRuntimeIdentity.evaluateRuntimeJar(expected(), components, origins).orElseThrow();
    }

    static FlinkRuntimeIdentity.ExpectedTarget expected() {
        var image = FlinkRuntimeIdentityTest.expected(Optional.empty());
        return new FlinkRuntimeIdentity.ExpectedTarget(image.imageId(), image.components(), Optional.of(JAR));
    }

    static List<FlinkComponentProvisioningEvidence> provisioning() {
        var components = FlinkRuntimeIdentityTest.provisioning(2);
        return List.of(components.get(0).withRuntimeJarEvidence(JAR, JM),
                components.get(1).withRuntimeJarEvidence(JAR, OLD_TM),
                components.get(2).withRuntimeJarEvidence(JAR, NEW_TM));
    }

    private static SubjectClassOrigins origins() {
        return new SubjectClassOrigins(JAR.containerPath(), List.of(
                origin(NEW_TM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS),
                origin(JM, FlinkRuntimeIdentity.RESOURCE_MANAGER_CLASS),
                origin(OLD_TM, FlinkRuntimeIdentity.TASK_EXECUTOR_CLASS)), Optional.empty());
    }

    private static SubjectClassOrigins.ProcessOrigin origin(String process, String requiredClass) {
        return new SubjectClassOrigins.ProcessOrigin(process, Map.of(requiredClass, List.of(JAR.containerPath())));
    }
}
