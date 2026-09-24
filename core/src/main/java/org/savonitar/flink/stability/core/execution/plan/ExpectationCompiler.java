package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionScope;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;

import java.util.List;

/** Compiles the selected expected result into the runner's typed expectation (R8.6c, R8.7a). */
final class ExpectationCompiler {

    private ExpectationCompiler() {}

    /**
     * Semantic validation already pairs every expected failure with a registered reason of a
     * declared validator; the runner can evaluate only its one kafka.id-set oracle.
     */
    static void validate(ResolvedScenarioPlan sourcePlan, List<RunnerCapabilityIssue> issues) {
        ObjectNode expectation = sourcePlan.expectationFor(ScenarioSide.SINGLE);
        if (isFailure(expectation) && !ExecutableScenarioPlanCompiler.KAFKA_ID_SET.equals(
                expectation.path("oracle").textValue())) {
            issues.add(new RunnerCapabilityIssue(
                    sourcePlan.selectedExpectation().specification().source(),
                    ResolutionScope.SINGLE,
                    "runner.expectation.outcome-unsupported",
                    sourcePlan.selectedExpectation().originPointer() + "/oracle",
                    "The first runner can expect failures only from the kafka.id-set oracle"));
        }
    }

    static ExecutableScenarioPlan.ExpectedOutcome map(ResolvedScenarioPlan sourcePlan) {
        ObjectNode expectation = sourcePlan.expectationFor(ScenarioSide.SINGLE);
        return isFailure(expectation)
                ? ExecutableScenarioPlan.ExpectedOutcome.failure(
                        expectation.path("oracle").textValue(),
                        expectation.path("reason").textValue())
                : ExecutableScenarioPlan.ExpectedOutcome.pass();
    }

    private static boolean isFailure(ObjectNode expectation) {
        return "fail".equals(expectation.path("outcome").textValue());
    }
}
