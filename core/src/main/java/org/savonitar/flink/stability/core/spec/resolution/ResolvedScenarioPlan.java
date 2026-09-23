package org.savonitar.flink.stability.core.spec.resolution;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/** A resolved scenario plus the exact committed expectation selected for it. */
public final class ResolvedScenarioPlan {
    private final ResolvedScenario scenario;
    private final SelectedExpectation selectedExpectation;

    ResolvedScenarioPlan(ResolvedScenario scenario, SelectedExpectation selectedExpectation) {
        this.scenario = Objects.requireNonNull(scenario, "scenario");
        this.selectedExpectation = Objects.requireNonNull(selectedExpectation, "selectedExpectation");
        String expectedScenario = selectedExpectation.specification().document()
                .at("/meta/scenario").textValue();
        if (!scenario.template().name().equals(expectedScenario)) {
            throw new IllegalArgumentException("Expected-result contract targets another scenario");
        }
        if (scenario.isExperiment() == selectedExpectation.expectation().has("outcome")) {
            throw new IllegalArgumentException("Selected expectation shape does not match the scenario");
        }
    }

    public ResolvedScenario scenario() {
        return scenario;
    }

    public SelectedExpectation selectedExpectation() {
        return selectedExpectation;
    }

    public ObjectNode expectationFor(ScenarioSide side) {
        scenario.side(side);
        return selectedExpectation.expectationFor(side);
    }
}
