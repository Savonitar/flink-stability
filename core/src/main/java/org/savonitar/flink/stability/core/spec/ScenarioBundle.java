package org.savonitar.flink.stability.core.spec;

import java.util.Objects;

/** A scenario and its single, validated expected-result sibling. */
public record ScenarioBundle(
        ScenarioSpecification scenario,
        ExpectedResultSpecification expectedResult) {
    public ScenarioBundle {
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(expectedResult, "expectedResult");
    }
}
