package org.savonitar.flink.stability.core.spec.document;

import java.nio.file.Path;
import java.util.Objects;

/** A scenario and its single, validated expected-result sibling. */
public record ScenarioBundle(
        ScenarioSpecification scenario,
        ExpectedResultSpecification expectedResult) {
    public ScenarioBundle {
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(expectedResult, "expectedResult");

        String scenarioFilename = scenario.source().getFileName().toString();
        if (!scenarioFilename.equals(scenario.name() + ".yaml")) {
            throw new IllegalArgumentException(
                    "Scenario filename must be '" + scenario.name() + ".yaml'");
        }
        Path expectedSibling = scenario.source().resolveSibling(
                scenario.name() + ".expected.yaml");
        if (!expectedResult.source().equals(expectedSibling)) {
            throw new IllegalArgumentException(
                    "Expected result must be the sibling '" + expectedSibling.getFileName() + "'");
        }
        String target = expectedResult.at("/meta/scenario").textValue();
        if (!scenario.name().equals(target)) {
            throw new IllegalArgumentException(
                    "Expected result targets scenario '" + target + "' instead of '" + scenario.name() + "'");
        }
        String requiredExpectedName = scenario.name() + ".expected";
        if (!requiredExpectedName.equals(expectedResult.name())) {
            throw new IllegalArgumentException(
                    "Expected-result name must be '" + requiredExpectedName + "'");
        }
    }
}
