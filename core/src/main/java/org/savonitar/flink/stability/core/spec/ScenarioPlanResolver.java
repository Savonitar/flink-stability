package org.savonitar.flink.stability.core.spec;

import java.util.Objects;

/** Resolves scenario parameters and then selects the committed expected-result contract. */
public final class ScenarioPlanResolver {
    private final ScenarioParameterResolver parameterResolver;
    private final ExpectedResultSelector expectedResultSelector;

    public ScenarioPlanResolver() {
        this(new ScenarioParameterResolver());
    }

    ScenarioPlanResolver(ScenarioParameterResolver parameterResolver) {
        this.parameterResolver = Objects.requireNonNull(parameterResolver, "parameterResolver");
        this.expectedResultSelector = new ExpectedResultSelector(parameterResolver);
    }

    public ResolvedScenarioPlan resolve(ScenarioBundle bundle, ResolutionRequest request) {
        Objects.requireNonNull(bundle, "bundle");
        ResolvedScenario scenario = parameterResolver.resolve(bundle.scenario(), request);
        return expectedResultSelector.select(bundle, scenario);
    }
}
