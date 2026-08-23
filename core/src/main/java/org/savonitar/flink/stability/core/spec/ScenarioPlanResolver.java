package org.savonitar.flink.stability.core.spec;

import java.util.Objects;

/** Resolves a scenario and completes its Docker-free semantic preflight. */
public final class ScenarioPlanResolver {
    private final ScenarioParameterResolver parameterResolver;
    private final ExpectedResultSelector expectedResultSelector;
    private final ScenarioPreflightValidator preflightValidator;

    public ScenarioPlanResolver() {
        this(new ScenarioParameterResolver());
    }

    ScenarioPlanResolver(ScenarioParameterResolver parameterResolver) {
        this.parameterResolver = Objects.requireNonNull(parameterResolver, "parameterResolver");
        this.expectedResultSelector = new ExpectedResultSelector(parameterResolver);
        this.preflightValidator = new ScenarioPreflightValidator();
    }

    public ResolvedScenarioPlan resolve(ScenarioBundle bundle, ResolutionRequest request) {
        Objects.requireNonNull(bundle, "bundle");
        ResolvedScenario scenario = parameterResolver.resolve(bundle.scenario(), request);
        preflightValidator.validateScenario(scenario);
        ResolvedScenarioPlan selected = expectedResultSelector.select(bundle, scenario);
        return preflightValidator.validate(selected);
    }
}
