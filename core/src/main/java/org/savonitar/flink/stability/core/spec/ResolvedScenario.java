package org.savonitar.flink.stability.core.spec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** The parameter-resolved executable side or differential pair for one scenario. */
public final class ResolvedScenario {
    private final ScenarioSpecification template;
    private final Map<String, EffectiveParameter> commonEffectiveParameters;
    private final ResolvedExperiment experiment;
    private final List<ResolvedSide> sides;

    ResolvedScenario(
            ScenarioSpecification template,
            Map<String, EffectiveParameter> commonEffectiveParameters,
            ResolvedExperiment experiment,
            List<ResolvedSide> sides) {
        this.template = Objects.requireNonNull(template, "template");
        this.commonEffectiveParameters = Collections.unmodifiableMap(
                new LinkedHashMap<>(commonEffectiveParameters));
        this.experiment = experiment;
        this.sides = List.copyOf(sides);
        Set<ScenarioSide> topology = this.sides.stream()
                .map(ResolvedSide::side)
                .collect(Collectors.toUnmodifiableSet());
        if (experiment == null) {
            if (this.sides.size() != 1 || !topology.equals(Set.of(ScenarioSide.SINGLE))) {
                throw new IllegalArgumentException("A plain scenario must contain exactly its SINGLE side");
            }
        } else {
            if (this.sides.size() != 2
                    || !topology.equals(Set.of(ScenarioSide.BASELINE, ScenarioSide.CANDIDATE))) {
                throw new IllegalArgumentException(
                        "An experiment must contain exactly one BASELINE and one CANDIDATE side");
            }
            if (experiment.varies().stream().anyMatch(this.commonEffectiveParameters::containsKey)) {
                throw new IllegalArgumentException(
                        "Common effective parameters must exclude experiment.varies");
            }
        }
    }

    public ScenarioSpecification template() {
        return template;
    }

    public Map<String, EffectiveParameter> commonEffectiveParameters() {
        return commonEffectiveParameters;
    }

    public List<ResolvedSide> sides() {
        return sides;
    }

    public Optional<ResolvedExperiment> experiment() {
        return Optional.ofNullable(experiment);
    }

    public boolean isExperiment() {
        return experiment != null;
    }

    public ResolvedSide side(ScenarioSide side) {
        return sides.stream()
                .filter(candidate -> candidate.side() == side)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No resolved side " + side));
    }
}
