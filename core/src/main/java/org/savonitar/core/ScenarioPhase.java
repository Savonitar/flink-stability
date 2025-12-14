package org.savonitar.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

class ScenarioPhase {

    @JsonProperty("name")
    private String name;

    @JsonProperty("steps")
    private List<ScenarioStep> steps;

    // Repeat the whole phase N times (default 1)
    @JsonProperty("repeat")
    private Integer repeat;

    public ScenarioPhase() {
    }

    public ScenarioPhase(String name, List<ScenarioStep> steps, Integer repeat) {
        this.name = name;
        this.steps = steps;
        this.repeat = repeat;
    }

    public String getName() {
        return name;
    }

    public List<ScenarioStep> getSteps() {
        return steps;
    }

    public Integer getRepeat() {
        return repeat;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSteps(List<ScenarioStep> steps) {
        this.steps = steps;
    }

    public void setRepeat(Integer repeat) {
        this.repeat = repeat;
    }

    public int effectiveRepeat() {
        return (repeat == null || repeat < 1) ? 1 : repeat;
    }

    @Override
    public String toString() {
        return "ScenarioPhase{" +
                "name='" + name + '\'' +
                ", steps=" + steps +
                ", repeat=" + repeat +
                '}';
    }
}