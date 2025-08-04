package org.savonitar.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ScenarioPhase {

    @JsonProperty("name")
    private String name;

    @JsonProperty("steps")
    private List<ScenarioStep> steps;

    public ScenarioPhase() {
    }

    public ScenarioPhase(String name, List<ScenarioStep> steps) {
        this.name = name;
        this.steps = steps;
    }

    public String getName() {
        return name;
    }

    public List<ScenarioStep> getSteps() {
        return steps;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSteps(List<ScenarioStep> steps) {
        this.steps = steps;
    }

    @Override
    public String toString() {
        return "ScenarioPhase{" +
                "name='" + name + '\'' +
                ", steps=" + steps +
                '}';
    }
}
