package org.savonitar.core;

import java.util.List;

public class Scenario {
    private List<ScenarioPhase> phases;
    
    // Default constructor for Jackson
    public Scenario() {}
    
    public Scenario(List<ScenarioPhase> phases) {
        this.phases = phases;
    }
    
    // Getter
    public List<ScenarioPhase> getPhases() {
        return phases;
    }
    
    // Setter
    public void setPhases(List<ScenarioPhase> phases) {
        this.phases = phases;
    }
    
    @Override
    public String toString() {
        return "Scenario{" +
                "phases=" + phases +
                '}';
    }
}
