package org.savonitar.flink.stability.core;

import java.util.List;

public class ScenarioFile {
    private Scenario scenario;
    private List<ChaosAction> chaos;
    private List<ValidationRule> validate;
    
    // Default constructor for Jackson
    public ScenarioFile() {}
    
    public ScenarioFile(Scenario scenario, List<ChaosAction> chaos, List<ValidationRule> validate) {
        this.scenario = scenario;
        this.chaos = chaos;
        this.validate = validate;
    }
    
    // Getters
    public Scenario getScenario() { return scenario; }
    public List<ChaosAction> getChaos() { return chaos; }
    public List<ValidationRule> getValidate() { return validate; }
    
    // Setters
    public void setScenario(Scenario scenario) { this.scenario = scenario; }
    public void setChaos(List<ChaosAction> chaos) { this.chaos = chaos; }
    public void setValidate(List<ValidationRule> validate) { this.validate = validate; }
    
    @Override
    public String toString() {
        return "ScenarioFile{" +
                "scenario=" + scenario +
                ", chaos=" + chaos +
                ", validate=" + validate +
                '}';
    }
}
