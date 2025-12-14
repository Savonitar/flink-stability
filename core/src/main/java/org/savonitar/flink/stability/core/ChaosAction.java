package org.savonitar.flink.stability.core;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ChaosAction {
    @JsonProperty("type")
    private String type;  // e.g. "kill-taskmanager", "rescale", etc.
    
    @JsonProperty("after_seconds")
    private int afterSeconds;
    
    // Default constructor for Jackson
    public ChaosAction() {}
    
    public ChaosAction(String type, int afterSeconds) {
        this.type = type;
        this.afterSeconds = afterSeconds;
    }
    

    public String getType() { return type; }
    public int getAfterSeconds() { return afterSeconds; }
        // Setters
    public void setType(String type) { this.type = type; }
    public void setAfterSeconds(int afterSeconds) { this.afterSeconds = afterSeconds; }
    
    @Override
    public String toString() {
        return "ChaosAction{" +
                "type='" + type + '\'' +
                ", afterSeconds=" + afterSeconds +
                '}';
    }
}
