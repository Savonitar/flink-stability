package org.savonitar.core;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValidationRule {
    @JsonProperty("type")
    private String type;  // e.g. "kafka-count", "kafka-unique-ids"
    
    @JsonProperty("topic")
    private String topic;
    
    @JsonProperty("expected_records")
    private int expectedRecords;
    
    // Default constructor for Jackson
    public ValidationRule() {}
    
    public ValidationRule(String type, String topic, int expectedRecords) {
        this.type = type;
        this.topic = topic;
        this.expectedRecords = expectedRecords;
    }
    

    public String getType() { return type; }
    public String getTopic() { return topic; }
    public int getExpectedRecords() { return expectedRecords; }
        // Setters
    public void setType(String type) { this.type = type; }
    public void setTopic(String topic) { this.topic = topic; }
    public void setExpectedRecords(int expectedRecords) { this.expectedRecords = expectedRecords; }
    
    @Override
    public String toString() {
        return "ValidationRule{" +
                "type='" + type + '\'' +
                ", topic='" + topic + '\'' +
                ", expectedRecords=" + expectedRecords +
                '}';
    }
}
