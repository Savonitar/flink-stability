package org.savonitar.core;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScenarioPhase {
    @JsonProperty("flink_version")
    private String flinkVersion;
    
    @JsonProperty("jar")
    private String jar;
    
    @JsonProperty("parallelism")
    private int parallelism;
    
    @JsonProperty("checkpoint_interval")
    private int checkpointInterval;
    
    @JsonProperty("total_records")
    private int totalRecords;
    
    // Default constructor for Jackson
    public ScenarioPhase() {}
    
    public ScenarioPhase(String flinkVersion, String jar, int parallelism, 
                       int checkpointInterval, int totalRecords) {
        this.flinkVersion = flinkVersion;
        this.jar = jar;
        this.parallelism = parallelism;
        this.checkpointInterval = checkpointInterval;
        this.totalRecords = totalRecords;
    }
    
    // Getters
    public String getFlinkVersion() { return flinkVersion; }
    public String getJar() { return jar; }
    public int getParallelism() { return parallelism; }
    public int getCheckpointInterval() { return checkpointInterval; }
    public int getTotalRecords() { return totalRecords; }
    
    // Setters
    public void setFlinkVersion(String flinkVersion) { this.flinkVersion = flinkVersion; }
    public void setJar(String jar) { this.jar = jar; }
    public void setParallelism(int parallelism) { this.parallelism = parallelism; }
    public void setCheckpointInterval(int checkpointInterval) { this.checkpointInterval = checkpointInterval; }
    public void setTotalRecords(int totalRecords) { this.totalRecords = totalRecords; }
    
    @Override
    public String toString() {
        return "ScenarioPhase{" +
                "flinkVersion='" + flinkVersion + '\'' +
                ", jar='" + jar + '\'' +
                ", parallelism=" + parallelism +
                ", checkpointInterval=" + checkpointInterval +
                ", totalRecords=" + totalRecords +
                '}';
    }
}
