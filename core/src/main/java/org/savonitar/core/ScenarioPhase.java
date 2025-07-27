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
    
    @JsonProperty("processing_delay_ms")
    private int processingDelayMs;
    
    // Default constructor for Jackson
    public ScenarioPhase() {}
    
    public ScenarioPhase(String flinkVersion, String jar, int parallelism, 
                       int checkpointInterval, int processingDelayMs) {
        this.flinkVersion = flinkVersion;
        this.jar = jar;
        this.parallelism = parallelism;
        this.checkpointInterval = checkpointInterval;
        this.processingDelayMs = processingDelayMs;
    }
    
    public String getFlinkVersion() { return flinkVersion; }
    public String getJar() { return jar; }
    public int getParallelism() { return parallelism; }
    public int getCheckpointInterval() { return checkpointInterval; }
    public int getProcessingDelayMs() { return processingDelayMs; }
    public void setFlinkVersion(String flinkVersion) { this.flinkVersion = flinkVersion; }
    public void setJar(String jar) { this.jar = jar; }
    public void setParallelism(int parallelism) { this.parallelism = parallelism; }
    public void setCheckpointInterval(int checkpointInterval) { this.checkpointInterval = checkpointInterval; }
    public void setProcessingDelayMs(int processingDelayMs) { this.processingDelayMs = processingDelayMs; }
    
    @Override
    public String toString() {
        return "ScenarioPhase{" +
                "flinkVersion='" + flinkVersion + '\'' +
                ", jar='" + jar + '\'' +
                ", parallelism=" + parallelism +
                ", checkpointInterval=" + checkpointInterval +
                ", processingDelayMs=" + processingDelayMs +
                '}';
    }
}
