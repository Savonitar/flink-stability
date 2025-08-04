package org.savonitar.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ScenarioStep {

    @JsonProperty("type")
    private String type; // start, stop, validate, kill, wait, etc.

    @JsonProperty("component")
    private String component; // kafka, flink, taskmanager, etc.

    @JsonProperty("image")
    private String image;

    @JsonProperty("jar")
    private String jar;

    @JsonProperty("args")
    private List<String> args;

    @JsonProperty("restore_from_savepoint")
    private boolean restoreFromSavepoint;

    @JsonProperty("parallelism")
    private Integer parallelism;

    @JsonProperty("checkpoint_interval")
    private Integer checkpointInterval;

    @JsonProperty("wait_ms")
    private Long waitMs;

    @JsonProperty("validations")
    private List<ValidationRule> validations;

    @JsonProperty("job")
    private String job; // Optional job ID or alias

    public ScenarioStep() {}

    public String getType() {
        return type;
    }

    public String getComponent() {
        return component;
    }

    public String getImage() {
        return image;
    }

    public String getJar() {
        return jar;
    }

    public List<String> getArgs() {
        return args;
    }

    public boolean isRestoreFromSavepoint() {
        return restoreFromSavepoint;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public Integer getCheckpointInterval() {
        return checkpointInterval;
    }

    public Long getWaitMs() {
        return waitMs;
    }

    public List<ValidationRule> getValidations() {
        return validations;
    }

    public String getJob() {
        return job;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public void setJar(String jar) {
        this.jar = jar;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public void setRestoreFromSavepoint(boolean restoreFromSavepoint) {
        this.restoreFromSavepoint = restoreFromSavepoint;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public void setCheckpointInterval(Integer checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public void setWaitMs(Long waitMs) {
        this.waitMs = waitMs;
    }

    public void setValidations(List<ValidationRule> validations) {
        this.validations = validations;
    }

    public void setJob(String job) {
        this.job = job;
    }

    @Override
    public String toString() {
        return "ScenarioStep{" +
                "type='" + type + '\'' +
                ", component='" + component + '\'' +
                ", image='" + image + '\'' +
                ", jar='" + jar + '\'' +
                ", args=" + args +
                ", restoreFromSavepoint=" + restoreFromSavepoint +
                ", parallelism=" + parallelism +
                ", checkpointInterval=" + checkpointInterval +
                ", waitMs=" + waitMs +
                ", validations=" + validations +
                ", job='" + job + '\'' +
                '}';
    }
}
