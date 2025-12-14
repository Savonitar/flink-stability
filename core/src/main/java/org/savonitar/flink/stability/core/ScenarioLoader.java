package org.savonitar.flink.stability.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

public class ScenarioLoader {

    public static ScenarioFile load(String path) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(new File(path), ScenarioFile.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load scenario: " + path, e);
        }
    }
}
