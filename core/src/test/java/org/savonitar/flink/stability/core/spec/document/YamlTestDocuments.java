package org.savonitar.flink.stability.core.spec.document;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.nio.file.Path;

/** Reads handwritten fixtures directly, independently of the production specification loader. */
final class YamlTestDocuments {
    private static final YAMLMapper YAML = new YAMLMapper();

    private YamlTestDocuments() {
    }

    static ObjectNode read(Path source) throws IOException {
        return YAML.readValue(source.toFile(), ObjectNode.class);
    }

    static ArrayNode readArray(String content) throws IOException {
        return YAML.readValue(content, ArrayNode.class);
    }

    static Path write(Path target, ObjectNode document) throws IOException {
        YAML.writeValue(target.toFile(), document);
        return target;
    }
}
