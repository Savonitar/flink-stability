package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.util.Objects;

/** A structurally valid v1 document and its source identity. */
public abstract sealed class LoadedSpecification
        permits ScenarioSpecification, ExpectedResultSpecification, SuiteSpecification {
    private final Path source;
    private final DocumentKind kind;
    private final ObjectNode document;

    LoadedSpecification(Path source, DocumentKind kind, ObjectNode document) {
        this.source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        this.kind = Objects.requireNonNull(kind, "kind");
        this.document = Objects.requireNonNull(document, "document").deepCopy();
    }

    public Path source() {
        return source;
    }

    public String format() {
        return document.path("format").textValue();
    }

    public DocumentKind kind() {
        return kind;
    }

    public String name() {
        return document.path("meta").path("name").textValue();
    }

    /** Returns a defensive copy because Jackson tree nodes are mutable. */
    public ObjectNode document() {
        return document.deepCopy();
    }

    public JsonNode at(String jsonPointer) {
        JsonNode value = document.at(jsonPointer);
        return value.isMissingNode() ? value : value.deepCopy();
    }
}
