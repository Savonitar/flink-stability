package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;

/** A structurally valid v1 scenario document. */
public final class ScenarioSpecification extends LoadedSpecification {
    ScenarioSpecification(Path source, ObjectNode document) {
        super(source, DocumentKind.SCENARIO, document);
    }
}
