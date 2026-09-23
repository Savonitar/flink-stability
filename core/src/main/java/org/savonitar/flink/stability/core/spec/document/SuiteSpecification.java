package org.savonitar.flink.stability.core.spec.document;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;

/** A structurally valid v1 suite document. */
public final class SuiteSpecification extends LoadedSpecification {
    SuiteSpecification(Path source, ObjectNode document) {
        super(source, DocumentKind.SUITE, document);
    }
}
