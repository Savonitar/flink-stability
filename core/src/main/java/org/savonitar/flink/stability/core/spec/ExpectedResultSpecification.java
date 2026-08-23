package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;

/** A structurally valid v1 expected-result document. */
public final class ExpectedResultSpecification extends LoadedSpecification {
    ExpectedResultSpecification(Path source, ObjectNode document) {
        super(source, DocumentKind.EXPECTED_RESULT, document);
    }
}
