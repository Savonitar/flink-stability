package org.savonitar.flink.stability.core.spec;

import java.util.Arrays;
import java.util.Optional;

/** The v1 document kinds understood by the harness. */
public enum DocumentKind {
    SCENARIO("scenario", "/schema/scenario-v1.schema.json"),
    EXPECTED_RESULT("expected-result", "/schema/expected-result-v1.schema.json"),
    SUITE("suite", "/schema/suite-v1.schema.json");

    private final String value;
    private final String schemaResource;

    DocumentKind(String value, String schemaResource) {
        this.value = value;
        this.schemaResource = schemaResource;
    }

    public String value() {
        return value;
    }

    String schemaResource() {
        return schemaResource;
    }

    static Optional<DocumentKind> fromValue(String value) {
        return Arrays.stream(values())
                .filter(kind -> kind.value.equals(value))
                .findFirst();
    }
}
