package org.savonitar.flink.stability.core.spec.document;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * One problem that stops a specification before provisioning: the document it is in, the
 * scenario side it applies to, the suite entry when a suite is being planned, a stable code, a
 * JSON pointer, and a message. {@link SpecificationException} names the stage that found it.
 */
public record Diagnostic(
        Path source,
        ResolutionScope scope,
        Optional<SuiteEntryIdentity> entry,
        String code,
        String path,
        String message) {
    public Diagnostic {
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }

    /** A problem in one scenario side, or in the whole document for {@code COMMON}. */
    public Diagnostic(
            Path source,
            ResolutionScope scope,
            String code,
            String path,
            String message) {
        this(source, scope, Optional.empty(), code, path, message);
    }

    /** A problem in the whole document. */
    public Diagnostic(Path source, String code, String path, String message) {
        this(source, ResolutionScope.COMMON, code, path, message);
    }

    /** The same problem, found while planning one suite entry. */
    public Diagnostic inSuiteEntry(SuiteEntryIdentity suiteEntry) {
        return new Diagnostic(
                source, scope, Optional.of(suiteEntry), code, path, message);
    }
}
