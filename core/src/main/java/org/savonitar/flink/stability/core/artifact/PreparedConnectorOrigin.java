package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.resolution.ResolutionScope;
import java.util.Objects;
import java.util.OptionalInt;

/** One declared connector artifact or dependency root that introduced a classpath entry. */
public record PreparedConnectorOrigin(
        ResolutionScope scope,
        RootKind rootKind,
        int declarationIndex,
        String declarationPath,
        String declaredReference) {

    public enum RootKind {
        PRIMARY,
        RUNTIME_DEPENDENCY
    }

    /** Primary roots use {@code -1}; dependency roots use their zero-based list index. */
    public PreparedConnectorOrigin {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(rootKind, "rootKind");
        if ((rootKind == RootKind.PRIMARY && declarationIndex != -1)
                || (rootKind == RootKind.RUNTIME_DEPENDENCY && declarationIndex < 0)) {
            throw new IllegalArgumentException(
                    "Primary roots use index -1; runtime-dependency roots use a list index");
        }
        declarationPath = requirePointer(declarationPath, "declarationPath");
        declaredReference = requireNonBlank(declaredReference, "declaredReference");
    }

    public boolean primaryRoot() {
        return rootKind == RootKind.PRIMARY;
    }

    public OptionalInt runtimeDependencyIndex() {
        return primaryRoot()
                ? OptionalInt.empty()
                : OptionalInt.of(declarationIndex);
    }

    private static String requirePointer(String value, String name) {
        value = requireNonBlank(value, name);
        if (!value.startsWith("$/")) {
            throw new IllegalArgumentException(name + " must be a root-relative JSON pointer");
        }
        return value;
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
