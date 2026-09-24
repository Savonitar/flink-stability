package org.savonitar.flink.stability.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CorePackageArchitectureTest {
    private static final String CORE_PREFIX = "org.savonitar.flink.stability.core";
    private static final Pattern PACKAGE = Pattern.compile(
            "(?m)^package\\s+([A-Za-z0-9_.]+);$");
    private static final Pattern CORE_REFERENCE = Pattern.compile(
            "\\borg\\.savonitar\\.flink\\.stability\\.core"
                    + "(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)+");
    private static final Map<Layer, Integer> EXPECTED_COUNTS = Map.of(
            Layer.DOCUMENT, 13,
            Layer.RESOLUTION, 36,
            Layer.ARTIFACT, 34,
            Layer.EXECUTION_PLAN, 6);

    @Test
    void preservesThePreExecutionPackageBoundaries() {
        Path sourceRoot = sourceRoot();
        List<SourceFile> sources = javaSources(sourceRoot);
        Map<Layer, Integer> counts = new EnumMap<>(Layer.class);

        for (SourceFile source : sources) {
            assertFalse(
                    source.packageName().equals(CORE_PREFIX + ".spec"),
                    () -> "The flat core.spec package must remain empty: " + source.path());
            assertFalse(
                    source.packageName().startsWith(CORE_PREFIX + ".spec.execution"),
                    () -> "Use core.execution.plan, not the former spec.execution package: "
                            + source.path());
            assertEquals(
                    expectedPath(sourceRoot, source),
                    source.path(),
                    () -> "Package declaration must match the source path for " + source.path());

            Layer sourceLayer = Layer.fromPackage(source.packageName());
            if (sourceLayer == null) {
                continue;
            }
            counts.merge(sourceLayer, 1, Integer::sum);
            for (String referencedType : coreReferences(source.contents())) {
                Layer referencedLayer = Layer.fromPackage(referencedType);
                assertTrue(
                        referencedLayer != null,
                        () -> source.packageName()
                                + " must not depend on an unrecognized core package: "
                                + referencedType);
                assertTrue(
                        referencedLayer.rank <= sourceLayer.rank,
                        () -> source.packageName() + " must not depend on downstream package "
                                + referencedType);
            }
        }

        assertEquals(EXPECTED_COUNTS, counts);
    }

    private static Path sourceRoot() {
        Path workingDirectory = Path.of("").toAbsolutePath().normalize();
        for (Path candidate : List.of(
                workingDirectory.resolve("src/main/java"),
                workingDirectory.resolve("core/src/main/java"))) {
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException(
                "Could not locate core/src/main/java from " + workingDirectory);
    }

    private static List<SourceFile> javaSources(Path sourceRoot) {
        List<SourceFile> result = new ArrayList<>();
        try (var paths = Files.walk(sourceRoot.resolve(CORE_PREFIX.replace('.', '/')))) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .sorted()
                    .forEach(path -> result.add(readSource(path)));
        } catch (IOException exception) {
            throw new UncheckedIOException("Could not inspect core source packages", exception);
        }
        return List.copyOf(result);
    }

    private static SourceFile readSource(Path path) {
        try {
            String contents = Files.readString(path);
            Matcher declaration = PACKAGE.matcher(contents);
            if (!declaration.find()) {
                throw new IllegalStateException("Source has no package declaration: " + path);
            }
            return new SourceFile(path.toAbsolutePath().normalize(), declaration.group(1), contents);
        } catch (IOException exception) {
            throw new UncheckedIOException("Could not read " + path, exception);
        }
    }

    private static Path expectedPath(Path sourceRoot, SourceFile source) {
        return sourceRoot.resolve(source.packageName().replace('.', '/'))
                .resolve(source.path().getFileName())
                .toAbsolutePath()
                .normalize();
    }

    private static List<String> coreReferences(String contents) {
        List<String> references = new ArrayList<>();
        Matcher matcher = CORE_REFERENCE.matcher(contents);
        while (matcher.find()) {
            references.add(matcher.group());
        }
        return List.copyOf(references);
    }

    private record SourceFile(Path path, String packageName, String contents) {}

    private enum Layer {
        DOCUMENT(".spec.document", 0),
        RESOLUTION(".spec.resolution", 1),
        ARTIFACT(".artifact", 2),
        EXECUTION_PLAN(".execution.plan", 3);

        private final String suffix;
        private final int rank;

        Layer(String suffix, int rank) {
            this.suffix = suffix;
            this.rank = rank;
        }

        private static Layer fromPackage(String packageOrType) {
            for (Layer candidate : values()) {
                String prefix = CORE_PREFIX + candidate.suffix;
                if (packageOrType.equals(prefix) || packageOrType.startsWith(prefix + ".")) {
                    return candidate;
                }
            }
            return null;
        }
    }
}
