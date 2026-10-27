package org.savonitar.flink.stability.core.artifact;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;

/** Preparation-only helper: use the framework's exact offline dependency selection. */
public final class ResolveClosure {
    private ResolveClosure() {}

    public static void main(String[] args) throws Exception {
        var closure = new CentralMavenArtifactLookup().resolveRuntimeClosure(
                List.of(new MavenRuntimeRoot(0, MavenCoordinate.parse(
                        "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2"))), true);
        var paths = new TreeSet<String>();
        for (var jar : closure.classpath()) {
            paths.add(jar.sourcePath().toString());
        }
        for (var pom : closure.consultedPoms()) {
            paths.add(pom.sourcePath().toString());
        }
        Files.write(Path.of(args[0]), paths);
        System.out.println("JARs=" + closure.classpath().size()
                + " POMs=" + closure.consultedPoms().size());
    }
}
