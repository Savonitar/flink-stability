package org.savonitar.flink.stability.core.execution.plan;

import org.savonitar.flink.stability.core.artifact.ConnectorBundleContribution;
import org.savonitar.flink.stability.core.artifact.PreparedConnectorBundle;
import org.savonitar.flink.stability.core.artifact.PreparedConnectorBundleEntry;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Checks before provisioning that the subject connector supplies the classes the protocol-v1
 * workload runs (SPEC-001 R5.6d, review finding F3). Installed bytes alone prove nothing: an
 * unrelated primary plus the released connector as a runtime dependency would test the release.
 */
final class SubjectEntryClassCheck {
    private static final List<String> PROTOCOL_V1_ENTRY_CLASSES =
            ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES;

    private static final Pattern VERSIONED_CLASS = Pattern.compile(
            "META-INF/versions/(?:9|[1-9][0-9]+)/(.+\\.class)");

    private SubjectEntryClassCheck() {}

    /**
     * The subject's primary artifact must contain every entry class, and nothing else that Flink
     * could load first may: neither another connector-bundle entry nor the workload JAR, which
     * Flink loads child-first.
     */
    static void verify(
            Path source,
            PreparedConnectorBundle bundle,
            Path workloadJar,
            String workloadPath) {
        List<Diagnostic> issues = new ArrayList<>();
        for (String alias : bundle.aliases()) {
            String artifactPath = "$/subject/connectors/" + alias + "/artifact";
            for (PreparedConnectorBundleEntry entry : bundle.entries()) {
                List<String> found = entryClassesIn(
                        entry.stagedPath(), source, artifactPath, issues);
                if (bundle.primaryEntry(alias).filter(entry::equals).isPresent()) {
                    List<String> missing = PROTOCOL_V1_ENTRY_CLASSES.stream()
                            .filter(entryClass -> !found.contains(entryClass))
                            .toList();
                    if (!missing.isEmpty()) {
                        issues.add(issue(source, "runner.subject.entry-class-missing",
                                artifactPath,
                                "The subject connector '" + alias + "' primary artifact lacks "
                                        + missing + ", so the job cannot run its code"));
                    }
                } else if (!found.isEmpty()) {
                    issues.add(issue(source, "runner.subject.entry-class-conflict",
                            artifactPath,
                            "Connector bundle entry " + entry.fileName() + " from "
                                    + origins(entry) + " also defines " + found
                                    + "; the executed copy would depend on classpath order"));
                }
            }
        }
        List<String> inWorkload = entryClassesIn(workloadJar, source, workloadPath, issues);
        if (!inWorkload.isEmpty()) {
            issues.add(issue(source, "runner.subject.entry-class-conflict", workloadPath,
                    "The workload JAR defines " + inWorkload + "; Flink loads it child-first,"
                            + " so its copies would run instead of the subject connector"));
        }
        if (!issues.isEmpty()) {
            throw new SpecificationException(Stage.RUNNER_CAPABILITY, issues);
        }
    }

    private static String origins(PreparedConnectorBundleEntry entry) {
        return entry.contributions().stream()
                .map(ConnectorBundleContribution::closureEntry)
                .map(closureEntry -> closureEntry.effectiveOrigin().declarationPath())
                .distinct()
                .collect(Collectors.joining(", "));
    }

    private static List<String> entryClassesIn(
            Path jar,
            Path source,
            String path,
            List<Diagnostic> issues) {
        try (JarFile jarFile = new JarFile(jar.toFile(), false)) {
            // The image reference does not establish its Java feature version. Do not use
            // the harness JVM's version to guess which versioned connector copy will load.
            if (jarFile.isMultiRelease()) {
                List<String> versioned = jarFile.stream()
                        .map(entry -> VERSIONED_CLASS.matcher(entry.getName()))
                        .filter(matcher -> matcher.matches())
                        .map(matcher -> matcher.group(1))
                        .filter(entry -> PROTOCOL_V1_ENTRY_CLASSES.stream().anyMatch(
                                entryClass -> entry.equals(
                                        entryClass.replace('.', '/') + ".class")))
                        .distinct()
                        .sorted()
                        .toList();
                if (!versioned.isEmpty()) {
                    issues.add(issue(source, "runner.subject.entry-class-versioned-unsupported",
                            path, "Cannot determine the target JVM's effective subject classes in "
                                    + jar.getFileName() + ": multi-release entries " + versioned
                                    + "; use a JAR without versioned subject entry classes"));
                }
            }
            return PROTOCOL_V1_ENTRY_CLASSES.stream()
                    .filter(entryClass -> jarFile.getJarEntry(
                            entryClass.replace('.', '/') + ".class") != null)
                    .toList();
        } catch (IOException unreadable) {
            issues.add(issue(source, "runner.subject.entry-class-unreadable", path,
                    "Cannot inspect " + jar.getFileName() + " for subject entry classes: "
                            + unreadable.getMessage()));
            return List.of();
        }
    }

    private static Diagnostic issue(
            Path source, String code, String path, String message) {
        return new Diagnostic(source, ResolutionScope.SINGLE, code, path, message);
    }
}
