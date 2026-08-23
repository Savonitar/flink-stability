package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.regex.PatternSyntaxException;
import java.util.zip.CRC32;

/** Resolves and checksums every static file artifact after semantic planning. */
public final class ArtifactPlanResolver {
    private static final Path STAGING_DIRECTORY = Path.of(
            ".flink-stability", "artifacts", "prepared");
    private static final Comparator<ArtifactReference> REFERENCE_ORDER = Comparator
            .comparing(ArtifactReference::scope)
            .thenComparing(ArtifactReference::path)
            .thenComparing(ArtifactReference::role);
    private static final Set<Character> GLOB_META = Set.of('*', '?', '[', ']', '{', '}');

    private final MavenArtifactLookup mavenLookup;

    public ArtifactPlanResolver() {
        this(new CentralMavenArtifactLookup());
    }

    ArtifactPlanResolver(MavenArtifactLookup mavenLookup) {
        this.mavenLookup = Objects.requireNonNull(mavenLookup, "mavenLookup");
    }

    public PreparedScenarioPlan resolve(
            ResolvedScenarioPlan plan,
            ArtifactResolutionOptions options) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(options, "options");
        Path source = plan.scenario().template().source();
        Path artifactRoot = canonicalArtifactRoot(options.artifactRoot(), source);
        ArtifactWorkspace workspace = createWorkspace(artifactRoot, source);
        try {
            return resolveScenario(plan, workspace, options.offline(), true);
        } catch (RuntimeException failure) {
            closeAfterFailure(workspace, failure);
            throw failure;
        }
    }

    public PreparedSuitePlan resolve(
            ResolvedSuitePlan plan,
            ArtifactResolutionOptions options) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(options, "options");
        Path artifactRoot = canonicalArtifactRoot(
                options.artifactRoot(), plan.specification().source());
        ArtifactWorkspace workspace = createWorkspace(
                artifactRoot, plan.specification().source());

        try {
            List<PreparedSuiteEntry> prepared = new ArrayList<>();
            List<SuitePlanningIssue> issues = new ArrayList<>();
            for (ResolvedSuiteEntry entry : plan.entries()) {
                try {
                    prepared.add(new PreparedSuiteEntry(
                            entry,
                            resolveScenario(
                                    entry.scenarioPlan(),
                                    workspace,
                                    options.offline(),
                                    false)));
                } catch (ArtifactResolutionException exception) {
                    exception.issues().forEach(issue -> issues.add(new SuitePlanningIssue(
                            entry.identity(),
                            issue.source(),
                            issue.scope(),
                            issue.code(),
                            issue.path(),
                            issue.message())));
                }
            }
            if (!issues.isEmpty()) {
                throw new SuitePlanningException(issues);
            }
            return new PreparedSuitePlan(plan, workspace, prepared);
        } catch (RuntimeException failure) {
            closeAfterFailure(workspace, failure);
            throw failure;
        }
    }

    private PreparedScenarioPlan resolveScenario(
            ResolvedScenarioPlan plan,
            ArtifactWorkspace workspace,
            boolean offline,
            boolean ownsWorkspace) {
        Path source = plan.scenario().template().source();
        List<ArtifactIssue> issues = new ArrayList<>();
        List<ResolvedArtifact> resolved = new ArrayList<>();
        for (ArtifactReference reference : references(plan.scenario())) {
            resolveReference(
                    reference,
                    source,
                    workspace.artifactRoot(),
                    workspace.preparationRoot(),
                    offline,
                    issues)
                    .ifPresent(resolved::add);
        }
        if (!issues.isEmpty()) {
            throw new ArtifactResolutionException(issues);
        }
        return new PreparedScenarioPlan(plan, workspace, resolved, ownsWorkspace);
    }

    private Optional<ResolvedArtifact> resolveReference(
            ArtifactReference reference,
            Path source,
            Path artifactRoot,
            Path stagingDirectory,
            boolean offline,
            List<ArtifactIssue> issues) {
        Path resolvedPath;
        String declared = reference.reference();
        boolean localReference = !declared.startsWith("maven:");
        if (declared.startsWith("maven:")) {
            if (!reference.role().permitsMavenCoordinate()) {
                issues.add(issue(source, reference,
                        "artifact.reference.invalid",
                        "Maven coordinates are supported only for subject connector artifacts"));
                return Optional.empty();
            }
            MavenCoordinate coordinate;
            try {
                coordinate = MavenCoordinate.parse(declared);
            } catch (IllegalArgumentException exception) {
                issues.add(issue(source, reference,
                        "artifact.maven.invalid-coordinate", exception.getMessage()));
                return Optional.empty();
            }
            try {
                resolvedPath = mavenLookup.resolve(coordinate, offline);
            } catch (MavenArtifactLookupException exception) {
                String code = switch (exception.kind()) {
                    case NOT_FOUND -> "artifact.maven.not-found";
                    case REPOSITORY_UNAVAILABLE -> "artifact.maven.repository-unavailable";
                    case OFFLINE_MISS -> "artifact.maven.offline-miss";
                };
                issues.add(issue(source, reference, code, exception.getMessage()));
                return Optional.empty();
            }
        } else {
            Optional<Path> local = resolveLocal(reference, source, artifactRoot, issues);
            if (local.isEmpty()) {
                return Optional.empty();
            }
            resolvedPath = local.get();
        }

        Optional<Path> usable = validateResolvedFile(
                reference, source, resolvedPath, artifactRoot, localReference, issues);
        if (usable.isEmpty()) {
            return Optional.empty();
        }
        Optional<StagedArtifact> staged = stage(
                reference, source, usable.get(), artifactRoot, stagingDirectory, issues);
        if (staged.isEmpty()) {
            return Optional.empty();
        }
        if (!validateJar(reference, source, staged.get().path(), issues)) {
            return Optional.empty();
        }
        return Optional.of(new ResolvedArtifact(
                reference.scope(),
                reference.role(),
                reference.path(),
                declared,
                usable.get(),
                staged.get().path(),
                staged.get().sha256()));
    }

    private static Optional<Path> resolveLocal(
            ArtifactReference reference,
            Path source,
            Path artifactRoot,
            List<ArtifactIssue> issues) {
        String declared = reference.reference();
        if (declared.startsWith("~") || declared.contains("://")) {
            issues.add(issue(source, reference, "artifact.reference.invalid",
                    "Local references do not expand '~' and cannot be URLs"));
            return Optional.empty();
        }

        final Path declaredPath;
        try {
            declaredPath = Path.of(declared);
        } catch (InvalidPathException exception) {
            issues.add(issue(source, reference, "artifact.reference.invalid",
                    "Invalid local path: " + safeMessage(exception)));
            return Optional.empty();
        }
        int patternedSegment = firstGlobSegment(declaredPath);
        if (patternedSegment >= 0
                && patternedSegment != declaredPath.getNameCount() - 1) {
            issues.add(issue(source, reference, "artifact.reference.invalid",
                    "Build-output pattern metacharacters are allowed only in the final filename"));
            return Optional.empty();
        }

        Path candidate = declaredPath.isAbsolute()
                ? declaredPath.normalize()
                : artifactRoot.resolve(declaredPath).normalize();
        if (!candidate.startsWith(artifactRoot)) {
            issues.add(issue(source, reference, "artifact.local.outside-root",
                    "Local artifact must remain under artifact root " + artifactRoot));
            return Optional.empty();
        }

        if (patternedSegment >= 0) {
            if (!reference.role().permitsBuildPattern()) {
                issues.add(issue(source, reference, "artifact.reference.invalid",
                        "Build-output patterns are not supported for file input"));
                return Optional.empty();
            }
            Path parent = candidate.getParent();
            if (parent == null || !Files.isDirectory(parent)) {
                issues.add(issue(source, reference, "artifact.local.not-found",
                        "Build-output pattern has no existing directory: " + parent));
                return Optional.empty();
            }
            try {
                if (!parent.toRealPath().startsWith(artifactRoot)) {
                    issues.add(issue(source, reference, "artifact.local.outside-root",
                            "Build-output pattern directory must remain under artifact root "
                                    + artifactRoot));
                    return Optional.empty();
                }
            } catch (IOException exception) {
                issues.add(issue(source, reference, "artifact.local.not-found",
                        "Build-output pattern directory is unavailable: "
                                + safeMessage(exception)));
                return Optional.empty();
            }
            List<Path> matches = new ArrayList<>(2);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    parent, candidate.getFileName().toString())) {
                for (Path path : stream) {
                    if (Files.isRegularFile(path)) {
                        matches.add(path);
                        if (matches.size() == 2) {
                            break;
                        }
                    }
                }
            } catch (IOException | DirectoryIteratorException | PatternSyntaxException exception) {
                issues.add(issue(source, reference, "artifact.reference.invalid",
                        "Could not evaluate build-output pattern: " + safeMessage(exception)));
                return Optional.empty();
            }
            if (matches.isEmpty()) {
                issues.add(issue(source, reference, "artifact.local.not-found",
                        "Build-output pattern matches no regular file under " + parent));
                return Optional.empty();
            }
            if (matches.size() > 1) {
                issues.add(issue(source, reference, "artifact.local.ambiguous",
                        "Build-output pattern matches more than one regular file under " + parent));
                return Optional.empty();
            }
            return Optional.of(matches.getFirst());
        }

        if (!Files.exists(candidate)) {
            issues.add(issue(source, reference, "artifact.local.not-found",
                    "Local artifact does not exist: " + candidate));
            return Optional.empty();
        }
        if (!Files.isRegularFile(candidate)) {
            issues.add(issue(source, reference, "artifact.local.not-regular-file",
                    "Local artifact is not a regular file: " + candidate));
            return Optional.empty();
        }
        return Optional.of(candidate);
    }

    private static Optional<Path> validateResolvedFile(
            ArtifactReference reference,
            Path source,
            Path resolvedPath,
            Path artifactRoot,
            boolean confinedToArtifactRoot,
            List<ArtifactIssue> issues) {
        final Path realPath;
        try {
            realPath = resolvedPath.toRealPath();
        } catch (IOException exception) {
            issues.add(issue(source, reference, "artifact.local.not-found",
                    "Resolved artifact is unavailable: " + safeMessage(exception)));
            return Optional.empty();
        }
        if (!Files.isRegularFile(realPath)) {
            issues.add(issue(source, reference, "artifact.local.not-regular-file",
                    "Resolved artifact is not a regular file: " + realPath));
            return Optional.empty();
        }
        if (confinedToArtifactRoot && !realPath.startsWith(artifactRoot)) {
            issues.add(issue(source, reference, "artifact.local.outside-root",
                    "Local artifact resolves outside artifact root " + artifactRoot));
            return Optional.empty();
        }
        if (reference.role().isJar()
                && !realPath.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar")) {
            issues.add(issue(source, reference, "artifact.jar.invalid",
                    "Executable and connector artifacts must resolve from a .jar file"));
            return Optional.empty();
        }
        return Optional.of(realPath);
    }

    private static Optional<StagedArtifact> stage(
            ArtifactReference reference,
            Path source,
            Path resolvedPath,
            Path artifactRoot,
            Path stagingDirectory,
            List<ArtifactIssue> issues) {
        Path temporary = null;
        try {
            temporary = Files.createTempFile(stagingDirectory, ".copy-", ".tmp");
            try (InputStream input = Files.newInputStream(
                    resolvedPath, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                Files.copy(input, temporary, StandardCopyOption.REPLACE_EXISTING);
            }
            String sha256 = uncheckedSha256(temporary);
            String suffix = reference.role().isJar() ? ".jar" : ".data";
            Path target = stagingDirectory.resolve(sha256 + suffix);
            if (!Files.exists(target)) {
                try {
                    Files.move(temporary, target);
                    temporary = null;
                } catch (FileAlreadyExistsException ignored) {
                    // Another resolver materialized the same content concurrently.
                }
            }
            if (Files.isSymbolicLink(target)) {
                issues.add(issue(source, reference, "artifact.staging.invalid",
                        "Prepared artifact must not be a symbolic link"));
                return Optional.empty();
            }
            Path staged = target.toRealPath();
            if (!staged.getParent().equals(stagingDirectory)
                    || !staged.startsWith(artifactRoot)
                    || !Files.isRegularFile(staged)) {
                issues.add(issue(source, reference, "artifact.staging.invalid",
                        "Prepared artifact is not a regular file under the artifact root"));
                return Optional.empty();
            }
            String stagedHash = uncheckedSha256(staged);
            if (!sha256.equals(stagedHash)) {
                issues.add(issue(source, reference, "artifact.staging.corrupt",
                        "Content-addressed artifact cache contains bytes with the wrong SHA-256"));
                return Optional.empty();
            }
            return Optional.of(new StagedArtifact(staged, sha256));
        } catch (IOException | ChecksumFailure exception) {
            Throwable detail = exception instanceof ChecksumFailure
                    ? exception.getCause()
                    : exception;
            issues.add(issue(source, reference, "artifact.staging.failed",
                    "Could not materialize a private artifact copy: " + safeMessage(detail)));
            return Optional.empty();
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException ignored) {
                    // Best-effort cleanup of a private temporary copy.
                }
            }
        }
    }

    private static boolean validateJar(
            ArtifactReference reference,
            Path source,
            Path realPath,
            List<ArtifactIssue> issues) {
        if (!reference.role().isJar()) {
            return true;
        }
        try (JarFile jar = new JarFile(realPath.toFile(), true)) {
            if (reference.role().requiresMainClass()) {
                String mainClass = jar.getManifest() == null
                        ? null
                        : jar.getManifest().getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
                if (mainClass == null || mainClass.isBlank()) {
                    issues.add(issue(source, reference, "artifact.jar.entrypoint-missing",
                            "The JAR requires a non-empty Main-Class manifest entry"));
                    return false;
                }
            }
            byte[] buffer = new byte[8192];
            var entries = jar.entries();
            while (entries.hasMoreElements()) {
                var entry = entries.nextElement();
                if (entry.isDirectory()) {
                    continue;
                }
                CRC32 checksum = new CRC32();
                long uncompressedSize = 0;
                try (InputStream input = jar.getInputStream(entry)) {
                    for (int read; (read = input.read(buffer)) >= 0;) {
                        if (read > 0) {
                            checksum.update(buffer, 0, read);
                            uncompressedSize += read;
                        }
                    }
                }
                if ((entry.getSize() >= 0 && entry.getSize() != uncompressedSize)
                        || (entry.getCrc() >= 0 && entry.getCrc() != checksum.getValue())) {
                    throw new IOException(
                            "JAR entry integrity check failed for " + entry.getName());
                }
            }
        } catch (IOException exception) {
            issues.add(issue(source, reference, "artifact.jar.invalid",
                    "Could not read JAR " + realPath + ": " + safeMessage(exception)));
            return false;
        }
        return true;
    }

    private static List<ArtifactReference> references(ResolvedScenario scenario) {
        if (!scenario.isExperiment()) {
            List<ArtifactReference> references = collect(
                    scenario.side(ScenarioSide.SINGLE).document(), ResolutionScope.SINGLE);
            return references.stream().sorted(REFERENCE_ORDER).toList();
        }

        Map<ReferenceIdentity, ArtifactReference> baseline = byIdentity(collect(
                scenario.side(ScenarioSide.BASELINE).document(), ResolutionScope.BASELINE));
        Map<ReferenceIdentity, ArtifactReference> candidate = byIdentity(collect(
                scenario.side(ScenarioSide.CANDIDATE).document(), ResolutionScope.CANDIDATE));
        List<ReferenceIdentity> identities = new ArrayList<>();
        identities.addAll(baseline.keySet());
        candidate.keySet().stream().filter(identity -> !baseline.containsKey(identity))
                .forEach(identities::add);
        identities.sort(Comparator.comparing(ReferenceIdentity::path)
                .thenComparing(ReferenceIdentity::role));

        List<ArtifactReference> merged = new ArrayList<>();
        for (ReferenceIdentity identity : identities) {
            ArtifactReference left = baseline.get(identity);
            ArtifactReference right = candidate.get(identity);
            if (left != null && right != null && left.reference().equals(right.reference())) {
                merged.add(new ArtifactReference(
                        ResolutionScope.COMMON, identity.role(), identity.path(), left.reference()));
            } else {
                if (left != null) {
                    merged.add(left);
                }
                if (right != null) {
                    merged.add(right);
                }
            }
        }
        return merged.stream().sorted(REFERENCE_ORDER).toList();
    }

    private static Map<ReferenceIdentity, ArtifactReference> byIdentity(
            List<ArtifactReference> references) {
        Map<ReferenceIdentity, ArtifactReference> result = new LinkedHashMap<>();
        references.forEach(reference -> result.put(
                new ReferenceIdentity(reference.role(), reference.path()), reference));
        return result;
    }

    private static List<ArtifactReference> collect(ObjectNode document, ResolutionScope scope) {
        List<ArtifactReference> references = new ArrayList<>();
        collectInputs(document, scope, references);
        collectSubject(document, scope, references);
        collectJobs(document, scope, references);
        collectTerminalValidators(document, scope, references);
        collectPhaseValidators(document, scope, references);
        return references;
    }

    private static void collectInputs(
            ObjectNode document,
            ResolutionScope scope,
            List<ArtifactReference> references) {
        JsonNode clusters = document.at("/setup/kafka/clusters");
        if (!(clusters instanceof ObjectNode clusterObject)) {
            return;
        }
        clusterObject.fields().forEachRemaining(cluster -> {
            JsonNode topics = cluster.getValue().get("topics");
            if (!(topics instanceof ArrayNode topicArray)) {
                return;
            }
            for (int index = 0; index < topicArray.size(); index++) {
                JsonNode input = topicArray.get(index).get("input_source");
                if (!(input instanceof ObjectNode inputObject)) {
                    continue;
                }
                String base = "$/setup/kafka/clusters/" + escapePointer(cluster.getKey())
                        + "/topics/" + index + "/input_source";
                String mode = inputObject.path("mode").textValue();
                if ("file".equals(mode)) {
                    addReference(references, scope, ArtifactRole.INPUT_FILE,
                            base + "/path", inputObject.get("path"));
                } else if ("custom".equals(mode)) {
                    addReference(references, scope, ArtifactRole.CUSTOM_INPUT,
                            base + "/artifact", inputObject.get("artifact"));
                }
            }
        });
    }

    private static void collectSubject(
            ObjectNode document,
            ResolutionScope scope,
            List<ArtifactReference> references) {
        JsonNode connectors = document.at("/subject/connectors");
        if (!(connectors instanceof ObjectNode connectorObject)) {
            return;
        }
        connectorObject.fields().forEachRemaining(connector -> addReference(
                references,
                scope,
                ArtifactRole.SUBJECT_CONNECTOR,
                "$/subject/connectors/" + escapePointer(connector.getKey()) + "/artifact",
                connector.getValue().get("artifact")));
    }

    private static void collectJobs(
            ObjectNode document,
            ResolutionScope scope,
            List<ArtifactReference> references) {
        JsonNode jobs = document.at("/workload/jobs");
        if (!(jobs instanceof ArrayNode jobArray)) {
            return;
        }
        for (int index = 0; index < jobArray.size(); index++) {
            addReference(references, scope, ArtifactRole.WORKLOAD_JOB,
                    "$/workload/jobs/" + index + "/jar", jobArray.get(index).get("jar"));
        }
    }

    private static void collectTerminalValidators(
            ObjectNode document,
            ResolutionScope scope,
            List<ArtifactReference> references) {
        JsonNode validators = document.get("terminal_validations");
        if (!(validators instanceof ArrayNode validatorArray)) {
            return;
        }
        for (int index = 0; index < validatorArray.size(); index++) {
            JsonNode validator = validatorArray.get(index);
            if ("custom".equals(validator.path("type").textValue())) {
                addReference(references, scope, ArtifactRole.CUSTOM_VALIDATOR,
                        "$/terminal_validations/" + index + "/artifact",
                        validator.get("artifact"));
            }
        }
    }

    private static void collectPhaseValidators(
            ObjectNode document,
            ResolutionScope scope,
            List<ArtifactReference> references) {
        JsonNode phases = document.get("phases");
        if (!(phases instanceof ArrayNode phaseArray)) {
            return;
        }
        for (int phaseIndex = 0; phaseIndex < phaseArray.size(); phaseIndex++) {
            JsonNode steps = phaseArray.get(phaseIndex).get("steps");
            collectStepValidators(steps, "$/phases/" + phaseIndex + "/steps", scope, references);
        }
    }

    private static void collectStepValidators(
            JsonNode steps,
            String path,
            ResolutionScope scope,
            List<ArtifactReference> references) {
        if (!(steps instanceof ArrayNode stepArray)) {
            return;
        }
        for (int stepIndex = 0; stepIndex < stepArray.size(); stepIndex++) {
            JsonNode step = stepArray.get(stepIndex);
            String stepPath = path + "/" + stepIndex;
            JsonNode validator = step.get("validate");
            if (validator != null && "custom".equals(validator.path("type").textValue())) {
                addReference(references, scope, ArtifactRole.CUSTOM_VALIDATOR,
                        stepPath + "/validate/artifact", validator.get("artifact"));
            }
            JsonNode loop = step.get("loop");
            if (loop != null) {
                collectStepValidators(loop.get("steps"), stepPath + "/loop/steps", scope, references);
            }
        }
    }

    private static void addReference(
            List<ArtifactReference> references,
            ResolutionScope scope,
            ArtifactRole role,
            String path,
            JsonNode value) {
        if (value != null && value.isTextual()) {
            references.add(new ArtifactReference(scope, role, path, value.textValue()));
        }
    }

    private static Path canonicalArtifactRoot(Path artifactRoot, Path source) {
        if (!Files.isDirectory(artifactRoot)) {
            throw new ArtifactResolutionException(List.of(new ArtifactIssue(
                    source,
                    ResolutionScope.COMMON,
                    "artifact.root.not-directory",
                    "$",
                    "Artifact root is not an existing directory: " + artifactRoot)));
        }
        try {
            return artifactRoot.toRealPath();
        } catch (IOException exception) {
            throw new ArtifactResolutionException(List.of(new ArtifactIssue(
                    source,
                    ResolutionScope.COMMON,
                    "artifact.root.unavailable",
                    "$",
                    "Artifact root is unavailable: " + safeMessage(exception))));
        }
    }

    private static ArtifactWorkspace createWorkspace(Path artifactRoot, Path source) {
        try {
            Path realParent = artifactRoot;
            for (Path component : STAGING_DIRECTORY) {
                Path next = realParent.resolve(component.toString());
                if (Files.exists(next, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    if (Files.isSymbolicLink(next)
                            || !Files.isDirectory(next, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException(
                                "Private artifact staging component is not a real directory: "
                                        + next);
                    }
                } else {
                    try {
                        Files.createDirectory(next);
                    } catch (FileAlreadyExistsException ignored) {
                        if (Files.isSymbolicLink(next)
                                || !Files.isDirectory(
                                        next, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                            throw new IOException(
                                    "Private artifact staging component is not a real directory: "
                                            + next);
                        }
                    }
                }
                Path realNext = next.toRealPath();
                if (!realNext.getParent().equals(realParent)
                        || !realNext.startsWith(artifactRoot)) {
                    throw new IOException(
                            "Private artifact staging directory resolves outside artifact root");
                }
                realParent = realNext;
            }
            Path directory = Files.createTempDirectory(realParent, "plan-").toRealPath();
            if (!directory.getParent().equals(realParent)
                    || !directory.startsWith(artifactRoot)) {
                throw new IOException(
                        "Private artifact plan directory resolves outside artifact root");
            }
            return new ArtifactWorkspace(artifactRoot, directory);
        } catch (IOException exception) {
            throw new ArtifactResolutionException(List.of(new ArtifactIssue(
                    source,
                    ResolutionScope.COMMON,
                    "artifact.staging.failed",
                    "$",
                    "Could not create private artifact staging: "
                            + safeMessage(exception))));
        }
    }

    private static void closeAfterFailure(
            ArtifactWorkspace workspace,
            RuntimeException failure) {
        try {
            workspace.close();
        } catch (RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private static int firstGlobSegment(Path path) {
        for (int index = 0; index < path.getNameCount(); index++) {
            String segment = path.getName(index).toString();
            if (containsGlobMeta(segment)) {
                return index;
            }
        }
        return -1;
    }

    private static boolean containsGlobMeta(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (GLOB_META.contains(value.charAt(index))) {
                return true;
            }
        }
        return false;
    }

    private static String uncheckedSha256(Path path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                for (int read; (read = input.read(buffer)) >= 0;) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (IOException | NoSuchAlgorithmException exception) {
            throw new ChecksumFailure(exception);
        }
    }

    private static ArtifactIssue issue(
            Path source,
            ArtifactReference reference,
            String code,
            String message) {
        return new ArtifactIssue(
                source, reference.scope(), code, reference.path(), message);
    }

    private static String safeMessage(Throwable throwable) {
        return throwable.getMessage() == null
                ? throwable.getClass().getSimpleName()
                : throwable.getMessage();
    }

    private static String escapePointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }

    private record ArtifactReference(
            ResolutionScope scope,
            ArtifactRole role,
            String path,
            String reference) {}

    private record ReferenceIdentity(ArtifactRole role, String path) {}

    private record StagedArtifact(Path path, String sha256) {}

    private static final class ChecksumFailure extends RuntimeException {
        private ChecksumFailure(Throwable cause) {
            super(cause);
        }
    }
}
