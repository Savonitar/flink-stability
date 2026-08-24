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
import java.util.HashMap;
import java.util.HashSet;
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
        PreparationContext context = new PreparationContext(workspace, options.offline());
        try {
            return resolveScenario(plan, context, true);
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
        PreparationContext context = new PreparationContext(workspace, options.offline());

        try {
            List<PreparedSuiteEntry> prepared = new ArrayList<>();
            List<SuitePlanningIssue> issues = new ArrayList<>();
            for (ResolvedSuiteEntry entry : plan.entries()) {
                try {
                    prepared.add(new PreparedSuiteEntry(
                            entry,
                            resolveScenario(
                                    entry.scenarioPlan(),
                                    context,
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
            PreparationContext context,
            boolean ownsWorkspace) {
        Path source = plan.scenario().template().source();
        List<ArtifactIssue> issues = new ArrayList<>();
        List<ResolvedArtifact> resolved = new ArrayList<>();
        for (ArtifactReference reference : references(plan.scenario())) {
            resolveReference(
                    reference,
                    source,
                    context,
                    issues)
                    .ifPresent(resolved::add);
        }
        List<PreparedConnectorClosure> connectorClosures = resolveConnectorClosures(
                plan.scenario(), source, context, resolved, issues);
        if (!issues.isEmpty()) {
            throw new ArtifactResolutionException(issues);
        }
        resolved.sort(Comparator
                .comparing(ResolvedArtifact::scope)
                .thenComparing(ResolvedArtifact::path)
                .thenComparing(ResolvedArtifact::role));
        connectorClosures.sort(Comparator
                .comparing(PreparedConnectorClosure::scope)
                .thenComparing(PreparedConnectorClosure::alias));
        return new PreparedScenarioPlan(
                plan,
                context.workspace(),
                resolved,
                connectorClosures,
                ownsWorkspace);
    }

    private Optional<ResolvedArtifact> resolveReference(
            ArtifactReference reference,
            Path source,
            PreparationContext context,
            List<ArtifactIssue> issues) {
        Path artifactRoot = context.workspace().artifactRoot();
        Path stagingDirectory = context.workspace().preparationRoot();
        Path resolvedPath;
        MavenCoordinate mavenCoordinate = null;
        String declared = reference.reference();
        boolean localReference = !declared.startsWith("maven:");
        if (declared.startsWith("maven:")) {
            if (!reference.role().permitsMavenCoordinate()) {
                issues.add(issue(source, reference,
                        "artifact.reference.invalid",
                        "Maven coordinates are supported only for subject connector artifacts"));
                return Optional.empty();
            }
            try {
                mavenCoordinate = MavenCoordinate.parse(declared);
            } catch (IllegalArgumentException exception) {
                issues.add(issue(source, reference,
                        "artifact.maven.invalid-coordinate", exception.getMessage()));
                return Optional.empty();
            }
            try {
                resolvedPath = resolvePrimaryMaven(mavenCoordinate, context).sourcePath();
            } catch (MavenArtifactLookupException exception) {
                issues.add(mavenIssue(source, reference, exception));
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
        if (mavenCoordinate != null && !verifyPrimaryMavenHash(
                mavenCoordinate,
                staged.get().sha256(),
                context,
                source,
                reference,
                issues)) {
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

    private List<PreparedConnectorClosure> resolveConnectorClosures(
            ResolvedScenario scenario,
            Path source,
            PreparationContext context,
            List<ResolvedArtifact> resolvedArtifacts,
            List<ArtifactIssue> issues) {
        List<PreparedConnectorClosure> closures = new ArrayList<>();
        if (!scenario.isExperiment()) {
            connectorDeclarations(scenario.side(ScenarioSide.SINGLE)).values().forEach(
                    declaration -> Optional.ofNullable(resolveConnector(
                                    declaration,
                                    ResolutionScope.SINGLE,
                                    source,
                                    context,
                                    resolvedArtifacts,
                                    issues))
                            .ifPresent(closures::add));
            return closures;
        }

        Map<String, ConnectorDeclaration> baseline = connectorDeclarations(
                scenario.side(ScenarioSide.BASELINE));
        Map<String, ConnectorDeclaration> candidate = connectorDeclarations(
                scenario.side(ScenarioSide.CANDIDATE));
        Set<String> aliases = new HashSet<>(baseline.keySet());
        aliases.addAll(candidate.keySet());
        aliases.stream().sorted().forEach(alias -> resolveExperimentConnector(
                baseline.get(alias),
                candidate.get(alias),
                source,
                context,
                resolvedArtifacts,
                issues,
                closures));
        return closures;
    }

    private void resolveExperimentConnector(
            ConnectorDeclaration baseline,
            ConnectorDeclaration candidate,
            Path source,
            PreparationContext context,
            List<ResolvedArtifact> resolvedArtifacts,
            List<ArtifactIssue> issues,
            List<PreparedConnectorClosure> closures) {
        if (baseline == null) {
            Optional.ofNullable(resolveConnector(
                            candidate,
                            ResolutionScope.CANDIDATE,
                            source,
                            context,
                            resolvedArtifacts,
                            issues))
                    .ifPresent(closures::add);
            return;
        }
        if (candidate == null) {
            Optional.ofNullable(resolveConnector(
                            baseline,
                            ResolutionScope.BASELINE,
                            source,
                            context,
                            resolvedArtifacts,
                            issues))
                    .ifPresent(closures::add);
            return;
        }
        if (baseline.sameDefinition(candidate)) {
            Optional.ofNullable(resolveConnector(
                            baseline,
                            ResolutionScope.COMMON,
                            source,
                            context,
                            resolvedArtifacts,
                            issues))
                    .ifPresent(closures::add);
            return;
        }
        if (!baseline.explicit() || !candidate.explicit()) {
            Optional.ofNullable(resolveConnector(
                            baseline,
                            ResolutionScope.BASELINE,
                            source,
                            context,
                            resolvedArtifacts,
                            issues))
                    .ifPresent(closures::add);
            Optional.ofNullable(resolveConnector(
                            candidate,
                            ResolutionScope.CANDIDATE,
                            source,
                            context,
                            resolvedArtifacts,
                            issues))
                    .ifPresent(closures::add);
            return;
        }

        PreparedPrimary baselinePrimary;
        PreparedPrimary candidatePrimary;
        if (baseline.artifactReference().equals(candidate.artifactReference())) {
            PreparedPrimary common = prepareDeclaredPrimary(
                    baseline,
                    ResolutionScope.COMMON,
                    source,
                    context,
                    resolvedArtifacts,
                    issues);
            baselinePrimary = common;
            candidatePrimary = common;
        } else {
            baselinePrimary = prepareDeclaredPrimary(
                    baseline,
                    ResolutionScope.BASELINE,
                    source,
                    context,
                    resolvedArtifacts,
                    issues);
            candidatePrimary = prepareDeclaredPrimary(
                    candidate,
                    ResolutionScope.CANDIDATE,
                    source,
                    context,
                    resolvedArtifacts,
                    issues);
        }

        ResolvedDependencyRoots baselineRoots = resolveDependencyRoots(
                baseline,
                ResolutionScope.BASELINE,
                nonNullPrimaries(baselinePrimary),
                source,
                context,
                issues);
        ResolvedDependencyRoots candidateRoots = resolveDependencyRoots(
                candidate,
                ResolutionScope.CANDIDATE,
                nonNullPrimaries(candidatePrimary),
                source,
                context,
                issues);
        DependencySet baselineDependencies = null;
        DependencySet candidateDependencies = null;
        boolean sharedDependencies = baselineRoots != null
                && candidateRoots != null
                && baselineRoots.identities().equals(candidateRoots.identities());
        if (sharedDependencies) {
            ResolvedDependencyRoots commonRoots = mergeDependencyRoots(
                    baselineRoots, candidateRoots);
            DependencySet common = prepareResolvedDependencies(
                    baseline,
                    ResolutionScope.COMMON,
                    nonNullPrimaries(baselinePrimary, candidatePrimary),
                    commonRoots,
                    source,
                    context,
                    issues);
            baselineDependencies = common;
            candidateDependencies = common;
        } else {
            if (baselineRoots != null) {
                baselineDependencies = prepareResolvedDependencies(
                        baseline,
                        ResolutionScope.BASELINE,
                        nonNullPrimaries(baselinePrimary),
                        baselineRoots,
                        source,
                        context,
                        issues);
            }
            if (candidateRoots != null) {
                candidateDependencies = prepareResolvedDependencies(
                        candidate,
                        ResolutionScope.CANDIDATE,
                        nonNullPrimaries(candidatePrimary),
                        candidateRoots,
                        source,
                        context,
                        issues);
            }
        }

        if (sharedDependencies
                && baselinePrimary != null
                && baselinePrimary == candidatePrimary
                && baselineDependencies != null) {
            closures.add(buildExplicitClosure(
                    baseline,
                    ResolutionScope.COMMON,
                    baselinePrimary,
                    baselineDependencies));
            return;
        }

        if (baselinePrimary != null && baselineDependencies != null) {
            closures.add(buildExplicitClosure(
                    baseline,
                    ResolutionScope.BASELINE,
                    baselinePrimary,
                    baselineDependencies));
        }
        if (candidatePrimary != null && candidateDependencies != null) {
            closures.add(buildExplicitClosure(
                    candidate,
                    ResolutionScope.CANDIDATE,
                    candidatePrimary,
                    candidateDependencies));
        }
    }

    private PreparedConnectorClosure resolveConnector(
            ConnectorDeclaration declaration,
            ResolutionScope scope,
            Path source,
            PreparationContext context,
            List<ResolvedArtifact> resolvedArtifacts,
            List<ArtifactIssue> issues) {
        if (!declaration.explicit()) {
            return prepareAutoConnector(
                    declaration,
                    scope,
                    source,
                    context,
                    resolvedArtifacts,
                    issues);
        }
        PreparedPrimary primary = prepareDeclaredPrimary(
                declaration, scope, source, context, resolvedArtifacts, issues);
        DependencySet dependencies = prepareExplicitDependencies(
                declaration,
                scope,
                nonNullPrimaries(primary),
                source,
                context,
                issues);
        return primary == null || dependencies == null
                ? null
                : buildExplicitClosure(declaration, scope, primary, dependencies);
    }

    private PreparedConnectorClosure prepareAutoConnector(
            ConnectorDeclaration declaration,
            ResolutionScope scope,
            Path source,
            PreparationContext context,
            List<ResolvedArtifact> resolvedArtifacts,
            List<ArtifactIssue> issues) {
        ArtifactReference primaryReference = declaration.primaryReference(scope);
        if (!declaration.artifactReference().startsWith("maven:")) {
            issues.add(issue(
                    source,
                    primaryReference,
                    "artifact.connector.runtime-dependencies-required",
                    "A local connector primary must declare runtime_dependencies,"
                            + " using [] for a self-contained JAR"));
            return null;
        }
        MavenCoordinate coordinate;
        try {
            coordinate = MavenCoordinate.parse(declaration.artifactReference());
        } catch (IllegalArgumentException exception) {
            issues.add(issue(source, primaryReference,
                    "artifact.maven.invalid-coordinate", exception.getMessage()));
            return null;
        }

        MavenRuntimeClosure runtimeClosure;
        try {
            runtimeClosure = resolveMavenClosure(
                    List.of(new MavenRuntimeRoot(0, coordinate)), context);
        } catch (MavenArtifactLookupException exception) {
            issues.add(mavenIssue(source, primaryReference, exception));
            return null;
        }
        if (runtimeClosure.classpath().isEmpty()) {
            issues.add(issue(source, primaryReference,
                    "artifact.maven.invalid-closure",
                    "Maven auto closure did not contain its primary JAR"));
            return null;
        }
        ResolvedMavenJar selectedPrimary = runtimeClosure.classpath().getFirst();
        MavenArtifactIdentity expectedPrimary = new MavenArtifactIdentity(
                coordinate.groupId(), coordinate.artifactId(), "jar", "", coordinate.version());
        if (selectedPrimary.depth() != 0
                || selectedPrimary.originRootIndex() != 0
                || !selectedPrimary.identity().equals(expectedPrimary)) {
            issues.add(issue(source, primaryReference,
                    "artifact.maven.invalid-closure",
                    "Maven auto closure primary does not match the declared connector"));
            return null;
        }

        PreparedConnectorArtifact primary = prepareMavenEntry(
                selectedPrimary,
                primaryReference,
                declaration.artifactReference(),
                scope,
                0,
                true,
                source,
                context,
                issues);
        if (primary == null) {
            return null;
        }
        resolvedArtifacts.add(new ResolvedArtifact(
                scope,
                ArtifactRole.SUBJECT_CONNECTOR,
                declaration.artifactPath(),
                declaration.artifactReference(),
                primary.sourcePath(),
                primary.preparedPath(),
                primary.sha256()));

        List<PreparedConnectorArtifact> dependencies = new ArrayList<>();
        Set<ClasspathIdentity> selectedIdentities = new HashSet<>();
        selectedIdentities.add(classpathIdentity(primary));
        int classpathIndex = 1;
        for (int index = 1; index < runtimeClosure.classpath().size(); index++) {
            ResolvedMavenJar selected = runtimeClosure.classpath().get(index);
            PreparedConnectorArtifact dependency = prepareMavenEntry(
                    selected,
                    primaryReference,
                    selected.identity().coordinate(),
                    scope,
                    classpathIndex,
                    false,
                    source,
                    context,
                    issues);
            if (dependency != null
                    && selectedIdentities.add(classpathIdentity(dependency))) {
                dependencies.add(dependency);
                classpathIndex++;
            }
        }
        if (issues.stream().anyMatch(issue -> issue.scope() == scope
                && issue.path().equals(declaration.artifactPath()))) {
            return null;
        }
        return new PreparedConnectorClosure(
                scope,
                declaration.alias(),
                declaration.connectorPath(),
                ConnectorDependencyMode.AUTO,
                primary,
                dependencies,
                runtimeClosure.consultedPoms(),
                runtimeClosure.conflicts());
    }

    private PreparedPrimary prepareDeclaredPrimary(
            ConnectorDeclaration declaration,
            ResolutionScope scope,
            Path source,
            PreparationContext context,
            List<ResolvedArtifact> resolvedArtifacts,
            List<ArtifactIssue> issues) {
        ArtifactReference reference = declaration.primaryReference(scope);
        Optional<ResolvedArtifact> resolved = resolveReference(
                reference, source, context, issues);
        if (resolved.isEmpty()) {
            return null;
        }
        ResolvedArtifact artifact = resolved.get();
        MavenArtifactIdentity mavenIdentity = null;
        List<MavenArtifactIdentity> mavenPath = List.of();
        if (declaration.artifactReference().startsWith("maven:")) {
            MavenCoordinate coordinate = MavenCoordinate.parse(declaration.artifactReference());
            mavenIdentity = new MavenArtifactIdentity(
                    coordinate.groupId(), coordinate.artifactId(), "jar", "", coordinate.version());
            mavenPath = List.of(mavenIdentity);
        }
        PreparedConnectorArtifact entry = new PreparedConnectorArtifact(
                scope,
                0,
                true,
                -1,
                -1,
                declaration.artifactPath(),
                declaration.artifactReference(),
                mavenIdentity,
                mavenPath,
                artifact.sourcePath(),
                artifact.preparedPath(),
                artifact.sha256());
        resolvedArtifacts.add(artifact);
        return new PreparedPrimary(artifact, entry);
    }

    private DependencySet prepareExplicitDependencies(
            ConnectorDeclaration declaration,
            ResolutionScope scope,
            List<PreparedPrimary> primaries,
            Path source,
            PreparationContext context,
            List<ArtifactIssue> issues) {
        ResolvedDependencyRoots roots = resolveDependencyRoots(
                declaration, scope, primaries, source, context, issues);
        return roots == null
                ? null
                : prepareResolvedDependencies(
                        declaration,
                        scope,
                        primaries,
                        roots,
                        source,
                        context,
                        issues);
    }

    private ResolvedDependencyRoots resolveDependencyRoots(
            ConnectorDeclaration declaration,
            ResolutionScope scope,
            List<PreparedPrimary> primaries,
            Path source,
            PreparationContext context,
            List<ArtifactIssue> issues) {
        int initialIssueCount = issues.size();
        List<ResolvedDependencyRoot> roots = new ArrayList<>();
        Set<ResolvedDependencyIdentity> identities = new HashSet<>();
        List<ResolvedDependencyIdentity> orderedIdentities = new ArrayList<>();
        Set<ResolvedDependencyIdentity> primaryIdentities = primaries.stream()
                .map(PreparedPrimary::entry)
                .map(ArtifactPlanResolver::resolvedIdentity)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        for (RuntimeDependencyDeclaration dependency : declaration.dependencies()) {
            ArtifactReference reference = dependency.reference(scope);
            if (dependency.declaredReference().startsWith("maven:")) {
                MavenCoordinate coordinate;
                try {
                    coordinate = MavenCoordinate.parse(dependency.declaredReference());
                } catch (IllegalArgumentException exception) {
                    issues.add(issue(source, reference,
                            "artifact.maven.invalid-coordinate", exception.getMessage()));
                    continue;
                }
                ResolvedDependencyIdentity identity = new ResolvedDependencyIdentity(
                        "maven", coordinate.declaredReference());
                if (!addDependencyIdentity(
                        identity, primaryIdentities, identities, source, reference, issues)) {
                    continue;
                }
                orderedIdentities.add(identity);
                roots.add(new ResolvedDependencyRoot(
                        List.of(new DependencyRootOrigin(scope, dependency)),
                        coordinate,
                        null,
                        identity));
                continue;
            }

            Optional<Path> local = resolveLocal(
                    reference, source, context.workspace().artifactRoot(), issues);
            if (local.isEmpty()) {
                continue;
            }
            Optional<Path> usable = validateResolvedFile(
                    reference,
                    source,
                    local.get(),
                    context.workspace().artifactRoot(),
                    true,
                    issues);
            if (usable.isEmpty()) {
                continue;
            }
            ResolvedDependencyIdentity identity = new ResolvedDependencyIdentity(
                    "local", usable.get().toString());
            if (!addDependencyIdentity(
                    identity, primaryIdentities, identities, source, reference, issues)) {
                continue;
            }
            orderedIdentities.add(identity);
            roots.add(new ResolvedDependencyRoot(
                    List.of(new DependencyRootOrigin(scope, dependency)),
                    null,
                    usable.get(),
                    identity));
        }

        return issues.size() == initialIssueCount
                ? new ResolvedDependencyRoots(roots, orderedIdentities)
                : null;
    }

    private DependencySet prepareResolvedDependencies(
            ConnectorDeclaration declaration,
            ResolutionScope scope,
            List<PreparedPrimary> primaries,
            ResolvedDependencyRoots resolvedRoots,
            Path source,
            PreparationContext context,
            List<ArtifactIssue> issues) {
        List<ResolvedDependencyRoot> roots = resolvedRoots.roots();

        List<MavenRuntimeRoot> mavenRoots = roots.stream()
                .filter(root -> root.mavenCoordinate() != null)
                .map(root -> new MavenRuntimeRoot(
                        root.index(), root.mavenCoordinate()))
                .toList();
        MavenRuntimeClosure mavenClosure = null;
        if (!mavenRoots.isEmpty()) {
            ArtifactReference failureReference = declaration.dependencyGraphReference(scope);
            try {
                mavenClosure = resolveMavenClosure(mavenRoots, context);
            } catch (MavenArtifactLookupException exception) {
                issues.add(mavenIssue(source, failureReference, exception));
                return null;
            }
        }

        Map<Integer, ResolvedDependencyRoot> byIndex = new HashMap<>();
        roots.forEach(root -> byIndex.put(root.index(), root));
        List<DependencyInput> inputs = new ArrayList<>();
        for (ResolvedDependencyRoot root : roots) {
            if (root.localPath() != null) {
                inputs.add(DependencyInput.local(root));
            }
        }
        if (mavenClosure != null) {
            int sequence = 0;
            for (ResolvedMavenJar selected : mavenClosure.classpath()) {
                ResolvedDependencyRoot origin = byIndex.get(selected.originRootIndex());
                if (origin == null) {
                    issues.add(issue(source, declaration.primaryReference(scope),
                            "artifact.maven.invalid-closure",
                            "Maven closure returned an unknown dependency root index "
                                    + selected.originRootIndex()));
                    return null;
                }
                inputs.add(DependencyInput.maven(selected, origin, sequence++));
            }
        }
        inputs.sort(Comparator
                .comparingInt(DependencyInput::depth)
                .thenComparingInt(input -> input.depth() == 0
                        ? input.originRootIndex()
                        : input.mavenSequence()));

        List<PreparedConnectorArtifact> dependencies = new ArrayList<>();
        Map<ClasspathIdentity, List<PreparedPrimary>> primariesByIdentity = new LinkedHashMap<>();
        for (PreparedPrimary primary : primaries) {
            primariesByIdentity.computeIfAbsent(
                    classpathIdentity(primary.entry()), ignored -> new ArrayList<>())
                    .add(primary);
        }
        Set<ClasspathIdentity> primaryClasspathIdentities = Set.copyOf(
                primariesByIdentity.keySet());
        Set<ClasspathIdentity> selectedIdentities = new HashSet<>(
                primaryClasspathIdentities);
        Map<ClasspathIdentity, Integer> selectedDependencyIndexes = new HashMap<>();
        int classpathIndex = 1;
        for (DependencyInput input : inputs) {
            PreparedConnectorArtifact prepared = prepareDependencyInput(
                    input,
                    scope,
                    classpathIndex,
                    source,
                    context,
                    issues);
            if (prepared != null
                    && primaryClasspathIdentities.contains(classpathIdentity(prepared))) {
                Set<ResolutionScope> reportedScopes = new HashSet<>();
                for (PreparedPrimary matching : primariesByIdentity.get(
                        classpathIdentity(prepared))) {
                    ResolutionScope issueScope = matching.entry().scope();
                    if (reportedScopes.add(issueScope)) {
                        issues.add(issue(source, input.reference(issueScope),
                                "artifact.connector.runtime-dependency-duplicate",
                                "Runtime dependency closure duplicates a connector primary"));
                    }
                }
                continue;
            }
            if (prepared == null) {
                continue;
            }
            ClasspathIdentity identity = classpathIdentity(prepared);
            if (selectedIdentities.add(identity)) {
                dependencies.add(prepared);
                selectedDependencyIndexes.put(identity, dependencies.size() - 1);
                classpathIndex++;
            } else {
                Integer retainedIndex = selectedDependencyIndexes.get(identity);
                if (retainedIndex != null) {
                    PreparedConnectorArtifact retained = dependencies.get(retainedIndex);
                    dependencies.set(
                            retainedIndex,
                            withAdditionalOrigins(retained, prepared.origins()));
                }
            }
        }
        return new DependencySet(
                dependencies,
                mavenClosure == null ? List.of() : mavenClosure.consultedPoms(),
                mavenClosure == null ? List.of() : mavenClosure.conflicts());
    }

    private static ResolvedDependencyRoots mergeDependencyRoots(
            ResolvedDependencyRoots baseline,
            ResolvedDependencyRoots candidate) {
        if (!baseline.identities().equals(candidate.identities())) {
            throw new IllegalArgumentException(
                    "Only equal ordered dependency-root identities may be shared");
        }
        List<ResolvedDependencyRoot> merged = new ArrayList<>(baseline.roots().size());
        for (int index = 0; index < baseline.roots().size(); index++) {
            ResolvedDependencyRoot left = baseline.roots().get(index);
            ResolvedDependencyRoot right = candidate.roots().get(index);
            List<DependencyRootOrigin> origins = new ArrayList<>(
                    left.origins().size() + right.origins().size());
            origins.addAll(left.origins());
            origins.addAll(right.origins());
            merged.add(new ResolvedDependencyRoot(
                    origins,
                    left.mavenCoordinate(),
                    left.localPath(),
                    left.identity()));
        }
        return new ResolvedDependencyRoots(merged, baseline.identities());
    }

    private PreparedConnectorArtifact prepareDependencyInput(
            DependencyInput input,
            ResolutionScope scope,
            int classpathIndex,
            Path source,
            PreparationContext context,
            List<ArtifactIssue> issues) {
        ArtifactReference reference = input.reference(scope);
        Optional<Path> usable = validateResolvedFile(
                reference,
                source,
                input.sourcePath(),
                context.workspace().artifactRoot(),
                input.mavenJar() == null,
                issues);
        if (usable.isEmpty()) {
            return null;
        }
        Optional<StagedArtifact> staged = stage(
                reference,
                source,
                usable.get(),
                context.workspace().artifactRoot(),
                context.workspace().preparationRoot(),
                issues);
        if (staged.isEmpty()) {
            return null;
        }
        if (input.mavenJar() != null
                && !input.mavenJar().sha256().equals(staged.get().sha256())) {
            issues.add(issue(source, reference,
                    "artifact.staging.source-changed",
                    "Resolved Maven JAR changed after closure resolution"));
            return null;
        }
        if (!validateJar(reference, source, staged.get().path(), issues)) {
            return null;
        }
        MavenArtifactIdentity identity = input.mavenJar() == null
                ? null
                : input.mavenJar().identity();
        return new PreparedConnectorArtifact(
                scope,
                classpathIndex,
                false,
                input.originRootIndex(),
                input.depth(),
                input.root().firstOrigin().declaration().path(),
                input.sourceReference(),
                input.root().preparedOrigins(),
                identity,
                input.mavenJar() == null
                        ? List.of()
                        : input.mavenJar().dependencyPath(),
                usable.get(),
                staged.get().path(),
                staged.get().sha256());
    }

    private PreparedConnectorArtifact prepareMavenEntry(
            ResolvedMavenJar selected,
            ArtifactReference declarationReference,
            String sourceReference,
            ResolutionScope scope,
            int classpathIndex,
            boolean primary,
            Path source,
            PreparationContext context,
            List<ArtifactIssue> issues) {
        Optional<Path> usable = validateResolvedFile(
                declarationReference,
                source,
                selected.sourcePath(),
                context.workspace().artifactRoot(),
                false,
                issues);
        if (usable.isEmpty()) {
            return null;
        }
        Optional<StagedArtifact> staged = stage(
                declarationReference,
                source,
                usable.get(),
                context.workspace().artifactRoot(),
                context.workspace().preparationRoot(),
                issues);
        if (staged.isEmpty()) {
            return null;
        }
        if (!selected.sha256().equals(staged.get().sha256())) {
            issues.add(issue(source, declarationReference,
                    "artifact.staging.source-changed",
                    "Resolved Maven JAR changed after closure resolution"));
            return null;
        }
        if (!validateJar(declarationReference, source, staged.get().path(), issues)) {
            return null;
        }
        return new PreparedConnectorArtifact(
                scope,
                classpathIndex,
                primary,
                primary ? -1 : selected.originRootIndex(),
                primary ? -1 : selected.depth(),
                declarationReference.path(),
                sourceReference,
                List.of(new PreparedConnectorOrigin(
                        scope,
                        PreparedConnectorOrigin.RootKind.PRIMARY,
                        -1,
                        declarationReference.path(),
                        declarationReference.reference())),
                selected.identity(),
                selected.dependencyPath(),
                usable.get(),
                staged.get().path(),
                staged.get().sha256());
    }

    private static boolean addDependencyIdentity(
            ResolvedDependencyIdentity identity,
            Set<ResolvedDependencyIdentity> primaryIdentities,
            Set<ResolvedDependencyIdentity> selected,
            Path source,
            ArtifactReference reference,
            List<ArtifactIssue> issues) {
        if (primaryIdentities.contains(identity) || !selected.add(identity)) {
            issues.add(issue(source, reference,
                    "artifact.connector.runtime-dependency-duplicate",
                    "Runtime dependency resolves to a duplicate primary or dependency reference"));
            return false;
        }
        return true;
    }

    private static ResolvedDependencyIdentity resolvedIdentity(
            PreparedConnectorArtifact artifact) {
        return artifact.mavenIdentity()
                .map(identity -> new ResolvedDependencyIdentity(
                        "maven",
                        "maven:" + identity.groupId() + ":" + identity.artifactId()
                                + ":" + identity.version()))
                .orElseGet(() -> new ResolvedDependencyIdentity(
                        "local", artifact.sourcePath().toString()));
    }

    private static ClasspathIdentity classpathIdentity(
            PreparedConnectorArtifact artifact) {
        return artifact.mavenIdentity()
                .map(identity -> new ClasspathIdentity("maven", identity))
                .orElseGet(() -> new ClasspathIdentity("local-sha256", artifact.sha256()));
    }

    private static PreparedConnectorArtifact withAdditionalOrigins(
            PreparedConnectorArtifact artifact,
            List<PreparedConnectorOrigin> additionalOrigins) {
        List<PreparedConnectorOrigin> origins = new ArrayList<>(artifact.origins());
        for (PreparedConnectorOrigin origin : additionalOrigins) {
            if (!origins.contains(origin)) {
                origins.add(origin);
            }
        }
        return new PreparedConnectorArtifact(
                artifact.scope(),
                artifact.classpathIndex(),
                artifact.primary(),
                artifact.originRootIndex().orElse(-1),
                artifact.dependencyDepth().orElse(-1),
                artifact.declarationPath(),
                artifact.sourceReference(),
                origins,
                artifact.mavenIdentity().orElse(null),
                artifact.mavenDependencyPath(),
                artifact.sourcePath(),
                artifact.preparedPath(),
                artifact.sha256());
    }

    private static List<PreparedPrimary> nonNullPrimaries(PreparedPrimary... primaries) {
        List<PreparedPrimary> selected = new ArrayList<>(primaries.length);
        for (PreparedPrimary primary : primaries) {
            if (primary != null) {
                selected.add(primary);
            }
        }
        return List.copyOf(selected);
    }

    private static PreparedConnectorClosure buildExplicitClosure(
            ConnectorDeclaration declaration,
            ResolutionScope scope,
            PreparedPrimary primary,
            DependencySet dependencies) {
        return new PreparedConnectorClosure(
                scope,
                declaration.alias(),
                declaration.connectorPath(),
                ConnectorDependencyMode.EXPLICIT,
                primary.entry(),
                dependencies.entries(),
                dependencies.consultedPoms(),
                dependencies.conflicts());
    }

    private MavenPrimaryResolution resolvePrimaryMaven(
            MavenCoordinate coordinate,
            PreparationContext context) throws MavenArtifactLookupException {
        MavenPrimaryResolution cached = context.mavenPrimaryCache().get(coordinate);
        if (cached != null) {
            return cached;
        }
        Path resolved = mavenLookup.resolve(coordinate, context.offline());
        if (resolved == null) {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                    "The configured Maven lookup returned no primary JAR for "
                            + coordinate.declaredReference());
        }
        String sha256;
        try {
            sha256 = uncheckedSha256(resolved);
        } catch (ChecksumFailure exception) {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                    "Could not hash resolved Maven primary " + coordinate.declaredReference(),
                    exception.getCause());
        }
        registerStableHashes(
                Map.of(mavenIdentity(coordinate), sha256),
                context.mavenJarHashes(),
                "Maven JAR");
        MavenPrimaryResolution resolution = new MavenPrimaryResolution(resolved, sha256);
        context.mavenPrimaryCache().put(coordinate, resolution);
        return resolution;
    }

    private static boolean verifyPrimaryMavenHash(
            MavenCoordinate coordinate,
            String stagedSha256,
            PreparationContext context,
            Path source,
            ArtifactReference reference,
            List<ArtifactIssue> issues) {
        MavenPrimaryResolution resolution = context.mavenPrimaryCache().get(coordinate);
        if (resolution == null) {
            throw new IllegalStateException("Maven primary was not cached before staging");
        }
        if (resolution.sha256().equals(stagedSha256)) {
            return true;
        }
        issues.add(issue(source, reference,
                "artifact.staging.source-changed",
                "Resolved Maven primary changed during this preparation"));
        return false;
    }

    private MavenRuntimeClosure resolveMavenClosure(
            List<MavenRuntimeRoot> roots,
            PreparationContext context) throws MavenArtifactLookupException {
        List<MavenRuntimeRoot> key = List.copyOf(roots);
        MavenRuntimeClosure cached = context.mavenClosureCache().get(key);
        if (cached != null) {
            validateAndRegisterClosureEvidence(cached, context);
            return cached;
        }
        MavenRuntimeClosure resolved;
        try {
            resolved = mavenLookup.resolveRuntimeClosure(key, context.offline());
        } catch (UnsupportedOperationException exception) {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                    "The configured Maven lookup cannot resolve runtime closures",
                    exception);
        }
        if (resolved == null) {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                    "The configured Maven lookup returned no runtime closure");
        }
        validateAndRegisterClosureEvidence(resolved, context);
        context.mavenClosureCache().put(key, resolved);
        return resolved;
    }

    private static void validateAndRegisterClosureEvidence(
            MavenRuntimeClosure closure,
            PreparationContext context) throws MavenArtifactLookupException {
        Map<MavenArtifactIdentity, String> jarHashes = new LinkedHashMap<>();
        for (ResolvedMavenJar selected : closure.classpath()) {
            String earlier = jarHashes.putIfAbsent(selected.identity(), selected.sha256());
            if (earlier != null && !earlier.equals(selected.sha256())) {
                throw unstableMavenIdentity("Maven JAR", selected.identity());
            }
        }

        Map<MavenArtifactIdentity, String> pomHashes = new LinkedHashMap<>();
        for (MavenPomEvidence evidence : closure.consultedPoms()) {
            String actual;
            try {
                actual = uncheckedSha256(evidence.sourcePath());
            } catch (ChecksumFailure exception) {
                throw new MavenArtifactLookupException(
                        MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                        "Could not rehash consulted Maven POM "
                                + evidence.identity().coordinate(),
                        exception.getCause());
            }
            if (!actual.equals(evidence.sha256())) {
                throw new MavenArtifactLookupException(
                        MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                        "Consulted Maven POM changed after closure resolution: "
                                + evidence.identity().coordinate());
            }
            String earlier = pomHashes.putIfAbsent(evidence.identity(), actual);
            if (earlier != null && !earlier.equals(actual)) {
                throw unstableMavenIdentity("Maven POM", evidence.identity());
            }
        }

        ensureStableHashes(jarHashes, context.mavenJarHashes(), "Maven JAR");
        ensureStableHashes(pomHashes, context.mavenPomHashes(), "Maven POM");
        context.mavenJarHashes().putAll(jarHashes);
        context.mavenPomHashes().putAll(pomHashes);
    }

    private static void registerStableHashes(
            Map<MavenArtifactIdentity, String> candidateHashes,
            Map<MavenArtifactIdentity, String> registeredHashes,
            String materialKind) throws MavenArtifactLookupException {
        ensureStableHashes(candidateHashes, registeredHashes, materialKind);
        registeredHashes.putAll(candidateHashes);
    }

    private static void ensureStableHashes(
            Map<MavenArtifactIdentity, String> candidateHashes,
            Map<MavenArtifactIdentity, String> registeredHashes,
            String materialKind) throws MavenArtifactLookupException {
        for (Map.Entry<MavenArtifactIdentity, String> candidate : candidateHashes.entrySet()) {
            String registered = registeredHashes.get(candidate.getKey());
            if (registered != null && !registered.equals(candidate.getValue())) {
                throw unstableMavenIdentity(materialKind, candidate.getKey());
            }
        }
    }

    private static MavenArtifactLookupException unstableMavenIdentity(
            String materialKind,
            MavenArtifactIdentity identity) {
        return new MavenArtifactLookupException(
                MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                materialKind + " identity resolved to different bytes in one preparation: "
                        + identity.coordinate());
    }

    private static MavenArtifactIdentity mavenIdentity(MavenCoordinate coordinate) {
        return new MavenArtifactIdentity(
                coordinate.groupId(),
                coordinate.artifactId(),
                "jar",
                "",
                coordinate.version());
    }

    private static ArtifactIssue mavenIssue(
            Path source,
            ArtifactReference reference,
            MavenArtifactLookupException exception) {
        String code = switch (exception.kind()) {
            case NOT_FOUND -> "artifact.maven.not-found";
            case REPOSITORY_UNAVAILABLE -> "artifact.maven.repository-unavailable";
            case OFFLINE_MISS -> "artifact.maven.offline-miss";
            case INVALID_CLOSURE -> "artifact.maven.invalid-closure";
        };
        return issue(source, reference, code, exception.getMessage());
    }

    private static Map<String, ConnectorDeclaration> connectorDeclarations(
            ResolvedSide side) {
        JsonNode connectors = side.document().at("/subject/connectors");
        if (!(connectors instanceof ObjectNode connectorObject)) {
            return Map.of();
        }
        Map<String, ConnectorDeclaration> declarations = new LinkedHashMap<>();
        connectorObject.fields().forEachRemaining(field -> {
            String alias = field.getKey();
            ObjectNode connector = (ObjectNode) field.getValue();
            String connectorPath = "$/subject/connectors/" + escapePointer(alias);
            List<RuntimeDependencyDeclaration> dependencies = new ArrayList<>();
            boolean explicit = connector.has("runtime_dependencies");
            if (connector.get("runtime_dependencies") instanceof ArrayNode dependencyArray) {
                for (int index = 0; index < dependencyArray.size(); index++) {
                    dependencies.add(new RuntimeDependencyDeclaration(
                            index,
                            connectorPath + "/runtime_dependencies/" + index,
                            dependencyArray.get(index).textValue()));
                }
            }
            declarations.put(alias, new ConnectorDeclaration(
                    alias,
                    connectorPath,
                    connectorPath + "/artifact",
                    connector.path("artifact").textValue(),
                    explicit,
                    dependencies));
        });
        return declarations.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (left, right) -> left,
                        LinkedHashMap::new));
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

    private record ConnectorDeclaration(
            String alias,
            String connectorPath,
            String artifactPath,
            String artifactReference,
            boolean explicit,
            List<RuntimeDependencyDeclaration> dependencies) {
        private ConnectorDeclaration {
            dependencies = List.copyOf(dependencies);
        }

        private ArtifactReference primaryReference(ResolutionScope scope) {
            return new ArtifactReference(
                    scope,
                    ArtifactRole.SUBJECT_CONNECTOR,
                    artifactPath,
                    artifactReference);
        }

        private ArtifactReference dependencyGraphReference(ResolutionScope scope) {
            return new ArtifactReference(
                    scope,
                    ArtifactRole.SUBJECT_CONNECTOR,
                    connectorPath + "/runtime_dependencies",
                    alias);
        }

        private boolean sameDefinition(ConnectorDeclaration other) {
            return artifactReference.equals(other.artifactReference)
                    && explicit == other.explicit
                    && sameDependencyReferences(other);
        }

        private boolean sameDependencyReferences(ConnectorDeclaration other) {
            return explicit == other.explicit
                    && dependencies.stream()
                            .map(RuntimeDependencyDeclaration::declaredReference)
                            .toList()
                            .equals(other.dependencies.stream()
                                    .map(RuntimeDependencyDeclaration::declaredReference)
                                    .toList());
        }
    }

    private record RuntimeDependencyDeclaration(
            int index,
            String path,
            String declaredReference) {
        private ArtifactReference reference(ResolutionScope scope) {
            return new ArtifactReference(
                    scope,
                    ArtifactRole.SUBJECT_CONNECTOR,
                    path,
                    declaredReference);
        }
    }

    private record PreparedPrimary(
            ResolvedArtifact artifact,
            PreparedConnectorArtifact entry) {}

    private record DependencySet(
            List<PreparedConnectorArtifact> entries,
            List<MavenPomEvidence> consultedPoms,
            List<MavenConflictDecision> conflicts) {
        private DependencySet {
            entries = List.copyOf(entries);
            consultedPoms = List.copyOf(consultedPoms);
            conflicts = List.copyOf(conflicts);
        }
    }

    private record DependencyRootOrigin(
            ResolutionScope scope,
            RuntimeDependencyDeclaration declaration) {
        private DependencyRootOrigin {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(declaration, "declaration");
        }

        private ArtifactReference reference() {
            return declaration.reference(scope);
        }

        private PreparedConnectorOrigin preparedOrigin() {
            return new PreparedConnectorOrigin(
                    scope,
                    PreparedConnectorOrigin.RootKind.RUNTIME_DEPENDENCY,
                    declaration.index(),
                    declaration.path(),
                    declaration.declaredReference());
        }
    }

    private record ResolvedDependencyRoot(
            List<DependencyRootOrigin> origins,
            MavenCoordinate mavenCoordinate,
            Path localPath,
            ResolvedDependencyIdentity identity) {
        private ResolvedDependencyRoot {
            origins = List.copyOf(origins);
            if (origins.isEmpty()) {
                throw new IllegalArgumentException("A dependency root needs an origin");
            }
            Objects.requireNonNull(identity, "identity");
            if ((mavenCoordinate == null) == (localPath == null)) {
                throw new IllegalArgumentException(
                        "A dependency root must be exactly one of Maven or local");
            }
            int index = origins.getFirst().declaration().index();
            if (origins.stream().anyMatch(origin -> origin.declaration().index() != index)) {
                throw new IllegalArgumentException(
                        "Shared dependency roots must keep the same declaration index");
            }
        }

        private int index() {
            return origins.getFirst().declaration().index();
        }

        private DependencyRootOrigin firstOrigin() {
            return origins.getFirst();
        }

        private List<PreparedConnectorOrigin> preparedOrigins() {
            return origins.stream().map(DependencyRootOrigin::preparedOrigin).toList();
        }
    }

    private record ResolvedDependencyRoots(
            List<ResolvedDependencyRoot> roots,
            List<ResolvedDependencyIdentity> identities) {
        private ResolvedDependencyRoots {
            roots = List.copyOf(roots);
            identities = List.copyOf(identities);
            if (roots.size() != identities.size()) {
                throw new IllegalArgumentException(
                        "Dependency roots and ordered identities must have equal size");
            }
        }
    }

    private record ResolvedDependencyIdentity(String kind, String identity) {}

    private record DependencyInput(
            ResolvedDependencyRoot root,
            Path sourcePath,
            ResolvedMavenJar mavenJar,
            int depth,
            int originRootIndex,
            int mavenSequence,
            String sourceReference) {
        private static DependencyInput local(ResolvedDependencyRoot root) {
            return new DependencyInput(
                    root,
                    root.localPath(),
                    null,
                    0,
                    root.index(),
                    Integer.MAX_VALUE,
                    root.firstOrigin().declaration().declaredReference());
        }

        private static DependencyInput maven(
                ResolvedMavenJar selected,
                ResolvedDependencyRoot root,
                int sequence) {
            return new DependencyInput(
                    root,
                    selected.sourcePath(),
                    selected,
                    selected.depth(),
                    selected.originRootIndex(),
                    sequence,
                    selected.depth() == 0
                            ? root.firstOrigin().declaration().declaredReference()
                            : selected.identity().coordinate());
        }

        private ArtifactReference reference(ResolutionScope scope) {
            RuntimeDependencyDeclaration declaration = root.firstOrigin().declaration();
            return new ArtifactReference(
                    scope,
                    ArtifactRole.SUBJECT_CONNECTOR,
                    declaration.path(),
                    declaration.declaredReference());
        }
    }

    private static final class PreparationContext {
        private final ArtifactWorkspace workspace;
        private final boolean offline;
        private final Map<MavenCoordinate, MavenPrimaryResolution> mavenPrimaryCache =
                new HashMap<>();
        private final Map<List<MavenRuntimeRoot>, MavenRuntimeClosure> mavenClosureCache =
                new HashMap<>();
        private final Map<MavenArtifactIdentity, String> mavenJarHashes = new HashMap<>();
        private final Map<MavenArtifactIdentity, String> mavenPomHashes = new HashMap<>();

        private PreparationContext(ArtifactWorkspace workspace, boolean offline) {
            this.workspace = Objects.requireNonNull(workspace, "workspace");
            this.offline = offline;
        }

        private ArtifactWorkspace workspace() {
            return workspace;
        }

        private boolean offline() {
            return offline;
        }

        private Map<MavenCoordinate, MavenPrimaryResolution> mavenPrimaryCache() {
            return mavenPrimaryCache;
        }

        private Map<List<MavenRuntimeRoot>, MavenRuntimeClosure> mavenClosureCache() {
            return mavenClosureCache;
        }

        private Map<MavenArtifactIdentity, String> mavenJarHashes() {
            return mavenJarHashes;
        }

        private Map<MavenArtifactIdentity, String> mavenPomHashes() {
            return mavenPomHashes;
        }
    }

    private record ReferenceIdentity(ArtifactRole role, String path) {}

    private record StagedArtifact(Path path, String sha256) {}

    private record MavenPrimaryResolution(Path sourcePath, String sha256) {
        private MavenPrimaryResolution {
            sourcePath = Objects.requireNonNull(sourcePath, "sourcePath")
                    .toAbsolutePath().normalize();
            sha256 = Objects.requireNonNull(sha256, "sha256");
        }
    }

    private record ClasspathIdentity(String kind, Object identity) {}

    private static final class ChecksumFailure extends RuntimeException {
        private ChecksumFailure(Throwable cause) {
            super(cause);
        }
    }
}
