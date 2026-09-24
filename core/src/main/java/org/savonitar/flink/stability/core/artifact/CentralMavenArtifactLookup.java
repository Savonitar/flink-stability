package org.savonitar.flink.stability.core.artifact;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.AbstractRepositoryListener;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositoryEvent;
import org.eclipse.aether.RepositoryException;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.collection.DependencyCollectionContext;
import org.eclipse.aether.collection.DependencyCollectionException;
import org.eclipse.aether.collection.DependencyGraphTransformationContext;
import org.eclipse.aether.collection.DependencySelector;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactDescriptorException;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.supplier.RepositorySystemSupplier;
import org.eclipse.aether.transfer.ArtifactNotFoundException;
import org.eclipse.aether.transfer.ArtifactTransferException;
import org.eclipse.aether.transfer.RepositoryOfflineException;
import org.eclipse.aether.util.graph.selector.AndDependencySelector;
import org.eclipse.aether.util.graph.transformer.ChainedDependencyGraphTransformer;
import org.eclipse.aether.util.graph.transformer.ConflictResolver;
import org.eclipse.aether.util.graph.transformer.JavaDependencyContextRefiner;
import org.eclipse.aether.util.graph.transformer.JavaScopeDeriver;
import org.eclipse.aether.util.graph.transformer.JavaScopeSelector;
import org.eclipse.aether.util.graph.transformer.SimpleOptionalitySelector;
import org.eclipse.aether.util.repository.SimpleArtifactDescriptorPolicy;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/** Resolves primary JARs and deterministic runtime closures from fixed repositories. */
final class CentralMavenArtifactLookup implements MavenArtifactLookup {
    private static final URI MAVEN_CENTRAL = URI.create(
            "https://repo.maven.apache.org/maven2/");
    private static final Pattern TIMESTAMPED_SNAPSHOT = Pattern.compile(
            ".*-\\d{8}\\.\\d{6}-\\d+$");
    private static final Set<String> RUNTIME_SCOPES = Set.of("compile", "runtime");
    private static final Map<String, String> CANONICAL_MODEL_SYSTEM_PROPERTIES = Map.of(
            "java.version", "21",
            "java.specification.version", "21");

    private final Path localRepository;
    private final List<RemoteRepository> repositories;

    CentralMavenArtifactLookup() {
        this.localRepository = null;
        this.repositories = repositories(List.of(MAVEN_CENTRAL));
    }

    CentralMavenArtifactLookup(Path localRepository, List<URI> repositories) {
        this.localRepository = Objects.requireNonNull(localRepository, "localRepository")
                .toAbsolutePath().normalize();
        this.repositories = repositories(repositories);
    }

    private static List<RemoteRepository> repositories(List<URI> requestedRepositories) {
        List<URI> uris = List.copyOf(Objects.requireNonNull(
                requestedRepositories, "requestedRepositories"));
        if (new HashSet<>(uris).size() != uris.size()) {
            throw new IllegalArgumentException("Configured Maven repositories must be unique");
        }
        List<RemoteRepository> fixedRepositories = new ArrayList<>();
        for (int index = 0; index < uris.size(); index++) {
            URI uri = Objects.requireNonNull(uris.get(index), "repository URI");
            String repositoryId = MAVEN_CENTRAL.equals(uri)
                    ? "central"
                    : "v1-repository-" + index + "-" + Digests.sha256(uri.toASCIIString());
            fixedRepositories.add(new RemoteRepository.Builder(
                    repositoryId, "default", uri.toString()).build());
        }
        return List.copyOf(fixedRepositories);
    }

    @Override
    public Path resolve(MavenCoordinate coordinate, boolean offline)
            throws MavenArtifactLookupException {
        Objects.requireNonNull(coordinate, "coordinate");
        try {
            Path effectiveLocalRepository = checkedLocalRepository();
            RepositorySystem system = newRepositorySystem();
            DefaultRepositorySystemSession session = new DefaultRepositorySystemSession();
            session.setLocalRepositoryManager(system.newLocalRepositoryManager(
                    session, new LocalRepository(effectiveLocalRepository.toString())));
            session.setOffline(offline);
            ArtifactRequest request = new ArtifactRequest(
                    new DefaultArtifact(coordinate.resolverCoordinate()), repositories, null);
            return system.resolveArtifact(session, request)
                    .getArtifact()
                    .getFile()
                    .toPath()
                    .toAbsolutePath()
                    .normalize();
        } catch (ArtifactResolutionException | RuntimeException exception) {
            throw resolutionFailure(
                    exception,
                    offline,
                    "Could not resolve " + coordinate.declaredReference()
                            + (offline
                                    ? " from the local Maven repository in offline mode"
                                    : " from the local Maven repository or Maven Central"));
        }
    }

    @Override
    public MavenRuntimeClosure resolveRuntimeClosure(
            List<MavenRuntimeRoot> requestedRoots,
            boolean offline) throws MavenArtifactLookupException {
        List<MavenRuntimeRoot> roots = checkedRoots(requestedRoots);
        String rootSummary = roots.stream()
                .map(root -> root.coordinate().declaredReference())
                .toList()
                .toString();
        PomEvidenceListener pomEvidence = new PomEvidenceListener();
        try {
            RepositorySystem system = newRepositorySystem();
            DefaultRepositorySystemSession session = closureSession(
                    system, checkedLocalRepository(), offline, pomEvidence);
            CollectRequest request = new CollectRequest()
                    .setRootArtifact(new DefaultArtifact(
                            "org.savonitar.flink.stability:runtime-closure:pom:1"))
                    .setDependencies(roots.stream()
                            .map(root -> new Dependency(
                                    new DefaultArtifact(root.coordinate().resolverCoordinate()),
                                    "runtime",
                                    Boolean.FALSE))
                            .toList())
                    .setRepositories(repositories);

            DependencyNode graph = system.collectDependencies(session, request).getRoot();
            GraphSelection selection = selectRuntimeGraph(graph, roots);
            DependencyResult resolved = system.resolveDependencies(
                    session,
                    new DependencyRequest(
                            graph,
                            (node, parents) -> selection.selectedNodeSet().contains(node)));

            List<ResolvedMavenJar> classpath = materializeClasspath(selection, resolved);
            return new MavenRuntimeClosure(
                    classpath,
                    selection.conflicts(),
                    pomEvidence.evidence());
        } catch (MavenArtifactLookupException exception) {
            throw exception;
        } catch (DependencyCollectionException | DependencyResolutionException exception) {
            throw resolutionFailure(
                    exception,
                    offline,
                    "Could not resolve Maven runtime closure rooted at " + rootSummary
                            + (offline
                                    ? " from the local Maven repository in offline mode"
                                    : " from the local Maven repository or the configured fixed repositories"));
        } catch (IOException exception) {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.REPOSITORY_UNAVAILABLE,
                    "Could not read or hash a resolved Maven runtime closure rooted at "
                            + rootSummary,
                    exception);
        } catch (RuntimeException exception) {
            throw resolutionFailure(
                    exception,
                    offline,
                    "Could not resolve Maven runtime closure rooted at " + rootSummary);
        }
    }

    private DefaultRepositorySystemSession closureSession(
            RepositorySystem system,
            Path effectiveLocalRepository,
            boolean offline,
            PomEvidenceListener pomEvidence) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        // Effective-model profile activation must not depend on the host JDK running the CLI.
        // Java 21 is the v1 preparation model and satisfies published Flink parent POMs whose
        // JDK ranges otherwise fail to evaluate in Resolver's initially empty system map.
        session.setSystemProperties(CANONICAL_MODEL_SYSTEM_PROPERTIES);
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(
                session, new LocalRepository(effectiveLocalRepository.toString())));
        session.setOffline(offline);
        session.setIgnoreArtifactDescriptorRepositories(true);
        session.setArtifactDescriptorPolicy(new SimpleArtifactDescriptorPolicy(false, false));
        session.setRepositoryListener(pomEvidence);
        session.setDependencySelector(AndDependencySelector.newInstance(
                session.getDependencySelector(),
                RuntimeDependencySelector.INSTANCE));
        session.setDependencyGraphTransformer(new ChainedDependencyGraphTransformer(
                new ConflictResolver(
                        new FirstBreadthFirstNearestVersionSelector(),
                        new JavaScopeSelector(),
                        new SimpleOptionalitySelector(),
                        new JavaScopeDeriver()),
                new JavaDependencyContextRefiner()));
        session.setConfigProperty(
                ConflictResolver.CONFIG_PROP_VERBOSE,
                ConflictResolver.Verbosity.FULL);
        return session;
    }

    private static List<MavenRuntimeRoot> checkedRoots(
            List<MavenRuntimeRoot> requestedRoots) {
        List<MavenRuntimeRoot> roots = List.copyOf(Objects.requireNonNull(
                requestedRoots, "requestedRoots"));
        if (roots.isEmpty()) {
            throw new IllegalArgumentException("At least one Maven runtime root is required");
        }
        Set<Integer> indexes = new HashSet<>();
        for (MavenRuntimeRoot root : roots) {
            if (!indexes.add(root.declarationIndex())) {
                throw new IllegalArgumentException(
                        "Duplicate Maven runtime root declaration index: "
                                + root.declarationIndex());
            }
        }
        return roots;
    }

    private static GraphSelection selectRuntimeGraph(
            DependencyNode syntheticRoot,
            List<MavenRuntimeRoot> roots) throws MavenArtifactLookupException {
        List<DependencyNode> rootNodes = syntheticRoot.getChildren();
        if (rootNodes.size() != roots.size()) {
            throw invalidClosure(
                    "Maven Resolver returned " + rootNodes.size() + " root nodes for "
                            + roots.size() + " requested roots");
        }

        IndexedGraph indexedGraph = indexGraph(rootNodes, roots);
        IdentityHashMap<DependencyNode, NodeVisit> allVisits = indexedGraph.visits();
        List<MavenConflictDecision> conflicts = new ArrayList<>();
        for (NodeVisit visit : indexedGraph.breadthFirstVisits()) {
            DependencyNode winnerNode = conflictWinner(visit.node());
            validateNode(visit, winnerNode == null);
            if (winnerNode == null) {
                continue;
            }
            NodeVisit winner = allVisits.get(winnerNode);
            if (winner == null) {
                throw invalidClosure(
                        "Maven Resolver reported a conflict winner outside the collected graph for "
                                + visit.identity().coordinate());
            }
            conflicts.add(conflictDecision(visit, winner));
        }

        Deque<NodeVisit> queue = new ArrayDeque<>();
        for (DependencyNode rootNode : rootNodes) {
            queue.addLast(allVisits.get(rootNode));
        }

        List<NodeVisit> selected = new ArrayList<>();
        Set<DependencyNode> selectedNodes = Collections.newSetFromMap(
                new IdentityHashMap<>());
        Set<DependencyNode> processed = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!queue.isEmpty()) {
            NodeVisit visit = queue.removeFirst();
            if (!processed.add(visit.node())) {
                continue;
            }
            DependencyNode winnerNode = conflictWinner(visit.node());
            if (winnerNode != null) {
                continue;
            }
            selected.add(visit);
            selectedNodes.add(visit.node());
            for (DependencyNode child : visit.node().getChildren()) {
                NodeVisit childVisit = allVisits.get(child);
                if (childVisit == null) {
                    throw invalidClosure("Maven Resolver returned an unindexed dependency node");
                }
                queue.addLast(childVisit);
            }
        }
        return new GraphSelection(selected, selectedNodes, conflicts);
    }

    private static IndexedGraph indexGraph(
            List<DependencyNode> rootNodes,
            List<MavenRuntimeRoot> roots) throws MavenArtifactLookupException {
        IdentityHashMap<DependencyNode, NodeVisit> visits = new IdentityHashMap<>();
        List<NodeVisit> breadthFirstVisits = new ArrayList<>();
        Deque<NodeVisit> queue = new ArrayDeque<>();
        for (int index = 0; index < rootNodes.size(); index++) {
            DependencyNode node = rootNodes.get(index);
            MavenArtifactIdentity identity = identityOf(node);
            MavenRuntimeRoot root = roots.get(index);
            NodeVisit visit = new NodeVisit(
                    node,
                    identity,
                    0,
                    root.declarationIndex(),
                    List.of(identity));
            visits.put(node, visit);
            breadthFirstVisits.add(visit);
            queue.addLast(visit);
        }

        while (!queue.isEmpty()) {
            NodeVisit parent = queue.removeFirst();
            for (DependencyNode child : parent.node().getChildren()) {
                if (visits.containsKey(child)) {
                    continue;
                }
                MavenArtifactIdentity identity = identityOf(child);
                List<MavenArtifactIdentity> path = new ArrayList<>(parent.dependencyPath());
                path.add(identity);
                NodeVisit visit = new NodeVisit(
                        child,
                        identity,
                        parent.depth() + 1,
                        parent.originRootIndex(),
                        path);
                visits.put(child, visit);
                breadthFirstVisits.add(visit);
                queue.addLast(visit);
            }
        }
        return new IndexedGraph(visits, breadthFirstVisits);
    }

    private static MavenArtifactIdentity identityOf(DependencyNode node)
            throws MavenArtifactLookupException {
        Artifact artifact = node.getArtifact();
        if (artifact == null) {
            throw invalidClosure("Maven Resolver returned a dependency node without an artifact");
        }
        try {
            return MavenArtifactIdentity.from(artifact);
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new MavenArtifactLookupException(
                    MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                    "Maven Resolver returned an incomplete artifact identity",
                    exception);
        }
    }

    private static DependencyNode conflictWinner(DependencyNode node)
            throws MavenArtifactLookupException {
        Object winner = node.getData().get(ConflictResolver.NODE_DATA_WINNER);
        if (winner == null) {
            return null;
        }
        if (!(winner instanceof DependencyNode winnerNode)) {
            throw invalidClosure(
                    "Maven Resolver returned malformed FULL conflict evidence for "
                            + identityOf(node).coordinate());
        }
        return winnerNode == node ? null : winnerNode;
    }

    private static void validateNode(NodeVisit visit, boolean selected)
            throws MavenArtifactLookupException {
        DependencyNode node = visit.node();
        MavenArtifactIdentity identity = visit.identity();
        if (containsPlaceholder(identity.groupId())
                || containsPlaceholder(identity.artifactId())
                || containsPlaceholder(identity.extension())
                || containsPlaceholder(identity.classifier())
                || containsPlaceholder(identity.version())) {
            throw invalidNode(visit, "contains an unresolved Maven property");
        }
        if (node.getVersionConstraint() != null
                && node.getVersionConstraint().getRange() != null) {
            throw invalidNode(visit, "uses a Maven version range");
        }
        String declaredVersion = node.getVersionConstraint() == null
                        || node.getVersionConstraint().getVersion() == null
                ? identity.version()
                : node.getVersionConstraint().getVersion().toString();
        if (isMutableVersion(identity.version())
                || isMutableVersion(declaredVersion)
                || node.getArtifact().isSnapshot()) {
            throw invalidNode(visit, "uses a mutable Maven version");
        }
        if (!selected) {
            return;
        }
        if (!"jar".equals(identity.extension())) {
            throw invalidNode(visit, "selects a non-JAR runtime artifact");
        }
        Dependency dependency = node.getDependency();
        if (dependency == null) {
            throw invalidNode(visit, "has no Maven dependency metadata");
        }
        if (dependency.isOptional()) {
            throw invalidNode(visit, "selects an optional dependency");
        }
        if (!RUNTIME_SCOPES.contains(dependency.getScope())) {
            throw invalidNode(
                    visit,
                    "selects unsupported scope '" + dependency.getScope() + "'");
        }
    }

    private static boolean containsPlaceholder(String value) {
        return value.contains("${");
    }

    private static boolean isMutableVersion(String version) {
        String upperVersion = version.toUpperCase(Locale.ROOT);
        return upperVersion.equals("LATEST")
                || upperVersion.equals("RELEASE")
                || upperVersion.contains("SNAPSHOT")
                || TIMESTAMPED_SNAPSHOT.matcher(version).matches();
    }

    private static MavenArtifactLookupException invalidNode(
            NodeVisit visit,
            String problem) {
        return invalidClosure(
                "Invalid Maven runtime closure at " + formatPath(visit.dependencyPath())
                        + ": " + problem);
    }

    private static MavenArtifactLookupException invalidClosure(String message) {
        return new MavenArtifactLookupException(
                MavenArtifactLookupException.Kind.INVALID_CLOSURE,
                message);
    }

    private static String formatPath(List<MavenArtifactIdentity> path) {
        return path.stream()
                .map(MavenArtifactIdentity::coordinate)
                .reduce((left, right) -> left + " -> " + right)
                .orElse("<empty>");
    }

    private static MavenConflictDecision conflictDecision(
            NodeVisit omitted,
            NodeVisit winner) throws MavenArtifactLookupException {
        if (winner.depth() > omitted.depth()) {
            throw invalidClosure(
                    "Maven Resolver selected a farther conflict winner for "
                            + omitted.identity().conflictKey());
        }
        MavenConflictDecision.Reason reason = winner.depth() < omitted.depth()
                ? MavenConflictDecision.Reason.NEAREST
                : MavenConflictDecision.Reason.FIRST_BREADTH_FIRST;
        return new MavenConflictDecision(
                omitted.identity(),
                winner.identity(),
                reason,
                omitted.depth(),
                winner.depth(),
                omitted.originRootIndex(),
                winner.originRootIndex(),
                omitted.dependencyPath(),
                winner.dependencyPath());
    }

    private static List<ResolvedMavenJar> materializeClasspath(
            GraphSelection selection,
            DependencyResult result) throws IOException, MavenArtifactLookupException {
        Map<MavenArtifactIdentity, Path> resolvedFiles = new HashMap<>();
        for (ArtifactResult artifactResult : result.getArtifactResults()) {
            Artifact artifact = artifactResult.getArtifact();
            if (artifact != null && artifact.getFile() != null) {
                resolvedFiles.putIfAbsent(
                        MavenArtifactIdentity.from(artifact),
                        artifact.getFile().toPath());
            }
        }

        List<ResolvedMavenJar> classpath = new ArrayList<>();
        for (NodeVisit visit : selection.selected()) {
            Artifact artifact = visit.node().getArtifact();
            Path source = artifact.getFile() == null
                    ? resolvedFiles.get(visit.identity())
                    : artifact.getFile().toPath();
            if (source == null || !Files.isRegularFile(source)) {
                throw invalidNode(visit, "did not resolve to a regular local JAR");
            }
            Path canonicalSource = source.toRealPath();
            classpath.add(new ResolvedMavenJar(
                    visit.identity(),
                    visit.depth(),
                    visit.originRootIndex(),
                    visit.node().getDependency().getScope(),
                    visit.dependencyPath(),
                    canonicalSource,
                    Digests.sha256(canonicalSource)));
        }
        return List.copyOf(classpath);
    }

    private Path checkedLocalRepository() {
        Path effectiveLocalRepository = localRepository == null
                ? defaultLocalRepository()
                : localRepository;
        if (Files.exists(effectiveLocalRepository)
                && !Files.isDirectory(effectiveLocalRepository)) {
            throw new IllegalStateException(
                    "Maven local repository is not a directory: "
                            + effectiveLocalRepository);
        }
        return effectiveLocalRepository;
    }

    private static MavenArtifactLookupException resolutionFailure(
            Throwable failure,
            boolean offline,
            String message) {
        FailureFacts facts = FailureFacts.inspect(failure);
        String failedArtifacts = facts.failedArtifacts().isEmpty()
                ? ""
                : "; failed artifact(s): " + String.join(", ", facts.failedArtifacts());
        return new MavenArtifactLookupException(
                facts.kind(offline),
                message + failedArtifacts,
                failure);
    }

    private static RepositorySystem newRepositorySystem() {
        return new RepositorySystemSupplier().get();
    }

    private static Path defaultLocalRepository() {
        String configured = System.getProperty("maven.repo.local");
        if (configured != null && !configured.isBlank()) {
            return Path.of(configured);
        }
        String userHome = System.getProperty("user.home");
        if (userHome == null || userHome.isBlank()) {
            throw new IllegalStateException(
                    "Neither maven.repo.local nor user.home identifies a Maven local repository");
        }
        return Path.of(userHome, ".m2", "repository");
    }

    private enum RuntimeDependencySelector implements DependencySelector {
        INSTANCE;

        @Override
        public boolean selectDependency(Dependency dependency) {
            return !dependency.isOptional()
                    && RUNTIME_SCOPES.contains(dependency.getScope());
        }

        @Override
        public DependencySelector deriveChildSelector(
                DependencyCollectionContext context) {
            return this;
        }
    }

    /** Nearest wins; equal depth uses the first node in explicit breadth-first order. */
    private static final class FirstBreadthFirstNearestVersionSelector
            extends ConflictResolver.VersionSelector {
        private final Map<DependencyNode, Integer> breadthFirstRanks;

        private FirstBreadthFirstNearestVersionSelector() {
            this(Map.of());
        }

        private FirstBreadthFirstNearestVersionSelector(
                Map<DependencyNode, Integer> breadthFirstRanks) {
            this.breadthFirstRanks = breadthFirstRanks;
        }

        @Override
        public ConflictResolver.VersionSelector getInstance(
                DependencyNode root,
                DependencyGraphTransformationContext context) {
            Map<DependencyNode, Integer> ranks = new IdentityHashMap<>();
            Set<DependencyNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
            Deque<DependencyNode> queue = new ArrayDeque<>();
            queue.add(root);
            int rank = 0;
            while (!queue.isEmpty()) {
                DependencyNode node = queue.removeFirst();
                if (!visited.add(node)) {
                    continue;
                }
                ranks.put(node, rank++);
                queue.addAll(node.getChildren());
            }
            return new FirstBreadthFirstNearestVersionSelector(ranks);
        }

        @Override
        public void selectVersion(ConflictResolver.ConflictContext context)
                throws RepositoryException {
            ConflictResolver.ConflictItem winner = null;
            for (ConflictResolver.ConflictItem candidate : context.getItems()) {
                if (winner == null
                        || candidate.getDepth() < winner.getDepth()
                        || (candidate.getDepth() == winner.getDepth()
                                && rank(candidate) < rank(winner))) {
                    winner = candidate;
                }
            }
            if (winner == null) {
                throw new RepositoryException("Maven conflict set is empty");
            }
            context.setWinner(winner);
        }

        private int rank(ConflictResolver.ConflictItem item) {
            return breadthFirstRanks.getOrDefault(item.getNode(), Integer.MAX_VALUE);
        }
    }

    private static final class PomEvidenceListener extends AbstractRepositoryListener {
        private final Map<MavenArtifactIdentity, Path> pomPaths = new ConcurrentHashMap<>();

        @Override
        public void artifactResolved(RepositoryEvent event) {
            Artifact artifact = event.getArtifact();
            if (event.getException() != null
                    || artifact == null
                    || !"pom".equals(artifact.getExtension())) {
                return;
            }
            java.io.File file = event.getFile() == null
                    ? artifact.getFile()
                    : event.getFile();
            if (file != null) {
                pomPaths.putIfAbsent(
                        MavenArtifactIdentity.from(artifact),
                        file.toPath().toAbsolutePath().normalize());
            }
        }

        private List<MavenPomEvidence> evidence() throws IOException {
            List<Map.Entry<MavenArtifactIdentity, Path>> entries = new ArrayList<>(
                    pomPaths.entrySet());
            entries.sort(Map.Entry.comparingByKey());
            List<MavenPomEvidence> evidence = new ArrayList<>();
            for (Map.Entry<MavenArtifactIdentity, Path> entry : entries) {
                Path source = entry.getValue().toRealPath();
                evidence.add(new MavenPomEvidence(
                        entry.getKey(), source, Digests.sha256(source)));
            }
            return List.copyOf(evidence);
        }
    }

    private static final class FailureFacts {
        private boolean notFound;
        private boolean repositoryOffline;
        private boolean transferFailure;
        private boolean invalidDescriptor;
        private final Set<String> failedArtifacts = new TreeSet<>();

        private static FailureFacts inspect(Throwable failure) {
            FailureFacts facts = new FailureFacts();
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            Deque<Throwable> queue = new ArrayDeque<>();
            queue.add(failure);
            while (!queue.isEmpty()) {
                Throwable current = queue.removeFirst();
                if (current == null || !seen.add(current)) {
                    continue;
                }
                if (current.getCause() != null) {
                    queue.addLast(current.getCause());
                }
                Collections.addAll(queue, current.getSuppressed());

                if (current instanceof ArtifactNotFoundException notFoundFailure) {
                    facts.notFound = true;
                    facts.addArtifact(notFoundFailure.getArtifact());
                } else if (current instanceof ArtifactTransferException transfer) {
                    facts.transferFailure = true;
                    facts.addArtifact(transfer.getArtifact());
                }
                if (current instanceof RepositoryOfflineException) {
                    facts.repositoryOffline = true;
                }
                if (current instanceof ArtifactResolutionException resolution) {
                    for (ArtifactResult result : resolution.getResults()) {
                        facts.addArtifact(result.getRequest().getArtifact());
                        queue.addAll(result.getExceptions());
                    }
                }
                if (current instanceof DependencyResolutionException resolution) {
                    DependencyResult result = resolution.getResult();
                    queue.addAll(result.getCollectExceptions());
                    for (ArtifactResult artifactResult : result.getArtifactResults()) {
                        facts.addArtifact(artifactResult.getRequest().getArtifact());
                        queue.addAll(artifactResult.getExceptions());
                    }
                }
                if (current instanceof DependencyCollectionException collection) {
                    queue.addAll(collection.getResult().getExceptions());
                }
                if (current instanceof ArtifactDescriptorException descriptor) {
                    facts.invalidDescriptor = true;
                    facts.addArtifact(descriptor.getResult().getRequest().getArtifact());
                    queue.addAll(descriptor.getResult().getExceptions());
                }
            }
            return facts;
        }

        private void addArtifact(Artifact artifact) {
            if (artifact == null) {
                return;
            }
            try {
                failedArtifacts.add(MavenArtifactIdentity.from(artifact).coordinate());
            } catch (IllegalArgumentException | NullPointerException ignored) {
                // The top-level failure still retains the original incomplete identity.
            }
        }

        private MavenArtifactLookupException.Kind kind(boolean offline) {
            if (offline && !transferFailure && (notFound || repositoryOffline)) {
                return MavenArtifactLookupException.Kind.OFFLINE_MISS;
            }
            if (!offline && !transferFailure && notFound) {
                return MavenArtifactLookupException.Kind.NOT_FOUND;
            }
            if (!transferFailure && invalidDescriptor) {
                return MavenArtifactLookupException.Kind.INVALID_CLOSURE;
            }
            return MavenArtifactLookupException.Kind.REPOSITORY_UNAVAILABLE;
        }

        private Set<String> failedArtifacts() {
            return Collections.unmodifiableSet(failedArtifacts);
        }
    }

    private record NodeVisit(
            DependencyNode node,
            MavenArtifactIdentity identity,
            int depth,
            int originRootIndex,
            List<MavenArtifactIdentity> dependencyPath) {
        private NodeVisit {
            Objects.requireNonNull(node, "node");
            Objects.requireNonNull(identity, "identity");
            dependencyPath = List.copyOf(dependencyPath);
        }
    }

    private record IndexedGraph(
            IdentityHashMap<DependencyNode, NodeVisit> visits,
            List<NodeVisit> breadthFirstVisits) {
        private IndexedGraph {
            Objects.requireNonNull(visits, "visits");
            breadthFirstVisits = List.copyOf(breadthFirstVisits);
        }
    }

    private record GraphSelection(
            List<NodeVisit> selected,
            Set<DependencyNode> selectedNodeSet,
            List<MavenConflictDecision> conflicts) {
        private GraphSelection {
            selected = List.copyOf(selected);
            Set<DependencyNode> selectedCopy = Collections.newSetFromMap(
                    new IdentityHashMap<>());
            selectedCopy.addAll(selectedNodeSet);
            selectedNodeSet = Collections.unmodifiableSet(selectedCopy);
            conflicts = List.copyOf(conflicts);
        }
    }
}
