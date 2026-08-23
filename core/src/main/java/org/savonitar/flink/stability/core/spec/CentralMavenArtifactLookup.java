package org.savonitar.flink.stability.core.spec;

import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.supplier.RepositorySystemSupplier;
import org.eclipse.aether.transfer.ArtifactNotFoundException;
import org.eclipse.aether.transfer.RepositoryOfflineException;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/** Resolves primary JARs from an isolated local cache and fixed repositories. */
final class CentralMavenArtifactLookup implements MavenArtifactLookup {
    private static final URI MAVEN_CENTRAL = URI.create("https://repo.maven.apache.org/maven2/");

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

    private static List<RemoteRepository> repositories(List<URI> repositories) {
        return Objects.requireNonNull(repositories, "repositories").stream()
                .map(Objects::requireNonNull)
                .map(uri -> new RemoteRepository.Builder(
                        MAVEN_CENTRAL.equals(uri)
                                ? "central"
                                : "v1-repository-" + Integer.toUnsignedString(uri.hashCode()),
                        "default",
                        uri.toString()).build())
                .toList();
    }

    @Override
    public Path resolve(MavenCoordinate coordinate, boolean offline)
            throws MavenArtifactLookupException {
        Objects.requireNonNull(coordinate, "coordinate");
        try {
            Path effectiveLocalRepository = localRepository == null
                    ? defaultLocalRepository()
                    : localRepository;
            if (Files.exists(effectiveLocalRepository)
                    && !Files.isDirectory(effectiveLocalRepository)) {
                throw new IllegalStateException(
                        "Maven local repository is not a directory: "
                                + effectiveLocalRepository);
            }
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
            throw new MavenArtifactLookupException(
                    failureKind(exception, offline),
                    "Could not resolve " + coordinate.declaredReference()
                            + (offline
                                    ? " from the local Maven repository in offline mode"
                                    : " from the local Maven repository or Maven Central"),
                    exception);
        }
    }

    private static MavenArtifactLookupException.Kind failureKind(
            Throwable failure, boolean offline) {
        boolean notFound = false;
        boolean repositoryOffline = false;
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof ArtifactNotFoundException) {
                notFound = true;
            }
            if (current instanceof RepositoryOfflineException) {
                repositoryOffline = true;
            }
            if (current instanceof ArtifactResolutionException resolution) {
                for (ArtifactResult result : resolution.getResults()) {
                    for (Exception resultFailure : result.getExceptions()) {
                        notFound |= causedBy(resultFailure, ArtifactNotFoundException.class);
                        repositoryOffline |= causedBy(
                                resultFailure, RepositoryOfflineException.class);
                    }
                }
            }
        }
        if (offline && (notFound || repositoryOffline)) {
            return MavenArtifactLookupException.Kind.OFFLINE_MISS;
        }
        if (!offline && notFound) {
            return MavenArtifactLookupException.Kind.NOT_FOUND;
        }
        return MavenArtifactLookupException.Kind.REPOSITORY_UNAVAILABLE;
    }

    private static boolean causedBy(
            Throwable failure,
            Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
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
}
