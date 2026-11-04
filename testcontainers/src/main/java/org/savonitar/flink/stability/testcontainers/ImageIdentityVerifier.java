package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import org.savonitar.flink.stability.runtime.api.ImageIdentityEvidence;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.savonitar.flink.stability.runtime.api.Checks.requireDockerImageId;

/** Reads both Docker identity forms from the image used by the stopped physical container. */
final class ImageIdentityVerifier {
    private ImageIdentityVerifier() {}

    record Observation(String containerImageId, String inspectedImageId, List<String> repoDigests,
                       String storageDriver, Optional<String> driverType) {
        Observation {
            if (repoDigests != null) repoDigests = List.copyOf(repoDigests);
        }
    }

    static Observation capture(DockerClient docker, String containerId, ContainerOperationDeadline deadline) {
        var container = call(deadline, "inspect stopped container image", () ->
                docker.inspectContainerCmd(containerId).exec());
        String containerImageId = requireDockerImageId(container.getImageId(), "container image ID");
        // Use the immutable ID observed on this container, never a tag that can move between reads.
        var image = call(deadline, "inspect stopped container image RepoDigests", () ->
                docker.inspectImageCmd(containerImageId).exec());
        var info = call(deadline, "inspect Docker image store backend", () -> docker.infoCmd().exec());
        Optional<String> driverType = Optional.empty();
        if (info.getDriverStatuses() != null) {
            if (info.getDriverStatuses().size() > 256) throw new IllegalStateException("Docker driver status exceeds limit");
            for (var pair : info.getDriverStatuses()) {
                if (pair != null && !pair.isEmpty() && "driver-type".equals(pair.getFirst())) {
                    if (pair.size() != 2 || driverType.isPresent())
                        throw new IllegalStateException("Docker driver-type evidence is ambiguous");
                    driverType = Optional.of(Objects.requireNonNull(pair.get(1), "driver-type"));
                }
            }
        }
        return new Observation(containerImageId, image.getId(), image.getRepoDigests(), info.getDriver(), driverType);
    }

    static ImageIdentityEvidence verify(Optional<String> expectedImageId, Optional<String> expectedImageDigest,
                                        Observation actual) {
        Objects.requireNonNull(actual, "image identity observation");
        try {
            requireDockerImageId(actual.containerImageId(), "container image ID");
            requireDockerImageId(actual.inspectedImageId(), "image inspect ID");
            if (actual.repoDigests() == null || actual.repoDigests().size() > 128
                    || actual.repoDigests().stream().distinct().count() != actual.repoDigests().size())
                throw new IllegalArgumentException("RepoDigests were not captured or exceed the limit");
            for (String value : actual.repoDigests()) {
                int separator = value.lastIndexOf('@');
                if (separator < 1 || value.length() > 2048 || !value.matches("[A-Za-z0-9._:/-]+@sha256:[0-9a-f]{64}"))
                    throw new IllegalArgumentException("Malformed Docker RepoDigest");
                requireDockerImageId(value.substring(separator + 1), "RepoDigest digest");
            }
            if (!actual.containerImageId().equals(actual.inspectedImageId())
                    && !containsDigest(actual.repoDigests(), actual.inspectedImageId()))
                throw new IllegalArgumentException("Image inspect ID is neither container identity nor an observed RepoDigest");
            if (actual.storageDriver() == null || actual.storageDriver().isBlank() || actual.storageDriver().length() > 128
                    || actual.driverType() == null || actual.driverType().stream().anyMatch(value -> value.isBlank() || value.length() > 256))
                throw new IllegalArgumentException("Docker image store backend was not captured");
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IllegalStateException("Invalid Docker image identity: actual " + actual.containerImageId(), failure);
        }
        ImageIdentityEvidence.MatchForm match = ImageIdentityEvidence.MatchForm.UNPINNED;
        if (expectedImageId.isPresent()) {
            String expected = expectedImageId.orElseThrow();
            if (expected.equals(actual.containerImageId())) match = ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID;
            else if (containsDigest(actual.repoDigests(), expected)) match = ImageIdentityEvidence.MatchForm.REPO_DIGEST;
            else throw new IllegalStateException("Docker image ID mismatch: expected " + expected
                        + ", actual " + actual.containerImageId() + ", RepoDigests " + actual.repoDigests());
        }
        if (expectedImageDigest.isPresent() && !containsDigest(actual.repoDigests(), expectedImageDigest.orElseThrow()))
            throw new IllegalStateException("Docker image digest mismatch: expected " + expectedImageDigest.orElseThrow()
                    + ", RepoDigests " + actual.repoDigests());
        return new ImageIdentityEvidence(actual.containerImageId(), actual.inspectedImageId(), actual.repoDigests(),
                expectedImageId, expectedImageDigest, match, actual.storageDriver(), actual.driverType());
    }

    private static boolean containsDigest(List<String> repoDigests, String digest) {
        return repoDigests.stream().anyMatch(value -> value.substring(value.lastIndexOf('@') + 1).equals(digest));
    }

    private static <T> T call(ContainerOperationDeadline deadline, String description, Callable<T> action) {
        checkActive(deadline, description);
        T value = ContainerDriverCallBoundary.call(deadline, description, action);
        checkActive(deadline, description);
        return value;
    }

    private static void checkActive(ContainerOperationDeadline deadline, String description) {
        if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Image verification was interrupted");
        deadline.remaining(description);
    }
}
