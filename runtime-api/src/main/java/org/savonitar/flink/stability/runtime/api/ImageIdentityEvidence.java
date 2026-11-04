package org.savonitar.flink.stability.runtime.api;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Both Docker identity forms observed from the image used by one container. */
public record ImageIdentityEvidence(String containerImageId, String inspectedImageId,
        List<String> repoDigests, Optional<String> declaredImageId, Optional<String> declaredImageDigest,
        MatchForm imageIdMatchForm, String storageDriver, Optional<String> driverType) {
    public enum MatchForm { CONTAINER_IMAGE_ID, REPO_DIGEST, UNPINNED }

    public ImageIdentityEvidence {
        containerImageId = Checks.requireDockerImageId(containerImageId, "containerImageId");
        inspectedImageId = Checks.requireDockerImageId(inspectedImageId, "inspectedImageId");
        repoDigests = List.copyOf(Objects.requireNonNull(repoDigests, "repoDigests"));
        if (repoDigests.size() > 128 || repoDigests.stream().distinct().count() != repoDigests.size())
            throw new IllegalArgumentException("Invalid repository digest inventory");
        for (String digest : repoDigests)
            if (digest.length() > 2048 || !digest.matches("[A-Za-z0-9._:/-]+@sha256:[0-9a-f]{64}"))
                throw new IllegalArgumentException("Invalid repository digest");
        if (!containerImageId.equals(inspectedImageId) && !containsDigest(repoDigests, inspectedImageId))
            throw new IllegalArgumentException("Inspected image identity is not bound to the recorded image forms");
        declaredImageId = Objects.requireNonNull(declaredImageId, "declaredImageId");
        declaredImageDigest = Objects.requireNonNull(declaredImageDigest, "declaredImageDigest");
        declaredImageId.ifPresent(value -> Checks.requireDockerImageId(value, "declaredImageId"));
        declaredImageDigest.ifPresent(value -> Checks.requireDockerImageId(value, "declaredImageDigest"));
        Objects.requireNonNull(imageIdMatchForm, "imageIdMatchForm");
        storageDriver = Checks.requireNonBlank(storageDriver, "storageDriver");
        driverType = Objects.requireNonNull(driverType, "driverType");
        driverType.ifPresent(value -> Checks.requireNonBlank(value, "driverType"));
        MatchForm expected = declaredImageId.isEmpty() ? MatchForm.UNPINNED
                : declaredImageId.get().equals(containerImageId) ? MatchForm.CONTAINER_IMAGE_ID
                : containsDigest(repoDigests, declaredImageId.get()) ? MatchForm.REPO_DIGEST : null;
        if (expected != imageIdMatchForm)
            throw new IllegalArgumentException("Declared image identity does not match observed identity forms");
        if (declaredImageDigest.isPresent() && !containsDigest(repoDigests, declaredImageDigest.get()))
            throw new IllegalArgumentException("Declared image digest is absent from observed repository digests");
    }

    public boolean matchesImageId(String pin) {
        return containerImageId.equals(pin) || containsDigest(repoDigests, pin);
    }

    public boolean matchesImageDigest(String pin) { return containsDigest(repoDigests, pin); }

    public static boolean containsDigest(List<String> digests, String pin) {
        return digests.stream().anyMatch(value -> value.endsWith("@" + pin));
    }
}
