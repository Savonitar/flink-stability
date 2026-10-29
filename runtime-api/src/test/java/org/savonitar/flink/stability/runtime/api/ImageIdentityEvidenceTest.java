package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;

class ImageIdentityEvidenceTest {
    private static final String CONFIG = "sha256:" + "a".repeat(64);
    private static final String INDEX = "sha256:" + "b".repeat(64);
    private static final List<String> DIGESTS = List.of("flink@" + INDEX);

    @Test void recordsClassicAndContainerdIdentitiesWithoutACompiledImagePin() {
        var classic = new ImageIdentityEvidence(CONFIG, CONFIG, DIGESTS, Optional.of(CONFIG), Optional.of(INDEX),
                ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, "overlay2", Optional.empty());
        var containerd = new ImageIdentityEvidence(INDEX, INDEX, DIGESTS, Optional.of(INDEX), Optional.of(INDEX),
                ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, "overlayfs", Optional.of("io.containerd.snapshotter.v1"));
        var alias = new ImageIdentityEvidence(CONFIG, INDEX, DIGESTS, Optional.of(INDEX), Optional.of(INDEX),
                ImageIdentityEvidence.MatchForm.REPO_DIGEST, "overlayfs", Optional.of("io.containerd.snapshotter.v1"));
        assertTrue(classic.matchesImageId(CONFIG)); assertTrue(classic.matchesImageId(INDEX));
        assertTrue(containerd.matchesImageDigest(INDEX));
        assertEquals(ImageIdentityEvidence.MatchForm.REPO_DIGEST, alias.imageIdMatchForm());
        assertThrows(UnsupportedOperationException.class, () -> alias.repoDigests().clear());
    }

    @Test void mismatchedPinsAndUnboundInspectionsCannotBecomeEvidence() {
        String other = "sha256:" + "c".repeat(64);
        assertThrows(IllegalArgumentException.class, () -> new ImageIdentityEvidence(CONFIG, CONFIG, DIGESTS,
                Optional.of(other), Optional.empty(), ImageIdentityEvidence.MatchForm.REPO_DIGEST, "overlay2", Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new ImageIdentityEvidence(CONFIG, CONFIG, DIGESTS,
                Optional.of(CONFIG), Optional.of(other), ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, "overlay2", Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new ImageIdentityEvidence(CONFIG, other, DIGESTS,
                Optional.of(CONFIG), Optional.empty(), ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, "overlay2", Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new ImageIdentityEvidence(CONFIG, CONFIG, List.of("user:secret@host@" + INDEX),
                Optional.of(CONFIG), Optional.empty(), ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, "overlay2", Optional.empty()));
    }

    @Test void missingCaptureAndContradictoryMatchFormAreRejected() {
        assertThrows(NullPointerException.class, () -> new ImageIdentityEvidence(CONFIG, CONFIG, null,
                Optional.of(CONFIG), Optional.empty(), ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, "overlay2", Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new ImageIdentityEvidence(CONFIG, CONFIG, DIGESTS,
                Optional.of(INDEX), Optional.empty(), ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, "overlay2", Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new ImageIdentityEvidence(CONFIG, CONFIG, DIGESTS,
                Optional.empty(), Optional.empty(), ImageIdentityEvidence.MatchForm.REPO_DIGEST, "overlay2", Optional.empty()));
    }
}
