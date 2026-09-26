package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkRuntimeTargetTest {
    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);

    @Test
    void expectedImageIdentityIsImmutableAndParticipatesInTargetEquality() {
        FlinkRuntimeTarget original = target();
        FlinkRuntimeTarget pinned = original.withExpectedImageId(IMAGE_ID);

        assertTrue(original.expectedImageId().isEmpty());
        assertEquals(IMAGE_ID, pinned.expectedImageId().orElseThrow());
        assertSame(original.connectorBundle(), pinned.connectorBundle());
        assertEquals(original.imageReference(), pinned.imageReference());
        assertNotEquals(original, pinned);
        assertNotEquals(pinned, original.withExpectedImageId("sha256:" + "b".repeat(64)));
        assertEquals(pinned, original.withExpectedImageId(IMAGE_ID));
        assertEquals(pinned.hashCode(), original.withExpectedImageId(IMAGE_ID).hashCode());
    }

    @Test
    void rejectsMissingOrMalformedExpectedDockerImageIdentity() {
        assertThrows(NullPointerException.class, () -> target().withExpectedImageId(null));
        for (String invalid : List.of("", "a".repeat(64), "sha256:" + "A".repeat(64),
                "sha256:" + "a".repeat(63), "sha256:" + "g".repeat(64),
                "flink@sha256:" + "a".repeat(64), IMAGE_ID + " ")) {
            assertThrows(IllegalArgumentException.class,
                    () -> target().withExpectedImageId(invalid), invalid);
        }
    }

    @Test
    void provisioningEvidenceRequiresAnObservedDockerImageIdentity() {
        FlinkComponentProvisioningEvidence evidence = evidence(IMAGE_ID);

        assertEquals(IMAGE_ID, evidence.imageId());
        assertEquals("local/flink:2.2.1-pr", evidence.imageReference());
        assertThrows(NullPointerException.class, () -> evidence(null));
        for (String invalid : List.of("", "flink:2.2.1", "a".repeat(64),
                "sha256:" + "A".repeat(64), "sha256:" + "a".repeat(63))) {
            assertThrows(IllegalArgumentException.class, () -> evidence(invalid), invalid);
        }
    }

    private static FlinkRuntimeTarget target() {
        String image = "local/flink:2.2.1-pr";
        return FlinkRuntimeTarget.withConnectorBundle(image,
                new FlinkConnectorBundleInstallation(
                        image, List.of(), new ConnectorClasspathManifest(List.of())));
    }

    private static FlinkComponentProvisioningEvidence evidence(String imageId) {
        return FlinkComponentProvisioningEvidence.verified(
                "jobmanager-1", FlinkComponentRole.JOB_MANAGER, "container-1",
                "local/flink:2.2.1-pr", imageId, "b".repeat(64), "c".repeat(64), List.of());
    }
}
