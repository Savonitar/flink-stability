package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkRuntimeTargetTest {
    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);

    @Test
    void haAndTokenConfigurationSurviveEveryTargetWither() {
        var ha = new FlinkRuntimeTarget.HighAvailability("zookeeper:3.9.3", Duration.ofSeconds(6));
        var tokens = new FlinkRuntimeTarget.TokenProvider(
                Duration.ofSeconds(2), Optional.of(Duration.ofSeconds(3)),
                Optional.of(FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB));
        var jar = new FlinkRuntimeTarget.RuntimeJar("/opt/flink/lib/flink-dist-2.4-SNAPSHOT.jar", "d".repeat(64));
        FlinkRuntimeTarget configured = target().withHighAvailability(ha).withTokenProvider(tokens)
                .withTaskManagers(2).withExpectedImageId(IMAGE_ID).withExpectedRuntimeJar(jar);
        assertEquals(1, target().jobManagers());
        assertEquals(2, configured.jobManagers());
        assertEquals(ha, configured.highAvailability().orElseThrow());
        assertEquals(tokens, configured.tokenProvider().orElseThrow());
        assertEquals(configured, target().withExpectedRuntimeJar(jar).withExpectedImageId(IMAGE_ID)
                .withTaskManagers(2).withTokenProvider(tokens).withHighAvailability(ha));
        assertThrows(IllegalArgumentException.class,
                () -> new FlinkRuntimeTarget.TokenProvider(Duration.ofMillis(49)));
        assertThrows(IllegalArgumentException.class,
                () -> new FlinkRuntimeTarget.TokenProvider(Duration.ofMinutes(31)));
    }

    @Test
    void tokenRetryBackoffIsOptionalAndExplicitValuesHaveBoundedDuration() {
        assertTrue(new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2)).retryBackoff().isEmpty());
        for (Duration backoff : List.of(Duration.ofSeconds(1), Duration.ofMinutes(5))) {
            assertEquals(Optional.of(backoff), new FlinkRuntimeTarget.TokenProvider(
                    Duration.ofSeconds(2), Optional.of(backoff)).retryBackoff());
        }
        for (Duration backoff : List.of(Duration.ofMillis(999), Duration.ofMinutes(5).plusMillis(1))) {
            assertThrows(IllegalArgumentException.class, () -> new FlinkRuntimeTarget.TokenProvider(
                    Duration.ofSeconds(2), Optional.of(backoff)));
        }
        assertThrows(NullPointerException.class,
                () -> new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2), null));
    }

    @Test
    void explicitTokenProofScopeChangesTargetIdentityWhileOmissionKeepsLegacyConstructorsEqual() {
        var legacy = new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2));
        assertTrue(legacy.proofScope().isEmpty());
        assertEquals(legacy, new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2), Optional.empty()));
        assertEquals(legacy, new FlinkRuntimeTarget.TokenProvider(
                Duration.ofSeconds(2), Optional.empty(), Optional.empty()));
        var bootstrap = new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2), Optional.empty(),
                Optional.of(FlinkRuntimeTarget.TokenProofScope.BOOTSTRAP));
        var submitted = new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2), Optional.empty(),
                Optional.of(FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB));
        assertNotEquals(target().withTokenProvider(legacy), target().withTokenProvider(bootstrap));
        assertNotEquals(target().withTokenProvider(bootstrap), target().withTokenProvider(submitted));
        assertThrows(NullPointerException.class, () -> new FlinkRuntimeTarget.TokenProvider(
                Duration.ofSeconds(2), Optional.empty(), null));
    }

    @Test
    void taskManagerCountIsImmutableAndSurvivesBothRuntimePins() {
        FlinkRuntimeTarget.RuntimeJar jar = new FlinkRuntimeTarget.RuntimeJar(
                "/opt/flink/lib/flink-dist-2.2.0.jar", "d".repeat(64));
        FlinkRuntimeTarget original = target();
        FlinkRuntimeTarget expanded = original.withTaskManagers(3)
                .withExpectedImageId(IMAGE_ID).withExpectedRuntimeJar(jar);

        assertEquals(1, original.taskManagers());
        assertEquals(3, expanded.taskManagers());
        assertEquals(2, FlinkRuntimeTarget.TASK_SLOTS_PER_TASK_MANAGER);
        assertSame(original.connectorBundle(), expanded.connectorBundle());
        FlinkRuntimeTarget reordered = original.withExpectedRuntimeJar(jar)
                .withExpectedImageId(IMAGE_ID).withTaskManagers(3);
        assertEquals(expanded, reordered);
        assertEquals(expanded.hashCode(), reordered.hashCode());
        assertNotEquals(expanded, expanded.withTaskManagers(2));
        assertThrows(IllegalArgumentException.class, () -> original.withTaskManagers(0));
        assertThrows(IllegalArgumentException.class, () -> original.withTaskManagers(-1));
        assertThrows(IllegalArgumentException.class, () -> original.withTaskManagers(17));
        assertThrows(IllegalArgumentException.class,
                () -> original.withTaskManagers(Integer.MAX_VALUE));
    }

    @Test
    void runtimeJarPinIsImmutableAndSurvivesImagePinningInEitherOrder() {
        FlinkRuntimeTarget.RuntimeJar jar = new FlinkRuntimeTarget.RuntimeJar(
                "/opt/flink/lib/flink-dist-2.2.1-SNAPSHOT.jar", "d".repeat(64));
        FlinkRuntimeTarget original = target();
        FlinkRuntimeTarget pinned = original.withExpectedRuntimeJar(jar).withExpectedImageId(IMAGE_ID);

        assertTrue(original.expectedRuntimeJar().isEmpty());
        assertEquals(jar, pinned.expectedRuntimeJar().orElseThrow());
        assertEquals(IMAGE_ID, pinned.expectedImageId().orElseThrow());
        assertSame(original.connectorBundle(), pinned.connectorBundle());
        assertEquals(pinned, original.withExpectedImageId(IMAGE_ID).withExpectedRuntimeJar(jar));
        assertEquals(pinned.hashCode(),
                original.withExpectedImageId(IMAGE_ID).withExpectedRuntimeJar(jar).hashCode());
        assertNotEquals(original.withExpectedImageId(IMAGE_ID), pinned);
        assertThrows(NullPointerException.class, () -> original.withExpectedRuntimeJar(null));
    }

    @Test
    void runtimeJarAcceptsPlainAndScalaSuffixedDistributionNamesWithTheSamePinContract() {
        for (String filename : List.of("flink-dist-2.2.0.jar", "flink-dist-vendor_2.4+build-1.jar",
                "flink-dist_2.12-1.20.2.jar", "flink-dist_2.12-2.3-vendor.jar",
                "flink-dist_2.13-2.4-SNAPSHOT+build_1.jar")) {
            String path = "/opt/flink/lib/" + filename;
            var jar = new FlinkRuntimeTarget.RuntimeJar(path, "a".repeat(64));
            assertEquals(path, jar.containerPath());
            assertEquals("a".repeat(64), jar.sha256());
        }
    }

    @Test void explicitLogMarkersSurviveTargetConfigurationAndAreImmutable() {
        var marker = new FlinkLogMarker("patched", "CUSTOM-FIX", "taskmanager", true);
        var original = target();
        var configured = original.withLogMarkers(List.of(marker)).withConfig(java.util.Map.of("pipeline.name", "subject"))
                .withExpectedImageId(IMAGE_ID).withTaskManagers(2);
        assertTrue(original.logMarkers().isEmpty());
        assertEquals(List.of(marker), configured.logMarkers());
        assertThrows(UnsupportedOperationException.class, () -> configured.logMarkers().add(marker));
        assertThrows(IllegalArgumentException.class, () -> original.withLogMarkers(List.of(marker, marker)));
    }

    @Test
    void runtimeJarRejectsTraversalOtherFilesAndMalformedChecksums() {
        for (String path : List.of("/opt/flink/lib/flink-dist-../escape.jar",
                "/opt/flink/lib/../flink-dist-2.2.1.jar", "/opt/flink/lib/flink-runtime-2.2.1.jar",
                "/opt/flink/lib/nested/flink-dist-2.2.1.jar", "/tmp/flink-dist-2.2.1.jar",
                "file:/opt/flink/lib/flink-dist-2.2.1.jar", "/opt/flink/lib/flink-dist-.jar",
                "/opt/flink/lib/flink-dist-2.2.1.jar;echo", "/opt/flink/lib/flink-dist-2.2.1 jar",
                "/opt/flink/lib/flink-dist_2.12-.jar", "/opt/flink/lib/flink-dist_-2.3.jar",
                "/opt/flink/lib/flink-dist_2.12.jar", "/opt/flink/lib/flink-dist_2.12-../escape.jar",
                "/opt/flink/lib/../flink-dist_2.12-2.3.jar", "/opt/flink/lib/nested/flink-dist_2.12-2.3.jar",
                "/tmp/flink-dist_2.12-2.3.jar", "/opt/flink/lib/flink-dist_2.12-.hidden.jar",
                "/opt/flink/lib/flink-dist_2.12-2.3.jar;echo", "/opt/flink/lib/flink-dist_2.12-2.3 jar",
                "/opt/flink/lib/flink-dist_2.12-2.3\n.jar", "/opt/flink/lib/flink-dist_2.12-$VERSION.jar")) {
            assertThrows(IllegalArgumentException.class,
                    () -> new FlinkRuntimeTarget.RuntimeJar(path, "a".repeat(64)), path);
        }
        for (String path : List.of("/opt/flink/lib/flink-dist-2.2.1.jar", "/opt/flink/lib/flink-dist_2.12-2.3.jar")) {
            for (String checksum : List.of("", "A".repeat(64), "a".repeat(63), "a".repeat(65),
                    "sha256:" + "a".repeat(64))) {
                assertThrows(IllegalArgumentException.class, () -> new FlinkRuntimeTarget.RuntimeJar(path, checksum), checksum);
            }
            assertThrows(NullPointerException.class, () -> new FlinkRuntimeTarget.RuntimeJar(path, null));
        }
    }

    @Test
    void runtimeJarEvidenceKeepsPhysicalContainerAndItsExplicitClassLoadProcess() {
        FlinkComponentProvisioningEvidence original = evidence(IMAGE_ID);
        FlinkRuntimeTarget.RuntimeJar jar = new FlinkRuntimeTarget.RuntimeJar(
                "/opt/flink/lib/flink-dist-2.2.1.jar", "d".repeat(64));
        FlinkComponentProvisioningEvidence copied = original.withRuntimeJarEvidence(jar, "jobmanager-1#3");

        assertTrue(original.runtimeJarEvidence().isEmpty());
        assertEquals(original.runtimeId(), copied.runtimeId());
        assertEquals(original.imageId(), copied.imageId());
        assertEquals(original.connectorArtifacts(), copied.connectorArtifacts());
        assertEquals(jar, copied.runtimeJarEvidence().orElseThrow().jar());
        assertEquals("jobmanager-1#3", copied.runtimeJarEvidence().orElseThrow().classLoadProcess());
        assertThrows(IllegalArgumentException.class, () -> original.withRuntimeJarEvidence(jar, " "));
        assertThrows(NullPointerException.class, () -> original.withRuntimeJarEvidence(null, "process"));
    }

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
