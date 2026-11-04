package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InfoCmd;
import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectImageCmd;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.model.Info;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.ConnectorClasspathManifest;
import org.savonitar.flink.stability.runtime.api.FlinkConnectorBundleInstallation;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.ImageIdentityEvidence;
import org.testcontainers.containers.Network;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class ImageIdentityVerifierTest {
    private static final String CONFIG = "sha256:" + "a".repeat(64);
    private static final String INDEX = "sha256:" + "b".repeat(64);
    private static final String OTHER = "sha256:" + "c".repeat(64);
    private static final String REPO_DIGEST = "flink@" + INDEX;
    @TempDir Path temporary;

    @Test void classicConfigPinAndContainerdIndexPinKeepBothIdentityForms() {
        var classic = observation(CONFIG, "overlay2", Optional.empty());
        var config = ImageIdentityVerifier.verify(Optional.of(CONFIG), Optional.empty(), classic);
        assertEquals(ImageIdentityEvidence.MatchForm.CONTAINER_IMAGE_ID, config.imageIdMatchForm());
        assertEquals(CONFIG, config.containerImageId()); assertEquals(CONFIG, config.inspectedImageId());
        assertEquals(List.of(REPO_DIGEST), config.repoDigests());
        assertEquals("overlay2", config.storageDriver()); assertTrue(config.driverType().isEmpty());

        var containerd = observation(INDEX, "overlayfs", Optional.of("io.containerd.snapshotter.v1"));
        var index = ImageIdentityVerifier.verify(Optional.of(INDEX), Optional.of(INDEX), containerd);
        assertEquals(ImageIdentityEvidence.MatchForm.REPO_DIGEST, index.imageIdMatchForm());
        assertEquals(CONFIG, index.containerImageId()); assertEquals(INDEX, index.inspectedImageId());
        assertEquals(List.of(REPO_DIGEST), index.repoDigests());
        assertEquals("io.containerd.snapshotter.v1", index.driverType().orElseThrow());
        assertThrows(UnsupportedOperationException.class, () -> index.repoDigests().clear());
    }

    @Test void inspectedImageIdAloneIsNotAnAlternativePinAndOptionalDigestIsIndependent() {
        var actual = observation(INDEX, "overlayfs", Optional.of("io.containerd.snapshotter.v1"));
        assertThrows(IllegalStateException.class, () -> ImageIdentityVerifier.verify(Optional.of(OTHER), Optional.empty(), actual));
        assertThrows(IllegalStateException.class, () -> ImageIdentityVerifier.verify(Optional.of(CONFIG), Optional.of(OTHER), actual));
        var unboundIndex = new ImageIdentityVerifier.Observation(CONFIG, INDEX, List.of(), "overlayfs", Optional.empty());
        assertThrows(IllegalStateException.class, () -> ImageIdentityVerifier.verify(Optional.of(INDEX), Optional.empty(), unboundIndex));
        assertThrows(IllegalStateException.class, () -> ImageIdentityVerifier.verify(Optional.empty(), Optional.empty(), unboundIndex));
        var unpinned = ImageIdentityVerifier.verify(Optional.empty(), Optional.empty(),
                new ImageIdentityVerifier.Observation(CONFIG, CONFIG, List.of(), "overlay2", Optional.empty()));
        assertEquals(ImageIdentityEvidence.MatchForm.UNPINNED, unpinned.imageIdMatchForm());
    }

    @Test void matchingOneFormStillRequiresCompleteObservationOfTheOtherAndBackend() {
        for (var incomplete : List.of(
                new ImageIdentityVerifier.Observation(CONFIG, CONFIG, null, "overlay2", Optional.empty()),
                new ImageIdentityVerifier.Observation(CONFIG, null, List.of(), "overlay2", Optional.empty()),
                new ImageIdentityVerifier.Observation(CONFIG, CONFIG, List.of(), null, Optional.empty()),
                new ImageIdentityVerifier.Observation(CONFIG, CONFIG, List.of("sha256:" + "b".repeat(64)), "overlay2", Optional.empty()))) {
            assertThrows(IllegalStateException.class, () -> ImageIdentityVerifier.verify(Optional.of(CONFIG), Optional.empty(), incomplete));
        }
    }

    @Test void stoppedContainerGateRejectsBothMismatchesAndCannotReusePreviousIncarnation() {
        String image = "flink:2.2.0";
        var target = FlinkRuntimeTarget.withConnectorBundle(image,
                new FlinkConnectorBundleInstallation(image, List.of(), new ConnectorClasspathManifest(List.of())))
                .withExpectedImageId(INDEX).withExpectedImageDigest(INDEX);
        var factory = new FlinkContainer(target, Network.SHARED, temporary);
        var container = (VerifiedFlinkContainer) factory.createJobManager("jobmanager-1");
        var actual = observation(INDEX, "overlayfs", Optional.of("io.containerd.snapshotter.v1"));
        container.verifyImageIdentity("created-before-start", ignored -> actual);
        assertEquals(CONFIG, container.verifiedImageId());
        assertEquals(ImageIdentityEvidence.MatchForm.REPO_DIGEST,
                container.imageIdentityEvidence("created-before-start").imageIdMatchForm());
        assertThrows(IllegalStateException.class, () -> container.imageIdentityEvidence("other-container"));
        var mismatch = new ImageIdentityVerifier.Observation(OTHER, OTHER, List.of("flink@" + OTHER), "overlay2", Optional.empty());
        assertThrows(IllegalStateException.class, () -> container.verifyImageIdentity("retry-before-start", ignored -> mismatch));
        assertThrows(IllegalStateException.class, container::verifiedImageId);
        assertThrows(IllegalStateException.class, () -> container.imageIdentityEvidence("created-before-start"));
    }

    @Test void captureInspectsContainerBoundImmutableImageAndOnlySelectedBackendFields() {
        var calls = new ArrayList<String>();
        DockerClient docker = docker(() -> new InspectImageResponse().withId(INDEX).withRepoDigests(List.of(REPO_DIGEST)),
                () -> new Info().withDriver("overlayfs").withDriverStatuses(List.of(
                        List.of("driver-type", "io.containerd.snapshotter.v1"), List.of("irrelevant", "not retained"))), calls);
        var captured = ImageIdentityVerifier.capture(docker, "physical-container", deadline(Duration.ofSeconds(1)));
        assertEquals(List.of("container:physical-container", "image:" + CONFIG, "info"), calls);
        assertEquals(INDEX, captured.inspectedImageId()); assertEquals(List.of(REPO_DIGEST), captured.repoDigests());
        assertEquals(Optional.of("io.containerd.snapshotter.v1"), captured.driverType());
    }

    @Test void ambiguousDriverTypeFailsClosed() {
        DockerClient docker = docker(() -> new InspectImageResponse().withId(CONFIG).withRepoDigests(List.of()),
                () -> new Info().withDriver("overlayfs").withDriverStatuses(List.of(
                        List.of("driver-type", "one"), List.of("driver-type", "two"))), new ArrayList<>());
        assertThrows(IllegalStateException.class,
                () -> ImageIdentityVerifier.capture(docker, "physical-container", deadline(Duration.ofSeconds(1))));
    }

    @Test void uncooperativeImageInspectReturnsBoundedlyAndCannotAdvanceToInfoAfterTimeout() throws Exception {
        CountDownLatch release = new CountDownLatch(1), returned = new CountDownLatch(1);
        AtomicInteger infoCalls = new AtomicInteger();
        DockerClient docker = docker(() -> {
            while (true) { try { release.await(); break; } catch (InterruptedException ignored) {} }
            returned.countDown();
            return new InspectImageResponse().withId(CONFIG).withRepoDigests(List.of(REPO_DIGEST));
        }, () -> { infoCalls.incrementAndGet(); return new Info().withDriver("overlay2"); }, new ArrayList<>());
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertThrows(ContainerOperationTimeoutException.class,
                    () -> ImageIdentityVerifier.capture(docker, "physical-container", deadline(Duration.ofMillis(100)))));
            assertEquals(0, infoCalls.get());
        } finally { release.countDown(); }
        assertTrue(returned.await(1, TimeUnit.SECONDS));
        assertEquals(0, infoCalls.get());
    }

    private static ImageIdentityVerifier.Observation observation(String inspected, String driver, Optional<String> type) {
        return new ImageIdentityVerifier.Observation(CONFIG, inspected, List.of(REPO_DIGEST), driver, type);
    }

    private static ContainerOperationDeadline deadline(Duration timeout) {
        return ContainerOperationDeadline.start("generated image identity check", timeout, System::nanoTime);
    }

    private static DockerClient docker(Supplier<InspectImageResponse> image, Supplier<Info> info, List<String> calls) {
        return proxy(DockerClient.class, (unused, method, args) -> switch (method.getName()) {
            case "inspectContainerCmd" -> { calls.add("container:" + args[0]); yield command(InspectContainerCmd.class,
                    () -> new InspectContainerResponse() { @Override public String getImageId() { return CONFIG; } }); }
            case "inspectImageCmd" -> { calls.add("image:" + args[0]); yield command(InspectImageCmd.class, image); }
            case "infoCmd" -> { calls.add("info"); yield command(InfoCmd.class, info); }
            default -> throw new AssertionError("Unexpected Docker call " + method.getName());
        });
    }

    private static <T> T command(Class<T> type, Supplier<?> action) {
        return proxy(type, (unused, method, args) -> {
            if (method.getName().equals("exec")) return action.get();
            if (method.getName().equals("close")) return null;
            throw new AssertionError("Unexpected command " + method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
