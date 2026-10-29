package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class FuseCheckpointStorageTest {
    private static final String IMAGE_ID = "sha256:" + "a".repeat(64);
    private static final Map<String, String> FILES = Map.of("checkpoint-fuse", "a".repeat(64),
            "checkpoint-fuse-selftest", "b".repeat(64), "probe.jar", "c".repeat(64));
    private static FuseCheckpointStorage storage() {
        return new FuseCheckpointStorage(Path.of("/prepared/helpers"), "d".repeat(64),
                IMAGE_ID, FuseCheckpointStorage.SUPPORTED_PLATFORM, FILES);
    }
    private static FlinkRuntimeTarget target() {
        String image = "flink:2.2.0";
        return FlinkRuntimeTarget.withConnectorBundle(image, new FlinkConnectorBundleInstallation(image,
                List.of(), new ConnectorClasspathManifest(List.of()))).withExpectedImageId(IMAGE_ID);
    }
    @Test void exactBundleAndAuthorDeclaredIdentityAreRequiredBeforeContainerProvisioning() {
        assertEquals(FILES, storage().files());
        assertThrows(UnsupportedOperationException.class, () -> storage().files().clear());
        assertThrows(IllegalArgumentException.class, () -> new FuseCheckpointStorage(Path.of("relative"), "d".repeat(64),
                IMAGE_ID, FuseCheckpointStorage.SUPPORTED_PLATFORM, FILES));
        assertThrows(IllegalArgumentException.class, () -> new FuseCheckpointStorage(Path.of("/prepared"), "d".repeat(64),
                IMAGE_ID, "linux/amd64", FILES));
        assertEquals("sha256:" + "e".repeat(64), new FuseCheckpointStorage(Path.of("/prepared"), "d".repeat(64),
                "sha256:" + "e".repeat(64), FuseCheckpointStorage.SUPPORTED_PLATFORM, FILES).imageId());
        assertThrows(IllegalArgumentException.class, () -> new FuseCheckpointStorage(Path.of("/prepared"), "d".repeat(64),
                IMAGE_ID, FuseCheckpointStorage.SUPPORTED_PLATFORM, Map.of()));
    }
    @Test void optionalRepositoryDigestSurvivesEveryTargetCopy() {
        String digest = "sha256:" + "f".repeat(64);
        var configured = target().withExpectedImageDigest(digest).withCheckpointStorage(storage())
                .withConfig(Map.of("custom.setting", "value")).withTaskManagers(1).withLogMarkers(List.of());
        assertEquals(java.util.Optional.of(digest), configured.expectedImageDigest());
        assertNotEquals(target(), target().withExpectedImageDigest(digest));
        assertThrows(IllegalArgumentException.class, () -> target().withExpectedImageDigest("flink@" + digest));
    }

    @Test void effectiveProcessConfigurationIncludesOwnedSettingsAndPreservesExactCustomValues() {
        Map<String, String> custom = Map.of("custom.setting", "literal value");
        var target = target().withConfig(custom);
        assertEquals(custom, target.effectiveConfig());
        var enabled = target.withCheckpointStorage(storage());
        var expected = new java.util.TreeMap<>(custom);
        expected.putAll(FuseCheckpointStorage.PROCESS_CONFIGURATION);
        assertEquals(expected, enabled.effectiveConfig());
        assertEquals(custom, enabled.config(), "Author configuration remains separate from capability settings");
        assertThrows(UnsupportedOperationException.class, () -> enabled.effectiveConfig().clear());
        for (var entry : FuseCheckpointStorage.PROCESS_CONFIGURATION.entrySet()) {
            assertThrows(IllegalArgumentException.class, () -> enabled.withConfig(Map.of(entry.getKey(), entry.getValue())));
            assertThrows(IllegalArgumentException.class, () -> target().withConfig(Map.of(entry.getKey(), "unexpected")).withCheckpointStorage(storage()));
        }
    }

    @Test void storageIdentitySurvivesCopiesAndRejectsUnsupportedLifecycle() {
        var configured = target().withCheckpointStorage(storage()).withConfig(Map.of("custom.setting", "value"))
                .withLogMarkers(List.of()).withTaskManagers(1).withExpectedImageId(IMAGE_ID);
        assertEquals(storage(), configured.checkpointStorage().orElseThrow());
        assertNotEquals(target(), target().withCheckpointStorage(storage()));
        assertEquals(configured, target().withConfig(Map.of("custom.setting", "value")).withCheckpointStorage(storage()));
        assertThrows(IllegalArgumentException.class, () -> configured.withTaskManagers(2));
        assertThrows(IllegalArgumentException.class, () -> configured.withExpectedImageId("sha256:" + "e".repeat(64)));
        assertThrows(IllegalArgumentException.class, () -> configured.withHighAvailability(
                new FlinkRuntimeTarget.HighAvailability("zookeeper:3.9.3", Duration.ofSeconds(6))));
        assertThrows(IllegalArgumentException.class, () -> configured.withExpectedRuntimeJar(
                new FlinkRuntimeTarget.RuntimeJar("/opt/flink/lib/flink-dist-2.2.0.jar", "f".repeat(64))));
    }

    @Test void declaredToleranceControlsEveryIncarnationAndUsesCanonicalByteUnit() {
        var storage = storage().withTolerableFailedCheckpoints(3);
        var configured = target().withCheckpointStorage(storage).withConfig(Map.of("custom.setting", "value"))
                .withTaskManagers(1).withLogMarkers(List.of()).withExpectedImageId(IMAGE_ID);
        assertEquals(3, configured.checkpointStorage().orElseThrow().tolerableFailedCheckpoints());
        assertEquals("3", configured.effectiveConfig().get("execution.checkpointing.tolerable-failed-checkpoints"));
        assertEquals("1", configured.effectiveConfig().get("execution.checkpointing.max-concurrent-checkpoints"));
        assertEquals("0 bytes", configured.effectiveConfig().get("execution.checkpointing.data-inline-threshold"));
        assertEquals(10, storage().tolerableFailedCheckpoints(), "Legacy constructor keeps its historical default");
        assertNotEquals(storage(), storage);
        assertThrows(IllegalArgumentException.class, () -> storage().withTolerableFailedCheckpoints(0));
        assertThrows(IllegalArgumentException.class, () -> storage().withTolerableFailedCheckpoints(11));
    }
}
