package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarInputStream;

import static org.junit.jupiter.api.Assertions.*;

class SyntheticTokenPluginTest {
    @Test
    void embedsOnlyFixtureClassesAndBothSpiRegistrationsWithVerifiedImmutableBytes() throws Exception {
        var plugin = new SyntheticTokenPlugin();
        byte[] altered = plugin.bytes();
        altered[0] ^= 1;
        assertEquals(plugin.sha256(), Digests.sha256(plugin.bytes()));
        plugin.verify(path -> {
            assertEquals(SyntheticTokenPlugin.CONTAINER_PATH, path);
            return plugin.sha256();
        });
        assertThrows(IllegalStateException.class, () -> plugin.verify(path -> "0".repeat(64)));
        Set<String> entries = new HashSet<>();
        try (var jar = new JarInputStream(new ByteArrayInputStream(plugin.bytes()))) {
            for (var entry = jar.getNextJarEntry(); entry != null; entry = jar.getNextJarEntry()) {
                entries.add(entry.getName());
            }
        }
        assertTrue(entries.contains("META-INF/services/org.apache.flink.core.security.token.DelegationTokenProvider"));
        assertTrue(entries.contains("META-INF/services/org.apache.flink.core.security.token.DelegationTokenReceiver"));
        assertTrue(entries.stream().noneMatch(name -> name.startsWith("org/apache/flink/")));
        assertTrue(SyntheticTokenPlugin.configuration(1234, "jobmanager-2#3", "jobmanager")
                .contains("process: jobmanager-2#3"));
        assertThrows(IllegalArgumentException.class,
                () -> SyntheticTokenPlugin.configuration(1234, "jobmanager-2#3", "taskmanager"));
    }
}
