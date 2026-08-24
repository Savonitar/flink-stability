package org.savonitar.flink.stability.core.spec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable, target-specific connector bundle ready for cluster provisioning. */
public final class PreparedConnectorBundle {
    public static final String TARGET_BINDING_FORMAT =
            "flink-stability.connector-cluster-bundle/v1";
    public static final String CLASSPATH_MANIFEST_FORMAT =
            "flink-stability-connector-classpath-v1";
    public static final String CLASSPATH_MANIFEST_CONTAINER_PATH =
            "/opt/flink/lib/.flink-stability-connector-classpath-v1.json";
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    private final ScenarioSide side;
    private final String targetFlinkImageReference;
    private final List<String> aliases;
    private final List<ConnectorClosureLock> closureLocks;
    private final List<PreparedConnectorBundleEntry> entries;
    private final String canonicalTargetBindingJson;
    private final String targetBindingSha256;
    private final String classpathManifestJson;
    private final String classpathManifestSha256;

    PreparedConnectorBundle(
            ScenarioSide side,
            String targetFlinkImageReference,
            List<String> aliases,
            List<ConnectorClosureLock> closureLocks,
            List<PreparedConnectorBundleEntry> entries,
            String canonicalTargetBindingJson,
            String targetBindingSha256,
            String classpathManifestJson,
            String classpathManifestSha256) {
        this.side = Objects.requireNonNull(side, "side");
        this.targetFlinkImageReference = requireNonBlank(
                targetFlinkImageReference, "targetFlinkImageReference");
        this.aliases = List.copyOf(Objects.requireNonNull(aliases, "aliases"));
        this.closureLocks = List.copyOf(Objects.requireNonNull(
                closureLocks, "closureLocks"));
        this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        this.canonicalTargetBindingJson = requireNonBlank(
                canonicalTargetBindingJson, "canonicalTargetBindingJson");
        this.targetBindingSha256 = Objects.requireNonNull(
                targetBindingSha256, "targetBindingSha256");
        if (!SHA_256.matcher(targetBindingSha256).matches()) {
            throw new IllegalArgumentException(
                    "targetBindingSha256 must be 64 lowercase hexadecimal characters");
        }
        this.classpathManifestJson = requireNonBlank(
                classpathManifestJson, "classpathManifestJson");
        this.classpathManifestSha256 = Objects.requireNonNull(
                classpathManifestSha256, "classpathManifestSha256");
        if (!SHA_256.matcher(classpathManifestSha256).matches()) {
            throw new IllegalArgumentException(
                    "classpathManifestSha256 must be 64 lowercase hexadecimal characters");
        }
        List<String> sortedAliases = this.aliases.stream().sorted().toList();
        if (!this.aliases.equals(sortedAliases)
                || this.aliases.stream().distinct().count() != this.aliases.size()) {
            throw new IllegalArgumentException(
                    "Bundle aliases must be distinct and lexicographically sorted");
        }
        if (!this.closureLocks.stream().map(ConnectorClosureLock::alias).toList()
                .equals(this.aliases)) {
            throw new IllegalArgumentException(
                    "Bundle locks must occur once in alias order");
        }
        if (this.closureLocks.stream().anyMatch(lock -> lock.side() != side
                || !targetFlinkImageReference.equals(lock.targetFlinkImageReference()))) {
            throw new IllegalArgumentException(
                    "Every bundle lock must agree on side and exact target image");
        }
        for (int index = 0; index < this.entries.size(); index++) {
            if (this.entries.get(index).globalClasspathIndex() != index) {
                throw new IllegalArgumentException(
                        "Bundle classpath indexes must be contiguous");
            }
        }
        List<String> entryHashes = new ArrayList<>(this.entries.size());
        this.entries.forEach(entry -> entryHashes.add(entry.sha256()));
        if (entryHashes.stream().distinct().count() != entryHashes.size()) {
            throw new IllegalArgumentException(
                    "Bundle entries must be coalesced by SHA-256");
        }
    }

    public ScenarioSide side() {
        return side;
    }

    public String targetFlinkImageReference() {
        return targetFlinkImageReference;
    }

    public List<String> aliases() {
        return aliases;
    }

    public List<ConnectorClosureLock> closureLocks() {
        return closureLocks;
    }

    public List<PreparedConnectorBundleEntry> entries() {
        return entries;
    }

    public String canonicalTargetBindingJson() {
        return canonicalTargetBindingJson;
    }

    public byte[] canonicalTargetBindingBytes() {
        return canonicalTargetBindingJson.getBytes(StandardCharsets.UTF_8);
    }

    public String targetBindingSha256() {
        return targetBindingSha256;
    }

    /** Canonical byte-only manifest copied identically to every Flink process image. */
    public String classpathManifestJson() {
        return classpathManifestJson;
    }

    public byte[] classpathManifestBytes() {
        return classpathManifestJson.getBytes(StandardCharsets.UTF_8);
    }

    public String classpathManifestSha256() {
        return classpathManifestSha256;
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
