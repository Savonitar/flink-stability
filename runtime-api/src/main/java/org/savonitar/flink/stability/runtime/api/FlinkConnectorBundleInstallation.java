package org.savonitar.flink.stability.runtime.api;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/**
 * Immutable target-specific connector binding paired with target-independent classpath bytes.
 * The binding identity is derived here from its complete structured input; callers cannot pair
 * an arbitrary binding hash with a different image or manifest.
 */
public record FlinkConnectorBundleInstallation(
        String targetFlinkImageReference,
        List<ClosureLockHash> closureLocks,
        ConnectorClasspathManifest classpathManifest) {
    public static final String FORMAT = "flink-stability.connector-cluster-bundle/v1";

    public FlinkConnectorBundleInstallation {
        targetFlinkImageReference = requireNonBlank(
                targetFlinkImageReference, "targetFlinkImageReference");
        closureLocks = sortedClosureLocks(closureLocks);
        classpathManifest = Objects.requireNonNull(classpathManifest, "classpathManifest");
    }

    /** Canonical UTF-8 target-binding JSON; these bytes are report evidence, not container input. */
    public byte[] canonicalTargetBindingBytes() {
        return canonicalTargetBindingJson().getBytes(StandardCharsets.UTF_8);
    }

    /** SHA-256 derived exclusively from {@link #canonicalTargetBindingBytes()}. */
    public String targetBindingSha256() {
        return Digests.sha256(canonicalTargetBindingBytes());
    }

    private String canonicalTargetBindingJson() {
        StringBuilder json = new StringBuilder()
                .append("{\"classpath_manifest_sha256\":\"")
                .append(classpathManifest.manifestSha256())
                .append("\",\"closures\":[");
        for (int index = 0; index < closureLocks.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            ClosureLockHash closure = closureLocks.get(index);
            json.append("{\"alias\":\"")
                    .append(escapeJson(closure.alias()))
                    .append("\",\"closure_sha256\":\"")
                    .append(closure.closureSha256())
                    .append("\"}");
        }
        return json.append("],\"format\":\"")
                .append(FORMAT)
                .append("\",\"target_flink_image_reference\":\"")
                .append(escapeJson(targetFlinkImageReference))
                .append("\"}")
                .toString();
    }

    private static List<ClosureLockHash> sortedClosureLocks(
            List<ClosureLockHash> closureLocks) {
        List<ClosureLockHash> sorted = new ArrayList<>(List.copyOf(
                Objects.requireNonNull(closureLocks, "closureLocks")));
        sorted.sort(Comparator.comparing(ClosureLockHash::alias));
        Set<String> aliases = new HashSet<>();
        for (ClosureLockHash closure : sorted) {
            if (!aliases.add(closure.alias())) {
                throw new IllegalArgumentException(
                        "closureLocks contains duplicate alias: " + closure.alias());
            }
        }
        return List.copyOf(sorted);
    }

    /* Matches Jackson's default compact JSON string escaping used by the core canonicalizer. */
    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) {
                        escaped.append(String.format(
                                Locale.ROOT, "\\u%04X", (int) character));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.toString();
    }

    /** One alias and the SHA-256 of its exact target-specific dependency closure lock. */
    public record ClosureLockHash(String alias, String closureSha256) {
        private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

        public ClosureLockHash {
            alias = requireNonBlank(alias, "alias");
            closureSha256 = Objects.requireNonNull(closureSha256, "closureSha256");
            if (!SHA_256.matcher(closureSha256).matches()) {
                throw new IllegalArgumentException(
                        "closureSha256 must contain 64 lowercase hexadecimal characters");
            }
        }
    }
}
