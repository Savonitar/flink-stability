package org.savonitar.flink.stability.runtime.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Immutable, target-image-independent connector bytes installed into every Flink process. */
public final class ConnectorClasspathManifest {
    public static final String FORMAT = "flink-stability-connector-classpath-v1";
    public static final String CONTAINER_MANIFEST_PATH =
            "/opt/flink/lib/.flink-stability-connector-classpath-v1.json";
    private static final String CONTAINER_DIRECTORY = "/opt/flink/lib/";
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    private final List<Entry> entries;
    private final byte[] canonicalBytes;
    private final String manifestSha256;

    public ConnectorClasspathManifest(List<Entry> entries) {
        this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        Set<String> selectedDigests = new HashSet<>();
        for (int index = 0; index < this.entries.size(); index++) {
            Entry entry = this.entries.get(index);
            if (entry.index() != index) {
                throw new IllegalArgumentException(
                        "Connector classpath indexes must be contiguous from zero");
            }
            if (!selectedDigests.add(entry.sha256())) {
                throw new IllegalArgumentException(
                        "Connector classpath manifest contains duplicate bytes at index " + index);
            }
        }
        this.canonicalBytes = canonicalJson(this.entries).getBytes(StandardCharsets.UTF_8);
        this.manifestSha256 = Digests.sha256(canonicalBytes);
    }

    public List<Entry> entries() {
        return entries;
    }

    /** Canonical UTF-8 JSON containing no host paths or target-image metadata. */
    public byte[] canonicalBytes() {
        return canonicalBytes.clone();
    }

    public String manifestSha256() {
        return manifestSha256;
    }

    /** Re-hashes every private staged file immediately before a container is configured. */
    public void verifyHostFiles() {
        for (Entry entry : entries) {
            Path path = entry.preparedPath();
            try {
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new ConnectorBundleProvisioningException(
                            "Prepared connector classpath entry is not a regular file: " + path);
                }
                String observed;
                try (InputStream input = Files.newInputStream(path)) {
                    observed = Digests.sha256(input);
                }
                if (!entry.sha256().equals(observed)) {
                    throw new ConnectorBundleProvisioningException(
                            "Prepared connector classpath entry changed before provisioning: "
                                    + path + " expected " + entry.sha256()
                                    + " but found " + observed);
                }
            } catch (IOException exception) {
                throw new ConnectorBundleProvisioningException(
                        "Could not verify prepared connector classpath entry " + path,
                        exception);
            }
        }
    }

    private static String canonicalJson(List<Entry> entries) {
        StringBuilder json = new StringBuilder("{\"entries\":[");
        for (int index = 0; index < entries.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            Entry entry = entries.get(index);
            json.append("{\"filename\":\"")
                    .append(entry.containerFilename())
                    .append("\",\"index\":")
                    .append(entry.index())
                    .append(",\"sha256\":\"")
                    .append(entry.sha256())
                    .append("\"}");
        }
        return json.append("],\"format\":\"")
                .append(FORMAT)
                .append("\"}")
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof ConnectorClasspathManifest manifest
                && entries.equals(manifest.entries)
                && Arrays.equals(canonicalBytes, manifest.canonicalBytes);
    }

    @Override
    public int hashCode() {
        return 31 * entries.hashCode() + Arrays.hashCode(canonicalBytes);
    }

    /** One globally ordered prepared JAR. Its host path is excluded from canonical JSON. */
    public record Entry(int index, Path preparedPath, String sha256) {
        public Entry {
            if (index < 0 || index > 99_999_999) {
                throw new IllegalArgumentException(
                        "index must fit exactly eight decimal digits");
            }
            preparedPath = Objects.requireNonNull(preparedPath, "preparedPath")
                    .toAbsolutePath()
                    .normalize();
            if (!preparedPath.isAbsolute()) {
                throw new IllegalArgumentException("preparedPath must be absolute");
            }
            sha256 = Objects.requireNonNull(sha256, "sha256");
            if (!SHA_256.matcher(sha256).matches()) {
                throw new IllegalArgumentException(
                        "sha256 must contain 64 lowercase hexadecimal characters");
            }
        }

        public String containerFilename() {
            return String.format(
                    Locale.ROOT,
                    "flink-stability-connector-%08d-%s.jar",
                    index,
                    sha256);
        }

        public String containerPath() {
            return CONTAINER_DIRECTORY + containerFilename();
        }
    }
}
