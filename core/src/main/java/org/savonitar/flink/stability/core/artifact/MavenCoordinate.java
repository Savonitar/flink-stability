package org.savonitar.flink.stability.core.artifact;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Canonical v1 primary-JAR Maven coordinate. */
record MavenCoordinate(String groupId, String artifactId, String version) {
    private static final Pattern REFERENCE = Pattern.compile(
            "^maven:([A-Za-z0-9_]+(?:[.-][A-Za-z0-9_]+)*):"
                    + "([A-Za-z0-9_]+(?:[._-][A-Za-z0-9_]+)*):"
                    + "([A-Za-z0-9_]+(?:[._+-][A-Za-z0-9_]+)*)$");
    private static final Pattern TIMESTAMPED_SNAPSHOT = Pattern.compile(
            ".*-\\d{8}\\.\\d{6}-\\d+$");

    MavenCoordinate {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(artifactId, "artifactId");
        Objects.requireNonNull(version, "version");
    }

    static MavenCoordinate parse(String reference) {
        Matcher matcher = REFERENCE.matcher(reference);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Expected maven:<groupId>:<artifactId>:<version> for a primary JAR");
        }
        String version = matcher.group(3);
        String upperVersion = version.toUpperCase(java.util.Locale.ROOT);
        if (version.equalsIgnoreCase("LATEST")
                || version.equalsIgnoreCase("RELEASE")
                || upperVersion.contains("SNAPSHOT")
                || TIMESTAMPED_SNAPSHOT.matcher(version).matches()) {
            throw new IllegalArgumentException(
                    "Maven version must be an immutable release, not '" + version + "'");
        }
        return new MavenCoordinate(matcher.group(1), matcher.group(2), version);
    }

    String resolverCoordinate() {
        return groupId + ":" + artifactId + ":jar:" + version;
    }

    String declaredReference() {
        return "maven:" + groupId + ":" + artifactId + ":" + version;
    }
}
