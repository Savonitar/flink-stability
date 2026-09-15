package org.savonitar.flink.stability.core.artifact;

import org.eclipse.aether.artifact.Artifact;

import java.util.Comparator;
import java.util.Objects;

/** Immutable identity used for Maven conflict mediation and evidence. */
public record MavenArtifactIdentity(
        String groupId,
        String artifactId,
        String extension,
        String classifier,
        String version) implements Comparable<MavenArtifactIdentity> {
    private static final Comparator<MavenArtifactIdentity> ORDER = Comparator
            .comparing(MavenArtifactIdentity::groupId)
            .thenComparing(MavenArtifactIdentity::artifactId)
            .thenComparing(MavenArtifactIdentity::extension)
            .thenComparing(MavenArtifactIdentity::classifier)
            .thenComparing(MavenArtifactIdentity::version);

    public MavenArtifactIdentity {
        groupId = requireNonBlank(groupId, "groupId");
        artifactId = requireNonBlank(artifactId, "artifactId");
        extension = requireNonBlank(extension, "extension");
        classifier = Objects.requireNonNull(classifier, "classifier");
        version = requireNonBlank(version, "version");
    }

    static MavenArtifactIdentity from(Artifact artifact) {
        Objects.requireNonNull(artifact, "artifact");
        return new MavenArtifactIdentity(
                artifact.getGroupId(),
                artifact.getArtifactId(),
                artifact.getExtension(),
                artifact.getClassifier(),
                artifact.getVersion());
    }

    public String conflictKey() {
        return groupId + ":" + artifactId + ":" + extension + ":" + classifier;
    }

    public String coordinate() {
        return groupId + ":" + artifactId + ":" + extension
                + (classifier.isEmpty() ? "" : ":" + classifier)
                + ":" + version;
    }

    @Override
    public int compareTo(MavenArtifactIdentity other) {
        return ORDER.compare(this, Objects.requireNonNull(other, "other"));
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
