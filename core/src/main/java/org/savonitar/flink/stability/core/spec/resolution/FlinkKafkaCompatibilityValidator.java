package org.savonitar.flink.stability.core.spec.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;

/** Validates the Docker-free Flink/Kafka-connector compatibility registry. */
final class FlinkKafkaCompatibilityValidator {
    static final String FLINK_LINE_UNSUPPORTED = "capability.flink-line.unsupported";
    static final String KAFKA_CONNECTOR_LINE_UNSUPPORTED =
            "capability.kafka-connector-line.unsupported";
    static final String CONNECTOR_MAVEN_COORDINATE_UNREGISTERED =
            "capability.connector-maven-coordinate.unregistered";
    static final String KAFKA_IMAGE_VERSION_UNSUPPORTED =
            "runner.kafka.image-version-unsupported";

    private static final String SUPPORTED_FLINK_LINE = "2.2";
    private static final String KAFKA_CONNECTOR_GROUP = "org.apache.flink";
    private static final String KAFKA_CONNECTOR_ARTIFACT = "flink-connector-kafka";
    private static final Pattern FLINK_TAG = Pattern.compile(
            "^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)"
                    + "(?:\\.(0|[1-9][0-9]*))?(?:[-_][A-Za-z0-9][A-Za-z0-9._-]*)?$");
    private static final Pattern MAVEN_REFERENCE = Pattern.compile(
            "^maven:([A-Za-z0-9_]+(?:[.-][A-Za-z0-9_]+)*):"
                    + "([A-Za-z0-9_]+(?:[._-][A-Za-z0-9_]+)*):"
                    + "([A-Za-z0-9_]+(?:[._+-][A-Za-z0-9_]+)*)$");
    private static final Pattern KAFKA_CONNECTOR_RELEASE = Pattern.compile(
            "^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)"
                    + "-(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$");
    private static final Pattern TIMESTAMPED_SNAPSHOT = Pattern.compile(
            ".*-\\d{8}\\.\\d{6}-\\d+$");
    List<Diagnostic> validate(Path source, ResolutionScope scope, ObjectNode document) {
        List<Diagnostic> issues = new ArrayList<>();
        List<ImageReference> images = flinkImages(document);
        boolean flinkLineSupported = true;
        for (ImageReference image : images) {
            if (!image.value().isTextual()) {
                // Resolved JSON Schema validation owns the value's structural root cause.
                continue;
            }
            String declared = image.value().textValue();
            Matcher tag = flinkTag(declared);
            if (tag == null || !isSupportedFlinkLine(tag)) {
                flinkLineSupported = false;
                issues.add(issue(
                        source,
                        scope,
                        FLINK_LINE_UNSUPPORTED,
                        image.path(),
                        flinkLineMessage(declared, tag)));
            }
        }

        for (ImageReference image : kafkaImages(document)) {
            if (!image.value().isTextual()) {
                continue;
            }
            String declared = image.value().textValue();
            if (!KafkaBrokerImagePolicy.isSupportedV1(declared)) {
                issues.add(issue(
                        source,
                        scope,
                        KAFKA_IMAGE_VERSION_UNSUPPORTED,
                        image.path(),
                        "Kafka image reference '" + declared
                                + "' is outside the initial registry; v1 requires exact official "
                                + "apache/kafka:4.0.<patch> tags with an optional sha256 digest"));
            }
        }

        // A connector/Flink combination cannot be assessed until every Flink image is on a
        // supported line. Report the image root cause without derivative connector noise.
        if (!flinkLineSupported) {
            return List.copyOf(issues);
        }

        JsonNode connectors = document.at("/subject/connectors");
        if (connectors instanceof ObjectNode connectorObject) {
            connectorObject.fields().forEachRemaining(entry -> validateConnector(
                    source,
                    scope,
                    entry.getKey(),
                    entry.getValue().get("artifact"),
                    issues));
        }
        return List.copyOf(issues);
    }

    private static List<ImageReference> flinkImages(ObjectNode document) {
        List<ImageReference> images = new ArrayList<>();
        JsonNode setupImage = document.at("/setup/flink/image");
        if (!setupImage.isMissingNode()) {
            images.add(new ImageReference("$/setup/flink/image", setupImage));
        }
        collectRestartImages(document.get("phases"), "$/phases", images);
        return List.copyOf(images);
    }

    private static List<ImageReference> kafkaImages(ObjectNode document) {
        List<ImageReference> images = new ArrayList<>();
        JsonNode clusters = document.at("/setup/kafka/clusters");
        if (clusters instanceof ObjectNode clusterObject) {
            clusterObject.fields().forEachRemaining(cluster -> {
                JsonNode image = cluster.getValue().get("image");
                if (image != null) {
                    images.add(new ImageReference(
                            "$/setup/kafka/clusters/" + escapePointer(cluster.getKey()) + "/image",
                            image));
                }
            });
        }
        collectKafkaRestartImages(document.get("phases"), "$/phases", images);
        return List.copyOf(images);
    }

    private static void collectRestartImages(
            JsonNode node,
            String path,
            List<ImageReference> images) {
        if (node instanceof ObjectNode object) {
            JsonNode restart = object.get("restart");
            if (restart instanceof ObjectNode restartObject
                    && isFlinkComponent(restartObject.path("component").textValue())
                    && restartObject.has("image")) {
                images.add(new ImageReference(path + "/restart/image", restartObject.get("image")));
            }
            object.fields().forEachRemaining(entry -> collectRestartImages(
                    entry.getValue(), path + "/" + escapePointer(entry.getKey()), images));
        } else if (node instanceof ArrayNode array) {
            for (int index = 0; index < array.size(); index++) {
                collectRestartImages(array.get(index), path + "/" + index, images);
            }
        }
    }

    private static void collectKafkaRestartImages(
            JsonNode node,
            String path,
            List<ImageReference> images) {
        if (node instanceof ObjectNode object) {
            JsonNode restart = object.get("restart");
            if (restart instanceof ObjectNode restartObject
                    && "kafka".equals(restartObject.path("component").textValue())
                    && restartObject.has("image")) {
                images.add(new ImageReference(path + "/restart/image", restartObject.get("image")));
            }
            object.fields().forEachRemaining(entry -> collectKafkaRestartImages(
                    entry.getValue(), path + "/" + escapePointer(entry.getKey()), images));
        } else if (node instanceof ArrayNode array) {
            for (int index = 0; index < array.size(); index++) {
                collectKafkaRestartImages(array.get(index), path + "/" + index, images);
            }
        }
    }

    private static boolean isFlinkComponent(String component) {
        return "flink".equals(component)
                || "jobmanager".equals(component)
                || "taskmanager".equals(component);
    }

    private static Matcher flinkTag(String image) {
        int digestStart = image.indexOf('@');
        String namedReference = digestStart >= 0 ? image.substring(0, digestStart) : image;
        int lastSlash = namedReference.lastIndexOf('/');
        int tagSeparator = namedReference.lastIndexOf(':');
        if (tagSeparator <= lastSlash || tagSeparator == namedReference.length() - 1) {
            return null;
        }
        Matcher matcher = FLINK_TAG.matcher(namedReference.substring(tagSeparator + 1));
        return matcher.matches() ? matcher : null;
    }

    private static boolean isSupportedFlinkLine(Matcher tag) {
        return "2".equals(tag.group(1)) && "2".equals(tag.group(2));
    }

    private static String flinkLineMessage(String declared, Matcher tag) {
        if (tag == null) {
            return "Cannot establish a supported Flink line from image reference '"
                    + declared + "'; v1 currently supports tags identifying Flink "
                    + SUPPORTED_FLINK_LINE + ".x";
        }
        return "Flink image reference '" + declared + "' identifies line "
                + tag.group(1) + "." + tag.group(2)
                + ", but v1 currently supports only Flink " + SUPPORTED_FLINK_LINE + ".x";
    }

    private static void validateConnector(
            Path source,
            ResolutionScope scope,
            String alias,
            JsonNode artifact,
            List<Diagnostic> issues) {
        if (artifact == null || !artifact.isTextual()) {
            return;
        }
        String reference = artifact.textValue();
        if (!reference.startsWith("maven:")) {
            // A local primary is the scenario author's compatibility assertion (R5.6b).
            return;
        }

        Matcher coordinate = MAVEN_REFERENCE.matcher(reference);
        if (!coordinate.matches() || hasArtifactOwnedVersionError(coordinate.group(3))) {
            // Canonical coordinate and immutable-release diagnostics belong to artifact
            // preparation. Do not replace them with a derivative compatibility failure.
            return;
        }
        if (!KAFKA_CONNECTOR_GROUP.equals(coordinate.group(1))
                || !KAFKA_CONNECTOR_ARTIFACT.equals(coordinate.group(2))) {
            issues.add(issue(
                    source,
                    scope,
                    CONNECTOR_MAVEN_COORDINATE_UNREGISTERED,
                    "$/subject/connectors/" + escapePointer(alias) + "/artifact",
                    "Connector Maven reference '" + reference
                            + "' is not registered for v1 execution; the initial registry accepts "
                            + "only org.apache.flink:flink-connector-kafka:"
                            + "5.0.<patch>-2.2, while local primaries are explicit author "
                            + "compatibility assertions"));
            return;
        }

        String version = coordinate.group(3);
        Matcher release = KAFKA_CONNECTOR_RELEASE.matcher(version);
        boolean supported = release.matches()
                && "5".equals(release.group(1))
                && "0".equals(release.group(2))
                && "2".equals(release.group(4))
                && "2".equals(release.group(5));
        if (!supported) {
            issues.add(issue(
                    source,
                    scope,
                    KAFKA_CONNECTOR_LINE_UNSUPPORTED,
                    "$/subject/connectors/" + escapePointer(alias) + "/artifact",
                    "Kafka connector reference '" + reference
                            + "' is outside the initial compatibility registry; supported Maven "
                            + "coordinates use org.apache.flink:flink-connector-kafka:"
                            + "5.0.<patch>-2.2 with Flink " + SUPPORTED_FLINK_LINE + ".x"));
        }
    }

    private static boolean hasArtifactOwnedVersionError(String version) {
        String upper = version.toUpperCase(Locale.ROOT);
        return upper.equals("LATEST")
                || upper.equals("RELEASE")
                || upper.contains("SNAPSHOT")
                || TIMESTAMPED_SNAPSHOT.matcher(version).matches();
    }

    private static Diagnostic issue(
            Path source,
            ResolutionScope scope,
            String code,
            String path,
            String message) {
        return new Diagnostic(source, scope, code, path, message);
    }

    private static String escapePointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }

    private record ImageReference(String path, JsonNode value) {}
}
