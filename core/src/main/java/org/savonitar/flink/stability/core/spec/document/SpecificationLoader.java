package org.savonitar.flink.stability.core.spec.document;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import com.networknt.schema.path.NodePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Parses, dispatches, and structurally validates v1 harness documents. */
public final class SpecificationLoader {
    public static final String SUPPORTED_FORMAT = "v1";

    private final ObjectMapper yamlMapper;
    private final Map<DocumentKind, Schema> schemas;

    public SpecificationLoader() {
        YAMLFactory yamlFactory = YAMLFactory.builder()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();
        this.yamlMapper = new ObjectMapper(yamlFactory);
        this.schemas = loadSchemas();
    }

    public LoadedSpecification load(String source) {
        return load(Path.of(source));
    }

    public LoadedSpecification load(Path source) {
        Path normalizedSource = source.toAbsolutePath().normalize();
        ObjectNode document = parse(normalizedSource);
        String format = requiredText(document, normalizedSource, "format", "document.missing-format");
        if (!SUPPORTED_FORMAT.equals(format)) {
            throw failure(normalizedSource, "document.unsupported-format", "$/format",
                    "Unsupported format '" + format + "'; supported formats: " + SUPPORTED_FORMAT);
        }

        String kindValue = requiredText(document, normalizedSource, "kind", "document.missing-kind");
        DocumentKind kind = DocumentKind.fromValue(kindValue)
                .orElseThrow(() -> failure(normalizedSource, "document.unknown-kind", "$/kind",
                        "Unknown kind '" + kindValue + "'; supported kinds: " + supportedKinds()));

        validateSchema(normalizedSource, kind, document);
        return createSpecification(normalizedSource, kind, document);
    }

    public LoadedSpecification load(Path source, DocumentKind expectedKind) {
        LoadedSpecification document = load(source);
        if (document.kind() != expectedKind) {
            throw failure(document.source(), "document.kind-mismatch", "$/kind",
                    "Expected kind '" + expectedKind.value() + "' but found '" + document.kind().value() + "'");
        }
        return document;
    }

    public ScenarioSpecification loadScenario(Path source) {
        return (ScenarioSpecification) load(source, DocumentKind.SCENARIO);
    }

    public ExpectedResultSpecification loadExpectedResult(Path source) {
        return (ExpectedResultSpecification) load(source, DocumentKind.EXPECTED_RESULT);
    }

    public SuiteSpecification loadSuite(Path source) {
        return (SuiteSpecification) load(source, DocumentKind.SUITE);
    }

    /** Validates an already-parsed raw scenario without reading it from disk. */
    public ScenarioSpecification validateScenarioDocument(Path source, ObjectNode document) {
        return (ScenarioSpecification) validateDocument(source, document, DocumentKind.SCENARIO);
    }

    /** Validates an already-parsed expected-result document without reading it from disk. */
    public ExpectedResultSpecification validateExpectedResultDocument(Path source, ObjectNode document) {
        return (ExpectedResultSpecification) validateDocument(source, document, DocumentKind.EXPECTED_RESULT);
    }

    /** Validates an already-parsed suite document without reading it from disk. */
    public SuiteSpecification validateSuiteDocument(Path source, ObjectNode document) {
        return (SuiteSpecification) validateDocument(source, document, DocumentKind.SUITE);
    }

    private LoadedSpecification validateDocument(
            Path source, ObjectNode document, DocumentKind expectedKind) {
        Path normalizedSource = source.toAbsolutePath().normalize();
        String format = requiredText(document, normalizedSource, "format", "document.missing-format");
        if (!SUPPORTED_FORMAT.equals(format)) {
            throw failure(normalizedSource, "document.unsupported-format", "$/format",
                    "Unsupported format '" + format + "'; supported formats: " + SUPPORTED_FORMAT);
        }
        String kind = requiredText(document, normalizedSource, "kind", "document.missing-kind");
        if (!expectedKind.value().equals(kind)) {
            throw failure(normalizedSource, "document.kind-mismatch", "$/kind",
                    "Expected kind '" + expectedKind.value() + "' but found '" + kind + "'");
        }
        validateSchema(normalizedSource, expectedKind, document);
        return createSpecification(normalizedSource, expectedKind, document);
    }

    /** Re-validates a fully materialized scenario and rejects unresolved constructs. */
    public ScenarioSpecification validateResolvedScenario(Path source, ObjectNode document) {
        ScenarioSpecification validated = validateScenarioDocument(source, document);
        List<ValidationIssue> issues = new ArrayList<>();
        if (document.has("parameters")) {
            issues.add(new ValidationIssue("resolved.parameters-present", "$/parameters",
                    "A materialized scenario must not contain parameter declarations"));
        }
        if (document.has("experiment")) {
            issues.add(new ValidationIssue("resolved.experiment-present", "$/experiment",
                    "An executable side must not contain the pair-level experiment block"));
        }
        findUnresolvedTemplates(document, "$", issues);
        if (!issues.isEmpty()) {
            throw new DocumentValidationException(validated.source(), issues);
        }
        return validated;
    }

    private ObjectNode parse(Path source) {
        if (!Files.isRegularFile(source)) {
            throw failure(source, "document.not-found", "$", "File does not exist or is not a regular file");
        }

        try (InputStream input = Files.newInputStream(source);
             MappingIterator<JsonNode> documents = yamlMapper.readerFor(JsonNode.class).readValues(input)) {
            if (!documents.hasNextValue()) {
                throw failure(source, "document.empty", "$", "Document must not be empty");
            }
            JsonNode parsed = documents.nextValue();
            if (documents.hasNextValue()) {
                throw failure(source, "document.multiple-documents", "$",
                        "Exactly one YAML document is allowed per file");
            }
            if (!(parsed instanceof ObjectNode objectNode)) {
                throw failure(source, "document.root-not-object", "$", "Document root must be an object");
            }
            return objectNode;
        } catch (DocumentValidationException exception) {
            throw exception;
        } catch (JsonProcessingException exception) {
            JsonLocation location = exception.getLocation();
            String path = location == null
                    ? "$"
                    : "$ (line " + location.getLineNr() + ", column " + location.getColumnNr() + ")";
            throw new DocumentValidationException(source,
                    new ValidationIssue("document.invalid-yaml", path, exception.getOriginalMessage()), exception);
        } catch (IOException exception) {
            throw new DocumentValidationException(source,
                    new ValidationIssue("document.io-error", "$", exception.getMessage()), exception);
        }
    }

    private String requiredText(ObjectNode document, Path source, String field, String missingCode) {
        JsonNode value = document.get(field);
        if (value == null || value.isNull()) {
            throw failure(source, missingCode, "$/" + field, "Required field is missing");
        }
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw failure(source, "document.invalid-" + field, "$/" + field,
                    "Field must be a non-empty string");
        }
        return value.textValue();
    }

    private void validateSchema(Path source, DocumentKind kind, ObjectNode document) {
        List<ValidationIssue> issues = schemas.get(kind).validate(document).stream()
                .map(SpecificationLoader::toIssue)
                .sorted((left, right) -> {
                    int pathComparison = left.path().compareTo(right.path());
                    return pathComparison != 0 ? pathComparison : left.message().compareTo(right.message());
                })
                .toList();
        if (!issues.isEmpty()) {
            throw new DocumentValidationException(source, issues);
        }
    }

    private Map<DocumentKind, Schema> loadSchemas() {
        SchemaRegistry registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
        ObjectMapper jsonMapper = new ObjectMapper();
        Map<DocumentKind, Schema> loadedSchemas = new EnumMap<>(DocumentKind.class);

        for (DocumentKind kind : DocumentKind.values()) {
            try (InputStream input = SpecificationLoader.class.getResourceAsStream(kind.schemaResource())) {
                if (input == null) {
                    throw new IllegalStateException("Missing schema resource " + kind.schemaResource());
                }
                JsonNode schemaDocument = jsonMapper.readTree(input);
                loadedSchemas.put(kind, registry.getSchema(schemaDocument));
            } catch (IOException exception) {
                throw new IllegalStateException("Cannot load schema resource " + kind.schemaResource(), exception);
            }
        }
        return Map.copyOf(loadedSchemas);
    }

    private static ValidationIssue toIssue(Error error) {
        return new ValidationIssue(schemaCode(error.getKeyword()), schemaPath(error.getInstanceLocation()),
                error.getMessage());
    }

    private static String schemaCode(String keyword) {
        return switch (keyword == null ? "" : keyword) {
            case "additionalProperties" -> "schema.additional-properties";
            case "allOf" -> "schema.all-of";
            case "anyOf" -> "schema.any-of";
            case "const" -> "schema.const";
            case "enum" -> "schema.enum";
            case "maxItems" -> "schema.max-items";
            case "maxLength" -> "schema.max-length";
            case "maxProperties" -> "schema.max-properties";
            case "maximum" -> "schema.maximum";
            case "minItems" -> "schema.min-items";
            case "minLength" -> "schema.min-length";
            case "minProperties" -> "schema.min-properties";
            case "minimum" -> "schema.minimum";
            case "not" -> "schema.not";
            case "oneOf" -> "schema.one-of";
            case "pattern" -> "schema.pattern";
            case "required" -> "schema.required";
            case "type" -> "schema.type";
            case "uniqueItems" -> "schema.unique-items";
            default -> "schema.violation";
        };
    }

    private static String schemaPath(NodePath location) {
        if (location == null || location.getNameCount() == 0) {
            return "$";
        }
        StringBuilder path = new StringBuilder("$");
        for (int index = 0; index < location.getNameCount(); index++) {
            path.append('/').append(escapeJsonPointerElement(String.valueOf(location.getElement(index))));
        }
        return path.toString();
    }

    private static String escapeJsonPointerElement(String element) {
        return element.replace("~", "~0").replace("/", "~1");
    }

    private static LoadedSpecification createSpecification(
            Path source, DocumentKind kind, ObjectNode document) {
        return switch (kind) {
            case SCENARIO -> new ScenarioSpecification(source, document);
            case EXPECTED_RESULT -> new ExpectedResultSpecification(source, document);
            case SUITE -> new SuiteSpecification(source, document);
        };
    }

    private static DocumentValidationException failure(Path source, String code, String path, String message) {
        return new DocumentValidationException(source, new ValidationIssue(code, path, message));
    }

    private static String supportedKinds() {
        return Arrays.stream(DocumentKind.values())
                .map(DocumentKind::value)
                .sorted()
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();
    }

    private static void findUnresolvedTemplates(
            JsonNode node, String path, List<ValidationIssue> issues) {
        if (node.isTextual() && node.textValue().contains("${")) {
            issues.add(new ValidationIssue("resolved.unresolved-template", path,
                    "Materialized value still contains parameter template syntax"));
            return;
        }
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldPath = path + "/" + escapeJsonPointerElement(field.getKey());
                if (field.getKey().contains("${")) {
                    issues.add(new ValidationIssue("resolved.unresolved-template", fieldPath,
                            "Materialized map key still contains parameter template syntax"));
                }
                findUnresolvedTemplates(field.getValue(), fieldPath, issues);
            }
        } else if (node.isArray()) {
            for (int index = 0; index < node.size(); index++) {
                findUnresolvedTemplates(node.get(index), path + "/" + index, issues);
            }
        }
    }
}
