package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Shared declaration and literal-value rules for scenario parameters. */
final class ScenarioParameterContract {
    private static final Pattern DURATION = Pattern.compile("[1-9][0-9]*(ms|s|m|h)");

    private ScenarioParameterContract() {}

    static Map<String, Definition> definitions(ObjectNode scenario) {
        Map<String, Definition> definitions = new LinkedHashMap<>();
        JsonNode parameters = scenario.get("parameters");
        if (!(parameters instanceof ObjectNode parameterObject)) {
            return definitions;
        }
        parameterObject.fields().forEachRemaining(entry -> {
            ObjectNode declaration = (ObjectNode) entry.getValue();
            definitions.put(entry.getKey(), new Definition(
                    entry.getKey(),
                    Type.from(declaration.path("type").textValue()),
                    declaration.get("default"),
                    declaration.path("required").asBoolean(false),
                    declaration.has("min") ? declaration.get("min").bigIntegerValue() : null,
                    declaration.has("max") ? declaration.get("max").bigIntegerValue() : null));
        });
        return definitions;
    }

    static List<ValueProblem> validate(Definition definition, JsonNode value) {
        List<ValueProblem> problems = new ArrayList<>();
        if (containsTemplate(value)) {
            problems.add(new ValueProblem("recursive-expansion",
                    "Parameter values may not contain templates: " + value));
            return problems;
        }

        boolean validType = switch (definition.type()) {
            case STRING -> value.isTextual();
            case INTEGER -> value.isIntegralNumber();
            case BOOLEAN -> value.isBoolean();
            case DURATION -> value.isTextual() && DURATION.matcher(value.textValue()).matches();
        };
        if (!validType) {
            String code = definition.type() == Type.DURATION && value.isTextual()
                    ? "invalid-duration"
                    : "type-mismatch";
            problems.add(new ValueProblem(code,
                    "Parameter '" + definition.name() + "' must be " + definition.type().wireName()));
            return problems;
        }

        if (definition.type() == Type.INTEGER) {
            BigInteger integer = value.bigIntegerValue();
            if (definition.minimum() != null && integer.compareTo(definition.minimum()) < 0) {
                problems.add(new ValueProblem("out-of-range",
                        "Parameter '" + definition.name() + "' must be at least " + definition.minimum()));
            }
            if (definition.maximum() != null && integer.compareTo(definition.maximum()) > 0) {
                problems.add(new ValueProblem("out-of-range",
                        "Parameter '" + definition.name() + "' must be at most " + definition.maximum()));
            }
        }
        return List.copyOf(problems);
    }

    private static boolean containsTemplate(JsonNode value) {
        if (value.isTextual()) {
            return value.textValue().contains("${");
        }
        if (value.isContainerNode()) {
            Iterator<JsonNode> elements = value.elements();
            while (elements.hasNext()) {
                if (containsTemplate(elements.next())) {
                    return true;
                }
            }
        }
        return false;
    }

    enum Type {
        STRING("string"),
        INTEGER("integer"),
        BOOLEAN("boolean"),
        DURATION("duration");

        private final String wireName;

        Type(String wireName) {
            this.wireName = wireName;
        }

        String wireName() {
            return wireName;
        }

        static Type from(String value) {
            return switch (value) {
                case "string" -> STRING;
                case "integer" -> INTEGER;
                case "boolean" -> BOOLEAN;
                case "duration" -> DURATION;
                default -> throw new IllegalArgumentException("Unsupported parameter type " + value);
            };
        }
    }

    record Definition(
            String name,
            Type type,
            JsonNode defaultValue,
            boolean required,
            BigInteger minimum,
            BigInteger maximum) {}

    record ValueProblem(String code, String message) {}
}
