package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import picocli.CommandLine;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

final class CliParameterParser {
    private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern INTEGER = Pattern.compile("-?(0|[1-9][0-9]*)");
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private CliParameterParser() {}

    static Map<String, JsonNode> parse(
            List<String> assignments,
            CommandLine commandLine) {
        Map<String, JsonNode> values = new LinkedHashMap<>();
        for (String assignment : assignments) {
            int separator = assignment.indexOf('=');
            if (separator <= 0) {
                throw usage(commandLine,
                        "Parameter must use NAME=VALUE: '" + assignment + "'");
            }
            String name = assignment.substring(0, separator);
            String rawValue = assignment.substring(separator + 1);
            if (!NAME.matcher(name).matches()) {
                throw usage(commandLine, "Invalid parameter name '" + name + "'");
            }
            if (rawValue.isEmpty()) {
                throw usage(commandLine, "Parameter '" + name + "' has an empty value");
            }
            if (values.containsKey(name)) {
                throw usage(commandLine, "Parameter '" + name + "' is assigned more than once");
            }
            values.put(name, scalar(rawValue, name, commandLine));
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    private static JsonNode scalar(
            String rawValue,
            String name,
            CommandLine commandLine) {
        if (rawValue.equals("true") || rawValue.equals("false")) {
            return BooleanNode.valueOf(Boolean.parseBoolean(rawValue));
        }
        if (INTEGER.matcher(rawValue).matches()) {
            return JsonNodeFactory.instance.numberNode(new BigInteger(rawValue));
        }
        if (rawValue.startsWith("\"") || rawValue.endsWith("\"")) {
            try {
                JsonNode parsed = JSON.readTree(rawValue);
                if (parsed != null && parsed.isTextual()) {
                    return parsed;
                }
            } catch (IOException ignored) {
                // The stable usage error below is more useful than parser internals.
            }
            throw usage(commandLine,
                    "Quoted value for parameter '" + name + "' must be a JSON string");
        }
        return TextNode.valueOf(rawValue);
    }

    private static CommandLine.ParameterException usage(
            CommandLine commandLine,
            String message) {
        return new CommandLine.ParameterException(commandLine, message);
    }
}
