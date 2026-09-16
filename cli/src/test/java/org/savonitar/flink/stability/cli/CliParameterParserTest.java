package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliParameterParserTest {
    @Test
    void preservesBooleanIntegerPlainTextAndQuotedNumericTextTypes() {
        Map<String, JsonNode> values = CliParameterParser.parse(
                List.of("enabled=true", "count=-12", "label=plain", "code=\"42\""),
                new CommandLine(new FlinkStabilityCommand()));

        assertTrue(values.get("enabled").isBoolean());
        assertTrue(values.get("enabled").booleanValue());
        assertTrue(values.get("count").isIntegralNumber());
        assertEquals(BigInteger.valueOf(-12), values.get("count").bigIntegerValue());
        assertTrue(values.get("label").isTextual());
        assertEquals("plain", values.get("label").textValue());
        assertTrue(values.get("code").isTextual());
        assertEquals("42", values.get("code").textValue());
        assertEquals(List.of("enabled", "count", "label", "code"),
                List.copyOf(values.keySet()));
    }
}
