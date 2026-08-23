package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

/** The complete expected-result replacement selected for one resolved invocation. */
public final class SelectedExpectation {
    private final ExpectedResultSpecification specification;
    private final ExpectationSelectionKind kind;
    private final Integer caseIndex;
    private final String originPointer;
    private final Map<String, JsonNode> conditions;
    private final ObjectNode expectation;

    SelectedExpectation(
            ExpectedResultSpecification specification,
            ExpectationSelectionKind kind,
            Integer caseIndex,
            String originPointer,
            Map<String, ? extends JsonNode> conditions,
            ObjectNode expectation) {
        this.specification = Objects.requireNonNull(specification, "specification");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.originPointer = Objects.requireNonNull(originPointer, "originPointer");
        this.expectation = Objects.requireNonNull(expectation, "expectation").deepCopy();
        Map<String, JsonNode> conditionCopy = new LinkedHashMap<>();
        Objects.requireNonNull(conditions, "conditions").forEach((name, value) -> conditionCopy.put(
                Objects.requireNonNull(name, "condition name"),
                Objects.requireNonNull(value, "condition value").deepCopy()));
        this.conditions = Collections.unmodifiableMap(conditionCopy);

        if (kind == ExpectationSelectionKind.DEFAULT) {
            if (caseIndex != null || !this.conditions.isEmpty() || !"$/default".equals(originPointer)) {
                throw new IllegalArgumentException("Default selection must have no case index or conditions");
            }
            this.caseIndex = null;
        } else {
            if (caseIndex == null || caseIndex < 0 || this.conditions.isEmpty()
                    || !originPointer.equals("$/cases/" + caseIndex)) {
                throw new IllegalArgumentException("Case selection requires matching index, pointer, and conditions");
            }
            this.caseIndex = caseIndex;
        }
    }

    public ExpectedResultSpecification specification() {
        return specification;
    }

    public ExpectationSelectionKind kind() {
        return kind;
    }

    public OptionalInt caseIndex() {
        return caseIndex == null ? OptionalInt.empty() : OptionalInt.of(caseIndex);
    }

    public String originPointer() {
        return originPointer;
    }

    public Map<String, JsonNode> conditions() {
        Map<String, JsonNode> copy = new LinkedHashMap<>();
        conditions.forEach((name, value) -> copy.put(name, value.deepCopy()));
        return Collections.unmodifiableMap(copy);
    }

    public ObjectNode expectation() {
        return expectation.deepCopy();
    }

    public ObjectNode expectationFor(ScenarioSide side) {
        Objects.requireNonNull(side, "side");
        if (side == ScenarioSide.SINGLE) {
            if (!expectation.has("outcome")) {
                throw new IllegalArgumentException("Experiment expectation has no SINGLE side");
            }
            return expectation.deepCopy();
        }
        if (expectation.has("outcome")) {
            throw new IllegalArgumentException("Plain expectation has no " + side + " side");
        }
        String field = side == ScenarioSide.BASELINE ? "baseline" : "candidate";
        return ((ObjectNode) expectation.get(field)).deepCopy();
    }
}
