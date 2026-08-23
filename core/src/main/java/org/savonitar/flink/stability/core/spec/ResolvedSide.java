package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** One fully materialized and schema-revalidated executable scenario side. */
public final class ResolvedSide {
    private final ScenarioSide side;
    private final ObjectNode document;
    private final Map<String, EffectiveParameter> effectiveParameters;
    private final Map<String, Set<String>> interpolationProvenance;
    private final List<AppliedDefault> appliedDefaults;

    ResolvedSide(
            ScenarioSide side,
            ObjectNode document,
            Map<String, EffectiveParameter> effectiveParameters,
            Map<String, Set<String>> interpolationProvenance,
            List<AppliedDefault> appliedDefaults) {
        this.side = Objects.requireNonNull(side, "side");
        this.document = Objects.requireNonNull(document, "document").deepCopy();
        this.effectiveParameters = Collections.unmodifiableMap(new LinkedHashMap<>(effectiveParameters));
        Map<String, Set<String>> provenanceCopy = new LinkedHashMap<>();
        interpolationProvenance.forEach((path, parameters) -> provenanceCopy.put(
                path, Collections.unmodifiableSet(new LinkedHashSet<>(parameters))));
        this.interpolationProvenance = Collections.unmodifiableMap(provenanceCopy);
        this.appliedDefaults = Collections.unmodifiableList(new ArrayList<>(appliedDefaults));
    }

    public ScenarioSide side() {
        return side;
    }

    public ObjectNode document() {
        return document.deepCopy();
    }

    public Map<String, EffectiveParameter> effectiveParameters() {
        return effectiveParameters;
    }

    public Map<String, Set<String>> interpolationProvenance() {
        return interpolationProvenance;
    }

    public List<AppliedDefault> appliedDefaults() {
        return appliedDefaults;
    }
}
