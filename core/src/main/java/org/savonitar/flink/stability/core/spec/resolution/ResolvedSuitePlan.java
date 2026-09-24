package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.SuiteEntryIdentity;
import org.savonitar.flink.stability.core.spec.document.SuiteSpecification;
import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** An eagerly validated, source-ordered suite plan safe to hand to execution. */
public final class ResolvedSuitePlan {
    private final SuiteSpecification specification;
    private final List<ResolvedSuiteEntry> entries;

    ResolvedSuitePlan(
            SuiteSpecification specification,
            List<ResolvedSuiteEntry> entries) {
        this.specification = Objects.requireNonNull(specification, "specification");
        this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (this.entries.size() != specification.at("/scenarios").size()) {
            throw new IllegalArgumentException(
                    "Resolved suite must contain every declared entry exactly once");
        }

        Set<String> entryIds = new HashSet<>();
        for (int index = 0; index < this.entries.size(); index++) {
            ResolvedSuiteEntry entry = this.entries.get(index);
            SuiteEntryIdentity identity = entry.identity();
            if (identity.entryIndex() != index
                    || !identity.suiteSource().equals(specification.source())
                    || !identity.suiteName().equals(specification.name())) {
                throw new IllegalArgumentException(
                        "Resolved suite entries must preserve suite identity and source order");
            }
            JsonNode declaration = specification.at("/scenarios/" + index);
            String declaredScenario = declaration.path("scenario").textValue();
            String declaredId = declaration.has("as")
                    ? declaration.path("as").textValue()
                    : declaredScenario;
            if (!identity.scenarioName().equals(declaredScenario)
                    || !identity.entryId().equals(declaredId)) {
                throw new IllegalArgumentException(
                        "Resolved suite entry identity must match its source declaration");
            }
            BigInteger declaredSuiteRuns = declaration.has("runs")
                    ? declaration.path("runs").bigIntegerValue()
                    : null;
            if (!entry.suiteRunsOverride().equals(
                    Optional.ofNullable(declaredSuiteRuns))) {
                throw new IllegalArgumentException(
                        "Resolved suite run override must match its source declaration");
            }
            if (!entryIds.add(identity.entryId())) {
                throw new IllegalArgumentException(
                        "Resolved suite entry IDs must be unique: " + identity.entryId());
            }
        }
    }

    public SuiteSpecification specification() {
        return specification;
    }

    public List<ResolvedSuiteEntry> entries() {
        return entries;
    }

    public Optional<ResolvedSuiteEntry> entry(String entryId) {
        Objects.requireNonNull(entryId, "entryId");
        return entries.stream()
                .filter(entry -> entry.identity().entryId().equals(entryId))
                .findFirst();
    }

    /** Returns a fresh lazy entry-major view without deferring any validation. */
    public Iterable<SuiteRunSlot> requiredCleanRunSlots() {
        return () -> new Iterator<>() {
            private int entryIndex;
            private Iterator<SuiteRunSlot> current = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                advance();
                return current.hasNext();
            }

            @Override
            public SuiteRunSlot next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }

            private void advance() {
                while (!current.hasNext() && entryIndex < entries.size()) {
                    current = entries.get(entryIndex++).requiredCleanRunSlots().iterator();
                }
            }
        };
    }
}
