package org.savonitar.flink.stability.core.spec;

import java.math.BigInteger;
import java.util.Objects;

/** One required clean plain or candidate attempt within a suite invocation. */
public final class SuiteRunSlot {
    private final SuiteEntryIdentity entry;
    private final ScenarioSide side;
    private final BigInteger ordinal;

    SuiteRunSlot(
            SuiteEntryIdentity entry,
            ScenarioSide side,
            BigInteger ordinal) {
        this.entry = Objects.requireNonNull(entry, "entry");
        this.side = Objects.requireNonNull(side, "side");
        this.ordinal = Objects.requireNonNull(ordinal, "ordinal");
        if (side == ScenarioSide.BASELINE) {
            throw new IllegalArgumentException(
                    "Experiment baselines are runtime orchestration, not required clean-run slots");
        }
        if (ordinal.signum() <= 0) {
            throw new IllegalArgumentException("ordinal must be positive");
        }
    }

    public SuiteEntryIdentity entry() {
        return entry;
    }

    public ScenarioSide side() {
        return side;
    }

    public BigInteger ordinal() {
        return ordinal;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SuiteRunSlot that
                && entry.equals(that.entry)
                && side == that.side
                && ordinal.equals(that.ordinal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entry, side, ordinal);
    }

    @Override
    public String toString() {
        return "SuiteRunSlot[entry=" + entry + ", side=" + side
                + ", ordinal=" + ordinal + ']';
    }
}
