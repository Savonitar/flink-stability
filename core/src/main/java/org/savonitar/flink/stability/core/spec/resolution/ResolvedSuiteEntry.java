package org.savonitar.flink.stability.core.spec.resolution;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import org.savonitar.flink.stability.core.spec.document.SuiteEntryIdentity;

/** Fully preflighted scenario invocation plus its suite-owned repetition policy. */
public final class ResolvedSuiteEntry {
    private final SuiteEntryIdentity identity;
    private final ResolvedScenarioPlan scenarioPlan;
    private final BigInteger declaredRuns;
    private final BigInteger suiteRunsOverride;
    private final BigInteger effectiveRuns;
    private final ScenarioSide repeatedSide;

    ResolvedSuiteEntry(
            SuiteEntryIdentity identity,
            ResolvedScenarioPlan scenarioPlan,
            BigInteger suiteRunsOverride) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.scenarioPlan = Objects.requireNonNull(scenarioPlan, "scenarioPlan");
        this.declaredRuns = declaredRuns(scenarioPlan);
        this.suiteRunsOverride = suiteRunsOverride == null
                ? null
                : positive(suiteRunsOverride, "suiteRunsOverride");
        this.effectiveRuns = this.suiteRunsOverride == null
                ? this.declaredRuns
                : this.suiteRunsOverride;
        this.repeatedSide = scenarioPlan.scenario().isExperiment()
                ? ScenarioSide.CANDIDATE
                : ScenarioSide.SINGLE;

        if (!identity.scenarioName().equals(scenarioPlan.scenario().template().name())) {
            throw new IllegalArgumentException(
                    "Suite entry scenario does not match its resolved scenario plan");
        }
    }

    private static BigInteger declaredRuns(ResolvedScenarioPlan plan) {
        ScenarioSide side = plan.scenario().isExperiment()
                ? ScenarioSide.CANDIDATE
                : ScenarioSide.SINGLE;
        return positive(
                plan.scenario().side(side).document().path("runs").bigIntegerValue(),
                "resolved scenario runs");
    }

    public SuiteEntryIdentity identity() {
        return identity;
    }

    public ResolvedScenarioPlan scenarioPlan() {
        return scenarioPlan;
    }

    /** The resolved scenario-owned K before an explicit suite-entry replacement. */
    public BigInteger declaredRuns() {
        return declaredRuns;
    }

    public Optional<BigInteger> suiteRunsOverride() {
        return Optional.ofNullable(suiteRunsOverride);
    }

    public BigInteger effectiveRuns() {
        return effectiveRuns;
    }

    public ScenarioSide repeatedSide() {
        return repeatedSide;
    }

    /**
     * Returns a fresh lazy view of required clean attempts. Dirty retries and experiment
     * baselines remain runtime decisions within this one invocation.
     */
    public Iterable<SuiteRunSlot> requiredCleanRunSlots() {
        return () -> new Iterator<>() {
            private BigInteger next = BigInteger.ONE;

            @Override
            public boolean hasNext() {
                return next.compareTo(effectiveRuns) <= 0;
            }

            @Override
            public SuiteRunSlot next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                SuiteRunSlot slot = new SuiteRunSlot(identity, repeatedSide, next);
                next = next.add(BigInteger.ONE);
                return slot;
            }
        };
    }

    private static BigInteger positive(BigInteger value, String name) {
        Objects.requireNonNull(value, name);
        if (value.signum() <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
