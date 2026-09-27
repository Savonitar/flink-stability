package org.savonitar.flink.stability.core.execution.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Selects a finalized feature on a fresh attempt cluster before any workload starts. */
public final class KafkaTransactionVersion {
    public static final String FEATURE = "transaction.version";
    public static final String UNCONFIRMED = "infrastructure.kafka-transaction-version-unconfirmed";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private final Factory factory;
    private final LongSupplier nanoTime;
    private final Sleeper sleeper;

    public KafkaTransactionVersion() {
        this(KafkaAdmin::new, System::nanoTime, Thread::sleep);
    }

    KafkaTransactionVersion(Factory factory, LongSupplier nanoTime, Sleeper sleeper) {
        this.factory = Objects.requireNonNull(factory, "factory");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    public Selection select(String bootstrapServers, Optional<Integer> requested) {
        // Absence preserves the existing startup path without a new Admin dependency.
        Selection pending = new Selection(requested, List.of(), Optional.empty());
        if (requested.isEmpty()) {
            return pending;
        }
        short target = requested.orElseThrow().shortValue();
        MonotonicDeadline deadline = MonotonicDeadline.start(TIMEOUT, nanoTime);
        List<Observation> observations = new ArrayList<>();
        Operations operations = null;
        String error = null;
        try {
            operations = factory.open(bootstrapServers, remaining(deadline));
            Observation initial = operations.observe(remaining(deadline));
            observations.add(initial);
            requireUsable(initial, target);
            if (!initial.matches(target)) {
                FeatureUpdate.UpgradeType type = initial.finalized().orElseThrow().maximum() > target
                        ? FeatureUpdate.UpgradeType.SAFE_DOWNGRADE : FeatureUpdate.UpgradeType.UPGRADE;
                operations.update(target, type, true, remaining(deadline));
                operations.update(target, type, false, remaining(deadline));
                while (true) {
                    Observation previous = observations.getLast();
                    Observation observed = operations.observe(remaining(deadline));
                    if (!observed.equals(previous)) {
                        observations.add(observed);
                    }
                    requireUsable(observed, target);
                    if (observed.metadataEpoch().orElseThrow() < previous.metadataEpoch().orElseThrow()) {
                        throw new IOException("Kafka feature metadata epoch went backwards");
                    }
                    if (observed.matches(target)) {
                        break;
                    }
                    Duration left = remaining(deadline);
                    sleeper.sleep(left.compareTo(POLL_INTERVAL) < 0 ? left : POLL_INTERVAL);
                }
            }
            remaining(deadline);
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            error = failure.toString();
            if (failure.getCause() != null) {
                error += "; caused by " + failure.getCause();
            }
        } finally {
            if (operations != null) {
                try {
                    operations.close(deadline.remaining());
                    remaining(deadline);
                } catch (Exception failure) {
                    if (failure instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    error = (error == null ? "" : error + "; ") + "Admin close: " + failure;
                }
            }
        }
        return new Selection(requested, observations, Optional.ofNullable(error));
    }

    private static Duration remaining(MonotonicDeadline deadline) throws IOException {
        return deadline.remainingOrThrow(() -> new IOException("Kafka transaction.version selection timed out"));
    }

    private static void requireUsable(Observation observed, short target) throws IOException {
        if (observed.metadataEpoch().isEmpty() || observed.finalized().isEmpty()
                || observed.supported().isEmpty()) {
            throw new IOException("Kafka did not report finalized transaction.version and its metadata epoch");
        }
        Range finalized = observed.finalized().orElseThrow();
        Range supported = observed.supported().orElseThrow();
        if (finalized.minimum() != finalized.maximum()) {
            throw new IOException("Kafka transaction.version is not finalized to a single level");
        }
        if (target < supported.minimum() || target > supported.maximum()) {
            throw new IOException("Kafka does not support requested transaction.version " + target);
        }
    }

    public record Range(short minimum, short maximum) {
        public Range {
            if (minimum < 0 || maximum < minimum) {
                throw new IllegalArgumentException("Invalid Kafka feature version range");
            }
        }
    }

    public record Observation(Optional<Range> finalized, Optional<Range> supported,
                              OptionalLong metadataEpoch) {
        public Observation {
            Objects.requireNonNull(finalized, "finalized");
            Objects.requireNonNull(supported, "supported");
            Objects.requireNonNull(metadataEpoch, "metadataEpoch");
            if (metadataEpoch.isPresent() && metadataEpoch.orElseThrow() < 0) {
                throw new IllegalArgumentException("Kafka finalized metadata epoch must be nonnegative");
            }
        }

        boolean matches(int requested) {
            return metadataEpoch.isPresent() && finalized.filter(range ->
                    range.minimum() == requested && range.maximum() == requested).isPresent()
                    && supported.filter(range -> range.minimum() <= requested
                            && range.maximum() >= requested).isPresent();
        }
    }

    public record Selection(Optional<Integer> requested, List<Observation> observations,
                            Optional<String> error) {
        public Selection {
            Objects.requireNonNull(requested, "requested");
            observations = List.copyOf(observations);
            Objects.requireNonNull(error, "error");
            if (requested.isPresent() && requested.orElseThrow() != 1 && requested.orElseThrow() != 2) {
                throw new IllegalArgumentException("transaction_version must be 1 or 2");
            }
            if (requested.isEmpty() && (!observations.isEmpty() || error.isPresent())) {
                throw new IllegalArgumentException("Unrequested feature selection has no observations");
            }
        }

        public static Selection notRequested() {
            return new Selection(Optional.empty(), List.of(), Optional.empty());
        }

        public boolean confirmed() {
            return requested.isPresent() && error.isEmpty() && !observations.isEmpty()
                    && observations.getLast().matches(requested.orElseThrow());
        }

        public boolean permitsPass() {
            return requested.isEmpty() || confirmed();
        }
    }

    interface Operations {
        Observation observe(Duration timeout) throws Exception;
        void update(short version, FeatureUpdate.UpgradeType type, boolean validateOnly,
                    Duration timeout) throws Exception;
        void close(Duration timeout) throws Exception;
    }

    @FunctionalInterface
    interface Factory {
        Operations open(String bootstrapServers, Duration timeout) throws Exception;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    private static final class KafkaAdmin implements Operations {
        private final Admin admin;

        KafkaAdmin(String bootstrapServers, Duration timeout) {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            int millis = Math.max(1, Math.toIntExact(timeout.toMillis()));
            properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, millis);
            properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, millis);
            admin = Admin.create(properties);
        }

        @Override
        public Observation observe(Duration timeout) throws Exception {
            FeatureMetadata features = admin.describeFeatures().featureMetadata()
                    .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            return new Observation(Optional.ofNullable(features.finalizedFeatures().get(FEATURE))
                    .map(value -> new Range(value.minVersionLevel(), value.maxVersionLevel())),
                    Optional.ofNullable(features.supportedFeatures().get(FEATURE))
                            .map(value -> new Range(value.minVersion(), value.maxVersion())),
                    features.finalizedFeaturesEpoch().isPresent()
                            ? OptionalLong.of(features.finalizedFeaturesEpoch().orElseThrow()) : OptionalLong.empty());
        }

        @Override
        public void update(short version, FeatureUpdate.UpgradeType type, boolean validateOnly,
                           Duration timeout) throws Exception {
            admin.updateFeatures(Map.of(FEATURE, new FeatureUpdate(version, type)),
                            new UpdateFeaturesOptions().validateOnly(validateOnly)
                                    .timeoutMs(Math.max(1, Math.toIntExact(timeout.toMillis()))))
                    .all().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public void close(Duration timeout) {
            admin.close(timeout);
        }
    }
}
