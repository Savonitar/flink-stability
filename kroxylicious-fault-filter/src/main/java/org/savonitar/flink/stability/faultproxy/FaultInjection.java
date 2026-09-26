package org.savonitar.flink.stability.faultproxy;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * The flink-stability fault filter: Kroxylicious loads it from the harness's classpath JAR and
 * creates one {@link EndTxnFaultFilter} per client connection, all sharing one rule book.
 */
@Plugin(configType = FaultInjection.Config.class)
public final class FaultInjection implements FilterFactory<FaultInjection.Config, FaultRuleBook> {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    /** The directory the harness shares with the proxy for rules and evidence. */
    public record Config(String controlDirectory) {}

    @Override
    public FaultRuleBook initialize(FilterFactoryContext context, Config config) {
        if (config == null || config.controlDirectory() == null
                || !Files.isDirectory(Path.of(config.controlDirectory()))) {
            throw new PluginConfigurationException(
                    "controlDirectory must name an existing directory");
        }
        return FaultRuleBook.start(Path.of(config.controlDirectory()), POLL_INTERVAL);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, FaultRuleBook book) {
        return new EndTxnFaultFilter(book);
    }

    @Override
    public void close(FaultRuleBook book) {
        book.close();
    }
}
