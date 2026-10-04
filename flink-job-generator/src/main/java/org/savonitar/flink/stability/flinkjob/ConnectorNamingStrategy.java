package org.savonitar.flink.stability.flinkjob;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

import org.savonitar.flink.stability.flinkjob.WorkloadProtocolV1Configuration.TransactionIdNamingStrategy;

/** Optional connector extension: no link to a version-specific class or method. */
final class ConnectorNamingStrategy {
    private ConnectorNamingStrategy() {}

    static void configure(Object builder, TransactionIdNamingStrategy strategy) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(strategy, "strategy");
        if (strategy == TransactionIdNamingStrategy.CONNECTOR_DEFAULT) {
            return;
        }
        final Method setter;
        try {
            setter = Arrays.stream(builder.getClass().getMethods())
                    .filter(method -> method.getName().equals("setTransactionNamingStrategy"))
                    .filter(method -> method.getParameterCount() == 1)
                    .filter(method -> method.getParameterTypes()[0].isEnum())
                    .findFirst().orElseThrow(() -> unsupported(strategy, null));
            Object value = Arrays.stream(setter.getParameterTypes()[0].getEnumConstants())
                    .filter(item -> ((Enum<?>) item).name().equals(strategy.name()))
                    .findFirst().orElseThrow(() -> unsupported(strategy, null));
            setter.invoke(builder, value);
        } catch (InvocationTargetException failure) {
            throw new IllegalArgumentException("Connector rejected transaction_id_naming_strategy "
                    + strategy.name(), failure.getCause());
        } catch (ReflectiveOperationException | LinkageError failure) {
            throw unsupported(strategy, failure);
        }
    }

    private static IllegalArgumentException unsupported(TransactionIdNamingStrategy strategy, Throwable cause) {
        return new IllegalArgumentException("Connector does not support transaction_id_naming_strategy "
                + strategy.name() + " through setTransactionNamingStrategy; explicitly select "
                + "connector-default to use this connector's own naming behavior", cause);
    }
}
