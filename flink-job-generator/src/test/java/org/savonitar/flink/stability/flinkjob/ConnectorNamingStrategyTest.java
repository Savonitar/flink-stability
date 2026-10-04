package org.savonitar.flink.stability.flinkjob;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.savonitar.flink.stability.flinkjob.WorkloadProtocolV1Configuration.TransactionIdNamingStrategy.*;

class ConnectorNamingStrategyTest {
    @Test
    void connectorDefaultWorksWithoutTheExtensionAndNeverCallsItWhenPresent() {
        assertDoesNotThrow(() -> ConnectorNamingStrategy.configure(new Object(), CONNECTOR_DEFAULT));
        ExtensionBuilder builder = new ExtensionBuilder();
        ConnectorNamingStrategy.configure(builder, CONNECTOR_DEFAULT);
        assertNull(builder.selected);
    }

    @Test
    void explicitChoiceCallsTheAvailableConnectorExtension() {
        ExtensionBuilder builder = new ExtensionBuilder();
        ConnectorNamingStrategy.configure(builder, POOLING);
        assertEquals(Strategy.POOLING, builder.selected);
        ConnectorNamingStrategy.configure(builder, INCREMENTING);
        assertEquals(Strategy.INCREMENTING, builder.selected);
    }

    @Test
    void absentMethodOrEnumValueFailsClearlyWithoutSubstitutingAnotherStrategy() {
        for (Object builder : new Object[]{new Object(), new PartialBuilder()}) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> ConnectorNamingStrategy.configure(builder, POOLING));
            assertTrue(failure.getMessage().contains("POOLING"));
            assertTrue(failure.getMessage().contains("connector-default"));
        }
    }

    @Test
    void preservesTheConnectorsOwnRejection() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> ConnectorNamingStrategy.configure(new RejectingBuilder(), POOLING));
        assertEquals("custom connector rejection", failure.getCause().getMessage());
    }

    public enum Strategy { INCREMENTING, POOLING }
    public enum PartialStrategy { INCREMENTING }

    public static class ExtensionBuilder {
        Strategy selected;
        public ExtensionBuilder setTransactionNamingStrategy(Strategy value) {
            selected = value;
            return this;
        }
    }

    public static class PartialBuilder {
        public PartialBuilder setTransactionNamingStrategy(PartialStrategy value) { return this; }
    }

    public static class RejectingBuilder {
        public RejectingBuilder setTransactionNamingStrategy(Strategy value) {
            throw new IllegalStateException("custom connector rejection");
        }
    }
}
