package org.savonitar.flink.stability.runtime.api;

/** A connector classpath bundle changed or could not be verified before Flink started. */
public final class ConnectorBundleProvisioningException extends IllegalStateException {
    public ConnectorBundleProvisioningException(String message) {
        super(message);
    }

    public ConnectorBundleProvisioningException(String message, Throwable cause) {
        super(message, cause);
    }
}
