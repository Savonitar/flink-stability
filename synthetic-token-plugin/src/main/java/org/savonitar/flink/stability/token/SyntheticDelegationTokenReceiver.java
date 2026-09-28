package org.savonitar.flink.stability.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenReceiver;

/** Acknowledges only tokens actually delivered through Flink's receiver SPI. */
public final class SyntheticDelegationTokenReceiver implements DelegationTokenReceiver {
    private TokenServiceClient client;

    @Override
    public String serviceName() {
        return TokenServiceClient.SERVICE;
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        client = new TokenServiceClient(configuration);
        client.request("/init-receiver", new byte[0]);
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        TokenServiceClient.expiresAt(tokens);
        client.request("/receipt", tokens.clone());
    }
}
