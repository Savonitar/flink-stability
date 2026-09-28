package org.savonitar.flink.stability.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;

import java.util.Optional;

/** Reusable Flink SPI fixture for acquisition, renewal and error recovery system tests. */
public final class SyntheticDelegationTokenProvider implements DelegationTokenProvider {
    private TokenServiceClient client;

    @Override
    public String serviceName() {
        return TokenServiceClient.SERVICE;
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        client = new TokenServiceClient(configuration);
        client.request("/init-provider", new byte[0]);
    }

    @Override
    public boolean delegationTokensRequired() {
        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        byte[] token = client.request("/token", new byte[0]);
        return new ObtainedDelegationTokens(token, Optional.of(TokenServiceClient.expiresAt(token)));
    }
}
