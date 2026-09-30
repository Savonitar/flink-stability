package org.savonitar.flink.stability.token;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Reusable Flink SPI fixture for acquisition, renewal and error recovery system tests. */
public final class SyntheticDelegationTokenProvider implements DelegationTokenProvider {
    private TokenServiceClient client;
    private final String instance = UUID.randomUUID().toString();
    private final Object stateLock = new Object();
    private final ArrayDeque<TokenServiceClient.Lifecycle> journal = new ArrayDeque<>();
    private String jobId = "-";
    private String jobAlias = "-";
    private long generation;
    private long sequence;
    private long acknowledged;
    private boolean coverageInvalid;
    private boolean closed;

    @Override
    public String serviceName() {
        return TokenServiceClient.SERVICE;
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        TokenServiceClient initialized = new TokenServiceClient(configuration, instance);
        initialized.request("/init-provider", new byte[0]);
        synchronized (stateLock) {
            if (client != null || closed) throw new IllegalStateException("Provider already initialized or closed");
            client = initialized;
        }
    }

    @Override
    public boolean delegationTokensRequired() {
        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        final TokenServiceClient transport;
        final TokenServiceClient.RequestSnapshot snapshot;
        synchronized (stateLock) {
            if (client == null || closed) throw new IOException("Provider is not initialized or is closed");
            transport = client;
            snapshot = new TokenServiceClient.RequestSnapshot(instance,
                    jobId.equals("-") ? "BOOTSTRAP" : "JOB", generation, jobId, jobAlias,
                    coverageInvalid, acknowledged, List.copyOf(journal));
        }
        byte[] bytes = transport.request("/token", snapshot.encode());
        TokenServiceClient.Token token = TokenServiceClient.token(bytes);
        if (!token.matches(snapshot)) throw new IOException("Synthetic response does not match its request snapshot");
        synchronized (stateLock) {
            // A later request can acknowledge more while this response is in flight.
            // Never regress its ACK or remove journal entries not sent by this request.
            acknowledged = Math.max(acknowledged, token.acknowledgedSequence());
            while (!journal.isEmpty() && journal.peekFirst().sequence() <= acknowledged) journal.removeFirst();
        }
        return new ObtainedDelegationTokens(bytes, Optional.of(token.expiresAt()));
    }

    /** Compatible with the candidate API; deliberately compiled against the released SPI. */
    public void registerJob(JobID suppliedId, Configuration configuration) {
        // Parse only the bounded synthetic alias, never retain/serialize the supplied configuration.
        final String alias;
        try {
            alias = configuration == null ? null : configuration.getString(TokenServiceClient.JOB_ALIAS, "");
        } catch (RuntimeException invalidConfiguration) {
            synchronized (stateLock) { invalidate(); }
            return;
        }
        synchronized (stateLock) {
            if (closed || suppliedId == null || !TokenServiceClient.validAlias(alias)) {
                invalidate();
                return;
            }
            String id = suppliedId.toString();
            if (!jobId.equals("-") && (!jobId.equals(id) || !jobAlias.equals(alias))) {
                invalidate();
                return;
            }
            if (jobId.equals("-")) {
                advanceGeneration();
                jobId = id;
                jobAlias = alias;
            }
            append("REGISTER");
        }
    }

    /** No transport or waiting for acquisition is performed on this hook. */
    public void unregisterJob(JobID suppliedId) {
        synchronized (stateLock) {
            if (closed) return;
            if (suppliedId == null) {
                invalidate();
                return;
            }
            if (jobId.equals(suppliedId.toString())) {
                String removedId = jobId;
                String removedAlias = jobAlias;
                advanceGeneration();
                jobId = "-";
                jobAlias = "-";
                append("UNREGISTER", removedId, removedAlias);
                return;
            }
            // Unknown/already-removed IDs are an idempotent no-op; do not label the
            // current job as removed just because a different ID was supplied.
        }
    }

    /** Final journal delivery is intentionally not guaranteed after process shutdown. */
    public void close() {
        synchronized (stateLock) {
            if (closed) return;
            if (!jobId.equals("-")) advanceGeneration();
            jobId = "-";
            jobAlias = "-";
            append("CLOSE");
            closed = true;
        }
    }

    private void invalidate() {
        coverageInvalid = true;
        append("INVALID");
    }

    private void advanceGeneration() {
        if (generation == Long.MAX_VALUE) coverageInvalid = true;
        else generation++;
    }

    /** All helpers below run only under stateLock; their work and storage are bounded. */
    private void append(String kind) {
        append(kind, jobId, jobAlias);
    }

    private void append(String kind, String eventJobId, String eventAlias) {
        if (journal.size() == TokenServiceClient.MAX_JOURNAL || sequence == Long.MAX_VALUE) {
            coverageInvalid = true;
            return;
        }
        journal.addLast(new TokenServiceClient.Lifecycle(++sequence, kind, generation, eventJobId, eventAlias));
    }
}
