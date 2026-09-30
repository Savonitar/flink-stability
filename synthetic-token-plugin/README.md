# Synthetic delegation-token fixture

This thin plugin implements Flink's provider and receiver SPIs. The engine installs
it under `/opt/flink/plugins/flink-stability-token/`, separately from the subject
connector. Flink supplies the API classes; the JAR has no bundled dependencies and
targets Java 17.

The provider obtains only synthetic tokens from the engine's loopback HTTP fixture,
reached from Docker through its explicit host tunnel. Version-2 tokens contain a
fixture UUID, sequence, issuance/expiration timestamps, provider instance and an
immutable registration snapshot. They authorize access to nothing.
The receiver validates this format and acknowledges the exact bytes actually
delivered by Flink. Neither implementation reads credential stores, environment
credentials, cloud metadata or user files.

The engine supplies `flink-stability.token-service.endpoint`, `.process` and `.role`.
Process labels include the logical slot and incarnation, such as `taskmanager-2#1`.
Both SPIs report initialization with separate instance UUIDs. Provider identity
is retained in issuance/receipt evidence separately from the receiving instance.
HTTP requests have fixed 5-second connect and
60-second read bounds, bypass proxies and reject redirects. HTTP 503 causes an
acquisition exception; fixture status 598 causes an acquisition `LinkageError`.
These faults are deliberately injected observations, never hidden successful tokens.
For either status, the response carries synthetic request and revision identifiers.
The client reports the status it received to `/fault-observed` before raising the
acquisition error. The service accepts this bounded acknowledgement only for the
same process, provider instance, request, revision and recorded status, retaining a `FAULT_OBSERVED`
event. An acknowledgement failure is suppressed on the original acquisition error;
it does not replace that error or retry acquisition. Proof requires both the
original failed request and the matching client acknowledgement, without a
transport-delivery failure. A recorded server response alone cannot establish that
the plugin received the injected error.

The host service supports healthy, delayed (1 ms through 30 s), failing and linkage-error acquisition.
Changing mode releases an existing delay; shutdown cancels outstanding requests.
Its immutable snapshots retain event order, wall and monotonic timestamps, request
identity, configuration revision, issuance/receipt identity and concurrency counts.
Request state is captured before a delay or failure is selected; a later job
registration cannot relabel a failed bootstrap request as per-job fault exposure.
Overflow or worker saturation marks the evidence incomplete and rejects subsequent
requests. Raw events and class-loading evidence must be preserved by the caller.

The common plugin compiles against Flink 2.2. It keeps legacy `init(Configuration)`
and declares public `registerJob(JobID, Configuration)`, `unregisterJob(JobID)` and
`close()` methods without linking the callback API. A runtime whose provider SPI
adds those hooks can dispatch to the same binary. Its default callback-aware init
delegates to legacy init; the plugin does not request callback-triggered obtains.
Actual API compatibility and class origins must be verified for each selected
runtime. Concrete-method unit tests alone do not prove runtime interface dispatch.

The provider supports one live workload job and reads only its actual JobID and
`flink-stability.workload.v1.job-alias`. Identical registration is idempotent for
state/generation; each hook occurrence has a separate journal sequence. Matching
unregister removes that job and records its identity; unknown or already-removed
IDs are a no-op. Hooks perform bounded in-memory work with no HTTP or token wait.
Conflicting aliases, a second live job or journal overflow permanently invalidate
coverage while retaining the existing state for diagnostics.

Each obtain captures state and unacknowledged journal together, then performs HTTP
outside the state lock. The service deduplicates identical provider/sequence
records and rejects gaps or conflicting duplicates. The response acknowledges only
the contiguous prefix sent by that request, so delayed responses cannot erase later
hook records. Failed obtains retain the prefix for retransmission. The journal is
bounded at 32 entries, aliases at 128 UTF-8 bytes and bodies at 16,384 bytes. Service
ledgers are capped at 128 provider instances and 10,000 journal records.

With no registered job, obtains return an explicit `BOOTSTRAP` token with finite
expiry, allowing periodic renewal to continue before registration. Registered jobs
produce `JOB` tokens naming the captured job, alias and generation. Both scopes use
the same expiry policy. The receiver validates synthetic format and acknowledges
exact bytes; service-wide delivery does not enforce cross-job secrecy or isolation.

An optional runtime token `proof_scope` declares `bootstrap` or `submitted-job`
evidence requirements. Omitting it preserves the existing service-wide evidence
contract; it does not infer per-job support from runtime versions or missing events.
The plugin/service wire format is versioned together and does not accept the old
empty acquisition request as a substitute for explicit context.

The first per-job slice covers registration/configuration, acquisition and delivery
when proven by real runtime evidence. It does not claim callback scheduling,
multi-job isolation or complete final cleanup. A close/unregister journal tail may
never be flushed after shutdown; an in-flight obtain retains its earlier snapshot.
Unit and direct API checks simulate receiver delivery and do not replace actual
JobMaster-to-ResourceManager propagation, TaskManager receipt or HA recovery runs.
