# Synthetic delegation-token fixture

This thin plugin implements Flink's provider and receiver SPIs. The engine installs
it under `/opt/flink/plugins/flink-stability-token/`, separately from the subject
connector. Flink supplies the API classes; the JAR has no bundled dependencies and
targets Java 17.

The provider obtains only synthetic tokens from the engine's loopback HTTP fixture,
reached from Docker through its explicit host tunnel. Tokens contain a fixture UUID,
sequence and issuance/expiration timestamps. They authorize access to nothing.
The receiver validates this format and acknowledges the exact bytes actually
delivered by Flink. Neither implementation reads credential stores, environment
credentials, cloud metadata or user files.

The engine supplies `flink-stability.token-service.endpoint`, `.process` and `.role`.
Process labels include the logical slot and incarnation, such as `taskmanager-2#1`.
Both SPIs report initialization. HTTP requests have fixed 5-second connect and
60-second read bounds, bypass proxies and reject redirects. HTTP 503 causes an
acquisition exception; fixture status 598 causes an acquisition `LinkageError`.
These faults are deliberately injected observations, never hidden successful tokens.
For either status, the response carries synthetic request and revision identifiers.
The client reports the status it received to `/fault-observed` before raising the
acquisition error. The service accepts this bounded acknowledgement only for the
same process, request, revision and recorded status, retaining a `FAULT_OBSERVED`
event. An acknowledgement failure is suppressed on the original acquisition error;
it does not replace that error or retry acquisition. Proof requires both the
original failed request and the matching client acknowledgement, without a
transport-delivery failure. A recorded server response alone cannot establish that
the plugin received the injected error.

The host service supports healthy, delayed (1 ms through 30 s), failing and linkage-error acquisition.
Changing mode releases an existing delay; shutdown cancels outstanding requests.
Its immutable snapshots retain event order, wall and monotonic timestamps, request
identity, configuration revision, issuance/receipt identity and concurrency counts.
Overflow or worker saturation marks the evidence incomplete and rejects subsequent
requests. Raw events and class-loading evidence must be preserved by the caller.

This fixture compiles against the cached Flink 2.2 SPI. It exercises acquisition,
periodic renewal, distribution, receiver initialization and receipt on 2.2 and API-
compatible runtimes. It does not implement the PR-specific callback-aware `init`,
job registration hooks or callback-triggered re-obtain. Those need a separately
compiled, explicitly pinned API adapter before their coverage can be claimed.
