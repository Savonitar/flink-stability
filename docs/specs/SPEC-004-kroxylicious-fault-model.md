# SPEC-004 - Kroxylicious Fault Model v1

**Status:** draft implementation contract derived from SPEC-001.
**Depends on:** SPEC-001 R6.8-R6.12, R8.4, R12.5-R12.6.

This document defines the v1 protocol-aware Kafka network-fault model. It covers
proxy declaration, fault-step shape, supported Kafka API targets, evidence, and
healing.

The default v1 chaos path can still be process faults such as TaskManager
`kill`. This document exists so network faults are precise when they are used,
not timer-driven sleeps against an opaque TCP connection.

## 1. Goals

- **K1.1** Network faults are declared infrastructure, not ad hoc shell actions.
- **K1.2** A step cannot reference a proxy that was not declared in `setup`.
- **K1.3** Network faults are bounded and healable.
- **K1.4** Every fault produces evidence that it was armed, triggered, active,
  healed, and checked.
- **K1.5** Protocol-aware faults target Kafka APIs, brokers, topics, or
  transactional-ID prefixes. They do not rely only on port-level timing. Client
  selectors are deferred until endpoints declare stable client aliases.

## 2. Proxy setup

- **K2.1** A scenario using network faults declares proxies under
  `setup.proxies`.
- **K2.2** A v1 Kroxylicious proxy has:

  ```yaml
  setup:
    proxies:
      kafka-proxy:
        type: kroxylicious
        cluster: main
        listen: kafka-proxy:9092
        bootstrap: { cluster: main }

  workload:
    jobs:
      - alias: eos-job
        source: { cluster: main, topic: input }
        sink:
          cluster: main
          topic: output
          connect_via_proxy: kafka-proxy
  ```

- **K2.3** `type` is required. v1 accepts `kroxylicious` for protocol-aware
  Kafka faults. A future coarse TCP proxy may use a separate `type`.
- **K2.4** `cluster` names a declared Kafka cluster.
- **K2.5** `listen` is the broker address exposed to the Flink job and harness
  components for this attempt.
- **K2.6** `bootstrap` selects the upstream the proxy connects to. It is
  discriminated: `{ cluster: <declared-cluster> }` for a managed scenario Kafka
  cluster, or `{ address: <host:port> }` for an explicit listener. This avoids
  alias/address collisions and makes semantic validation deterministic. For
  `{ cluster: ... }`, the bootstrap cluster must equal the proxy's own `cluster`
  in v1; cross-cluster proxying is out of scope.
- **K2.6a** Resolved proxy `listen` addresses are unique within a scenario.
  Host comparison uses DNS identity: ASCII case and one or more trailing root
  dots do not distinguish listeners. Duplicate listeners are rejected because
  provisioning could not bind both routes deterministically.
- **K2.6b** An explicit `bootstrap: { address: ... }` must not equal that
  proxy's own resolved `listen` address under the same DNS-identity comparison.
  This rejects the immediate self-route cycle; other explicit addresses remain
  an intentional trust boundary and are not guessed back to managed cluster
  identities.
- **K2.7** An endpoint that should observe proxy faults declares
  `connect_via_proxy: <proxy-alias>` under its Kafka `source`, `sink`, or topic
  `input_source`. Omitted `connect_via_proxy` means the endpoint uses the direct
  broker listener. The runner must not silently rewrite a direct endpoint through
  the proxy. Semantic validation rejects a network-fault scenario where the
  affected endpoint bypasses the declared proxy or names a proxy attached to
  another cluster.
- **K2.7a** Endpoint matching follows SPEC-001 R6.8b. In short: `produce` matches
  routed workload sinks and harness input producers; `fetch` matches routed
  workload sources; `metadata` matches any routed Kafka endpoint on the target
  cluster; transaction APIs match exactly-once sinks and may further filter by
  transactional-ID prefix and source topic for offset-enlistment APIs.
- **K2.8** Each attempt gets isolated proxy rule names and proxy state. Rules
  from one attempt must not survive into another.

## 3. Fault step shape

- **K3.1** The v1 network-fault step is named `network_fault`.
- **K3.2** The step has this shape:

  ```yaml
  - network_fault:
      proxy: kafka-proxy
      target:
        cluster: main
        broker: broker-1
      match:
        api: produce
        topic: output
      fault:
        type: delay
        latency: 5s
      duration: 30s
      heal: restore-proxy-rule
  ```

- **K3.3** `proxy` is required and names `setup.proxies.<name>`.
- **K3.4** `target` is required. v1 target selectors are static:
  `cluster` and optional `broker`. Topic and transactional-ID-prefix selection
  live only under `match` in v1, so there is one canonical selector location.
  Client selection is out of scope until endpoints declare stable client aliases.
- **K3.5** Runtime target selectors are out of scope for v1.
- **K3.6** `match` is required for protocol-aware faults. It selects Kafka API
  traffic.
- **K3.7** `fault` is required and declares what the proxy does to matching
  traffic.
- **K3.8** `duration` is required for held network faults (`delay`,
  `disconnect`, `error-response`) and uses the SPEC-001 duration grammar. It is
  not accepted for a counted fault (K3.11).
- **K3.9** `heal` is required and must be `restore-proxy-rule` in v1.
- **K3.10** A held network fault is a held fault under SPEC-001 R6.9. A counted
  fault is bounded by its occurrences and trigger deadline instead.
- **K3.11** `drop-request` and `drop-response` are **counted** faults. They affect
  the first `occurrences` matching messages after the rule is armed, then the
  rule heals:

  ```yaml
  - network_fault:
      proxy: kafka-proxy
      target: { cluster: main }
      match: { api: end-txn, result: commit, transactional_id_prefix: eos }
      fault: { type: drop-response }
      occurrences: 1          # positive integer
      trigger_deadline: 2m    # SPEC-001 duration grammar
      heal: restore-proxy-rule
  ```

  `occurrences` and `trigger_deadline` are required for a counted fault and
  rejected for a held one. The step waits until every occurrence has completed or
  the trigger deadline has passed, and heals the rule either way. A counted fault
  is how a scenario loses *one* specific message, such as the commit response of
  the canonical example, instead of a time window of traffic.

## 4. Supported Kafka API matches

- **K4.1** v1 API names are lowercase stable aliases. The implementation maps
  them to Kafka protocol API keys.
- **K4.2** v1 supports these API aliases:

  | Alias | Use |
  | --- | --- |
  | `produce` | Sink writes and source producer writes. |
  | `fetch` | Consumer reads. |
  | `metadata` | Topic and partition metadata lookup. |
  | `init-producer-id` | Transactional producer initialization. |
  | `add-partitions-to-txn` | Transactional sink partition enlistment. |
  | `add-offsets-to-txn` | Source offset transaction enlistment. |
  | `txn-offset-commit` | Transactional offset commit. |
  | `end-txn` | Transaction commit or abort. |

- **K4.2a** The semantic endpoint and topic-selector profiles are fail-closed:

  | API | Candidate endpoint | `match.topic` |
  | --- | --- | --- |
  | `produce` | workload sink or harness input producer | endpoint topic |
  | `fetch` | workload source | endpoint topic |
  | `metadata` | source, sink, or input producer | optional endpoint topic |
  | `init-producer-id` | exactly-once sink | forbidden |
  | `add-partitions-to-txn` | exactly-once sink | sink topic |
  | `add-offsets-to-txn` | exactly-once sink | forbidden |
  | `txn-offset-commit` | exactly-once sink | paired source topic on the target cluster |
  | `end-txn` | exactly-once sink | forbidden |

  `match.transactional_id_prefix` is accepted only for the five transaction
  APIs and, when present, equals the sink's configured prefix exactly.
  The distinction between `add-offsets-to-txn` and `txn-offset-commit` follows
  their request fields in the
  [Apache Kafka 4.0 protocol reference](https://kafka.apache.org/40/design/protocol/).

- **K4.3** A scenario naming an unsupported API alias is rejected before
  provisioning.
- **K4.4** API matching may be combined with a static broker target plus topic
  or transactional-ID-prefix matches. v1 does not accept client selectors.
- **K4.5** API aliases are semantic names, not raw numeric Kafka API keys, so
  scenario files do not change when Kafka protocol numbers are hidden behind
  client libraries.
- **K4.6** `match.result: commit | abort` selects `end-txn` requests by the
  outcome the client asks for. It is accepted only for `end-txn`; omitted, both
  outcomes match.

## 5. Fault types

- **K5.1** v1 supports these protocol-aware fault types:

  | Type | Required fields | Effect |
  | --- | --- | --- |
  | `delay` | `latency` | Delay matching requests or responses. |
  | `disconnect` | none | Close matching connections. |
  | `error-response` | `error` | Return a Kafka error for matching requests where the API supports it. |
  | `drop-request` | none (counted, K3.11) | Neither forward nor answer a matching request. |
  | `drop-response` | none (counted, K3.11) | Forward a matching request, then discard the broker's successful response. |

- **K5.1a** A dropped request never reaches the broker; the client sees only a
  request timeout and may retry. A retry is a new message and counts as another
  occurrence while the rule is armed. `drop-response` drops only a successful
  response (error `NONE`), so a dropped response means the broker *acted* on the
  request, but the client never learns the outcome: for `end-txn` with `result:
  commit`, the transaction is committed while the committer still believes it is
  pending. An error response passes through to the client and does not use up an
  occurrence; the next matching request is claimed instead. `drop-request` catches
  code that assumes an unacknowledged commit happened; `drop-response` catches
  code that assumes it did not and writes the data again.

- **K5.2** `latency` uses the SPEC-001 duration grammar.
- **K5.3** `error` is a stable Kafka error alias such as
  `not-leader-or-follower`, `request-timed-out`, or
  `coordinator-not-available`.
- **K5.4** The implementation rejects an `error-response` for APIs where the
  selected error cannot be represented safely.
- **K5.4a** The initial v1 safe-response registry is intentionally small:

  | API | Allowed `error` values |
  | --- | --- |
  | `produce` | `not-leader-or-follower`, `request-timed-out` |
  | `fetch` | `not-leader-or-follower`, `request-timed-out` |
  | `metadata` | none |
  | `init-producer-id` | `coordinator-not-available`, `request-timed-out` |
  | `add-partitions-to-txn` | `not-leader-or-follower`, `request-timed-out` |
  | `add-offsets-to-txn` | `coordinator-not-available`, `request-timed-out` |
  | `txn-offset-commit` | `coordinator-not-available`, `request-timed-out` |
  | `end-txn` | `coordinator-not-available`, `request-timed-out` |

  Every unlisted API/error pair is rejected before provisioning. Each listed
  pair still requires adapter contract tests for the Kafka API versions pinned
  by the runner; adding support expands this registry explicitly.
  Kroxylicious can construct API-specific global or per-entity responses via
  its
  [`RequestFilterResultBuilder.errorResponse`](https://kroxylicious.io/documentation/0.21.0/javadoc/io/kroxylicious/proxy/filter/RequestFilterResultBuilder.html),
  but that generic mechanism does not replace this scenario-level compatibility
  allow-list.
- **K5.5** Faults that mutate payload bytes are out of scope for v1.
- **K5.6** Random fault probability is out of scope for v1. v1 faults are
  deterministic once armed.

## 6. Lifecycle and evidence

- **K6.1** Each fault has a unique attempt-local fault ID.
- **K6.2** The lifecycle is:

  ```text
  declared -> armed -> trigger-confirmed -> active -> healed -> evidence-checked
  ```

- **K6.3** `declared` is recorded when semantic validation accepts the step.
- **K6.4** `armed` is recorded when the proxy confirms the rule is installed.
- **K6.5** `trigger-confirmed` is recorded when the proxy observes at least one
  matching request or response affected by the rule.
- **K6.6** `active` covers the interval between trigger confirmation and heal.
- **K6.7** `healed` is recorded when the proxy confirms the rule was removed or
  disabled.
- **K6.8** `evidence-checked` is recorded after the runner verifies the expected
  proxy evidence exists in the run report.
- **K6.9** A fault that never reaches both `trigger-confirmed` and
  `evidence-checked` makes the attempt `inconclusive`.
- **K6.9a** For a counted fault, `armed` is the filter's acknowledgement of the
  rule, `trigger-confirmed` is reached when evidence proves every requested
  occurrence completed (for `drop-response`, the successful response dropped)
  strictly before the trigger deadline. The filter measures the budget on its own
  monotonic clock starting when it arms the rule; harness polling latency never
  extends it or disqualifies an already completed drop. `healed` is the filter's acknowledgement that
  the rule is gone. A drop that lands after the deadline, while the rule heals, is
  reported but does not count. A passing terminal oracle with fewer completed
  occurrences than requested makes the attempt `inconclusive` with
  `network-fault.trigger-missed`; a failing oracle keeps its attempt `fail`, but
  cannot match an expected failure while fault evidence is unconfirmed (R8.7a). A rule the
  proxy rejects, or does not arm or heal within `30s`, fails the step as
  `inconclusive` with `network-fault.infrastructure`.
- **K6.10** Proxy evidence includes timestamps, proxy name, rule name, target,
  match, fault type, affected API alias, broker, endpoint identity when
  available, topic when available, transactional ID when available, and request
  count.
- **K6.11** The runner and the proxy's fault filter share one control directory
  per attempt. The runner arms a fault by moving a complete
  `rules/<fault-id>.json` into place. The internal rule carries positive integer
  `triggerDeadlineNanos` (durations beyond the representable nanosecond budget
  clamp to `Long.MAX_VALUE`). The runner heals it by deleting that file; the
  filter appends one JSON line per lifecycle event to `events/<fault-id>.jsonl`:
  `armed`, `rejected`, `request-dropped`, `request-forwarded`, `response-dropped`,
  `response-forwarded` (an error response let through), `healed`, and
  `retry-observed` (K6.12). Each line
  carries the fault ID and a timestamp. A line about a message also carries its
  claim number, which pairs a forwarded request with its response; its API
  version, correlation ID, transactional ID, producer ID and epoch, and commit
  flag; for a response, the broker's error and returned producer epoch; and, once
  the message is dropped, its occurrence number and explicit boolean
  `beforeDeadline`, measured by the filter. Missing or false deadline evidence
  never confirms a drop. After expiry the filter accepts no new claims; an
  already pending response may still be dropped, but cannot count as in time.
  A claim holds its occurrence
  while its response is pending, so no more messages are affected than requested.
  The fault ID is the step's JSON pointer without `$/`, with `/` replaced by `-`
  (`phases-1-steps-0`). The run report keeps the proxy image and every dropped
  message with the broker's answer that the client never saw.
- **K6.12** For each dropped EndTxn request or response, the filter retains the
  original request's transactional ID, producer ID, producer epoch, and commit
  flag until it records an exact matching request after that rule heals,
  or until the rule book closes. The match spans client connections and uses the
  request epoch, not an epoch returned in the dropped broker response. The
  `retry-observed` line refers to the original fault and claim and records the
  matching request's timestamp, API version, correlation ID, client ID, channel,
  and original request identity. There is at most one witness per dropped claim,
  bounding retained identities and extra events by the completed occurrences.
  A matching request may witness several preceding drops of the same identity.
  The first matching request is recorded when the event path is writable. A
  failed optional observation write leaves the witness pending for a later
  match and does not prevent normal request handling.
  This records a repeated EndTxn identity only: it does not prove forwarding,
  broker acceptance, application replay, or uniquely identify a transaction
  when the producer reuses those fields. Observation does not change routing or
  claims; another armed fault may drop the observed request. The event usually
  arrives a client request timeout after the fault step ended, so the runner reads
  the event file again after the process fence, when no Flink process can send
  again, and reports each dropped message's witness (`retryObservedAtMillis`,
  `retryAfterMillis`, `retryClientId`) with its `claim`. An unreadable file adds
  `network-fault.retry-evidence-unavailable` to the diagnostics and leaves the
  witness out. Its presence is not an additional trigger-confirmation or
  scenario-verdict requirement.

## 7. Healing

- **K7.1** The scenario-declared `heal` is the normal path.
- **K7.2** The runner also owns a best-effort `finally` heal to remove any active
  proxy rules after cancellation, timeout, or crash recovery.
- **K7.3** A failed heal makes the attempt `inconclusive` with an infrastructure
  reason code.
- **K7.4** The runner records both normal heal and finalizer heal attempts.
- **K7.5** A later attempt must start with an empty proxy rule set for its
  isolated proxy state.
- **K7.6** Healing a counted fault stops new matches only. An occurrence the proxy
  decided while the rule was armed completes: a request it forwarded still has
  its successful response dropped. When the step fails after arming, the runner deletes the
  rule before reporting the failure (K7.2).

## 8. Environment-health classification

- **K8.1** A proxy process crash is dirty health.
- **K8.2** A rule installation failure is `inconclusive`.
- **K8.3** A fault that affects traffic outside its declared target is dirty
  health unless the broader target was explicitly declared.
- **K8.4** A Kafka broker failure caused by a declared network fault is fault
  evidence only when it is causally linked to the fault ID; otherwise it is dirty
  health.
- **K8.5** Docker/network reachability failures outside the proxy rule are dirty
  health.

## 9. Out of scope for v1

- **K9.1** Nested or parallel fault orchestration.
- **K9.2** Runtime selectors such as "the broker currently hosting partition
  leader for topic X".
- **K9.3** Packet corruption or payload mutation.
- **K9.4** Probabilistic fault injection.
- **K9.5** TLS/SASL-specific proxy configuration beyond passing through a
  locally usable listener.
- **K9.6** Cross-cluster replication faults.

## 10. Implementation notes

- **K10.1** JSON Schema handles proxy and step shape.
- **K10.2** Semantic validation checks proxy references, cluster references,
  unique/non-cyclic listeners, workload endpoint `connect_via_proxy` routes,
  target broker bounds, the K4.2a endpoint/selector profile, the K5.4a safe
  error-response registry, duration grammar, and heal value.
- **K10.3** Runtime validation checks rule installation, triggering, healing, and
  evidence capture.
- **K10.4** Run reports store proxy evidence as first-class fault artifacts so
  terminal validators can distinguish a real system failure from an untriggered
  fault.

## 11. Executable subset of the first runner

The runner executes a deliberately small part of this model; everything else is
rejected before provisioning with a `runner.*` diagnostic:

- **K11.1** At most one proxy (`runner.kafka.proxy-count-unsupported`), in front
  of the one Kafka cluster with `bootstrap: { cluster: ... }`
  (`runner.kafka.proxy-bootstrap-unsupported`). Its `listen` is
  `<lower-kebab-host>:<port>` with a host no runner container uses and ten free
  ports above the bootstrap port, which the proxy advertises for brokers
  (`runner.kafka.proxy-listen-unsupported`).
- **K11.2** Only the job sink may route through the proxy. A routed job source
  or generated input is `runner.kafka.proxy-route-unsupported`.
- **K11.3** Network faults are counted `drop-request` or `drop-response` faults
  on `end-txn` (`runner.network-fault.type-unsupported`,
  `runner.network-fault.api-unsupported`) and do not appear inside loops
  (`runner.network-fault.loop-unsupported`).
- **K11.4** The proxy is Kroxylicious 0.21.0, pinned by digest, with the harness's
  own fault filter plugin (module `kroxylicious-fault-filter`) loaded from its
  classpath. Only connections that route through the proxy can be affected; the
  harness input producer, the job source, and the terminal validators always use
  the direct listener.
