# HA and delegation-token fault scenarios

This engine slice adds a ZooKeeper-backed pair of JobManagers and a synthetic
delegation-token provider/receiver installed as a real Flink plugin. It is intended
to expose recovery, acquisition and distribution behavior during leadership changes.
It does not modify Flink or put upstream-only unit tests in the engine.

```yaml
setup:
  flink:
    image: flink:2.2.0
    jobmanagers: 2
    taskmanagers: 2
    high_availability:
      zookeeper_image: zookeeper:3.9.3
      session_timeout: 6s
    token_provider:
      renewal_interval: 2s
      retry_backoff: 2s
```

The workload must use filesystem checkpoint storage. All Flink processes share an
attempt-owned directory containing checkpoints and HA metadata. Leader discovery
reads the published ZooKeeper sessions and addresses, then matches them to unique
container incarnations. A standby HTTP response is insufficient proof of leadership.
The attempt-owned ZooKeeper fixture is anonymous; HA Flink processes explicitly
disable ZooKeeper SASL, and authenticated ZooKeeper is outside this fixture's coverage.

One operation selects the current leader, applies its fault, holds it and heals it:

```yaml
- leader_fault:
    mode: isolate-zookeeper
    duration: 15s
    timeout: 2m
    token_fault:
      mode: delay
      delay: 5s
```

Modes are `kill`, `pause` and `isolate-zookeeper`. Isolation closes the target's
existing ZooKeeper TCP connections and blocks reconnection while its JVM remains
alive. Other Flink/Kafka/token-service paths remain available. Kill recreates the
logical slot, and pause resumes the same process. Every operation retains its
application/healing evidence and errors.
The operation timeout is shared by the initial job observation, fault work,
leadership discovery and the recovered-job observation. Cleanup always
gets separate bounded budgets: five seconds to heal the token service and thirty
seconds to heal the process or network gate. Both are attempted independently;
cleanup cannot turn an expired observation deadline into a confirmed transfer.
An invocation is limited to 100 leader faults, including expanded loops. A pending
TaskManager kill must be healed first; simultaneous RM/TM loss is outside this slice.
No long-run thread/timer leak result follows from these bounded catalogs.

Optional token modes are `delay`, `fail` and `linkage-error`. They are enabled before
disrupting the leader and healed after the hold interval. FAIL returns HTTP 503;
LINKAGE_ERROR makes the fixture throw `LinkageError` from token acquisition. This
does not simulate an arbitrary broken plugin dependency. Initialization and receipt
telemetry remain available while acquisition fails. The service is bound to host
loopback and exposed only through the explicit Testcontainers host tunnel.

The plugin uses Flink's 2.2 provider/receiver SPI, with no shaded Flink classes or real
credentials. Its token lifetime is twice `renewal_interval`; the Flink renewal ratio
is explicitly 0.5. Issuance, receipt, process identity, fault revision, timestamps and
request concurrency are recorded. This supports observing bursts; it does not define
a universal acceptable request rate. Saturation or truncated evidence prevents PASS.
Optional `retry_backoff` (1 second–5 minutes) sets a controlled fixed retry interval.
It sets the legacy retry-backoff key and the newer initial/max-backoff keys to the
same value; omitting it preserves the selected Flink version's defaults. The token
catalogs explicitly use two seconds so recovery can be observed within their bounded
workload. They do not test default or exponential retry timing.

Version-specific callback APIs and authenticated token use by Kafka are outside this
fixture's coverage.

The catalogs under `scenarios/ha/` retain the existing exact-ID oracle and use
3,000 records, parallelism four and four Kafka partitions. The single-fault catalogs use a 100 ms source delay. The repeated-recovery pair
uses 300 ms and a five-minute completion timeout to leave time for three independent
transfers. These are new recovery windows; existing catalogs are unchanged.

| Catalog | Fault |
| --- | --- |
| `ha-eos-control` | HA without an injected fault or token fixture |
| `ha-eos-kill` | Kill and replace the active JM |
| `ha-eos-pause` | Pause and resume the active JM |
| `ha-eos-isolate` | Disconnect the living leader from ZooKeeper |
| `ha-repeat-control` | Matched longer workload without injected faults |
| `ha-repeat-isolate` | Three independently proven leadership transfers in one job |
| `ha-token-control` | Healthy plugin acquisition/distribution |
| `ha-token-delay` | Leader isolation with slow acquisition |
| `ha-token-failure` | Leader isolation with acquisition failures |
| `ha-token-linkage` | Leader isolation with acquisition LinkageError |

After building with JDK 21, an example run is:

```bash
mvn -q exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario ha-token-delay --artifact-root . --offline"
```

The runtime uses Flink/Kafka/ZooKeeper containers and explicit host TCP/HTTP services.
The first local ten-case matrix ran on engine `7e6cef3`, Flink 2.2.0, Kafka 4.0.0,
ZooKeeper 3.9.3 and Kafka connector 5.0.0-2.2. Each catalog ran once, with unchanged
PASS expectations, workload, fault settings and completion deadlines.

| Cases | Observed result |
| --- | --- |
| HA control, token control, repeated-recovery control | PASS: exactly 3,000 IDs, no missing, duplicate, malformed or unexpected IDs |
| JM kill, pause, isolation; token delay and HTTP 503; repeated isolation | FAIL: bounded job completion timed out. Declared leadership/recovery evidence was confirmed; delay and HTTP 503 also confirmed acquisition on the new RM and healthy issuance/TM receipt after healing. |
| Token LinkageError | FAIL: bounded job completion timed out; leadership and token recovery evidence was unconfirmed. The new RM reported fatal startup after the injected acquisition error; healthy issuance later occurred on the original RM. |

The repeated-isolation case confirmed all three declared transfers with distinct
sessions. Its logs contained four restores across JobManagers, while the final
REST snapshot counted two on the final coordinator; those are different scopes.
The failed cases did not reach the terminal data oracle. They establish neither
missing records nor exact final data. Their FAIL results remain authoritative.

For the kill, pause, isolation and repeated-isolation cases, all restored source
readers finished their bounded splits but received no new `NoMoreSplits` event;
checkpoints continued without job completion. Inspection of the provisioned release
connector bytecode supports a lost enumerator completion flag after restoration and
empty partition discovery. This remains a connector hypothesis pending a targeted
reproduction and verified correction, not a reason to change the catalogs.

The four token traces had at most one concurrent acquisition and two request starts
in any one-second window, under the explicit two-second settings. These bounded
observations do not establish a general storm threshold or long-run leak behavior.
Component findings also include fenced Kafka commits, metric collisions, ZooKeeper
leader-latch errors and a shutdown-time token delivery failure. A passing data
oracle does not dismiss those observations.

All 81 recorded owned container IDs were confirmed absent. Separate cleanup
follow-ups preserved the original results; no completed catalog was replayed.
The explicit ZooKeeper SASL setting documented above was added after this matrix
to address authentication errors against the anonymous fixture. A separately
authorized control on corrected engine `97c671e` used the same Flink image and
unchanged `ha-eos-control` catalog: exact 3,000 IDs, FINISHED, no restores, and
complete runtime, class-origin and process-fence evidence. Both JMs and both TMs
loaded the explicit setting and established ZooKeeper sessions; the full log
contained none of the previous SASL/JAAS/authentication errors. All eight recorded
owned containers were confirmed absent. This verifies the anonymous-fixture
configuration fix for initial processes; replacement configuration is covered by
unit tests. The original matrix failures and other component findings remain open.

A selected experimental Flink 2.4 build still requires its existing image/runtime-JAR
pins and a matching local workload/connector closure; a release 2.2 run says nothing
about a particular upstream PR.

PASS requires exact final data plus complete experiment evidence. For leader faults,
that includes changed sessions on another physical JM, a confirmed effective fault,
healing, and recovery of the same submitted job. A requested token fault additionally
requires a request from the new RM incarnation that actually experienced the fault,
then healthy issuance/receipt. For HTTP failures, the plugin acknowledges receiving
the matching synthetic response; a server-side response attempt alone is insufficient.
Repeated operations need independent sessions and restore observations in execution
order, with append-only token traces. Missing proof stays inconclusive. Component errors and
unexpected recovery amplification remain findings even when the exact-ID oracle passes.
