# Packet faults between a TaskManager and Kafka

The first slice operates inside the selected TaskManager's network namespace.
A short-lived sidecar joins `container:<exact TaskManager ID>` with only
`NET_ADMIN`, a read-only root filesystem and an ephemeral `/run`; it has no host
network, Docker socket or host-directory mounts. It requires an explicitly
supplied immutable `docker.io/nicolaka/netshoot@sha256:<digest>` image with `sh`,
`ip`, `tc` and `iptables`. The canonical index digest and linux/arm64 config identity are recorded in
[`packet-image-pin.json`](packet-image-pin.json). One network-none container verified
all four tools with all capabilities dropped. Docker inspect did not expose the
child platform manifest digest when the pin was recorded, including with
platform-specific inspect. Its value is `null` with status `unavailable`; the
config image ID is recorded separately. The NET_ADMIN probe has status `not-run`
and remains required before live packet stages.

The owner resolves a named broker, current partition leader or open sink
transaction coordinator using the existing Kafka Admin selector. It freezes that
selection and binds both container IDs, image IDs, the attempt network ID and the
broker's IPv4 address. Re-inspection must confirm the same running identities
before injection and counter collection. The sidecar discovers the route device
for that address; interfaces, IPs, ports and numeric fault options are validated
before command construction. No host shell input is accepted. This slice targets
the broker's internal client listener (19092); Kafka controller traffic and other
brokers are outside the rule.

An all-brokers delay binds all three broker identities before one installation,
checks every route uses the same device, and adds three exact destination filters
to one shared delay queue. Its aggregate counter delta proves queue traffic, not
that each broker individually exchanged traffic.

Packet loss (integer 1–100%) and delay (1–5000ms, jitter 0–delay) apply to outbound
TaskManager TCP traffic to that broker/port. A private `prio` qdisc sends matching
traffic to a `netem` child; other traffic uses the unaffected band. Installation
fails closed if the device has a foreign root qdisc. Blackhole is bidirectional,
using a private iptables chain with exact source/destination and port matches.
It never flushes global tables or installs a host-wide rule. Only one packet
fault may own a TaskManager namespace at a time.

One monotonic deadline bounds selection, provisioning, installation, hold and
counter reads. Normal completion and every failure/interruption attempt healing
of the same namespace with a separate bounded cleanup budget. The sidecar also
runs a watchdog with trap-based cleanup so a disconnected harness does not leave
an intentionally persistent fault. Cleanup removes only the owned qdisc/chain,
then verifies absence; it never overwrites another owner's configuration. If
ownership, counters or healing cannot be proved, evidence stays unconfirmed.
A hard Docker/host failure cannot be proved healed and must remain visible.

Evidence contains the frozen broker selection and identity binding, command
receipts, requested parameters, monotonic timing, before/after counters and heal
verification. Netem packet counters prove traffic crossed the configured delay
queue; loss additionally needs a positive dropped-packet delta. Iptables drop
counters prove blackholed traffic. Counters alone do not measure end-to-end
latency. Data validation still uses the existing exact-ID oracle; a passing
oracle cannot substitute for a missing fault receipt.

Canonical TV1/TV2 controls and packet catalogs are enabled in chaos-full only.
They use the immutable image pin and enforce the observed linux/arm64 config ID.
No live packet fault has run. The prepared live batch must first pass an isolated
NET_ADMIN tc/iptables probe; tool availability alone does not establish kernel
support or permission to manipulate a TaskManager namespace.
