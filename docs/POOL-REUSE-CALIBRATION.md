# Pool reuse during an unfinished checkpoint

The four `pool-reuse-inflight-{kill,control}-{v1,v2}` catalogs test producer reuse
with POOLING naming against Kafka 4.0 and Flink 2.2. The workload uses 14,000 IDs,
5 ms processing delay, a 12 s checkpoint interval and an 8 s asynchronous snapshot
delay. The delay wraps an asynchronous snapshot future in the source throttle
operator without blocking the mailbox or sink snapshot. Jobs with zero delay use
the plain map operator. Both release and mutant use the harness default transaction
timeout of 7,200,000 ms (two hours). The [harness setting](../core/src/main/java/org/savonitar/flink/stability/core/execution/plan/ExecutableScenarioPlan.java)
explicitly overrides the released
[connector's one-hour default](https://github.com/apache/flink-connector-kafka/blob/v5.0.0/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/KafkaSinkBuilder.java#L65).

The [single-class mutant recipe](../calibration/pool-reuse-mutant/README.md) skips
session reinitialization only when a recycled producer is requested with the same
non-null transactional ID. Release and mutant each have a no-kill control for
both Kafka transaction versions. All catalogs expect PASS; a duplicate-ID failure
from the deliberately broken TV1 connector demonstrates detection by the oracle.

## Window and qualification

`checkpoint-in-progress` observes checkpoint 2 after checkpoint 1 completes. The
fault records the sink acknowledgement, checkpoint overview, broker producer and
transaction state, and JobManager clock bounds around the physical TaskManager
kill. The [recipe's calibration contract](../calibration/pool-reuse-mutant/README.md#calibration-contract)
also requires the prior transaction to commit before checkpoint 2, at least
3,000 ms of asynchronous work remaining at the kill, 200–900 contiguous reused
records, and recovery before transaction expiry.

These bounds are calibration parameters chosen for this workload. They are not
general Flink or Kafka correctness limits, and the retained batch is not an
independent validation of their suitability. `qualified: true` means the supplied
evidence satisfies this recipe's checks; it does not assert acceptance against an
external specification. A raw data PASS alone cannot qualify a fault run.

Unexpected data failures retain FAIL and their original reason even when window
evidence is missing. A successful observation that misses the checkpoint window
uses `checkpoint-window.missed`. Failed REST polls use
`await.checkpoint-in-progress.infrastructure`; failed observations around the kill
use `checkpoint-window.observation-infrastructure`, retaining the cause and REST
error evidence. See [SPEC-001](specs/SPEC-001-scenario-schema.md), R6.4b–c and R8.7a.

Broker observations select the sink's cluster explicitly and enumerate every
declared sink partition. The checker accepts historical single-object receipts
and current arrays, while requiring exactly one partition for these calibration
catalogs. The runner currently supports one Kafka cluster; mismatched or missing
cluster endpoints are rejected.

## Observed results

The retained batch contains 20 attempts: four controls, two pilots, twelve initial
fault runs and two supplemental TV2 fault runs. Pilots are excluded from the
repetition counts. The evidence audit satisfies the recipe's checks:

| Subject | TV1 fault | TV2 fault | No-kill controls |
| --- | --- | --- | --- |
| Release | 3 confirmed PASS | 3 confirmed PASS; 1 window miss | PASS for both versions |
| Same-ID reuse mutant | 3 confirmed duplicate-ID FAIL | 3 confirmed PASS with fenced stale resume; 1 window miss | PASS for both versions |

TV1 mutant failures contain contiguous duplicate blocks of 608, 688 and 438 IDs,
each on exactly one reused transactional ID and producer session. TV2 stale
resumes were fenced; no unresolved transactions were observed. Passing release
fault runs in both versions also emitted fencing events. Those component events
remain relevant even when the data oracle passes.

Two initial TV2 attempts had only 2.660 and 2.686 s of remaining asynchronous work
and remain inconclusive for calibration. The supplemental TV2 pair used a 1 s
post-trigger wait and confirmed both windows. Current TV1 uses 2 s and TV2 uses
1 s. Confirmed fault runs restored checkpoint 1 within 11.322–18.273 s after the
kill, before the harness-configured 7,200,000 ms transaction timeout.

Raw logs, frozen artifacts and per-attempt reports are retained locally and are
not distributed with this repository. The results describe that retained batch;
the current harness's verdict and observation changes have regression coverage
but have not been exercised in a new live batch. Timing varies between attempts,
so these results do not guarantee that every future run will hit the window.

## Running and retaining evidence

Select the catalogs explicitly with repeated `--scenario` options as described in
[PR testing](PR-TESTING.md), using a local connector JAR and its runtime
dependencies. Run the recipe checker over the retained checkpoint, broker, log
and artifact evidence. Keep missed attempts and their original verdicts alongside
confirmed runs; write each audit to a fresh output file.

Keep validation builds in a separate Maven cache and output directory from frozen
experiments. Reproduction requires the recorded harness and connector hashes;
mutable Maven SNAPSHOT artifacts do not preserve a historical harness version.
