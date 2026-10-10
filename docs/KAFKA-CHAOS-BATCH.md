# Serial Kafka regression batch

`tools/kafka_chaos_batch.py` prepares and resumes a serial live regression batch.
Preparation and `plan` do not start containers. `run --execute` starts Docker.
For a single Kafka PID1/TERM/shutdown check, use the separate
[Kafka lifecycle probe](KAFKA-LIFECYCLE-PROBE.md); it does not launch this batch.
Packet, parallel, at-least-once and savepoint stages follow the original
99 cells. The packet stage first requires a successful isolated NET_ADMIN probe.

| Stage | Attempts | Warm-image estimate |
| --- | ---: | ---: |
| Broker EOS control, then named broker kill | 2 | 4–10 min |
| Leader kill, leader pause, coordinator pause | 3 | 6–15 min |
| Legacy control/assume/rewrite × request/response loss | 6 | 12–30 min |
| Release/discard healthy control, twice each | 4 | 6–12 min |
| Unchanged chaos-quick, release/discard, twice each | 40 | 60–120 min |
| New POOLING/rolling controls | 6 | 12–30 min |
| New POOLING faults | 30 | 90–240 min |
| New rolling faults | 8 | 32–80 min |
| Packet controls and TV1/TV2 loss, delay/jitter, blackhole | 8 | 32–64 min |
| Parallelism-four controls and faults | 36 | 108–288 min |
| At-least-once controls and faults | 8 | 32–64 min |
| Savepoint controls, rescaling and strategy switch | 6 | 24–60 min |

Total: 157 serial attempts, roughly 418–1013 minutes (7.0–16.9 hours). These are
planning estimates, not measured timings; cold pulls add time. The wrapper gives
each process 30 minutes, retains a timeout as unresolved and stops. Scenario
deadlines still apply. A timed-out/interrupted process may require manual cleanup.

Prepare after the final JDK 21 build with a clean committed harness. The launcher
JSON contains `argv` for the direct Java CLI ending in
`org.savonitar.flink.stability.cli.Main`, using `-cp` with built JARs, plus an
explicit non-secret `environment`. Use the tested JDK, isolated Maven repository
and temporary/home directories. The two build directories hold the already
verified calibration JARs, runtime closure and `build-evidence.json`; these can
be retained builds inside the harness. Preparation checks their recipe and
artifact pins and creates new catalogs without changing canonical files.

```sh
python3 tools/kafka_chaos_batch.py plan
python3 tools/kafka_chaos_batch.py prepare --output jobs/live/kafka-chaos-batch \
  --launcher jobs/validation/batch-launcher.json \
  --legacy-build path/inside/harness/connector-mutants/target \
  --quick-build path/inside/harness/chaos-profile-mutant/target
python3 tools/kafka_chaos_batch.py status --output jobs/live/kafka-chaos-batch
```

After reviewing the frozen manifest, use this command to start or resume:

```sh
python3 tools/kafka_chaos_batch.py run --execute \
  --output jobs/live/kafka-chaos-batch --max-new-cells 157
```

The manifest freezes the source tree, engine/subject JARs, workload, runtime
dependencies, launcher and catalog hashes. A process lock prevents overlapping
launchers. Every cell has an exclusive directory, durable start/completion
records, raw stdout/stderr, physical Kafka logs and a retained checkpoint-root
reference. Existing completed cells are verified and skipped. Missing completion,
changed inputs/results, failed controls and unresolved evidence stop the batch.
There is no automatic retry or reset command; classify the observation and
prepare a separately reviewed plan if another attempt is needed. Keep all
checkpoint/log directories and never run `mvn clean` over live evidence.

Both legacy controls precede mutants. The two sensitive legacy cells retain
their actual FAIL and positive missing/duplicate counts. Matching counts alone
do not establish calibration: review matching fault/timeout/decision transaction
IDs, complete fence/oracle and class-origin evidence, and absence of
`CALIBRATION_UNSUPPORTED`. The summary explicitly leaves that correlation review
pending. The existing quick calibration checker must establish both CONTROL_PASS
and CALIBRATION_DETECTED before the new-feature stages start. All six new healthy
controls run before any new POOLING or rolling fault.

## Select the new stages

The original 99 IDs, order and calibration cells are unchanged. New stages can be
selected independently with repeated `--stage packet`, `--stage parallel`,
`--stage at-least-once` or `--stage savepoint` on `plan`, `prepare` and `run`.
Omitting the selector includes the original batch first. Each new stage runs all
its matched controls before its faults; a failed control stops that stage. An
independently selected stage does not claim that the earlier calibration ran.
A stage absent from the frozen manifest is rejected. Examples (future execution):

```sh
python3 tools/kafka_chaos_batch.py plan --stage packet --stage parallel
python3 tools/kafka_chaos_batch.py run --execute --stage packet \
  --output jobs/live/kafka-chaos-batch --max-new-cells 8
```

The packet manifest includes the exact probe command and reviewed image pin. The
probe uses one disposable container on `--network none`, drops every capability
then adds NET_ADMIN, has a read-only root plus a small `/run` tmpfs, and adds no
host mounts. It exercises a local netem qdisc and private iptables chain, removes
both and emits a completion marker. Docker may retrieve the immutable image if
missing. The runner checks the marker, exit code, image config ID, platform and
index digest before launching any packet scenario. Its start/completion records,
stdout/stderr and inspection output are retained and hashed. Failure, timeout,
changed evidence or interruption blocks the packet stage; the probe is never
retried automatically. A timeout may leave an ambiguous container named in the
start record and requires inspection before preparing another attempt.
The image tool check alone does not prove NET_ADMIN support.

At-least-once cells require the matching explicit oracle mode. They permit counted
duplicates only with PASS, zero missing IDs, valid subject provenance and confirmed
fault receipts. Strict cells retain the zero-duplicate requirement. Savepoint cells
require lifecycle confirmation in addition to the whole-input terminal oracle.
Re-freezing always uses a new output directory; old manifests and run evidence
remain historical and are never overwritten or silently upgraded.
