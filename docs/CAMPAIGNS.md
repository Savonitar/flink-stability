# Seeded fault schedules

`campaign` creates concrete, offline-validated scenarios. It never provisions or runs
infrastructure. The first slice uses sequential, self-healing fault units; it does not
search for failures, adapt to feedback, or claim a minimal reproduction.

Inputs are a resolved single scenario, a signed 64-bit seed and a strict JSON constraints
file. Submit-time `-p NAME=VALUE` overrides use the existing parameter resolver. The
selected expected result is frozen. The named phase is **replaced in the generated copy**;
all other phases, setup, jobs, workload, checkpoints, retries and validations stay intact.
Choose the existing fault phase, preserving the base's warmup phase. Originals are never
edited. Multiple generated cases remain separate runs; do not run them concurrently with
shared transactional-ID prefixes.

For `broker-leader-pause`, save this as `jobs/my-campaign/constraints.json`:

```json
{
  "phase": "broker-recovery",
  "min_faults": 1,
  "max_faults": 3,
  "min_gap_ms": 0,
  "max_gap_ms": 1000,
  "faults": [
    [{"broker_fault": {
      "mode": "pause", "duration": "20s", "timeout": "2m",
      "target": {"kind": "selector", "role": "broker", "cluster": "main",
                 "type": "partition-leader", "topic": "output", "partition": 0}
    }}],
    [{"broker_fault": {
      "mode": "kill", "duration": "1s", "timeout": "2m",
      "target": {"kind": "named", "role": "broker", "cluster": "main", "name": "broker-1"}
    }}]
  ]
}
```

Use the existing CLI launcher (`flink-stability` below denotes that launcher):

```sh
flink-stability campaign generate --catalog-root scenarios/brokers \
  --scenario broker-leader-pause --seed 42 \
  --constraints jobs/my-campaign/constraints.json --output jobs/my-campaign/generated \
  --artifact-root .
flink-stability campaign replay --manifest jobs/my-campaign/generated/campaign.json \
  --output jobs/my-campaign/replayed --artifact-root .
flink-stability campaign shrink --manifest jobs/my-campaign/generated/campaign.json \
  --output jobs/my-campaign/candidates --artifact-root .
```

Output must be a new directory below `--workspace-root` (default `.`) `/jobs/`, with no
symlink traversal. Existing outputs are refused. Each catalog has a scenario YAML,
expected-result YAML and `campaign.json`; shrink puts each candidate in its own subdirectory.
`validate --catalog-root DIR --scenario NAME --artifact-root . --offline` works normally.
Artifact preparation in all three commands is always offline, using the same checks as
`validate --offline`, after the executable compiler has accepted each schedule. Missing
artifacts or incompatible fault/topology combinations fail before output is published.

`faults` contains 1–32 alternatives, each an array holding one complete fault unit:

- `kill`, `wait`, `restart`: a named TaskManager or Kafka broker, explicitly naming the
  same process on restart (`component: taskmanager` or `kafka`).
- One `leader_fault`: JM kill, pause or ZooKeeper isolation, optionally with the existing
  token delay/fail/linkage or submitted-job token fault. Existing HA/proof requirements apply.
- One `broker_fault`: named broker, partition leader or transaction coordinator; kill or
  pause with automatic restart/resume. Three-broker topology is required.
- One `network_fault` matching `end-txn`, with `drop-request` or `drop-response` and its
  existing occurrence, trigger-deadline and healing fields. The base must declare its proxy.

Durations/timeouts are concrete, unchanged template values. Different durations can be
separate alternatives. Count bounds are 1–32, gap bounds 0–120000 milliseconds inclusive;
min ≤ max. A gap is drawn **before each fault**, including the first; zero emits no wait.
Every template is checked, even if the seed does not choose it. Existing runner bounds
also apply to the expanded schedule. This slice does not combine mutually incompatible
HA/proxy/topology features or relax their existing validation rules.

`fault-schedule-v1` specifies Java Random's 48-bit algorithm: draw count, then template
index and gap for each unit, in that order. JSON object keys are sorted before hashing
and YAML serialization; array order is significant. No clock, machine path or environment
value is added. `meta.campaign` records seed, generator version and recipe SHA-256.
The manifest embeds the materialized base, selected expectation, constraints and shrink
edits. Replay regenerates from these inputs and checks both YAML SHA-256 values; changing
inputs, using an unknown version or a serialization mismatch is an error. It does not
copy the original output files and does not promise identical distributed execution.

Shrink emits one-step candidates: remove a whole fault unit (including its preceding gap),
or halve its hold in milliseconds, minimum 1 ms. Paired process restart is retained; JM
and broker automatic healing remains intact. Token delay is halved with the JM hold.
Timeouts, trigger deadlines and the oracle remain unchanged. EndTxn occurrence loss has
no hold to shorten, so it only produces a removal candidate. Removing the final unit
removes the chosen phase. Invalid reductions are omitted with reasons in `reductions.json`.
Each candidate has its own replayable manifest. Iterate by selecting a candidate based
on a separate recorded run, then shrinking its manifest again.
