# Distributed TaskManager recovery

The engine can run one bounded job across several TaskManagers and Kafka partitions.
The local runner supports 1–16 TaskManagers, each with two slots. The selected job parallelism must fit the total
capacity; it is preserved in both REST submission and the Flink configuration.

The two new catalogs use one JobManager, two TaskManagers, parallelism four, four
partitions per topic and 3,000 integer IDs. Each source subtask delays records by
20 ms to leave a recovery window. These are separate scenarios; `bounded-eos` and
the existing calibration contracts are unchanged.

- `distributed-eos`: after a completed checkpoint, kill `taskmanager-2`, wait one
  second, and recreate only that logical slot.
- `distributed-eos-no-fault`: the same topology and workload without the recovery
  phase, as a healthy control.

After `mvn clean install`, run either catalog with Docker:

```bash
mvn -q exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario distributed-eos --artifact-root . --offline"
mvn -q exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario distributed-eos-no-fault --artifact-root . --offline"
```

Both expect an ordinary PASS: exact IDs with no missing, duplicate, unexpected or
malformed records, complete input/output boundaries and a confirmed process fence.
Runtime validation is still pending for these new catalogs. Passing Docker-free
tests does not establish that a real distributed recovery occurred.

## Targeted operations and evidence

With several TaskManagers, a restart must name its target:

```yaml
- kill:
    target: { kind: named, role: taskmanager, name: taskmanager-2 }
- restart: { component: taskmanager, name: taskmanager-2 }
```

The compiler tracks each target independently through phases and loops. It rejects
an ambiguous restart, double kill, wrong-target restart or an unhealed kill. Only
sequential faults are supported: restart the previous killed TM before killing another.
The
single-TM restart shorthand remains supported. Named image upgrades are not supported.

Every container incarnation receives its own Flink ResourceID. Kill evidence binds
the logical target to that ResourceID and its actual Docker runtime ID. A successful
kill alone is insufficient: before injection, REST must show a RUNNING subtask on
that exact ResourceID; afterward it must show a new failure on that ResourceID and
the required checkpoint restore or restart. An unrelated failure on another TM,
or a kill of an idle TM, cannot satisfy this check.

`evidence.taskManagerKills` records the target identity, matching subtask/failure
observations and any identity lookup error.
`evidence.taskManagerRestarts` records the old and replacement identities. Image,
runtime-JAR and connector evidence still cover the provisioned processes, and the
terminal fence must cover the latest incarnation in every logical slot. Missing
evidence prevents PASS without hiding a detected data violation.

This slice supports partial worker loss. It does not yet provide a state-sensitive
oracle, rescaling, continuous input, multiple brokers, paused workers, JobManager HA
or token-provider faults. Selecting a host by subtask placement remains future work;
these scenarios select a named process and independently check its observed placement.
