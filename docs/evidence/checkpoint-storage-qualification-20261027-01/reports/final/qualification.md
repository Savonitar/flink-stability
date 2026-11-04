> Sanitized publication copy. Original source: `jobs/checkpoint-storage-qualification-20261027-01/reports/final/qualification.md`; original SHA-256: `30f96ec95304dd144c3ae246f5a0346e80669ac36b5c299b261b79bb3ee5ce11`. Host paths are redacted. Embedded artifact/report hashes refer to originals, not this copy. See the publication manifest for this copy's SHA-256.

# Checkpoint-storage qualification

**QUALIFIED**

Fixed matrix: four controls × three attempts. Catalog expectations remain PASS. A confirmed mutant calibration retains its raw state-mismatch FAIL.

| Cell | Attempt | Raw result | Raw reason | Qualification | Confirmed restores | Native EIO | Component events | REST errors | Cleanup |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| correct-healthy | 1 | PASS | validator.kafka.state-prefix.match | confirmed | 0 | 0 | 0 | 0 | confirmed |
| correct-healthy | 2 | PASS | validator.kafka.state-prefix.match | confirmed | 0 | 0 | 0 | 0 | confirmed |
| correct-healthy | 3 | PASS | validator.kafka.state-prefix.match | confirmed | 0 | 0 | 0 | 0 | confirmed |
| correct-restart | 1 | PASS | validator.kafka.state-prefix.match | confirmed | 1 | 0 | 2 | 0 | confirmed |
| correct-restart | 2 | PASS | validator.kafka.state-prefix.match | confirmed | 1 | 0 | 2 | 0 | confirmed |
| correct-restart | 3 | PASS | validator.kafka.state-prefix.match | confirmed | 1 | 0 | 2 | 0 | confirmed |
| correct-eio | 1 | PASS | validator.kafka.state-prefix.match | confirmed | 1 | 20 | 2 | 0 | confirmed |
| correct-eio | 2 | PASS | validator.kafka.state-prefix.match | confirmed | 1 | 20 | 2 | 0 | confirmed |
| correct-eio | 3 | PASS | validator.kafka.state-prefix.match | confirmed | 1 | 20 | 2 | 0 | confirmed |
| mutant-eio | 1 | FAIL | validator.kafka.state-prefix.state-mismatch | confirmed | 1 | 20 | 2 | 0 | confirmed |
| mutant-eio | 2 | FAIL | validator.kafka.state-prefix.state-mismatch | confirmed | 1 | 20 | 2 | 0 | confirmed |
| mutant-eio | 3 | FAIL | validator.kafka.state-prefix.state-mismatch | confirmed | 1 | 20 | 2 | 0 | confirmed |

## Observed totals

- Confirmed attempts: correct-healthy 3/3, correct-restart 3/3, correct-eio 3/3, mutant-eio 3/3.
- Component events: 18; REST errors: 0; confirmed cleanup receipts: 12/12.
- Confirmed restorations: 9 across 12 attempts with recovery counts; 0 unknown.
- Native EIO operations: 120 across 12 attempts with verified storage counts; 0 unknown.
- Missing rows: 0; duplicated allocations: 0.

Component events remain observations even when data validation passes; they are not silently subtracted as expected noise.

## Exact frozen identities

- Matrix manifest SHA-256: c9f673f7cd883796b58e8a4dd70e61646e50a8bfaab026cc07bf4d7d1448d945
- Framework manifest SHA-256: dab6678930cf0e5b40af97748bd431a39ccc5968ed4ff5523c1572be09242cac
- correct workload: artifacts/correct.jar
- correct workload SHA-256: 4c8ca4211b01d26ba0c49a2060259c117b95016fef798b4a0c187a332edd9711
- mutant workload: artifacts/mutant.jar
- mutant workload SHA-256: 0ad8e5eeec5deb8e732a346dffc54c3675a6cb7c086e5c1bfa23a1389798d54a
- Helper manifest SHA-256: 9ab2001ccc55e700a76c12d0ba64d4824d1089d21e85adffa5216d75d8582ecf
- Qualified platform: linux/arm64
- Helper checkpoint-fuse SHA-256: 49d01b4f62b6a7bd8256779777cf1e6d193d2e1d7365796c115f4ffb66c85f66
- Helper checkpoint-fuse-selftest SHA-256: bbdd6a07da940c9d9db135958ec7f1caf8502f03767203f349aa65b5c7a3b9d5
- Helper probe.jar SHA-256: 2fd880248a7515b08ee419e1ff875cc1edff8e0f784878aa55a0aaa0a1780983
- flink image: flink:2.2.0
- flink local image ID: sha256:2e93ded2cb7bf1a50f71b663d5e3ea01420bcc4e549cbf666d05120f939713c7
- kafka image: apache/kafka:4.0.0
- kafka local image ID: sha256:2cecec9e3ea2d63fd5d1da1ab95e6bb7c33a7ff81053c4eb46f4176cd0bad8c9
- Connector primary SHA-256: 6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da
- Host Java executable SHA-256: e09233a5d606938176734fb4eb70407659a8fede3073949c495631f43612e65c
- Docker API version: 1.44
- Local Docker endpoint SHA-256: 18e932287de8b0182fc9eb897aa39555bd462ede65f195b8df7342d9e16b4f5e
- Platform evidence SHA-256: 979f1decd92cd412925ce4074554708f7689f3de765f14d383bbd1e8a467d465
- Docker Engine: 29.1.3
- Docker kernel: 6.12.54-linuxkit
- Docker architecture: aarch64
- Docker operating system: Docker Desktop
- Docker OS type: linux
- Host OS: Darwin
- Host OS release: 25.5.0
- Host architecture: arm64
- Host Python: 3.13.1 (main, Dec  3 2024, 17:59:52) \[Clang 16.0.0 (clang-1600.0.26.4)\]

Local Docker endpoint is displayed by hash; each original command receipt retains the exact endpoint and isolated environment.
The frozen framework manifest retains the ordered runtime classpath, dependency closure, source and configuration hashes.

| Connector container path | SHA-256 |
| --- | --- |
| /opt/flink/lib/flink-stability-connector-00000000-6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da.jar | 6bb63f7b09930d99745325393b481c092b0c26d626e738b7a1fd6fd8d7d4f1da |
| /opt/flink/lib/flink-stability-connector-00000001-4eeeae8d20c078abb64b015ec158add383ac581571cddc45c68f0c9ae0230720.jar | 4eeeae8d20c078abb64b015ec158add383ac581571cddc45c68f0c9ae0230720 |
| /opt/flink/lib/flink-stability-connector-00000002-d8054ae7c0d1c2d2f55d28e46026ebe5892881f3fab5f439233184381c3b4a1f.jar | d8054ae7c0d1c2d2f55d28e46026ebe5892881f3fab5f439233184381c3b4a1f |
| /opt/flink/lib/flink-stability-connector-00000003-4b364e6850dc89172fcf1d4dd26b8ff5488eda44ff4657e22dd265203dd5ab3c.jar | 4b364e6850dc89172fcf1d4dd26b8ff5488eda44ff4657e22dd265203dd5ab3c |
| /opt/flink/lib/flink-stability-connector-00000004-e2d202d4606e23aeaf8a5a9632db06f5fefd5b63d251c3f503f9faaa78530e5c.jar | e2d202d4606e23aeaf8a5a9632db06f5fefd5b63d251c3f503f9faaa78530e5c |
| /opt/flink/lib/flink-stability-connector-00000005-f30d77f5826b9e9813342e84ab412095ed4ed5cf4fef6f93cebb848cb0fd0294.jar | f30d77f5826b9e9813342e84ab412095ed4ed5cf4fef6f93cebb848cb0fd0294 |
| /opt/flink/lib/flink-stability-connector-00000006-bcf39f152c5553c2501a491fc4a73c5b0330741e8eed96a481323b8ee89738af.jar | bcf39f152c5553c2501a491fc4a73c5b0330741e8eed96a481323b8ee89738af |
| /opt/flink/lib/flink-stability-connector-00000007-581bd61000ef7648943f781ca05689e56d03f6052748365a8e2b3a9b5d3fa32f.jar | 581bd61000ef7648943f781ca05689e56d03f6052748365a8e2b3a9b5d3fa32f |
| /opt/flink/lib/flink-stability-connector-00000008-b6c3237e24b8252a5a9c3a0e1ae30ef8f323412e74bd358548b7a9e527d676df.jar | b6c3237e24b8252a5a9c3a0e1ae30ef8f323412e74bd358548b7a9e527d676df |
| /opt/flink/lib/flink-stability-connector-00000009-a58a84c4271e50df4c96ed916ccb7e48a869f8ed9cdcda1ad5d3d4c33b0214a3.jar | a58a84c4271e50df4c96ed916ccb7e48a869f8ed9cdcda1ad5d3d4c33b0214a3 |
| /opt/flink/lib/flink-stability-connector-00000010-4c766cb3f855415ee734b2392949a0b6f12a60879334a74518deaf6270d32e36.jar | 4c766cb3f855415ee734b2392949a0b6f12a60879334a74518deaf6270d32e36 |
| /opt/flink/lib/flink-stability-connector-00000011-d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0.jar | d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0 |

Workload contract: {"checkpointIntervalMillis": 10000, "completionTimeoutMillis": 300000, "keyCount": 8, "maxConcurrent": 1, "parallelism": 1, "processingDelayMillis": 5, "smallFileThreshold": 0, "tolerableFailures": 10, "totalRecords": 30000}.

## Separate pilot references

| Control | Original workspace path | Frozen result path | SHA-256 |
| --- | --- | --- | --- |
| correct-healthy | jobs/checkpoint-storage-integration-pilots-20261027-02/attempts/healthy-fuse/run/pilot-01/stdout.json | evidence/references/correct-healthy.json | e17a408c9f0f6f5ecd39b0bd99129e2fff67ab9ce2b89b15780606aaa0d73db4 |
| correct-restart | jobs/checkpoint-storage-qualification-pilots-20261027-01/attempts/healthy-restart/run/pilot-01/stdout.json | evidence/references/correct-restart.json | 49da248550df5b46bab26df1b46777efb775e99779f42d7686786ed5bf843e37 |
| correct-eio | jobs/checkpoint-storage-integration-pilots-20261027-02/attempts/correct-eio/run/pilot-03/stdout.json | evidence/references/correct-eio.json | e9625a6a0cfe4f5e828a5b251479c0dad01d608e07c9711682a21fa472aab03a |
| mutant-eio | jobs/checkpoint-storage-integration-pilots-20261027-02/attempts/mutant-eio/run/pilot-01/stdout.json | evidence/references/mutant-eio.json | b7642c06315649293e444a71b575749b65eba3e227a3f5347b92e13537bf6d25 |

Pilots are separate evidence and do not replace any of the twelve qualification attempts.

## Gaps and diagnostics

No gaps reported by the independent checker.

## Scope and limitations

- This matrix covers released Flink 2.2.0, HashMap keyed state, TTL disabled, exactly-once Kafka, one JobManager and one TaskManager, parallelism one and one source/sink partition.
- Qualification is limited to the pinned FUSE helper and Docker Desktop linux/arm64 environment. It does not qualify RocksDB, HA, multiple workers, other platforms, ENOSPC, permanent disk loss or broker storage faults.
- Input IDs are reconstructed from the frozen integer-sequence contract; complete input and read_committed traversal receipts remain required.
- Checkpoint binaries may be deleted after natural completion. Materialization uses pinned inspector and native I/O receipts; absent checkpoint bytes are not represented as retained.
- Artifact hashes identify exact bytes. They do not independently attest a Git revision or compiler provenance. Offline tests, pilot observations and these qualification attempts are separate evidence.
