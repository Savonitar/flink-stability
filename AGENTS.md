# Contributor and coding-agent guide

This is an experimental fault-injection and correctness-testing harness for real
Flink and Kafka clusters. Start with [README.md](README.md), especially the
execution boundary and current executable subset. The schema accepts more than
the runner can execute; unsupported capabilities must reject explicitly.

## Repository map

| Path | Responsibility |
| --- | --- |
| `core/` | Specification loading/resolution, artifact preparation, execution planning, orchestration, Flink/Kafka protocol clients and terminal validators |
| `runtime-api/` | JDK-only runtime contracts and immutable evidence |
| `testcontainers/` | Container lifecycle, connector installation, fault adapters, fencing and cleanup |
| `cli/` | Commands, diagnostics and JSON rendering |
| `flink-job-generator/` | Thin protocol-v1 workload, including opt-in checkpoint hooks |
| `kroxylicious-fault-filter/` | Kafka protocol fault injection |
| `synthetic-token-plugin/` | Synthetic delegation-token test fixture |
| `scenarios/`, `calibration/`, `tools/` | Scenario/expectation pairs, deliberate mutants, comparison and evidence tooling |

Keep Docker/Testcontainers provisioning and container-specific process/network
operations in `testcontainers/`; `core/` owns orchestration and Flink/Kafka protocol
clients. Keep `runtime-api/` free of non-JDK production dependencies. Construct
specification documents through `SpecificationLoader`; preserve the document,
resolution, artifact and execution-plan boundaries. See
[SPEC-001](docs/specs/SPEC-001-scenario-schema.md)
for the contract and [SPEC-005](docs/specs/SPEC-005-semantic-validation-test-cases.md)
for semantic validation cases.

## Build, validate and run

Use JDK 21 to match CI; the build enforces a minimum of 21. Use Maven 3.8+ and
Python 3.10+ for tooling. Run commands from the repository root.
Docker is required only for live scenarios, not the normal CI checks:

```sh
mvn -B -ntp verify
python3 -m unittest discover -s tools/tests -v
```

Before invoking the CLI, install the same checkout's reactor artifacts and build
the workload JAR. Reinstall after relevant source changes:

```sh
mvn -B -ntp install
mvn -q exec:java -pl cli -Dexec.args="validate --catalog-root scenarios --scenario bounded-eos --artifact-root . --show-plan"
```

For a first live run with a reachable Docker daemon:

```sh
mvn -q exec:java -pl cli -Dexec.args="run --catalog-root scenarios --scenario bounded-eos --artifact-root ."
```

If Docker Engine 29 rejects startup with an API-version negotiation error, apply
both client settings described in [README requirements](README.md#requirements).

The first build/run may download dependencies or public images. Maven `-o` and
CLI `--offline` restrict their respective Maven resolution paths to cached
artifacts; neither is a Docker image no-pull setting. Respect an experiment's
explicit offline and execution constraints. See [VALIDATION.md](docs/VALIDATION.md)
for retained evidence and [PR-TESTING.md](docs/PR-TESTING.md) for connector comparisons.

## Checks appropriate to a change

- Java changes: start with the affected module and dependencies, for example
  `mvn -B -ntp -pl core -am test`. This excludes downstream consumers such as
  `testcontainers` and `cli`. Run full reactor `mvn -B -ntp verify` for core API
  changes, changes crossing module boundaries or before claiming full regression
  coverage.
- Python tooling: run its corresponding test file, for example
  `python3 -m unittest discover -s tools/tests -p 'test_pr_gate.py' -v`.
- Schema/catalog changes: update independent fixtures and expected-result siblings
  when the contract changes, then validate affected scenarios with `--show-plan`.
- Documentation-only changes: check links, commands and `git diff --check`;
  a Docker campaign is unnecessary.

Normal [CI checks](.github/workflows/ci.yml) do not start Docker. The manually
dispatched [bounded-eos workflow](.github/workflows/bounded-eos.yml) runs real
containers and uploads `bounded-eos-result.json` and `logs/` when present.

Report offline checks, live observations and mutant calibration separately.
Implemented faults are not automatically live-qualified;
consult each guide's verification scope and pending qualification requirements.
Keep missed windows, failures and component errors visible, including when the
final data check passes. Do not claim that one scenario covers every fault path.

## Correctness and provenance invariants

- Preserve natural bounded-job completion, a confirmed physical Flink write fence,
  a fixed raw Kafka high-watermark boundary and complete `read_committed` traversal
  before the exact-ID verdict. Counts or a RUNNING job are insufficient.
- Compare with the input manifest using the declared terminal oracle mode.
  Exactly-once rejects duplicates; explicit at-least-once permits and counts them.
  Both reject missing, unexpected and malformed IDs. See
  [at-least-once coverage](docs/KAFKA-AT-LEAST-ONCE.md).
- Incomplete verification, missing subject evidence or an unconfirmed fault must
  not become PASS. Keep observed attempt failures separate from scenario verdicts:
  a pinned expected-failure control passes only when the exact failure and all
  required evidence match.
- Do not change expectations to hide a failure. The intended TV1 same-ID reuse
  mutant failure must remain FAIL against its catalog's PASS expectation; see the
  [calibration guide](docs/POOL-REUSE-CALIBRATION.md).
- Preserve connector hashes, dependency closure, duplicate-class rejection and
  per-incarnation class-origin checks. Keep the workload thin: do not shade the
  connector or Flink into it. The default runtime is a released Docker distribution;
  checkpoint hooks belong to the workload, not patched Flink core.
- Custom runtimes need the explicit identity and compatibility contract in
  [FLINK-RUNTIME-TESTING.md](docs/FLINK-RUNTIME-TESTING.md) and
  [CUSTOM-RUNTIME-SUBJECTS.md](docs/CUSTOM-RUNTIME-SUBJECTS.md). Binary identity
  does not prove source revision, compatibility or execution of a changed method.
- Preserve each CLI command's output contract. A completed `run` emits one JSON
  result on stdout, including captured cleanup failures. Preparation/setup errors
  and exceptions escaping execution can instead terminate with stderr diagnostics
  and no JSON result. Plain `validate` and campaign commands retain their text
  summaries. For `run` and `validate`, exit `0` means `pass` or valid, respectively;
  `1` means a non-pass verdict or an execution, validation or preparation failure;
  `2` means a usage error or unknown target. Keep operational diagnostics on stderr,
  including shutdown logs.

## Preserve evidence and prepare safe reports

Default working data includes `logs/application.log` relative to the process
working directory, attempt directories under the Maven reactor root's
`checkpoints/` (or the current directory's when no reactor root is set), and
temporary artifact snapshots under `<artifact-root>/.flink-stability/`.
Campaign `--output` must name a new directory under `<workspace-root>/jobs/`;
prefer fresh `jobs/<name>/` directories for retained run evidence too.
Repository ignore rules cover `jobs/`, `logs/` (including `cli/logs/`),
`checkpoints/`, `savepoints/` and `.flink-stability/`.

Treat retained `jobs/` runs, frozen catalogs, manifests, artifacts and raw logs as
immutable evidence. Use fresh output directories; do not rerun over an old attempt
or rebuild artifacts referenced by it. Use isolated build/cache locations for
frozen comparisons. Root `mvn clean` also removes `logs/`, `cli/logs/`,
`checkpoints/` and `.flink-stability/`; preserve needed evidence before cleaning.

Use generated data for examples. Configuration values, REST error bodies, record
payloads and local paths can appear in reports; do not put secrets in scenario
inputs or commit private run output. Review selected evidence before sharing it,
and make sanitized report copies without altering the retained originals. State
the exact tested artifacts, commands, result and limitations; keep observed facts
separate from inferred causes.
