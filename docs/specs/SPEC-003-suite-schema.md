# SPEC-003 - Suite Schema v1

**Status:** draft implementation contract derived from SPEC-001. Catalog
membership, eager suite planning, per-entry resolution, and artifact preparation
are implemented; sequential Docker execution and suite reporting remain pending.
**Depends on:** SPEC-001 R1.1-R1.4, R2.7-R2.10, R6.13, R8.1, R8.3-R8.8.

This document defines the v1 `kind: suite` document. A suite is an ordered list
of scenario invocations with optional parameter bindings and suite-specific
`runs`.

Suite files are the merge-gate contract. They are deliberately explicit: adding
or removing a scenario changes the suite file, so review sees the gate changing.

## 1. Envelope

- **S1.1** A suite document has:

  ```yaml
  format: v1
  kind: suite
  meta:
    name: <suite-name>
  scenarios: [...]
  ```

- **S1.2** `meta.name` is stable, lower-kebab-case, and globally unique among
  suite names.
- **S1.3** `meta.description` is optional for suites.
- **S1.4** Unknown fields are rejected.

## 2. Scenario entries

- **S2.1** `scenarios` is a required non-empty ordered list.
- **S2.2** Each suite entry has this shape:

  ```yaml
  - scenario: <scenario-name>
    as: <suite-entry-id>        # optional unless needed by S2.4
    parameters: { ... }         # optional
    runs: 20                    # optional
  ```

- **S2.3** `scenario` is required and names a scenario `meta.name`, not a file
  path.
- **S2.4** `as` defaults to `scenario`. `as` is required when the same scenario
  appears more than once in a suite.
- **S2.5** `as` is lower-kebab-case and unique within the suite.
- **S2.6** Every run artifact, report key, retry record, and CI annotation is
  keyed on `as`, never on `scenario` alone.
- **S2.7** Suite order is execution order unless a future runner explicitly
  implements an independent scheduler. v1 executes sequentially.

## 3. Parameter binding

- **S3.1** `parameters` is optional. When present, it is a non-empty map from
  declared scenario parameter names to literal values.
- **S3.2** A suite parameter binding has higher precedence than scenario
  defaults and lower precedence than submit-time CLI overrides.
  Suite submit overrides are global and strict in v1: the same map is applied to
  every entry, and an unknown key in any referenced scenario rejects the whole
  suite. v1 does not have alias-qualified transient overrides; use the entry's
  committed `parameters` map for entry-specific values.
- **S3.3** A suite parameter binding naming an undeclared scenario parameter is a
  semantic validation error before provisioning.
- **S3.4** A suite parameter value must pass the scenario parameter type check
  and any capability validation induced by the field where it resolves.
- **S3.5** A suite parameter binding must not name a parameter listed in
  `experiment.varies`. The committed experiment defines those side-specific
  values.
- **S3.6** The selected expected-result case is determined after suite parameter
  binding and submit-time override resolution.

Example:

```yaml
scenarios:
  - scenario: eos-statebackend-independence
    as: sb-independence-pooling
    parameters: { kafka_transaction_id_naming_strategy: POOLING }
```

## 4. Suite-specific runs

- **S4.1** `runs` is optional on a suite entry.
- **S4.2** When present, `runs` is a positive integer and replaces the scenario
  `runs` only for that suite invocation.
- **S4.3** The report records both declared scenario `runs` and effective suite
  `runs`.
- **S4.4** Submit-time CLI overrides for `runs` are not supported in v1.
- **S4.5** `health_retry_limit` cannot be set or overridden in a suite entry.
  It is scenario-owned.
- **S4.5a** Parameter interpolation cannot bypass S4.2-S4.5. A suite binding or
  submit override whose effective value reaches scenario `runs` or
  `health_retry_limit` is rejected. Suite repetition is changed only by the
  entry's literal `runs` field.
- **S4.6** The aggregation semantics for effective `runs` are exactly the
  scenario semantics in SPEC-001 R8.3-R8.8.

## 5. Referential validation

- **S5.1** A suite entry naming a missing scenario is a semantic validation
  error.
- **S5.2** A referenced scenario must have exactly one valid expected-result
  sibling.
- **S5.3** Expected-result case validation uses the scenario parameters after
  suite binding is considered.
- **S5.4** Suite validation rejects duplicate `as` values before any scenario
  starts.
- **S5.5** Suite validation rejects entries that would be indistinguishable in
  reports or artifact directories.

## 6. CI semantics

- **S6.1** The suite result is green only when every entry returns `pass`.
- **S6.2** Any entry returning `fail` makes the suite fail.
- **S6.3** Any entry returning `inconclusive` leaves the suite unvalidated unless
  a higher-level CI policy retries and obtains a conclusive result.
- **S6.4** `inconclusive` is not green and is not attributed to the code change.
- **S6.5** Suite reports include per-entry status, effective parameters,
  effective runs, selected expected-result case, artifact identities, and links
  to each scenario report.

## 7. Minimal example

```yaml
format: v1
kind: suite

meta:
  name: eos-nightly

scenarios:
  - scenario: eos-tm-kill-open-transaction
    as: tm-kill-smoke
    runs: 1

  - scenario: eos-statebackend-independence
    as: sb-independence-default

  - scenario: eos-statebackend-independence
    as: sb-independence-pooling
    parameters: { kafka_transaction_id_naming_strategy: POOLING }
    runs: 20
```

## 8. Implementation notes

- **S8.1** JSON Schema handles envelope shape, required entry fields,
  no-unknown-fields, and scalar type checks.
- **S8.2** Semantic validation handles scenario-name lookup, expected-result
  lookup, duplicate `as`, parameter existence, parameter precedence conflicts,
  and effective-runs calculation.
- **S8.3** A suite runner resolves every entry and every entry's file/Maven
  artifacts, including checksums, before provisioning the first scenario. A late
  failure after some scenarios have already run weakens the usefulness of the
  report. Artifact failures are aggregated with suite-entry identity and no
  partial prepared plan is returned.
- **S8.4** Planning preserves source order and returns no partial plan. Each
  planned entry carries its suite source, source index, exact JSON pointer,
  effective `as`, scenario name, resolved scenario plan, resolved scenario K,
  and effective suite K. Diagnostics preserve the original scenario or
  expected-result location while also identifying the suite entry; errors in a
  suite binding point to the actual `scenarios[i].parameters` field.
- **S8.5** `runs: K` remains one scenario invocation with one shared health
  retry budget. A planner may expose lazy logical clean-run slots (`SINGLE` for
  a plain scenario, `CANDIDATE` for an experiment), but initial and confirmatory
  baselines and dirty retries are runtime decisions, not independent expanded
  suite entries.
