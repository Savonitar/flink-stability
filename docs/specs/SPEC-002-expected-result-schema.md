# SPEC-002 - Expected-Result Schema v1

**Status:** draft implementation contract derived from SPEC-001.
**Depends on:** SPEC-001 R1.3a-R1.3c, R3.5-R3.7, R7.2a, R8.2a-R8.8.

This document defines the v1 `kind: expected-result` document. It holds the
committed expectation for a scenario: the expected outcome, and for expected
failures the exact oracle and reason code.

It does not hold expected data. Runtime data such as generated input IDs comes
from run artifacts, especially the input manifest defined in SPEC-001.

## 1. Envelope and discovery

- **E1.1** An expected-result document has:

  ```yaml
  format: v1
  kind: expected-result
  meta:
    name: <scenario-name>.expected
    scenario: <scenario-name>
  ```

- **E1.2** For a scenario file `X.yaml`, the required expected-result sibling is
  `X.expected.yaml` in the same directory.
- **E1.3** `meta.name` is exactly `X.expected`, where `X` is the scenario
  `meta.name`.
- **E1.4** `meta.scenario` is exactly the scenario `meta.name`.
- **E1.5** Recursive scenario discovery dispatches by `kind`; it never executes
  expected-result files as scenarios.
- **E1.6** Semantic validation rejects all discovery mismatches before
  provisioning: missing sibling, orphan expected-result, duplicate target,
  path/name mismatch, and `meta.scenario` mismatch.
- **E1.7** Unknown fields are rejected.

## 2. Expectation shapes

- **E2.1** Every expected-result document has a required `default` expectation.
- **E2.2** A scenario with `experiment` uses an experiment expectation:

  ```yaml
  default:
    baseline:
      outcome: pass
    candidate:
      outcome: fail
      oracle: kafka.id-set
      reason: validator.kafka.id-set.missing-ids
  ```

- **E2.3** A scenario without `experiment` uses a plain expectation:

  ```yaml
  default:
    outcome: pass
  ```

- **E2.4** `outcome` is one of `pass` or `fail`. `inconclusive` is a runtime
  verdict, not an expected outcome.
- **E2.5** A plain expected `fail` requires both `oracle` and `reason`.
- **E2.6** An experiment `baseline` outcome is always `pass` and must not
  declare `oracle` or `reason`.
- **E2.7** An experiment `candidate` may expect `pass` or `fail`; expected
  `fail` requires both `oracle` and `reason`.
- **E2.8** An expected `pass` must not declare `oracle` or `reason`; those fields
  are only valid on expected failures.
- **E2.9** `oracle` names exactly one top-level terminal validator type, never an
  inline validation step. v1 rejects duplicate top-level terminal validator
  types so this identity is unambiguous.
- **E2.10** `reason` is a stable namespaced terminal-validator `fail` code. It
  must be registered for the named `oracle` by SPEC-001 R7.2b or, for a custom
  validator, declared in that validator's `failure_reasons`. It is never an
  error-message substring, await/fault/infrastructure code, or inconclusive
  reason.

## 3. Parameter-selected cases

- **E3.1** `cases` is optional. When present, it is a non-empty list.
- **E3.2** Each case has:

  ```yaml
  - when:
      <parameter-name>: <literal-value>
    <complete expectation replacement>
  ```

- **E3.3** `when` is a non-empty map of declared scenario parameter names to
  literal values.
- **E3.4** The case expectation is a complete replacement for `default`, not a
  patch. A case for an experiment provides both `baseline` and `candidate`; a
  case for a plain scenario provides one `outcome`.
- **E3.5** For an experiment, `when` must not name any key from
  `experiment.varies`; that value differs by side and cannot select a common
  expectation.
- **E3.6** Validation rejects any case that references an undeclared parameter.
- **E3.7** Validation rejects any case value that fails parameter type or
  capability validation.
- **E3.8** Validation rejects overlapping cases. Two cases overlap when their
  shared keys do not contradict one another.
- **E3.9** Validation rejects a case whose replacement expectation is identical
  to `default`.
- **E3.10** Case selection is independent of suite-entry `as`; the same resolved
  parameters select the same expectation in direct runs, suite runs, and
  CLI-parameterized runs.

Example:

```yaml
default:
  baseline:
    outcome: pass
  candidate:
    outcome: fail
    oracle: kafka.id-set
    reason: validator.kafka.id-set.missing-ids

cases:
  - when: { kafka_transaction_id_naming_strategy: POOLING }
    baseline:
      outcome: pass
    candidate:
      outcome: pass
```

## 4. Matching semantics

- **E4.1** Attempt result aggregation is defined by SPEC-001 R7.1a.
- **E4.2** An expectation is evaluated only against a clean, non-inconclusive
  attempt result.
- **E4.3** Expected `pass` matches only an attempt result of `pass`.
- **E4.4** Expected `fail` matches only an attempt result of `fail` from the
  expected `oracle` with the expected `reason` and confirmed experiment evidence
  (SPEC-001 R8.7a). A matching data failure with unconfirmed fault evidence or
  incomplete phase/fence/oracle evidence remains an attempt failure but gives an
  `inconclusive` scenario verdict.
- **E4.5** A clean candidate result that does not match its expectation is a
  scenario `fail`, subject to experiment aggregation in SPEC-001 R8.7.
- **E4.6** Baseline mismatch in an experiment is `inconclusive`, not a candidate
  finding.
- **E4.7** Terminal-validator `inconclusive` under clean health is an
  `inconclusive` scenario verdict and is not retried automatically.

## 5. Regression-contract changes

- **E5.1** The expected-result file is the regression contract for the scenario.
- **E5.2** Any change to `outcome`, `oracle`, `reason`, or selected `cases` is a
  contract change and must be reviewed as such.
- **E5.3** Flipping a bug-reproduction candidate from expected `fail` to expected
  `pass` means the bug is considered fixed and the scenario has become a
  regression guard.
- **E5.4** Removing an expected failure reason without replacing it with an
  equally specific reason is a weakening change.
- **E5.5** Adding a case that is equivalent to `default` is invalid; it adds no
  behavior and can drift silently.

## 6. Minimal examples

Plain pass:

```yaml
format: v1
kind: expected-result

meta:
  name: eos-minimal.expected
  scenario: eos-minimal

default:
  outcome: pass
```

Experiment with pinned expected failure:

```yaml
format: v1
kind: expected-result

meta:
  name: eos-statebackend-independence.expected
  scenario: eos-statebackend-independence

default:
  baseline:
    outcome: pass
  candidate:
    outcome: fail
    oracle: kafka.id-set
    reason: validator.kafka.id-set.missing-ids
```

## 7. Implementation notes

- **E7.1** JSON Schema handles envelope shape, required fields, allowed enum
  values, and no-unknown-fields checks.
- **E7.2** Semantic validation handles sibling discovery, target matching,
  parameter case validation, overlap detection, validator-name existence, and
  exact oracle/reason-pair existence. It checks `default` and every case, not
  only the branch selected by the current parameters.
- **E7.3** The runner records the selected expectation in the run report after
  parameter resolution and case selection.
