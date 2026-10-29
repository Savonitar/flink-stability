# Checkpoint-storage review follow-ups

This is the local issue ledger for optional section C of the October review.
It does not replace the required A/B fixes or extend the historical live
qualification. Original evidence and expectations remain unchanged.

| Follow-up | Concrete remaining work / acceptance |
| --- | --- |
| Pilot runner lifecycle | Replace the pilot-only `calibration/state-recovery/run.py` subprocess lifecycle with a bounded process group, per-attempt HOME/TMPDIR/empty Docker configuration and SIGTERM/SIGHUP cleanup; retain interrupted receipts and never retry an allocated attempt. The README now labels the limitation. |
| Workload identity lifecycle | Re-resolve the immutable build identity in `KeyedPrefixFunction.open()` and reject disagreement with its serialized identity; constrain directory code sources to tests and pin LF for the identity resource. Preserve identical correct/mutant class bytes. |
| Tool path portability | Scope personal-directory name checks to actual authorized roots; allow explicitly selected symlinked Docker/JDK installations without following links into prohibited locations. Make read-only re-audits independent of the original absolute Java launcher path. |
| Direct launcher properties | Remove inert Maven settings system properties from the direct Java launcher and its documentation; retain the real resolver/cache isolation checks. |
| Runner signal retention | Add SIGTERM/SIGHUP handlers to the checkpoint matrix runner, retaining the same termination and cleanup evidence as its interrupt path. |
| Native EIO audit scope | Constrain every errno=5 row on every instance to the armed fault family, identity, lease and expected checkpoint window; preserve the mandatory original-TaskManager create/write proof and reject replacement EIO. |
| Step ledger | Introduce explicit statuses for a skipped keyed kill and an unconfirmed atomic storage fault, instead of SUCCEEDED plus detail text; keep runtime identity and expected kill counts consistent. |
| REST error wording | Treat REST error text as corroboration of required native errno=5 evidence, rather than requiring the English literal `Input/output error`; keep exact failed checkpoint correlation. |
| Publication redaction | Broaden report path redaction beyond the current host patterns, with fixtures for additional platform formats and no changes to retained raw originals. |
| Workload packaging | Evaluate removing `log4j2.properties` from the workload JAR; check logging compatibility and preserve the thin-JAR contract. |

Completed alongside the review fixes: native overlong-path handling, main-thread
unmount, early-exit socket/FUSE cleanup, distinct not-armed and handle-limit
receipts, sequence advancement before sync, package verification and network
disconnection before installation, backend-aware builder identity, documented
journal duration/minimum bounds and expired-rule behavior. Oracle range
classification and the remaining small reporting fixes are verified with the
final regression results; none of these changes retroactively alters a retained
qualification.
