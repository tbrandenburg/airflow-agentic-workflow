# P6 acceptance evidence

Run `make acceptance` against the healthy owned stack after authorized provider
staging. It does not start or stop services. A complete run must verify twelve
scenario IDs; any failed assertion exits nonzero and no successful bundle is
published. Requests, fixed fixture IDs, expected/observed states, run URLs,
publication manifests, REST task-log references, artifact counts and independently
enumerated mirror before/after inventories are retained in the JSON evidence.

## Scenarios

| ID | Scenario | Execution |
| --- | --- | --- |
| S01 | Happy path: health ok → healthy | Real Planner, Coder and Reviewer via authenticated gateway |
| S02 | Validation failure | Fixed full workflow, gate task success, reviewer skipped, package/publisher success, DAG failed |
| S03 | Agent retry | Fixed transient-once Coder; two attempts, one successful completion |
| S04 | Malformed output | Fixed malformed Coder; nonretry failure, dependents blocked, failure report |
| S05 | Bash failure | Fixed context executable exit 42; stderr, bounded retry, blocked dependents |
| S06 | Worker separation | Fixed exclusive queues; different workers reconstruct verified artifact inputs/base SHA |
| S07 | Repository safety | Independent 21-file inventory, sizes/modes, directories, refs and HEAD compared across execution |
| S08 | Pool saturation | Two actual runs, demand above slots; interval peaks model 2 / validation 1, executor 4 |
| S09 | Timeout | Fixed hanging Coder descendant; bounded nonretry failure, stopped PID, removed workspace |
| S10 | Patch safety | Fixed `.git/config` patch rejected before admission, blocked validation, unchanged mirror |
| S11 | Retry recovery | Actual REST clear; unchanged completion/reference, false model invocation, no output key additions |
| S12 | Report resilience | Fixed package exit 42; core publisher succeeds, valid failure report, DAG fails |

S06/S08/S11 reuse one fresh P5 operational proof with separate scenario records.
Their inventory boundaries bracket that combined execution. S07 independently
compares the suite-wide initial/final inventories; each workflow execution also
has a before/after comparison. This avoids redundant model demand while preserving
scenario-specific expectations and API evidence.

## Checklist and immutable publication

The original plan's exact 28 Section 17 criteria in
[acceptance_criteria.py](/tools/acceptance_criteria.py) map to scenario records:
Functional (8), Artifacts reliability (11), Operations safety (9). Verification
requires twelve passed records and the associated inspected implementation proof;
missing records or unverified proof prevent publication.

Read-only deployed DAG inspection records actual operator classes, fixed Node/Bash
commands and runtime/source hashes. Actual API requests reject unregistered
prompt/script/fixture fields. Docker mount inspection records destination and
writable status without reading secrets: only MinIO and the approved provider
credentials are mounted, with a read-only synthetic mirror and no Docker socket.
Fixed command source inventory and per-attempt argv evidence support the
no-PR/deployment criterion. Source text/hashes are included in immutable proof.

The external-resource criterion is scoped to the owned local stack, synthetic
mirror, disposable workspaces, MinIO evidence and authorized model requests.
Network denial/CONNECT proof and fixed command inspection support that boundary.
Unrelated external systems are not observable; this is not a universal claim
about effects outside that boundary.

The dedicated `acceptance-evidence` bucket contains
`bundles/<SHA256(report.json)>/{report.json,report.md,artifacts.json,published.json}`.
Writes use the existing checksum-verifying conditional ArtifactStore. JSON holds
the detailed proof; Markdown holds the checklist and publication links; the index
holds their hash/size/type references. `published.json` is written after verification.
Local ignored copies are `.local/acceptance.json`, `acceptance.md`, and
`acceptance-publication.json`. Run publications remain in the `artifacts` bucket
with their accepted names `run-report.json`, `run-report.md`, `artifact-index.json`.

## Reporting regression proof

```sh
.venv/bin/python -m pytest -q tests/integration/test_acceptance_reporting.py
make lint format-check typecheck test test-storage schemas-check shellcheck audit
make acceptance
```

The real-MinIO reporting tests verify first-step failure without an attempt keeps
persisted task text, an actual failed task wins over registry-ordered blocked tasks,
malformed attempt evidence publishes a failure report, and a partial uncommitted
CAS bundle can be completed without rewriting its existing JSON. Storage outages
remain explicit errors, covered by the existing integration test.

## Verified bundle

Verified at `2026-10-10T16:43:05.565914+00:00`: **12/12 scenarios**, **28/28 original
Section 17 criteria**, grouped **8/11/9**, zero unresolved within the documented
local boundary. Bundle root:

`s3://acceptance-evidence/bundles/b744ec381e5d4c4e955f1b360d02c3e756da1b667cd7a5cba3999905d31a8f31/`

Objects: `report.json` (1656954 bytes), `report.md` (4290 bytes), `artifacts.json`
(555 bytes), and verified `published.json`. JSON SHA256 equals the bundle digest.

| Scenarios | DAG/run ID | State/outcome |
| --- | --- | --- |
| S01 | `code_review/p4_070498103aba443795f2aafa12af8e6e` | success/passed |
| S02 | `code_review_validation_fail/p6_1887e7d980064eeba785fae635530933` | failed/validation_failed |
| S03 | `code_review_transient/p6_a1e1697da42943839541be08f3385b2f` | success/passed |
| S04 | `code_review_malformed/p6_597f7465cb0d473fa7a896039010768f` | failed/execution_failed |
| S05 | `code_review_context_failure/p6_149edb43075a4306b3762785559301e4` | failed/execution_failed |
| S06/S07/S08/S11 | `operational_proof/p5_39ded41f5ae6407484e6b1853bfbe2c4` | success/passed |
| S06/S07/S08/S11 | `operational_proof/p5_57ce2571c31e4020ba8424d7b2f3d062` | success/passed |
| S09 | `code_review_timeout/p6_08e510e02455415199030fd376500ad5` | failed/execution_failed |
| S10 | `code_review_unsafe_patch/p6_ba8d1c7a3c3f4844be65522d890e04de` | failed/execution_failed |
| S12 | `code_review_package_failure/p6_c016f03990b6457697a15bc6f64365a2` | failed/execution_failed |

Airflow URLs use `http://127.0.0.1:18080/dags/<dag>/runs/<run>`; exact publication
URIs and checksum-bearing task-log references are in the JSON/Markdown bundle.
Observed interval peaks were 2 model / 1 validation. Clear attempt 2 reused
original attempt 1 and retained the same completion reference/output keys.
Credential scans across the ten workflow runs reported counts
`99, 85, 111, 53, 43, 53, 68, 102, 89, 85`, with values absent and host auth unchanged.
Both worker inventories retained 21 files, refs and HEAD; no workspaces remained.

The original P6 gate reported 16 Node tests, six schemas, passing ShellCheck and
zero audit vulnerabilities. This checklist revision ran `make lint format-check
typecheck`, reporting strict typing in 70 files, then targeted criterion/reporting
tests (`3 passed`) and a fresh `make acceptance` (`scenario_count: 12`). The
post-run Python suite reported `85 passed`, including 17 live MinIO integration
tests. The criterion regression rejects missing/failed scenario records and
unverified implementation proof before any criteria can be marked verified.
`make status` confirmed eleven healthy running services and successful init exit.

## CI and maintainer follow-up

Coordinator verification completed `make acceptance` with twelve scenarios after
the final API body-limit and lookup-link corrections. The final evidence bundle is:

`s3://acceptance-evidence/bundles/a15fe8b4031af4be3b932b3a6668062c178038e0f86f1822121dc72a71af0049/`

Its JSON contains the 28 original criteria and run-specific proof. The genuine
workflow run was `code_review/p4_8522e1a1a06e4e79aa201a177aa32771`.
Coordinator quality checks passed with 16 Node tests, 86 Python tests, 17 separate
MinIO integration tests, six schemas and zero audit findings. Retention was
configured and read back from MinIO at 30 days for the dedicated run bucket.

CI runs credential-free quality checks and real MinIO integration tests. It does
not fabricate or mock live-provider acceptance. The maintainer must configure
required quality status checks and PR/high-risk approval in branch protection.
The coordinator records lessons in its approved AGENTS document and performs its
own final acceptance rerun before deciding on commit/push/PR.
