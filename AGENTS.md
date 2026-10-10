# Project guidance

## Purpose and source map

This local PoC orchestrates agent-assisted changes to a synthetic read-only Git
mirror. Airflow owns execution; OpenCode returns a patch; controlled scripts apply
and validate it in disposable workspaces. See [README.md](/README.md) for the API
example and [local operations](/docs/operations.md) for deployment controls.

- `airflow-core/airflow_core/`: generic operators, policy, artifact storage and reporting.
- `packages/agent-runtime/`: pinned Node/OpenCode subprocess wrapper and deterministic fixtures.
- `packages/agent-contracts/`: generated versioned schemas.
- `projects/example-service/`: project DAGs, prompts, registered scripts and synthetic fixture.
- `services/run-api/`: authenticated, policy-bounded API gateway.
- `infra/`: Compose platform and approved model-egress proxy.
- `tools/`: deployment, smoke, acceptance, credential-staging and retention commands.

## Setup and use

From the repository root, run `make install`. Real execution needs explicitly
authorized GitHub Copilot authentication staged once with
`.venv/bin/python tools/provider_auth.py stage`; never inspect the auth values.
Run `make run`, then `make smoke-p4` for a real workflow and failed-gate proof.
Trigger approved changes through `POST /runs`; retrieve the returned status/report
links. Do not use this implementation repository as the workflow's target mirror.

Check free ports before starting services. `make run` performs this preflight.
Manage only the recorded owned stack unless the user explicitly requests a wider
Docker operation. Use `make stop && make run` for an owned restart.

## Make targets

| Target | Behavior / prerequisite |
| --- | --- |
| `install` | Install Node/Python dependencies, compile runtime, bootstrap identity |
| `build` | Run install and build container images |
| `run` | Run install, verify ports, start/build owned services and wait for health |
| `stop` | Remove owned containers/networks; preserve volumes and auth |
| `status` | Show owned service state |
| `clean` | Stop stack, remove check caches; preserve deployment data |
| `lint` | ESLint and Ruff checks; does not fix formatting |
| `format-check` | Prettier and Ruff formatting checks |
| `typecheck` | TypeScript and strict mypy |
| `test` | Build/test Node runtime, verify schemas, run Python suite; live MinIO required |
| `test-storage` | Real-MinIO integration tests; live MinIO required |
| `schemas` | Regenerate six contract schemas |
| `schemas-check` | Reject source/schema drift |
| `shellcheck` | Check registered Bash scripts |
| `audit` | Audit Node dependencies |
| `smoke` | API/platform proof without inference; full owned stack required |
| `smoke-p2` | Artifact-backed script workflow proof |
| `smoke-p3` | Real Planner plus controlled runtime failure fixtures |
| `smoke-p4` | Real full workflow plus failing quality gate |
| `smoke-p5` | Worker separation, pools, reuse, cleanup and network proof |
| `acceptance` | Twelve real-stack scenarios; requires approved model auth |

For documentation-only or offline changes, keep services stopped and run:

```sh
make lint format-check typecheck schemas-check shellcheck audit
npm test --prefix packages/agent-runtime
.venv/bin/python -m pytest -q --ignore=tests/integration
```

For execution/storage changes, use the full gate below and appropriate live smoke
or acceptance checks. CI provisions real storage; do not replace storage/E2E checks
with mocks. Regenerate schemas after changing their source models.

## Invariants and publication hygiene

- Keep the workflow artifact-only: reconstruct disposable workspaces from the approved SHA and verified inputs; never mutate the target mirror.
- Run `make lint format-check typecheck test test-storage schemas-check shellcheck audit` with the owned MinIO stack available. Run `make acceptance` for workflow integration changes.
- Never read credential values into agent context. Stage only explicitly authorized provider authentication using `tools/provider_auth.py`.
- Keep modules small; use a 200-line soft limit and specification-based assertions rather than implementation-derived expectations.
- Preserve failed-gate versus execution-failure semantics and explicit publisher dependencies on every workflow task.
- Keep `.local/`, secret files, dependencies, compiled output and browser logs out of Git. Only curated, reviewed screenshots belong in `docs/images/`.
- Review `git status`, staged paths and the diff before committing. Never stage local auth or runtime evidence; commit/push/merge only when requested.

## Lessons Learned

- 2026-10-10: Pitfall: A credential-free CLI smoke assumed provider availability. Prevention: Verify real inference before integration; stage only an explicitly authorized provider credential into disposable sessions, and keep host authentication unchanged.
- 2026-10-10: Pitfall: Hashing only Bash entrypoints missed executable helper changes. Prevention: Include deployment helpers, prompts, schemas and runtime dependencies in completion fingerprints.
- 2026-10-10: Pitfall: Contract rejection was classified as a failed quality gate. Prevention: Reserve `validation_failed` for a completed trustworthy gate; classify execution, policy and schema failures separately.
- 2026-10-10: Pitfall: Storage-only CI skipped volume ownership initialization required by nonroot MinIO. Prevention: Initialize fresh volume permissions before starting isolated services, even when the full local stack has a separate init service.
