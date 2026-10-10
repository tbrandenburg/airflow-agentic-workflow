# Local Airflow agent workflow PoC

An API-triggered, artifact-only code-review workflow running on local Airflow with
two Celery workers and MinIO. The target is a tiny deployment-owned synthetic Git
mirror. Planner, Coder and Reviewer use isolated OpenCode processes; fixed Bash
scripts collect context, admit/apply patches, validate and package results.

## Start and verify

Requires Docker Engine/Compose, Python 3.12, `uv`, Node 24.15.0, ShellCheck,
approximately 10 GB free disk and 4 GB available RAM.

```sh
make install
# After explicit authorization to reuse GitHub Copilot authentication:
.venv/bin/python tools/provider_auth.py metadata
.venv/bin/python tools/provider_auth.py stage
make run
make lint format-check typecheck test test-storage schemas-check shellcheck audit
make acceptance
make status
```

`make run` checks that ports 18080, 18081, 19000 and 19001 are free before starting
and waits up to 420 seconds for health. Restart using `make stop && make run`.
`make stop` targets the recorded Compose project and retains volumes. `make clean`
stops that project and removes check caches, retaining deployment data/secrets.
`make build` installs dependencies and builds images. `make schemas` regenerates
six schemas; `make schemas-check` detects source/schema drift.

| Service | Local URL |
| --- | --- |
| Airflow UI / REST v2 | http://127.0.0.1:18080 |
| Run gateway | http://127.0.0.1:18081 |
| MinIO S3 | http://127.0.0.1:19000 |
| MinIO console | http://127.0.0.1:19001 |

Airflow is pinned to `3.3.2-python3.12`, Node to 24.15.0, OpenCode to 1.18.32,
PostgreSQL to 16.10 and Redis to 7.2.10. The unavailable original MinIO image is
replaced by an [official-source build](/infra/airflow/Minio.Dockerfile) pinned to
release `RELEASE.2025-04-22T22-12-26Z`. Eleven long-running services have health
checks; the bounded init service exits after provisioning.

## Credentials and caller contract

Bootstrap generates credentials in ignored `infra/airflow/.local/secrets` without
printing them. UI username is `operator`; its password is `operator_password` in
that directory. Gateway token is `gateway_token`. Optional first-install values
are `POC_GATEWAY_TOKEN`, `POC_AIRFLOW_PASSWORD`, `POC_OPERATOR_PASSWORD` and
`POC_MINIO_PASSWORD`. Deployment secret directories are owner-only.

The explicitly approved model is `github-copilot/gpt-6.1-sol`, replacing the
original anonymous provider whose real requests returned 403. `provider_auth.py
stage` copies only the authorized provider into ignored `agent_provider.json`;
it never changes host authentication. Workers alone mount that read-only secret.
Each model invocation receives a private mode-0600 disposable XDG auth copy.
`provider_auth.py verify --run-root <hashed-run>` internally scans artifacts and
prints only integrity booleans/counts, never credential values.

Authenticated `POST /runs` takes:

```json
{
  "project": "example-service",
  "workflow": "code-review",
  "task": "Change service.py health() return value from ok to healthy. No other changes.",
  "base_ref": "64b998b135f36de249d9708a2307d423e935c2e8",
  "model_settings": {"model": "github-copilot/gpt-6.1-sol"}
}
```

Use `Authorization: Bearer <deployment gateway token>`. `task` is untrusted text
bounded to 1–500 characters. Unknown fields return 422; unapproved project,
workflow, SHA or model returns 403; missing/incorrect authentication returns 401.
Raw request bodies are bounded to 16 KiB before parsing, including streamed bodies;
oversized requests return 413. Trigger responses include status, report and artifact links.
Callers cannot select DAGs, scripts, prompts, paths, faults, policy or credentials.
`GET /runs/{dag_run_id}` and `/runs/{dag_run_id}/artifacts` return observed Airflow
state and verified run-scoped reports/artifacts. `/health` is unauthenticated.
The gateway uses its separate `gateway:user` Airflow account.

## Execution and evidence

```text
prepare -> plan -> collect_context -> implement -> apply_patch -> validate
  -> validation_branch -> review | skip_review -> package
  -> publish_run -> assert_outcome
```

Each script/agent independently exports allowlisted `service.py` at the approved
SHA from a read-only bare mirror and downloads declared SHA256-verified artifacts.
Prepare and plan use successive exclusive worker queues. No workspace crosses
tasks. The mirror is never a worktree or writable target. Coder only proposes a
strict JSON/unified diff; admission rejects protected/traversal paths, links,
renames, modes, binary diffs and nonallowlisted files before `git apply --check`.
Validation independently reapplies the patch and runs fixed Ruff format/lint,
pytest health checks and a credential-pattern scanner.

A normal validation check failure produces a valid failed gate with task exit 0,
skips Reviewer, packages evidence and publishes `validation_failed`; final
assertion makes the DAG fail. Execution/contract failures block dependents but
the `all_done` publisher can still produce an `execution_failed` report.
Required-success assertions prevent a successful publisher hiding workflow failure.

MinIO run keys hash canonical DAG/run identity. Each attempt persists input,
stdout/stderr, output manifest, task result and operational diagnostics. Successful
steps commit `completed.json` with conditional create-if-absent writes. Matching
registry/runtime/input fingerprints reuse completion; Airflow clear records
`model_invoked=false` separately without another output attempt. Fingerprints
cover executable Python helpers, core runtime, scripts, prompts, policy, compiled
agent runtime, source and package locks, excluding auth secret bytes.

The immutable CAS publication contains `run-report.json`, `run-report.md` and
`artifact-index.json`; `published.json` atomically selects the verified bundle.
Reports contain persisted task text, request/base/policy/runtime, states, attempt
manifests, failure/skipped diagnostics, patch reference, `validation_artifact`
reference plus inline checks, optional review and timing. Report generation
precedes publisher/assertion completion, so terminal Airflow state is obtained
separately. Requested names `report.json`, `report.md`, `artifacts.json` map to
these established run names; the consolidated acceptance bundle uses the requested
names directly. No duplicate run-report aliases are written.

Deployment-controlled retention can be configured and verified against MinIO with
`PYTHONPATH=airflow-core .venv/bin/python tools/retention.py --days 30`.
This expires run artifacts in the dedicated `artifacts` bucket; the separate
`acceptance-evidence` bucket is retained. Disposable workspaces are cleaned on task exit.

## Capacity and boundaries

Two nonroot workers have concurrency 2 and exclusive `worker-1`/`worker-2` queues
plus `default`; executor parallelism is 4. Model pool size is 2, validation pool
size 1, default active-run limit 1 per DAG. Supported deployment settings are
`POC_AGENT_POOL_SLOTS=1–2`, `POC_VALIDATION_POOL_SLOTS=1`,
`POC_MAX_ACTIVE_RUNS=1–2`, applied on an owned restart. Tasks have finite timeouts,
one bounded transient retry (contract failures and final assertion do not retry)
and workflow run deadlines. Workers have CPU/memory/PID/FD ceilings; subprocesses
have CPU/file/FD limits and bounded captured output.

Workers attach only to the internal infrastructure network. A deployment-owned
CONNECT proxy permits HTTPS 443 only to `github.com`, `api.github.com` and
dot-delimited `*.githubcopilot.com`; direct external TCP is blocked. Model
subprocesses receive only the fixed proxy environment, with tools/plugins/MCP,
model-fetch and autoupdate disabled. No Docker socket or target-repository
credentials are mounted. Owned PID/birth identities track detached descendants;
timeout/cancellation stops them and removes disposable workspaces.

This is a trusted synthetic-execution PoC, not a hostile-code sandbox. Scripts
share the worker UID/network; SimpleAuthManager is development authentication;
DNS covert channels and trusted internal-service forwarding are outside scope.
There is no target commit, push, PR or service deployment.

## Acceptance and CI

[Acceptance evidence and scenario mapping](/docs/acceptance.md) documents the
twelve real-stack scenarios, original Section 17 checklist (28 criteria grouped
8 functional / 11 artifacts reliability / 9 operations safety), inspected
operator/command/mount proof and consolidated immutable MinIO bundle. `make
acceptance` executes real API requests, polls tasks, fetches logs and verifies
storage bytes. Its happy path uses the real provider; failure/pool cases use
fixed deployment-owned deterministic agent responses through real Airflow/MinIO.
Local evidence copies are ignored under `infra/airflow/.local`.

Phase-specific regression commands remain `make smoke`, `smoke-p2`, `smoke-p3`,
`smoke-p4`, `smoke-p5`. Unit/contract tests and real MinIO integration tests run
through `make test`; `make test-storage` isolates storage integration checks.
The [CI workflow](/.github/workflows/quality.yml) checks lint, format, strict typing,
six schemas, ShellCheck, audit, Node/Python tests and actual MinIO integration.
Live provider acceptance requires separately authorized credentials and a local
owned stack. Branch protection/required PR review and high-risk merge approval
are maintainer settings; no automatic merge is configured.

## Official implementation references

- [Airflow 3.3.2 Compose](https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml),
  [branching](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/dags.html),
  [pools](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/pools.html),
  [pinned REST clear model](https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/api_fastapi/core_api/datamodels/task_instances.py).
- [OpenCode CLI](https://opencode.ai/docs/cli/),
  [network proxy](https://opencode.ai/docs/network/),
  [GitHub Copilot allowlist](https://docs.github.com/en/copilot/reference/copilot-allowlist-reference).
- [Boto3 conditional PutObject](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html),
  [Git apply](https://git-scm.com/docs/git-apply),
  [Compose internal networks](https://docs.docker.com/reference/compose-file/networks/).

Targeted Context7 research used `/apache/airflow` and `/boto/boto3`; pinned
official upstream source resolved the exact Airflow 3.3.2 REST contract.
