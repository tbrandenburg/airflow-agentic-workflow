# Airflow Agentic Workflow

**Propose code changes with AI agents. Orchestrate with Airflow. Keep the evidence.**

[![Quality gates](https://github.com/tbrandenburg/airflow-agentic-workflow/actions/workflows/quality.yml/badge.svg?branch=main)](https://github.com/tbrandenburg/airflow-agentic-workflow/actions/workflows/quality.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](/LICENSE)

A local proof of concept that turns an authenticated API request into a plan,
unified diff, validation result, and review. Apache Airflow coordinates Planner,
Coder, and Reviewer through one generic OpenCode runtime. MinIO preserves the
artifacts, logs, checksums, and final JSON/Markdown reports.

**Artifact-only:** the workflow targets a synthetic read-only Git mirror. Changes
are applied in disposable workspaces; it never commits, pushes, opens a PR, or deploys.

![Successful OpenCode implementation task in the Airflow workflow graph](/docs/images/airflow-agent-workflow.png)

*A real successful run in Airflow: the selected `implement` task uses
`AgentRunOperator`; Planner and Reviewer use the same operator.*

[Quickstart](#quickstart) · [Run a change](#run-a-change) · [How it works](#how-it-works) ·
[Commands](#commands) · [Acceptance evidence](/docs/acceptance.md) · [Agent guide](/AGENTS.md)

## Why this project?

- **One runtime, three roles.** Project-owned prompts specialize the same Python
  operator and Node.js wrapper for planning, patch generation, and review.
- **Independent task workspaces.** Each step reconstructs the approved base SHA
  and verified input artifacts, including when tasks execute on different workers.
- **Honest failure outcomes.** A failed quality gate skips review, preserves its
  report, and fails the DAG. Upstream execution failures still reach the publisher.
- **Immutable evidence.** SHA256 checks, attempt-specific objects, and conditional
  completion markers prevent conflicting logical publications.
- **Bounded execution.** Pools, retries, timeouts, resource limits, scoped model
  authentication, and allowlisted model egress keep the local experiment controlled.

## Quickstart

### Requirements

Linux with Docker Engine/Compose, Git, Make, Python **3.12**, `uv`, Node.js
**24.15.0**, and ShellCheck. Allow approximately **10 GB disk** and **4 GB RAM**;
the first pinned MinIO source build can take several minutes.

Real model execution requires local OpenCode authentication for the approved
`github-copilot/gpt-6.1-sol` model. Provider access and model availability are prerequisites.

```sh
git clone https://github.com/tbrandenburg/airflow-agentic-workflow.git
```

From the cloned repository:

```sh
make install
# Explicitly authorize staging only the approved provider credential:
.venv/bin/python tools/provider_auth.py metadata
.venv/bin/python tools/provider_auth.py stage
make run
make smoke-p4
```

`stage` reads the local OpenCode auth store internally and creates a scoped copy;
it does not print credentials or modify host authentication. Run it once per fresh
deployment: it deliberately refuses to overwrite an existing staged credential.

`make run` checks free ports before startup and waits for health. `make smoke-p4`
executes the real three-role workflow and a controlled failing-gate scenario.

| Service | Local URL |
| --- | --- |
| Airflow UI / REST v2 | http://127.0.0.1:18080 |
| Run API | http://127.0.0.1:18081 |
| MinIO S3 | http://127.0.0.1:19000 |
| MinIO console | http://127.0.0.1:19001 |

The Airflow UI user is `operator`. Deployment credentials live in ignored
`infra/airflow/.local/secrets`; see [operations](/docs/operations.md) for their roles.

## Run a change

`POST /runs` accepts only the approved project, workflow, immutable SHA, model,
and bounded task text. This example loads the gateway token internally and prints
only the returned run metadata:

```sh
.venv/bin/python - <<'PY'
from pathlib import Path
import httpx

token = Path("infra/airflow/.local/secrets/gateway_token").read_text().strip()
payload = {
    "project": "example-service",
    "workflow": "code-review",
    "task": "Change service.py health() return value from ok to healthy. No other changes.",
    "base_ref": "64b998b135f36de249d9708a2307d423e935c2e8",
    "model_settings": {"model": "github-copilot/gpt-6.1-sol"},
}
with httpx.Client(base_url="http://127.0.0.1:18081", timeout=30,
                 headers={"Authorization": f"Bearer {token}"}) as client:
    response = client.post("/runs", json=payload)
    response.raise_for_status()
    print(response.json())
PY
```

The response includes `dag_id`, `dag_run_id`, and relative status/report/artifact
links. Retrieve `GET /runs/{dag_run_id}` with the same authentication to inspect
the observed DAG state and published report. `GET /runs/{dag_run_id}/artifacts`
returns the verified run-scoped artifact index.

Unknown fields return **422**, unapproved policy values **403**, missing or invalid
authentication **401**, and bodies exceeding **16 KiB** **413**. Task text is limited
to 1–500 characters. Callers cannot select scripts, prompts, fault modes, or commands.

## How it works

```text
prepare → plan → collect_context → implement → apply_patch → validate
                                                              ↓
                                                       validation_branch
                                                        ↙           ↘
                                                     review     skip_review
                                                        ↘           ↙
                                                           package

Every workflow step → publish_run → assert_outcome
```

Coder returns a structured response and unified diff. A deployment-owned script
admits the patch, checks applicability, and applies it only in a disposable checkout.
Validation independently reconstructs the source and reapplies the verified patch
before fixed formatter, lint, test, and secret-scan checks.

| Validation result | Airflow validation task | Next step | Final outcome |
| --- | --- | --- | --- |
| Checks pass | Success | Reviewer | `passed` / successful DAG |
| Checks complete but fail | Success | Bypass Reviewer, package evidence | `validation_failed` / failed DAG |
| Execution crashes or result is invalid | Failure | Dependents blocked; publisher attempts a report | `execution_failed` / failed DAG |

Reports are immutable bundles containing `run-report.json`, `run-report.md`, and
`artifact-index.json`, selected by `published.json`. They include task states,
attempt evidence, input/output checksums, patch and validation references, review,
and timing. Report publication precedes the authoritative final DAG assertion.

### Stack and source map

| Component | Location / version |
| --- | --- |
| Core operators, artifacts, policy, workspaces | [`airflow-core/airflow_core/`](/airflow-core/airflow_core) |
| Generic OpenCode wrapper | [`packages/agent-runtime/`](/packages/agent-runtime), Node 24.15.0 / OpenCode 1.18.32 |
| Versioned contracts | [`packages/agent-contracts/`](/packages/agent-contracts) |
| Synthetic project DAGs, prompts, scripts | [`projects/example-service/`](/projects/example-service) |
| Authenticated Run API | [`services/run-api/`](/services/run-api) |
| Local platform | [`infra/airflow/`](/infra/airflow), Airflow 3.3.2 / Python 3.12 |
| Storage and broker | PostgreSQL 16.10, Redis 7.2.10, pinned-source MinIO |

## Commands

| Target | Purpose |
| --- | --- |
| `make install` | Install dependencies, compile runtime, initialize local deployment identity |
| `make build` | Install and build container images |
| `make run` | Check ports, build and start the owned stack |
| `make status` | Inspect owned containers and health |
| `make stop` | Remove owned containers/networks; retain volumes and credentials |
| `make clean` | Stop the stack and remove check caches; retain deployment data |
| `make lint format-check typecheck` | Check code style, formatting and strict types |
| `make test` | Run Node/Python tests; requires live MinIO for integration tests |
| `make test-storage` | Run real-MinIO integration tests |
| `make schemas-check shellcheck audit` | Check schema drift, Bash scripts and Node advisories |
| `make acceptance` | Execute twelve real-stack acceptance scenarios and publish evidence |

Phase-specific smoke targets and offline validation commands are listed in
[AGENTS.md](/AGENTS.md). Restart a running stack with `make stop && make run`.

## Verified behavior and boundaries

The six implementation phases were exercised against the local stack.
[Acceptance evidence](/docs/acceptance.md) records **12 scenarios** and the original
**28 criteria**, including a real Planner/Coder/Reviewer run, failed-gate branching,
agent retries, malformed output, Bash failure, worker separation, mirror integrity,
pool saturation, timeout cleanup, unsafe patches, result reuse, and reporting resilience.

The [quality workflow](/.github/workflows/quality.yml) runs lint, formatting, typing,
schemas, ShellCheck, audit, Node/Python tests, and real MinIO integration. Live
provider acceptance requires separately authorized local credentials.

This is a **synthetic-source local PoC**, not a hostile-code sandbox. Workers are
nonroot, resource-bounded, and attached to an internal network. Approved model
HTTPS goes through a restricted CONNECT proxy. Scripts share the worker UID;
SimpleAuthManager is development authentication. See [operations](/docs/operations.md)
for credential, retention, concurrency, and networking details.

## Contributing

Open an [issue](https://github.com/tbrandenburg/airflow-agentic-workflow/issues) with
the expected behavior, reproduction command, and redacted evidence. Keep changes
focused and follow [AGENTS.md](/AGENTS.md). Workflow integration changes require the
full quality gate and `make acceptance`; documentation-only changes can be checked offline.

## License

[MIT](/LICENSE). README structure draws on the curated examples in
[Awesome README](https://github.com/matiassingers/awesome-readme) and the
[Make a README guide](https://www.makeareadme.com/).
