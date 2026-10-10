# Local operations

## Deployment identity and credentials

`make install` initializes ignored `infra/airflow/.local/` files. Bootstrap prints
deployment metadata without exposing credentials. First-install environment
overrides are `POC_GATEWAY_TOKEN`, `POC_AIRFLOW_PASSWORD`, `POC_OPERATOR_PASSWORD`,
and `POC_MINIO_PASSWORD`. Existing deployment identity is retained.

| Credential file in `.local/secrets/` | Consumer |
| --- | --- |
| `operator_password` | Human Airflow UI login as `operator` |
| `gateway_token` | Caller authentication to the Run API |
| `airflow_password` | Gateway's separate `gateway:user` Airflow account |
| `minio_user`, `minio_password` | Artifact publication by operators and gateway |
| `agent_provider.json` | Only workers; approved GitHub Copilot authentication |

Stage provider authentication only after explicit authorization using
`tools/provider_auth.py stage`. It selects the required OAuth fields for one
provider, refuses overwrite, and leaves the host auth store unchanged. Each
invocation gets a fresh private XDG copy; refreshes are never synced back to the host.
The anonymous Big Pickle provider was replaced by the authorized Copilot model
after real free-tier requests returned HTTP 403.

Never print secret files or put credentials in API payloads, logs, screenshots,
Git, or artifacts. Check host/auth artifact integrity with:

```sh
PYTHONPATH=airflow-core .venv/bin/python tools/provider_auth.py verify --run-root <hashed-run>
```

## Containers, restarts, and storage

Compose provides eleven long-running services and one bounded initialization
service. PostgreSQL and Redis have no published ports. Host ports are loopback-only.
`make run` rejects occupied ports; `make stop` targets the recorded project and
preserves named volumes. Container pruning does not delete those volumes.

When a checkout replaces bind-mounted host directories while containers are
running, recreate the owned containers to refresh those mounts. Otherwise Docker
can keep the old directory inode and Airflow can show the DAG as deactivated.

```sh
make stop
make run
```

The original MinIO release image/archive was unavailable. The
[MinIO Dockerfile](/infra/airflow/Minio.Dockerfile) builds official source pinned to
`RELEASE.2025-04-22T22-12-26Z`, commit
`0d7408fc9969caf07de6a8c3a84f9fbb10a6739e`. Init sets fresh data-volume ownership
for nonroot MinIO; storage-only CI performs the same initialization.

## Artifacts and retention

Task workspaces are removed on success, failure, or cancellation. Durable evidence
uses hashed run keys, attempt-specific objects, SHA256 checks, and create-if-absent
completion markers. Fingerprints include inputs, executable helpers, policy,
prompts, compiled runtime, schemas, and dependency locks, excluding credential bytes.

Matching completion fingerprints permit logical reuse without a new model call.
An interrupted provider request may still incur model execution or billing; this
is not an exactly-once billing guarantee.

Configure and read back deployment-owned retention against running MinIO:

```sh
PYTHONPATH=airflow-core .venv/bin/python tools/retention.py --days 30
```

This rule expires objects in the dedicated `artifacts` bucket. The separate
`acceptance-evidence` bucket is retained. Per-run report names are
`run-report.json`, `run-report.md`, and `artifact-index.json`; the consolidated
acceptance bundle uses `report.json`, `report.md`, and `artifacts.json`.

## Capacity and networking

Two workers each have concurrency 2. They consume `default` plus exclusive
`worker-1`/`worker-2` queues; executor parallelism is 4. Defaults are model pool 2,
validation pool 1, and one active run per DAG. Deployment settings, applied on an
owned restart, are:

| Setting | Supported range |
| --- | --- |
| `POC_AGENT_POOL_SLOTS` | 1–2 |
| `POC_VALIDATION_POOL_SLOTS` | 1 |
| `POC_MAX_ACTIVE_RUNS` | 1–2 per DAG |

Tasks have finite retries/timeouts; deterministic contract failures and final
assertion do not retry. Workers enforce CPU/memory/PID/FD ceilings; subprocesses
enforce CPU/file/FD limits and bounded capture. Cleanup tracks owned PID birth
identities and detached descendants.

Workers attach only to the internal infrastructure network. A deployment-owned
CONNECT proxy permits port 443 to `github.com`, `api.github.com`, and
dot-delimited `*.githubcopilot.com`. Model subprocesses inherit only their fixed
proxy/model channel, with tools, external plugins, MCP, model fetch, and upgrades
disabled. They do not inherit Airflow or MinIO credentials. No Docker socket or
target-repository write credential is mounted.

Scripts remain trusted synthetic execution sharing a worker UID/network.
DNS covert channels and trusted internal-service forwarding are outside this PoC's isolation scope.

## References

- [Airflow 3.3.2 Compose](https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml)
- [OpenCode network configuration](https://opencode.ai/docs/network/)
- [GitHub Copilot allowlist](https://docs.github.com/en/copilot/reference/copilot-allowlist-reference)
- [Boto3 conditional object publication](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html)
- [Acceptance evidence](/docs/acceptance.md)
