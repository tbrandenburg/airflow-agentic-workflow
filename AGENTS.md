# Project guidance

- Keep the workflow artifact-only: reconstruct disposable workspaces from the approved SHA and verified inputs; never mutate the target mirror.
- Run `make lint format-check typecheck test test-storage schemas-check shellcheck audit` with the owned MinIO stack available. Run `make acceptance` for workflow integration changes.
- Never read credential values into agent context. Stage only explicitly authorized provider authentication using `tools/provider_auth.py`.
- Keep modules small; use a 200-line soft limit and specification-based assertions rather than implementation-derived expectations.

## Lessons Learned

- 2026-10-10: Pitfall: A credential-free CLI smoke assumed provider availability. Prevention: Verify real inference before integration; stage only an explicitly authorized provider credential into disposable sessions, and keep host authentication unchanged.
- 2026-10-10: Pitfall: Hashing only Bash entrypoints missed executable helper changes. Prevention: Include deployment helpers, prompts, schemas and runtime dependencies in completion fingerprints.
- 2026-10-10: Pitfall: Contract rejection was classified as a failed quality gate. Prevention: Reserve `validation_failed` for a completed trustworthy gate; classify execution, policy and schema failures separately.
