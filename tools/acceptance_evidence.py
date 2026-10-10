"""Independent source checks and immutable P6 evidence publication."""

import json
from datetime import UTC, datetime
from typing import cast

from airflow_core.artifacts import ArtifactStore, canonical, digest, run_key
from airflow_core.contracts import AgentOutput, InputManifest, Publication
from deploy import LOCAL
from p5_airflow import ProofAPI
from p5_integrity import cleaned, worker
from smoke_p3 import read_report


def snapshot() -> dict[str, dict[str, object]]:
    code = (
        "import hashlib,json,subprocess; from pathlib import Path; "
        "root=Path('/mirrors/example-service.git'); "
        "files={str(p.relative_to(root)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),"
        "'mode':p.stat().st_mode & 511,'size':p.stat().st_size} "
        "for p in sorted(root.rglob('*')) if p.is_file()}; "
        "print(json.dumps({'files':files,'head':(root/'HEAD').read_text(),"
        "'refs':subprocess.check_output(['git','--git-dir',str(root),'show-ref','--head'],"
        "timeout=10).decode(),'directories':[str(p.relative_to(root)) "
        "for p in sorted(root.rglob('*')) if p.is_dir()]}))"
    )
    result = {service: worker(service, code) for service in ("worker-1", "worker-2")}
    assert result["worker-1"] == result["worker-2"]
    for entry in result.values():
        assert len(cast(dict[str, object], entry["files"])) == 21
    return result


def run_evidence(store: ArtifactStore, api: ProofAPI, dag: str, run: str) -> dict[str, object]:
    root = run_key(dag, run)
    report = read_report(store, dag, run)
    marker = Publication.model_validate_json(store.read(f"{root}/published.json")[1])
    tasks = api.tasks(dag, run)
    logs: dict[str, object] = {}
    manifests: dict[str, object] = {}
    commands: dict[str, object] = {}
    for task in tasks:
        task_id = str(task["task_id"])
        tries = task["try_number"]
        assert isinstance(tries, int)
        for number in range(1, tries + 1):
            path = f"/dags/{dag}/dagRuns/{run}/taskInstances/{task_id}/logs/{number}"
            response = api.client.get("/api/v2" + path)
            response.raise_for_status()
            ref = store.put(
                f"{root}/acceptance/task-logs/{task_id}/{number}.json",
                response.content,
                "application/json",
            )
            logs[f"{task_id}/{number}"] = ref.model_dump(mode="json")
    for ref in report.artifact_index.values():
        store.get(ref)
    for evidence in report.tasks:
        for attempt in evidence.attempts:
            assert attempt.base_sha == report.base_sha
            assert attempt.workspace_path
            log = json.loads(store.get(attempt.diagnostics["task-log.json"]))
            assert log["cleanup_live_pids"] == []
            commands[f"{evidence.task_id}/{attempt.attempt}"] = log["argv"]
            for ref in (*attempt.outputs.values(), *attempt.diagnostics.values()):
                store.get(ref)
            if attempt.outcome == "passed":
                assert evidence.completed is not None and attempt.output_manifest is not None
                output = AgentOutput.model_validate_json(store.get(attempt.output_manifest))
                assert output.task_id == evidence.task_id and output.base_sha == report.base_sha
                assert attempt.output_manifest.size <= 65536
                manifest = InputManifest.model_validate_json(
                    store.read(
                        f"{root}/steps/{evidence.task_id}/attempts/{attempt.attempt}/input.json"
                    )[1]
                )
                assert manifest.input_artifacts == attempt.input_artifacts
                manifests[evidence.task_id] = {
                    "output": attempt.output_manifest.model_dump(mode="json"),
                    "workspace": attempt.workspace_path,
                    "inputs": {
                        name: ref.model_dump(mode="json")
                        for name, ref in manifest.input_artifacts.items()
                    },
                    "validated": True,
                }
    return {
        "dag_id": dag,
        "dag_run_id": run,
        "dag_url": f"http://127.0.0.1:18080/dags/{dag}/runs/{run}",
        "state": api.request("GET", f"/dags/{dag}/dagRuns/{run}")["state"],
        "outcome": report.outcome,
        "publication_uri": f"s3://artifacts/{root}/published.json",
        "report_manifest": marker.model_dump(mode="json"),
        "task_logs": logs,
        "task_states": {str(task["task_id"]): task["state"] for task in tasks},
        "attempt_counts": {task.task_id: len(task.attempts) for task in report.tasks},
        "verified_artifact_count": len(report.artifact_index),
        "cleanup": cleaned(),
        "successful_manifests": manifests,
        "fixed_commands": commands,
    }


def record(
    identifier: str,
    fixture: str,
    request: dict[str, object],
    expected: dict[str, object],
    observed: dict[str, object],
    before: dict[str, dict[str, object]],
) -> dict[str, object]:
    after = snapshot()
    assert before == after, "Independent full mirror inventory changed"
    return {
        "id": identifier,
        "fixture_id": fixture,
        "request": request,
        "expected": expected,
        "observed": observed,
        "mirror_before": before,
        "mirror_after": after,
        "integrity_pass": True,
        "pass": True,
        "verified_at": datetime.now(UTC).isoformat(),
    }


def publish_bundle(
    records: list[dict[str, object]],
    checklist: list[dict[str, object]],
    implementation: dict[str, object],
) -> None:
    assert len(records) == 12 and len({item["id"] for item in records}) == 12
    assert len(checklist) == 28 and all(item["verified"] is True for item in checklist)
    assert {
        group: sum(item["group"] == group for item in checklist)
        for group in ("Functional", "Artifacts reliability", "Operations safety")
    } == {"Functional": 8, "Artifacts reliability": 11, "Operations safety": 9}
    passed = {str(item["id"]) for item in records if item["pass"] is True}
    assert len(passed) == 12
    for item in checklist:
        assert set(cast(list[str], item["scenarios"])) <= passed
    bundle: dict[str, object] = {
        "verified_at": datetime.now(UTC).isoformat(),
        "scenario_count": len(records),
        "scenarios": records,
        "checklist": checklist,
        "original_criteria_source": "Original plan Section 17 in conversation; exact 28 criteria",
        "criteria_count": 28,
        "unresolved_criteria": [],
        "implementation_proof": implementation,
        "report_name_mapping": {
            "report.json": "run-report.json",
            "report.md": "run-report.md",
            "artifacts.json": "artifact-index.json",
        },
    }
    data = canonical(bundle)
    prefix = "bundles/" + digest(data)
    store = ArtifactStore.deployment("http://127.0.0.1:19000", LOCAL / "secrets")
    evidence = ArtifactStore(store.client, "acceptance-evidence")
    evidence.bootstrap()
    markdown = "# P6 acceptance\n\nVerified: " + str(bundle["verified_at"]) + "\n\n"
    markdown += "| Scenario | Fixture | Publication | Verified |\n| --- | --- | --- | --- |\n"
    for item in records:
        observed = cast(dict[str, object], item["observed"])
        runs = cast(list[dict[str, object]], observed.get("runs", [observed]))
        links = "<br>".join(str(run["publication_uri"]) for run in runs)
        markdown += f"| {item['id']} | {item['fixture_id']} | {links} | PASS |\n"
    for group in ("Functional", "Artifacts reliability", "Operations safety"):
        markdown += f"\n\n## {group}\n\n"
        markdown += "\n".join(
            f"- [x] {item['id']}: {item['criterion']} "
            f"({', '.join(cast(list[str], item['scenarios']))})"
            for item in checklist
            if item["group"] == group
        )
    markdown += (
        "\n\n28/28 criteria verified; zero unresolved within the documented local boundary.\n"
    )
    refs = {
        "report.json": evidence.put(f"{prefix}/report.json", data, "application/json"),
        "report.md": evidence.put(f"{prefix}/report.md", markdown.encode(), "text/markdown"),
    }
    index: dict[str, object] = {name: ref.model_dump(mode="json") for name, ref in refs.items()}
    refs["artifacts.json"] = evidence.put(
        f"{prefix}/artifacts.json", canonical(index), "application/json"
    )
    manifest: dict[str, object] = {name: ref.model_dump(mode="json") for name, ref in refs.items()}
    for ref in refs.values():
        evidence.get(ref)
    evidence.put(f"{prefix}/published.json", canonical(manifest), "application/json")
    (LOCAL / "acceptance.json").write_bytes(data)
    (LOCAL / "acceptance.md").write_text(markdown)
    (LOCAL / "acceptance-publication.json").write_bytes(canonical(manifest))
    print(json.dumps({"scenario_count": 12, "reports": manifest}, indent=2), flush=True)
