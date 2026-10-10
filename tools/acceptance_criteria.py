"""Original Section 17 wording, bound to verified execution and implementation proof."""

from typing import cast

CRITERIA = (
    ("Functional", "API-triggered execution works", "S01", ""),
    (
        "Functional",
        "Planner,coder,reviewer share one generic agent operator and one Node runtime",
        "S01",
        "generic_operators",
    ),
    (
        "Functional",
        "Project Bash scripts execute through generic script operator",
        "S01 S05",
        "generic_operators",
    ),
    ("Functional", "Planner and coder generate patch for approved fixture", "S01", ""),
    (
        "Functional",
        "Validation executes from clean reconstructed workspace",
        "S01",
        "clean_reconstruction",
    ),
    ("Functional", "Successful validation reaches reviewer", "S01", ""),
    ("Functional", "Failed validation bypasses reviewer", "S02", ""),
    (
        "Functional",
        "Failure reporting produces overall failed DAG outcome",
        "S02 S04 S05 S09 S10 S12",
        "",
    ),
    (
        "Artifacts reliability",
        "Every successful executed step produces valid compact manifest",
        "S01 S02 S03 S06 S08 S11 S12",
        "",
    ),
    (
        "Artifacts reliability",
        "Failed attempts publish diagnostic evidence where execution permits",
        "S03 S04 S05 S09 S10 S12",
        "",
    ),
    (
        "Artifacts reliability",
        "Skipped/upstream-failed represented final report",
        "S02 S04 S05",
        "",
    ),
    (
        "Artifacts reliability",
        "All substantial outputs stored MinIO",
        "S01 S02 S03 S04 S05 S06 S08 S09 S10 S11 S12",
        "",
    ),
    (
        "Artifacts reliability",
        "Artifact integrity verified checksums",
        "S01 S02 S03 S04 S05 S06 S08 S09 S10 S11 S12",
        "",
    ),
    (
        "Artifacts reliability",
        "Downstream do not require upstream local files",
        "S06",
        "clean_reconstruction",
    ),
    ("Artifacts reliability", "Worker separation succeeds", "S06", ""),
    (
        "Artifacts reliability",
        "Retries do not duplicate authoritative logical results",
        "S03 S11",
        "",
    ),
    ("Artifacts reliability", "Invalid agent output rejected", "S04", ""),
    ("Artifacts reliability", "NonzeroBash stops dependents", "S05", ""),
    (
        "Artifacts reliability",
        "Final reporting survives expected upstreamfailure",
        "S04 S05 S09 S10 S12",
        "",
    ),
    ("Operations safety", "Model pool concurrency enforced", "S08", ""),
    ("Operations safety", "Timeouts/retries bounded", "S03 S05 S09", ""),
    ("Operations safety", "Agent subprocesses terminate cancellation/timeout", "S09 S08", ""),
    (
        "Operations safety",
        "Logs/artifactmanifests preserve error evidence",
        "S03 S04 S05 S09 S10 S12",
        "",
    ),
    (
        "Operations safety",
        "APIinput cannot select unregistered prompts/scripts",
        "S01",
        "api_rejections",
    ),
    (
        "Operations safety",
        "No agent/script receives targetrepo write credentials",
        "S01 S05 S10",
        "credential_boundary",
    ),
    ("Operations safety", "No targetrepo filebranchrefcommit changes", "S07 S10", ""),
    ("Operations safety", "No PRdeployment occurs", "S01 S07", "command_boundary"),
    ("Operations safety", "No unrelated external resource mutated", "S07 S08", "external_boundary"),
)


def checklist(
    records: list[dict[str, object]], proof: dict[str, object]
) -> list[dict[str, object]]:
    indexed = {str(record["id"]): record for record in records}
    if len(indexed) != 12 or any(record["pass"] is not True for record in records):
        raise ValueError("Twelve passed scenario records required")
    results: list[dict[str, object]] = []
    for index, (group, text, scenarios, requirement) in enumerate(CRITERIA, 1):
        references = scenarios.split()
        if not set(references) <= indexed.keys():
            raise ValueError("Criterion scenario evidence missing")
        if requirement and cast(dict[str, object], proof[requirement])["verified"] is not True:
            raise ValueError("Criterion implementation proof unverified")
        results.append(
            {
                "id": f"AC{index:02}",
                "group": group,
                "criterion": text,
                "scenarios": references,
                "implementation_proof": requirement or None,
                "verified": True,
                "evidence": [
                    {"scenario": reference, "observed": indexed[reference]["observed"]}
                    for reference in references
                ],
            }
        )
    return results
