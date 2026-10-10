"""Original criteria cannot be marked verified without execution and boundary proof."""

import importlib.util
from pathlib import Path

import pytest


def test_original_groups_require_passed_scenarios_and_inspected_proof() -> None:
    spec = importlib.util.spec_from_file_location(
        "acceptance_criteria", Path("tools/acceptance_criteria.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    records: list[dict[str, object]] = [
        {"id": f"S{index:02}", "pass": True, "observed": {"publication_uri": f"proof/{index}"}}
        for index in range(1, 13)
    ]
    proof: dict[str, object] = {
        key: {"verified": True}
        for key in (
            "generic_operators",
            "clean_reconstruction",
            "api_rejections",
            "credential_boundary",
            "command_boundary",
            "external_boundary",
        )
    }
    with pytest.raises(ValueError, match="Twelve passed"):
        module.checklist(records[:-1], proof)
    records[0]["pass"] = False
    with pytest.raises(ValueError, match="Twelve passed"):
        module.checklist(records, proof)
    records[0]["pass"] = True
    proof["generic_operators"] = {"verified": False}
    with pytest.raises(ValueError, match="implementation proof"):
        module.checklist(records, proof)
    proof["generic_operators"] = {"verified": True}
    result = module.checklist(records, proof)
    assert len(result) == 28
    assert [
        sum(item["group"] == group for item in result)
        for group in ("Functional", "Artifacts reliability", "Operations safety")
    ] == [8, 11, 9]
    assert result[0]["criterion"] == "API-triggered execution works"
    assert result[-1]["criterion"] == "No unrelated external resource mutated"
    assert every_verified(result)


def every_verified(result: list[dict[str, object]]) -> bool:
    return all(item["verified"] is True and item["evidence"] for item in result)
