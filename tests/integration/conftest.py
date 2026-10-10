from pathlib import Path

import pytest
from airflow_core.artifacts import ArtifactStore


@pytest.fixture
def store() -> ArtifactStore:
    storage = ArtifactStore.deployment(
        "http://127.0.0.1:19000", Path("infra/airflow/.local/secrets")
    )
    storage.bootstrap()
    return storage
