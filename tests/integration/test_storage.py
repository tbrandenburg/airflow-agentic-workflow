from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from airflow_core.artifacts import ArtifactStore, ContractError
from pydantic import ValidationError


def test_roundtrip_rejects_corruption_size_type_and_uri(
    store: ArtifactStore, tmp_path: Path
) -> None:
    ref = store.put(f"proof/{uuid4().hex}/object.json", b'{"hello":"world"}', "application/json")
    assert store.get(ref) == b'{"hello":"world"}'
    assert store.download(ref, tmp_path, "inputs/object.json").read_bytes() == store.get(ref)
    for update in (
        {"sha256": "0" * 64},
        {"size": ref.size + 1},
        {"content_type": "text/plain"},
        {"uri": "s3://other-bucket/object.json"},
        {"uri": "s3://artifacts/a/../object.json"},
    ):
        with pytest.raises(ContractError):
            store.get(ref.model_copy(update=update))
    with pytest.raises(ValidationError):
        type(ref).model_validate({**ref.model_dump(), "content_type": "text/html"})
    with pytest.raises(ContractError):
        store.download(ref, tmp_path, "../escape")
    with pytest.raises(ContractError):
        store.put("proof/oversized", b"x" * 8_388_609, "text/plain")
    with pytest.raises(ContractError):
        store.put(store.key(ref), b"conflicting data", "application/json")
    assert store.get(ref) == b'{"hello":"world"}'


def test_real_minio_conditional_write_race(store: ArtifactStore) -> None:
    key = f"proof/{uuid4().hex}/race"
    barrier = Barrier(2)

    def contender(data: bytes) -> str:
        barrier.wait(timeout=5)
        try:
            store.put(key, data, "text/plain")
            return "winner"
        except ContractError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contender, [b"first", b"second"]))
    assert sorted(results) == ["conflict", "winner"]
    assert store.read(key)[1] in (b"first", b"second")


def test_stored_corruption_rejected(store: ArtifactStore) -> None:
    key = f"proof/{uuid4().hex}/corrupt"
    store.client.put_object(
        Bucket=store.bucket,
        Key=key,
        Body=b"corrupt",
        ContentType="text/plain",
        Metadata={"sha256": "0" * 64},
        IfNoneMatch="*",
    )
    with pytest.raises(ContractError, match="integrity"):
        store.read(key)
