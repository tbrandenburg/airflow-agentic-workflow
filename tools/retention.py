"""Configure deployment-owned artifact retention; acceptance evidence is retained separately."""

import argparse
from pathlib import Path

from airflow_core.artifacts import ArtifactStore
from mypy_boto3_s3.type_defs import LifecycleRuleTypeDef


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    arguments = parser.parse_args()
    if not 1 <= arguments.days <= 365:
        raise ValueError("Retention must be between 1 and 365 days")
    store = ArtifactStore.deployment("http://127.0.0.1:19000", Path("infra/airflow/.local/secrets"))
    rule: LifecycleRuleTypeDef = {
        "ID": "local-run-retention",
        "Status": "Enabled",
        "Filter": {"Prefix": ""},
        "Expiration": {"Days": arguments.days},
    }
    store.client.put_bucket_lifecycle_configuration(
        Bucket=store.bucket, LifecycleConfiguration={"Rules": [rule]}
    )
    observed = store.client.get_bucket_lifecycle_configuration(Bucket=store.bucket)
    if observed["Rules"] != [rule]:
        raise RuntimeError("MinIO retention verification failed")
    print(f"Verified {arguments.days}-day retention for dedicated {store.bucket} bucket")


if __name__ == "__main__":
    main()
