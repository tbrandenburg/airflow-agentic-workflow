"""Bounded, SHA256-verified S3 storage with server-enforced create-if-absent writes."""

import base64
import hashlib
import json
import re
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client
from pydantic import BaseModel

from airflow_core.contracts import ArtifactRef, ContentType

LIMIT = 8_388_608


class ContractError(ValueError):
    """Deterministic policy, integrity or immutable-key violation; do not retry."""


def canonical(value: BaseModel | dict[str, object]) -> bytes:
    data = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run_key(dag: str, run: str) -> str:
    return digest(canonical({"dag_id": dag, "dag_run_id": run}))


def safe_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not re.fullmatch(r"[a-zA-Z0-9_./-]{1,240}", value)
        or path.is_absolute()
        or any(part in ("", ".", "..") for part in value.split("/"))
        or str(path) != value
    ):
        raise ContractError("Unsafe noncanonical path")
    return value


class ArtifactStore:
    def __init__(self, client: S3Client, bucket: str = "artifacts") -> None:
        self.client = client
        self.bucket = bucket

    @classmethod
    def deployment(
        cls, endpoint: str = "http://minio:9000", secrets: Path = Path("/secrets")
    ) -> "ArtifactStore":
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=(secrets / "minio_user").read_text().strip(),
            aws_secret_access_key=(secrets / "minio_password").read_text().strip(),
            region_name="us-east-1",
            config=Config(
                connect_timeout=5,
                read_timeout=15,
                retries={"max_attempts": 2, "mode": "standard"},
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        return cls(client)

    def bootstrap(self) -> None:
        try:
            self.client.create_bucket(Bucket=self.bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise

    def key(self, ref: ArtifactRef) -> str:
        uri = urlsplit(ref.uri)
        if uri.scheme != "s3" or uri.netloc != self.bucket or uri.query or uri.fragment:
            raise ContractError("Artifact reference outside deployment bucket")
        return safe_path(uri.path.removeprefix("/"))

    def put(self, key: str, data: bytes, content_type: ContentType) -> ArtifactRef:
        safe_path(key)
        if len(data) > LIMIT:
            raise ContractError("Artifact size limit exceeded")
        ref = ArtifactRef(
            uri=f"s3://{self.bucket}/{key}",
            size=len(data),
            sha256=digest(data),
            content_type=content_type,
        )
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
                ContentLength=len(data),
                ChecksumSHA256=base64.b64encode(hashlib.sha256(data).digest()).decode(),
                Metadata={"sha256": ref.sha256},
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "PreconditionFailed":
                raise
            # Identical retries are permitted; conflicting bytes or types are never replaced.
            if self.get(ref) != data:
                raise ContractError("Immutable artifact conflict") from error
        self.get(ref)
        return ref

    def get(self, ref: ArtifactRef) -> bytes:
        actual, data = self.read(self.key(ref))
        if actual != ref:
            raise ContractError("Artifact checksum, size or content type mismatch")
        return data

    def read(self, key: str) -> tuple[ArtifactRef, bytes]:
        response = self.client.get_object(Bucket=self.bucket, Key=safe_path(key))
        stream = response["Body"]
        try:
            if response["ContentLength"] > LIMIT:
                raise ContractError("Artifact size limit exceeded")
            data = stream.read(LIMIT + 1)
        finally:
            stream.close()
        checksum = digest(data)
        if len(data) != response["ContentLength"] or len(data) > LIMIT:
            raise ContractError("Artifact size mismatch")
        if response.get("Metadata", {}).get("sha256") != checksum:
            raise ContractError("Stored artifact integrity mismatch")
        ref = ArtifactRef.model_validate(
            {
                "uri": f"s3://{self.bucket}/{key}",
                "size": len(data),
                "sha256": checksum,
                "content_type": response["ContentType"],
            }
        )
        return ref, data

    def optional(self, key: str) -> tuple[ArtifactRef, bytes] | None:
        try:
            return self.read(key)
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def keys(self, prefix: str) -> list[str]:
        safe_path(prefix.rstrip("/"))
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
            if len(keys) > 1000:
                raise ContractError("Run artifact count limit exceeded")
        return sorted(keys)

    def download(self, ref: ArtifactRef, root: Path, relative: str) -> Path:
        from airflow_core.workspace import file_path

        destination = file_path(root, relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as stream:
            stream.write(self.get(ref))
        return destination
