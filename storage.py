from typing import Protocol

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from duckdb import DuckDBPyConnection


class DuckLakeStorage(Protocol):
    bucket: str

    def create_bucket(self) -> None: ...

    def create_duckdb_secret(self, ddb: DuckDBPyConnection) -> None: ...


class MinioStorage(DuckLakeStorage):
    def __init__(self, endpoint: str, bucket_name: str, user: str, password: str):
        self._endpoint = endpoint
        self._bucket_name = bucket_name
        self._user = user
        self._password = password

    @property
    def bucket(self) -> str:
        return self._bucket_name

    def create_bucket(self) -> None:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{self._endpoint}",
            aws_access_key_id=self._user,
            aws_secret_access_key=self._password,
            config=Config(signature_version="s3v4"),
        )
        try:
            s3.create_bucket(Bucket=self._bucket_name)
        except ClientError as e:
            if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                raise

    def create_duckdb_secret(self, ddb: DuckDBPyConnection) -> None:
        ddb.execute(
            f"""
            CREATE SECRET s3_secret (
                TYPE S3,
                KEY_ID '{self._user}',
                SECRET '{self._password}',
                ENDPOINT '{self._endpoint}',
                USE_SSL false,
                URL_STYLE 'path'
            );
        """
        )


class S3Storage(DuckLakeStorage):
    def __init__(
        self, bucket_name: str, region: str, access_key_id: str, secret_access_key: str
    ):
        self._bucket_name = bucket_name
        self._region = region
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key

    @property
    def bucket(self) -> str:
        return self._bucket_name

    def create_bucket(self) -> None:
        s3 = boto3.client("s3", region_name=self._region)
        try:
            s3.create_bucket(
                Bucket=self._bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self._region},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise

    def create_duckdb_secret(self, ddb: DuckDBPyConnection) -> None:
        ddb.execute(
            f"""
            CREATE SECRET s3_secret (
                TYPE S3,
                KEY_ID '{self._access_key_id}',
                SECRET '{self._secret_access_key}',
                REGION '{self._region}',
                USE_SSL true
            );
        """
        )


def get_minio_storage() -> DuckLakeStorage:
    import os

    load_dotenv()
    MINIO_USER = os.environ["MINIO_ROOT_USER"]
    MINIO_PASS = os.environ["MINIO_ROOT_PASSWORD"]
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "ducklake")

    storage = MinioStorage(
        endpoint=MINIO_ENDPOINT,
        bucket_name=MINIO_BUCKET,
        user=MINIO_USER,
        password=MINIO_PASS,
    )
    return storage


def get_s3_storage() -> DuckLakeStorage:
    import os

    load_dotenv()
    AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
    S3_BUCKET = os.environ["S3_BUCKET"]

    s3_storage = S3Storage(
        region=AWS_REGION,
        access_key_id=AWS_ACCESS_KEY_ID,
        secret_access_key=AWS_SECRET_ACCESS_KEY,
        bucket_name=S3_BUCKET,
    )
    return s3_storage
