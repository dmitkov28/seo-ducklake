from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from duckdb import DuckDBPyConnection
import duckdb


@dataclass
class PgConfig:
    pg_host: str
    pg_user: str
    pg_pass: str
    pg_db: str
    pg_port: Optional[int] = 5432


@dataclass
class MinioConfig:
    endpoint: str
    bucket: str
    user: str
    password: str


class DuckLakeClient:
    def __init__(
        self, ddb: DuckDBPyConnection, pg_config: PgConfig, minio_config: MinioConfig
    ):
        self._ddb = ddb
        self._pg = pg_config
        self._minio = minio_config

    def _create_minio_secret(self):
        self._ddb.execute(
            f"""
            CREATE SECRET minio_secret (
                TYPE S3,
                KEY_ID '{self._minio.user}',
                SECRET '{self._minio.password}',
                ENDPOINT '{self._minio.endpoint}',
                USE_SSL false,
                URL_STYLE 'path'
            );
        """
        )

    def connect(self):
        self._create_minio_secret()
        self._ddb.execute(
            f"""
            ATTACH 'ducklake:postgres:dbname={self._pg.pg_db} host={self._pg.pg_host} user={self._pg.pg_user} password={self._pg.pg_pass}' 
            AS ducklake (DATA_PATH 's3://{self._minio.bucket}/');
            """
        )

    def register(self, view_name: str, data: object):
        self._ddb.register(view_name, data)

    def query(self, query: str):
        return self._ddb.query(query=query)

    def execute(self, command: str):
        return self._ddb.execute(command)


def get_client() -> DuckLakeClient:
    import os

    load_dotenv()

    PG_USER = os.environ["POSTGRES_USER"]
    PG_PASS = os.environ["POSTGRES_PASSWORD"]
    PG_DB = os.environ["POSTGRES_DB"]

    MINIO_USER = os.environ["MINIO_ROOT_USER"]
    MINIO_PASS = os.environ["MINIO_ROOT_PASSWORD"]
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "ducklake")

    pg = PgConfig(pg_host="localhost", pg_user=PG_USER, pg_pass=PG_PASS, pg_db=PG_DB)
    minio = MinioConfig(
        endpoint=MINIO_ENDPOINT,
        bucket=MINIO_BUCKET,
        user=MINIO_USER,
        password=MINIO_PASS,
    )

    client = DuckLakeClient(ddb=duckdb.connect(), pg_config=pg, minio_config=minio)
    return client
