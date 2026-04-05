from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from duckdb import DuckDBPyConnection
import duckdb

from storage import DuckLakeStorage


@dataclass
class PgConfig:
    pg_host: str
    pg_user: str
    pg_pass: str
    pg_db: str
    pg_port: Optional[int] = 5432


class DuckLakeClient:
    def __init__(
        self, ddb: DuckDBPyConnection, pg_config: PgConfig, storage: DuckLakeStorage
    ):
        self._ddb = ddb
        self._pg = pg_config
        self._storage = storage

    def connect(self):
        self._storage.create_duckdb_secret(self._ddb)
        self._ddb.execute(
            f"""
            ATTACH 'ducklake:postgres:dbname={self._pg.pg_db} host={self._pg.pg_host} user={self._pg.pg_user} password={self._pg.pg_pass}' 
            AS ducklake (DATA_PATH 's3://{self._storage.bucket}/');
            """
        )

    def register(self, view_name: str, data: object):
        self._ddb.register(view_name, data)

    def query(self, query: str):
        return self._ddb.query(query=query)

    def execute(self, command: str):
        return self._ddb.execute(command)


def get_client(storage: DuckLakeStorage) -> DuckLakeClient:
    import os

    load_dotenv()

    PG_USER = os.environ["POSTGRES_USER"]
    PG_PASS = os.environ["POSTGRES_PASSWORD"]
    PG_DB = os.environ["POSTGRES_DB"]

    pg = PgConfig(pg_host="localhost", pg_user=PG_USER, pg_pass=PG_PASS, pg_db=PG_DB)

    client = DuckLakeClient(ddb=duckdb.connect(), pg_config=pg, storage=storage)
    return client
