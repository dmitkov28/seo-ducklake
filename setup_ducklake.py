import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config

from client import get_client

load_dotenv()

MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASS = os.environ["MINIO_ROOT_PASSWORD"]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "ducklake")

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "ducklake")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASS,
    config=Config(signature_version="s3v4"),
)

s3.create_bucket(Bucket=MINIO_BUCKET)

print(f"Bucket created: {MINIO_BUCKET}")

client = get_client()
client.connect()


client.execute("INSTALL ducklake; INSTALL postgres; INSTALL httpfs;")
client.execute("LOAD ducklake; LOAD postgres; LOAD httpfs;")


client.execute(
    """
    CREATE TABLE IF NOT EXISTS ducklake.main.keywords (
    keyword VARCHAR,
    year BIGINT,
    month BIGINT,
    search_volume BIGINT,
    competition VARCHAR,
    competition_index DOUBLE,
    cpc DOUBLE,
    low_top_of_page_bid DOUBLE,
    high_top_of_page_bid DOUBLE,
    location_code INTEGER,
    language_code INTEGER,
);
    """
)

print("DuckLake catalog attached successfully.")
print(
    "Databases:",
    client.query("SELECT database_name FROM duckdb_databases()").fetchall(),
)
print("Tables:", client.execute("SHOW TABLES FROM ducklake.main").fetchall())
