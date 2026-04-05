from client import get_client
from storage import get_s3_storage

storage = get_s3_storage()
storage.create_bucket()

client = get_client(storage=storage)
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
