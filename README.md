# seo-ducklake

SEO keyword analytics powered by [DuckLake](https://ducklake.select/) — an open lakehouse format that uses DuckDB as the query engine, PostgreSQL as the metadata catalog, and MinIO for object storage.

## Architecture

- **DuckDB** — in-process analytical query engine
- **PostgreSQL** — DuckLake metadata catalog
- **MinIO** — S3-compatible object storage for Parquet data files
- **pgAdmin** — optional Postgres web UI (running on port 5050)

## Schema

`ducklake.main.keywords`

| Column | Type |
|---|---|
| keyword | VARCHAR |
| year | BIGINT |
| month | BIGINT |
| search_volume | BIGINT |
| competition | VARCHAR (LOW/MEDIUM/HIGH) |
| competition_index | DOUBLE |
| cpc | DOUBLE |
| low_top_of_page_bid | DOUBLE |
| high_top_of_page_bid | DOUBLE |
| location_code | INTEGER |
| language_code | INTEGER |

## Setup

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- [uv](https://github.com/astral-sh/uv) (recommended)

### 1. Configure environment

```bash
cp .env.example .env
# fill in credentials in .env
```

### 2. Start infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL (5432), pgAdmin (5050), and MinIO (9000/9001).

### 3. Install dependencies

```bash
uv sync
```

### 4. Initialize DuckLake

Creates the MinIO bucket, attaches the DuckLake catalog, and creates the keywords table:

```bash
uv run python setup_ducklake.py
```

### 5. Seed data

Generates and inserts 100k fake keyword rows:

```bash
uv run python seed_ducklake.py
```

### 6. Run demo queries

```bash
uv run python queries.py
```
