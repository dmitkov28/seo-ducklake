from time import perf_counter

from client import DuckLakeClient, get_client
from storage import get_s3_storage

if __name__ == "__main__":
    client = get_client(storage=get_s3_storage())
    client.connect()

    TABLE = "ducklake.main.keywords"

    def timed_query(label, sql, client: DuckLakeClient = client):
        start = perf_counter()
        result = client.query(sql).fetchall()
        elapsed = perf_counter() - start
        print(f"{label} in {elapsed:.3f} seconds")
        for row in result[:10]:
            print("  ", row)
        print()
        return result

    # Total keyword count
    timed_query("Counted keywords", f"SELECT COUNT(*) AS total FROM {TABLE}")

    # Top 10 keywords by search volume
    timed_query(
        "Queried top 10 keywords by search volume",
        f"SELECT keyword, search_volume FROM {TABLE} ORDER BY search_volume DESC LIMIT 10",
    )

    # Average CPC by competition level
    timed_query(
        "Queried avg CPC by competition",
        f"SELECT competition, ROUND(AVG(cpc), 2) AS avg_cpc FROM {TABLE} GROUP BY competition ORDER BY avg_cpc DESC",
    )

    # Monthly search volume trend
    timed_query(
        "Queried monthly search volume trend",
        f"SELECT year, month, SUM(search_volume) AS total_volume FROM {TABLE} GROUP BY year, month ORDER BY year, month",
    )

    # High-value keywords (high volume + high CPC)
    timed_query(
        "Queried high-value keywords",
        f"SELECT keyword, search_volume, cpc FROM {TABLE} WHERE search_volume > 1000 AND cpc > 1.0 ORDER BY search_volume * cpc DESC LIMIT 10",
    )

    # Competition distribution
    timed_query(
        "Queried competition distribution",
        f"SELECT competition, COUNT(*) AS cnt FROM {TABLE} GROUP BY competition ORDER BY cnt DESC",
    )

    # Top bid spread keywords (biggest gap between low and high page bids)
    timed_query(
        "Queried top bid spread keywords",
        f"SELECT keyword, low_top_of_page_bid, high_top_of_page_bid, high_top_of_page_bid - low_top_of_page_bid AS spread FROM {TABLE} WHERE high_top_of_page_bid IS NOT NULL ORDER BY spread DESC LIMIT 10",
    )
