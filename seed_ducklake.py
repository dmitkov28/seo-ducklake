from client import get_client

from faker import Faker
import pandas as pd
import random

from storage import get_s3_storage

fake = Faker()


def generate_keywords(n=100_000):
    competitions = ["LOW", "MEDIUM", "HIGH"]
    rows = []
    for _ in range(n):
        rows.append(
            {
                "keyword": fake.bs(),
                "year": random.choice([2024, 2025, 2026]),
                "month": random.randint(1, 12),
                "search_volume": random.randint(100, 100_000),
                "competition": random.choice(competitions),
                "competition_index": round(random.uniform(0, 1), 4),
                "cpc": round(random.uniform(0.1, 20.0), 2),
                "low_top_of_page_bid": round(random.uniform(0.1, 5.0), 2),
                "high_top_of_page_bid": round(random.uniform(5.0, 30.0), 2),
                "location_code": random.choice([2840, 2826, 2276]),  # US, UK, DE
                "language_code": random.choice([1000, 1001, 1003]),
            }
        )
    return pd.DataFrame(rows)


if __name__ == "__main__":
    client = get_client(storage=get_s3_storage())
    client.connect()
    seed_df = generate_keywords()
    client.register("seed_df", seed_df)
    client.execute("INSERT INTO ducklake.main.keywords SELECT * FROM seed_df")
    print(f"Inserted {len(seed_df)} keywords into ducklake.")
