from client import get_client
from storage import get_s3_storage

if __name__ == "__main__":
    client = get_client(storage=get_s3_storage())
    client.connect()
    client.execute("DELETE FROM ducklake.main.keywords")
    client.execute("DROP TABLE ducklake.main.keywords")
    client.execute("CALL ducklake_expire_snapshots('ducklake', older_than=NOW())")
    client.execute("CALL ducklake_cleanup_old_files('ducklake', cleanup_all=true)")

    print("Cleanup completed.")
