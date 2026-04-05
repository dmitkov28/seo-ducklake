from storage import get_s3_storage
from client import get_client

client = get_client(storage=get_s3_storage())
client.connect()
client._ddb.sql("CALL start_ui()")
input("Press Enter to stop...")
