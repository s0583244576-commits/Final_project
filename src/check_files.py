import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import boto3
import tempfile
from sqlalchemy import text
from utils.db import engine, get_duckdb_connection
from pipeline.readData import process_file

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:18900",
    aws_access_key_id="tessera",
    aws_secret_access_key="tessera-secret",
)

# create the file_tracking table if it doesn't exist
# table to check which files have already been processed and their ETags
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS file_tracking (
            filename     TEXT PRIMARY KEY,
            etag         TEXT NOT NULL,
            processed_at TIMESTAMP NOT NULL
        )
    """))
    conn.commit()

#   what's in the MinIO bucket?
response = s3.list_objects_v2(Bucket="tlc-drops")
bucket_files = {
    obj["Key"]: obj["ETag"]
    for obj in response.get("Contents", [])
    if obj["Key"].endswith(".parquet")
}
print(f"ב-MinIO: {list(bucket_files.keys())}")

#   what's already processed
with engine.connect() as conn:
    existing = {
        row[0]: row[1]
        for row in conn.execute(text("SELECT filename, etag FROM file_tracking"))
    }

# compare and find new or updated files
to_process = []
for filename, etag in bucket_files.items():
    if filename not in existing:
        to_process.append((filename, etag, "new"))
    elif existing[filename] != etag:
        to_process.append((filename, etag, "updated"))

if not to_process:
    print("no new or updated files to process")
else:
    print(f"\n{len(to_process)} files to process:")
    for filename, _, reason in to_process:
        print(f"  {filename} ← {reason}")

    con = get_duckdb_connection()
    with tempfile.TemporaryDirectory() as tmpdir:
        zone_path = os.path.join(tmpdir, "taxi_zone_lookup.csv")
        s3.download_file("tlc-drops", "taxi_zone_lookup.csv", zone_path)
        print()

        for filename, etag, reason in to_process:
            print(f"processing {filename} ({reason})...")
            try:
                process_file(filename, etag, engine, s3, con, tmpdir, zone_path)
            except Exception as e:
                print(f"  ⚠️  {filename} failed ({e.__class__.__name__}: {e}) — will try again in the next cycle")

    print("\ncompletion — all files processed")