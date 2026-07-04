import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
from sqlalchemy import text
from utils.db import engine


def run_query_2(conn, context):
    meta = conn.execute(text(
        "SELECT start_month, end_month FROM q2_hotspots LIMIT 1"
    )).fetchone()
    if not meta:
        print("No data in q2_hotspots — run the watcher first")
        return
    print(f"Window: {meta.start_month} to {meta.end_month} (max in data)")

    t = time.perf_counter()
    rows = conn.execute(text("""
        SELECT pickup_zone, p50, p90, sample_size
        FROM q2_hotspots
        ORDER BY p90 DESC
    """)).fetchall()
    elapsed = time.perf_counter() - t

    print(f"Time: {elapsed*1000:.1f} ms")
    print(f"{'Zone':<35} {'p50':>10} {'p90':>10} {'Sample':>10}")
    print("-" * 68)
    for row in rows:
        print(f"{row.pickup_zone:<35} {row.p50:>10.1f} {row.p90:>10.1f} {row.sample_size:>10}")


if __name__ == "__main__":
    import requests
    context = requests.get("http://localhost:18910/simulated_now").json()
    with engine.connect() as conn:
        run_query_2(conn, context)
