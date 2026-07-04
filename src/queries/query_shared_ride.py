import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
from datetime import datetime, timedelta
from sqlalchemy import text
from utils.db import engine


def run_query_3(conn, context):
    max_month = conn.execute(text(
        "SELECT MAX(SUBSTRING(filename FROM '\\d{4}-\\d{2}')) FROM q3_progress"
    )).scalar()
    if not max_month:
        print("No data in q3_progress")
        return
    max_date = datetime.strptime(max_month, "%Y-%m")
    start_date = max_date - timedelta(days=90)
    start_month_str = start_date.strftime("%Y-%m")
    print(f"Window: {start_month_str} to {max_month} (max in data)")

    t = time.perf_counter()
    rows = conn.execute(text("""
        SELECT
            pickup_zone,
            dropoff_zone,
            SUM(shared_matches)::float / SUM(shared_requests) AS match_rate,
            SUM(shared_requests)                               AS total_requests
        FROM q3_progress
        WHERE SUBSTRING(filename FROM '\\d{4}-\\d{2}') BETWEEN :start_month AND :end_month
        GROUP BY pickup_zone, dropoff_zone
        HAVING SUM(shared_requests) >= 500
        ORDER BY match_rate DESC
        LIMIT 20
    """), {"start_month": start_month_str, "end_month": max_month}).fetchall()
    elapsed = time.perf_counter() - t

    print(f"Time: {elapsed*1000:.1f} ms")
    print(f"{'Pickup Zone':<25} {'Dropoff Zone':<25} {'Match Rate':>10} {'Requests':>10}")
    print("-" * 73)
    for row in rows:
        print(f"{row.pickup_zone:<25} {row.dropoff_zone:<25} {row.match_rate:>10.4f} {row.total_requests:>10}")


if __name__ == "__main__":
    import requests
    context = requests.get("http://localhost:18910/simulated_now").json()
    with engine.connect() as conn:
        run_query_3(conn, context)
