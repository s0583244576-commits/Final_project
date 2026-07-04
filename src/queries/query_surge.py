import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
from sqlalchemy import text
from utils.db import engine


def run_query_1(conn, context):
    last_month = conn.execute(text(
        "SELECT MAX(SUBSTRING(filename FROM '\\d{4}-\\d{2}')) FROM q1_progress"
    )).scalar()
    if not last_month:
        print("No data in q1_progress")
        return
    print(f"Last month in data: {last_month}")

    t = time.perf_counter()
    rows = conn.execute(text("""
        SELECT pickup_zone, hour_of_day, day_of_week,
               median_dollars_per_mile, trip_count
        FROM surge_hotspots
        WHERE month = :month
        ORDER BY median_dollars_per_mile DESC
        LIMIT 100
    """), {"month": last_month}).fetchall()
    elapsed = time.perf_counter() - t

    print(f"Time: {elapsed*1000:.1f} ms | Results: {len(rows)} rows")
    print(f"{'Zone':<35} {'Hour':>5} {'Day':>5} {'$/mile':>10} {'Trips':>8}")
    print("-" * 68)
    for row in rows[:10]:
        print(f"{row.pickup_zone:<35} {row.hour_of_day:>5} {row.day_of_week:>5} {row.median_dollars_per_mile:>10.2f} {row.trip_count:>8}")


if __name__ == "__main__":
    import requests
    context = requests.get("http://localhost:18910/simulated_now").json()
    with engine.connect() as conn:
        run_query_1(conn, context)
