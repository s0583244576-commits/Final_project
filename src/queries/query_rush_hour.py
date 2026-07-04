import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
from sqlalchemy import text
from utils.db import engine


def run_query_5(conn, context):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS q5_medians (
            filename TEXT,
            month TEXT,
            pickup_borough TEXT,
            dropoff_borough TEXT,
            median_time DOUBLE PRECISION,
            sample_size BIGINT
        )
    """))
    conn.commit()

    # שליפת החודשים הקיימים בטבלה החדשה
    months = [
        row[0] for row in conn.execute(text("""
            SELECT DISTINCT month FROM q5_medians ORDER BY 1
        """))
    ]
    if not months:
        months = ["2024-09", "2024-10", "2024-11", "2024-12"]

    mid_idx = len(months) // 2
    split_month = months[mid_idx]
    print(f"First half: {months[:mid_idx]} | Second half: {months[mid_idx:]}")

    t = time.perf_counter()
    rows = conn.execute(text("""
        WITH grouped_parts AS (
            SELECT
                pickup_borough,
                dropoff_borough,
                CASE WHEN month < :split_month THEN 1 ELSE 2 END AS half_part,
                AVG(median_time) AS med_time,
                SUM(sample_size) AS sample_size
            FROM q5_medians
            GROUP BY pickup_borough, dropoff_borough, 
                     CASE WHEN month < :split_month THEN 1 ELSE 2 END
        ),
        p1 AS (SELECT * FROM grouped_parts WHERE half_part = 1),
        p2 AS (SELECT * FROM grouped_parts WHERE half_part = 2)
        SELECT
            p1.pickup_borough,
            p1.dropoff_borough,
            p1.med_time AS med_1,
            p2.med_time AS med_2,
            ABS(((p2.med_time - p1.med_time) / NULLIF(p1.med_time, 0)) * 100) AS pct_change,
            p1.sample_size AS size_1,
            p2.sample_size AS size_2
        FROM p1
        JOIN p2 ON p1.pickup_borough = p2.pickup_borough
               AND p1.dropoff_borough = p2.dropoff_borough
        WHERE p1.sample_size >= 100 AND p2.sample_size >= 100  -- 🌟 סינון הרעשים הסטטיסטיים!
          AND p1.pickup_borough != 'N/A' AND p1.dropoff_borough != 'N/A' -- אופציונלי: ניקוי מידע חסר
        ORDER BY pct_change DESC
        LIMIT 10
    """), {"split_month": split_month}).fetchall()
    elapsed = time.perf_counter() - t

    print(f"Time: {elapsed*1000:.1f} ms")
    print("-" * 95)
    print(f"{'Pickup Boro':<15} {'Dropoff Boro':<15} {'Med 1':>12} {'Med 2':>12} {'Change':>10} {'Size 1':>8} {'Size 2':>8}")
    print("-" * 95)
    for row in rows:
        print(f"{row.pickup_borough:<15} {row.dropoff_borough:<15} {row.med_1:>12.1f} {row.med_2:>12.1f} {row.pct_change:>9.2f}% {row.size_1:>8} {row.size_2:>8}")


if __name__ == "__main__":
    import requests
    context = requests.get("http://localhost:18910/simulated_now").json()
    with engine.connect() as conn:
        run_query_5(conn, context)
