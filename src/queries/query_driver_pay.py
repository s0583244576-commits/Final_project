import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
from sqlalchemy import text
from utils.db import engine

LICENSEE = {"HV0003": "Uber", "HV0005": "Lyft", "HV0004": "Via"}


def run_query_4(conn, context):
    t = time.perf_counter()
    rows = conn.execute(text("""
        SELECT
            month,
            hvfhs_license_num,
            SUM(total_gross_fare)                                AS gross_fare,
            SUM(total_driver_pay)                                AS driver_pay,
            SUM(total_driver_pay) / NULLIF(SUM(total_gross_fare), 0) * 100 AS driver_pay_pct
        FROM q4_progress
        GROUP BY month, hvfhs_license_num
        ORDER BY month, hvfhs_license_num
    """)).fetchall()
    elapsed = time.perf_counter() - t

    print(f"Time: {elapsed*1000:.1f} ms\n")
    print(f"{'Month':<10} {'Company':<8} {'Gross':>15} {'Driver Pay':>15} {'Pct':>8}")
    print("-" * 60)
    for row in rows:
        name = LICENSEE.get(row.hvfhs_license_num, row.hvfhs_license_num)
        print(f"{row.month:<10} {name:<8} {row.gross_fare:>15,.0f} {row.driver_pay:>15,.0f} {row.driver_pay_pct:>7.1f}%")


if __name__ == "__main__":
    import requests
    context = requests.get("http://localhost:18910/simulated_now").json()
    with engine.connect() as conn:
        run_query_4(conn, context)
