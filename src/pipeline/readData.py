import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Float, BigInteger
from utils.db import POSTGRES_URL as PG_URL

_pg_ext_ready = False


def _setup_pg_extension(con):
    """מנסה לחבר DuckDB ישירות ל-PostgreSQL. מחזיר True אם הצליח."""
    global _pg_ext_ready
    if _pg_ext_ready:
        return True
    try:
        con.execute("LOAD postgres")
        con.execute(f"ATTACH IF NOT EXISTS '{PG_URL}' AS pg (TYPE POSTGRES)")
        _pg_ext_ready = True
        print("  DuckDB → PostgreSQL ישיר: פעיל")
        return True
    except Exception:
        print("  DuckDB → PostgreSQL ישיר: לא זמין, משתמש ב-pandas fallback")
        return False


def _save(df, table, engine, con, dtype=None):
    """כותב DataFrame ל-PostgreSQL — דרך DuckDB אם אפשר, אחרת דרך pandas."""
    if _pg_ext_ready:
        try:
            con.register("_tmp_write", df)
            con.execute(f"INSERT INTO pg.{table} SELECT * FROM _tmp_write")
            con.unregister("_tmp_write")
            return
        except Exception:
            pass
    # psycopg2 לא מכיר numpy types — ממיר עמודות-רשימה לפני הכתיבה
    # DuckDB מחזיר numpy.ndarray (לא list) עבור array_agg, לכן בודקים שניהם
    for col in df.columns:
        if df[col].dtype == object and len(df) > 0:
            first = df[col].iloc[0]
            if isinstance(first, (list, np.ndarray)):
                df[col] = df[col].apply(
                    lambda arr: [float(x) for x in arr] if arr is not None else arr
                )
    df.to_sql(table, engine, if_exists="append", index=False, dtype=dtype)


def process_file(filename, etag, engine, s3, con, tmpdir, zone_path):
    _setup_pg_extension(con)

    month = filename.replace("fhvhv_tripdata_", "").replace(".parquet", "")
    local_path = os.path.join(tmpdir, filename).replace("\\", "/")
    zp = zone_path.replace("\\", "/")

    print(f"  downloading {filename}...")
    s3.download_file("tlc-drops", filename, local_path)

    # create progress tables if they don't exist, and delete any existing rows for this file
    # Separate table creation per query
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS q1_progress (
                filename TEXT, pickup_zone TEXT, pickup_borough TEXT,
                location_id INT, hour_of_day INT, day_of_week INT,
                dpm_values DOUBLE PRECISION[]
            );
            CREATE TABLE IF NOT EXISTS q2_progress (
                filename TEXT, pickup_zone TEXT, location_id INT,
                hour_of_day INT, wait_values DOUBLE PRECISION[]
            );
            CREATE TABLE IF NOT EXISTS q3_progress (
                filename TEXT, pickup_zone TEXT, dropoff_zone TEXT,
                shared_requests BIGINT, shared_matches BIGINT
            );
            CREATE TABLE IF NOT EXISTS q4_progress (
                filename TEXT, month TEXT, hvfhs_license_num TEXT,
                total_gross_fare DOUBLE PRECISION, total_driver_pay DOUBLE PRECISION
            );
            CREATE TABLE IF NOT EXISTS q5_progress (
                filename TEXT, pickup_date DATE, pickup_borough TEXT,
                dropoff_borough TEXT, trip_time_values DOUBLE PRECISION[]
            );
            CREATE TABLE IF NOT EXISTS q2_hotspots (
                pickup_zone TEXT,
                p50         DOUBLE PRECISION,
                p90         DOUBLE PRECISION,
                sample_size BIGINT,
                start_month TEXT,
                end_month   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_q2_hour_zone
                ON q2_progress (hour_of_day, location_id);
            CREATE INDEX IF NOT EXISTS idx_q3_progress_filename
                ON q3_progress (filename);
            CREATE TABLE IF NOT EXISTS q5_medians (
                filename TEXT,
                month TEXT,
                pickup_borough TEXT,
                dropoff_borough TEXT,
                median_time DOUBLE PRECISION,
                sample_size BIGINT
            );
            CREATE TABLE IF NOT EXISTS surge_hotspots (
                pickup_zone TEXT,
                hour_of_day INT,
                day_of_week INT,
                median_dollars_per_mile DOUBLE PRECISION,
                trip_count BIGINT,
                month TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_surge_month ON surge_hotspots (month);
        """))
        for table in ["q1_progress", "q2_progress", "q3_progress", "q4_progress"]:
            conn.execute(text(
                f"DELETE FROM {table} WHERE filename = :f"
            ), {"f": filename})
        conn.commit()

    # Q1: All dollars_per_mile values by (zone, hour, day_of_week)
    # isodow: 1=Monday ... 7=Sunday (consistent with ISO, independent of DB)
    # Stores pre-aggregated trip cells for each file. 
    # dpm_values holds an array of all raw dollars-per-mile values per (zone, hour, day) 
    # to preserve the full distribution for exact median calculations in subsequent steps.
    df1 = con.execute(f"""
        WITH zones AS (
            SELECT LocationID, Zone AS pickup_zone, Borough AS pickup_borough
            FROM read_csv_auto('{zp}')
        )
        SELECT
            '{filename}'                                              AS filename,
            z.pickup_zone,
            z.pickup_borough,
            t.PULocationID                                            AS location_id,
            hour(t.pickup_datetime)                                   AS hour_of_day,
            isodow(t.pickup_datetime)                                 AS day_of_week,
            array_agg(t.base_passenger_fare / NULLIF(t.trip_miles, 0)) AS dpm_values
        FROM read_parquet('{local_path}') t
        JOIN zones z ON t.PULocationID = z.LocationID
        WHERE t.base_passenger_fare IS NOT NULL AND t.trip_miles > 0
        GROUP BY z.pickup_zone, z.pickup_borough, t.PULocationID,
                 hour_of_day, day_of_week
    """).df()
    _save(df1, "q1_progress", engine, con, dtype={"dpm_values": ARRAY(Float)})
    print(f"  Q1: {len(df1)} rows")

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM surge_hotspots WHERE month = :m"), {"m": month})
        conn.execute(text("""
            INSERT INTO surge_hotspots
                   (pickup_zone, hour_of_day, day_of_week,
                    median_dollars_per_mile, trip_count, month)
            WITH unnested AS (
                SELECT pickup_zone, UNNEST(dpm_values) AS dpm,
                       hour_of_day, day_of_week
                FROM q1_progress
                WHERE SUBSTRING(filename FROM '\\d{4}-\\d{2}') = :m
            )
            SELECT
                pickup_zone, hour_of_day, day_of_week,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dpm) AS median_dollars_per_mile,
                COUNT(dpm) AS trip_count,
                :m AS month
            FROM unnested
            WHERE dpm IS NOT NULL
            GROUP BY pickup_zone, hour_of_day, day_of_week
        """), {"m": month})
        conn.commit()
    print(f"  Q1 surge_hotspots cache עודכן ({month})")

    # Stores pre-aggregated passenger wait times (in seconds) for each file.
    # wait_values holds an array of all raw wait times per (zone, hour) 
    # to preserve the full distribution for exact percentile calculations later.
    df2 = con.execute(f"""
        WITH zones AS (
            SELECT LocationID, Zone AS pickup_zone
            FROM read_csv_auto('{zp}')
        )
        SELECT
            '{filename}'                                           AS filename,
            z.pickup_zone,
            t.PULocationID                                         AS location_id,
            hour(t.pickup_datetime)                                AS hour_of_day,
            array_agg(
                epoch(t.pickup_datetime) - epoch(t.request_datetime)
            )                                                      AS wait_values
        FROM read_parquet('{local_path}') t
        JOIN zones z ON t.PULocationID = z.LocationID
        WHERE t.request_datetime IS NOT NULL
          AND t.pickup_datetime >= t.request_datetime
        GROUP BY z.pickup_zone, t.PULocationID, hour_of_day
    """).df()
    _save(df2, "q2_progress", engine, con, dtype={"wait_values": ARRAY(Float)})
    print(f"  Q2: {len(df2)} rows")

    # Stores pre-aggregated shared ride counts (requests vs matches) for each file.
    # Aggregated by pickup and dropoff zone pairs to analyze route-based carpool efficiency.
    df3 = con.execute(f"""
        WITH zones AS (SELECT LocationID, Zone FROM read_csv_auto('{zp}'))
        SELECT
            '{filename}'                                            AS filename,
            pu.Zone                                                 AS pickup_zone,
            dz.Zone                                                 AS dropoff_zone,
            COUNT(*) FILTER (WHERE shared_request_flag = 'Y')      AS shared_requests,
            COUNT(*) FILTER (WHERE shared_match_flag  = 'Y')       AS shared_matches
        FROM read_parquet('{local_path}') t
        JOIN zones pu ON t.PULocationID = pu.LocationID
        JOIN zones dz ON t.DOLocationID = dz.LocationID
        WHERE shared_request_flag = 'Y'
        GROUP BY pu.Zone, dz.Zone
    """).df()
    _save(df3, "q3_progress", engine, con,
          dtype={"shared_requests": BigInteger(), "shared_matches": BigInteger()})
    print(f"  Q3: {len(df3)} rows")

    # Stores monthly financial summaries (gross passenger fares vs. driver payouts) per licensee.
    # Aggregates all fare components (tolls, tips, taxes, etc.) to calculate total market revenue.
    df4 = con.execute(f"""
        SELECT
            '{filename}'       AS filename,
            '{month}'          AS month,
            hvfhs_license_num,
            SUM(base_passenger_fare + tolls + bcf + sales_tax
                + congestion_surcharge + airport_fee + tips) AS total_gross_fare,
            SUM(driver_pay)                                   AS total_driver_pay
        FROM read_parquet('{local_path}')
        GROUP BY hvfhs_license_num
    """).df()
    _save(df4, "q4_progress", engine, con)
    print(f"  Q4: {len(df4)} rows")

    # Stores weekday rush-hour trip time medians and sample sizes aggregated by borough pairs.
    # Filters for Mon-Fri morning (7-10 AM) and evening (4-7 PM) peak windows to measure macro congestion.
    df5 = con.execute(f"""
        WITH zones AS (SELECT LocationID, Borough FROM read_csv_auto('{zp}'))
        SELECT
            '{filename}'                                    AS filename,
            '{month}'                                       AS month,
            pu.Borough                                      AS pickup_borough,
            dz.Borough                                      AS dropoff_borough,
            MEDIAN(t.trip_time)                             AS median_time,
            COUNT(t.trip_time)                              AS sample_size
        FROM read_parquet('{local_path}') t
        JOIN zones pu ON t.PULocationID = pu.LocationID
        JOIN zones dz ON t.DOLocationID = dz.LocationID
        WHERE isodow(t.pickup_datetime) BETWEEN 1 AND 5
          AND (
               (hour(t.pickup_datetime) >= 7  AND hour(t.pickup_datetime) < 10)
            OR (hour(t.pickup_datetime) >= 16 AND hour(t.pickup_datetime) < 19)
          )
          AND t.trip_time IS NOT NULL
        GROUP BY pu.Borough, dz.Borough
    """).df()
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM q5_medians WHERE filename = :f"), {"f": filename})
        conn.commit()
        
    _save(df5, "q5_medians", engine, con)
    print(f"  Q5 (Cached): {len(df5)} rows ready")

    # עדכן file_tracking
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO file_tracking (filename, etag, processed_at)
            VALUES (:f, :e, NOW())
            ON CONFLICT (filename) DO UPDATE
                SET etag         = EXCLUDED.etag,
                    processed_at = EXCLUDED.processed_at
        """), {"f": filename, "e": etag})
        conn.commit()

    # Rebuilds the Q2 hotspots cache table. Pre-computes heavy P50/P90 percentile 
    # calculations at ingestion time to allow instant, sub-millisecond API/Dashboard queries.
    with engine.connect() as conn:
        max_month = conn.execute(text(
            "SELECT MAX(SUBSTRING(filename FROM '\\d{4}-\\d{2}')) FROM q2_progress"
        )).scalar()
        if max_month:
            start_month = (
                datetime.strptime(max_month, "%Y-%m") - timedelta(days=30)
            ).strftime("%Y-%m")
            conn.execute(text("TRUNCATE q2_hotspots"))
            conn.execute(text("""
                INSERT INTO q2_hotspots
                       (pickup_zone, p50, p90, sample_size, start_month, end_month)
                WITH unnested AS (
                    SELECT pickup_zone, UNNEST(wait_values) AS wait_second
                    FROM q2_progress
                    WHERE hour_of_day = 8
                      AND SUBSTRING(filename FROM '\\d{4}-\\d{2}')
                          BETWEEN :s AND :e
                )
                SELECT
                    pickup_zone,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wait_second) AS p50,
                    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY wait_second) AS p90,
                    COUNT(wait_second)                                        AS sample_size,
                    :s, :e
                FROM unnested
                GROUP BY pickup_zone
                ORDER BY p90 DESC
                LIMIT 25
            """), {"s": start_month, "e": max_month})
            conn.commit()
            print(f"  Q2 hotspots cache updated ({start_month} until {max_month})")

    os.unlink(local_path)
    print(f"  {filename} ← done\n")
