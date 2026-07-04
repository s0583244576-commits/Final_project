╔══════════════════════════════════════════════════════════════════════════╗
║                        TESSERA — DATA FLOW                               ║
╚══════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────┐
│                          UPSTREAM (vendor)                               │
│                                                                         │
│   tlc-publisher  ──→  MinIO (S3)  bucket: tlc-drops                    │
│   (simulated clock)   fhvhv_tripdata_2024-09.parquet  ~20M rows        │
│   /simulated_now      fhvhv_tripdata_2024-10.parquet  ~20M rows        │
│   /stats              fhvhv_tripdata_2024-11.parquet  ~20M rows        │
│                        fhvhv_tripdata_2024-12.parquet  ~20M rows       │
│   [late correction]    taxi_zone_lookup.csv                             │
│   new ETag if file     ──────────────────────────────                  │
│   was re-published     total: ~80M rows, ~2 GB                         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ S3 LIST every 30s
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION PIPELINE                                │
│                                                                         │
│  watcher.py  (loop every 30s)                                           │
│       │                                                                 │
│       ▼                                                                 │
│  check_files.py                                                         │
│    • S3 LIST → get (filename, ETag) for each file                      │
│    • compare to file_tracking table in PostgreSQL                       │
│    • detect: NEW file  OR  ETag changed (= late correction)            │
│       │                                                                 │
│       ▼  (only for new/changed files)                                   │
│  readData.py  ←  DuckDB  (fast parquet engine)                         │
│    • downloads parquet from MinIO to temp dir                           │
│    • joins with taxi_zone_lookup.csv                                    │
│    • runs 5 pre-aggregation queries (one per business question)         │
│    • writes results to PostgreSQL                                       │
│    • deletes old rows for that month first (handles corrections)        │
│    • updates file_tracking (filename, ETag, processed_at)              │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ pre-aggregated rows
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     POSTGRESQL  (persistent storage)                     │
│                                                                         │
│  file_tracking        filename | etag | processed_at                   │
│  ─────────────────────────────────────────────────────────────────────  │
│  q1_progress          (zone, hour, day_of_week) → dpm_values[]         │
│  surge_hotspots  ◄─── materialized from q1_progress per month          │
│                        pickup_zone, hour, day, median_$/mile, count     │
│  ─────────────────────────────────────────────────────────────────────  │
│  q2_progress          (zone, hour) → wait_values[] (seconds)           │
│  q2_hotspots     ◄─── top-25 zones by p90 wait at 8 AM                │
│  ─────────────────────────────────────────────────────────────────────  │
│  q3_progress          (pickup_zone, dropoff_zone) → shared counts      │
│  ─────────────────────────────────────────────────────────────────────  │
│  q4_progress          (month, licensee) → gross_fare, driver_pay       │
│  ─────────────────────────────────────────────────────────────────────  │
│  q5_medians           (pickup_borough, dropoff_borough) →              │
│                        median trip_time, sample_size per month          │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ simple SELECT, no heavy GROUP BY
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          SERVING LAYER                                   │
│                                                                         │
│  python main.py [1-5]                                                   │
│    1. GET /simulated_now  →  resolve "most recent month" / "trailing N  │
│                               days" at query time                       │
│    2. SELECT from pre-aggregated table                                  │
│    3. return result                                                     │
│                                                                         │
│  Q1 surge hotspots      → SELECT FROM surge_hotspots WHERE month=?     │
│  Q2 wait time p50/p90   → SELECT FROM q2_hotspots  (pre-computed)      │
│  Q3 shared-ride match   → SELECT FROM q3_progress  GROUP BY zone pair  │
│  Q4 driver pay share    → SELECT FROM q4_progress  GROUP BY month      │
│  Q5 rush-hour shift     → SELECT FROM q5_medians   GROUP BY borough    │
│                                                                         │
│                    ✓  every query < 1 second                           │
└─────────────────────────────────────────────────────────────────────────┘

LATE CORRECTION FLOW:
  MinIO re-PUT file with new ETag
      → check_files detects ETag mismatch
      → process_file deletes old rows for that month
      → re-aggregates from corrected parquet
      → surge_hotspots / q2_hotspots refreshed
      → next query returns corrected values  (<1s still)








הפרויקט מתחיל בבדיקה על הקבצים האם השתנו או התווספו .
בדיקה זו רצה כל 30 שניות.

שמירת הנתונים:
השתמשנו ב DUCKDB כדי לקרוא נתונים מ S3 ולבצע עליהם מניפולצית ואגריגרציות לפני שמירת נתונים ב Postgars .
לאחר מכן יצרנו לכל שאילתה טבלה מותאמת משלה לפי הנתונים שהיא זקוקה כדי לקצר את הזמן בשליפת הנתונים.
כאשר היה צוך לחשב אחוזון/ ממוצע, שמרנו במערך אחד בטבלה את כל הנתונים, ואז בזמן השליפה פירקנו את המערך , וזה כדי לשמור על אמינות.

# תיאור טבלת q1_progress:
# -------------------------------------------------------------------------
# מטרה: שומרת מצבי צבירת ביניים (Intermediate aggregation states) של "דולר למייל" (DPM)
#       לצורך עיבוד מבוסס שינויים (אינקרמנטלי) ותמיכה באנליטיקות המשך (כמו משימת Q2).
#
# רמת פירוט (Granularity): שורה אחת לכל שילוב של (filename, pickup_zone, hour_of_day, day_of_week).
#
# עמודות:
#   - filename: קובץ המקור / חודש הסימולציה המעובד כעת.
#   - pickup_zone / pickup_borough / location_id: הממדים המרחביים של מיקום תחילת הנסיעה.
#   - hour_of_day: שעת איסוף הנוסעים (0-23).
#   - day_of_week: יום בשבוע לפי תקן ISO (כאשר 1 = שני, 7 = ראשון).
#   - dpm_values: מערך (ARRAY) של *כל* ערכי ה"דולר למייל" הגולמיים עבור תא ספציפי זה.
#                 נשמר כמערך כדי לאפשר חישובי חציון (Median) או אחוזונים מדויקים
#                 בהמשך, מבלי לאבד את התפלגות הנתונים המקורית.
# -------------------------------------------------------------------------

# תיאור טבלת q2_progress:
# -------------------------------------------------------------------------
# מטרה: שומרת מצבי צבירת ביניים (Intermediate aggregation states) של זמני המתנת נוסעים 
#       (בשניות) לצורך עיבוד מבוסס שינויים (אינקרמנטלי) ותמיכה באנליטיקות המשך.
#
# רמת פירוט (Granularity): שורה אחת לכל שילוב של (filename, pickup_zone, hour_of_day).
#
# עמודות:
#   - filename: קובץ המקור / חודש הסימולציה המעובד כעת.
#   - pickup_zone / location_id: הממדים המרחביים של מיקום תחילת הנסיעה.
#   - hour_of_day: שעת איסוף הנוסעים (0-23).
#   - wait_values: מערך (ARRAY) של *כל* זמני ההמתנה הגולמיים (זמן האיסוף פחות זמן ההזמנה)
#                  בשניות עבור תא ספציפי זה. נשמר כמערך כדי לאפשר חישובי
#                  אחוזונים או חציון מדויקים בהמשך.
# -------------------------------------------------------------------------

# תיאור טבלת q3_progress:
# -------------------------------------------------------------------------
# מטרה: שומרת סיכומי ביניים של ספירת בקשות לנסיעות משותפות והתאמות מוצלחות,
#       לצורך ניתוח דפוסי הביקוש לנסיעות שיתופיות (Carpooling).
#
# רמת פירוט (Granularity): שורה אחת לכל שילוב של (filename, pickup_zone, dropoff_zone).
#
# עמודות:
#   - filename: קובץ המקור / חודש הסימולציה המעובד כעת.
#   - pickup_zone: האזור המרחבי שבו התחילה הנסיעה.
#   - dropoff_zone: האזור המרחבי שבו הסתיימה הנסיעה.
#   - shared_requests: סך כל הנסיעות שבהן התבקשה נסיעה משותפת.
#   - shared_matches: סך כל הנסיעות שבהן נמצאה התאמה מוצלחת עם נוסע נוסף.
# -------------------------------------------------------------------------

# תיאור טבלת q4_progress:
# -------------------------------------------------------------------------
# מטרה: שומרת סיכומים פיננסיים של מצבי ביניים כדי להשוות בין סך הכנסות הברוטו
#       מהנוסעים לבין סך התשלומים שהועברו בפועל לנהגים עבור כל חברה מפעילה (בעלת רישיון).
#
# רמת פירוט (Granularity): שורה אחת לכל שילוב של (filename, month, hvfhs_license_num).
#
# עמודות:
#   - filename / month: קובץ המקור וחודש לוח השנה הסימולטיבי המעובד.
#   - hvfhs_license_num: מספר הרישיון הייחודי של חברת ההסעות (למשל אובר, ליפט).
#   - total_gross_fare: סיכום כל רכיבי התשלום ששולמו ע"י הנוסעים (בסיס, אגרות, מיסים, טיפים ועמלות).
#   - total_driver_pay: סיכום סך כל התשלום נטו שחולק והועבר לנהגים.
# -------------------------------------------------------------------------

# תיאור טבלת q5_medians:
# -------------------------------------------------------------------------
# מטרה: שומרת זמני נסיעה בשעות עומס של ימי חול ברמת מאקרו, כדי לנתח 
#       מגמות של גודש תנועה (פקקים) בין רובעים שונים.
#
# רמת פירוט (Granularity): שורה אחת לכל שילוב של (filename, month, pickup_borough, dropoff_borough).
#
# מסננים שהופעלו:
#   - ימי חול בלבד (שני עד שישי, ימי ISO 1-5).
#   - עומס בוקר (07:00 - 09:59) ועומס ערב (16:00 - 18:59).
#
# עמודות:
#   - filename / month: קובץ המקור וחודש הסימולציה.
#   - pickup_borough / dropoff_borough: אזורי מאקרו מרחביים (למשל מנהטן, ברוקלין).
#   - median_time: חציון משך זמן הנסיעה במהלך שעות השיא.
#   - sample_size: סך כל הנסיעות שנכללו במדגם עבור שילוב רובעים ספציפי זה.
# -------------------------------------------------------------------------

# תיאור טבלת q2_hotspots (מודל קריאה / טבלת מטמון):
# -------------------------------------------------------------------------
# מטרה: משמשת כמטמון (Cache) שחושב מראש עבור 25 אזורי העומס הבעייתיים ביותר
#       מבחינת זמני המתנה במהלך שעת שיא הבוקר (08:00).
#
# למה הטבלה הזו נחוצה:
#   חישוב אחוזונים מדויקים (P50, P90) באמצעות פתיחת מערכים (UNNEST) על פני מיליוני
#   שורות היא פעולת מעבד (CPU) כבדה ויקרה מאוד. במקום להריץ את השאילתה הזו לפי דרישה
#   כאשר משתמש קצה מבקש מידע (מה שיוצר השהייה/Latency גבוהה), אנחנו מחשבים אותה פעם אחת
#   בזמן קליטת המידע, כך שאפליקציות או דשבורדים יוכלו לשלוף ממנה נתונים באופן מיידי.
#
# רמת פירוט: 25 השורות המובילות בלבד עבור חלון הזמן הנבחר (טווח של חודשיים).
# -------------------------------------------------------------------------

