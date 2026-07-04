import os
import duckdb
import sqlalchemy
from sqlalchemy.orm import sessionmaker

# 1. מחרוזות התחברות (מומלץ להשתמש ב-Environment Variables, או hardcoded אם זה מקומי)
POSTGRES_URL = os.getenv("DATABASE_URL", "postgresql://tessera:tessera-secret@localhost:5432/tessera")

# 2. יצירת ה-Engine המרכזי של SQLAlchemy (נוצר פעם אחת בלבד לכל האפליקציה)
# הגדרת pool_size עוזרת לנהל את כמות החיבורים המקבילים בזמן ה-Chaos Mode
engine = sqlalchemy.create_engine(
    POSTGRES_URL, 
    pool_size=10, 
    max_overflow=20
)

# ייצור מחלקת Session לשימוש במקרה שתרצו לעבוד עם טרנזקציות מסודרות
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ==========================================
# פונקציות עזר לייבוש (Helper Functions)
# ==========================================

def get_pg_connection():
    """מחזיר חיבור פעיל ל-PostgreSQL שנסגר אוטומטית בסיום השימוש"""
    return engine.connect()

def get_duckdb_connection():
    """מאתחל ומחזיר חיבור למנוע ה-DuckDB המקומי"""
    con = duckdb.connect()
    # כאן אפשר להגדיר הגדרות גלובליות ל-DuckDB במידת הצורך (כמו הגדרות זיכרון או תוספים)
    con.execute("SET memory_limit='2GB';")
    return con