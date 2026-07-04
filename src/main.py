import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from src.utils.db import get_pg_connection

# ייבוא פונקציות השליפה מתוך תת-התיקייה queries
from src.queries.query_surge import run_query_1
from src.queries.query_wait_time import run_query_2
from src.queries.query_shared_ride import run_query_3
from src.queries.query_driver_pay import run_query_4
from src.queries.query_rush_hour import run_query_5

def get_simulator_context():
    try:
        resp = requests.get("http://localhost:18910/simulated_now").json()
        return resp
    except Exception as e:
        print(f"❌ Error communicating with simulator: {e}")
        sys.exit(1)

def main():
    # בדיקה שהמשתמש הזין מספר שאלה (למשל: python main.py 4)
    if len(sys.argv) < 2:
        print("💡 Usage: python main.py [question number 1-5]")
        sys.exit(1)
        
    question = sys.argv[1]
    
    # קבלת הקשר מהסימולטור (זמן מדומה וחודשים שפורסמו)
    context = get_simulator_context()
    
    # מיפוי פונקציות השאילתה לפי בחירת המשתמש
    query_mapping = {
        "1": run_query_1,
        "2": run_query_2,
        "3": run_query_3,
        "4": run_query_4,
        "5": run_query_5
    }
    
    if question not in query_mapping:
        print(f"❌ Question {question} does not exist or has no wrapper implemented.")
        sys.exit(1)
        
    print(f"🚀 Running query for question {question} according to simulation time...")
    
    # פתיחת חיבור יחיד מה-Pool המרכזי והעברתו לפונקציה הרלוונטית
    with get_pg_connection() as conn:
        query_mapping[question](conn, context)

if __name__ == "__main__":
    main()