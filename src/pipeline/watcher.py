import time
import subprocess
import sys

# This script runs the check_files.py script every 30 seconds 
# to monitor for new or updated files in the MinIO bucket.
INTERVAL = 30

while True:
    print(f"\n[{time.strftime('%H:%M:%S')}] check files ...")
    subprocess.run([sys.executable, "src/check_files.py"])
    print(f"waiting {INTERVAL} seconds...")
    time.sleep(INTERVAL)