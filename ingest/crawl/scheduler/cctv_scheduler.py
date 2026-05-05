import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
from cctv_ingest_lifetime import crawlOnce, loadCameraRows
POLL_SECONDS = 30


def run(con: duckdb.DuckDBPyConnection):
    camera_rows = loadCameraRows(con)
    print(f"CCTV scheduler started. cameras={len(camera_rows)} interval={POLL_SECONDS}s")
    while True:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Running CCTV crawl...")
        started = time.time()
        try:
            crawlOnce(con, camera_rows)
        except Exception as e:
            print(f"ERROR in CCTV crawl: {e}")
        elapsed = time.time() - started
        sleep_for = max(0.0, POLL_SECONDS - elapsed)
        print(f"Crawl took {elapsed:.1f}s, sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)
