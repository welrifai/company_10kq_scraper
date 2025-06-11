import subprocess
import sys
import time
import threading
import sqlite3
from edgar_sp500_scraper import DB_PATH

# Configurable parameters
SCRAPER_CMD = [sys.executable, 'edgar_sp500_scraper.py']
LLM_WORKER_CMD = [sys.executable, 'llm_risk_worker.py', '--limit', '40', '--max-workers', '8', '--sleep', '1.0']


def run_scraper():
    print("[orchestrator] Starting EDGAR scraper...")
    proc = subprocess.Popen(SCRAPER_CMD)
    proc.wait()
    print("[orchestrator] EDGAR scraper finished.")


def run_llm_worker():
    print("[orchestrator] Starting LLM risk worker (continuous)...")
    while True:
        proc = subprocess.Popen(LLM_WORKER_CMD)
        proc.wait()
        print("[orchestrator] LLM worker batch finished. Sleeping 10s before next batch...")
        time.sleep(10)


def llm_work_remaining():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM filings WHERE llm_status='pending'")
    count = c.fetchone()[0]
    conn.close()
    return count


def main():
    # Start LLM worker in a background thread
    llm_thread = threading.Thread(target=run_llm_worker, daemon=True)
    llm_thread.start()
    # Run the scraper in the main thread
    run_scraper()
    print("[orchestrator] Scraper done. Waiting for LLM worker to finish all filings...")
    # Wait for LLM worker to finish all work
    while True:
        remaining = llm_work_remaining()
        print(f"[orchestrator] LLM work remaining: {remaining}")
        if remaining == 0:
            print("[orchestrator] All filings processed. Exiting.")
            break
        time.sleep(30)

if __name__ == "__main__":
    main()
