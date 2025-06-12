import subprocess
import sys
import time
import threading
import sqlite3
from edgar_sp500_scraper import DB_PATH

# Configurable parameters
SCRAPER_CMD = [sys.executable, 'edgar_sp500_scraper.py']
LLM_WORKER_CMD = [sys.executable, 'llm_risk_worker.py', '--limit', '40', '--max-workers', '8', '--sleep', '1.0']
MITIGATION_WORKER_CMD = [sys.executable, 'llm_mitigation_worker.py', '--limit', '40', '--max-workers', '8', '--sleep', '1.0']


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


def run_mitigation_worker():
    print("[orchestrator] Starting LLM mitigation worker (continuous)...")
    while True:
        proc = subprocess.Popen(MITIGATION_WORKER_CMD)
        proc.wait()
        print("[orchestrator] Mitigation worker batch finished. Sleeping 10s before next batch...")
        time.sleep(10)


def llm_work_remaining():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM filings WHERE llm_status='pending'")
    count = c.fetchone()[0]
    conn.close()
    return count


def mitigation_work_remaining():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM risks WHERE mitigation_idea IS NULL OR mitigation_rank IS NULL")
    count = c.fetchone()[0]
    conn.close()
    return count


def main():
    # Start LLM risk worker in a background thread
    llm_thread = threading.Thread(target=run_llm_worker, daemon=True)
    llm_thread.start()
    # Start mitigation worker in a background thread
    mitigation_thread = threading.Thread(target=run_mitigation_worker, daemon=True)
    mitigation_thread.start()
    # Run the scraper in the main thread
    run_scraper()
    print("[orchestrator] Scraper done. Waiting for LLM workers to finish all filings...")
    # Wait for LLM and mitigation workers to finish all work
    while True:
        remaining_risk = llm_work_remaining()
        remaining_mit = mitigation_work_remaining()
        print(f"[orchestrator] LLM risk work remaining: {remaining_risk}, mitigation work remaining: {remaining_mit}")
        if remaining_risk == 0 and remaining_mit == 0:
            print("[orchestrator] All filings and mitigations processed. Exiting.")
            break
        time.sleep(30)

if __name__ == "__main__":
    main()
