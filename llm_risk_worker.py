import sqlite3
import time
import concurrent.futures
from edgar_sp500_scraper import DB_PATH, llm_parse_risks, save_risks_to_db_llm

def process_single_filing(filing_id, risk_factors):
    print(f"Processing filing_id={filing_id}...")
    try:
        risks_llm = llm_parse_risks(risk_factors)
        if risks_llm:
            print(f"  LLM extracted {len(risks_llm)} risks.")
            save_risks_to_db_llm(filing_id, risks_llm)
            status = 'done'
        else:
            print(f"  LLM did not extract risks.")
            status = 'error'
    except Exception as e:
        print(f"  LLM error: {e}")
        status = 'error'
    # Update llm_status
    conn2 = sqlite3.connect(DB_PATH)
    c2 = conn2.cursor()
    c2.execute("UPDATE filings SET llm_status=? WHERE id=?", (status, filing_id))
    conn2.commit()
    conn2.close()
    return status

def process_pending_llm(limit=5, sleep_time=1.0):
    """
    Process filings with llm_status='pending', run LLM, and update DB.
    limit: number of filings to process per run (for batching)
    sleep_time: seconds to sleep between LLM calls (rate limiting)
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, risk_factors FROM filings WHERE llm_status='pending' AND risk_factors IS NOT NULL LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        print("No pending filings for LLM processing.")
        return
    for filing_id, risk_factors in rows:
        print(f"Processing filing_id={filing_id}...")
        try:
            risks_llm = llm_parse_risks(risk_factors)
            if risks_llm:
                print(f"  LLM extracted {len(risks_llm)} risks.")
                save_risks_to_db_llm(filing_id, risks_llm)
                status = 'done'
            else:
                print(f"  LLM did not extract risks.")
                status = 'error'
        except Exception as e:
            print(f"  LLM error: {e}")
            status = 'error'
        # Update llm_status
        conn2 = sqlite3.connect(DB_PATH)
        c2 = conn2.cursor()
        c2.execute("UPDATE filings SET llm_status=? WHERE id=?", (status, filing_id))
        conn2.commit()
        conn2.close()
        time.sleep(sleep_time)

def process_pending_llm_parallel(limit=20, max_workers=4, sleep_time=1.0):
    """
    Process filings with llm_status='pending' in parallel using ThreadPoolExecutor.
    limit: number of filings to process per run (for batching)
    max_workers: number of parallel LLM calls
    sleep_time: seconds to sleep between LLM calls (rate limiting per worker)
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, risk_factors FROM filings WHERE llm_status='pending' AND risk_factors IS NOT NULL LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        print("No pending filings for LLM processing.")
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for filing_id, risk_factors in rows:
            futures.append(executor.submit(process_single_filing, filing_id, risk_factors))
            time.sleep(sleep_time)  # crude global rate limit
        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process pending filings with LLM in parallel.")
    parser.add_argument('--limit', type=int, default=20, help='Number of filings to process per run')
    parser.add_argument('--max-workers', type=int, default=4, help='Number of parallel LLM calls')
    parser.add_argument('--sleep', type=float, default=1.0, help='Seconds to sleep between LLM calls (per worker)')
    args = parser.parse_args()
    process_pending_llm_parallel(limit=args.limit, max_workers=args.max_workers, sleep_time=args.sleep)
