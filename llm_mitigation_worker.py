import sqlite3
import time
import concurrent.futures
from edgar_sp500_scraper import DB_PATH
import openai
import os
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# LLM mitigation prompt
MITIGATION_PROMPT = """
You are an expert in risk management and artificial intelligence. For the following business risk, suggest a specific way that AI could help mitigate this risk. Then, rank the usefulness of this mitigation idea as 1 (very useful and likely to be impactful), 2 (potentially useful but with caveats), or 3 (dubious, unlikely to help, or not a good fit for AI). Be critical and do not default to 1 unless it is truly highly impactful. Return your answer as a JSON object with fields: mitigation_idea (string), mitigation_rank (integer: 1, 2, or 3).

Risk:
"""

def llm_mitigation_idea(risk_text):
    if not OPENAI_API_KEY:
        print("[ERROR] No OpenAI API key. Returning None.")
        return None, None
    prompt = MITIGATION_PROMPT + risk_text + "\n\nJSON:"
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=300
        )
        import json
        import re
        text = response.choices[0].message.content
        match = re.search(r'\{.*?\}', text, re.DOTALL)
        if match:
            mitigation = json.loads(match.group(0))
            return mitigation.get('mitigation_idea'), mitigation.get('mitigation_rank')
        else:
            print("[WARNING] LLM mitigation response did not contain a JSON object.")
            return None, None
    except Exception as e:
        print(f"[ERROR] LLM mitigation failed: {e}")
        return None, None

def process_single_mitigation(risk_id, risk_text):
    print(f"Processing risk_id={risk_id}...")
    idea, rank = llm_mitigation_idea(risk_text)
    if idea:
        print(f"  Mitigation: {idea} (rank {rank})")
    else:
        print(f"  No mitigation idea returned.")
    conn2 = sqlite3.connect(DB_PATH)
    c2 = conn2.cursor()
    c2.execute("UPDATE risks SET mitigation_idea=?, mitigation_rank=? WHERE id=?", (idea, rank, risk_id))
    conn2.commit()
    conn2.close()
    return idea, rank

def process_pending_mitigations_parallel(limit=40, max_workers=10, sleep_time=1.0):
    print(f"[mitigation_worker] Starting batch: limit={limit}, max_workers={max_workers}, sleep={sleep_time}")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, risk_text FROM risks WHERE mitigation_idea IS NULL OR mitigation_rank IS NULL LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        print("[mitigation_worker] No risks pending mitigation ideas.")
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for risk_id, risk_text in rows:
            print(f"[mitigation_worker] Queueing risk_id={risk_id}")
            futures.append(executor.submit(process_single_mitigation, risk_id, risk_text))
            time.sleep(sleep_time)
        for future in concurrent.futures.as_completed(futures):
            future.result()
    print(f"[mitigation_worker] Batch complete. Processed {len(rows)} risks.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process pending risk mitigations with LLM in parallel.")
    parser.add_argument('--limit', type=int, default=40, help='Number of risks to process per run')
    parser.add_argument('--max-workers', type=int, default=10, help='Number of parallel LLM calls')
    parser.add_argument('--sleep', type=float, default=1.0, help='Seconds to sleep between LLM calls (per worker)')
    args = parser.parse_args()
    process_pending_mitigations_parallel(limit=args.limit, max_workers=args.max_workers, sleep_time=args.sleep)
