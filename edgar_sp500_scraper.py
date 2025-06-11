import os
import requests
import sqlite3
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
import pandas as pd
from dotenv import load_dotenv
import openai

# Load environment variables from .env
load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
else:
    print("[ERROR] OPENAI_API_KEY not found in .env file. LLM features will not work.")

# Constants
BASE_DIR = 'edgar_filings'
DB_PATH = 'edgar_filings.db'
USER_AGENT = 'Wael Elrifai wael@elrifai.org'  # Replace with your real info
SEC_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept-Encoding': 'gzip, deflate'
}
SEC_TICKER_CIK_URL = 'https://www.sec.gov/files/company_tickers_exchange.json'
SEC_SUBMISSIONS_URL = 'https://data.sec.gov/submissions/CIK{cik}.json'
SEC_ARCHIVES_URL = 'https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no}/{filename}'

# Ensure base directory exists
os.makedirs(BASE_DIR, exist_ok=True)

def get_sp500_tickers():
    # Use Wikipedia for S&P 500 tickers
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    df = tables[0]
    return df[['Symbol', 'CIK']].rename(columns={'Symbol': 'ticker', 'CIK': 'cik'})

def get_filings_for_company(cik, form_types, since_date):
    url = SEC_SUBMISSIONS_URL.format(cik=str(cik).zfill(10))
    resp = requests.get(url, headers=SEC_HEADERS)
    if resp.status_code != 200:
        print(f"Failed to fetch submissions for CIK {cik}")
        return []
    data = resp.json()
    filings = []
    # Build a list of filings, skipping amendments
    for idx, form in enumerate(data['filings']['recent']['form']):
        if form not in form_types:
            continue
        if form.endswith('/A'):
            continue  # skip amendments
        filed = data['filings']['recent']['filingDate'][idx]
        if filed < since_date:
            continue
        accession = data['filings']['recent']['accessionNumber'][idx].replace('-', '')
        primary_doc = data['filings']['recent']['primaryDocument'][idx]
        filings.append({
            'cik': cik,
            'form': form,
            'filed': filed,
            'accession': accession,
            'primary_doc': primary_doc
        })
    # Keep only the latest filing for each (form, filed) pair
    unique = {}
    for f in filings:
        key = (f['form'], f['filed'])
        if key not in unique or f['accession'] > unique[key]['accession']:
            unique[key] = f
    return list(unique.values())

def download_filing(cik, accession, filename):
    url = SEC_ARCHIVES_URL.format(cik=int(cik), accession_no=accession, filename=filename)
    resp = requests.get(url, headers=SEC_HEADERS)
    if resp.status_code == 200:
        local_dir = os.path.join(BASE_DIR, str(cik), accession)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, filename)
        with open(local_path, 'wb') as f:
            f.write(resp.content)
        return local_path
    else:
        print(f"Failed to download {url}")
        return None

def extract_risk_factors(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        text = f.read()
    # Check if file is HTML/XML (iXBRL)
    if '<html' in text.lower() or '<xbrl' in text.lower() or '<document' in text.lower():
        try:
            soup = BeautifulSoup(text, 'lxml')
            # Remove script/style
            for tag in soup(['script', 'style']):
                tag.decompose()
            visible_text = soup.get_text(separator='\n')
            text_to_search = visible_text
        except Exception as e:
            print(f"[WARNING] BeautifulSoup parsing failed for {filepath}: {e}")
            text_to_search = text
    else:
        text_to_search = text
    # Find all occurrences of the Risk Factors section
    pattern = re.compile(
        r'(Item\s*1A[\s\.:\-–—]*Risk Factors.*?)(?=Item\s*1B[\s\.:\-–—]+|Item\s*2[\s\.:\-–—]+|Item\s*7[\s\.:\-–—]+|ITEM\s+[0-9A-Z][A-Z\s\.:\-–—]+)',
        re.DOTALL | re.IGNORECASE
    )
    matches = list(pattern.finditer(text_to_search))
    if matches:
        # Choose the longest match (most likely the real section, not TOC)
        best_match = max(matches, key=lambda m: len(m.group(1)))
        return best_match.group(1).strip()
    return ''

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS filings (
        id INTEGER PRIMARY KEY,
        cik TEXT,
        ticker TEXT,
        form TEXT,
        filed DATE,
        accession TEXT,
        local_path TEXT,
        risk_factors TEXT,
        llm_status TEXT DEFAULT 'pending' -- new column for LLM queue
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS risks (
        id INTEGER PRIMARY KEY,
        filing_id INTEGER,
        risk_text TEXT,
        category TEXT,
        FOREIGN KEY(filing_id) REFERENCES filings(id)
    )''')
    # Add llm_status column if it doesn't exist (for upgrades)
    try:
        c.execute("ALTER TABLE filings ADD COLUMN llm_status TEXT DEFAULT 'pending'")
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.commit()
    conn.close()

def save_filing_to_db(cik, ticker, form, filed, accession, local_path, risk_factors):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Check if this filing already exists
    c.execute('''SELECT id FROM filings WHERE cik=? AND form=? AND filed=? AND accession=?''',
              (cik, form, filed, accession))
    result = c.fetchone()
    if result:
        filing_id = result[0]
    else:
        c.execute('''INSERT INTO filings (cik, ticker, form, filed, accession, local_path, risk_factors, llm_status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (cik, ticker, form, filed, accession, local_path, risk_factors, 'pending' if risk_factors else None))
        filing_id = c.lastrowid
        conn.commit()
    conn.close()
    return filing_id

def save_risks_to_db(filing_id, risks):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for risk_text in risks:
        # Check for duplicate risk for this filing
        c.execute('''SELECT id FROM risks WHERE filing_id=? AND risk_text=?''', (filing_id, risk_text.strip()))
        if not c.fetchone():
            c.execute('''INSERT INTO risks (filing_id, risk_text, category) VALUES (?, ?, ?)''', (filing_id, risk_text.strip(), None))
    conn.commit()
    conn.close()

def save_risks_to_db_llm(filing_id, risks):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for risk in risks:
        risk_text = risk.get('risk_text', '').strip()
        summary = risk.get('summary', None)
        category = risk.get('category', None)
        if not risk_text:
            continue
        # Check for duplicate risk for this filing
        c.execute('''SELECT id FROM risks WHERE filing_id=? AND risk_text=?''', (filing_id, risk_text))
        if not c.fetchone():
            c.execute('''INSERT INTO risks (filing_id, risk_text, category) VALUES (?, ?, ?)''', (filing_id, risk_text, category))
    conn.commit()
    conn.close()

def split_risks(risk_factors_text):
    # Heuristic: split by double newlines or lines starting with '•', '-', or all caps (common in filings)
    risks = re.split(r'\n\s*\n|\n\s*[•\-]\s+|\n[A-Z][A-Z\s,\-]{10,}\n', risk_factors_text)
    # Remove empty or very short entries
    return [r.strip() for r in risks if len(r.strip()) > 40]

def llm_parse_risks(risk_factors_text):
    """
    Use OpenAI LLM to split, summarize, and classify risk factors.
    Returns a list of dicts: {risk_text, summary, category}
    """
    if not OPENAI_API_KEY:
        print("[ERROR] No OpenAI API key. Returning empty list.")
        return []
    # Remove 'Risk Factors Summary' and intro if present
    text = risk_factors_text
    summary_match = re.search(r'Risk Factors Summary.*?(?=\n[A-Z][^\n]{0,80}\n)', text, re.DOTALL | re.IGNORECASE)
    if summary_match:
        text = text[summary_match.end():]
    prompt = f"""
You are an expert in SEC filings. Given the following text from an SEC 10-K or 10-Q 'Risk Factors' section, extract each individual risk factor, provide a one-sentence summary, and classify it as one of: Market, Operational, Regulatory, Financial, Legal, Environmental, or Other.

Return ONLY a JSON array (no explanation, no preamble, no markdown) with fields: 'risk_text', 'summary', 'category'.

Risk Factors Section:
"""
    prompt += text[:6000]  # Truncate to fit model context window
    prompt += "\n\nJSON:"
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1500
        )
        import json
        text = response.choices[0].message.content
        # Use regex to extract the first JSON array
        match = re.search(r'\[.*?\]', text, re.DOTALL)
        if match:
            json_str = match.group(0)
            risks = json.loads(json_str)
            return risks
        else:
            print("[WARNING] LLM response did not contain a JSON array.")
            return []
    except Exception as e:
        print(f"[ERROR] LLM risk parsing failed: {e}")
        return []

def main():
    print('Initializing database...')
    init_db()
    print('Fetching S&P 500 tickers...')
    sp500 = get_sp500_tickers()
    since_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    form_types = ['10-K', '10-Q']
    for _, row in sp500.iterrows():
        cik = row['cik']
        ticker = row['ticker']
        print(f'Processing {ticker} ({cik})...')
        filings = get_filings_for_company(cik, form_types, since_date)
        for filing in filings:
            print(f"    Checking DB for: cik={cik}, form={filing['form']}, filed={filing['filed']}, accession={filing['accession']}")
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id FROM filings WHERE cik=? AND form=? AND filed=? AND accession=?''',
                      (cik, filing['form'], filing['filed'], filing['accession']))
            result = c.fetchone()
            conn.close()
            if result:
                print(f"  Skipping already-processed {filing['form']} filed {filing['filed']} (already in DB)")
                continue
            print(f"  Downloading {filing['form']} filed {filing['filed']}...")
            local_path = download_filing(cik, filing['accession'], filing['primary_doc'])
            if local_path:
                risk_factors = extract_risk_factors(local_path)
                if risk_factors:
                    print(f"    Extracted risk factors for {ticker} ({filing['form']} {filing['filed']})")
                    print(f"    Preview of extracted risk factors (first 500 chars):\n{risk_factors[:500]}\n{'-'*60}")
                else:
                    print(f"    No risk factors found for {ticker} ({filing['form']} {filing['filed']})")
                filing_id = save_filing_to_db(cik, ticker, filing['form'], filing['filed'], filing['accession'], local_path, risk_factors)
                if risk_factors:
                    risks_llm = llm_parse_risks(risk_factors)
                    if risks_llm:
                        print(f"    LLM extracted {len(risks_llm)} risks with summaries and categories.")
                        save_risks_to_db_llm(filing_id, risks_llm)
                    else:
                        print(f"    LLM did not extract risks, falling back to regex split.")
                        risks = split_risks(risk_factors)
                        print(f"    Found {len(risks)} individual risks.")
                        save_risks_to_db(filing_id, risks)
                else:
                    print(f"    No individual risks extracted.")
            time.sleep(0.12)  # SEC rate limit: 10 requests/sec, use 0.12s for safety

if __name__ == '__main__':
    main()
