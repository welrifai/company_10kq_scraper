import sqlite3
import pandas as pd
from edgar_sp500_scraper import DB_PATH

def update_company_names():
    # Load S&P 500 tickers and company names from Wikipedia
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    df = tables[0]
    # Map CIK to company name
    cik_to_name = dict(zip(df['CIK'].astype(str), df['Security']))
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, cik FROM filings")
    rows = c.fetchall()
    updated = 0
    for filing_id, cik in rows:
        cik_str = str(int(cik)) if cik is not None else None
        company_name = cik_to_name.get(cik_str)
        if company_name:
            c.execute("UPDATE filings SET company_name=? WHERE id=?", (company_name, filing_id))
            updated += 1
    conn.commit()
    conn.close()
    print(f"Updated company_name for {updated} filings.")

def fix_mitigation_columns():
    # Ensure mitigation_idea and mitigation_rank columns exist in risks
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("ALTER TABLE risks ADD COLUMN mitigation_idea TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE risks ADD COLUMN mitigation_rank INTEGER")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()
    print("Mitigation columns ensured in risks table.")

def fix_company_name_column():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("ALTER TABLE filings ADD COLUMN company_name TEXT")
        print("company_name column added to filings table.")
    except sqlite3.OperationalError:
        print("company_name column already exists in filings table.")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_mitigation_columns()
    fix_company_name_column()
    update_company_names()
    print("Correction script complete. Now run llm_mitigation_worker.py to fill mitigations.")
