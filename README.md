# SEC 10-K/10-Q Risk Factor Extraction & AI Mitigation Pipeline

This project automates the extraction, analysis, and AI-powered mitigation suggestion for risk factors disclosed in SEC 10-K and 10-Q filings for S&P 500 companies.

## Features
- **Automated Download:** Fetches the latest 10-K and 10-Q filings for all S&P 500 companies from the SEC EDGAR system.
- **Robust Extraction:** Extracts the full "Item 1A. Risk Factors" section from each filing, handling HTML/iXBRL and plain text formats.
- **LLM-Powered Analysis:**
  - Splits the risk section into individual risk factors using an LLM (OpenAI GPT-3.5 Turbo).
  - Summarizes and classifies each risk factor.
  - Suggests AI-driven mitigation ideas for each risk and ranks their usefulness (1=very useful, 2=potentially useful, 3=dubious).
- **Parallel Processing:** Risk extraction and mitigation suggestion are performed in parallel for high throughput.
- **Database Storage:** All filings, risks, and mitigations are stored in a local SQLite database for easy querying and analysis.

## Project Structure

- `edgar_sp500_scraper.py` — Main script for downloading filings and extracting risk factors.
- `llm_risk_worker.py` — Splits and classifies risk factors using an LLM (parallelized).
- `llm_mitigation_worker.py` — Suggests and ranks AI mitigations for each risk (parallelized).
- `orchestrator.py` — Runs the full pipeline: scraping, risk extraction, and mitigation in parallel.
- `correction_script.py` — Ensures database schema is up to date and populates company names.
- `edgar_filings.db` — SQLite database storing all filings, risks, and mitigations.
- `edgar_filings/` — Directory containing downloaded filings.

## Setup

1. **Clone the repository and install dependencies:**
   ```bash
   git clone <repo-url>
   cd company_10kq_scraper
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Set up your OpenAI API key:**
   - Create a `.env` file in the project root:
     ```
     OPENAI_API_KEY=sk-...
     ```

3. **(Optional) Install system dependencies:**
   - For SQLite CLI: `sudo apt install sqlite3`
   - For pandas HTML parsing: `pip install lxml html5lib beautifulsoup4 pandas`

## Usage

### Full Pipeline (Recommended)
Run the orchestrator to download filings, extract risks, and generate mitigations in parallel:
```bash
python orchestrator.py
```

### Individual Steps
- **Download and extract filings:**
  ```bash
  python edgar_sp500_scraper.py
  ```
- **Extract and classify risks (parallel):**
  ```bash
  python llm_risk_worker.py --limit 100 --max-workers 10 --sleep 1.0
  ```
- **Suggest and rank AI mitigations (parallel):**
  ```bash
  python llm_mitigation_worker.py --limit 100 --max-workers 10 --sleep 1.0
  ```
- **Fix or update the database schema and company names:**
  ```bash
  python correction_script.py
  ```

## Database Schema
- **filings**: Stores filing metadata, extracted risk section, and company info.
- **risks**: Stores individual risk factors, their classification, and AI mitigation suggestions.

## Customization
- Adjust batch sizes and parallelism with `--limit` and `--max-workers` CLI arguments.
- Update the LLM prompts in the worker scripts for different analysis or mitigation strategies.

## Notes
- The pipeline is designed for production-scale, robust to SEC rate limits and LLM API constraints.
- All LLM calls are parallelized for speed, but you can adjust concurrency to control costs.
- The system is modular: you can run any step independently or as a full pipeline.

## License
MIT License

## Contact
For questions or contributions, contact [Wael Elrifai](mailto:wael@elrifai.org).
