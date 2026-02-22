# scrape_edu

Multi-threaded Python scraper that collects CS and Data Science course catalogs, syllabi, faculty data, and textbook information from US universities. Downloads files, organizes by institution, and tracks progress in a resumable manifest.

## Prerequisites

- Python 3.11+
- A [Serper.dev](https://serper.dev) account (free tier available, no credit card required)

## Installation

```bash
git clone https://github.com/yourusername/scrape_edu.git
cd scrape_edu
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
playwright install chromium
```

## Serper.dev API Setup

1. Go to [serper.dev](https://serper.dev)
2. Click **"Get 2,500 free queries"** (no credit card required)
3. Create an account with email or Google sign-in
4. After sign-in, your API key is displayed on the dashboard
5. Copy the API key
6. Create a `.env` file in the project root:
   ```bash
   cp .env.example .env
   ```
7. Edit `.env` and paste your API key:
   ```
   SERPER_API_KEY=your_api_key_here
   ```
8. **Free tier**: 2,500 queries total (covers ~1,250 schools for initial discovery)
9. **Paid tier**: $50/month for 50,000 queries (covers full 4,000+ school run with margin)
10. Each school uses ~2 search queries (CS department + DS department)

## IPEDS Data Setup

The scraper uses IPEDS/NCES data to build its list of US universities with CS/DS programs.

**Option 1: Automatic download**
```bash
python scripts/download_ipeds.py
```
This downloads two CSV files from [NCES](https://nces.ed.gov/ipeds/use-the-data):
- **Institutional Characteristics (HD)** — School names, URLs, locations, types
- **Completions (C)** — Degree awards by CIP code (used to filter to CS/DS programs)

Files are saved to `data/ipeds/` (gitignored).

**Option 2: Manual download**
1. Go to [NCES IPEDS](https://nces.ed.gov/ipeds/use-the-data) > "Complete Data Files"
2. Select most recent year > **"Institutional Characteristics"** > download CSV
3. Select most recent year > **"Completions"** > download CSV
4. Place both files in `data/ipeds/`

## Configuration

| Source | Purpose | Example |
|--------|---------|---------|
| `config/default.yaml` | Default settings | workers, rate limits, timeouts |
| `.env` | API keys (never committed) | `SERPER_API_KEY=...` |
| CLI flags | Per-run overrides | `--workers 8` |

Priority: CLI flags > `.env` > `config/default.yaml`

## Usage

```bash
# Check how many schools are loaded
python -m scrape_edu status

# Run full pipeline with default settings (5 workers)
python -m scrape_edu run

# Run with more workers
python -m scrape_edu run --workers 8

# Run only for specific schools
python -m scrape_edu run --schools "mit,stanford,cmu"

# Run only the discovery phase (find department pages, don't download yet)
python -m scrape_edu run --phase discovery

# Flag specific schools for re-scraping
python -m scrape_edu rescrape --schools "mit"

# Flag ALL schools for re-scraping
python -m scrape_edu rescrape --all

# Analyze progress
python scripts/analyze_manifest.py
```

## Output Structure

```
output/
├── manifest.json           # Master tracking file
├── mit/
│   ├── metadata.json       # URLs, robots.txt status, errors, phase progress
│   ├── catalog/            # Course catalog PDFs
│   ├── syllabi/            # Course syllabi PDFs
│   └── faculty/            # Faculty .html + .json files
├── stanford/
│   └── ...
```

## Resumability

The scraper automatically picks up where it left off:

- On restart, schools marked as **SCRAPING** are reset to **PENDING** (crash recovery)
- **COMPLETED** schools are skipped
- Each phase within a school tracks its own completion status
- Individual files are tracked by URL (already-downloaded files are skipped)
- Use `python -m scrape_edu rescrape --schools "mit"` to force re-scrape specific schools
- Use `python -m scrape_edu rescrape --all` to re-scrape everything

## Cost Estimate

| Scale | Serper Queries | Cost |
|-------|---------------|------|
| 50 test schools | ~100 | Free tier |
| 1,250 schools | ~2,500 | Free tier (exhausts it) |
| All ~4,000 schools | ~8,000 | ~$8 on paid tier |

## Troubleshooting

**Playwright install fails**
```bash
# Try installing system dependencies first
playwright install-deps
playwright install chromium
```

**API key not found**
- Make sure `.env` exists in the project root (not in a subdirectory)
- Check that `SERPER_API_KEY` is set (not empty)
- Run `python -m scrape_edu status` to verify configuration

**IPEDS files missing**
```bash
python scripts/download_ipeds.py
```
Or download manually from [NCES](https://nces.ed.gov/ipeds/use-the-data) and place CSVs in `data/ipeds/`.

**Rate limiting / timeouts**
- Adjust `config/default.yaml`: increase `rate_limit.max_delay` or `timeouts.read`
- Reduce workers: `python -m scrape_edu run --workers 2`

**Resume after crash**
Just run the same command again. The scraper will automatically skip completed work and resume from where it stopped.
