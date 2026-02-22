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
- **Institutional Characteristics (HD)** -- School names, URLs, locations, types
- **Completions (C)** -- Degree awards by CIP code (used to filter to CS/DS programs)

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
# Check how many schools are loaded and manifest status
python -m scrape_edu status

# Preview what the pipeline would do (no HTTP requests)
python -m scrape_edu run --dry-run

# Run full pipeline with default settings (5 workers)
python -m scrape_edu run

# Run with more workers
python -m scrape_edu run --workers 8

# Run only for specific schools
python -m scrape_edu run --schools "mit,stanford,cmu"

# Run only the discovery phase (find department pages, don't download yet)
python -m scrape_edu run --phase discovery

# Dry-run with filters to verify before a long run
python -m scrape_edu run --schools "mit,stanford" --phase catalog --dry-run

# Flag specific schools for re-scraping
python -m scrape_edu rescrape --schools "mit"

# Flag ALL schools for re-scraping
python -m scrape_edu rescrape --all

# Analyze progress (status breakdown, errors, disk usage)
python scripts/analyze_manifest.py

# Analyze with JSON output for scripting
python scripts/analyze_manifest.py --json
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

## Architecture

The pipeline processes each school through five phases in order:

1. **Robots** -- Fetch and log `robots.txt` (informational only, does not block scraping)
2. **Discovery** -- Use Serper.dev to search for CS/DS catalog, faculty, and syllabus URLs
3. **Catalog** -- Download course catalog pages (PDF direct download or HTML rendered to PDF via Playwright)
4. **Faculty** -- Download faculty directory HTML and parse to JSON (best-effort)
5. **Syllabi** -- Find and download syllabus PDFs from discovered URLs and faculty pages

Key design decisions:
- **Threading**: `ThreadPoolExecutor` with configurable workers (default 5). Each worker processes one school at a time through all phases.
- **Rate limiting**: Per-domain delays (1-3s configurable) prevent overwhelming any single server. Thread-safe with per-domain locks.
- **Resumability**: Manifest-based. Schools are claimed atomically (PENDING -> SCRAPING). On crash, SCRAPING schools are reset to PENDING on restart. Completed phases are skipped.
- **Graceful shutdown**: SIGINT handler sets a shutdown event. Workers finish their current phase and stop. Resume picks up where it left off.
- **Atomic writes**: All file writes use a temp-file + `os.replace` pattern to prevent corruption from crashes.

## Resumability

The scraper automatically picks up where it left off:

- On restart, schools marked as **SCRAPING** are reset to **PENDING** (crash recovery)
- **COMPLETED** schools are skipped
- Each phase within a school tracks its own completion status
- Individual files are tracked by URL (already-downloaded files are skipped)
- Use `python -m scrape_edu rescrape --schools "mit"` to force re-scrape specific schools
- Use `python -m scrape_edu rescrape --all` to re-scrape everything

## Analyzing Results

The `scripts/analyze_manifest.py` script provides a comprehensive analysis of scraping progress:

```bash
# Human-readable summary
python scripts/analyze_manifest.py

# Point at a different output directory
python scripts/analyze_manifest.py --output-dir /path/to/output

# JSON output for programmatic use
python scripts/analyze_manifest.py --json
```

The analysis includes:
- School status breakdown (pending, completed, failed, etc.)
- Per-phase completion rates
- Error summary with the top 5 most common errors
- File counts by type (PDF, HTML, JSON)
- Total disk usage

## Cost Estimate

| Scale | Serper Queries | Cost |
|-------|---------------|------|
| 50 test schools | ~100 | Free tier |
| 1,250 schools | ~2,500 | Free tier (exhausts it) |
| All ~4,000 schools | ~8,000 | ~$8 on paid tier |

## Development

### Project Structure

```
scrape_edu/
├── cli.py                  # CLI entry point (run, status, rescrape)
├── config.py               # Layered config (YAML + .env + CLI)
├── data/
│   ├── school.py           # School dataclass
│   ├── ipeds_loader.py     # IPEDS CSV loading and filtering
│   ├── manifest.py         # Thread-safe manifest + per-school metadata
│   └── models.py           # Pydantic models (FacultyMember, CatalogEntry, etc.)
├── net/
│   ├── rate_limiter.py     # Per-domain rate limiting
│   └── http_client.py      # HTTP client with retries
├── browser/
│   ├── playwright_pool.py  # Browser context pool
│   └── renderer.py         # HTML-to-PDF rendering
├── discovery/
│   ├── serper_search.py    # Serper.dev API client
│   ├── url_classifier.py   # URL categorization (catalog/faculty/syllabus)
│   └── homepage_crawler.py # BFS fallback crawler
├── scrapers/
│   ├── base.py             # Abstract base scraper
│   ├── robots_checker.py   # robots.txt fetcher
│   ├── catalog_scraper.py  # Course catalog downloader
│   ├── faculty_scraper.py  # Faculty page downloader + parser
│   └── syllabus_scraper.py # Syllabus finder + downloader
├── pipeline/
│   ├── phases.py           # Phase enum and ordering
│   ├── school_worker.py    # Per-school phase runner
│   ├── orchestrator.py     # Thread pool + progress reporting
│   └── phase_handlers.py   # Phase handler factory
└── utils/
    ├── slug.py             # University name -> filesystem slug
    ├── logging_setup.py    # Structured JSON logging
    ├── url_utils.py        # URL normalization
    └── file_utils.py       # Atomic file writes
```

### Running Tests

```bash
source .venv/bin/activate
python -m pytest                    # Run all tests
python -m pytest --tb=short -q      # Compact output
python -m pytest tests/test_cli.py  # Run specific test file
```

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
