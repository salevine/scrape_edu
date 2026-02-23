"""CLI entry point for scrape_edu."""

import argparse
import sys
from pathlib import Path

from scrape_edu.browser.playwright_pool import PlaywrightPool
from scrape_edu.browser.renderer import PageRenderer
from scrape_edu.config import load_config
from scrape_edu.data.ipeds_loader import load_schools
from scrape_edu.data.manifest import ManifestManager
from scrape_edu.discovery.serper_search import SerperClient
from scrape_edu.net.http_client import HttpClient
from scrape_edu.net.rate_limiter import RateLimiter
from scrape_edu.pipeline.orchestrator import Orchestrator
from scrape_edu.pipeline.phase_handlers import build_phase_handlers
from scrape_edu.pipeline.phases import PHASE_ORDER, Phase
from scrape_edu.utils.logging_setup import setup_logging


def main(argv: list[str] | None = None) -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="scrape_edu",
        description="Scrape CS/DS course catalogs, syllabi, and faculty data from US universities",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- run command ---
    run_parser = subparsers.add_parser("run", help="Run the scraping pipeline")
    run_parser.add_argument("--workers", type=int, help="Number of worker threads")
    run_parser.add_argument("--schools", type=str, help="Comma-separated list of school slugs")
    run_parser.add_argument("--phase", type=str, choices=["discovery", "catalog", "faculty", "syllabi"], help="Run only a specific phase")
    run_parser.add_argument("--config", type=Path, help="Path to config YAML file")
    run_parser.add_argument("--dry-run", action="store_true", help="Show what the pipeline would do without actually scraping")

    # --- rescrape command ---
    rescrape_parser = subparsers.add_parser("rescrape", help="Flag schools for re-scraping")
    rescrape_parser.add_argument("--schools", type=str, help="Comma-separated list of school slugs")
    rescrape_parser.add_argument("--all", action="store_true", help="Flag all schools for re-scraping")

    # --- menu command ---
    subparsers.add_parser("menu", help="Launch interactive menu")

    # --- status command ---
    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.add_argument("--config", type=Path, help="Path to config YAML file")

    args = parser.parse_args(argv)

    if args.command is None or args.command == "menu":
        from scrape_edu.interactive import interactive_menu
        return interactive_menu()

    if args.command == "status":
        return cmd_status(args)
    elif args.command == "run":
        return cmd_run(args)
    elif args.command == "rescrape":
        return cmd_rescrape(args)

    return 0


def cmd_status(args) -> int:
    """Show pipeline status: school count, manifest summary."""
    config = load_config(config_path=getattr(args, "config", None))
    logger = setup_logging(level=config.get("logging", {}).get("level", "INFO"))

    ipeds_dir = Path(config.get("ipeds_dir", "./data/ipeds"))
    output_dir = Path(config.get("output_dir", "./output"))

    # Load schools
    try:
        schools = load_schools(ipeds_dir)
        print(f"Schools loaded from IPEDS: {len(schools)}")
    except FileNotFoundError as e:
        print(f"IPEDS data not found: {e}")
        print("Run 'python scripts/download_ipeds.py' to download IPEDS data.")
        # Don't return error - still show manifest status if available
        schools = []

    # Show manifest status if it exists
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        manifest = ManifestManager(output_dir)
        summary = manifest.get_summary()
        print(f"\nManifest status:")
        for status, count in sorted(summary.items()):
            print(f"  {status}: {count}")
        total = sum(summary.values())
        print(f"  total: {total}")
    else:
        print(f"\nNo manifest found at {manifest_path}")
        print("Run 'python -m scrape_edu run' to start scraping.")

    return 0


def cmd_run(args) -> int:
    """Run the scraping pipeline."""
    cli_overrides = {}
    if args.workers is not None:
        cli_overrides["workers"] = args.workers

    config = load_config(
        config_path=getattr(args, "config", None),
        cli_overrides=cli_overrides,
    )
    setup_logging(level=config.get("logging", {}).get("level", "INFO"))

    ipeds_dir = Path(config.get("ipeds_dir", "./data/ipeds"))
    output_dir = Path(config.get("output_dir", "./output"))
    workers = config.get("workers", 5)

    # Load schools
    try:
        schools = load_schools(ipeds_dir)
    except FileNotFoundError as e:
        print(f"IPEDS data not found: {e}")
        print("Run 'python scripts/download_ipeds.py' to download IPEDS data.")
        return 1

    print(f"Loaded {len(schools)} schools")

    # Parse filters
    schools_filter = None
    if args.schools:
        schools_filter = [s.strip() for s in args.schools.split(",")]

    phases_filter = None
    if args.phase:
        phases_filter = [Phase(args.phase)]

    # --- Dry-run mode: display what would happen and exit ---
    if getattr(args, "dry_run", False):
        return _print_dry_run(
            config=config,
            schools=schools,
            schools_filter=schools_filter,
            phases_filter=phases_filter,
            output_dir=output_dir,
            workers=workers,
        )

    # Build networking and scraper components
    rate_limit = config.get("rate_limit", {})
    rate_limiter = RateLimiter(
        min_delay=rate_limit.get("min_delay", 1.0),
        max_delay=rate_limit.get("max_delay", 3.0),
    )
    http_client = HttpClient(
        rate_limiter=rate_limiter,
        user_agent=config.get("user_agent", "scrape_edu/0.1.0"),
        timeout=(
            config.get("timeouts", {}).get("connect", 10),
            config.get("timeouts", {}).get("read", 30),
        ),
        max_retries=config.get("retries", 3),
    )

    # Set up Serper client if API key is available
    api_key = config.get("search", {}).get("api_key", "")
    serper_client = SerperClient(api_key=api_key) if api_key else None
    if not serper_client:
        print("Warning: No SERPER_API_KEY set. Discovery will be limited.")

    # Set up Playwright renderer for HTML-to-PDF catalog conversion
    pool = PlaywrightPool(pool_size=min(workers, 3))
    renderer = None
    try:
        pool.start()
        renderer = PageRenderer(pool)
    except RuntimeError as e:
        print(f"Warning: Playwright unavailable, HTML catalogs will be skipped: {e}")

    # Build phase handlers
    phase_handlers = build_phase_handlers(
        http_client=http_client,
        config=config,
        serper_client=serper_client,
        renderer=renderer,
    )

    # Run pipeline
    orchestrator = Orchestrator(
        schools=schools,
        output_dir=output_dir,
        config=config,
        workers=workers,
        phase_handlers=phase_handlers,
    )

    try:
        results = orchestrator.run(
            schools_filter=schools_filter,
            phases_filter=phases_filter,
        )
    finally:
        http_client.close()
        pool.stop()

    print(f"\nPipeline complete:")
    print(f"  Completed: {results.get('completed', 0)}")
    print(f"  Failed:    {results.get('failed', 0)}")
    print(f"  Skipped:   {results.get('skipped', 0)}")
    if results.get("interrupted", 0):
        print(f"  Interrupted: {results['interrupted']}")

    return 0


def _print_dry_run(
    *,
    config: dict,
    schools: list,
    schools_filter: list[str] | None,
    phases_filter: list[Phase] | None,
    output_dir: Path,
    workers: int,
) -> int:
    """Print what the pipeline would do and return 0."""
    rate_limit = config.get("rate_limit", {})
    min_delay = rate_limit.get("min_delay", 1.0)
    max_delay = rate_limit.get("max_delay", 3.0)

    print("=" * 60)
    print("DRY RUN — no HTTP requests will be made")
    print("=" * 60)

    # --- Configuration ---
    print(f"\nConfiguration:")
    print(f"  Output directory: {output_dir}")
    print(f"  Workers:          {workers}")
    print(f"  Rate limit:       {min_delay}–{max_delay}s per request")
    print(f"  User-agent:       {config.get('user_agent', 'scrape_edu/0.1.0')}")
    retries = config.get("retries", 3)
    print(f"  Max retries:      {retries}")

    # --- Phases ---
    phases = phases_filter if phases_filter else PHASE_ORDER
    print(f"\nPhases to run ({len(phases)}):")
    for phase in phases:
        print(f"  - {phase.value}")

    # --- Schools ---
    if schools_filter:
        # Filter the school list to only matching slugs
        slug_set = set(schools_filter)
        filtered = [s for s in schools if s.slug in slug_set]
        # Also note any slugs that didn't match loaded schools
        loaded_slugs = {s.slug for s in schools}
        unknown = [slug for slug in schools_filter if slug not in loaded_slugs]
        school_list = filtered
        print(f"\nSchools to process ({len(filtered)} of {len(schools)} loaded, filtered by --schools):")
        if unknown:
            print(f"  WARNING: {len(unknown)} slug(s) not found in IPEDS data: {', '.join(unknown)}")
    else:
        school_list = schools
        print(f"\nSchools to process ({len(schools)}):")

    max_display = 20
    for school in school_list[:max_display]:
        print(f"  - {school.slug} ({school.name})")
    if len(school_list) > max_display:
        print(f"  ... and {len(school_list) - max_display} more")

    # --- Manifest status (if one exists) ---
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        manifest = ManifestManager(output_dir)
        summary = manifest.get_summary()
        print(f"\nExisting manifest status:")
        for status, count in sorted(summary.items()):
            print(f"  {status}: {count}")
        print(f"  total: {sum(summary.values())}")
    else:
        print(f"\nNo existing manifest found (first run).")

    print()
    return 0


def cmd_rescrape(args) -> int:
    """Flag schools for re-scraping."""
    config = load_config()
    output_dir = Path(config.get("output_dir", "./output"))

    manifest_path = output_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"No manifest found at {manifest_path}. Nothing to rescrape.")
        return 1

    manifest = ManifestManager(output_dir)

    if args.all:
        count = manifest.flag_rescrape()
        print(f"Flagged {count} schools for re-scraping.")
    elif args.schools:
        slugs = [s.strip() for s in args.schools.split(",")]
        count = manifest.flag_rescrape(slugs)
        print(f"Flagged {count} schools for re-scraping.")
    else:
        print("Specify --schools or --all")
        return 1

    return 0
