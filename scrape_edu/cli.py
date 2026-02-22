"""CLI entry point for scrape_edu."""

import argparse
import sys
from pathlib import Path

from scrape_edu.config import load_config
from scrape_edu.data.ipeds_loader import load_schools
from scrape_edu.data.manifest import ManifestManager
from scrape_edu.discovery.serper_search import SerperClient
from scrape_edu.net.http_client import HttpClient
from scrape_edu.net.rate_limiter import RateLimiter
from scrape_edu.pipeline.orchestrator import Orchestrator
from scrape_edu.pipeline.phase_handlers import build_phase_handlers
from scrape_edu.pipeline.phases import Phase
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

    # --- rescrape command ---
    rescrape_parser = subparsers.add_parser("rescrape", help="Flag schools for re-scraping")
    rescrape_parser.add_argument("--schools", type=str, help="Comma-separated list of school slugs")
    rescrape_parser.add_argument("--all", action="store_true", help="Flag all schools for re-scraping")

    # --- status command ---
    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.add_argument("--config", type=Path, help="Path to config YAML file")

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

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

    # Build phase handlers
    phase_handlers = build_phase_handlers(
        http_client=http_client,
        config=config,
        serper_client=serper_client,
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

    print(f"\nPipeline complete:")
    print(f"  Completed: {results.get('completed', 0)}")
    print(f"  Failed:    {results.get('failed', 0)}")
    print(f"  Skipped:   {results.get('skipped', 0)}")
    if results.get("interrupted", 0):
        print(f"  Interrupted: {results['interrupted']}")

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
