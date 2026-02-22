"""Interactive TUI menu for scrape_edu using rich."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table

from scrape_edu.config import load_config

console = Console()


MAIN_MENU_CHOICES = {
    "1": "Setup",
    "2": "Run pipeline",
    "3": "Check status",
    "4": "Rescrape schools",
    "5": "Analyze results",
    "6": "Dry run (preview)",
    "7": "Exit",
}


def show_main_menu() -> str:
    """Display the main menu and return the user's choice."""
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Key", style="bold cyan", width=4)
    table.add_column("Action", style="white")
    for key, label in MAIN_MENU_CHOICES.items():
        table.add_row(key, label)

    console.print()
    console.print(
        Panel(table, title="[bold]scrape_edu[/bold]", subtitle="Interactive Menu", border_style="blue")
    )
    choice = Prompt.ask(
        "Choose an option",
        choices=list(MAIN_MENU_CHOICES.keys()),
        default="7",
    )
    return choice


def run_setup() -> None:
    """First-time onboarding: check IPEDS data and API key."""
    console.print("\n[bold]Setup[/bold]", style="blue")

    config = load_config()
    ipeds_dir = Path(config.get("ipeds_dir", "./data/ipeds"))
    ready = True

    # Check IPEDS data
    hd_files = list(ipeds_dir.glob("hd*.csv"))
    if hd_files:
        console.print(f"  [green]\u2713[/green] IPEDS data found in {ipeds_dir}")
    else:
        console.print(f"  [red]\u2717[/red] IPEDS data missing from {ipeds_dir}")
        ready = False
        if Confirm.ask("    Download IPEDS data now?", default=True):
            subprocess.run(
                [sys.executable, "scripts/download_ipeds.py", "--output-dir", str(ipeds_dir)],
                check=False,
            )

    # Check .env / API key
    env_path = Path(".env")
    api_key = config.get("search", {}).get("api_key", "")
    if api_key:
        console.print("  [green]\u2713[/green] SERPER_API_KEY is set")
    else:
        console.print("  [red]\u2717[/red] SERPER_API_KEY not found")
        ready = False
        key = Prompt.ask("    Enter your Serper API key (or leave blank to skip)", default="")
        if key:
            # Append/create .env
            existing = env_path.read_text() if env_path.exists() else ""
            if "SERPER_API_KEY" in existing:
                lines = existing.splitlines()
                lines = [
                    f"SERPER_API_KEY={key}" if l.startswith("SERPER_API_KEY") else l
                    for l in lines
                ]
                env_path.write_text("\n".join(lines) + "\n")
            else:
                with open(env_path, "a") as f:
                    f.write(f"SERPER_API_KEY={key}\n")
            console.print("    [green]Saved to .env[/green]")

    if ready:
        console.print("\n  [bold green]All set![/bold green] You're ready to run the pipeline.")
    else:
        console.print("\n  [yellow]Some items still need attention.[/yellow]")


def run_pipeline_flow() -> None:
    """Interactive flow for running the pipeline."""
    console.print("\n[bold]Run Pipeline[/bold]", style="blue")

    workers = IntPrompt.ask("Workers", default=5)
    if workers < 1:
        console.print("[red]Workers must be at least 1.[/red]")
        return

    schools_input = Prompt.ask(
        "Filter to specific schools? (comma-separated slugs, or blank for all)",
        default="",
    )

    phase = Prompt.ask(
        "Run specific phase?",
        choices=["all", "discovery", "catalog", "faculty", "syllabi"],
        default="all",
    )

    # Build command
    cmd = [sys.executable, "-m", "scrape_edu", "run", "--workers", str(workers)]
    if schools_input.strip():
        cmd.extend(["--schools", schools_input.strip()])
    if phase != "all":
        cmd.extend(["--phase", phase])

    console.print(f"\n  Command: [dim]{' '.join(cmd)}[/dim]")
    if Confirm.ask("  Proceed?", default=True):
        subprocess.run(cmd, check=False)
    else:
        console.print("  [yellow]Cancelled.[/yellow]")


def check_status_flow() -> None:
    """Run the status subcommand."""
    console.print("\n[bold]Pipeline Status[/bold]", style="blue")
    subprocess.run([sys.executable, "-m", "scrape_edu", "status"], check=False)


def rescrape_flow() -> None:
    """Interactive flow for re-scraping schools."""
    console.print("\n[bold]Rescrape Schools[/bold]", style="blue")

    scope = Prompt.ask(
        "Rescrape all schools or specific ones?",
        choices=["all", "specific"],
        default="all",
    )

    cmd = [sys.executable, "-m", "scrape_edu", "rescrape"]
    if scope == "all":
        cmd.append("--all")
    else:
        slugs = Prompt.ask("Enter school slugs (comma-separated)")
        if not slugs.strip():
            console.print("[yellow]No slugs entered. Cancelled.[/yellow]")
            return
        cmd.extend(["--schools", slugs.strip()])

    console.print(f"\n  Command: [dim]{' '.join(cmd)}[/dim]")
    if Confirm.ask("  Proceed?", default=True):
        subprocess.run(cmd, check=False)
    else:
        console.print("  [yellow]Cancelled.[/yellow]")


def analyze_flow() -> None:
    """Run the analyze_manifest script."""
    console.print("\n[bold]Analyze Results[/bold]", style="blue")
    subprocess.run(
        [sys.executable, "scripts/analyze_manifest.py"],
        check=False,
    )


def dry_run_flow() -> None:
    """Interactive flow for a dry run."""
    console.print("\n[bold]Dry Run (Preview)[/bold]", style="blue")

    workers = IntPrompt.ask("Workers", default=5)
    if workers < 1:
        console.print("[red]Workers must be at least 1.[/red]")
        return

    schools_input = Prompt.ask(
        "Filter to specific schools? (comma-separated slugs, or blank for all)",
        default="",
    )

    phase = Prompt.ask(
        "Run specific phase?",
        choices=["all", "discovery", "catalog", "faculty", "syllabi"],
        default="all",
    )

    cmd = [sys.executable, "-m", "scrape_edu", "run", "--dry-run", "--workers", str(workers)]
    if schools_input.strip():
        cmd.extend(["--schools", schools_input.strip()])
    if phase != "all":
        cmd.extend(["--phase", phase])

    subprocess.run(cmd, check=False)


DISPATCH = {
    "1": run_setup,
    "2": run_pipeline_flow,
    "3": check_status_flow,
    "4": rescrape_flow,
    "5": analyze_flow,
    "6": dry_run_flow,
}


def interactive_menu() -> int:
    """Run the interactive menu loop. Returns exit code."""
    console.print("[bold blue]scrape_edu[/bold blue] — Interactive Mode\n", style="bold")

    while True:
        choice = show_main_menu()
        if choice == "7":
            console.print("\nGoodbye!", style="bold blue")
            return 0

        handler = DISPATCH.get(choice)
        if handler:
            handler()
        console.print()
