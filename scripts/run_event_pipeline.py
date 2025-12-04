#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path

def run_quiet(cmd):
    """Run a command silently, exit if it fails."""
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode != 0:
        print(f"ERROR: Command failed: {' '.join(cmd)}", file=sys.stderr)
        sys.exit(result.returncode)

def main():
    parser = argparse.ArgumentParser(description="Run full event pipeline: fetch, plot, correlate.")
    parser.add_argument("--article", required=True, help="Wikipedia article title")
    parser.add_argument("--outdir", required=True, help="Event folder (e.g., data/events/boston_bombing")
    parser.add_argument("--anniversary", required=True, help="Anniversary in MM-DD format (e.g., 04-15)")

    args = parser.parse_args()

    # Ensure output directory exists
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    folder_name = outdir.name

    parquet_file = outdir / f"{folder_name}_pageviews.parquet"
    plot_file = outdir / f"plotted_{folder_name}_pageviews.png"
    correlation_file = outdir / f"{folder_name}_correlation_results.txt"

    # Step 1: Fetch pageviews
    run_quiet([
        sys.executable,
        "scripts/fetch_pageviews.py",
        "--article", args.article,
        "--outfile", str(parquet_file)
    ])

    # Step 2: Plot pageviews
    run_quiet([
        sys.executable,
        "scripts/plot_pageviews.py",
        "--parquet", str(parquet_file),
        "--article", args.article,
        "--anniversary", args.anniversary
    ])

    # Step 3: Correlation
    run_quiet([
        sys.executable,
        "scripts/run_correlation.py",
        "--parquet", str(parquet_file),
        "--anniversary", args.anniversary,
        "--outfile", str(correlation_file)
    ])

if __name__ == "__main__":
    main()
