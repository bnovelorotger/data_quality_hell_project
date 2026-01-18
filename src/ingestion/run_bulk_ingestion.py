#!/usr/bin/env python3
"""
Bulk Ingestion Orchestrator (Data Quality Hell project)

Iterates over countries in data/reference/countries.json and calls fetch_raw.py
for each one.
"""

import json
import subprocess
import sys
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Bulk fetch job ads for multiple countries")
    parser.add_argument("--pages", type=int, default=1, help="Pages per country. Default: 1")
    parser.add_argument("--results-per-page", type=int, default=50, help="Results per page. Default: 50")
    parser.add_argument("--what", default="data", help="Search keywords. Default: data")
    parser.add_argument("--max-days-old", type=int, default=None, help="Include jobs up to X days old")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of countries to process (for testing)")
    
    args = parser.parse_args()

    countries_file = Path("data/reference/countries.json")
    if not countries_file.exists():
        print(f"ERROR: {countries_file} not found. Run fetch_countries.py first (or fallback).", file=sys.stderr)
        return 1

    with open(countries_file, "r", encoding="utf-8") as f:
        countries = json.load(f)

    if args.limit:
        countries = countries[:args.limit]

    print(f"Starting bulk ingestion for {len(countries)} countries...")
    print(f"Settings: pages={args.pages}, results-per-page={args.results_per_page}, what='{args.what}'")
    
    script_path = Path("src/ingestion/fetch_raw.py")

    for idx, country_info in enumerate(countries, 1):
        code = country_info["code"]
        name = country_info["name"]
        
        print(f"\n[{idx}/{len(countries)}] Processing {name} ({code.upper()})...")
        
        cmd = [
            sys.executable,
            str(script_path),
            "--country", code,
            "--what", args.what,
            "--pages", str(args.pages),
            "--results-per-page", str(args.results_per_page)
        ]
        
        if args.max_days_old:
            cmd.extend(["--max-days-old", str(args.max_days_old)])
        
        try:
            # We use check=True to stop if one fails, but maybe we want to continue?
            # For data quality hell, let's continue but log errors.
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"Success for {name}")
                # Print only the last line of output (usually the summary)
                last_lines = result.stdout.strip().split("\n")
                if last_lines:
                    print(f"   {last_lines[-1]}")
            else:
                print(f"Error for {name}:")
                print(result.stderr)
        except Exception as e:
            print(f"Critical error for {name}: {e}")

    print("\nBulk ingestion completed.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
