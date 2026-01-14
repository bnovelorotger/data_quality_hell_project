#!/usr/bin/env python3
"""
Data Merger (Data Quality Hell project)

Consolidates all per-country CSV files from data/interim/ into a single
master CSV file and adds a 'country_code' column.
"""

import csv
import sys
from pathlib import Path

def merge_csv_files(interim_dir: Path, output_file: Path):
    """Merges all country-specific CSVs into a single master file."""
    csv_files = sorted([f for f in interim_dir.glob("*_jobs.csv") if f.name != output_file.name])
    
    if not csv_files:
        print("No country CSV files found to merge.")
        return

    print(f"Merging {len(csv_files)} files into {output_file.name}...")
    
    first_file = True
    total_rows = 0
    
    with open(output_file, "w", encoding="utf-8", newline="") as master_f:
        writer = None
        
        for csv_f in csv_files:
            country_code = csv_f.name.split("_")[0]
            print(f"  Processing {csv_f.name} ({country_code.upper()})...")
            
            with open(csv_f, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                
                if first_file:
                    # Prepare fieldnames: add country_code at the beginning
                    fieldnames = ["country_code"] + reader.fieldnames
                    writer = csv.DictWriter(master_f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                    writer.writeheader()
                    first_file = False
                
                rows_in_file = 0
                for row in reader:
                    # Inject country code
                    row["country_code"] = country_code
                    writer.writerow(row)
                    rows_in_file += 1
                
                total_rows += rows_in_file
                print(f"    Added {rows_in_file} rows.")

    print(f"\n✅ Successfully merged {total_rows} total rows into {output_file.name}")

def main():
    interim_dir = Path("data/interim")
    output_file = interim_dir / "all_jobs_merged.csv"
    
    if not interim_dir.exists():
        print(f"ERROR: {interim_dir} does not exist.")
        return 1
        
    merge_csv_files(interim_dir, output_file)
    return 0

if __name__ == "__main__":
    sys.exit(main())
