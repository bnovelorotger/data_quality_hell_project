#!/usr/bin/env python3
"""
Raw Data Flattener and Cleanup (Data Quality Hell project)

1. Identifies all snapshots in data/raw/
2. For each country, keeps only the latest snapshot and deletes others.
3. Processes the latest snapshots to extract:
   description, title, id, company.display_name, adref, location.display_name, created
4. Saves per-country CSVs in data/interim/
"""

import os
import json
import csv
import shutil
import sys
from pathlib import Path
from collections import defaultdict

def cleanup_snapshots(raw_dir: Path):
    """Keeps only the latest snapshot for each country."""
    print("🧹 Cleaning up old snapshots...")
    snapshots = [d for d in raw_dir.iterdir() if d.is_dir() and d.name.startswith("adzuna__")]
    
    # Group by country: adzuna__gb__... -> country = gb
    grouped = defaultdict(list)
    for s in snapshots:
        parts = s.name.split("__")
        if len(parts) >= 2:
            country = parts[1]
            # timestamp is at the end: 20260114T170001Z
            timestamp = parts[-1]
            grouped[country].append((timestamp, s))
    
    deleted_count = 0
    for country, items in grouped.items():
        # Sort by timestamp (alphabetical sort works for YYYYMMDDTHHMMSSZ)
        items.sort(key=lambda x: x[0], reverse=True)
        latest = items[0][1]
        others = items[1:]
        
        for ts, path in others:
            print(f"   Deleting old snapshot: {path.name}")
            shutil.rmtree(path)
            deleted_count += 1
            
    print(f"✨ Cleanup finished. Deleted {deleted_count} old snapshots.")
    return {country: items[0][1] for country, items in grouped.items()}

def flatten_data(latest_snapshots: dict, interim_dir: Path):
    """Extracts job data and saves to CSV."""
    interim_dir.mkdir(parents=True, exist_ok=True)
    
    for country, snapshot_path in latest_snapshots.items():
        print(f"📄 Processing {country.upper()} from {snapshot_path.name}...")
        
        output_file = interim_dir / f"{country}_jobs.csv"
        
        # Collect all job results from all JSON pages
        all_jobs = []
        json_files = sorted(list(snapshot_path.glob("adzuna_search__*__page*.json")))
        
        for jf in json_files:
            try:
                with open(jf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    results = data.get("results", [])
                    if isinstance(results, list):
                        all_jobs.extend(results)
            except Exception as e:
                print(f"   ⚠️ Error reading {jf.name}: {e}")

        if not all_jobs:
            print(f"   ⚠️ No jobs found for {country}")
            continue

        # Write to CSV
        fieldnames = ["description", "title", "id", "company", "adref", "location", "created"]
        
        try:
            with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                
                for job in all_jobs:
                    # Extraction logic with fallbacks
                    row = {
                        "description": job.get("description", ""),
                        "title": job.get("title", ""),
                        "id": job.get("id", ""),
                        "company": job.get("company", {}).get("display_name", "") if isinstance(job.get("company"), dict) else "",
                        "adref": job.get("adref", ""),
                        "location": job.get("location", {}).get("display_name", "") if isinstance(job.get("location"), dict) else "",
                        "created": job.get("created", "")
                    }
                    writer.writerow(row)
            
            print(f"   ✅ Saved {len(all_jobs)} jobs to {output_file.name}")
        except Exception as e:
            print(f"   ❌ Error writing CSV for {country}: {e}")

def main():
    raw_dir = Path("data/raw")
    interim_dir = Path("data/interim")
    
    if not raw_dir.exists():
        print("ERROR: data/raw directory not found.")
        return 1
        
    latest_snapshots = cleanup_snapshots(raw_dir)
    flatten_data(latest_snapshots, interim_dir)
    
    print("\nProcessing complete.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
