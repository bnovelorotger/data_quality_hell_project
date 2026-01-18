import subprocess
import json
import sys
import time
from pathlib import Path

def main():
    # Roles to fetch
    tech_roles = [
        "Data Engineer",
        "Data Scientist",
        "Data Analyst",
        "MLOps",
        "Data Architect"
    ]
    
    countries_file = Path("data/reference/countries.json")
    if not countries_file.exists():
        print(f"Error: {countries_file} not found.")
        sys.exit(1)
        
    with open(countries_file, "r") as f:
        countries = json.load(f)

    # Global parameters
    pages = 50  # Increased depth for specialized terms
    results_per_page = 50
    max_days_old = 25

    total_steps = len(tech_roles) * len(countries)
    current_step = 0

    print(f"🚀 Starting Tech-Specialized Bulk Ingestion")
    print(f"Targeting {len(tech_roles)} roles across {len(countries)} countries.")
    print(f"Total operations: {total_steps}\n")

    for role in tech_roles:
        print(f"\n--- 🛠️ Processing Role: {role} ---")
        for country_info in countries:
            current_step += 1
            code = country_info["code"]
            name = country_info["name"]
            
            print(f"[{current_step}/{total_steps}] Fetching '{role}' for {name} ({code})...")
            
            # Build command
            # Using --what to specify the exact tech role
            cmd = [
                sys.executable, "src/ingestion/fetch_raw.py",
                "--country", code,
                "--pages", str(pages),
                "--results-per-page", str(results_per_page),
                "--max-days-old", str(max_days_old),
                "--what", role
            ]
            
            # Execute fetch_raw.py
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"   ✅ Success for {code}")
            else:
                print(f"   ❌ Failed for {code}")
                print(f"   Error: {result.stderr}")
            
            # Brief pause to be respectful to the API between countries
            time.sleep(1)

    print("\n✅ All specialized ingests completed.")

if __name__ == "__main__":
    main()
