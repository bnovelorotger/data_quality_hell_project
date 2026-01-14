#!/ reentry/env python3
"""
Adzuna Country List Ingestion (Data Quality Hell project)

Fetches the list of supported countries from the Adzuna Intelligence API:
GET https://api.intelligence.adzuna.com/api/v1.1/countries/
"""

import os
import json
import sys
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

INTELLIGENCE_API_URL = "https://api.intelligence.adzuna.com/api/v1.1/countries/"

def fetch_countries():
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")

    if not app_id or not app_key:
        print("ERROR: Missing ADZUNA_APP_ID / ADZUNA_APP_KEY in environment.", file=sys.stderr)
        return 2

    params = {
        "app_id": app_id,
        "app_key": app_key
    }

    print(f"Fetching country list from {INTELLIGENCE_API_URL}...")
    
    try:
        response = requests.get(INTELLIGENCE_API_URL, params=params, timeout=30)
        response.raise_for_status()
        countries_data = response.json()
    except requests.RequestException as e:
        print(f"ERROR: Failed to fetch countries: {e}", file=sys.stderr)
        return 1
    except json.JSONDecodeError:
        print("ERROR: Failed to parse JSON response.", file=sys.stderr)
        return 1

    # Save to data/reference/countries.json
    output_dir = Path("data/reference")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "countries.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(countries_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Country list saved to {output_file}")
    return 0

if __name__ == "__main__":
    sys.exit(fetch_countries())
