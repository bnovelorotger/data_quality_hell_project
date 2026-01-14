#!/usr/bin/env python3
"""
Adzuna RAW ingestion (Data Quality Hell project)

- Pulls job ads using Adzuna Search endpoint:
  /v1/api/jobs/{country}/search/{page}?app_id=...&app_key=...&what=...

- Stores immutable RAW JSON snapshots per page in data/raw/
- Writes a manifest.json with metadata for reproducibility
- Respects default rate limits (25 hits/min, etc.) with throttling + retries
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

API_BASE = "https://api.adzuna.com/v1/api/jobs"


# ---------- helpers ----------
def utc_ts_compact() -> str:
    # Example: 20260114T150000Z
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_slug(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in s).strip("_")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class SnapshotManifest:
    snapshot_id: str
    created_utc: str
    country: str
    what: str
    where: Optional[str]
    results_per_page: int
    pages_requested: int
    pages_fetched: int
    request_url_template: str
    params_template: Dict[str, Any]
    files: Dict[str, Dict[str, Any]]  # filename -> {path, sha256, bytes, page, count}
    notes: str


# ---------- API call with retries ----------
def fetch_page(
    session: requests.Session,
    country: str,
    page: int,
    params: Dict[str, Any],
    timeout_s: int = 30,
    max_retries: int = 6,
) -> Dict[str, Any]:
    url = f"{API_BASE}/{country}/search/{page}"
    backoff = 1.5

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout_s)
        except requests.RequestException as e:
            if attempt == max_retries:
                raise RuntimeError(f"Network error after {max_retries} attempts: {e}") from e
            time.sleep(backoff)
            backoff *= 2
            continue

        # Handle rate limits and transient errors
        if r.status_code in (429, 500, 502, 503, 504):
            if attempt == max_retries:
                raise RuntimeError(
                    f"Adzuna API error {r.status_code} after {max_retries} attempts: {r.text[:300]}"
                )
            # Respect Retry-After if present
            retry_after = r.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else backoff
            time.sleep(sleep_s)
            backoff *= 2
            continue

        if not r.ok:
            raise RuntimeError(f"Adzuna API error {r.status_code}: {r.text[:500]}")

        return r.json()

    raise RuntimeError("Unreachable")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch RAW job ads from Adzuna API")
    parser.add_argument("--country", default="es", help="Country code (e.g. es, gb, us). Default: es")
    parser.add_argument("--what", default="data", help="Search keywords for job ads. Default: data")
    parser.add_argument("--where", dest="where_", default=None, help="Location filter (city/region). Optional")
    parser.add_argument("--pages", type=int, default=10, help="Number of pages to fetch (starting at 1). Default: 5")
    parser.add_argument("--results-per-page", type=int, default=50, help="Results per page (max 50). Default: 50")
    parser.add_argument("--sort-by", default="date", choices=["date", "relevance", "salary"], help="Sort. Default: date")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Throttle between requests. Default: 3.0")
    args = parser.parse_args()

    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        print("ERROR: Missing ADZUNA_APP_ID / ADZUNA_APP_KEY in environment.", file=sys.stderr)
        print("Tip: create a .env and export variables before running.", file=sys.stderr)
        return 2

    results_per_page = int(args.results_per_page)
    if results_per_page < 1 or results_per_page > 50:
        print("ERROR: results-per-page must be between 1 and 50.", file=sys.stderr)
        return 2

    snapshot_id = f"adzuna__{args.country}__what_{safe_slug(args.what)}__{utc_ts_compact()}"
    raw_dir = Path("data") / "raw" / snapshot_id
    raw_dir.mkdir(parents=True, exist_ok=True)

    params: Dict[str, Any] = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": results_per_page,
        "what": args.what,
        "content-type": "application/json",
        "sort_by": args.sort_by,
    }
    if args.where_:
        params["where"] = args.where_

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "data-quality-hell/1.0 (portfolio project; respectful rate limiting)",
            "Accept": "application/json",
        }
    )

    files_meta: Dict[str, Dict[str, Any]] = {}
    pages_fetched = 0

    for page in range(1, args.pages + 1):
        payload = fetch_page(session, args.country, page, params=params)

        # Save RAW exactly as received (no cleaning here)
        filename = f"adzuna_search__{args.country}__page{page:03d}.json"
        out_path = raw_dir / filename
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        # Collect minimal file meta
        count = len(payload.get("results", [])) if isinstance(payload, dict) else None
        files_meta[filename] = {
            "path": str(out_path.as_posix()),
            "sha256": sha256_file(out_path),
            "bytes": out_path.stat().st_size,
            "page": page,
            "results_count": count,
        }

        pages_fetched += 1

        # Throttle between requests (avoid hitting default rate limits)
        if page < args.pages:
            time.sleep(max(0.0, args.sleep_seconds))

    manifest = SnapshotManifest(
        snapshot_id=snapshot_id,
        created_utc=datetime.now(timezone.utc).isoformat(),
        country=args.country,
        what=args.what,
        where=args.where_,
        results_per_page=results_per_page,
        pages_requested=args.pages,
        pages_fetched=pages_fetched,
        request_url_template=f"{API_BASE}/{{country}}/search/{{page}}",
        params_template={k: ("***" if k in ("app_id", "app_key") else v) for k, v in params.items()},
        files=files_meta,
        notes=(
            "RAW snapshot saved per page. Do not edit files in data/raw/. "
            "Downstream steps should read from this snapshot and create interim/cleaned datasets."
        ),
    )

    (raw_dir / "manifest.json").write_text(
        json.dumps(asdict(manifest), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Snapshot created: {snapshot_id}")
    print(f"RAW saved under: {raw_dir}")
    print("Next: build an interim table by flattening results[] without cleaning semantics.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
