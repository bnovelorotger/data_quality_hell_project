# Data Extraction & Processing Guide

This guide details the steps to replicate the data extraction, ingestion, and initial processing flow for the **Data Quality Hell** project.

## 1. Prerequisites

### API Credentials
You need an Adzuna API ID and Key. Create a `.env` file in the root directory:
```env
ADZUNA_APP_ID=your_id_here
ADZUNA_APP_KEY=your_key_here
```

### Reference Data
The list of targeted countries is stored in `data/reference/countries.json`. Currently, it includes 19 supported countries. (RU = RUSSIA is not supported anymore)

---

## 2. Process Flow

```mermaid
graph TD
    A[API Credentials .env] --> B(run_bulk_ingestion.py)
    C[Country List countries.json] --> B
    B --> D{data/raw/}
    D --> E(flatten_raw.py)
    E --> F{data/interim/}
    F --> G(merge_data.py)
    G --> H[all_jobs_merged.csv]
    
    subgraph "Automatic Cleanup"
    E -- Deletes old snapshots --> D
    end
```

---

## 3. Step-by-Step Execution

### Step A: Bulk Ingestion
Run the bulk ingestion script to fetch raw JSON data for all countries. This script orchestrates multiple calls to the Adzuna API.

```bash
# Fetch 10 pages per country (50 results per page)
python src/ingestion/run_bulk_ingestion.py --pages 10 --results-per-page 50
```
- **Output**: Multi-page JSON files saved in `data/raw/snapshot_id/`.
- **Note**: The script handles rate limiting and retries automatically.

### Step B: Flattening & Cleanup
Process the raw JSON snapshots into structured CSV files while cleaning up old data.

```bash
python src/processing/flatten_raw.py
```
- **Actions**:
  1. Identifies all snapshots in `data/raw/`.
  2. Keeps only the **latest** version for each country and deletes the rest.
  3. Extracts fields: `description`, `title`, `id`, `company`, `adref`, `location`, `created`.
- **Output**: Individual CSVs in `data/interim/{country}_jobs.csv`.

### Step C: Data Merging
Consolidate all individual country CSVs into a single master dataset for analysis.

```bash
python src/processing/merge_data.py
```
- **Action**: Merges all `*_jobs.csv` files and adds a `country_code` column.
- **Output**: `data/interim/all_jobs_merged.csv` (approx. 9,500 rows).

---

## 4. Git & Data Strategy

To maintain a clean and professional repository, we follow these rules:

- **Included in Git**: All source code (`src/`), documentation (`docs/`), and reference metadata (`data/reference/`).
- **Excluded from Git**: Raw JSON data (`data/raw/`) and interim CSVs (`data/interim/`).
- **Rationale**: This demonstrates "Data Engineering rigor"—anyone who clones the repo can perfectly replicate the datasets by running the documented pipeline, rather than downloading stale binary files.

---

## 5. Replication Checklist
- [ ] Verify `.env` variables.
- [ ] Run `run_bulk_ingestion.py`.
- [ ] Run `flatten_raw.py`.
- [ ] Run `merge_data.py`.
- [ ] Final result available in `data/interim/all_jobs_merged.csv`.
