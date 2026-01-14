import pandas as pd
import numpy as np
from pathlib import Path

def run_preliminary_eda(input_path: Path):
    print(f"--- Preliminary EDA: {input_path.name} ---")
    
    # 1. Load Data
    try:
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        print(f"ERROR: File {input_path} not found.")
        return
    
    print(f"\n[INFO] Initial Shape: {df.shape}")
    print("\n[INFO] First 5 rows:")
    print(df.head())
    
    print("\n[INFO] Data Info:")
    df.info()

    # 2. Eliminate Unnecessary Columns
    cols_to_drop = ['description', 'adref']
    print(f"\n[ACTION] Dropping columns: {cols_to_drop}")
    df = df.drop(columns=cols_to_drop)
    
    # 3. Data Types & Conversion
    print("\n[ACTION] Converting 'created' to datetime...")
    # Using errors='coerce' since the user warned about potential confusion in characters
    df['created'] = pd.to_datetime(df['created'], errors='coerce')
    
    # 4. Check Nulls
    print("\n[INFO] Null Values Count:")
    null_counts = df.isnull().sum()
    print(null_counts)
    
    if null_counts['created'] > 0:
        print(f"  Warning: {null_counts['created']} rows had invalid date formats and became NaT.")

    # 5. Check Duplicates
    print("\n[INFO] Checking for duplicates...")
    duplicates_count = df.duplicated().sum()
    print(f"  Found {duplicates_count} duplicate rows.")
    
    if duplicates_count > 0:
        print("\n[INFO] Sample of duplicated rows:")
        print(df[df.duplicated()].head())

    print(f"\n[INFO] Final Shape after drop: {df.shape}")
    
    # Return df for future steps if needed (though this script runs as main)
    return df

if __name__ == "__main__":
    input_csv = Path("data/interim/all_jobs_merged.csv")
    df_cleaned = run_preliminary_eda(input_csv)
    
    # Optional: Save a intermediate check-point if desired, 
    # but the user didn't explicitly ask to save yet, just to "create file and proceed"
    # output_csv = Path("data/interim/all_jobs_pre_eda.csv")
    # df_cleaned.to_csv(output_csv, index=False)
    # print(f"\n[SAVE] Preliminary cleaned data saved to {output_csv}")
