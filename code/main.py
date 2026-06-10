import pandas as pd
import pyodbc
import numpy as np
import os
import sys
import time
from pathlib import Path
import shutil
from tableauhyperapi import (
    HyperProcess, Connection, Telemetry,
    TableDefinition, TableName, SqlType, Inserter, CreateMode
)

# --------------------------------------------------
# PROJECT PATH RESOLUTION
# --------------------------------------------------

# This file = mwe.01/code/main.py
CODE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CODE_DIR.parent

DATA_DIR = PROJECT_ROOT / "data"
RUNS_DIR = DATA_DIR / "runs"
INPUT_BASE_DIR = DATA_DIR / "input"
OUTPUT_BASE_DIR = DATA_DIR / "output"

# Ensure base dirs exist
for d in [DATA_DIR, RUNS_DIR, INPUT_BASE_DIR, OUTPUT_BASE_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

START_TIME = time.perf_counter()

start_year = 2024
end_year   = 2025   # inclusive range

YEAR_PREFIX = f"{start_year}_{end_year}"

# Timestamped run folder
timestamp = time.strftime("%Y%m%d_%H%M%S")

run_dir = RUNS_DIR / f"run_{timestamp}"
input_dir = run_dir / "inputs"
output_dir = run_dir / "outputs"
code_dir = run_dir / "code"

for d in [run_dir, input_dir, output_dir, code_dir]:
    d.mkdir(parents=True, exist_ok=True)

log_file = run_dir / "logfile.txt"

def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# Save a copy of this script into run/code
try:
    shutil.copy(__file__, code_dir / "main.py")
except:
    pass

log("🚀 Script started")
log(f"📁 Run directory: {run_dir}")

# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

log("🔌 Connecting to SQL Server...")

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=OMAPI-HCADB19;"
    "DATABASE=CLIENT;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

log("✅ Connected to database")

splits = ["Main"]

for split in splits:
    log(f"✅ Processing {split}...")

    # --------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------

    df = pd.read_sql("SELECT * FROM stagnes.emergency", conn)
    log(f"✅ Loaded {len(df):,} records from database")
    df_raw = df.copy()

    # --------------------------------------------------
    # PSYCH VS NON-PSYCH SPLIT
    # --------------------------------------------------

    if split == "Main":
        df = df
        df_raw = df
    else:  
        df = df
        df_raw = df

    log(f"{split} encounter count: {len(df)}")

    # --------------------------------------------------
    # CANONICAL START/END 
    # --------------------------------------------------

    log("🛠️ Creating canonical start/end timestamps (ED → TRIAGE → VISIT fallback)")

    # Convert all relevant columns to datetime once
    for col in [
        "ed_start_dtm", "ed_end_dtm",
        "triage_start_dtm", "triage_stop_dtm",
        "visit_dtm"
    ]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # --------------------------------------------------
    # DEFINE FALLBACK MASKS
    # --------------------------------------------------

    # ED missing (both null)
    ed_missing_mask = df["ed_start_dtm"].isna() & df["ed_end_dtm"].isna()

    # TRIAGE incomplete (either start OR stop missing)
    triage_incomplete_mask = (
        df["triage_start_dtm"].isna() | df["triage_stop_dtm"].isna()
    )

    # VISIT fallback (new rule)
    visit_fallback_mask = ed_missing_mask & triage_incomplete_mask

    # --------------------------------------------------
    # INITIALIZE WITH ED
    # --------------------------------------------------

    df["start_dtm"] = df["ed_start_dtm"]
    df["end_dtm"]   = df["ed_end_dtm"]

    # --------------------------------------------------
    # FALLBACK 1: TRIAGE (only if ED missing AND triage complete)
    # --------------------------------------------------

    triage_valid_mask = ed_missing_mask & ~triage_incomplete_mask

    df.loc[triage_valid_mask, "start_dtm"] = df.loc[triage_valid_mask, "triage_start_dtm"]
    df.loc[triage_valid_mask, "end_dtm"]   = df.loc[triage_valid_mask, "triage_stop_dtm"]

    # --------------------------------------------------
    # FALLBACK 2: VISIT (new logic)
    # --------------------------------------------------

    visit_valid_mask = visit_fallback_mask & df["visit_dtm"].notna()

    df.loc[visit_valid_mask, "start_dtm"] = df.loc[visit_valid_mask, "visit_dtm"]
    df.loc[visit_valid_mask, "end_dtm"]   = (
        df.loc[visit_valid_mask, "visit_dtm"] + pd.Timedelta(minutes=1)
    )

    # --------------------------------------------------
    # LOGGING
    # --------------------------------------------------

    log("✅ Canonical timestamps created")
    log(f"   ↪ TRIAGE fallback applied to {triage_valid_mask.sum():,} records")
    log(f"   ↪ VISIT fallback applied to {visit_valid_mask.sum():,} records")

    # --------------------------------------------------
    # SAVE ENRICHED INPUT SNAPSHOT
    # --------------------------------------------------

    # Raw extract 
    raw_input_path = input_dir / f"{YEAR_PREFIX}_{split}_stagnes_client_raw_extract.csv"
    df_raw.to_csv(raw_input_path, index=False)

    # After transformations
    processed_input_path = input_dir / f"{YEAR_PREFIX}_{split}_stagnes_client_enriched.csv"
    df.to_csv(processed_input_path, index=False)

    log(f"📥 Saved raw input snapshot : {raw_input_path}")
    log(f"📥 Saved enriched input snapshot (with canonical timestamps): {processed_input_path}")

    # --------------------------------------------------
    # DATETIME PREP
    # --------------------------------------------------

    df['start_dtm'] = pd.to_datetime(df['start_dtm'], errors='coerce')
    df['end_dtm']   = pd.to_datetime(df['end_dtm'], errors='coerce')

    # Drop only records where we truly cannot recover
    df = df.dropna(subset=['start_dtm', 'end_dtm'])

    # --------------------------------------------------
    # FIX NEGATIVE OR ZERO-LENGTH VISITS
    # --------------------------------------------------

    invalid_duration_mask = df['end_dtm'] < df['start_dtm']

    # Impute end = start + 140 minutes
    df.loc[invalid_duration_mask, 'end_dtm'] = (
        df.loc[invalid_duration_mask, 'start_dtm'] + pd.Timedelta(minutes=140)
    )

    # Optional: also catch exact-equal timestamps (0-minute stays)
    zero_length_mask = df['end_dtm'] == df['start_dtm']

    df.loc[zero_length_mask, 'end_dtm'] = (
        df.loc[zero_length_mask, 'start_dtm'] + pd.Timedelta(minutes=140)
    )

    # Logging
    log(f"🛠️ Fixed {invalid_duration_mask.sum():,} negative-duration records")
    log(f"🛠️ Fixed {zero_length_mask.sum():,} zero-length records")

    # --------------------------------------------------
    # DEFINE MULTI-YEAR CALENDAR
    # --------------------------------------------------

    year_start = pd.Timestamp(f"{start_year}-07-01 00:00:00")
    year_end   = pd.Timestamp(f"{end_year}-06-30 23:59:00")

    intervals = pd.date_range(start=year_start, end=year_end, freq="1min")

    log(f"📅 Calendar built: {year_start} → {year_end} ({len(intervals):,} minutes)")

    # --------------------------------------------------
    # FILTER RECORDS
    # --------------------------------------------------

    df = df[
        (df['start_dtm'] <= year_end) &
        (df['end_dtm'] >= year_start)
    ].copy()

    log(f"🔎 Records overlapping range: {len(df):,}")

    # --------------------------------------------------
    # NORMALIZE ACUITY
    # --------------------------------------------------

    df['acuity_name'] = (
        df['acuity']
        .astype(str)
        .str.strip()
        .replace({'': np.nan})
        .map({
            'Non-Urgent': '5-Non-Urgent',
            'Less Urgent': '4-Less Urgent',
            'Urgent': '3-Urgent',
            'Emergent': '2-Emergent',
            'Immediate': '1-Immediate',
            '*Unspecified Acuity': '0-Unspecified',
        })
        .fillna('0-Unknown')
    )

    acuities = df['acuity_name'].unique()

    # --------------------------------------------------
    # CLIP VISITS
    # --------------------------------------------------

    df['start'] = df['start_dtm'].clip(lower=year_start, upper=year_end)
    df['end']   = df['end_dtm'].clip(lower=year_start, upper=year_end)
    df['end']   = df['end'] + pd.Timedelta(minutes=1)

    # --------------------------------------------------
    # EVENT ENGINE
    # --------------------------------------------------

    start_events = df[['acuity_name', 'start']].rename(columns={'start': 'interval'})
    start_events['delta'] = 1

    end_events = df[['acuity_name', 'end']].rename(columns={'end': 'interval'})
    end_events['delta'] = -1

    events = pd.concat([start_events, end_events])

    events = events[
        (events['interval'] >= year_start) &
        (events['interval'] <= year_end)
    ]

    events = (
        events.groupby(['acuity_name', 'interval'], as_index=False)['delta']
        .sum()
        .sort_values(['acuity_name', 'interval'])
    )

    # --------------------------------------------------
    # BUILD TIME GRID
    # --------------------------------------------------

    base = pd.MultiIndex.from_product(
        [acuities, intervals],
        names=['acuity_name', 'interval']
    ).to_frame(index=False)

    ts = base.merge(events, on=['acuity_name', 'interval'], how='left')
    ts['delta'] = ts['delta'].fillna(0)

    # --------------------------------------------------
    # BASELINE CARRYOVER
    # --------------------------------------------------

    initial_counts = (
        df[
            (df['start_dtm'] < year_start) &
            (df['end_dtm'] >= year_start)
        ]
        .groupby('acuity_name')
        .size()
    )

    # --------------------------------------------------
    # FINAL CENSUS
    # --------------------------------------------------

    ts['census'] = ts.groupby('acuity_name')['delta'].cumsum()
    ts['census'] += ts['acuity_name'].map(initial_counts).fillna(0)

    stagnes_emergency_ts = ts[['acuity_name', 'interval', 'census']]

    stagnes_emergency_ts["census"] = (
        pd.to_numeric(stagnes_emergency_ts["census"], errors="coerce")
        .round()
        .astype("Int64")
    )

    # --------------------------------------------------
    # NON-ACUITY ROLLUP (collapse across acuity)
    # --------------------------------------------------

    stagnes_emergency_rollup = (
        stagnes_emergency_ts
        .groupby("interval", as_index=False)["census"]
        .sum()
    )

    # Ensure clean integer typing
    stagnes_emergency_rollup["census"] = (
        pd.to_numeric(stagnes_emergency_rollup["census"], errors="coerce")
        .round()
        .astype("Int64")
    )

    # --------------------------------------------------
    # EXPORT OUTPUTS (Run folder)
    # --------------------------------------------------

    # --- ACUITY VERSION ---
    acuity_output = output_dir / f"{YEAR_PREFIX}_{split}_stagnes_emergency_ts.acuity.csv"
    stagnes_emergency_ts.to_csv(acuity_output, index=False)

    log(f"📤 Saved acuity output: {acuity_output} ({len(stagnes_emergency_ts):,} rows)")

    # --- NON-ACUITY ROLLUP ---
    rollup_output = output_dir / f"{YEAR_PREFIX}_{split}_stagnes_emergency_ts.non-acuity-rollup.csv"
    stagnes_emergency_rollup.to_csv(rollup_output, index=False)

    log(f"📤 Saved rollup output: {rollup_output} ({len(stagnes_emergency_rollup):,} rows)")

    # Per-acuity
    for acuity, sub_df in stagnes_emergency_ts.groupby('acuity_name'):
        safe_name = acuity.replace(' ', '_').replace('-', '_')
        file_path = output_dir / f"{YEAR_PREFIX}_{split}_stagnes_ed_{safe_name}.csv"
        sub_df.to_csv(file_path, index=False)
        log(f"📤 Saved acuity file: {file_path}")  

    # --------------------------------------------------
    # WRITE HYPER FILES
    # --------------------------------------------------

    def write_census_hyper(df_out, path):
        log(f"🧱 Writing census hyper: {path}")

        table = TableDefinition(
            table_name=TableName("Extract", "Extract"),
            columns=[
                TableDefinition.Column("acuity_name", SqlType.text()),
                TableDefinition.Column("interval", SqlType.timestamp()),
                TableDefinition.Column("census", SqlType.big_int()),
            ]
        )

        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=path,
                create_mode=CreateMode.CREATE_AND_REPLACE
            ) as conn:

                conn.catalog.create_schema("Extract")
                conn.catalog.create_table(table)

                with Inserter(conn, table) as inserter:
                    rows = [
                        (
                            r[0],              # acuity_name
                            r[1],              # interval
                            int(r[2]) if r[2] is not None else None  # census ✅ FORCE PYTHON INT
                        )
                        for r in df_out.itertuples(index=False, name=None)
                    ]

                    inserter.add_rows(rows)
                    inserter.execute()

        log(f"✅ Census hyper written")


    def write_uc_hyper(df_raw, path):
        log(f"🧱 Writing UC hyper: {path}")

        table = TableDefinition(
            TableName("Extract", "Extract"),
            [
                TableDefinition.Column("hdr_mdr", SqlType.text()),
                TableDefinition.Column("hdr_har", SqlType.text()),
                TableDefinition.Column("visit_dt", SqlType.date()),
                TableDefinition.Column("trg_complete_dtm", SqlType.timestamp()),
                TableDefinition.Column("arrival_dtm", SqlType.timestamp()),
                TableDefinition.Column("departure_dtm", SqlType.timestamp()),
                TableDefinition.Column("patient_type", SqlType.text()),
                TableDefinition.Column("age", SqlType.big_int()),
                TableDefinition.Column("gender", SqlType.text()),
                TableDefinition.Column("patient_zipcode", SqlType.text()),
                TableDefinition.Column("acuity_name", SqlType.text()),
                TableDefinition.Column("arrival_method", SqlType.text()),
                TableDefinition.Column("dss_entity", SqlType.text()),
                TableDefinition.Column("icd10_diagnosis", SqlType.text()),
                TableDefinition.Column("icd10_px", SqlType.text()),
                TableDefinition.Column("discharge_status", SqlType.text()),
                TableDefinition.Column("encounter_count", SqlType.big_int()),
            ]
        )

        # --------------------------------------------------
        # BUILD DATAFRAME (schema-aligned, no value coercion)
        # --------------------------------------------------

        df_uc = pd.DataFrame({

            # IDs (direct passthrough)
            "hdr_mdr": df_raw["patient_id"].astype(str),
            "hdr_har": df_raw["encounter_id"].astype(str),

            # Dates / timestamps (typed only)
            "visit_dt": pd.to_datetime(df_raw["visit_dtm"]).dt.date,
            "trg_complete_dtm": pd.to_datetime(df_raw["end_dtm"]),
            "arrival_dtm": pd.to_datetime(df_raw["start_dtm"]),
            "departure_dtm": pd.to_datetime(df_raw["end_dtm"]),

            # Dimensions (no remapping)
            "patient_type": df_raw["patient_type"],
            "age": 0,
            "gender": "UNK",
            "patient_zipcode": "99999",

            "acuity_name": df_raw["acuity_name"],  

            "arrival_method": "UNK",
            "dss_entity": "St Agnes Medical Center",

            # Clinical
            "icd10_diagnosis": "UNK",
            "icd10_px": "UNK",

            # Disposition
            "discharge_status": "UNK",

            # Measure (required)
            "encounter_count": 1
        })

        # ✅ CRITICAL: preserve NULLs for Hyper/Tableau
        df_uc = df_uc.where(pd.notnull(df_uc), None)

        text_cols = [
            "hdr_mdr", "hdr_har", "patient_type", "gender",
            "patient_zipcode", "acuity_name", "arrival_method",
            "dss_entity", "icd10_diagnosis", "icd10_px",
            "discharge_status"
        ]

        for col in text_cols:
            df_uc[col] = df_uc[col].astype(object).where(pd.notna(df_uc[col]), None)

        # --------------------------------------------------
        # WRITE TO HYPER
        # --------------------------------------------------

        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=path,
                create_mode=CreateMode.CREATE_AND_REPLACE
            ) as conn:

                conn.catalog.create_schema("Extract")
                conn.catalog.create_table(table)

                with Inserter(conn, table) as inserter:
                    rows = []
                    for r in df_uc.itertuples(index=False):

                        rows.append((
                            r.hdr_mdr,
                            r.hdr_har,
                            r.visit_dt,
                            r.trg_complete_dtm,
                            r.arrival_dtm,
                            r.departure_dtm,
                            r.patient_type,
                            int(r.age) if pd.notna(r.age) else None,
                            r.gender,
                            r.patient_zipcode,
                            r.acuity_name,
                            r.arrival_method,
                            r.dss_entity,
                            r.icd10_diagnosis,
                            r.icd10_px,
                            r.discharge_status,
                            1
                        ))

                    inserter.add_rows(rows)
                    inserter.execute()

        log(f"✅ UC hyper written")


    # --------------------------------------------------
    # EXECUTE HYPER WRITES
    # --------------------------------------------------

    census_hyper_path = output_dir / f"{YEAR_PREFIX}_{split}_ed_census.hyper"
    uc_hyper_path = output_dir / f"{YEAR_PREFIX}_{split}_ed_uc.hyper"

    try:
        write_census_hyper(stagnes_emergency_ts, census_hyper_path)
    except Exception as e:
        log(f"❌ Census hyper FAILED: {e}")

    try:
        write_uc_hyper(df, uc_hyper_path)
    except Exception as e:
        log(f"❌ UC hyper FAILED: {e}")

    # --------------------------------------------------
    # EXPORT PER-YEAR OUTPUTS
    # --------------------------------------------------

    log("📆 Creating per-year outputs...")

    # Add year column once
    stagnes_emergency_ts["year"] = stagnes_emergency_ts["interval"].dt.year

    for year, year_df in stagnes_emergency_ts.groupby("year"):

        # ---- FULL YEAR FILE ----
        year_output = output_dir / f"{year}_{split}_stagnes_emergency_ts.csv"
        year_df.drop(columns=["year"]).to_csv(year_output, index=False)

        log(f"📤 Saved yearly output: {year_output} ({len(year_df):,} rows)")

        # ---- PER-ACUITY WITHIN YEAR ----
        for acuity, sub_df in year_df.groupby("acuity_name"):
            safe_name = acuity.replace(" ", "_").replace("-", "_")

            file_path = output_dir / f"{year}_{split}_stagnes_ed_{safe_name}.csv"
            sub_df.drop(columns=["year"]).to_csv(file_path, index=False)

            log(f"📤 Saved yearly acuity file: {file_path}")

        # ---- NON-ACUITY YEAR ROLLUP ----
        year_rollup = (
            year_df
            .groupby("interval", as_index=False)["census"]
            .sum()
        )

        year_rollup_output = output_dir / f"{year}_{split}_stagnes_emergency_ts.non-acuity-rollup.csv"
        year_rollup.to_csv(year_rollup_output, index=False)

        log(f"📤 Saved yearly rollup: {year_rollup_output}")

        # ---- HYPER OUTPUTS PER YEAR ----

        # Acuity Hyper (same schema as main)
        year_hyper_path = output_dir / f"{year}_{split}_ed_census.hyper"

        try:
            write_census_hyper(
                year_df.drop(columns=["year"]),
                year_hyper_path
            )
        except Exception as e:
            log(f"❌ Year {year} census hyper FAILED: {e}")

# --------------------------------------------------
# FINALIZE
# --------------------------------------------------

elapsed = time.perf_counter() - START_TIME
log(f"✅ Completed in {elapsed:.2f} seconds test git")