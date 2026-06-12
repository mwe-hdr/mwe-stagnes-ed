import pandas as pd
import pyodbc
import numpy as np
import os
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

CODE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CODE_DIR.parent

DATA_DIR = PROJECT_ROOT / "data"
RUNS_DIR = DATA_DIR / "runs"
INPUT_BASE_DIR = DATA_DIR / "input"
OUTPUT_BASE_DIR = DATA_DIR / "output"

for d in [DATA_DIR, RUNS_DIR, INPUT_BASE_DIR, OUTPUT_BASE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

START_TIME = time.perf_counter()

# ✅ FIXED FISCAL YEAR
year_start = pd.Timestamp("2024-07-01 00:00:00")
year_end   = pd.Timestamp("2025-06-30 23:59:00")

YEAR_PREFIX = "FY2024_2025"

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

try:
    shutil.copy(__file__, code_dir / "main.py")
except:
    pass

log("🚀 Script started")
log(f"📁 Run directory: {run_dir}")

# --------------------------------------------------
# AUDIT CONFIG (OPTIONAL)
# --------------------------------------------------

AUDIT_DATE = "2024-10-15"   

if AUDIT_DATE:
    audit_date = pd.Timestamp(AUDIT_DATE)
    audit_start = audit_date.normalize()
    audit_end = audit_start + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
    log(f"🔍 Audit mode enabled for {audit_date.date()}")

# --------------------------------------------------
# CAPACITY CONFIG
# --------------------------------------------------

ED_CAPACITY = 60   
CAPACITY_THRESHOLD = 0.8

CAPACITY_80 = int(ED_CAPACITY * CAPACITY_THRESHOLD)

log(f"🏥 Capacity set to {ED_CAPACITY}")
log(f"⚠️  80% threshold = {CAPACITY_80}")

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

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

df = pd.read_sql("SELECT * FROM stagnes.emergency", conn)
log(f"✅ Loaded {len(df):,} records from database")
df_raw = df.copy()

# --------------------------------------------------
# CANONICAL START/END
# --------------------------------------------------

for col in [
    "ed_start_dtm", "ed_end_dtm",
    "triage_start_dtm", "triage_stop_dtm",
    "visit_dtm"
]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

ed_missing_mask = df["ed_start_dtm"].isna() & df["ed_end_dtm"].isna()
triage_incomplete_mask = (
    df["triage_start_dtm"].isna() | df["triage_stop_dtm"].isna()
)
visit_fallback_mask = ed_missing_mask & triage_incomplete_mask

df["start_dtm"] = df["ed_start_dtm"]
df["end_dtm"]   = df["ed_end_dtm"]

triage_valid_mask = ed_missing_mask & ~triage_incomplete_mask

df.loc[triage_valid_mask, "start_dtm"] = df.loc[triage_valid_mask, "triage_start_dtm"]
df.loc[triage_valid_mask, "end_dtm"]   = df.loc[triage_valid_mask, "triage_stop_dtm"]

visit_valid_mask = visit_fallback_mask & df["visit_dtm"].notna()

df.loc[visit_valid_mask, "start_dtm"] = df.loc[visit_valid_mask, "visit_dtm"]
df.loc[visit_valid_mask, "end_dtm"]   = (
    df.loc[visit_valid_mask, "visit_dtm"] + pd.Timedelta(minutes=1)
)

log(f"✅ Canonical timestamps created")

# --------------------------------------------------
# SAVE INPUT SNAPSHOTS
# --------------------------------------------------

df_raw.to_csv(input_dir / f"{YEAR_PREFIX}_raw.csv", index=False)
df.to_csv(input_dir / f"{YEAR_PREFIX}_enriched.csv", index=False)

# --------------------------------------------------
# CLEAN TIMESTAMPS
# --------------------------------------------------

df['start_dtm'] = pd.to_datetime(df['start_dtm'], errors='coerce')
df['end_dtm']   = pd.to_datetime(df['end_dtm'], errors='coerce')

df = df.dropna(subset=['start_dtm', 'end_dtm'])

invalid_mask = df['end_dtm'] < df['start_dtm']
zero_mask = df['end_dtm'] == df['start_dtm']

df.loc[invalid_mask, 'end_dtm'] = df['start_dtm'] + pd.Timedelta(minutes=140)
df.loc[zero_mask, 'end_dtm']    = df['start_dtm'] + pd.Timedelta(minutes=140)

# --------------------------------------------------
# FILTER TO FISCAL YEAR
# --------------------------------------------------

df = df[
    (df['start_dtm'] <= year_end) &
    (df['end_dtm'] >= year_start)
].copy()

log(f"🔎 Records in fiscal window: {len(df):,}")

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

events = (
    events.groupby(['acuity_name', 'interval'], as_index=False)['delta']
    .sum()
    .sort_values(['acuity_name', 'interval'])
)

# --------------------------------------------------
# BUILD TIME GRID
# --------------------------------------------------

intervals = pd.date_range(start=year_start, end=year_end, freq="1min")

acuities = df['acuity_name'].unique()

base = pd.MultiIndex.from_product(
    [acuities, intervals],
    names=['acuity_name', 'interval']
).to_frame(index=False)

ts = base.merge(events, on=['acuity_name', 'interval'], how='left')
ts['delta'] = ts['delta'].fillna(0)

# --------------------------------------------------
# FINAL CENSUS
# --------------------------------------------------

ts['census'] = ts.groupby('acuity_name')['delta'].cumsum()

stagnes_emergency_ts = ts[['acuity_name', 'interval', 'census']]
stagnes_emergency_ts["census"] = stagnes_emergency_ts["census"].astype("Int64")

# --------------------------------------------------
# AUDIT OUTPUTS
# --------------------------------------------------

if AUDIT_DATE:

    log("🧪 Building detailed audit outputs...")

    # ---------------------------
    # FILTER MINUTE-LEVEL SERIES
    # ---------------------------
    audit_ts = stagnes_emergency_ts[
        (stagnes_emergency_ts["interval"] >= audit_start) &
        (stagnes_emergency_ts["interval"] <= audit_end)
    ].copy()

    # ---------------------------
    # ADD DELTA COLUMN BACK IN
    # ---------------------------
    # rebuild delta for audit transparency
    audit_events = events.copy()

    audit_events = audit_events[
        (audit_events["interval"] >= audit_start) &
        (audit_events["interval"] <= audit_end)
    ]

    audit_detail = audit_ts.merge(
        audit_events,
        on=["acuity_name", "interval"],
        how="left"
    )

    audit_detail["delta"] = audit_detail["delta"].fillna(0)

    # ---------------------------
    # ADD ROLLUP BY MINUTE
    # ---------------------------
    audit_rollup = (
        audit_detail.groupby("interval")["census"]
        .sum()
        .rename("total_census")
        .reset_index()
    )

    audit_detail = audit_detail.merge(
        audit_rollup,
        on="interval",
        how="left"
    )

    audit_detail["above_80"] = audit_detail["total_census"] >= CAPACITY_80

    # ---------------------------
    # SORT CLEANLY
    # ---------------------------
    audit_detail = audit_detail.sort_values(
        ["interval", "acuity_name"]
    )

    # ---------------------------
    # SUMMARY METRICS
    # ---------------------------

    summary = pd.DataFrame([
        {"metric": "start_of_day", "value": audit_rollup["total_census"].iloc[0]},
        {"metric": "max_census",  "value": audit_rollup["total_census"].max()},
        {"metric": "end_of_day", "value": audit_rollup["total_census"].iloc[-1]},
    ])

    # ---------------------------
    # ENCOUNTERS SUPPORTING DAY
    # ---------------------------
    audit_encounters = df[
        (df["start"] <= audit_end) &
        (df["end"] >= audit_start)
    ].copy()

    # Add duration overlap for clarity
    audit_encounters["overlap_start"] = audit_encounters["start"].clip(lower=audit_start)
    audit_encounters["overlap_end"]   = audit_encounters["end"].clip(upper=audit_end)

    audit_encounters["overlap_minutes"] = (
        (audit_encounters["overlap_end"] - audit_encounters["overlap_start"])
        .dt.total_seconds() / 60
    )

    audit_encounters["encounter_key"] = (
    audit_encounters["patient_id"].astype(str) + "_" +
    audit_encounters["encounter_id"].astype(str)
    )   

    # ---------------------------
    # AUDIT CAPACITY METRICS
    # ---------------------------

    audit_rollup["above_80"] = audit_rollup["total_census"] >= CAPACITY_80

    audit_minutes_above_80 = audit_rollup["above_80"].sum()
    audit_total_minutes = len(audit_rollup)

    audit_pct_above_80 = (
        audit_minutes_above_80 / audit_total_minutes
        if audit_total_minutes > 0 else 0
    )

    audit_capacity_summary = pd.DataFrame([
        {"metric": "capacity", "value": ED_CAPACITY},
        {"metric": "threshold_80_pct", "value": CAPACITY_80},
        {"metric": "minutes_above_80", "value": int(audit_minutes_above_80)},
        {"metric": "total_minutes", "value": int(audit_total_minutes)},
        {"metric": "pct_time_above_80", "value": audit_pct_above_80},
    ])

    audit_capacity_summary.to_csv(
        output_dir / f"{YEAR_PREFIX}_capacity.audit.summary.csv",
        index=False
    )

    # ---------------------------
    # EXPORT FILES
    # ---------------------------
    audit_detail.to_csv(
        output_dir / f"{YEAR_PREFIX}_census.audit.detail.csv",
        index=False
    )

    summary.to_csv(
        output_dir / f"{YEAR_PREFIX}_census.audit.summary.csv",
        index=False
    )

    audit_encounters.to_csv(
        output_dir / f"{YEAR_PREFIX}_census.audit.encounters.csv",
        index=False
    )

    audit_sentence = (
    f"On {audit_date.date()}, the emergency department operated at or above "
    f"80% of capacity for {audit_minutes_above_80:,} minutes "
    f"({audit_pct_above_80:.1%} of the day)."
    )

    audit_capacity_summary.loc[len(audit_capacity_summary)] = {
        "metric": "narrative",
        "value": audit_sentence
    }

    log("✅ Audit outputs written")

# --------------------------------------------------
# ROLLUP
# --------------------------------------------------

rollup = (
    stagnes_emergency_ts
    .groupby("interval", as_index=False)["census"]
    .sum()
)

# --------------------------------------------------
# CAPACITY ANALYSIS (FULL FY)
# --------------------------------------------------

rollup["above_80"] = rollup["census"] >= CAPACITY_80

minutes_above_80 = rollup["above_80"].sum()
total_minutes = len(rollup)

pct_above_80 = minutes_above_80 / total_minutes if total_minutes > 0 else 0

capacity_summary = pd.DataFrame([
    {"metric": "capacity", "value": ED_CAPACITY},
    {"metric": "threshold_80_pct", "value": CAPACITY_80},
    {"metric": "minutes_above_80", "value": int(minutes_above_80)},
    {"metric": "total_minutes", "value": int(total_minutes)},
    {"metric": "pct_time_above_80", "value": pct_above_80},
])

capacity_summary.to_csv(
    output_dir / f"{YEAR_PREFIX}_capacity_summary.csv",
    index=False
)

log(f"✅ Minutes above 80% capacity: {minutes_above_80:,}")
log(f"✅ Percent time above 80%: {pct_above_80:.2%}")

# --------------------------------------------------
# CAPACITY NARRATIVE SENTENCE
# --------------------------------------------------

sentence = (
    f"During the fiscal year, the emergency department operated at or above "
    f"80% of its {ED_CAPACITY}-bed capacity ({CAPACITY_80} patients) for "
    f"approximately {minutes_above_80:,.0f} minutes—representing "
    f"{pct_above_80:.1%} of the total observed time."
)

# Add to summary table
capacity_summary.loc[len(capacity_summary)] = {
    "metric": "narrative",
    "value": sentence
}

# Optional: also write to a standalone text file (nice for slides)
with open(output_dir / f"{YEAR_PREFIX}_capacity_summary.txt", "w", encoding="utf-8") as f:
    f.write(sentence)

# --------------------------------------------------
# EXPORT CSV
# --------------------------------------------------

stagnes_emergency_ts.to_csv(
    output_dir / f"{YEAR_PREFIX}_ed_census.acuity.csv", index=False
)

rollup.to_csv(
    output_dir / f"{YEAR_PREFIX}_ed_census.rollup.csv", index=False
)

log("📤 CSV outputs written")

# --------------------------------------------------
# HYPER WRITERS 
# --------------------------------------------------

def write_census_hyper(df_out, path):

    table = TableDefinition(
        TableName("Extract", "Extract"),
        [
            TableDefinition.Column("acuity_name", SqlType.text()),
            TableDefinition.Column("interval", SqlType.timestamp()),
            TableDefinition.Column("census", SqlType.big_int()),
        ],
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
                    (r[0], r[1], int(r[2]) if r[2] is not None else None)
                    for r in df_out.itertuples(index=False, name=None)
                ]
                inserter.add_rows(rows)
                inserter.execute()


def write_uc_hyper(df_raw, path):

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
        ],
    )

    df_uc = pd.DataFrame({
        "hdr_mdr": df_raw["patient_id"].astype(str),
        "hdr_har": df_raw["encounter_id"].astype(str),
        "visit_dt": pd.to_datetime(df_raw["start_dtm"]).dt.date,
        "trg_complete_dtm": df_raw["end_dtm"],
        "arrival_dtm": df_raw["start_dtm"],
        "departure_dtm": df_raw["end_dtm"],
        "patient_type": df_raw["patient_type"],
        "age": 0,
        "gender": "UNK",
        "patient_zipcode": "99999",
        "acuity_name": df_raw["acuity_name"],
        "arrival_method": "UNK",
        "dss_entity": "St Agnes Medical Center",
        "icd10_diagnosis": "UNK",
        "icd10_px": "UNK",
        "discharge_status": "UNK",
        "encounter_count": 1
    })

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=path,
            create_mode=CreateMode.CREATE_AND_REPLACE
        ) as conn:

            conn.catalog.create_schema("Extract")
            conn.catalog.create_table(table)

            with Inserter(conn, table) as inserter:
                inserter.add_rows(list(df_uc.itertuples(index=False, name=None)))
                inserter.execute()

# --------------------------------------------------
# WRITE HYPERS
# --------------------------------------------------

write_census_hyper(
    stagnes_emergency_ts,
    output_dir / f"{YEAR_PREFIX}_ed_census.hyper"
)

write_uc_hyper(
    df,
    output_dir / f"{YEAR_PREFIX}_ed_uc.hyper"
)

# --------------------------------------------------
# FINALIZE
# --------------------------------------------------

elapsed = time.perf_counter() - START_TIME
log(f"✅ Completed in {elapsed:.2f} seconds (FY run)")