"""
Pipeline configuration — insurance-claims-pipeline
All paths and parameters in one place.
Change SEMESTRE to process a different SUSEP AUTOSEG release.
"""

# ── Reference period ──────────────────────────────────────────────────────────
# Format: YYYYSn (e.g. "2019S2" = second semester of 2019)
# Must match the SUSEP AUTOSEG files you downloaded.
SEMESTRE = "YOUR_SEMESTRE"   # e.g. "2019S2"

# ── Unity Catalog coordinates ─────────────────────────────────────────────────
# Replace with your own catalog, schema, and volume names before running
CATALOG = "YOUR_CATALOG"     # e.g. "project_pipeline_databricks"
SCHEMA  = "YOUR_SCHEMA"      # e.g. "default"
VOLUME  = "YOUR_VOLUME"      # e.g. "pipeline_databricks_project"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# ── Source files ──────────────────────────────────────────────────────────────
PREM_SOURCE   = f"{BASE}/PremReg.csv"
SIN_SOURCE    = f"{BASE}/SinReg.csv"
AUTO_REG_PATH = f"{BASE}/auto_reg.csv"

# ── Bronze paths ──────────────────────────────────────────────────────────────
BRONZE_PREM = f"{BASE}/bronze/premios/"
BRONZE_SIN  = f"{BASE}/bronze/sinistros/"

# ── Silver paths ──────────────────────────────────────────────────────────────
SILVER_PATH     = f"{BASE}/silver/sinistralidade/"
QUARANTINE_PATH = f"{BASE}/silver/_quarantine/"

# ── Gold paths ────────────────────────────────────────────────────────────────
GOLD_BY_REGION = f"{BASE}/gold/by_region/"
GOLD_BY_TIPO   = f"{BASE}/gold/by_tipo/"
GOLD_ALERTS    = f"{BASE}/gold/alerts/"
GOLD_SUMMARY   = f"{BASE}/gold/summary/"

# ── CSV read options (common to all SUSEP files) ──────────────────────────────
CSV_OPTIONS = {
    "header":     "true",
    "sep":        ";",
    "encoding":   "ISO-8859-1",
    "nullValue":  "",
    "emptyValue": "",
}

# ── Business thresholds ───────────────────────────────────────────────────────
SINISTRALIDADE_LOSS_THRESHOLD  = 1.0   # above = loss-making segment
SINISTRALIDADE_ALERT_THRESHOLD = 2.0   # above = data quality alert