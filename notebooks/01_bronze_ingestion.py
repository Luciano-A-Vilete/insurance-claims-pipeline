# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest SUSEP AUTOSEG insurance data from CSV into Delta Lake.
# MAGIC
# MAGIC **Source files:**
# MAGIC - `PremReg.csv` — premiums and exposure by region (205 records)
# MAGIC - `SinReg.csv`  — claims and indemnizations by region (200 records)
# MAGIC
# MAGIC **Design decisions:**
# MAGIC - No business transformations at this layer — raw data preserved as-is
# MAGIC - Schema explicitly defined (never inferred) for stability
# MAGIC - Numeric fields read as StringType — Brazilian CSVs use comma as decimal
# MAGIC   separator (e.g. `369319,443`), which breaks DoubleType parsing silently.
# MAGIC   Type casting with proper locale handling happens at Silver.
# MAGIC - Semestre added as metadata column for partitioning — source files have
# MAGIC   no date column, so we inject the reference period at ingestion time
# MAGIC - **Idempotent:** re-running overwrites only the current semestre partition
# MAGIC
# MAGIC **Source:** SUSEP AUTOSEG — https://dados.gov.br/dataset/dados-estatisticos-do-seguro-de-automoveis-autoseg
# MAGIC
# MAGIC **Output:**
# MAGIC - `/Volumes/project_pipeline_databricks/default/pipeline_databricks_project/bronze/premios/`
# MAGIC - `/Volumes/project_pipeline_databricks/default/pipeline_databricks_project/bronze/sinistros/`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# ── Semestre reference (format: YYYYSn) ───────────────────────────────────────
# Change this to match the period of the files you downloaded from SUSEP
SEMESTRE = "2019S2"

# ── Paths (DBFS — Databricks Community Edition) ───────────────────────────────
PREM_SOURCE = "/Volumes/project_pipeline_databricks/default/pipeline_databricks_project/PremReg.csv"
SIN_SOURCE  = "/Volumes/project_pipeline_databricks/default/pipeline_databricks_project/SinReg.csv"
BRONZE_PREM = "/Volumes/project_pipeline_databricks/default/pipeline_databricks_project/bronze/premios/"
BRONZE_SIN  = "/Volumes/project_pipeline_databricks/default/pipeline_databricks_project/bronze/sinistros/"

print(f"Semestre      : {SEMESTRE}")
print(f"Prêmios source: {PREM_SOURCE}")
print(f"Sinistros src : {SIN_SOURCE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Schema Definition
# MAGIC
# MAGIC Schemas match exact column names from the SUSEP AUTOSEG data dictionary.
# MAGIC
# MAGIC All numeric fields are intentionally read as StringType at Bronze.
# MAGIC Brazilian CSVs use comma as decimal separator (`369319,443`), which
# MAGIC causes silent data loss if cast to DoubleType at read time.
# MAGIC Proper type casting with `regexp_replace` happens at Silver.

# COMMAND ----------

# PremReg.csv — premiums and exposure by region
# Source columns: regiao;descricao;tipo_prem;exposicao;premios;is_media
SCHEMA_PREMIOS = StructType([
    StructField("regiao",    StringType(), True),  # Region code (01–41)
    StructField("descricao", StringType(), True),  # Region name (e.g. "RS - Met. Porto Alegre")
    StructField("tipo_prem", StringType(), True),  # Premium type: APP, CASCO, etc.
    StructField("exposicao", StringType(), True),  # Vehicle-months exposed (comma decimal)
    StructField("premios",   StringType(), True),  # Total premiums in BRL (comma decimal)
    StructField("is_media",  StringType(), True),  # Average insured amount BRL (comma decimal)
])

# SinReg.csv — claims and indemnizations by region
# Source columns: regiao;descricao;tipo_sin;numSinistros;indenizacoes
SCHEMA_SINISTROS = StructType([
    StructField("regiao",       StringType(), True),  # Region code (joins with PremReg)
    StructField("descricao",    StringType(), True),  # Region name
    StructField("tipo_sin",     StringType(), True),  # Claim type: APP, CASCO, etc.
    StructField("numSinistros", StringType(), True),  # Number of claims (integer)
    StructField("indenizacoes", StringType(), True),  # Total indemnizations BRL (integer)
])

print("SCHEMA_PREMIOS:")
for f in SCHEMA_PREMIOS:
    print(f"  {f.name:<15} {f.dataType}")

print("\nSCHEMA_SINISTROS:")
for f in SCHEMA_SINISTROS:
    print(f"  {f.name:<15} {f.dataType}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Read Source CSVs
# MAGIC
# MAGIC SUSEP AUTOSEG files use:
# MAGIC - Semicolon (`;`) as column delimiter
# MAGIC - ISO-8859-1 (Latin-1) encoding — required for Portuguese characters (ã, ç, etc.)
# MAGIC - Comma (`,`) as decimal separator in numeric fields

# COMMAND ----------

def read_susep_csv(path, schema):
    """Read a SUSEP AUTOSEG CSV with correct encoding and delimiter."""
    return (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header",     "true")
        .option("sep",        ";")
        .option("encoding",   "ISO-8859-1")
        .option("nullValue",  "")
        .option("emptyValue", "")
        .load(path)
    )

df_premios   = read_susep_csv(PREM_SOURCE, SCHEMA_PREMIOS)
df_sinistros = read_susep_csv(SIN_SOURCE,  SCHEMA_SINISTROS)

prem_count = df_premios.count()
sin_count  = df_sinistros.count()

print(f"PremReg records read : {prem_count:,}")
print(f"SinReg  records read : {sin_count:,}")

if prem_count == 0 or sin_count == 0:
    raise ValueError(
        "One or both source files returned 0 records. "
        "Check that PremReg.csv and SinReg.csv are uploaded to DBFS."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Add Ingestion Metadata
# MAGIC
# MAGIC Three metadata columns added at Bronze:
# MAGIC - `semestre`    — reference period ("2019S2"), used for partitioning
# MAGIC - `ingested_at` — exact timestamp of this run, for traceability
# MAGIC - `source_file` — which file each record came from, for debugging

# COMMAND ----------

def add_metadata(df, semestre):
    """Add standard Bronze metadata columns."""
    return (
        df
        .withColumn("semestre",    lit(semestre))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

df_premios_bronze   = add_metadata(df_premios,   SEMESTRE)
df_sinistros_bronze = add_metadata(df_sinistros, SEMESTRE)

# Quick sanity checks
print("Distribution by tipo_prem:")
df_premios_bronze.groupBy("tipo_prem").count().orderBy("tipo_prem").show()

print("Distribution by tipo_sin:")
df_sinistros_bronze.groupBy("tipo_sin").count().orderBy("tipo_sin").show()

print("Regions in Prêmios:")
df_premios_bronze.select("regiao", "descricao").distinct().orderBy("regiao").show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write to Delta Lake (Idempotent)
# MAGIC
# MAGIC **Why partition by semestre?**
# MAGIC SUSEP AUTOSEG releases one file per semester. Partitioning by semestre means:
# MAGIC - Each file maps to exactly one partition
# MAGIC - Re-running for the same semester overwrites only that partition
# MAGIC - Historical semesters are untouched when adding new data
# MAGIC
# MAGIC **Why replaceWhere instead of overwrite?**
# MAGIC Plain `overwrite` would delete all historical semesters on every run.
# MAGIC `replaceWhere` surgically replaces only the partition matching the current
# MAGIC semestre — making re-runs completely safe.

# COMMAND ----------

def write_bronze_delta(df, path, semestre):
    """
    Write DataFrame to Delta Lake partitioned by semestre.
    Idempotent: overwrites only the current semestre partition.
    """
    table_exists = DeltaTable.isDeltaTable(spark, path)

    write_op = (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("semestre")
    )

    if table_exists:
        write_op.option("replaceWhere", f"semestre = '{semestre}'").save(path)
        print(f"  Updated existing partition semestre={semestre} at {path}")
    else:
        write_op.save(path)
        print(f"  Created new Delta table at {path}")

print("Writing Bronze — Prêmios...")
write_bronze_delta(df_premios_bronze,   BRONZE_PREM, SEMESTRE)

print("\nWriting Bronze — Sinistros...")
write_bronze_delta(df_sinistros_bronze, BRONZE_SIN,  SEMESTRE)

print("\nDone.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Validation

# COMMAND ----------

df_prem_out = spark.read.format("delta").load(BRONZE_PREM)
df_sin_out  = spark.read.format("delta").load(BRONZE_SIN)

print("=" * 55)
print("BRONZE LAYER — INGESTION SUMMARY")
print("=" * 55)
print(f"Semestre                    : {SEMESTRE}")
print(f"Prêmios   — source records  : {prem_count:,}")
print(f"Prêmios   — written records : {df_prem_out.count():,}")
print(f"Sinistros — source records  : {sin_count:,}")
print(f"Sinistros — written records : {df_sin_out.count():,}")
print("=" * 55)

print("\nDelta history — Prêmios (last 3 operations):")
DeltaTable.forPath(spark, BRONZE_PREM) \
    .history(3) \
    .select("version", "timestamp", "operation") \
    .show(truncate=False)

print("Sample — Prêmios (5 rows):")
df_prem_out.show(5, truncate=True)

print("Sample — Sinistros (5 rows):")
df_sin_out.show(5, truncate=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Next Step
# MAGIC
# MAGIC Bronze layer complete. Proceed to:
# MAGIC **`02_silver_transformation.py`**
# MAGIC
# MAGIC Silver will:
# MAGIC - Replace comma decimals with dot (`369319,44` → `369319.44`)
# MAGIC - Cast all numeric fields to proper types (DoubleType, LongType)
# MAGIC - Join Prêmios + Sinistros on `regiao` + `tipo`
# MAGIC - Enrich with dimension table `auto_reg` (region descriptions)
# MAGIC - Quarantine any records that fail validation
# MAGIC
# MAGIC ---
# MAGIC *Pipeline: insurance-claims-pipeline | Layer: Bronze | Author: Luciano Vilete*