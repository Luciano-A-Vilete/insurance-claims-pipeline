# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Transformation & Join
# MAGIC
# MAGIC **Purpose:** Transform Bronze raw data into clean, typed, joined records
# MAGIC ready for Gold aggregations.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Read Bronze Delta tables (premios + sinistros)
# MAGIC 2. Cast numeric fields — replace Brazilian comma decimal with dot
# MAGIC 3. Join Prêmios + Sinistros on `regiao` + `tipo` (left join to preserve all premium rows)
# MAGIC 4. Enrich with `auto_reg` dimension table for validated region descriptions
# MAGIC 5. Validate: quarantine records that fail business rules
# MAGIC 6. Write clean records to Silver Delta table
# MAGIC
# MAGIC **Design decisions:**
# MAGIC - Quarantine over hard drops — invalid records are visible, traceable, recoverable
# MAGIC - Left join on premios → sinistros: a region with premiums but no claims is valid data
# MAGIC - `auto_reg` used as reference for region validation (not just enrichment)
# MAGIC - Idempotent: replaceWhere on semestre
# MAGIC
# MAGIC **Input:**
# MAGIC - `bronze/premios/`   — Delta, partitioned by semestre
# MAGIC - `bronze/sinistros/` — Delta, partitioned by semestre
# MAGIC - `auto_reg.csv`      — dimension table (41 regions)
# MAGIC
# MAGIC **Output:**
# MAGIC - `/Volumes/.../silver/sinistralidade/` — clean joined Delta table
# MAGIC - `/Volumes/.../silver/_quarantine/`    — failed records with failure_reason

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, regexp_replace,
    when, isnull, round as spark_round,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from delta.tables import DeltaTable

# ── Semestre — must match what was ingested in notebook 01 ────────────────────
SEMESTRE = "YOUR_SEMESTRE"   # e.g. "2019S2"

# ── Unity Catalog coordinates ─────────────────────────────────────────────────
CATALOG = "YOUR_CATALOG"     # e.g. "project_pipeline_databricks"
SCHEMA  = "YOUR_SCHEMA"      # e.g. "default"
VOLUME  = "YOUR_VOLUME"      # e.g. "pipeline_databricks_project"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# ── Input paths (Bronze) ──────────────────────────────────────────────────────
BRONZE_PREM    = f"{BASE}/bronze/premios/"
BRONZE_SIN     = f"{BASE}/bronze/sinistros/"
AUTO_REG_PATH  = f"{BASE}/auto_reg.csv"

# ── Output paths (Silver) ─────────────────────────────────────────────────────
SILVER_PATH     = f"{BASE}/silver/sinistralidade/"
QUARANTINE_PATH = f"{BASE}/silver/_quarantine/"

print(f"Semestre        : {SEMESTRE}")
print(f"Bronze premios  : {BRONZE_PREM}")
print(f"Bronze sinistros: {BRONZE_SIN}")
print(f"auto_reg        : {AUTO_REG_PATH}")
print(f"Silver output   : {SILVER_PATH}")
print(f"Quarantine      : {QUARANTINE_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read Bronze Delta Tables

# COMMAND ----------

# Read only the current semestre partition — partition pruning in action
df_prem = (
    spark.read.format("delta").load(BRONZE_PREM)
    .filter(col("semestre") == SEMESTRE)
)

df_sin = (
    spark.read.format("delta").load(BRONZE_SIN)
    .filter(col("semestre") == SEMESTRE)
)

print(f"Bronze premios  ({SEMESTRE}): {df_prem.count():,} records")
print(f"Bronze sinistros ({SEMESTRE}): {df_sin.count():,} records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Read Dimension Table — auto_reg
# MAGIC
# MAGIC `auto_reg.csv` maps region codes to official SUSEP region descriptions.
# MAGIC Used to validate that every `regiao` in the main tables is a known region.

# COMMAND ----------

SCHEMA_AUTO_REG = StructType([
    StructField("CODIGO",   StringType(), True),
    StructField("DESCRICAO", StringType(), True),
])

df_auto_reg = (
    spark.read
    .format("csv")
    .schema(SCHEMA_AUTO_REG)
    .option("header",   "true")
    .option("sep",      ";")
    .option("encoding", "ISO-8859-1")
    .load(AUTO_REG_PATH)
    .withColumnRenamed("CODIGO",   "reg_codigo")
    .withColumnRenamed("DESCRICAO", "reg_descricao_oficial")
)

reg_count = df_auto_reg.count()
print(f"auto_reg loaded: {reg_count} regions")
df_auto_reg.show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Type Casting
# MAGIC
# MAGIC Brazilian CSVs use comma as decimal separator (`369319,443`).
# MAGIC We replace comma with dot before casting to DoubleType.
# MAGIC Null-safe: if `regexp_replace` returns empty string, cast produces null
# MAGIC — which is caught by the quarantine step below.

# COMMAND ----------

def br_to_double(column_name):
    """Convert Brazilian decimal string (comma) to DoubleType."""
    return regexp_replace(col(column_name), ",", ".").cast(DoubleType())

def to_long(column_name):
    """Cast string to LongType (for integer fields like numSinistros)."""
    return col(column_name).cast(LongType())

# Cast Prêmios numeric fields
df_prem_typed = (
    df_prem
    .withColumn("exposicao_num", br_to_double("exposicao"))
    .withColumn("premios_num",   br_to_double("premios"))
    .withColumn("is_media_num",  br_to_double("is_media"))
    .select(
        col("regiao"),
        col("descricao").alias("descricao_prem"),
        col("tipo_prem"),
        col("exposicao_num").alias("exposicao"),
        col("premios_num").alias("premios"),
        col("is_media_num").alias("is_media"),
        col("semestre"),
        col("ingested_at").alias("prem_ingested_at"),
    )
)

# Cast Sinistros numeric fields
df_sin_typed = (
    df_sin
    .withColumn("numSinistros_num",  to_long("numSinistros"))
    .withColumn("indenizacoes_num",  br_to_double("indenizacoes"))
    .select(
        col("regiao").alias("sin_regiao"),
        col("tipo_sin"),
        col("numSinistros_num").alias("num_sinistros"),
        col("indenizacoes_num").alias("indenizacoes"),
        col("semestre").alias("sin_semestre"),
        col("ingested_at").alias("sin_ingested_at"),
    )
)

print("Prêmios typed sample:")
df_prem_typed.show(3, truncate=False)

print("Sinistros typed sample:")
df_sin_typed.show(3, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Join Prêmios + Sinistros
# MAGIC
# MAGIC Join key: `regiao` + `tipo` (tipo_prem in premios = tipo_sin in sinistros).
# MAGIC
# MAGIC **Why LEFT join?**
# MAGIC A region with premiums collected but zero claims is valid and important data
# MAGIC — it means low-risk regions. A plain INNER join would silently drop those rows.
# MAGIC
# MAGIC After join, sinistralidade is computed:
# MAGIC `sinistralidade = indenizacoes / premios`
# MAGIC A ratio > 1.0 means the insurer paid more than it collected in that region.

# COMMAND ----------

df_joined = (
    df_prem_typed
    .join(
        df_sin_typed,
        on=(
            (col("regiao") == col("sin_regiao")) &
            (col("tipo_prem") == col("tipo_sin"))
        ),
        how="left"
    )
    # Fill nulls from left join: regions with no claims have 0 sinistros
    .withColumn("num_sinistros", when(isnull(col("num_sinistros")), 0).otherwise(col("num_sinistros")))
    .withColumn("indenizacoes",  when(isnull(col("indenizacoes")),  0.0).otherwise(col("indenizacoes")))
    # Compute sinistralidade — the key metric
    .withColumn(
        "sinistralidade",
        when(col("premios") > 0,
            spark_round(col("indenizacoes") / col("premios"), 4)
        ).otherwise(None)
    )
    # Drop redundant join columns
    .drop("sin_regiao", "tipo_sin", "sin_semestre", "sin_ingested_at")
)

print(f"Joined records: {df_joined.count():,}")
print("\nSample (sorted by sinistralidade desc):")
df_joined.orderBy(col("sinistralidade").desc()).show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Enrich with auto_reg Dimension
# MAGIC
# MAGIC Left join with `auto_reg` to add the official SUSEP region description
# MAGIC and to flag any `regiao` codes that don't exist in the reference table
# MAGIC (those will be quarantined in the next step).

# COMMAND ----------

df_enriched = (
    df_joined
    .join(df_auto_reg, on=(col("regiao") == col("reg_codigo")), how="left")
    .drop("reg_codigo")
    # Add Silver metadata
    .withColumn("processed_at", current_timestamp())
    .withColumn("semestre",     lit(SEMESTRE))
)

print(f"Enriched records: {df_enriched.count():,}")
df_enriched.select(
    "regiao", "reg_descricao_oficial", "tipo_prem",
    "premios", "indenizacoes", "sinistralidade"
).show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Validation & Quarantine
# MAGIC
# MAGIC Records that fail any rule are written to `_quarantine` with a
# MAGIC `failure_reason` column — not dropped silently.
# MAGIC
# MAGIC **Rules:**
# MAGIC - `regiao` must not be null
# MAGIC - `premios` must be > 0 (can't compute sinistralidade on zero premium)
# MAGIC - `indenizacoes` must be >= 0 (negative indemnization = data error)
# MAGIC - `regiao` must exist in auto_reg (reg_descricao_oficial not null after join)
# MAGIC - `sinistralidade` alert if > 2.0 (not quarantined, but flagged)

# COMMAND ----------

# Tag each record with failure reason (null = passed all checks)
df_validated = (
    df_enriched
    .withColumn(
        "failure_reason",
        when(isnull(col("regiao")),                   lit("null regiao"))
        .when(isnull(col("premios")),                 lit("null premios after cast"))
        .when(col("premios") <= 0,                    lit("premios <= 0"))
        .when(col("indenizacoes") < 0,                lit("indenizacoes < 0"))
        .when(isnull(col("reg_descricao_oficial")),   lit("unknown regiao — not in auto_reg"))
        .otherwise(None)
    )
    .withColumn(
        "sinistralidade_alert",
        when(col("sinistralidade") > 2.0, lit(True)).otherwise(lit(False))
    )
)

# Split: clean vs quarantine
df_clean      = df_validated.filter(col("failure_reason").isNull()).drop("failure_reason")
df_quarantine = df_validated.filter(col("failure_reason").isNotNull())

clean_count      = df_clean.count()
quarantine_count = df_quarantine.count()

print(f"Clean records     : {clean_count:,}")
print(f"Quarantine records: {quarantine_count:,}")

if quarantine_count > 0:
    print("\nQuarantine breakdown:")
    df_quarantine.groupBy("failure_reason").count().show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Write Silver Delta Tables (Idempotent)

# COMMAND ----------

def write_silver(df, path, semestre, label):
    table_exists = DeltaTable.isDeltaTable(spark, path)
    write_op = (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("semestre")
    )
    if table_exists:
        write_op.option("replaceWhere", f"semestre = '{semestre}'").save(path)
        print(f"  {label}: updated semestre={semestre} at {path}")
    else:
        write_op.save(path)
        print(f"  {label}: created new table at {path}")

print("Writing Silver — clean records...")
write_silver(df_clean, SILVER_PATH, SEMESTRE, "sinistralidade")

if quarantine_count > 0:
    print("Writing Silver — quarantine...")
    write_silver(df_quarantine, QUARANTINE_PATH, SEMESTRE, "_quarantine")
else:
    print("No quarantine records — skipping quarantine write.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Validation Summary

# COMMAND ----------

df_silver_out = spark.read.format("delta").load(SILVER_PATH)

print("=" * 60)
print("SILVER LAYER — TRANSFORMATION SUMMARY")
print("=" * 60)
print(f"Semestre                : {SEMESTRE}")
print(f"Bronze premios  (input) : {df_prem.count():,}")
print(f"Bronze sinistros (input): {df_sin.count():,}")
print(f"After join              : {df_joined.count():,}")
print(f"Clean → Silver          : {clean_count:,}")
print(f"Quarantined             : {quarantine_count:,}")
print("=" * 60)

print("\nTop 10 regions by sinistralidade (highest risk):")
(
    df_silver_out
    .select("regiao", "reg_descricao_oficial", "tipo_prem",
            "premios", "indenizacoes", "sinistralidade", "sinistralidade_alert")
    .orderBy(col("sinistralidade").desc())
    .show(10, truncate=False)
)

print("\nDelta history — Silver (last 3):")
DeltaTable.forPath(spark, SILVER_PATH) \
    .history(3) \
    .select("version", "timestamp", "operation") \
    .show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Next Step
# MAGIC
# MAGIC Silver layer complete. Proceed to:
# MAGIC **`03_gold_aggregation.py`**
# MAGIC
# MAGIC Gold will produce:
# MAGIC - Sinistralidade by region (ranked — highest risk regions first)
# MAGIC - Sinistralidade by coverage type (tipo_prem)
# MAGIC - Exposure-weighted average sinistralidade per region
# MAGIC - Regions with sinistralidade_alert = True (loss ratio > 2.0)
# MAGIC
# MAGIC ---
# MAGIC *Pipeline: insurance-claims-pipeline | Layer: Silver | Author: Luciano Vilete*