# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Sinistralidade Analytics
# MAGIC
# MAGIC **Purpose:** Produce business-ready aggregated tables from Silver data.
# MAGIC
# MAGIC **What this notebook produces:**
# MAGIC 1. `gold_by_region`    — sinistralidade ranking per region (all coverage types combined)
# MAGIC 2. `gold_by_tipo`      — sinistralidade breakdown per coverage type (tipo_prem)
# MAGIC 3. `gold_alerts`       — regions where sinistralidade > 1.0 (loss-making segments)
# MAGIC 4. `gold_summary`      — single-row portfolio summary for the whole semestre
# MAGIC
# MAGIC **Key metric — sinistralidade (loss ratio):**
# MAGIC - < 0.7  → healthy (insurer retains 30%+ after claims)
# MAGIC - 0.7–1.0 → watch zone
# MAGIC - > 1.0  → loss-making (insurer paid more than it collected)
# MAGIC - > 1.27 → highest observed (DF - Brasília, RCDM coverage, 2019S2)
# MAGIC
# MAGIC **Input:**  `/Volumes/.../silver/sinistralidade/`
# MAGIC **Output:** `/Volumes/.../gold/` (4 Delta tables)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, sum as spark_sum, avg, count, max as spark_max,
    min as spark_min, round as spark_round, when, current_timestamp,
)
from delta.tables import DeltaTable

SEMESTRE = "YOUR_SEMESTRE"   # e.g. "2019S2"

# ── Unity Catalog coordinates ─────────────────────────────────────────────────
CATALOG = "YOUR_CATALOG"     # e.g. "project_pipeline_databricks"
SCHEMA  = "YOUR_SCHEMA"      # e.g. "default"
VOLUME  = "YOUR_VOLUME"      # e.g. "pipeline_databricks_project"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

SILVER_PATH      = f"{BASE}/silver/sinistralidade/"
GOLD_BY_REGION   = f"{BASE}/gold/by_region/"
GOLD_BY_TIPO     = f"{BASE}/gold/by_tipo/"
GOLD_ALERTS      = f"{BASE}/gold/alerts/"
GOLD_SUMMARY     = f"{BASE}/gold/summary/"

print(f"Semestre : {SEMESTRE}")
print(f"Silver   : {SILVER_PATH}")
print(f"Gold     : {BASE}/gold/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read Silver

# COMMAND ----------

df_silver = (
    spark.read.format("delta").load(SILVER_PATH)
    .filter(col("semestre") == SEMESTRE)
)

record_count = df_silver.count()
print(f"Silver records loaded ({SEMESTRE}): {record_count:,}")
print(f"Coverage types: {[r[0] for r in df_silver.select('tipo_prem').distinct().collect()]}")
print(f"Regions: {df_silver.select('regiao').distinct().count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Gold Table 1 — Sinistralidade by Region
# MAGIC
# MAGIC Aggregates all coverage types per region.
# MAGIC Produces a clean ranking: which regions are highest risk?
# MAGIC
# MAGIC Exposure-weighted sinistralidade is the correct metric here —
# MAGIC a region with 1,000 exposed vehicles at sinistralidade 1.5
# MAGIC matters more than a region with 10 vehicles at sinistralidade 2.0.

# COMMAND ----------

df_by_region = (
    df_silver
    .groupBy("regiao", "reg_descricao_oficial", "semestre")
    .agg(
        spark_round(spark_sum("premios"),       2).alias("total_premios"),
        spark_round(spark_sum("indenizacoes"),  2).alias("total_indenizacoes"),
        spark_round(spark_sum("exposicao"),     2).alias("total_exposicao"),
        spark_round(spark_sum("indenizacoes") / spark_sum("premios"), 4)
                                                  .alias("sinistralidade"),
        count("tipo_prem")                        .alias("num_coberturas"),
    )
    .withColumn(
        "risk_tier",
        when(col("sinistralidade") > 1.0,  lit("HIGH"))
        .when(col("sinistralidade") > 0.7, lit("WATCH"))
        .otherwise(                         lit("HEALTHY"))
    )
    .withColumn("processed_at", current_timestamp())
    .orderBy(col("sinistralidade").desc())
)

print(f"Regions in Gold by_region: {df_by_region.count()}")
print("\nTop 10 highest-risk regions:")
df_by_region.select(
    "regiao", "reg_descricao_oficial",
    "total_premios", "total_indenizacoes",
    "sinistralidade", "risk_tier"
).show(10, truncate=False)

print("\nRisk tier distribution:")
df_by_region.groupBy("risk_tier").count().orderBy("risk_tier").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Gold Table 2 — Sinistralidade by Coverage Type (tipo_prem)
# MAGIC
# MAGIC Which coverage types are most loss-prone across all regions?
# MAGIC RCDM (Responsabilidade Civil Danos Materiais) vs APP vs CASCO, etc.

# COMMAND ----------

df_by_tipo = (
    df_silver
    .groupBy("tipo_prem", "semestre")
    .agg(
        spark_round(spark_sum("premios"),      2).alias("total_premios"),
        spark_round(spark_sum("indenizacoes"), 2).alias("total_indenizacoes"),
        spark_round(spark_sum("exposicao"),    2).alias("total_exposicao"),
        spark_round(spark_sum("indenizacoes") / spark_sum("premios"), 4)
                                                 .alias("sinistralidade"),
        count("regiao")                          .alias("num_regioes"),
        spark_round(avg("is_media"),           2).alias("avg_is_media"),
    )
    .withColumn("processed_at", current_timestamp())
    .orderBy(col("sinistralidade").desc())
)

print("Sinistralidade by coverage type:")
df_by_tipo.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Gold Table 3 — Alerts (Loss-Making Segments)
# MAGIC
# MAGIC Regions + coverage types where sinistralidade > 1.0.
# MAGIC These are segments where the insurer is currently losing money.
# MAGIC In a real pricing team, this table would trigger a tariff review.

# COMMAND ----------

df_alerts = (
    df_silver
    .filter(col("sinistralidade") > 1.0)
    .select(
        "regiao",
        "reg_descricao_oficial",
        "tipo_prem",
        "premios",
        "indenizacoes",
        "sinistralidade",
        "exposicao",
        "semestre",
    )
    .withColumn(
        "loss_amount",
        spark_round(col("indenizacoes") - col("premios"), 2)
    )
    .withColumn("processed_at", current_timestamp())
    .orderBy(col("sinistralidade").desc())
)

alert_count = df_alerts.count()
print(f"Loss-making segments (sinistralidade > 1.0): {alert_count}")
print("\nAll alerts:")
df_alerts.select(
    "regiao", "reg_descricao_oficial", "tipo_prem",
    "sinistralidade", "loss_amount"
).show(20, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Gold Table 4 — Portfolio Summary
# MAGIC
# MAGIC Single-row summary of the entire portfolio for this semestre.
# MAGIC The kind of number a CFO or Chief Actuary looks at first.

# COMMAND ----------

df_summary = (
    df_silver
    .groupBy("semestre")
    .agg(
        spark_round(spark_sum("premios"),      2).alias("total_premios"),
        spark_round(spark_sum("indenizacoes"), 2).alias("total_indenizacoes"),
        spark_round(spark_sum("exposicao"),    2).alias("total_exposicao"),
        spark_round(
            spark_sum("indenizacoes") / spark_sum("premios"), 4
        ).alias("sinistralidade_portfolio"),
        count("regiao")                          .alias("total_segments"),
        spark_sum(
            when(col("sinistralidade") > 1.0, 1).otherwise(0)
        ).alias("loss_making_segments"),
        spark_round(spark_max("sinistralidade"), 4).alias("max_sinistralidade"),
        spark_round(spark_min("sinistralidade"), 4).alias("min_sinistralidade"),
    )
    .withColumn("processed_at", current_timestamp())
)

print("Portfolio summary:")
df_summary.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Write Gold Delta Tables (Idempotent)

# COMMAND ----------

def write_gold(df, path, semestre, label):
    table_exists = DeltaTable.isDeltaTable(spark, path)
    write_op = (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("semestre")
    )
    if table_exists:
        write_op.option("replaceWhere", f"semestre = '{semestre}'").save(path)
        print(f"  {label}: updated semestre={semestre}")
    else:
        write_op.save(path)
        print(f"  {label}: created")

write_gold(df_by_region, GOLD_BY_REGION, SEMESTRE, "gold/by_region")
write_gold(df_by_tipo,   GOLD_BY_TIPO,   SEMESTRE, "gold/by_tipo")
write_gold(df_alerts,    GOLD_ALERTS,    SEMESTRE, "gold/alerts")
write_gold(df_summary,   GOLD_SUMMARY,   SEMESTRE, "gold/summary")

print("\nAll Gold tables written.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Final Summary

# COMMAND ----------

print("=" * 65)
print("GOLD LAYER — PIPELINE COMPLETE")
print("=" * 65)
print(f"Semestre             : {SEMESTRE}")
print(f"Silver records used  : {record_count:,}")
print()

summary_row = df_summary.collect()[0]
print(f"Portfolio sinistralidade : {summary_row['sinistralidade_portfolio']:.4f}")
print(f"Total premios (BRL)      : R$ {summary_row['total_premios']:,.2f}")
print(f"Total indenizacoes (BRL) : R$ {summary_row['total_indenizacoes']:,.2f}")
print(f"Total exposicao          : {summary_row['total_exposicao']:,.0f} vehicle-months")
print(f"Total segments analyzed  : {summary_row['total_segments']}")
print(f"Loss-making segments     : {summary_row['loss_making_segments']}")
print(f"Highest sinistralidade   : {summary_row['max_sinistralidade']:.4f} (DF - Brasilia, RCDM)")
print(f"Lowest sinistralidade    : {summary_row['min_sinistralidade']:.4f}")
print()
print("Gold tables written:")
print(f"  {GOLD_BY_REGION}  ← sinistralidade ranking by region")
print(f"  {GOLD_BY_TIPO}    ← sinistralidade by coverage type")
print(f"  {GOLD_ALERTS}     ← loss-making segments (> 1.0)")
print(f"  {GOLD_SUMMARY}    ← portfolio summary")
print("=" * 65)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Complete ✓
# MAGIC
# MAGIC ```
# MAGIC PremReg.csv + SinReg.csv
# MAGIC       ↓
# MAGIC    Bronze  (raw Delta, partitioned by semestre)
# MAGIC       ↓
# MAGIC    Silver  (typed, joined, validated, quarantine)
# MAGIC       ↓
# MAGIC    Gold    (by_region, by_tipo, alerts, summary)
# MAGIC ```
# MAGIC
# MAGIC **Key insight from 2019S2 data:**
# MAGIC DF - Brasília shows the highest sinistralidade (1.27) for RCDM coverage,
# MAGIC meaning the market paid out R$ 1.27 for every R$ 1.00 collected in that
# MAGIC segment — a loss-making position that would typically trigger a tariff review.
# MAGIC
# MAGIC ---
# MAGIC *Pipeline: insurance-claims-pipeline | Layer: Gold | Author: Luciano Vilete*