# 🏥 Insurance Claims Pipeline

> End-to-end PySpark + Databricks pipeline implementing Medallion Architecture
> for Brazilian auto insurance data — from raw SUSEP AUTOSEG ingestion to
> sinistralidade analytics by region.

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4+-orange.svg)](https://spark.apache.org)
[![Databricks](https://img.shields.io/badge/Platform-Databricks-red.svg)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-blue.svg)](https://delta.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📋 Overview

This project implements a production-grade data pipeline processing Brazilian
auto insurance data from [SUSEP AUTOSEG](https://dados.gov.br/dataset/dados-estatisticos-do-seguro-de-automoveis-autoseg)
— the open dataset published by Brazil's insurance regulatory authority.

The pipeline follows **Medallion Architecture** (Bronze → Silver → Gold),
transforming raw regulatory CSV files into sinistralidade analytics
(claims-to-premium ratio) by region, using PySpark on Databricks with
Delta Lake as the storage layer.

**Why this project?**
After working on a large-scale insurance data POC at IBM — validating a
Databricks + PySpark architecture processing billions of records for one of
Brazil's largest insurance groups — I built this pipeline to demonstrate
the full end-to-end pattern in a reproducible, documented way using real
public data from the same industry.

---

## 🏗️ Architecture

![Architecture](architecture/diagram.svg)

```
Source (SUSEP)     Bronze              Silver              Gold
──────────────     ──────────────      ──────────────      ──────────────
PremReg.csv    →   Raw Delta       →   Typed + Joined  →   Sinistralidade
SinReg.csv         Schema enforced     Validated           by region
auto_reg.csv       Partition by        Quarantine          Time series
(+ aux tables)     semestre            pattern             KPI tables
```

---

## 🛠️ Stack

| Layer | Technology | Why |
|---|---|---|
| **Processing** | PySpark 3.4+ | Distributed processing, scales to billions of records |
| **Platform** | Databricks | Unified analytics, native Delta Lake support |
| **Storage** | Delta Lake | ACID transactions, time travel, idempotent writes |
| **Source** | SUSEP AUTOSEG | Real Brazilian auto insurance regulatory data |
| **Language** | Python 3.10+ | Pipeline orchestration and transformation logic |

---

## 📁 Project Structure

```
insurance-claims-pipeline/
│
├── README.md
├── architecture/
│   └── diagram.svg                  ← Architecture diagram
│
├── notebooks/
│   ├── 01_bronze_ingestion.py       ← Raw ingestion to Delta Lake
│   ├── 02_silver_transformation.py  ← Typing, joining, quarantine
│   └── 03_gold_aggregation.py       ← Sinistralidade and KPIs
│
├── src/
│   ├── ingestion.py
│   ├── transformations.py
│   └── quality_checks.py
│
├── data/
│   └── sample/
│
└── docs/
    └── decisions.md
```

---

## 📊 Data Source — SUSEP AUTOSEG

**SUSEP AUTOSEG** is the official Brazilian auto insurance statistics system,
published by SUSEP (Superintendência de Seguros Privados).

**Main tables used:**

| File | Records | Description |
|---|---|---|
| `PremReg.csv` | ~205 | Premiums and vehicle exposure by region |
| `SinReg.csv` | ~200 | Claims and indemnizations by region |

**Auxiliary/dimension tables:**

| File | Description |
|---|---|
| `auto_reg.csv` | Region codes and descriptions (41 regions) |
| `auto_cob.csv` | Coverage types (Compreensiva, Incêndio e roubo, etc.) |
| `auto_cat.csv` | Vehicle categories (Passeio nacional, importado, etc.) |
| `auto2_vei.csv` | Vehicle models (~8,500 entries) |
| `auto2_grupo.csv` | Vehicle model groups |
| `auto_cau.csv` | Claim causes (Roubo, Colisão parcial, Perda total, etc.) |
| `auto_sexo.csv` | Driver gender (M, F, Jurídica) |
| `auto_idade.csv` | Driver age groups |

**Key fields in `PremReg.csv`:**
- `regiao` — region code (01–41, matches SUSEP regional breakdown)
- `descricao` — region name (e.g. "RS - Met. Porto Alegre e Caxias do Sul")
- `tipo_prem` — premium type (APP, CASCO, etc.)
- `exposicao` — vehicle-months exposed (proxy for number of insured vehicles)
- `premios` — total premiums collected in BRL
- `is_media` — average insured amount in BRL

**Key fields in `SinReg.csv`:**
- `regiao` — region code (joins with PremReg on `regiao` + `tipo`)
- `tipo_sin` — claim type (mirrors `tipo_prem` for join)
- `numSinistros` — number of claims occurred
- `indenizacoes` — total indemnizations paid in BRL

---

## 🚀 Pipeline Layers

### Bronze — Raw Ingestion

Ingests `PremReg.csv` and `SinReg.csv` into separate Delta tables with
no business transformation. Two key design decisions at this layer:

**1. Numeric fields as StringType:**
SUSEP files use comma as decimal separator (`369319,44`), which silently
corrupts data if cast to DoubleType at read time. Reading as String at
Bronze and applying `regexp_replace` at Silver is the safe pattern.

**2. Semestre as injected metadata:**
Source files have no date column. The reference period ("2019S2") is
injected at ingestion time and used as the partition key — enabling
idempotent re-runs via `replaceWhere`.

```python
# Idempotent write: overwrites only the current semestre partition
df.write.format("delta")
  .mode("overwrite")
  .option("replaceWhere", f"semestre = '{semestre}'")
  .partitionBy("semestre")
  .save(bronze_path)
```

### Silver — Transformation & Join

Type casting with Brazilian locale handling, join between Prêmios and
Sinistros on `regiao`, and quarantine of invalid records.

```python
# Brazilian decimal: comma → dot before casting
col_numeric = regexp_replace(col("premios"), ",", ".").cast(DoubleType())

# Join: one row per region + coverage type with both premium and claim data
df_silver = df_premios.join(df_sinistros, on=["regiao", "semestre"], how="left")
```

### Gold — Sinistralidade Analytics

The key metric: **sinistralidade** = total indemnizations / total premiums.
A ratio above 1.0 means the insurer paid out more than it collected — a
critical signal for pricing and risk teams.

```python
df_gold = df_silver.withColumn(
    "sinistralidade",
    col("indenizacoes") / col("premios")
)
```

---

## ⚙️ Key Technical Decisions

### 1. Two Delta tables at Bronze (not one wide table)

`PremReg` and `SinReg` have different grain structures and are joined
at Silver after proper type casting. Merging them at Bronze would mix
raw and transformed data, violating the Bronze principle of full fidelity.

### 2. Quarantine over hard drops

Invalid records at Silver are written to a `_quarantine` Delta table with
a `failure_reason` column — not silently dropped. In insurance data, a
dropped claim represents real money and a real event.

### 3. Semestre as injected partition key

Source files carry no date column. Rather than parsing dates from file
names (fragile), we inject `SEMESTRE` as an explicit config parameter.
This makes the partition scheme transparent, auditable, and testable.

### 4. StringType numerics at Bronze

Brazilian regulatory files use comma decimals. Casting at read time with
`inferSchema=True` or `DoubleType` produces silent nulls. Reading as
String and converting explicitly at Silver makes the transformation
visible, testable, and recoverable if the format changes.

---

## 🔧 How to Run

### Prerequisites

- Databricks account (Community Edition works — free)
- Runtime: DBR 13.x LTS (includes PySpark 3.4 + Delta Lake)

### Storage — Unity Catalog Volumes

This pipeline uses **Databricks Unity Catalog Volumes** for storage —
the modern, governance-ready alternative to legacy DBFS paths.

```
Catalog  : project_pipeline_databricks
Schema   : default
Volume   : pipeline_databricks_project

Source files:
  /Volumes/.../PremReg.csv       ← uploaded from SUSEP AUTOSEG
  /Volumes/.../SinReg.csv        ← uploaded from SUSEP AUTOSEG

Bronze output:
  /Volumes/.../bronze/premios/   ← Delta table, partitioned by semestre
  /Volumes/.../bronze/sinistros/ ← Delta table, partitioned by semestre
```

Unity Catalog Volumes provide fine-grained access control, data lineage,
and auditability — important properties in regulated industries like insurance.

### Steps

1. **Create a cluster** in Databricks (Runtime 13.x LTS)

2. **Upload source files** to your Unity Catalog Volume:
   - `Catalog → your volume → Upload`
   - Upload `PremReg.csv` and `SinReg.csv`

3. **Import notebooks** from `/notebooks/` into your Databricks workspace

4. **Set `SEMESTRE`** in notebook cell 1 to match your file's reference period

5. **Run in order:** `01` → `02` → `03`

### Download source data

```
https://dados.gov.br/dataset/dados-estatisticos-do-seguro-de-automoveis-autoseg
```

Select any available semester. The pipeline handles any AUTOSEG release
from 2019 onward.

---

## ✅ Data Quality Checks

Applied at Silver layer:

| Check | Column | Rule |
|---|---|---|
| Not null | `regiao` | Zero nulls allowed |
| Not null | `semestre` | Zero nulls allowed |
| Range | `premios` | Must be > 0 |
| Range | `indenizacoes` | Must be >= 0 |
| Range | `sinistralidade` | Alert if > 1.5 (potential data issue) |
| Referential | `regiao` | Must exist in `auto_reg` dimension table |

---

## 📈 Results

| Metric | Value |
|---|---|
| Source files processed | 2 (PremReg + SinReg) per semester |
| Total records per semester | ~405 (205 premium + 200 claims) |
| Gold table: sinistralidade by region | 41 regions × coverage types |
| Processing time (Databricks Community) | < 2 minutes end-to-end |

---

## 🗺️ Roadmap

- [ ] Add `auto_reg` dimension join at Silver for enriched region descriptions
- [ ] Extend to multi-semester: track sinistralidade trends over time
- [ ] Add Great Expectations for richer data quality profiling
- [ ] Build Streamlit dashboard on top of Gold layer
- [ ] Extend to multi-cloud: sync Gold layer to AWS S3

---

## 🤝 About

Built by [Luciano Vilete](https://linkedin.com/in/luciano-vilete) — Senior Data Engineer
with 6 years of experience building data pipelines across GCP, AWS, and Azure.

This project demonstrates production-grade data engineering patterns with
PySpark and Databricks, using real Brazilian insurance regulatory data.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

Data source: SUSEP AUTOSEG — Open Government Data (Dados Abertos).