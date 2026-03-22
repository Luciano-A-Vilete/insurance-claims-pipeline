# Architecture Decision Records

This document captures the key technical decisions made during the design
of the insurance-claims-pipeline, including the alternatives considered
and the reasoning behind each choice.

---

## ADR-001 — Medallion Architecture (Bronze → Silver → Gold)

**Decision:** Implement three distinct data layers instead of a single
transform-on-ingest pattern.

**Context:** The pipeline needs to handle SUSEP CSV files that may have
encoding issues, Brazilian decimal formats, and no date column. Downstream
consumers need both raw-fidelity access and analytics-ready tables.

**Alternatives considered:**
- Single-layer ETL (transform at ingest): simpler, but loses raw data
  if transformation logic changes
- Two layers (raw + serving): considered, but analytics aggregations
  benefit from a separate layer to avoid mixing business logic with cleaning

**Decision rationale:**
- Bronze preserves raw data for auditability — important in regulated industries
- Silver separates data cleaning from business logic
- Gold separates aggregation from row-level data — serving layer is fast and simple
- Each layer is independently re-runnable without re-ingesting from source

**Trade-off:** More storage, more complexity. Acceptable given the reliability
and auditability requirements of insurance data.

---

## ADR-002 — StringType for Numeric Fields at Bronze

**Decision:** Read all numeric fields as StringType at the Bronze layer
and cast to DoubleType/LongType at Silver.

**Context:** SUSEP AUTOSEG files use comma as decimal separator
(e.g. `369319,443`). PySpark's DoubleType casting treats comma-delimited
numbers as invalid and produces null silently — a data loss that is
extremely difficult to detect downstream.

**Alternatives considered:**
- `inferSchema=True`: would infer StringType anyway for comma decimals,
  but removes the explicit contract
- Custom CSV deserializer: over-engineering for this use case
- Pre-processing CSVs before ingestion: violates Bronze purity principle

**Decision rationale:**
Reading as String at Bronze makes the data exactly match the source file.
The explicit `regexp_replace(col, ",", ".")` at Silver makes the conversion
visible, testable, and immediately obvious to anyone reading the code.

**Trade-off:** Extra transformation step at Silver. Worth it for transparency
and correctness.

---

## ADR-003 — Semestre as Injected Partition Key

**Decision:** Inject `SEMESTRE` as an explicit configuration parameter
and use it as the Delta Lake partition key.

**Context:** SUSEP AUTOSEG releases one dataset per semester. The source
CSV files contain no date column — there is no way to infer the reference
period from the data itself.

**Alternatives considered:**
- Parse period from filename (e.g. `PremReg_2019S2.csv`): fragile, breaks
  if SUSEP changes naming convention
- Use `ingested_at` as proxy for period: incorrect — ingestion date ≠ data period
- Store all data in a single non-partitioned table: makes idempotent re-runs
  impossible without full table overwrite

**Decision rationale:**
Explicit is better than implicit. `SEMESTRE = "2019S2"` as a config
parameter is transparent, auditable, and easy to change when processing
a new release. Partition by semestre enables `replaceWhere` for idempotent
re-runs while preserving historical data.

---

## ADR-004 — Quarantine Pattern Over Hard Drops

**Decision:** Invalid records are written to a `_quarantine` Delta table
with a `failure_reason` column — never silently dropped.

**Context:** Data quality issues in SUSEP data are rare but possible
(unknown region codes, malformed decimals, zero-premium records). In
insurance data, dropping a record means losing a claim — a real financial
event.

**Alternatives considered:**
- Hard drop invalid records: simple, but makes data issues invisible
- Raise exception and halt pipeline: too aggressive for a small number
  of bad records in an otherwise healthy dataset
- Write errors to a log file: less queryable than a Delta table

**Decision rationale:**
The quarantine table makes failures visible, traceable, and recoverable.
A data engineer can query the quarantine table, understand the failure
reasons, and reprocess records after fixing the root cause. This pattern
is especially important in regulated industries where data completeness
is auditable.

**Trade-off:** Extra write operation per pipeline run. The quarantine table
is only written when there are actual failures — zero overhead on clean runs.

---

## ADR-005 — LEFT JOIN (Prêmios → Sinistros)

**Decision:** Use a LEFT JOIN when joining PremReg with SinReg on
`regiao` + `tipo`.

**Context:** Not every region/coverage-type combination that collected
premiums will have recorded claims in the same semester. A region with
zero claims is valid and meaningful data — it indicates low risk.

**Alternatives considered:**
- INNER JOIN: simpler, but silently drops low-risk regions with no claims
- FULL OUTER JOIN: unnecessary — SinReg records without matching PremReg
  rows represent an anomaly, not a valid business case

**Decision rationale:**
In actuarial analysis, a region with premiums but no claims is a valuable
signal. Dropping it via INNER JOIN would bias the sinistralidade ranking
by excluding the best-performing segments. LEFT JOIN with null-fill for
claim columns (0 sinistros, 0 indenizacoes) correctly represents this case.

---

## ADR-006 — Unity Catalog Volumes over Legacy DBFS

**Decision:** Use Unity Catalog Volumes (`/Volumes/...`) for all file
storage instead of legacy DBFS paths (`dbfs:/...`).

**Context:** Databricks Unity Catalog is the current standard for
governance-enabled data storage. DBFS is legacy and not recommended
for new projects.

**Decision rationale:**
- Fine-grained access control at the file level
- Data lineage tracking across the catalog
- Auditability of who accessed which file and when
- Required for compliance in regulated industries like insurance
- Future-proof: DBFS is being deprecated in new Databricks workspaces

**Trade-off:** Requires Unity Catalog setup, which is not available in
Databricks Community Edition's free tier. For local development, paths
can be swapped to local filesystem or DBFS in `config.py`.