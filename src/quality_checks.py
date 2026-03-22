"""
Data quality checks — insurance-claims-pipeline

Validation functions for the Silver layer.
Uses a quarantine pattern: invalid records are written to a separate
_quarantine Delta table with a failure_reason column — never silently dropped.

Why quarantine over hard drops:
In insurance data, a "dropped" claim record represents a real event
with real financial consequences. Dropping silently hides data quality
issues and makes them impossible to trace or recover from.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, isnull
from typing import Tuple


# ── Validation rules ──────────────────────────────────────────────────────────
# Each rule is a (condition, failure_reason) pair.
# condition:      PySpark column expression that evaluates to True when the
#                 record FAILS the check (not when it passes)
# failure_reason: Human-readable string written to the quarantine table

VALIDATION_RULES = [
    (
        isnull(col("regiao")),
        "null regiao",
    ),
    (
        isnull(col("premios")),
        "null premios after cast — likely malformed decimal in source",
    ),
    (
        col("premios") <= 0,
        "premios <= 0 — cannot compute sinistralidade on zero or negative premium",
    ),
    (
        col("indenizacoes") < 0,
        "indenizacoes < 0 — negative indemnization is a data error",
    ),
    (
        isnull(col("reg_descricao_oficial")),
        "unknown regiao — not present in auto_reg dimension table",
    ),
]

# ── Alert rules (flag but do not quarantine) ──────────────────────────────────
ALERT_RULES = [
    (
        col("sinistralidade") > 2.0,
        "sinistralidade_alert",
        "sinistralidade > 2.0 — unusually high, verify source data",
    ),
]


def apply_validation(df: DataFrame) -> DataFrame:
    """
    Apply all validation rules and tag each record with a failure_reason.

    Records that pass all checks get failure_reason = null.
    Records that fail any check get the first matching failure_reason.

    Args:
        df: Input DataFrame (enriched Silver records)

    Returns:
        DataFrame with failure_reason column added
    """
    failure_expr = None

    for condition, reason in VALIDATION_RULES:
        if failure_expr is None:
            failure_expr = when(condition, lit(reason))
        else:
            failure_expr = failure_expr.when(condition, lit(reason))

    failure_expr = failure_expr.otherwise(None)

    return df.withColumn("failure_reason", failure_expr)


def apply_alerts(df: DataFrame) -> DataFrame:
    """
    Apply alert rules and add boolean alert columns.

    Alert columns flag unusual values without quarantining the record.
    Useful for downstream monitoring and investigation.

    Args:
        df: Input DataFrame (after validation)

    Returns:
        DataFrame with one boolean column per alert rule
    """
    for condition, col_name, _ in ALERT_RULES:
        df = df.withColumn(
            col_name,
            when(condition, lit(True)).otherwise(lit(False))
        )
    return df


def split_clean_quarantine(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split a validated DataFrame into clean records and quarantine records.

    Args:
        df: DataFrame with failure_reason column (from apply_validation)

    Returns:
        Tuple of (df_clean, df_quarantine)
        - df_clean:      Records where failure_reason is null (passed all checks)
        - df_quarantine: Records where failure_reason is not null (failed at least one check)
    """
    df_clean      = df.filter(col("failure_reason").isNull()).drop("failure_reason")
    df_quarantine = df.filter(col("failure_reason").isNotNull())
    return df_clean, df_quarantine


def quality_report(df_clean: DataFrame, df_quarantine: DataFrame) -> None:
    """
    Print a data quality summary to the notebook output.

    Args:
        df_clean:      Clean records DataFrame
        df_quarantine: Quarantine records DataFrame
    """
    clean_count      = df_clean.count()
    quarantine_count = df_quarantine.count()
    total            = clean_count + quarantine_count
    pass_rate        = (clean_count / total * 100) if total > 0 else 0

    print("DATA QUALITY REPORT")
    print("-" * 40)
    print(f"Total records   : {total:,}")
    print(f"Clean           : {clean_count:,}")
    print(f"Quarantined     : {quarantine_count:,}")
    print(f"Pass rate       : {pass_rate:.1f}%")

    if quarantine_count > 0:
        print("\nQuarantine breakdown:")
        df_quarantine.groupBy("failure_reason").count().orderBy("count", ascending=False).show(truncate=False)
    else:
        print("\nNo quarantined records — all checks passed.")