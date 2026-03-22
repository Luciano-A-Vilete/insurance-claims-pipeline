"""
Transformation utilities — insurance-claims-pipeline

Type casting, joining, and enrichment functions for the Silver layer.
All functions are pure (no side effects) and testable in isolation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, regexp_replace, when, isnull,
    round as spark_round, current_timestamp,
)
from pyspark.sql.types import DoubleType, LongType


def br_decimal_to_double(df: DataFrame, column_name: str) -> DataFrame:
    """
    Convert a Brazilian decimal string column to DoubleType.

    Brazilian regulatory files (including SUSEP) use comma as the decimal
    separator (e.g. "369319,443"). Casting directly to DoubleType without
    replacing the comma produces nulls silently — a data quality failure
    that is hard to detect downstream.

    This function replaces comma with dot before casting, making the
    conversion explicit and safe.

    Args:
        df:          Input DataFrame
        column_name: Name of the column to convert (must be StringType)

    Returns:
        DataFrame with column replaced by DoubleType equivalent
    """
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), ",", ".").cast(DoubleType())
    )


def cast_numeric_columns(df: DataFrame, br_decimal_cols: list, long_cols: list) -> DataFrame:
    """
    Apply type casting to multiple columns at once.

    Args:
        df:               Input DataFrame
        br_decimal_cols:  Columns with Brazilian decimal format → DoubleType
        long_cols:        Columns with integer values → LongType

    Returns:
        DataFrame with all specified columns cast to their target types
    """
    for col_name in br_decimal_cols:
        df = br_decimal_to_double(df, col_name)
    for col_name in long_cols:
        df = df.withColumn(col_name, col(col_name).cast(LongType()))
    return df


def join_premios_sinistros(df_prem: DataFrame, df_sin: DataFrame) -> DataFrame:
    """
    Join Prêmios and Sinistros DataFrames on regiao + tipo.

    Uses LEFT join to preserve all premium rows — a region with premiums
    collected but zero claims is valid and important data (low-risk region).
    An INNER join would silently drop those rows.

    After joining, fills null claim values with 0 (left join nulls)
    and computes sinistralidade = indenizacoes / premios.

    Args:
        df_prem: Premios DataFrame (typed, from Bronze)
        df_sin:  Sinistros DataFrame (typed, from Bronze)

    Returns:
        Joined DataFrame with sinistralidade column
    """
    df_joined = (
        df_prem.join(
            df_sin,
            on=(
                (col("regiao") == col("sin_regiao")) &
                (col("tipo_prem") == col("tipo_sin"))
            ),
            how="left",
        )
        .withColumn(
            "num_sinistros",
            when(isnull(col("num_sinistros")), 0).otherwise(col("num_sinistros"))
        )
        .withColumn(
            "indenizacoes",
            when(isnull(col("indenizacoes")), 0.0).otherwise(col("indenizacoes"))
        )
        .withColumn(
            "sinistralidade",
            when(
                col("premios") > 0,
                spark_round(col("indenizacoes") / col("premios"), 4)
            ).otherwise(None)
        )
        .drop("sin_regiao", "tipo_sin", "sin_semestre", "sin_ingested_at")
    )
    return df_joined


def enrich_with_region(df: DataFrame, df_auto_reg: DataFrame) -> DataFrame:
    """
    Enrich the main DataFrame with official region descriptions from auto_reg.

    Joins on regiao → reg_codigo (left join to preserve all rows).
    Records where reg_descricao_oficial is null after this join have an
    unknown region code and will be quarantined in the validation step.

    Args:
        df:          Main DataFrame (joined premios + sinistros)
        df_auto_reg: Region dimension DataFrame (from auto_reg.csv)

    Returns:
        DataFrame enriched with reg_descricao_oficial column
    """
    return (
        df
        .join(df_auto_reg, on=(col("regiao") == col("reg_codigo")), how="left")
        .drop("reg_codigo")
        .withColumn("processed_at", current_timestamp())
    )


def compute_sinistralidade(
    total_indenizacoes: float,
    total_premios: float,
    decimals: int = 4,
) -> float:
    """
    Compute sinistralidade (loss ratio) safely.

    sinistralidade = total indemnizations / total premiums

    Interpretation:
    - < 0.70 → healthy portfolio
    - 0.70–1.0 → watch zone
    - > 1.0   → loss-making (paid more than collected)

    Args:
        total_indenizacoes: Sum of indemnizations paid
        total_premios:      Sum of premiums collected
        decimals:           Rounding precision (default: 4)

    Returns:
        Sinistralidade ratio, or None if premios == 0
    """
    if total_premios <= 0:
        return None
    return round(total_indenizacoes / total_premios, decimals)