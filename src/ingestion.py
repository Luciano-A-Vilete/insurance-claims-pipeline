"""
Ingestion utilities — insurance-claims-pipeline

Reusable functions for reading SUSEP AUTOSEG CSV files into PySpark DataFrames
and writing them to Delta Lake with idempotent partition overwrite.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name
from pyspark.sql.types import StructType
from delta.tables import DeltaTable


def read_susep_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """
    Read a SUSEP AUTOSEG CSV file into a PySpark DataFrame.

    SUSEP files use:
    - Semicolon (;) as column delimiter
    - ISO-8859-1 encoding (required for Portuguese characters)
    - Comma (,) as decimal separator — numeric fields read as StringType

    Args:
        spark:  Active SparkSession
        path:   Full path to the CSV file (DBFS or Unity Catalog Volume)
        schema: Explicit StructType schema — never use inferSchema for SUSEP files

    Returns:
        DataFrame with raw data exactly as in the source file
    """
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


def add_bronze_metadata(df: DataFrame, semestre: str) -> DataFrame:
    """
    Add standard Bronze layer metadata columns to a DataFrame.

    Metadata columns added:
    - semestre:    Reference period (e.g. "2019S2") — used as partition key
    - ingested_at: Exact timestamp of this ingestion run
    - source_file: Source file path for each record (debugging)

    Args:
        df:       Input DataFrame (raw from source)
        semestre: Reference period string (e.g. "2019S2")

    Returns:
        DataFrame with three additional metadata columns
    """
    return (
        df
        .withColumn("semestre",    lit(semestre))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", input_file_name())
    )


def write_delta_idempotent(
    df: DataFrame,
    path: str,
    semestre: str,
    partition_col: str = "semestre",
) -> None:
    """
    Write a DataFrame to Delta Lake with idempotent partition overwrite.

    On first run: creates the Delta table.
    On subsequent runs: overwrites only the partition matching `semestre`,
    leaving all other partitions untouched.

    This pattern (replaceWhere) is safe for scheduled pipelines where
    multiple semesters accumulate over time.

    Args:
        df:            DataFrame to write
        path:          Delta table path
        semestre:      Current semestre value (used in replaceWhere predicate)
        partition_col: Column to partition by (default: "semestre")
    """
    spark = df.sparkSession
    table_exists = DeltaTable.isDeltaTable(spark, path)

    write_op = (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy(partition_col)
    )

    if table_exists:
        write_op.option(
            "replaceWhere", f"{partition_col} = '{semestre}'"
        ).save(path)
    else:
        write_op.save(path)


def validate_record_count(df: DataFrame, source_path: str, min_records: int = 1) -> None:
    """
    Raise ValueError if DataFrame has fewer records than expected.
    Prevents silent empty-read failures from propagating downstream.

    Args:
        df:          DataFrame to validate
        source_path: Source path (for error message)
        min_records: Minimum acceptable record count (default: 1)

    Raises:
        ValueError if record count < min_records
    """
    count = df.count()
    if count < min_records:
        raise ValueError(
            f"Expected at least {min_records} record(s) from {source_path}, "
            f"got {count}. Check that the file exists and is non-empty."
        )