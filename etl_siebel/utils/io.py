from __future__ import annotations

from pathlib import Path
from typing import Callable
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, functions as F


def _create_table_with_retry(spark: SparkSession, full_table_name: str, df: DataFrame, fmt: str) -> None:
    """Create a managed table and clean up orphan local warehouse folders when needed."""
    try:
        df.write.mode("overwrite").format(fmt).saveAsTable(full_table_name)
    except Exception as exc:
        # Local Spark/Hive catalogs can leave orphan managed-table folders.
        if "LOCATION_ALREADY_EXISTS" not in str(exc):
            raise

        db_name, table_name = full_table_name.split(".", 1)
        warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "")
        if warehouse_dir:
            warehouse_path = Path(warehouse_dir.replace("file:", ""))
            orphan_location = warehouse_path / f"{db_name}.db" / table_name
            if orphan_location.exists():
                import shutil

                shutil.rmtree(orphan_location)
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        df.write.mode("overwrite").format(fmt).saveAsTable(full_table_name)


def overwrite_table_in_place(
    spark: SparkSession, full_table_name: str, df: DataFrame, fmt: str = "parquet"
) -> None:
    """Replace table contents without dropping the table object first."""
    if not spark.catalog.tableExists(full_table_name):
        _create_table_with_retry(spark, full_table_name, df, fmt)
        spark.catalog.refreshTable(full_table_name)
        return

    existing_schema = spark.table(full_table_name).schema
    existing_columns = [field.name for field in existing_schema]
    missing_columns = [column for column in existing_columns if column not in df.columns]
    extra_columns = [column for column in df.columns if column not in existing_columns]
    if missing_columns or extra_columns:
        raise ValueError(
            f"Schema mismatch for {full_table_name}: missing={missing_columns}, extra={extra_columns}"
        )

    temp_view_name = f"__overwrite_{uuid4().hex}"
    aligned_df = df.select(*(F.col(field.name).cast(field.dataType).alias(field.name) for field in existing_schema))
    aligned_df = aligned_df.localCheckpoint(eager=True)
    aligned_df.createOrReplaceTempView(temp_view_name)
    try:
        spark.sql(f"INSERT OVERWRITE TABLE {full_table_name} SELECT * FROM {temp_view_name}")
    finally:
        spark.catalog.dropTempView(temp_view_name)
    spark.catalog.refreshTable(full_table_name)


def mutate_table_with_in_place_overwrite(
    spark: SparkSession, full_table_name: str, transform_fn: Callable[[DataFrame], DataFrame]
) -> None:
    """Apply a transformation function and overwrite the same table object in place."""
    current_df = spark.table(full_table_name)
    next_df = transform_fn(current_df)
    overwrite_table_in_place(spark, full_table_name, next_df)


def overwrite_table_with_staging_swap(
    spark: SparkSession, full_table_name: str, df: DataFrame, fmt: str = "parquet"
) -> None:
    """Backward-compatible alias for in-place table overwrite."""
    overwrite_table_in_place(spark, full_table_name, df, fmt=fmt)


def mutate_table_with_overwrite_swap(
    spark: SparkSession, full_table_name: str, transform_fn: Callable[[DataFrame], DataFrame]
) -> None:
    """Backward-compatible alias for in-place overwrite mutation."""
    mutate_table_with_in_place_overwrite(spark, full_table_name, transform_fn)
