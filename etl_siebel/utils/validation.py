from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from .normalization import is_open_ended_ts


def assert_single_active_seed_per_rel_id(direct_df: DataFrame, open_end_ts: str) -> None:
  """Log rel_id values with multiple active direct-bank seed keys (diagnostic only)."""
  violations = (
    direct_df.filter((F.col("drc_bnk_f") == "Y") & is_open_ended_ts("edl_valid_to_ts", open_end_ts))
    .groupBy("rel_id")
    .agg(F.countDistinct("id_key").alias("active_seed_key_count"))
    .filter(F.col("active_seed_key_count") > 1)
  )

  if violations.limit(1).count() > 0:
    sample = [
      (str(r["rel_id"]), int(r["active_seed_key_count"]))
      for r in violations.select("rel_id", "active_seed_key_count").orderBy("rel_id").limit(20).collect()
    ]
    print(
      "[siebel_id_update] assert_single_active_seed_per_rel_id: multiple active seed keys detected (diagnostic only). "
      f"Sample rel_id counts: {sample}"
    )


def ensure_tables(spark: SparkSession, cfg) -> None:
    """Create required state/output databases and tables if missing."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.state_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.target_db}")

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {cfg.state_db}.{cfg.tables.edge_snapshot} (
      source_table STRING,
      rel_id STRING,
      id_key STRING
    ) USING PARQUET
    """
    )

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {cfg.state_db}.{cfg.tables.seed_snapshot} (
            seed_rel_id STRING,
            seed_valid_from_dts TIMESTAMP
    ) USING PARQUET
    """
    )

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {cfg.state_db}.{cfg.tables.seed_membership} (
      rel_id STRING,
      rel_id_regie_klant STRING
    ) USING PARQUET
    """
    )

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {cfg.state_db}.{cfg.tables.run_audit} (
      run_id STRING,
      run_ts TIMESTAMP,
      changed_edges BIGINT,
      added_seeds BIGINT,
      removed_seeds BIGINT,
      impacted_rel_ids BIGINT,
      output_rows BIGINT,
      rel_truncated BOOLEAN,
      seed_truncated BOOLEAN
    ) USING PARQUET
    """
    )

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {cfg.target_db}.{cfg.tables.output} (
      REL_ID STRING,
      REL_ID_REGIE_KLANT STRING
    ) USING PARQUET
    """
    )
