from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def is_open_ended_ts(col_name: str, open_end_ts: str) -> F.Column:
    """Build a timestamp predicate that matches the configured open-ended value."""
    return F.to_timestamp(F.col(col_name)) == F.to_timestamp(F.lit(open_end_ts))


def normalize_active_direct_bank(direct_df: DataFrame, open_end_ts: str) -> DataFrame:
    """Normalize direct_bank into historical graph edges plus seed metadata."""
    _ = open_end_ts
    filtered = (
        direct_df.select(
            F.col("rel_id").cast("string").alias("rel_id_s"),
            F.col("np_sbl_id").cast("string").alias("np_sbl_id_s"),
            F.col("drc_bnk_f").cast("string").alias("drc_bnk_f"),
            F.to_timestamp(F.col("edl_valid_to_dts")).alias("edl_valid_to_ts"),
            F.to_timestamp(F.col("edl_valid_from_dts")).alias("edl_valid_from_ts"),
        )
        .filter(
            F.col("rel_id_s").isNotNull()
            & F.col("np_sbl_id_s").isNotNull()
            & (F.trim(F.col("rel_id_s")) != "")
            & (F.trim(F.col("np_sbl_id_s")) != "")
        )
    )

    w = Window.partitionBy("rel_id_s", "np_sbl_id_s").orderBy(
        F.col("edl_valid_from_ts").desc_nulls_last(),
    )

    return (
        filtered.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            F.col("rel_id_s").alias("rel_id"),
            F.col("np_sbl_id_s").alias("id_key"),
            F.col("drc_bnk_f"),
            F.col("edl_valid_to_ts"),
            F.col("edl_valid_from_ts"),
        )
    )


def normalize_active_ggm_np(ggm_df: DataFrame, open_end_ts: str) -> DataFrame:
    """Normalize ggm_np as historical connectivity edges."""
    _ = open_end_ts
    return (
        ggm_df.select(
            F.col("rel_id").cast("string").alias("rel_id_s"),
            F.col("ikb_no").cast("string").alias("ikb_no_s"),
        )
        .filter(
            F.col("rel_id_s").isNotNull()
            & F.col("ikb_no_s").isNotNull()
            & (F.trim(F.col("rel_id_s")) != "")
            & (F.trim(F.col("ikb_no_s")) != "")
        )
        .dropDuplicates(["rel_id_s", "ikb_no_s"])
        .select(F.col("rel_id_s").alias("rel_id"), F.col("ikb_no_s").alias("id_key"))
    )
