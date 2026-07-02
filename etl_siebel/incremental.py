from __future__ import annotations

"""Incremental ETL for REL_ID -> REL_ID_REGIE_KLANT mapping.

Design intent:
- Keep a snapshot of graph edges + seed membership from the previous run.
- Detect deltas (added/removed edges and seeds).
- Recompute only impacted subgraphs instead of rebuilding everything.
- Persist output and state snapshots by overwriting the same table objects in place.

The graph is modeled as undirected connectivity over (rel_id <-> id_key) edges.
`id_key` comes from:
- direct_bank.np_sbl_id
- ggm_np.ikb_no

Business rules implemented in this module:
- direct_bank edges: all historical rel_id<->np_sbl_id pairs are used for
    connectivity (after key cleanup and pair dedup).
- direct_bank seeds: only rows with drc_bnk_f='Y' and open-ended
    edl_valid_to_dts are active seed candidates.
- ggm_np: historical connectivity is used (no del_f / valid_to filtering).
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from .config import EtlConfig
from .utils import (
    anti_diff,
    assert_single_active_seed_per_rel_id,
    ensure_tables,
    expand_rel_component,
    expand_seed_pairs,
    has_rows,
    is_open_ended_ts,
    normalize_active_direct_bank,
    normalize_active_ggm_np,
    overwrite_table_in_place,
)


def run_incremental_update(
    spark: SparkSession,
    cfg: EtlConfig,
    show_samples: bool = False,
    checkpoint_dir: str | None = None,
) -> dict:
    """Execute one incremental ETL cycle and return run statistics.

    High-level flow:
    1) Read + normalize current source edges/seeds.
    2) Compare with previous snapshots to detect graph/seed deltas.
    3) Compute impacted rel_id set (including removed-seed side effects).
    4) Recompute only impacted output rows via seed reachability.
    5) Merge with unimpacted rows, keep only currently active seeds.
    6) Persist output + next snapshots + audit row.

    Returned stats keys:
    - changed_edges
    - added_seeds
    - removed_seeds
    - impacted_rel_ids
    - output_rows
    """
    opt = cfg.options
    if opt.seed_tie_breaker not in {"numeric_then_lex", "latest_then_numeric"}:
        raise ValueError("seed_tie_breaker must be 'numeric_then_lex' or 'latest_then_numeric'")

    checkpoint_every = opt.checkpoint_every
    # Optional checkpointing support for long iterative traversals.
    if checkpoint_every > 0:
        ckpt_dir = checkpoint_dir or str((Path.cwd() / ".local" / "spark" / "checkpoints" / "etl_siebel_update").resolve())
        Path(ckpt_dir).mkdir(parents=True, exist_ok=True)
        try:
            spark.sparkContext.setCheckpointDir(ckpt_dir)
        except Exception:
            checkpoint_every = 0

    ensure_tables(spark, cfg)

    # Extract current graph edges from both source tables.
    direct_raw = spark.table(f"{cfg.source_db}.{cfg.tables.direct_bank}")
    ggm_raw = spark.table(f"{cfg.source_db}.{cfg.tables.ggm_np}")
    direct = normalize_active_direct_bank(direct_raw, open_end_ts=opt.open_ended_ts)
    ggm = normalize_active_ggm_np(ggm_raw, open_end_ts=opt.open_ended_ts)
    assert_single_active_seed_per_rel_id(direct, opt.open_ended_ts)

    edges_cur = (
        direct.select(F.lit("direct_bank").alias("source_table"), "rel_id", "id_key")
        .unionByName(ggm.select(F.lit("ggm_np").alias("source_table"), "rel_id", "id_key"))
        .dropDuplicates(["source_table", "rel_id", "id_key"])
    )

    # Active seeds are a subset of normalized direct edges.
    seeds_cur = (
        direct.filter((F.col("drc_bnk_f") == "Y") & is_open_ended_ts("edl_valid_to_ts", opt.open_ended_ts))
        .select(F.col("rel_id").alias("seed_rel_id"), F.col("edl_valid_from_ts").alias("seed_valid_from_dts"))
        .dropDuplicates(["seed_rel_id", "seed_valid_from_dts"])
    )
    seeds_cur_snapshot = (
        seeds_cur.groupBy("seed_rel_id")
        .agg(F.max("seed_valid_from_dts").alias("seed_valid_from_dts"))
        .dropDuplicates(["seed_rel_id", "seed_valid_from_dts"])
    )

    edges_prev = spark.table(f"{cfg.state_db}.{cfg.tables.edge_snapshot}").dropDuplicates(["source_table", "rel_id", "id_key"])
    seeds_prev_raw = spark.table(f"{cfg.state_db}.{cfg.tables.seed_snapshot}")
    if "seed_valid_from_dts" in seeds_prev_raw.columns:
        seeds_prev = seeds_prev_raw.select(
            F.col("seed_rel_id").cast("string").alias("seed_rel_id"),
            F.to_timestamp(F.col("seed_valid_from_dts")).alias("seed_valid_from_dts"),
        ).dropDuplicates(["seed_rel_id", "seed_valid_from_dts"])
    else:
        # Backward compatibility with pre-migration snapshots that stored seed_rel_id only.
        seeds_prev = seeds_prev_raw.select(
            F.col("seed_rel_id").cast("string").alias("seed_rel_id"),
            F.lit(None).cast("timestamp").alias("seed_valid_from_dts"),
        ).dropDuplicates(["seed_rel_id", "seed_valid_from_dts"])
    membership_prev = spark.table(f"{cfg.state_db}.{cfg.tables.seed_membership}").dropDuplicates(["rel_id", "rel_id_regie_klant"])

    edge_cols = ["source_table", "rel_id", "id_key"]
    # Core delta detection for graph and seed snapshots.
    edge_added, edge_removed = anti_diff(edges_cur, edges_prev, edge_cols)
    edge_changed = edge_added.unionByName(edge_removed).dropDuplicates(edge_cols)

    seed_added, seed_removed = anti_diff(
        seeds_cur_snapshot,
        seeds_prev,
        ["seed_rel_id", "seed_valid_from_dts"],
    )
    seed_added_ids = seed_added.select("seed_rel_id").dropDuplicates(["seed_rel_id"])
    seed_removed_ids = seed_removed.select("seed_rel_id").dropDuplicates(["seed_rel_id"])

    affected_rel_from_removed = (
        membership_prev.join(seed_removed_ids, membership_prev.rel_id_regie_klant == seed_removed_ids.seed_rel_id, "inner")
        .select("rel_id")
        .dropDuplicates(["rel_id"])
    )

    # Start set for impacted traversal combines:
    # - rel_id touched by edge changes
    # - added/removed seeds
    # - rel_id that previously depended on removed seeds
    impact_start_rel = (
        edge_changed.select("rel_id")
        .unionByName(seed_added_ids.select(F.col("seed_rel_id").alias("rel_id")))
        .unionByName(seed_removed_ids.select(F.col("seed_rel_id").alias("rel_id")))
        .unionByName(affected_rel_from_removed.select("rel_id"))
        .dropDuplicates(["rel_id"])
    )

    edges_full = edges_cur.select("rel_id", "id_key").dropDuplicates(["rel_id", "id_key"])
    rel_truncated = False
    if not has_rows(edges_prev) and not has_rows(seeds_prev) and not has_rows(membership_prev):
        impacted_rel_cur = edges_full.select("rel_id").dropDuplicates(["rel_id"])
    elif not has_rows(impact_start_rel):
        impacted_rel_cur = spark.createDataFrame([], "rel_id string")
    else:
        impacted_rel_cur, rel_truncated = expand_rel_component(
            impact_start_rel,
            edges_full,
            max_iter=opt.max_iter,
            checkpoint_every=checkpoint_every,
        )

    # Include seeds connected to impacted rel_id in previous output, so removals
    # and remaps are handled correctly.
    impacted_seeds_prev = (
        membership_prev.join(impacted_rel_cur, "rel_id", "inner")
        .select(F.col("rel_id_regie_klant").alias("seed_rel_id"))
        .dropDuplicates(["seed_rel_id"])
    )

    impacted_seeds = (
        impacted_seeds_prev.unionByName(seed_added.select("seed_rel_id"))
        .unionByName(seed_removed_ids.select("seed_rel_id"))
        .dropDuplicates(["seed_rel_id"])
    )

    impacted_active_seeds = impacted_seeds.join(
        seeds_cur.select("seed_rel_id").dropDuplicates(["seed_rel_id"]), "seed_rel_id", "inner"
    )

    impacted_rel_prev = (
        membership_prev.join(seed_removed_ids.withColumnRenamed("seed_rel_id", "rel_id_regie_klant"), "rel_id_regie_klant", "inner")
        .select("rel_id")
        .dropDuplicates(["rel_id"])
    )

    impacted_rel_all = impacted_rel_cur.unionByName(impacted_rel_prev).dropDuplicates(["rel_id"])

    seed_truncated = False
    # Recompute only when there are active seeds to propagate.
    if not has_rows(impacted_active_seeds):
        recomputed = spark.createDataFrame([], "REL_ID string, REL_ID_REGIE_KLANT string")
    else:
        seed_pairs, seed_truncated = expand_seed_pairs(
            impacted_active_seeds,
            edges_full,
            max_iter=opt.max_iter,
            checkpoint_every=checkpoint_every,
        )
        seed_pairs = seed_pairs.join(impacted_rel_all, "rel_id", "inner")

        # Resolve potentially multiple reachable active seeds per rel_id
        # using the configured deterministic tie-break policy.
        if opt.seed_tie_breaker == "latest_then_numeric":
            seed_rank = (
                seeds_cur.groupBy("seed_rel_id")
                .agg(F.max("seed_valid_from_dts").alias("seed_valid_from_dts"))
                .withColumn("seed_num", F.col("seed_rel_id").cast("bigint"))
            )
            recomputed = (
                seed_pairs.join(seed_rank, "seed_rel_id", "left")
                .withColumn(
                    "_rn",
                    F.row_number().over(
                        Window.partitionBy("rel_id").orderBy(
                            F.col("seed_valid_from_dts").desc_nulls_last(),
                            F.col("seed_num").asc_nulls_last(),
                            F.col("seed_rel_id").asc(),
                        )
                    ),
                )
                .filter(F.col("_rn") == 1)
                .select(F.col("rel_id").alias("REL_ID"), F.col("seed_rel_id").alias("REL_ID_REGIE_KLANT"))
            )
        else:
            recomputed = (
                seed_pairs.withColumn("seed_num", F.col("seed_rel_id").cast("bigint"))
                .groupBy("rel_id")
                .agg(
                    F.min(F.when(F.col("seed_num").isNotNull(), F.col("seed_num"))).alias("min_seed_num"),
                    F.min("seed_rel_id").alias("min_seed_lex"),
                )
                .withColumn(
                    "REL_ID_REGIE_KLANT",
                    F.when(F.col("min_seed_num").isNotNull(), F.col("min_seed_num").cast("string")).otherwise(F.col("min_seed_lex")),
                )
                .select(F.col("rel_id").alias("REL_ID"), "REL_ID_REGIE_KLANT")
            )

        recomputed = recomputed.dropDuplicates(["REL_ID"])

    # Fail fast when traversal truncation could produce incomplete results.
    if opt.fail_on_incomplete and (rel_truncated or seed_truncated):
        raise RuntimeError(
            f"Graph expansion truncated (rel_truncated={rel_truncated}, seed_truncated={seed_truncated}) at max_iter={opt.max_iter}"
        )

    changed_edges_count = edge_changed.count()
    added_seeds_count = seed_added_ids.count()
    removed_seeds_count = seed_removed_ids.count()
    impacted_rel_count = impacted_rel_all.count()

    output_prev = spark.table(f"{cfg.target_db}.{cfg.tables.output}").select(
        F.col("REL_ID").cast("string").alias("REL_ID"),
        F.col("REL_ID_REGIE_KLANT").cast("string").alias("REL_ID_REGIE_KLANT"),
    )
    # Keep unimpacted output rows and replace only impacted subset.
    if not has_rows(impacted_rel_all):
        output_new = output_prev
    else:
        old_unimpacted = output_prev.join(
            impacted_rel_all.select(F.col("rel_id").alias("REL_ID")), "REL_ID", "left_anti"
        )
        output_new = old_unimpacted.unionByName(recomputed).dropDuplicates(["REL_ID"])

    # Final guard: mapping must reference currently active seeds only.
    output_new = output_new.join(
        seeds_cur.select(F.col("seed_rel_id").alias("REL_ID_REGIE_KLANT")).dropDuplicates(["REL_ID_REGIE_KLANT"]),
        "REL_ID_REGIE_KLANT",
        "inner",
    )

    overwrite_table_in_place(spark, f"{cfg.target_db}.{cfg.tables.output}", output_new)
    output_new_local = spark.table(f"{cfg.target_db}.{cfg.tables.output}").select(
        F.col("REL_ID").cast("string").alias("REL_ID"),
        F.col("REL_ID_REGIE_KLANT").cast("string").alias("REL_ID_REGIE_KLANT"),
    )

    overwrite_table_in_place(spark, f"{cfg.state_db}.{cfg.tables.edge_snapshot}", edges_cur)
    seed_snapshot_table = f"{cfg.state_db}.{cfg.tables.seed_snapshot}"
    seed_snapshot_df = seeds_cur_snapshot.select("seed_rel_id", "seed_valid_from_dts")
    try:
        overwrite_table_in_place(spark, seed_snapshot_table, seed_snapshot_df)
    except ValueError:
        # One-time migration path from old snapshot schema (seed_rel_id only).
        spark.sql(f"DROP TABLE IF EXISTS {seed_snapshot_table}")
        seed_snapshot_df.write.mode("overwrite").format("parquet").saveAsTable(seed_snapshot_table)
    overwrite_table_in_place(
        spark,
        f"{cfg.state_db}.{cfg.tables.seed_membership}",
        output_new_local.select(F.col("REL_ID").alias("rel_id"), F.col("REL_ID_REGIE_KLANT").alias("rel_id_regie_klant")),
    )

    # Persist audit row for observability and debugging.
    run_id = spark.sql("SELECT uuid() AS run_id").collect()[0]["run_id"]
    audit_row = spark.createDataFrame(
        [
            (
                run_id,
                changed_edges_count,
                added_seeds_count,
                removed_seeds_count,
                impacted_rel_count,
                output_new_local.count(),
                rel_truncated,
                seed_truncated,
            )
        ],
        [
            "run_id",
            "changed_edges",
            "added_seeds",
            "removed_seeds",
            "impacted_rel_ids",
            "output_rows",
            "rel_truncated",
            "seed_truncated",
        ],
    ).withColumn("run_ts", F.current_timestamp()).select(
        "run_id",
        "run_ts",
        "changed_edges",
        "added_seeds",
        "removed_seeds",
        "impacted_rel_ids",
        "output_rows",
        "rel_truncated",
        "seed_truncated",
    )
    run_audit_prev = spark.table(f"{cfg.state_db}.{cfg.tables.run_audit}")
    overwrite_table_in_place(spark, f"{cfg.state_db}.{cfg.tables.run_audit}", run_audit_prev.unionByName(audit_row))

    spark.catalog.clearCache()

    stats = {
        "changed_edges": changed_edges_count,
        "added_seeds": added_seeds_count,
        "removed_seeds": removed_seeds_count,
        "impacted_rel_ids": impacted_rel_count,
        "output_rows": output_new_local.count(),
    }

    if show_samples:
        print("Run stats:", stats)

    return stats
