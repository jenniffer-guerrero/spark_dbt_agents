from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def has_rows(df: DataFrame) -> bool:
    """Return True when DataFrame has at least one row."""
    return df.limit(1).count() > 0


def checkpoint(df: DataFrame, enabled: bool) -> DataFrame:
    """Checkpoint conditionally to truncate Spark lineage during traversals."""
    return df.checkpoint(eager=True) if enabled else df


def anti_diff(cur_df: DataFrame, prev_df: DataFrame, key_cols: list[str]) -> tuple[DataFrame, DataFrame]:
    """Return added and removed key rows between current and previous snapshots."""
    cur_keys = cur_df.select(*key_cols).dropDuplicates(key_cols)
    prev_keys = prev_df.select(*key_cols).dropDuplicates(key_cols)
    added = cur_keys.join(prev_keys, key_cols, "left_anti")
    removed = prev_keys.join(cur_keys, key_cols, "left_anti")
    return added, removed


def expand_rel_component(
    start_rel_df: DataFrame, edges_df: DataFrame, max_iter: int = 40, checkpoint_every: int = 2
) -> tuple[DataFrame, bool]:
    """Expand impacted REL_ID component via BFS-style traversal."""
    frontier = start_rel_df.select("rel_id").dropDuplicates(["rel_id"])
    frontier = checkpoint(frontier, checkpoint_every > 0)
    visited = frontier
    truncated = False

    for step in range(max_iter):
        via_id = frontier.join(edges_df, "rel_id", "inner").select("id_key").dropDuplicates(["id_key"])
        next_rel = via_id.join(edges_df, "id_key", "inner").select("rel_id").dropDuplicates(["rel_id"])

        new_rel = next_rel.join(visited, "rel_id", "left_anti")
        if not has_rows(new_rel):
            break

        visited = visited.unionByName(new_rel).dropDuplicates(["rel_id"])
        frontier = new_rel

        if step == max_iter - 1:
            truncated = True

        if checkpoint_every > 0 and (step + 1) % checkpoint_every == 0:
            visited = checkpoint(visited, True)
            frontier = checkpoint(frontier, True)

    return visited, truncated


def expand_seed_pairs(
    active_seeds_df: DataFrame, edges_df: DataFrame, max_iter: int = 40, checkpoint_every: int = 2
) -> tuple[DataFrame, bool]:
    """Expand reachability pairs (seed_rel_id, rel_id) for active seeds."""
    frontier = active_seeds_df.select(F.col("seed_rel_id"), F.col("seed_rel_id").alias("rel_id"))
    frontier = checkpoint(frontier, checkpoint_every > 0)
    visited = frontier
    truncated = False

    for step in range(max_iter):
        via_id = (
            frontier.alias("f")
            .join(edges_df.alias("e"), F.col("f.rel_id") == F.col("e.rel_id"), "inner")
            .select(F.col("f.seed_rel_id"), F.col("e.id_key"))
            .dropDuplicates(["seed_rel_id", "id_key"])
        )

        next_rel = (
            via_id.alias("i")
            .join(edges_df.alias("e"), "id_key", "inner")
            .select(F.col("i.seed_rel_id"), F.col("e.rel_id"))
            .dropDuplicates(["seed_rel_id", "rel_id"])
        )

        new_pairs = next_rel.join(visited, ["seed_rel_id", "rel_id"], "left_anti")
        if not has_rows(new_pairs):
            break

        visited = visited.unionByName(new_pairs).dropDuplicates(["seed_rel_id", "rel_id"])
        frontier = new_pairs

        if step == max_iter - 1:
            truncated = True

        if checkpoint_every > 0 and (step + 1) % checkpoint_every == 0:
            visited = checkpoint(visited, True)
            frontier = checkpoint(frontier, True)

    return visited, truncated
