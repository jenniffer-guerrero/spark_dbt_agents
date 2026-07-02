from __future__ import annotations

"""Case-driven unit/integration tests for incremental siebel ETL.

Each test starts from the same deterministic baseline loaded from
`use_cases/siebel_id/test/*.csv`, applies one mutation, runs ETL, and validates:
1) Delta in (REL_ID, REL_ID_REGIE_KLANT) is exactly as expected.
2) Output contains no duplicate pairs.

Why this style:
- It mirrors the notebook validation flow while being CI-friendly and repeatable.
"""

from dataclasses import replace
import json
from pathlib import Path
from uuid import uuid4

import pytest
from pyspark.sql import functions as F

from etl_siebel.config import EtlConfig, load_config
from etl_siebel.incremental import run_incremental_update
from etl_siebel.utils import mutate_table_with_in_place_overwrite, overwrite_table_in_place
from scripts.shared_spark import build_spark_session


WORK_TABLE_NAMES = ["direct_bank", "ggm_np", "output"]


def _pair_set(df) -> set[tuple[str, str]]:
    """Convert output DataFrame into a comparable pair set for delta assertions."""
    return {(str(r["REL_ID"]), str(r["REL_ID_REGIE_KLANT"])) for r in df.select("REL_ID", "REL_ID_REGIE_KLANT").collect()}


def _assert_delta(case_name: str, prev_pairs: set, cur_pairs: set, expected_inserted: list, expected_removed: list) -> None:
    """Assert exact inserted/removed pair delta against the expected scenario output."""
    inserted = sorted(cur_pairs - prev_pairs)
    removed = sorted(prev_pairs - cur_pairs)
    assert inserted == sorted(expected_inserted), f"{case_name} inserted mismatch: {inserted}"
    assert removed == sorted(expected_removed), f"{case_name} removed mismatch: {removed}"


def _assert_output_has_no_duplicates(spark, cfg: EtlConfig) -> None:
    """Ensure output obeys uniqueness, N->1 cardinality, and active-seed eligibility."""
    out_df = spark.table(f"{cfg.target_db}.{cfg.tables.output}")
    assert out_df.count() == out_df.dropDuplicates(["REL_ID", "REL_ID_REGIE_KLANT"]).count()

    # N->1 rule: one REL_ID must not map to multiple active regie seeds.
    rel_multi_seed = (
        out_df.groupBy("REL_ID")
        .agg(F.countDistinct("REL_ID_REGIE_KLANT").alias("seed_count"))
        .filter(F.col("seed_count") > 1)
    )
    assert rel_multi_seed.count() == 0

    # Output regie seeds must be active in direct_bank (Y + open-ended valid_to).
    active_seeds = (
        spark.table(f"{cfg.source_db}.{cfg.tables.direct_bank}")
        .filter(
            (F.col("drc_bnk_f") == F.lit("Y"))
            & (F.to_timestamp(F.col("edl_valid_to_dts")) == F.to_timestamp(F.lit(cfg.options.open_ended_ts)))
        )
        .select(F.col("rel_id").cast("string").alias("REL_ID_REGIE_KLANT"))
        .dropDuplicates(["REL_ID_REGIE_KLANT"])
    )
    invalid_output = out_df.select("REL_ID_REGIE_KLANT").dropDuplicates(["REL_ID_REGIE_KLANT"]).join(
        active_seeds, "REL_ID_REGIE_KLANT", "left_anti"
    )
    assert invalid_output.count() == 0


def _assert_regie_seed_absent(spark, cfg: EtlConfig, seed_rel_id: str) -> None:
    """Ensure a rel_id never appears as REL_ID_REGIE_KLANT in output."""
    out_df = spark.table(f"{cfg.target_db}.{cfg.tables.output}")
    assert out_df.filter(F.col("REL_ID_REGIE_KLANT") == F.lit(seed_rel_id)).count() == 0


def _write_dataframe_to_table(spark, full_table_name: str, df) -> None:
    """Write or replace a table without swapping away the original object."""
    if spark.catalog.tableExists(full_table_name):
        overwrite_table_in_place(spark, full_table_name, df)
    else:
        df.write.mode("overwrite").format("parquet").saveAsTable(full_table_name)


def _reset_sequence_to_test_tables(spark, cfg: EtlConfig, test_input_dir: Path) -> None:
    """Reset source/target/state tables to test CSV baseline.

    Why:
    - Every case must be independent and reproducible.
    - State snapshots from previous runs must be removed before baseline replay.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.source_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.target_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.state_db}")

    for csv_name in WORK_TABLE_NAMES:
        csv_path = test_input_dir / f"{csv_name}.csv"
        df = (
            spark.read.option("header", True)
            .option("sep", "\t")
            .option("inferSchema", True)
            .csv(str(csv_path))
        )
        target_db = cfg.target_db if csv_name == "output" else cfg.source_db
        _write_dataframe_to_table(spark, f"{target_db}.{csv_name}", df)

    spark.sql(f"DROP TABLE IF EXISTS {cfg.state_db}.{cfg.tables.edge_snapshot}")
    spark.sql(f"DROP TABLE IF EXISTS {cfg.state_db}.{cfg.tables.seed_snapshot}")
    spark.sql(f"DROP TABLE IF EXISTS {cfg.state_db}.{cfg.tables.seed_membership}")
    spark.sql(f"DROP TABLE IF EXISTS {cfg.state_db}.{cfg.tables.run_audit}")
    spark.catalog.clearCache()


def _prepare_case_baseline(spark, cfg: EtlConfig, test_input_dir: Path) -> set[tuple[str, str]]:
    """Build baseline by reset + first ETL run, then return baseline output set."""
    _reset_sequence_to_test_tables(spark, cfg, test_input_dir)
    run_incremental_update(spark, cfg=cfg, show_samples=False)
    return _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))


def _update_column_with_fallback(spark, full_table_name: str, col_name: str, condition, new_value) -> None:
    """Apply UPDATE-like mutation via DataFrame overwrite fallback.

    Why:
    - Local Spark Parquet tables may not support direct SQL UPDATE semantics.
    """

    def _transform(df):
        return df.withColumn(col_name, F.when(condition, new_value).otherwise(F.col(col_name)))

    mutate_table_with_in_place_overwrite(spark, full_table_name, _transform)


@pytest.fixture(scope="session")
def spark():
    """Provide one SparkSession for the full test session.

    Checkpoint dir is set for iterative traversal stability/performance.
    """
    spark_session = build_spark_session("siebel-id-pytest")
    checkpoint_dir = (Path.cwd() / ".local" / "spark" / "checkpoints" / "siebel_id_pytest").resolve()
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    spark_session.sparkContext.setCheckpointDir(str(checkpoint_dir))
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="session")
def cfg(root_dir: Path) -> EtlConfig:
    """Load base JSON config and isolate DB names with a random suffix.

    Why:
    - Prevent collisions with manual notebook runs or previous test sessions.
    """
    base = load_config(root_dir / "config" / "siebel_id_etl.json")
    suffix = uuid4().hex[:8]
    return replace(
        base,
        source_db=f"{base.source_db}_{suffix}",
        target_db=f"{base.target_db}_{suffix}",
        state_db=f"{base.state_db}_{suffix}",
    )


@pytest.fixture(scope="session")
def test_input_dir(root_dir: Path) -> Path:
    """Location of tab-separated test baseline CSV files."""
    return root_dir / "use_cases" / "siebel_id" / "test"


@pytest.fixture(scope="session", autouse=True)
def teardown_databases(spark, cfg: EtlConfig):
    """Ensure ephemeral test databases are dropped after session completion."""
    yield
    spark.sql(f"DROP DATABASE IF EXISTS {cfg.state_db} CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS {cfg.source_db} CASCADE")
    if cfg.target_db != cfg.source_db:
        spark.sql(f"DROP DATABASE IF EXISTS {cfg.target_db} CASCADE")


def test_overwrite_table_in_place_reorders_and_casts_columns(spark, cfg: EtlConfig):
    """In-place overwrite should align reordered compatible columns to the table schema."""
    full_table_name = f"{cfg.source_db}.overwrite_cast_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.source_db}")

    base_df = spark.createDataFrame([(1, 10), (2, 20)], ["id", "score"])
    overwrite_table_in_place(spark, full_table_name, base_df)

    next_df = spark.createDataFrame([("30", "3"), ("40", "4")], ["score", "id"])
    overwrite_table_in_place(spark, full_table_name, next_df)

    rows = [tuple(row) for row in spark.table(full_table_name).orderBy("id").collect()]
    assert rows == [(3, 30), (4, 40)]


def test_overwrite_table_in_place_schema_mismatch_raises(spark, cfg: EtlConfig):
    """In-place overwrite must fail fast when columns do not match the existing table."""
    full_table_name = f"{cfg.source_db}.overwrite_mismatch_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.source_db}")

    base_df = spark.createDataFrame([(1, 10)], ["id", "score"])
    overwrite_table_in_place(spark, full_table_name, base_df)

    bad_df = spark.createDataFrame([(1, 10, 99)], ["id", "score", "extra_flag"])
    with pytest.raises(ValueError, match="Schema mismatch"):
        overwrite_table_in_place(spark, full_table_name, bad_df)


def test_mutate_table_with_in_place_overwrite_updates_without_staging_table(spark, cfg: EtlConfig):
    """Same-table mutation should succeed without leaving a staging table behind."""
    full_table_name = f"{cfg.source_db}.mutate_in_place_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.source_db}")

    base_df = spark.createDataFrame([(1,), (2,)], ["id"])
    overwrite_table_in_place(spark, full_table_name, base_df)

    mutate_table_with_in_place_overwrite(spark, full_table_name, lambda df: df.select((F.col("id") + 10).alias("id")))

    rows = [row[0] for row in spark.table(full_table_name).orderBy("id").collect()]
    assert rows == [11, 12]
    assert not spark.catalog.tableExists(f"{full_table_name}__staging")


def test_run_audit_grows_one_row_per_run(spark, cfg: EtlConfig, test_input_dir: Path):
    """Each ETL execution should append one run_audit row under in-place overwrite."""
    _prepare_case_baseline(spark, cfg, test_input_dir)
    audit_table = f"{cfg.state_db}.{cfg.tables.run_audit}"

    baseline_count = spark.table(audit_table).count()
    assert baseline_count == 1

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    assert spark.table(audit_table).count() == baseline_count + 1


def test_case_1_new_direct_bank_active_seed(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 1: insert new active direct_bank seed.

    Expected output delta:
    - inserted: ('130000001', '130000001')
    - removed: none

    Rationale:
    - New active seed should map to itself as regie seed.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000001, to_timestamp('2026-05-18 09:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-Z-NEW1', 'Y')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 1", prev_pairs, cur_pairs, [("130000001", "130000001")], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_2_flip_seed_off_with_fallback(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 2: deactivate one existing seed (Y -> N) using fallback mutation.

    Expected output delta:
    - inserted: none
    - removed: ('11410074','118492640'), ('117786942','118492640'), ('118492640','118492640')

    Rationale:
    - Rows depending on removed active seed must disappear from output.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    cond = (
        (F.col("rel_id") == F.lit(118492640))
        & (F.col("np_sbl_id") == F.lit("1-1TWAZ5GR"))
        & (F.col("drc_bnk_f") == F.lit("Y"))
        & (F.col("edl_valid_to_dts") == F.to_timestamp(F.lit(cfg.options.open_ended_ts)))
    )
    _update_column_with_fallback(spark, f"{cfg.source_db}.direct_bank", "drc_bnk_f", cond, F.lit("N"))

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta(
        "Case 2",
        prev_pairs,
        cur_pairs,
        [],
        [("11410074", "118492640"), ("117786942", "118492640"), ("118492640", "118492640")],
    )
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_3_ggm_rel_id_reassignment(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 3: reassign ggm_np relation from one rel_id to another.

    Expected output delta:
    - inserted: ('130000003','118220032')
    - removed: ('130000002','118220032')

    Rationale:
    - Connectivity should migrate to the new rel_id after reassignment.
    """
    _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000002, to_timestamp('2026-05-18 11:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-1N09820A', 'N')
        """
    )
    run_incremental_update(spark, cfg=cfg, show_samples=False)
    prev_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    cond = (
        (F.col("rel_id") == F.lit(130000002))
        & (F.col("ikb_no") == F.lit("1-1N09820A"))
        & (F.col("edl_valid_from_dts") == F.to_timestamp(F.lit("2026-05-18 11:00:00")))
    )
    _update_column_with_fallback(spark, f"{cfg.source_db}.ggm_np", "rel_id", cond, F.lit(130000003))

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 3", prev_pairs, cur_pairs, [("130000003", "118220032")], [("130000002", "118220032")])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_4_idempotent_no_change_run(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 4: run incremental update with no source changes.

    Expected output delta:
    - inserted: none
    - removed: none

    Rationale:
    - Incremental run must be idempotent in absence of deltas.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 4", prev_pairs, cur_pairs, [], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_5_new_ikb_same_rel(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 5: add new ikb_no for an existing rel_id.

    Expected output delta:
    - inserted: none
    - removed: none

    Rationale:
    - New historical connectivity should not create duplicate or changed output
      when chosen seed remains the same.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (118220032, to_timestamp('2026-05-18 12:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-ALT-IKB-118220032', 'N')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 5", prev_pairs, cur_pairs, [], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_6_duplicate_source_row(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 6: duplicate an existing source row in ggm_np.

    Expected output delta:
    - inserted: none
    - removed: none

    Rationale:
    - Source duplication should be neutralized by dedup logic.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np
        SELECT rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f
        FROM {cfg.source_db}.ggm_np
        WHERE rel_id = 118220032
          AND ikb_no = '1-1N09820A'
          AND edl_valid_to_dts = to_timestamp('{cfg.options.open_ended_ts}')
        LIMIT 1
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 6", prev_pairs, cur_pairs, [], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_7_new_rel_existing_ikb(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 7: add a new rel_id linked to an existing ikb_no.

    Expected output delta:
    - inserted: ('130000004','118220032')
    - removed: none

    Rationale:
    - New rel_id should inherit mapping through existing connectivity.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000004, to_timestamp('2026-05-18 12:30:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-1N09820A', 'N')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 7", prev_pairs, cur_pairs, [("130000004", "118220032")], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_8_simultaneous_new_relationships(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 8: insert new direct_bank seed + new ggm_np relation in same run.

    Expected output delta:
    - inserted: ('130000010','130000010'), ('130000011','130000010')
    - removed: none

    Rationale:
    - New ggm rel_id connected via shared id_key should inherit the new seed.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000010, to_timestamp('2026-05-18 13:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-CROSS-LINK-01', 'Y')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000011, to_timestamp('2026-05-18 13:05:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-CROSS-LINK-01', 'N')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 8", prev_pairs, cur_pairs, [("130000010", "130000010"), ("130000011", "130000010")], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_9_cross_table_new_relids_existing_seed_and_idempotent(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 9: cross-table new rel_ids tied to existing active seed + idempotency.

    Expected first-run delta:
    - inserted: ('130000012','118220032'), ('130000013','118220032')
    - removed: none

    Expected second-run delta (idempotency):
    - inserted: none
    - removed: none

    Rationale:
    - direct_bank historical edges are included in connectivity.
    - New relations connected to existing network should map to existing seed.
    - Immediate rerun must not change output.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000012, to_timestamp('2026-05-18 13:20:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-1N09820A', 'N')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000013, to_timestamp('2026-05-18 13:21:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-1N09820A', 'N')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 9", prev_pairs, cur_pairs, [("130000012", "118220032"), ("130000013", "118220032")], [])
    _assert_output_has_no_duplicates(spark, cfg)

    prev_pairs_2 = cur_pairs
    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs_2 = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 9 idempotency", prev_pairs_2, cur_pairs_2, [], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_10_historical_direct_bank_rel_included_in_output_rel_id(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 10: include historical direct_bank rel_id in output REL_ID.

    Expected output delta:
    - inserted: ('130000100','118220032')
    - removed: none

    Rationale:
    - Historical (closed) direct_bank rows must still contribute rel_id edges.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000100, to_timestamp('2026-06-10 09:00:00'), to_timestamp('2026-06-10 09:05:00'), '1-1N09820A', 'N')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 10", prev_pairs, cur_pairs, [("130000100", "118220032")], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_11_closed_y_row_not_eligible_as_regie_seed(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 11: closed Y direct_bank row cannot be REL_ID_REGIE_KLANT.

    Expected output delta:
    - inserted: ('110000000','118220032')
    - removed: none

    Rationale:
        - A row with drc_bnk_f='Y' but non-open-ended edl_valid_to_dts must not
            be selected as active regie seed.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (110000000, to_timestamp('2026-06-10 10:00:00'), to_timestamp('2026-06-10 10:05:00'), '1-1N09820A', 'Y')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 11", prev_pairs, cur_pairs, [("110000000", "118220032")], [])
    _assert_regie_seed_absent(spark, cfg, "110000000")
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_12_cross_source_bridge_same_rel_id_connects_both_id_keys(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 12: same rel_id in both sources bridges two id_key domains.

    Scenario:
    - direct_bank adds rel_id 130000200 linked to existing network key 1-1N09820A.
    - ggm_np adds the same rel_id 130000200 linked to a new key 1-BRIDGE-IKB-12.
    - ggm_np adds rel_id 130000201 on that new key.

    Expected output delta:
    - inserted: ('130000200','118220032'), ('130000201','118220032')
    - removed: none

    Rationale:
    - The shared rel_id (130000200) must connect both id_keys in one graph.
    - Connectivity should propagate seed reachability from 1-1N09820A to 1-BRIDGE-IKB-12.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000200, to_timestamp('2026-06-10 11:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-1N09820A', 'N')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000200, to_timestamp('2026-06-10 11:01:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-BRIDGE-IKB-12', 'N')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000201, to_timestamp('2026-06-10 11:02:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-BRIDGE-IKB-12', 'N')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 12", prev_pairs, cur_pairs, [("130000200", "118220032"), ("130000201", "118220032")], [])

    edge_keys = {
        r["id_key"]
        for r in spark.table(f"{cfg.state_db}.{cfg.tables.edge_snapshot}")
        .filter(F.col("rel_id") == F.lit("130000200"))
        .select("id_key")
        .collect()
    }
    assert edge_keys == {"1-1N09820A", "1-BRIDGE-IKB-12"}, f"Case 12 edge bridge mismatch: {sorted(edge_keys)}"
    _assert_output_has_no_duplicates(spark, cfg)


def test_case_13_multi_seed_conflict_resolves_latest_then_numeric(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 13: one rel_id reachable by two active seeds is resolved deterministically.

    Scenario:
    - Two active seeds are connected through the same ggm key.
    - A non-seed rel_id is reachable from both active seeds.

    Expectation under default tie-breaker (latest_then_numeric):
    - The seed with latest seed_valid_from_dts wins.
    """
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000900, to_timestamp('2026-06-12 09:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-MULTI-SEED-A', 'Y')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000901, to_timestamp('2026-06-12 10:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-MULTI-SEED-B', 'Y')
        """
    )

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000900, to_timestamp('2026-06-12 10:10:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-MULTI-CONNECT', 'N')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000901, to_timestamp('2026-06-12 10:11:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-MULTI-CONNECT', 'N')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
        VALUES (130000902, to_timestamp('2026-06-12 10:12:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-MULTI-CONNECT', 'N')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)

    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))
    _assert_delta(
        "Case 13",
        prev_pairs,
        cur_pairs,
        [("130000900", "130000901"), ("130000901", "130000901"), ("130000902", "130000901")],
        [],
    )

    winner_rows = (
        spark.table(f"{cfg.target_db}.{cfg.tables.output}")
        .filter(F.col("REL_ID") == F.lit("130000902"))
        .select(F.col("REL_ID_REGIE_KLANT").cast("string").alias("REL_ID_REGIE_KLANT"))
        .collect()
    )
    assert winner_rows and winner_rows[0]["REL_ID_REGIE_KLANT"] == "130000901"
    _assert_output_has_no_duplicates(spark, cfg)

    prev_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))
    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))
    _assert_delta("Case 13 idempotency", prev_pairs, cur_pairs, [], [])


def test_case_14_multi_seed_conflict_resolves_numeric_then_lex(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 14: numeric_then_lex chooses the smallest numeric seed when multiple are reachable."""
    cfg_numeric = replace(cfg, options=replace(cfg.options, seed_tie_breaker="numeric_then_lex"))
    prev_pairs = _prepare_case_baseline(spark, cfg_numeric, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg_numeric.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000910, to_timestamp('2026-06-12 09:00:00'), to_timestamp('{cfg_numeric.options.open_ended_ts}'), '1-MULTI-SEED-C', 'Y')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {cfg_numeric.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (130000911, to_timestamp('2026-06-12 10:00:00'), to_timestamp('{cfg_numeric.options.open_ended_ts}'), '1-MULTI-SEED-D', 'Y')
        """
    )

    for rel_id in (130000910, 130000911, 130000912):
        spark.sql(
            f"""
            INSERT INTO {cfg_numeric.source_db}.ggm_np (rel_id, edl_valid_from_dts, edl_valid_to_dts, ikb_no, del_f)
            VALUES ({rel_id}, to_timestamp('2026-06-12 10:30:00'), to_timestamp('{cfg_numeric.options.open_ended_ts}'), '1-MULTI-CONNECT-2', 'N')
            """
        )

    run_incremental_update(spark, cfg=cfg_numeric, show_samples=False)

    cur_pairs = _pair_set(spark.table(f"{cfg_numeric.target_db}.{cfg_numeric.tables.output}"))
    _assert_delta(
        "Case 14",
        prev_pairs,
        cur_pairs,
        [("130000910", "130000910"), ("130000911", "130000910"), ("130000912", "130000910")],
        [],
    )

    winner_rows = (
        spark.table(f"{cfg_numeric.target_db}.{cfg_numeric.tables.output}")
        .filter(F.col("REL_ID") == F.lit("130000912"))
        .select(F.col("REL_ID_REGIE_KLANT").cast("string").alias("REL_ID_REGIE_KLANT"))
        .collect()
    )
    assert winner_rows and winner_rows[0]["REL_ID_REGIE_KLANT"] == "130000910"
    _assert_output_has_no_duplicates(spark, cfg_numeric)

    prev_pairs = _pair_set(spark.table(f"{cfg_numeric.target_db}.{cfg_numeric.tables.output}"))
    run_incremental_update(spark, cfg=cfg_numeric, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg_numeric.target_db}.{cfg_numeric.tables.output}"))
    _assert_delta("Case 14 idempotency", prev_pairs, cur_pairs, [], [])


def test_case_15_multiple_active_seed_keys_same_rel_id_is_diagnostic_only(spark, cfg: EtlConfig, test_input_dir: Path):
    """Case 15: duplicate active seed keys on one rel_id logs diagnostics but does not change output."""
    prev_pairs = _prepare_case_baseline(spark, cfg, test_input_dir)

    spark.sql(
        f"""
        INSERT INTO {cfg.source_db}.direct_bank (rel_id, edl_valid_from_dts, edl_valid_to_dts, np_sbl_id, drc_bnk_f)
        VALUES (118220032, to_timestamp('2026-06-12 12:00:00'), to_timestamp('{cfg.options.open_ended_ts}'), '1-DUP-ACTIVE-15', 'Y')
        """
    )

    run_incremental_update(spark, cfg=cfg, show_samples=False)
    cur_pairs = _pair_set(spark.table(f"{cfg.target_db}.{cfg.tables.output}"))

    _assert_delta("Case 15", prev_pairs, cur_pairs, [], [])
    _assert_output_has_no_duplicates(spark, cfg)


def test_update_notebooks_active_seed_validation_sync(root_dir: Path):
    """Notebook update implementations must keep active-seed assertion usage synchronized."""

    def _notebook_source_text(notebook_path: Path) -> str:
        notebook_json = json.loads(notebook_path.read_text(encoding="utf-8"))
        source_chunks: list[str] = []
        for cell in notebook_json.get("cells", []):
            source = cell.get("source", [])
            if isinstance(source, list):
                source_chunks.append("".join(source))
        return "\n".join(source_chunks)

    local_update = root_dir / "notebooks" / "siebel_id_update.ipynb"
    databricks_update = root_dir / "notebooks" / "siebel_id_update_databricks.ipynb"

    local_source = _notebook_source_text(local_update)
    databricks_source = _notebook_source_text(databricks_update)

    expected_call = "assert_single_active_seed_per_rel_id(direct, open_ended_ts)"
    removed_helper = "def _assert_single_seed_candidate_per_rel_id"

    assert expected_call in local_source
    assert expected_call in databricks_source
    assert removed_helper not in local_source
    assert removed_helper not in databricks_source
