import json
from dataclasses import dataclass
from pathlib import Path

"""Configuration models for the incremental ETL job.

Why this module exists:
- Remove hardcoded database/table/runtime values from transformation code.
- Allow running the same ETL logic in notebooks, tests, and scripts by only
    switching JSON configuration.
"""


@dataclass(frozen=True)
class TableNames:
    """Logical table names used by the job.

    These names are combined with configured database names at runtime.
    """

    direct_bank: str
    ggm_np: str
    output: str
    edge_snapshot: str
    seed_snapshot: str
    seed_membership: str
    run_audit: str


@dataclass(frozen=True)
class RunOptions:
    """Runtime behaviors controlling traversal and safety checks.

    - max_iter/checkpoint_every: iterative graph traversal controls.
    - open_ended_ts: value used to identify active direct_bank records.
    - fail_on_incomplete: fail when traversal truncates.
        - seed_tie_breaker: strategy used to pick one seed per rel_id.
            Default is latest_then_numeric (latest seed_valid_from_dts first).
    """

    max_iter: int = 40
    checkpoint_every: int = 2
    open_ended_ts: str = "9999-12-31 00:00:00"
    fail_on_incomplete: bool = True
    seed_tie_breaker: str = "latest_then_numeric"


@dataclass(frozen=True)
class EtlConfig:
    """Complete ETL runtime configuration loaded from JSON."""

    source_db: str
    target_db: str
    state_db: str
    tables: TableNames
    options: RunOptions


def load_config(config_path: str | Path) -> EtlConfig:
    """Load JSON config file into strongly typed dataclasses.

    Why:
    - Centralized validation/parsing of config shape.
    - Callers get explicit attributes instead of untyped nested dict access.
    """
    path = Path(config_path)
    raw = json.loads(path.read_text(encoding="utf-8"))

    tables = TableNames(**raw["tables"])
    options = RunOptions(**raw.get("options", {}))

    return EtlConfig(
        source_db=raw["source_db"],
        target_db=raw["target_db"],
        state_db=raw["state_db"],
        tables=tables,
        options=options,
    )
