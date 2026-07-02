from __future__ import annotations

"""Shared local Spark session utilities.

Why this module exists:
- Centralize Spark/Hive-metastore local settings used by notebooks, scripts,
  and tests.
- Keep warehouse/metastore paths consistent across all execution entry points.
"""

import os
import sys
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession


ROOT_DIR = Path(__file__).resolve().parents[1]
LOCAL_DIR = ROOT_DIR / ".local" / "spark"
WAREHOUSE_DIR = LOCAL_DIR / "warehouse"
METASTORE_DIR = LOCAL_DIR / "metastore_db"
DATABASE_NAME = os.getenv("SPARK_SQL_DATABASE", "analytics")
THRIFT_HOST = os.getenv("SPARK_THRIFT_HOST", "127.0.0.1")
THRIFT_PORT = int(os.getenv("SPARK_THRIFT_PORT", "10001"))


def _resolve_metastore_dir() -> Path:
    """Resolve metastore directory, optionally using isolated per-run storage.

    Resolution order:
    1) SPARK_SQL_METASTORE_DIR explicit path override.
    2) SPARK_SQL_USE_ISOLATED_METASTORE=1 -> unique directory under .local/spark.
    3) Default shared repository metastore directory.
    """
    explicit = os.getenv("SPARK_SQL_METASTORE_DIR")
    if explicit:
        return Path(explicit)

    if os.getenv("SPARK_SQL_USE_ISOLATED_METASTORE", "0") == "1":
        run_id = os.getenv("SPARK_SQL_METASTORE_RUN_ID", uuid4().hex[:12])
        return LOCAL_DIR / f"metastore_db_{run_id}"

    return METASTORE_DIR


def metastore_connection_url(metastore_dir: Path) -> str:
    """Return Derby JDBC URL used by local Hive metastore."""
    return f"jdbc:derby:;databaseName={metastore_dir};create=true"


def ensure_local_dirs(metastore_dir: Path) -> None:
    """Create required local directories for warehouse and metastore."""
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    metastore_dir.parent.mkdir(parents=True, exist_ok=True)


def build_spark_session(app_name: str) -> SparkSession:
    """Build SparkSession with Hive support and repository-local persistence.

    Important behavior:
    - Pins worker/driver Python to configured interpreter to avoid mismatch.
    - Uses local warehouse + embedded Derby metastore for reproducible runs.
    """
    metastore_dir = _resolve_metastore_dir()
    ensure_local_dirs(metastore_dir)
    pyspark_python = os.getenv("PYSPARK_PYTHON", sys.executable)
    pyspark_driver_python = os.getenv("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.pyspark.python", pyspark_python)
        .config("spark.pyspark.driver.python", pyspark_driver_python)
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastore_connection_url(metastore_dir))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        )
        .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
        .config("spark.hadoop.hive.metastore.schema.verification", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
