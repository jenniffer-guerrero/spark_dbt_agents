from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession


ROOT_DIR = Path(__file__).resolve().parents[1]
LOCAL_DIR = ROOT_DIR / ".local" / "spark"
WAREHOUSE_DIR = LOCAL_DIR / "warehouse"
METASTORE_DIR = LOCAL_DIR / "metastore_db"
DATABASE_NAME = os.getenv("SPARK_SQL_DATABASE", "analytics")
THRIFT_HOST = os.getenv("SPARK_THRIFT_HOST", "127.0.0.1")
THRIFT_PORT = int(os.getenv("SPARK_THRIFT_PORT", "10001"))


def metastore_connection_url() -> str:
    return f"jdbc:derby:;databaseName={METASTORE_DIR};create=true"


def ensure_local_dirs() -> None:
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    METASTORE_DIR.parent.mkdir(parents=True, exist_ok=True)


def build_spark_session(app_name: str) -> SparkSession:
    ensure_local_dirs()

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastore_connection_url())
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        )
        .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
        .config("spark.hadoop.hive.metastore.schema.verification", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
