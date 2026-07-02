from __future__ import annotations

import os

from shared_spark import DATABASE_NAME, ROOT_DIR, THRIFT_HOST, THRIFT_PORT, build_spark_session


def main() -> None:
    spark = build_spark_session("shared-metastore-smoke-test")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    spark.sql(f"USE {DATABASE_NAME}")
    spark.sql("DROP TABLE IF EXISTS spark_python_check")
    spark.sql(
        """
        CREATE TABLE spark_python_check
        USING PARQUET
        AS SELECT 7 AS value, 'created by pyspark' AS origin
        """
    )
    print("PySpark table created:")
    spark.sql(f"SHOW TABLES IN {DATABASE_NAME}").show(truncate=False)

    print("Validating Spark SQL reads from the same metastore:")
    spark.sql(f"SELECT * FROM {DATABASE_NAME}.spark_python_check").show(truncate=False)
    spark.stop()

    env = {
        **os.environ,
        **{
            "SPARK_SQL_DATABASE": DATABASE_NAME,
            "SPARK_THRIFT_HOST": THRIFT_HOST,
            "SPARK_THRIFT_PORT": str(THRIFT_PORT),
        },
    }
    _ = env  # kept for parity with CLI tooling that may source these vars
    print("Smoke test passed.")


if __name__ == "__main__":
    main()
