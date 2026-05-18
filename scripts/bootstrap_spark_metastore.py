from __future__ import annotations

from shared_spark import DATABASE_NAME, build_spark_session


def main() -> None:
    spark = build_spark_session("bootstrap-spark-metastore")

    spark.sql("CREATE DATABASE IF NOT EXISTS default")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    spark.sql(f"USE {DATABASE_NAME}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS metastore_healthcheck (
            id INT,
            note STRING
        )
        USING PARQUET
        """
    )
    spark.sql(
        """
        INSERT INTO metastore_healthcheck
        SELECT 1 AS id, 'bootstrap complete' AS note
        WHERE NOT EXISTS (SELECT 1 FROM metastore_healthcheck WHERE id = 1)
        """
    )
    spark.sql(f"SHOW TABLES IN {DATABASE_NAME}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
