# Local Spark SQL + Hive Metastore + dbt setup

This workspace is configured so PySpark code and dbt models point to the same local Spark SQL warehouse and Hive metastore.

## Shared paths

- Warehouse: `/Users/jguerrrero/code/jguerrero_personal/.local/spark/warehouse`
- Hive metastore: `/Users/jguerrrero/code/jguerrero_personal/.local/spark/metastore_db`
- dbt profile: `/Users/jguerrrero/code/jguerrero_personal/.dbt/profiles.yml`
- dbt project: `/Users/jguerrrero/code/jguerrero_personal/dbt_spark`

## Commands

Bootstrap the metastore:

```bash
/Users/jguerrrero/code/jguerrero_personal/.venv/bin/python /Users/jguerrrero/code/jguerrero_personal/scripts/bootstrap_spark_metastore.py
```

Start the Spark Thrift server:

```bash
bash /Users/jguerrrero/code/jguerrero_personal/scripts/start_spark_thrift.sh
```

Run dbt against the same metastore:

```bash
export DBT_PROFILES_DIR=/Users/jguerrrero/code/jguerrero_personal/.dbt
export SPARK_SQL_DATABASE=analytics
/Users/jguerrrero/code/jguerrero_personal/.venv/bin/dbt debug --project-dir /Users/jguerrrero/code/jguerrero_personal/dbt_spark
/Users/jguerrrero/code/jguerrero_personal/.venv/bin/dbt run --project-dir /Users/jguerrrero/code/jguerrero_personal/dbt_spark
```

Stop the Spark Thrift server:

```bash
bash /Users/jguerrrero/code/jguerrero_personal/scripts/stop_spark_thrift.sh
```

## Notes

- The default Spark SQL database is `analytics`.
- If you want a different database, set `SPARK_SQL_DATABASE` before running the bootstrap script, the thrift server, and dbt.
- dbt uses the thrift connection method so it can connect to the same Spark catalog that your PySpark code uses.

## Reproducible Python environment (Pipenv)

Use Pipenv for deterministic installs across machines.

### First-time setup

```bash
cd /Users/jguerrrero/code/jguerrero_personal

# Create env with Python 3.10 if needed
pipenv --python 3.10

# Install exact dependencies from lock file
pipenv sync
```

### If dependencies change

```bash
cd /Users/jguerrrero/code/jguerrero_personal

# Add or update dependencies, then regenerate lock
pipenv install <package>
pipenv lock
```

Commit both `Pipfile` and `Pipfile.lock` to keep runs reproducible.

### Use Pipenv kernel in notebooks

```bash
cd /Users/jguerrrero/code/jguerrero_personal
pipenv run python -m ipykernel install --user --name jguerrero-personal --display-name "Python (pipenv jguerrero)"
```

Then select the `Python (pipenv jguerrero)` kernel in VS Code notebooks.