# Siebel ID Incremental Mapping (Spark)

This repository is a case-study project for incrementally maintaining the mapping:

- REL_ID -> REL_ID_REGIE_KLANT

Core transformation logic is implemented in Python/PySpark (ETL module) and validated with pytest scenarios that reproduce stateful notebook cases (1..14).

## 1. Project goal

This repository implements an incremental flow that:

- Reads relationships from direct_bank and ggm_np.
- Builds and updates a connectivity graph using rel_id <-> id_key edges.
- Detects deltas against previous run state (edge_snapshot, seed_snapshot, seed_membership).
- Recomputes only impacted nodes.
- Persists output and state snapshots for the next run.

Persistence note:

- The ETL now overwrites the same output/state table objects in place instead of using a staging-table drop/rename swap.
- This avoids the temporary "table missing" window that can be problematic in governed Databricks environments such as Unity Catalog.

The final result is stored in output table columns:

- REL_ID
- REL_ID_REGIE_KLANT

Business rules enforced by the ETL:

- Historical direct_bank relationships are included in graph edges so historical rel_id values can be reported in REL_ID.
- Only direct_bank rows with drc_bnk_f='Y' and open-ended edl_valid_to_dts ('9999-12-31 00:00:00') are eligible to appear in REL_ID_REGIE_KLANT.
- The same diagnostic helper `assert_single_active_seed_per_rel_id(...)` is used in ETL Python and update notebooks to report rel_id values that have multiple active seed keys.

## 2. Repository structure

- etl_siebel/: modular ETL code (config.py, incremental.py).
- config/siebel_id_etl.json: runtime configuration for databases, table names, and options.
- tests/: Spark-based unit/integration tests (test_siebel_incremental.py).
- scripts/: operational scripts for environment setup, Spark shells, and Thrift server.
- notebooks/: exploratory and validation notebooks.
- use_cases/siebel_id/test/: baseline CSV data used by reproducible tests.
- docs/diagrams/: HTML diagrams.
- .local/spark/: local Spark state (warehouse, metastore, logs, checkpoints).

## 3. Requirements

- macOS or Linux with a compatible shell (bash or zsh).
- Python 3.10 recommended.
- Java in supported range 11..21.
- VS Code is optional but recommended for notebook workflows.

Java note:

- The project does not require a fixed Java 17 path anymore.
- scripts/spark_env.sh detects JAVA_HOME and enforces supported range 11..21.

### 3.1 Java installation (required)

Java is mandatory for Spark and must be installed at the system level.

- Java is NOT installed inside `.venv`.
- `.venv` only contains Python packages.
- Spark launches a JVM process, so Java must exist in your OS environment.

If Java is missing, install one supported version (11, 17, or 21).

macOS (Homebrew):

brew install openjdk@17

Alternative macOS options:

- `brew install openjdk@21`
- `brew install openjdk@11`

Linux (Ubuntu/Debian examples):

sudo apt-get update
sudo apt-get install -y openjdk-17-jdk

Version check:

java -version

Expected: major version must be between 11 and 21.

Optional manual override:

If you want to force a specific Java installation, export JAVA_HOME before loading the project environment:

export JAVA_HOME="/path/to/your/jdk"
source scripts/setup_env.sh

## 4. Setup from scratch

Run commands from the repository root.

### 4.1 Create virtual environment and install dependencies

Run:

bash scripts/setup_venv.sh

This script:

- Creates .venv.
- Installs Python dependencies (pyspark, pandas, pyarrow, ipykernel).
- Registers a Jupyter kernel for notebooks.
- Validates critical imports.

### 4.2 Activate project environment for each session

Run:

source scripts/setup_env.sh

This script:

- Loads Java/Spark environment via scripts/spark_env.sh.
- Activates .venv.
- Exports PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON, and SPARK_CONF_DIR.

## 5. Recommended daily workflow

### 5.1 Run tests

Full suite:

bash scripts/run_pytest.sh

Single case:

bash scripts/run_pytest.sh -q tests/test_siebel_incremental.py -k case_1

Notes:

- `source scripts/setup_env.sh` initializes `.venv`, `PYTHONPATH`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON`.
- `scripts/setup_venv.sh` also installs an activation hook so direct `source .venv/bin/activate` sets the same variables.

### 5.2 Open Spark shells

Interactive PySpark:

bash scripts/open_pyspark.sh

Spark SQL CLI:

bash scripts/open_spark_sql.sh

If Derby metastore is locked:

bash scripts/stop_spark_holders.sh --apply
bash scripts/open_spark_sql.sh

Temporary isolated metastore mode:

SPARK_SQL_USE_ISOLATED_METASTORE=1 bash scripts/open_spark_sql.sh

Derby format mismatch note:

- If you see `ERROR XSLAN` (incompatible metastore format), run with an isolated metastore directory.
- Pytest is configured to use an isolated metastore automatically.

### 5.3 Start and stop Spark Thrift Server (optional)

Start:

bash scripts/start_spark_thrift.sh

Stop:

bash scripts/stop_spark_thrift.sh

Optional variables:

- SPARK_THRIFT_HOST (default 127.0.0.1)
- SPARK_THRIFT_PORT (default 10001)

### 5.4 Shared metastore smoke test

source scripts/setup_env.sh
python scripts/smoke_test_shared_metastore.py

## 6. Programmatic ETL usage

There is no dedicated ETL CLI. Use the module directly from Python:

from pathlib import Path
from etl_siebel.config import load_config
from etl_siebel.incremental import run_incremental_update
from scripts.shared_spark import build_spark_session

spark = build_spark_session("siebel-id-run")
cfg = load_config(Path("config/siebel_id_etl.json"))
stats = run_incremental_update(spark, cfg=cfg, show_samples=False)
print(stats)

## 7. ETL configuration

File: config/siebel_id_etl.json

It defines:

- Databases: source_db, target_db, state_db.
- Table names for source, output, and state snapshots.
- Runtime options:
  - max_iter
  - checkpoint_every
  - open_ended_ts
  - fail_on_incomplete
  - seed_tie_breaker

Tie-breaker behavior:

- Default: latest_then_numeric.
- Used whenever one REL_ID is reachable by multiple active seeds at the same time.
- Winner priority order:
  1) Most recent seed_valid_from_dts
  2) Smallest numeric seed_rel_id (if comparable)
  3) Lexical order fallback

## 8. Notebooks

Main notebooks:

- notebooks/siebel_id_update.ipynb
- notebooks/siebel_id_test.ipynb
- notebooks/siebel_id_origin.ipynb

For consistent execution:

- Activate environment with source scripts/setup_env.sh.
- Select kernel Python (.venv) jguerrero in VS Code.
- Use CSV baseline data from use_cases/siebel_id/test/.

## 9. Local Spark state layout

Canonical Spark state paths:

- .local/spark/warehouse
- .local/spark/metastore_db
- .local/spark/logs
- .local/spark/checkpoints

Avoid creating parallel metastore or warehouse locations outside .local/spark.

## 10. Script catalog

### scripts/setup_venv.sh

- Reproducible Python environment bootstrap.
- Installs dependencies and registers notebook kernel.

### scripts/setup_env.sh

- Activates .venv and exports Spark/Java session variables.

### scripts/spark_env.sh

- Detects JAVA_HOME automatically.
- Validates supported Java range 11..21.
- Exports SPARK_CONF_DIR.

### scripts/open_pyspark.sh

- Opens PySpark shell in project context.

### scripts/open_spark_sql.sh

- Opens spark-sql with local metastore.
- Detects Derby lock conflicts and prints recovery guidance.
- Supports isolated mode via SPARK_SQL_USE_ISOLATED_METASTORE=1.

### scripts/stop_spark_holders.sh

- Shows processes holding metastore lock files.
- With --apply, terminates those processes.

### scripts/start_spark_thrift.sh

- Starts Spark Thrift Server with local warehouse/metastore settings.

### scripts/stop_spark_thrift.sh

- Stops Spark Thrift Server.

### scripts/bootstrap_spark_metastore.py

- Initializes and validates local metastore prerequisites.

### scripts/shared_spark.py

- Shared SparkSession builder with consistent local configuration.

### scripts/smoke_test_shared_metastore.py

- Quick write/read verification against local Spark metastore.

## 11. Troubleshooting

### Python version mismatch in Spark workers

Symptom:

- PYTHON_VERSION_MISMATCH

Fix:

export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

### Derby lock error in spark-sql

Symptom:

- ERROR XSDB6: Another instance of Derby may have already booted the database

Fix:

bash scripts/stop_spark_holders.sh --apply
bash scripts/open_spark_sql.sh

### Unsupported JAVA_HOME

Symptom:

- scripts/spark_env.sh reports unsupported Java version.

Fix:

- Install Java in supported range 11..21.
- Export a valid JAVA_HOME before running source scripts/setup_env.sh.

## 12. Quick reference commands

Initial setup:

bash scripts/setup_venv.sh
source scripts/setup_env.sh

Tests:

pytest -q tests/test_siebel_incremental.py

Spark shells:

bash scripts/open_pyspark.sh
bash scripts/open_spark_sql.sh

Thrift server:

bash scripts/start_spark_thrift.sh
bash scripts/stop_spark_thrift.sh

If needed, this repository can add a Makefile with common targets such as setup, test, spark-sql, thrift-start, and thrift-stop.
