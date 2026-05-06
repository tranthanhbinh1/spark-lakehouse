# AGENTS.md

## Repo Shape
- Python project pinned to `requires-python ==3.12`; dependencies live in `pyproject.toml` and `uv.lock`, so prefer `uv sync` and keep both files in sync when dependencies change.
- Production ETL code is under `src/etl/`: `jobs/` contains PySpark jobs and `sql/` contains SparkSQL/Iceberg DDL.
- Airflow DAGs live in `orchestration/dags/`; they submit Spark jobs from `/opt/spark/jobs`, which is expected to be a container mount of `src/etl/jobs`.
- `scripts/bootstrap/initial_load.py` downloads NYC TLC parquet files into local `data/{year}/`; `data/` is ignored.
- `notebooks/` are exploratory only; move production logic into `src/`.

## Commands
- Install/update env: `uv sync`.
- Lint: `uv run ruff check .`.
- Format: `uv run ruff format .`.
- No test suite or pytest config exists yet; use lint plus focused Spark dry runs when changing jobs.
- Unified staging job dry run: `uv run python src/etl/jobs/nyc_tlc_stg_trip_data.py --dataset yellow --year 2011 --month 1 --dry-run`.
- Unified staging job write path: `uv run python src/etl/jobs/nyc_tlc_stg_trip_data.py --dataset green --year 2014 --month 1 --input-base s3a://raw/data`.
- Bootstrap currently downloads all four TLC datasets for hardcoded `year_to_download = 2025`; edit or parameterize before running if you need another year.

## Spark And Iceberg
- `src/etl/jobs/nyc_tlc_stg_trip_data.py` is the current unified monthly job for `--dataset yellow|green`; older dataset-specific jobs still exist but Airflow points at the unified job.
- Input path convention is `s3a://raw/data/{year}/{dataset}_tripdata_{year}-{month:02d}.parquet`.
- Outputs are Iceberg tables `lakehouse.silver.yellow_trips` and `lakehouse.silver.green_trips`; DDL lives in `src/etl/sql/01_silver_yellow_trips.sql` and `02_silver_green_trips.sql`.
- Writes use `overwritePartitions()`, so rerunning the same month is intended to be retry-safe.
- Keep output columns and casts aligned with the SQL DDL; yellow and green schemas intentionally differ.
- Preserve `year` and `month` as deterministic partition columns derived from `pickup_ts`.

## Airflow Simulation
- DAG IDs: `yellow_trips_simulated_arrival` and `green_trips_simulated_arrival`; both run daily with `catchup=False` and `max_active_runs=1`.
- Monthly progress is stored in Airflow Variables, not logical dates: `yellow_trips_next_partition`, `green_trips_next_partition`, plus optional `*_end_partition`.
- Default ranges are yellow `2011-01` through `2025-12` and green `2014-01` through `2025-12`.
- DAG flow must stay `choose_partition -> stage_*_trips -> advance_partition`; only advance after Spark succeeds or failed retries will skip data.
- Spark Standalone with PySpark must run Airflow `SparkSubmitOperator` in client mode; executors need the driver reachable via `spark.driver.host=airflow-airflow-worker-1` and `spark.driver.bindAddress=0.0.0.0`.
- The Airflow Spark client runtime documented in `docs/spark_airflow_taxi_batch_simulation.md` uses Python 3.10 to match Spark workers; do not assume the repo's local Python 3.12 venv is the Airflow driver runtime.
- Airflow workers need Spark 3.5.6, Java 17, `/opt/spark/spark-events`, S3A config, and AWS/MinIO env vars because the driver runs in the Airflow worker.

## Local Artifacts And Secrets
- Do not stage `.env`, `data/`, `orchestration/logs/`, `orchestration/plugins/`, `orchestration/config/`, `.idea/`, `.vscode/`, or `lakehouse_homelab.code-workspace`; they are local/ignored artifacts.
- `test.py` is an ignored local S3 connectivity probe that loads `.env`; treat it as environment-specific unless the user explicitly asks to change it.
- `build/` contains generated/package output; edit source under `src/` instead.

## Change Guidance
- Search existing Spark jobs and DAGs before changing schemas or partition behavior; schema changes usually require updating both Python normalization and `src/etl/sql/*.sql`.
- Use `pyspark.sql.functions as f`, matching the existing job style.
- Avoid broad `collect()`/`toPandas()` in ETL jobs; these pipelines target large taxi datasets.
- Ask before changing S3 path conventions, Iceberg table names, Airflow Variable names, or the simulated monthly progression contract.
