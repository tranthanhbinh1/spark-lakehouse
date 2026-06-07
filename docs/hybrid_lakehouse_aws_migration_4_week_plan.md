# 4-Week Hybrid Lakehouse AWS Migration Plan

## Summary

Build a working hybrid lakehouse demo where local Airflow/Spark remains the compute plane, while AWS S3 and AWS Glue Data Catalog become the cloud storage and Iceberg metadata plane.

Target outcome after 4 weeks:

- NYC TLC monthly pipeline reads raw parquet from AWS S3.
- Spark writes Apache Iceberg v2 tables to an S3 warehouse.
- Glue Catalog registers `lakehouse.silver`, `lakehouse.quality`, and `lakehouse.gold`.
- Existing local MinIO path remains as a fallback/dev profile.
- Deliverables include a runnable demo, architecture diagram, migration notes, test evidence, and internship report material.

Important correction: S3 is storage, not a table format. The table-format decision is separate. Default decision: keep Apache Iceberg and migrate the catalog to AWS Glue.

## Week 1: Baseline, Architecture, AWS Foundation

- Document current local architecture:
  - Airflow DAGs
  - Spark Standalone cluster
  - MinIO raw zone
  - Iceberg silver tables
  - monthly partition simulation
- Define target hybrid architecture:
  - Local compute: Airflow scheduler/worker and Spark driver/executors
  - AWS storage/catalog: S3 raw bucket, S3 Iceberg warehouse, Glue Data Catalog
  - Optional consumer: Athena read-only validation
- Add minimal AWS infrastructure definition, preferably under `infra/aws/`:
  - S3 bucket with prefixes: `raw/data/`, `warehouse/`, `logs/`
  - Glue databases: `silver`, `quality`, `gold`
  - IAM policy for local Spark/Airflow role or user
  - S3 encryption, versioning, and lifecycle rules for raw/warehouse/logs
- Reauthenticate AWS locally before running AWS work. Current local AWS session inspection showed expired session state.

## Week 2: S3 + Glue Iceberg Migration

- Add an AWS Spark profile using Iceberg's Glue catalog pattern:
  - `spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog`
  - `spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`
  - `spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO`
  - `spark.sql.catalog.lakehouse.warehouse=s3://<bucket>/warehouse`
  - `spark.sql.catalog.lakehouse.client.region=<region>`
- Keep table identifiers stable:
  - `lakehouse.silver.yellow_trips`
  - `lakehouse.silver.green_trips`
  - future `lakehouse.quality.*`
  - future `lakehouse.gold.*`
- Parameterize hardcoded local paths:
  - Raw input base remains configurable and defaults locally to `s3a://raw/data`.
  - AWS raw input base becomes `s3a://<bucket>/raw/data`.
  - Iceberg warehouse uses `s3://<bucket>/warehouse`.
- Upload a small raw sample to S3, then run one yellow and one green partition through Spark.
- Verify Glue table registration and read back tables from Spark.

## Week 3: Flesh Out Platform Capabilities

- Implement the planned quality layer:
  - `lakehouse.quality.silver_trip_quality_results`
  - Pandera/PySpark validation job for silver partitions
  - Airflow flow: `choose_partition -> stage -> quality -> advance`
- Add first gold mart:
  - `lakehouse.gold.trip_revenue_monthly`
  - monthly revenue/trip aggregate by dataset, year, month
  - Airflow flow: `choose_partition -> stage -> quality -> gold -> advance`
- Add operational visibility:
  - row counts per partition
  - failed quality checks in the audit table
  - basic runtime metrics captured in logs or a small report table
- Prove hybrid behavior by running the same logical pipeline against:
  - local MinIO profile
  - AWS S3 + Glue profile

## Week 4: Evaluation, Hardening, Report

- Run controlled experiments:
  - local MinIO vs AWS S3/Glue for 1-3 monthly partitions
  - first run vs retry/idempotent overwrite
  - missing partition failure
  - bad-data quality failure
- Validate interoperability:
  - Spark reads/writes Iceberg tables through Glue.
  - Athena or another AWS query path can read at least one Iceberg table if available in the account.
- Produce final internship artifacts:
  - architecture diagram
  - migration guide
  - runbook for local and AWS profiles
  - cost/security notes
  - benchmark/evaluation table
  - research discussion: benefits, limitations, and next steps

## Public Interfaces And Defaults

- Keep existing job CLI shape:
  - `--dataset yellow|green`
  - `--year`
  - `--month`
  - `--input-base`
  - `--dry-run`
- Add an environment/config profile concept:
  - `local`: MinIO + current local Iceberg catalog
  - `aws`: S3 raw data + Glue Catalog + S3 warehouse
- Do not rename Airflow DAG IDs or Airflow partition variables.
- Do not change table names unless explicitly required.
- Keep Iceberg table format version `2`.
- Use AWS region `us-east-1` unless the internship/account requires another region.

## Test Plan

- Run `uv run ruff check .`.
- Run local dry runs:
  - yellow `2011-01`
  - green `2014-01`
- Run AWS dry run against S3 raw input.
- Run AWS write test for one yellow and one green month.
- Re-run the same month and confirm `overwritePartitions()` is idempotent.
- Confirm Glue databases/tables exist after writes.
- Confirm row counts match expected source partitions.
- Confirm quality job blocks bad or missing partitions.
- Confirm gold monthly revenue table builds only after quality success.
- Optional: query one Iceberg table from Athena for interoperability evidence.

## Assumptions

- Scope excludes migrating Airflow or Spark compute to AWS in this 4-week slice.
- AWS credentials and budget are available for small sample data only.
- Glue Catalog is the default catalog choice.
- Polaris/REST Catalog and AWS S3 Tables are research extensions, not the primary path.
- The final artifact must be both runnable and explainable, not just a report.
