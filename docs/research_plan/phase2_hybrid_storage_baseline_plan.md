# Phase 2 Hybrid Storage Baseline Plan

## Relationship To The Canonical Plan

Parent plan:
`docs/research_plan/hybrid_storage_tradeoff_research_plan.md`

Parent phase:
`Phase 2: Hybrid Storage Baseline`

Goal:
move only storage and catalog to AWS while keeping Airflow, Spark, and Trino
local.

Hard boundaries:

- Do not use AWS Glue ETL.
- Do not use S3 Tables.
- Do not move Airflow, Spark, or Trino compute to AWS.
- Do not change ETL business logic or table schemas for Phase 2.
- Do not store benchmark metrics in the hybrid catalog; keep metrics in
  `lakehouse.benchmark.run_metrics`.

## Phase 2 Target

```text
Local Airflow
  -> local Spark Standalone
  -> local Trino
  -> AWS S3 raw data
  -> AWS S3 Iceberg warehouse
  -> Apache Iceberg tables
  -> AWS Glue Data Catalog
```

Hybrid data catalog:

```text
lakehouse_hybrid
```

Glue databases / Iceberg namespaces:

```text
silver
gold
quality
```

Benchmark metrics table remains:

```text
lakehouse.benchmark.run_metrics
```

## Added Repo Artifacts

- `conf/environments/hybrid_aws.toml`
- `conf/spark/spark-defaults.hybrid-aws.conf`
- `conf/trino/catalog/lakehouse_hybrid.properties`
- `src/etl/sql/hybrid_aws/*.sql`
- `scripts/aws/bootstrap_phase2_hybrid.py`

## Current AWS Defaults

The profile is explicit because the local default AWS region is not the
canonical plan region.

```text
aws_profile = lakehouse-aws
aws_account_id = 174029311478
aws_region = us-east-1
raw_bucket = lakehouse-hybrid-174029311478-us-east-1-raw
warehouse_bucket = lakehouse-hybrid-174029311478-us-east-1-warehouse
```

Changing these values is allowed, but change `conf/environments/hybrid_aws.toml`,
`conf/spark/spark-defaults.hybrid-aws.conf`, and
`conf/trino/catalog/lakehouse_hybrid.properties` together.

## Bootstrap Steps

### 1. Verify AWS Identity

```bash
aws sts get-caller-identity --profile lakehouse-aws
aws configure list --profile lakehouse-aws
```

The account must match `174029311478`, unless the Phase 2 config is edited.

### 2. Preview AWS Resource Creation

```bash
uv run python scripts/aws/bootstrap_phase2_hybrid.py --dry-run
```

This previews:

- S3 raw bucket
- S3 warehouse bucket
- raw and warehouse prefix markers
- Glue databases
- smoke workload raw parquet uploads

### 3. Create AWS Resources And Upload Smoke Raw Data

```bash
uv run python scripts/aws/bootstrap_phase2_hybrid.py
```

If local raw files are missing, run the existing bootstrap for the benchmark
years or use `--skip-upload` and upload raw parquet separately.

Expected raw S3 paths:

```text
s3://lakehouse-hybrid-174029311478-us-east-1-raw/data/2011/yellow_tripdata_2011-01.parquet
s3://lakehouse-hybrid-174029311478-us-east-1-raw/data/2014/green_tripdata_2014-01.parquet
```

### 4. Mount Hybrid Spark Catalog Config

The Spark config must be present for the Airflow worker Spark driver and Spark
runtime containers.

Use `conf/spark/spark-defaults.hybrid-aws.conf` as the source for
`/opt/spark/conf/spark-defaults.conf` or merge its `lakehouse_hybrid` settings
into the active Spark defaults.

The active runtime must have AWS credentials available through the default AWS
provider chain. Do not hardcode AWS keys into repo files.

### 5. Mount Hybrid Trino Catalog Config

Mount:

```text
conf/trino/catalog/lakehouse_hybrid.properties
```

to:

```text
/etc/trino/catalog/lakehouse_hybrid.properties
```

Then restart Trino.

### 6. Create Hybrid Iceberg Tables

Run the hybrid DDL files through Spark SQL after the Spark config is active:

```bash
docker exec airflow-airflow-worker-1 spark-sql -f /opt/lakehouse/src/etl/sql/hybrid_aws/00_namespaces.sql
docker exec airflow-airflow-worker-1 spark-sql -f /opt/lakehouse/src/etl/sql/hybrid_aws/01_silver_yellow_trips.sql
docker exec airflow-airflow-worker-1 spark-sql -f /opt/lakehouse/src/etl/sql/hybrid_aws/02_silver_green_trips.sql
docker exec airflow-airflow-worker-1 spark-sql -f /opt/lakehouse/src/etl/sql/hybrid_aws/03_silver_trips_quality_results.sql
docker exec airflow-airflow-worker-1 spark-sql -f /opt/lakehouse/src/etl/sql/hybrid_aws/04_gold_trip_revenue_monthly.sql
```

### 7. Run Hybrid Smoke Benchmark

Use the same workload and benchmark runner:

```bash
AIRFLOW_USERNAME=airflow AIRFLOW_PASSWORD=<local-airflow-password> \
uv run python scripts/benchmarks/run_benchmark.py \
  --profile conf/environments/hybrid_aws.toml
```

Acceptance is the same shape as the accepted on-premises smoke baseline:

- 3 successful pipeline repetitions for `yellow 2011-01`
- 3 successful pipeline repetitions for `green 2014-01`
- 5 successful Trino query repetitions per query and partition
- every Airflow task metric has a Spark application ID
- repeated Silver and Gold partition metrics remain stable
- metrics insert succeeds into `lakehouse.benchmark.run_metrics`

## Validation Queries

After the hybrid smoke run, verify through Trino:

```sql
select metric_type, count(*)
from lakehouse.benchmark.run_metrics
where architecture = 'hybrid_storage'
  and environment = 'local_aws'
group by metric_type
order by metric_type;
```

Verify Glue-backed data through Trino:

```sql
select count(*)
from lakehouse_hybrid.silver.yellow_trips
where year = 2011 and month = 1;

select count(*)
from lakehouse_hybrid.silver.green_trips
where year = 2014 and month = 1;
```

## Phase 2 Exit Criteria

Phase 2 is complete only when:

- AWS resources exist in the configured account and region.
- Raw smoke parquet files exist in S3.
- Spark can create/read/write `lakehouse_hybrid` Iceberg tables through Glue.
- Trino can read `lakehouse_hybrid` Iceberg tables through Glue.
- The hybrid smoke benchmark passes with the same workload shape as the
  accepted on-premises smoke benchmark.
- The artifact path and `benchmark_run_id` are recorded back in the canonical
  research plan.


## Execution Status - 2026-07-07

Completed setup:

- Created/verified the configured S3 raw and warehouse buckets in `us-east-1`.
- Uploaded the smoke raw files for `yellow 2011-01` and `green 2014-01`.
- Created Glue databases `silver`, `gold`, and `quality`.
- Created Glue-backed Iceberg tables for hybrid Silver, Gold, and quality outputs.
- Mounted the hybrid Spark and Trino catalog configuration into the local runtime.
- Verified Trino can read `lakehouse_hybrid` through the Glue-backed Iceberg catalog.

Validated partial smoke:

```text
benchmark_run_id = bench_hybrid_aws_phase2_green_smoke_20260707T164959Z_d79fe1b
artifact = benchmarks/artifacts/bench_hybrid_aws_phase2_green_smoke_20260707T164959Z_d79fe1b/benchmark_run.json
workload = benchmarks/workloads/phase2_green_smoke.toml
partition = green 2014-01
dag_run_state = success
metrics_inserted = 13
silver_green_rows = 803609
quality_rows = 3
```

Resolved blocker:

- On 2026-07-08, the previously failing yellow S3 byte-range read completed through `aws s3api get-object --range`.
- The full `benchmarks/workloads/smoke.toml` Phase 2 gate then completed with the same workload shape as the accepted on-premises smoke baseline.

Accepted full hybrid smoke:

```text
benchmark_run_id = bench_hybrid_aws_smoke_20260708T053029Z_d79fe1b
config_hash = 768f8591dbb2841a1ea9c7cdd7b248830a8d8fae45a0b8e5edbdad4b7cd64eb9
artifact = benchmarks/artifacts/bench_hybrid_aws_smoke_20260708T053029Z_d79fe1b/benchmark_run.json
workload = benchmarks/workloads/smoke.toml
dag_results = 6
metrics = 106
query_results = 70
```

Metrics inserted into `lakehouse.benchmark.run_metrics`:

```text
airflow_task       18
iceberg_partition 12
pipeline           6
trino_query        70
```

Glue-backed Silver validation through Trino:

```text
yellow 2011-01 rows = 13,393,301
green 2014-01 rows  = 803,609
```

The green-only smoke remains setup evidence only. The accepted Phase 2 smoke baseline is the full yellow + green run above.
