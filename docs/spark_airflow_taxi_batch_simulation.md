# Spark and Airflow Taxi Batch Simulation Findings

## Objective

Build a production-like local ETL simulation for NYC taxi trip data where Airflow schedules monthly Spark batch jobs against a local Spark Standalone cluster.

The target behavior is:

```text
Static NYC taxi files
  -> simulated monthly arrivals
  -> Spark staging job
  -> Iceberg silver tables
  -> safe retries and incremental partition processing
```

## Architecture Decision

We kept `SparkSubmitOperator` instead of building a custom REST submitter.

This is closer to a common production pattern where Airflow submits Spark jobs through a Spark client installed on the Airflow worker or on an edge node.

The important constraint is that `SparkSubmitOperator` is not a remote API client. It shells out to a local `spark-submit` binary from the Airflow worker.

Because the cluster is Spark Standalone and the job is PySpark, we must use:

```text
deploy-mode=client
```

Spark Standalone does not support PySpark cluster deploy mode:

```text
Cluster deploy mode is currently not supported for python applications on standalone clusters.
```

So the runtime shape is:

```text
Airflow worker
  runs spark-submit
  runs the Spark driver

Spark master
  coordinates the application

Spark workers
  run executors
```

## Airflow Image Changes

The original Airflow image only had:

```text
apache-airflow-providers-apache-spark
```

That provider gives Airflow the operator, but it does not install Spark.

We rebuilt the Airflow image so the worker can act as a Spark client:

```text
Java 17
Spark 3.5.6
spark-submit
Python 3.10
apache-airflow-providers-apache-spark
```

We used the existing `spark-local:3.5.6-java17` image as a build stage so the Airflow Spark client matches the running Spark cluster.

We also switched Airflow to the Python 3.10 base image because the Spark workers use Python 3.10. PySpark requires the driver and worker Python minor versions to match.

## Airflow Compose Changes

We mounted Spark runtime files into Airflow:

```yaml
- /home/tb24/projects/lakehouse/src/etl/jobs:/opt/spark/jobs:ro
- ../spark/conf/core-site.xml:/opt/spark/conf/core-site.xml:ro
- ../spark/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
- ../data/spark/events:/opt/spark/spark-events
```

We also added Spark and PySpark environment variables:

```yaml
JAVA_HOME: /opt/java/openjdk
SPARK_HOME: /opt/spark
PATH: /opt/spark/bin:/opt/spark/sbin:/opt/java/openjdk/bin:...
PYSPARK_PYTHON: /usr/bin/python3
PYSPARK_DRIVER_PYTHON: /usr/python/bin/python
```

Because the Spark driver runs inside Airflow in client mode, Airflow also needs MinIO/AWS settings for Iceberg writes:

```yaml
AWS_ACCESS_KEY_ID: admin
AWS_SECRET_ACCESS_KEY: binhdeptrai
AWS_REGION: us-east-1
```

## Spark Driver Networking

In client mode, Spark executors must connect back to the driver running inside the Airflow worker container.

The driver initially advertised an unreachable container hostname/IP. We fixed this by passing Spark driver networking config from the DAG:

```python
conf={
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.host": "airflow-airflow-worker-1",
}
```

## Failure Sequence and Fixes

### Missing XCom

Error:

```text
UndefinedError: 'None' has no attribute 'year'
```

Cause:

`stage_yellow_trips` could render before `choose_partition` produced its XCom.

Fix:

Use TaskFlow `XComArg` directly and wire the dependency explicitly:

```python
partition = choose_partition()
advance = advance_partition(partition["year"], partition["month"])

partition >> stage_partition >> advance
```

### Permission Denied: spark-submit

Error:

```text
PermissionError: [Errno 13] Permission denied: 'spark-submit'
```

Cause:

The Airflow worker did not have a real Spark client installed.

Fix:

Rebuild the Airflow image with Java and Spark 3.5.6.

### Executor Cannot Connect to Driver

Error pattern:

```text
Failed to connect to <airflow-worker-container>:<driver-port>
```

Cause:

Executors could not reach the client-mode driver in the Airflow worker.

Fix:

Set:

```text
spark.driver.host=airflow-airflow-worker-1
spark.driver.bindAddress=0.0.0.0
```

### PySpark Python Version Mismatch

Error:

```text
[PYTHON_VERSION_MISMATCH] Python in worker has different version than that in driver
```

Cause:

Airflow driver Python and Spark worker Python had different minor versions.

Fix:

Use the Airflow Python 3.10 image to match Spark workers.

### Missing Spark Event Log Directory

Error:

```text
java.io.FileNotFoundException: File file:/opt/spark/spark-events does not exist
```

Cause:

`spark-defaults.conf` enables event logging, but `/opt/spark/spark-events` was not mounted into the Airflow worker.

Fix:

Mount the same Spark event directory into Airflow:

```yaml
- ../data/spark/events:/opt/spark/spark-events
```

### Missing AWS Region

Error:

```text
Unable to load region from any of the providers in the chain
```

Cause:

Iceberg's AWS SDK ran in the Airflow driver and did not have `AWS_REGION`.

Fix:

Add AWS/MinIO environment variables to Airflow.

## Taxi Trip Spark Job

We created a unified Spark job:

```text
src/etl/jobs/nyc_tlc_stg_trip_data.py
```

It supports both taxi datasets:

```bash
--dataset yellow
--dataset green
```

It processes one monthly partition per run:

```bash
--year 2011 --month 1
```

Input path:

```text
s3a://raw/data/{year}/{dataset}_tripdata_{year}-{month}.parquet
```

Output tables:

```text
lakehouse.silver.yellow_trips
lakehouse.silver.green_trips
```

The job uses:

```python
overwritePartitions()
```

That makes retries safe for a monthly partition. If a run fails midway, rerunning the same month overwrites that partition.

## Schema Normalization

The unified job uses readable rename maps to handle yellow and green schema differences.

Examples:

```python
{
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
}
```

The job standardizes output columns and derives:

```text
trip_duration_min
year
month
is_valid_trip
has_tip
tip_ratio
```

## Airflow Simulation DAGs

We created dataset-specific DAGs:

```text
yellow_trips_simulated_arrival
green_trips_simulated_arrival
```

They run daily with:

```python
catchup=False
max_active_runs=1
```

Instead of using Airflow logical date as the historical taxi month, the DAGs track the next partition through Airflow Variables:

```text
yellow_trips_next_partition
green_trips_next_partition
```

The DAG flow is:

```text
choose_partition
  -> stage_*_trips
  -> advance_partition
```

`advance_partition` only runs after Spark succeeds. That means failed runs can be retried safely without skipping a month.

Default ranges:

```text
yellow: 2011-01 through 2025-12
green: 2014-01 through 2025-12
```

## Verification

We verified the final stack by running the same Spark command used by the DAG from inside `airflow-airflow-worker-1`.

The successful write committed:

```text
table: lakehouse.silver.yellow_trips
partition: 2011-01
operation: overwrite
records: 13,393,301
```

The Iceberg commit completed with:

```text
Committed snapshot 7027314079305054257
```

## Current State

The Spark and Airflow integration is working.

Airflow can now:

```text
submit PySpark jobs with SparkSubmitOperator
run the driver in the Airflow worker
execute work on Spark workers
read source parquet files from MinIO via S3A
write Iceberg silver tables
advance monthly partitions only after success
```

The remaining work is to continue running the DAGs through the historical monthly partitions and later add data quality checks, monitoring, gold marts, and backfill handling.
