import logging

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)


def parse_partition(value: str) -> tuple[int, int]:
    year, month = value.split("-", maxsplit=1)
    return int(year), int(month)


def format_partition(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def next_partition(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


STATE_VAR = "green_trips_next_partition"
END_VAR = "green_trips_end_partition"
DEFAULT_START_PARTITION = "2014-01"
DEFAULT_END_PARTITION = "2025-12"
SPARK_CONF = {
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.host": "airflow-airflow-worker-1",
}
RAW_BUCKET = "raw"
RAW_ENDPOINT_URL = "http://minio:9000"


@dag(
    dag_id="green_trips_simulated_arrival",
    schedule="@daily",
    description="Daily simulation that processes the next green trips month",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
)
def green_trips_dag():
    @task(multiple_outputs=True)
    def choose_partition() -> dict[str, str]:
        partition = Variable.get(STATE_VAR, default=DEFAULT_START_PARTITION)
        end_partition = Variable.get(END_VAR, default=DEFAULT_END_PARTITION)

        year, month = parse_partition(partition)
        end_year, end_month = parse_partition(end_partition)
        if (year, month) > (end_year, end_month):
            raise AirflowSkipException(
                f"No green trip partitions left after {end_partition}."
            )

        return {"year": str(year), "month": str(month)}

    @task.branch
    def route_raw_partition(year: str, month: str) -> str:
        import boto3
        from botocore.exceptions import ClientError

        object_key = f"data/{year}/green_tripdata_{year}-{int(month):02d}.parquet"
        s3 = boto3.client("s3", endpoint_url=RAW_ENDPOINT_URL)

        try:
            s3.head_object(Bucket=RAW_BUCKET, Key=object_key)
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                logger.info(
                    f"Missing raw Green trips object; advancing past {object_key}."
                )
                return "advance_missing_partition"
            raise

        return "stage_green_trips"

    def _advance_partition(year: str, month: str) -> None:
        next_year, next_month = next_partition(int(year), int(month))
        Variable.set(STATE_VAR, format_partition(next_year, next_month))

    @task
    def advance_missing_partition(year: str, month: str) -> None:
        _advance_partition(year, month)

    @task
    def advance_processed_partition(year: str, month: str) -> None:
        _advance_partition(year, month)

    partition = choose_partition()
    route = route_raw_partition(partition["year"], partition["month"])

    stage_partition = SparkSubmitOperator(
        task_id="stage_green_trips",
        application="/opt/lakehouse/src/etl/jobs/nyc_tlc_stg_trip_data.py",
        conn_id="spark",
        name="sim__{{ run_id }}__green__stage",
        conf=SPARK_CONF,
        application_args=[
            "--dataset",
            "green",
            "--year",
            partition["year"],
            "--month",
            partition["month"],
            "--catalog",
            "lakehouse",
            "--dag-run-id",
            "{{ run_id }}",
            "--input-base",
            "s3a://raw/data",
        ],
    )

    check_silver_quality = SparkSubmitOperator(
        task_id="check_silver_quality",
        application="/opt/lakehouse/src/etl/jobs/nyc_tlc_silver_quality.py",
        conn_id="spark",
        name="sim__{{ run_id }}__green__quality",
        conf=SPARK_CONF,
        application_args=[
            "--dataset",
            "green",
            "--year",
            partition["year"],
            "--month",
            partition["month"],
            "--catalog",
            "lakehouse",
            "--dag-run-id",
            "{{ run_id }}",
        ],
    )

    build_gold_revenue = SparkSubmitOperator(
        task_id="build_gold_revenue",
        application="/opt/lakehouse/src/etl/jobs/nyc_tlc_gold_revenue.py",
        conn_id="spark",
        name="sim__{{ run_id }}__green__gold",
        conf=SPARK_CONF,
        application_args=[
            "--dataset",
            "green",
            "--year",
            partition["year"],
            "--month",
            partition["month"],
            "--catalog",
            "lakehouse",
            "--dag-run-id",
            "{{ run_id }}",
        ],
    )

    advance_missing = advance_missing_partition(partition["year"], partition["month"])
    advance_processed = advance_processed_partition(
        partition["year"], partition["month"]
    )

    route >> [stage_partition, advance_missing]
    stage_partition >> check_silver_quality >> build_gold_revenue >> advance_processed


green_trips_dag()
