import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException


STATE_VAR = "yellow_trips_next_partition"
END_VAR = "yellow_trips_end_partition"
DEFAULT_START_PARTITION = "2011-01"
DEFAULT_END_PARTITION = "2025-12"


def _parse_partition(value: str) -> tuple[int, int]:
    year, month = value.split("-", maxsplit=1)
    return int(year), int(month)


def _format_partition(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _next_partition(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


@dag(
    dag_id="yellow_trips_simulated_arrival",
    schedule="@daily",
    description="Daily simulation that processes the next yellow trips month",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
)
def yellow_trips_dag():
    @task
    def choose_partition() -> dict[str, int]:
        partition = Variable.get(STATE_VAR, default=DEFAULT_START_PARTITION)
        end_partition = Variable.get(END_VAR, default=DEFAULT_END_PARTITION)

        year, month = _parse_partition(partition)
        end_year, end_month = _parse_partition(end_partition)
        if (year, month) > (end_year, end_month):
            raise AirflowSkipException(
                f"No yellow trip partitions left after {end_partition}."
            )

        return {"year": year, "month": month}

    @task
    def advance_partition(partition: dict[str, int]) -> None:
        next_year, next_month = _next_partition(partition["year"], partition["month"])
        Variable.set(STATE_VAR, _format_partition(next_year, next_month))

    stage_partition = SparkSubmitOperator(
        task_id="stage_yellow_trips",
        application="/opt/spark/jobs/nyc_tlc_stg_trip_data.py",
        conn_id="spark",
        application_args=[
            "--dataset",
            "yellow",
            "--year",
            "{{ ti.xcom_pull(task_ids='choose_partition')['year'] }}",
            "--month",
            "{{ ti.xcom_pull(task_ids='choose_partition')['month'] }}",
            "--input-base",
            "s3a://raw/data",
        ],
    )

    partition = choose_partition()
    stage_partition >> advance_partition(partition)


yellow_trips_dag()
