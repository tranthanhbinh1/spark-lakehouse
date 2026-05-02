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
    @task(multiple_outputs=True)
    def choose_partition() -> dict[str, str]:
        partition = Variable.get(STATE_VAR, default=DEFAULT_START_PARTITION)
        end_partition = Variable.get(END_VAR, default=DEFAULT_END_PARTITION)

        year, month = _parse_partition(partition)
        end_year, end_month = _parse_partition(end_partition)
        if (year, month) > (end_year, end_month):
            raise AirflowSkipException(
                f"No yellow trip partitions left after {end_partition}."
            )

        return {"year": str(year), "month": str(month)}

    @task
    def advance_partition(year: str, month: str) -> None:
        next_year, next_month = _next_partition(int(year), int(month))
        Variable.set(STATE_VAR, _format_partition(next_year, next_month))

    partition = choose_partition()

    stage_partition = SparkSubmitOperator(
        task_id="stage_yellow_trips",
        application="/opt/spark/jobs/nyc_tlc_stg_trip_data.py",
        conn_id="spark",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-airflow-worker-1",
        },
        application_args=[
            "--dataset",
            "yellow",
            "--year",
            partition["year"],
            "--month",
            partition["month"],
            "--input-base",
            "s3a://raw/data",
        ],
    )

    advance = advance_partition(partition["year"], partition["month"])

    partition >> stage_partition >> advance


yellow_trips_dag()
