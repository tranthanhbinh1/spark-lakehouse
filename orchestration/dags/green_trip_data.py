import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from utils.common import parse_partition, next_partition, format_partition

STATE_VAR = "green_trips_next_partition"
END_VAR = "green_trips_end_partition"
DEFAULT_START_PARTITION = "2014-01"
DEFAULT_END_PARTITION = "2025-12"


@dag(
    dag_id="green_trips_simulated_arrival",
    schedule="@daily",
    description="Daily simulation that processes the next green trips month",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
)
def green_trips_dag():
    @task
    def choose_partition() -> dict[str, int]:
        partition = Variable.get(STATE_VAR, default=DEFAULT_START_PARTITION)
        end_partition = Variable.get(END_VAR, default=DEFAULT_END_PARTITION)

        year, month = parse_partition(partition)
        end_year, end_month = parse_partition(end_partition)
        if (year, month) > (end_year, end_month):
            raise AirflowSkipException(
                f"No green trip partitions left after {end_partition}."
            )

        return {"year": year, "month": month}

    @task
    def advance_partition(partition: dict[str, int]) -> None:
        next_year, next_month = next_partition(partition["year"], partition["month"])
        Variable.set(STATE_VAR, format_partition(next_year, next_month))

    stage_partition = SparkSubmitOperator(
        task_id="stage_green_trips",
        application="/opt/spark/jobs/nyc_tlc_stg_trip_data.py",
        conn_id="spark",
        application_args=[
            "--dataset",
            "green",
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


green_trips_dag()
