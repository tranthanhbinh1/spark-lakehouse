import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from utils.common import format_partition, next_partition, parse_partition


STATE_VAR = "green_trips_next_partition"
END_VAR = "green_trips_end_partition"
DEFAULT_START_PARTITION = "2014-01"
DEFAULT_END_PARTITION = "2025-12"
SPARK_CONF = {
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.host": "airflow-airflow-worker-1",
}


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

    @task
    def advance_partition(year: str, month: str) -> None:
        next_year, next_month = next_partition(int(year), int(month))
        Variable.set(STATE_VAR, format_partition(next_year, next_month))

    partition = choose_partition()

    stage_partition = SparkSubmitOperator(
        task_id="stage_green_trips",
        application="/opt/spark/jobs/nyc_tlc_stg_trip_data.py",
        conn_id="spark",
        conf=SPARK_CONF,
        application_args=[
            "--dataset",
            "green",
            "--year",
            partition["year"],
            "--month",
            partition["month"],
            "--input-base",
            "s3a://raw/data",
        ],
    )

    check_silver_quality = SparkSubmitOperator(
        task_id="check_silver_quality",
        application="/opt/spark/jobs/nyc_tlc_silver_quality.py",
        conn_id="spark",
        conf=SPARK_CONF,
        application_args=[
            "--dataset",
            "green",
            "--year",
            partition["year"],
            "--month",
            partition["month"],
        ],
    )

    build_gold_revenue = SparkSubmitOperator(
        task_id="build_gold_revenue",
        application="/opt/spark/jobs/nyc_tlc_gold_revenue.py",
        conn_id="spark",
        conf=SPARK_CONF,
        application_args=[
            "--dataset",
            "green",
            "--year",
            partition["year"],
            "--month",
            partition["month"],
        ],
    )

    advance = advance_partition(partition["year"], partition["month"])

    (
        partition
        >> stage_partition
        >> check_silver_quality
        >> build_gold_revenue
        >> advance
    )


green_trips_dag()
