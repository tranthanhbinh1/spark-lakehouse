import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag

SPARK_CONF = {
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.host": "airflow-airflow-worker-1",
}


def conf_value(key: str) -> str:
    return f"{{{{ dag_run.conf['{key}'] }}}}"


@dag(
    dag_id="taxi_benchmark_pipeline",
    schedule=None,
    description="Benchmark wrapper for fixed NYC TLC stage, quality, and gold runs",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
)
def taxi_benchmark_pipeline():
    stage_partition = SparkSubmitOperator(
        task_id="stage_trips",
        application="/opt/lakehouse/src/etl/jobs/nyc_tlc_stg_trip_data.py",
        conn_id="spark",
        conf=SPARK_CONF,
        name=conf_value("application_name_stage"),
        application_args=[
            "--dataset",
            conf_value("dataset"),
            "--year",
            conf_value("year"),
            "--month",
            conf_value("month"),
            "--input-base",
            conf_value("input_base"),
            "--catalog",
            conf_value("catalog"),
            "--benchmark-run-id",
            conf_value("benchmark_run_id"),
            "--dag-run-id",
            "{{ run_id }}",
            "--repetition",
            conf_value("repetition"),
            "--application-name",
            conf_value("application_name_stage"),
        ],
    )

    check_silver_quality = SparkSubmitOperator(
        task_id="check_silver_quality",
        application="/opt/lakehouse/src/etl/jobs/nyc_tlc_silver_quality.py",
        conn_id="spark",
        conf=SPARK_CONF,
        name=conf_value("application_name_quality"),
        application_args=[
            "--dataset",
            conf_value("dataset"),
            "--year",
            conf_value("year"),
            "--month",
            conf_value("month"),
            "--catalog",
            conf_value("catalog"),
            "--benchmark-run-id",
            conf_value("benchmark_run_id"),
            "--dag-run-id",
            "{{ run_id }}",
            "--repetition",
            conf_value("repetition"),
            "--application-name",
            conf_value("application_name_quality"),
        ],
    )

    build_gold_revenue = SparkSubmitOperator(
        task_id="build_gold_revenue",
        application="/opt/lakehouse/src/etl/jobs/nyc_tlc_gold_revenue.py",
        conn_id="spark",
        conf=SPARK_CONF,
        name=conf_value("application_name_gold"),
        application_args=[
            "--dataset",
            conf_value("dataset"),
            "--year",
            conf_value("year"),
            "--month",
            conf_value("month"),
            "--catalog",
            conf_value("catalog"),
            "--benchmark-run-id",
            conf_value("benchmark_run_id"),
            "--dag-run-id",
            "{{ run_id }}",
            "--repetition",
            conf_value("repetition"),
            "--application-name",
            conf_value("application_name_gold"),
        ],
    )

    stage_partition >> check_silver_quality >> build_gold_revenue


taxi_benchmark_pipeline()
