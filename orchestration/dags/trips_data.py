import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    schedule="@daily",
    description="DAG to process trips data",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
)
def yellow_trips_dag():
    _ = SparkSubmitOperator(
        task_id="process_trips_data",
        application="./src/etl/jobs/nyc_tlc_stg_yellow_trips_data.py",
        conn_id="spark",
    )


yellow_trips_dag()
