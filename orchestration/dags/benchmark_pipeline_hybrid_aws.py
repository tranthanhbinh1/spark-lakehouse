import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag

HYBRID_AWS_SPARK_CONF = {
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.host": "airflow-airflow-worker-1",
    "spark.executorEnv.AWS_PROFILE": "lakehouse-aws",
    "spark.executorEnv.AWS_CREDENTIAL_PROFILES_FILE": (
        "/home/spark/.aws/credentials"
    ),
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    ),
    "spark.hadoop.fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    "spark.hadoop.fs.s3a.path.style.access": "false",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.hadoop.fs.s3a.connection.establish.timeout": "15000",
    "spark.hadoop.fs.s3a.connection.timeout": "60000",
    "spark.hadoop.fs.s3a.connection.request.timeout": "60000",
    "spark.hadoop.fs.s3a.attempts.maximum": "3",
    "spark.hadoop.fs.s3a.retry.limit": "3",
    "spark.hadoop.fs.s3a.experimental.input.fadvise": "sequential",
    "spark.sql.files.maxPartitionBytes": "268435456",
    "spark.sql.catalog.lakehouse_hybrid": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse_hybrid.type": "glue",
    "spark.sql.catalog.lakehouse_hybrid.warehouse": (
        "s3://lakehouse-hybrid-174029311478-us-east-1-warehouse/warehouse"
    ),
    "spark.sql.catalog.lakehouse_hybrid.io-impl": (
        "org.apache.iceberg.aws.s3.S3FileIO"
    ),
    "spark.sql.catalog.lakehouse_hybrid.client.region": "us-east-1",
    "spark.sql.catalog.lakehouse_hybrid.glue.region": "us-east-1",
    "spark.sql.catalog.lakehouse_hybrid.glue.account-id": "174029311478",
    "spark.sql.catalog.lakehouse_hybrid.s3.credentials-provider": (
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    ),
}


def conf_value(key: str) -> str:
    return f"{{{{ dag_run.conf['{key}'] }}}}"


@dag(
    dag_id="taxi_benchmark_pipeline_hybrid_aws",
    schedule=None,
    description="Hybrid AWS benchmark wrapper for fixed NYC TLC pipeline runs",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
)
def taxi_benchmark_pipeline_hybrid_aws():
    stage_partition = SparkSubmitOperator(
        task_id="stage_trips",
        application="/opt/lakehouse/src/etl/jobs/nyc_tlc_stg_trip_data.py",
        conn_id="spark",
        conf=HYBRID_AWS_SPARK_CONF,
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
            "--silver-namespace",
            conf_value("silver_namespace"),
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
        conf=HYBRID_AWS_SPARK_CONF,
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
            "--silver-namespace",
            conf_value("silver_namespace"),
            "--quality-namespace",
            conf_value("quality_namespace"),
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
        conf=HYBRID_AWS_SPARK_CONF,
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
            "--silver-namespace",
            conf_value("silver_namespace"),
            "--gold-namespace",
            conf_value("gold_namespace"),
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


taxi_benchmark_pipeline_hybrid_aws()
