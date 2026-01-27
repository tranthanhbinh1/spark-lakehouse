from typing import Any, cast
from pyspark.sql import SparkSession


def log_jvm_heap(spark: SparkSession) -> None:
    jvm = cast(Any, spark._jvm)
    runtime = jvm.java.lang.Runtime.getRuntime()
    mb = 1024 * 1024
    max_mb = runtime.maxMemory() / mb
    total_mb = runtime.totalMemory() / mb
    free_mb = runtime.freeMemory() / mb
    conf = spark.sparkContext.getConf()
    executor_mem = conf.get("spark.executor.memory", "unset")
    driver_mem = conf.get("spark.driver.memory", "unset")
    logger = jvm.org.apache.log4j.LogManager.getLogger("nyc-tlc-stg")
    logger.info(
        f"JVM heap (driver): max={max_mb:.1f}MiB total={total_mb:.1f}MiB free={free_mb:.1f}MiB"
    )
    logger.info(f"Spark memory conf: executor={executor_mem} driver={driver_mem}")
