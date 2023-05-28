from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from settings import SPARK_DATA_DIR, DatalakeZones

SPARK_CONNECT_ENDPOINT = "sc://localhost"
SAMPLE_FILES_DIR = SPARK_DATA_DIR / "sample"

spark: SparkSession = (
    SparkSession.builder.appName("Spark Connect Test")  # type: ignore
    .remote(SPARK_CONNECT_ENDPOINT)
    .getOrCreate()
)

df: DataFrame = (
    spark.read.format("com.databricks.spark.xml")
    .options(rowTag="DadosEconomicoFinanceiros")
    .load(SAMPLE_FILES_DIR.as_posix())
)

df.show()

df.repartition(4).write.parquet(
    path=f"{DatalakeZones.BRONZE.to_s3_uri}/sample",
    mode="overwrite",
)
