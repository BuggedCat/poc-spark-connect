import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

SPARK_DIR = "/opt/spark/data"
MINIO_ENDPOINT = "http://localhost:9000"
SPARK_CONNECT_ENDPOINT = "sc://localhost"
LOCAL_DIR = "data"
SCHEMA_PATH = Path(LOCAL_DIR) / "rendimentos-schema-full.json"


spark: SparkSession = (
    SparkSession.builder.appName("Spark Connect Test")  # type: ignore
    .remote(SPARK_CONNECT_ENDPOINT)
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

df: DataFrame = (
    spark.read.format("com.databricks.spark.xml")
    .options(rowTag="DadosEconomicoFinanceiros")
    .load(f"{SPARK_DIR}/rendimentos_amortizacoes")
)

df.repartition(4).write.parquet(
    path="s3a://spark-connect-landing/rendimentos/",
    compression="snappy",
    mode="overwrite",
)


# Salvar o Schema do Dataframe
schema_json = df.schema.json()
SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
SCHEMA_PATH.write_text(schema_json)


# Ler os dados em parquet
json_schema = json.loads(SCHEMA_PATH.read_bytes())
schema = StructType.fromJson(json_schema)

df: DataFrame = (
    spark.read.format("parquet")
    .schema(schema)
    .load("s3a://spark-connect-landing/rendimentos/")
)
df.show()
df.printSchema()
