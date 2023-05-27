import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

SPARK_DIR = "/opt/spark/data"
MINIO_ENDPOINT = "http://localhost:9000"
BUCKET_NAME = "rendimentos-landing"
BUCKET_KEY = ""
SPARK_CONNECT_ENDPOINT = "sc://localhost"
LOCAL_DIR = "data"
SCHEMA_PATH = Path(LOCAL_DIR) / "rendimentos-schema-full.json"


spark: SparkSession = (
    SparkSession.builder.appName("Spark Connect Test").remote(
        SPARK_CONNECT_ENDPOINT
    )  # type: ignore
    # .config(
    #     "spark.hadoop.fs.s3a.aws.credentials.provider",
    #     "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider",
    # )
    # .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    # .config("spark.hadoop.fs.s3a.path.style.access", "true")
    # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

df: DataFrame = (
    spark.read.format("com.databricks.spark.xml")
    .options(rowTag="DadosEconomicoFinanceiros")
    .load("s3a://rendimentos-landing/teste/6960.xml")
    # .load(f"{SPARK_DIR}/sample")
)

df.show()

# df.write.parquet(path=f"s3a://{BUCKET_NAME}/teste")

print("Oi")

# # Salvar o Schema do Dataframe
# schema_json = df.schema.json()
# SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
# SCHEMA_PATH.write_text(schema_json)


# # Ler os dados em parquet
# json_schema = json.loads(SCHEMA_PATH.read_bytes())
# schema = StructType.fromJson(json_schema)

# df_sample: DataFrame = (
#     spark.read.format("parquet").schema(schema).load(f"s3a://{BUCKET_NAME}/rendimentos/")
# )
# df_sample.show()
# df_sample.printSchema()
