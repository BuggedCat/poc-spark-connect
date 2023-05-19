import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

LOCAL_DIR = "data"
SPARK_DIR = "/opt/spark/data"
SCHEMA_PATH = Path(LOCAL_DIR) / "rendimentos-schema.json"

spark: SparkSession = (
    SparkSession.builder.appName("Spark Connect Test")  # type: ignore
    .remote("sc://localhost")  # o mesmo que sc://localhost:15002
    .getOrCreate()
)

# Ler os dados em XML
df: DataFrame = (
    spark.read.format("com.databricks.spark.xml")
    .options(rowTag="DadosEconomicoFinanceiros")
    .load(f"{SPARK_DIR}/sample")
)

# Salvar o Schema do Dataframe
schema_json = df.schema.json()
SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
SCHEMA_PATH.write_text(schema_json)

# Escrever os dados no formato parquet
df.repartition(4).write.parquet(
    path=f"{SPARK_DIR}/rendimentos",
    compression="snappy",
    mode="overwrite",
)

# Ler os dados em parquet
json_schema = json.loads(SCHEMA_PATH.read_bytes())
schema = StructType.fromJson(json_schema)

df: DataFrame = spark.read.format("parquet").schema(schema).load(f"{SPARK_DIR}/rendimentos")
df.show()
df.printSchema()
