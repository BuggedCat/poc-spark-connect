#!/bin/bash

# Definir pacotes como uma lista
packages=(
  "org.apache.spark:spark-connect_2.12:3.4.0"
  "org.apache.hadoop:hadoop-aws:3.3.4"
  "org.apache.hadoop:hadoop-common:3.3.4"
  "com.databricks:spark-xml_2.12:0.16.0"
)

# Converter a lista de pacotes em uma string separada por vírgulas
packages_string=$(IFS=,; echo "${packages[*]}")

# Iniciar o Spark Master
/opt/spark/sbin/start-master.sh &

# Aguardar até que o Spark Master esteja pronto
echo "Waiting for Spark Master to be ready..."
while ! curl -sSf "http://master:8080" >/dev/null; do sleep 1; done

echo "Spark Master is ready. Starting Spark Connect..."

/opt/spark/sbin/start-connect-server.sh --packages "$packages_string"  \
    --conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
    --conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
    --conf "spark.hadoop.fs.s3a.endpoint=$AWS_ENDPOINT" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf spark.executor.memory=$SPARK_CONNECT_EXECUTORS_MEMORY \
    --conf spark.executor.cores=$SPARK_CONNECT_EXECUTORS_CORE \
    --num-executors $SPARK_CONNECT_NUM_EXECUTORS
