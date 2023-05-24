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

# Iniciar o Spark Connect com a string de pacotes
/opt/spark/sbin/start-connect-server.sh --packages "$packages_string" --conf spark.executor.memory=8g --conf spark.executor.cores=3 --num-executors 4

