#!/bin/bash

# Start the Spark Master
/opt/spark/sbin/start-master.sh &

# Wait until the Spark Master is ready
echo "Waiting for Spark Master to be ready..."
while ! curl -sSf "http://master:8080" >/dev/null; do sleep 1; done

echo "Spark Master is ready. Starting Spark Connect..."

# Start Spark Connect
/opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.2.0,com.databricks:spark-xml_2.12:0.16.0 --conf spark.executor.memory=8g --conf spark.executor.cores=3 --num-executors 4
