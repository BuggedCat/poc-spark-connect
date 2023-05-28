/opt/spark/bin/pyspark --packages "org.apache.spark:spark-connect_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,com.databricks:spark-xml_2.12:0.16.0"  \
    --conf spark.executor.memory=8g \
    --conf spark.executor.cores=3 \
    --conf "spark.hadoop.fs.s3a.access.key=minio" \
    --conf "spark.hadoop.fs.s3a.secret.key=p@ssw0rd" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --num-executors 4


df = spark.read.format("com.databricks.spark.xml").options(rowTag="DadosEconomicoFinanceiros").load("s3a://rendimentos-landing/teste/6960.xml")

df.show()


df.write.parquet(path=f"s3a://rendimentos-landing/teste-parquet")