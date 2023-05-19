FROM apache/spark-py:v3.4.0

ENV SPARK_NO_DAEMONIZE=true

USER root

RUN mkdir -p /opt/spark/logs 
RUN mkdir -p /opt/spark/work 
RUN mkdir -p /opt/spark/.ivy2/ 
RUN mkdir -p /opt/spark/data/

RUN chmod -R a+wr /opt/spark/

# && chmod a+wr /opt/spark/logs
# && chmod a+wr /opt/spark/work
# && chmod a+wr /opt/spark/.ivy2/
# && chmod a+wr /opt/spark/data/

COPY ./requirements.txt /opt/spark/requirements.txt

RUN pip install -r /opt/spark/requirements.txt

COPY ./entrypoint.sh /opt/spark/entrypoint.sh
