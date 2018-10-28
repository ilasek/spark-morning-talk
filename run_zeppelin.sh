#!/usr/bin/env bash
# Starts Apache Zeppelin notebook on port 8080 using Docker
docker run -it --rm -p 7077:7077 -p 8080:8080 --privileged=true  \
-v $PWD/data:/data -v $PWD/logs:/logs -v $PWD/zeppelin-notebook:/notebook \
-e ZEPPELIN_NOTEBOOK_DIR='/notebook' \
-e ZEPPELIN_LOG_DIR='/logs' \
-e SPARK_SUBMIT_OPTIONS='--jars /zeppelin/interpreter/psql/postgresql-9.4-1201-jdbc41.jar' \
apache/zeppelin:0.7.3