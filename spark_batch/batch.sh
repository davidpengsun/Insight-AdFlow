#!/bin/bash


/usr/local/spark/bin/spark-submit --master spark://ip-172-31-0-36:7077 \
--driver-memory 8G \
--executor-memory 8G \
--packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 \
--conf spark.cassandra.connection.host=172.31.0.36 \
/home/ubuntu/insight/spark_batch/batch.py
