#!/bin/bash
spark-submit --master spark://ip-172-31-0-36:7077 \
--driver-memory 900M \
--executor-memory 900M \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 \
~/insight/spark_streaming/dashboardTrend.py localhost:2181 vectors
