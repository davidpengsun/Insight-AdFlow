#!/bin/bash
spark-submit --master spark://ip-172-31-0-36:7077 \
--driver-memory 2G \
--executor-memory 2G \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 \
--conf spark.cassandra.connection.host=172.31.0.36 \
~/insight/spark_streaming/processTweets.py localhost:2181 tweets 52.2.60.169:9092 vectors
