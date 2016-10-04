#!/bin/bash
spark-submit --master spark://ip-172-31-0-36:7077 \
--driver-memory 4G \
--executor-memory 4G \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 \
--conf spark.cassandra.connection.host=172.31.0.36 \
~/insight/spark_streaming/tweetstream.py localhost:2181 textticks
#--jars /home/ubuntu/lib/spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar \
#--packages datastax:spark-cassandra-connector:1.6.0-s_2.10 \
#--packages datastax:spark-cassandra-connector:1.6.0-s_2.10, org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 \
#--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 \
#--conf spark.cassandra.connection.host=172.31.0.40 \
