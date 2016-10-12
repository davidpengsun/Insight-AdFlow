!/bin/bash


/usr/local/spark/bin/spark-submit --master spark://ip-172-31-0-36:7077 \
--driver-memory 4G \
--executor-memory 4G \
--packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 \
/home/ubuntu/insight/spark_batch/balance.py
