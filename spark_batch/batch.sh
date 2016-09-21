#!/bin/bash


/usr/local/spark/bin/spark-submit --packages datastax:spark-cassandra-connector:1.6.0-s_2.10 --conf spark.cassandra.connection.host=172.31.0.40 /home/ubuntu/insight/spark_batch/batch.py
#/usr/local/spark/bin/spark-submit --packages datastax:spark-cassandra-connector:1.6.0-s_2.10\
#				  --conf spark.cassandra.connection.host=172.31.0.40\
#				  --master spark://ip-172-31-0-40:7077\
#				  --driver-memory 4G\
#				  --executor-memory 4G\
#				  /home/ubuntu/insight/spark_batch/batch.py # &\
#				 > /usr/local/insight/spark_batch/log.txt

#fpath=hdfs://ec2-52-45-61-78.compute-1.amazonaws.com:9000/camus/topics/textticks/hourly
#for file in `hdfs dfs -lsr $fpath | grep gz`; do
#/usr/local/spark/bin/spark-submit --master spark://ip-172-31-0-40:7077 --driver-memory 2G --executor-memory 2G ~/insight/spark_batch/batch.py $file
#done
#for year in `hdfs dfs -ls /camus/topics/textticks/hourly/ | awk '{print $8}' | sed "s#/camus/topics/textticks/hourly/##"`; do
#  for month in `hdfs dfs -ls /camus/topics/textticks/hourly/$year | awk '{print $8}' | sed "s#/camus/topics/textticks/hourly/$year/##"`; do
#    for day in `hdfs dfs -ls /camus/topics/textticks/hourly/$year/$month | awk '{print $8}' | sed "s#/camus/topics/textticks/hourly/$year/$month/##"`; do
#      echo $year/$month/$day
#    done
#  done
#done
#for year in `hdfs dfs -ls /camus/topics/textticks/hourly/ | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/##"`; do
#	for month in `hdfs dfs -ls /camus/topics/trades/hourly/$year | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/$year/##"`; do
#		for day in `hdfs dfs -ls /camus/topics/trades/hourly/$year/$month | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/$year/$month/##"`; do
#			for hour in `hdfs dfs -ls /camus/topics/trades/hourly/$year/$month/$day | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/$year/$month/$day/##"`; do
#				for file in `hdfs dfs -ls /camus/topics/trades/hourly/$year/$month/$day/$hour | awk '{print $8}'`; do
#					echo $file
#				done
#			done
#		done
#	done
#done
