nohup bash insight/kafka/produceTweets.sh &
nohup bash insight/spark_stream/processTweets.sh &
nohup bash insight/spark_stream/processEvents.sh &
nohup bash insight/spark-stream/processBids.sh &
nohup bash insight/spark_stream/matchBids.sh &
nohup bash insight/spark_stream/dashboardTrend.sh &
