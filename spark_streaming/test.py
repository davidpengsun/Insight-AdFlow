from __future__ import print_function

import sys, json

from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def process(lines):
    tmp=lines.map(lambda x: x['topic'])\
                .flatMap(lambda x: x)\
                .filter(lambda x: x != '')\
                .map(lambda x:(x,1))\
                .reduceByKey(lambda x,y: x+y)

    sqlContext=getSqlContextInstance(tmp.context)
    rowRDD=tmp.map(lambda x:Row(topic=x[0],count=x[1]))
    output=sqlContext.createDataFrame(rowRDD)
    output.write\
  	.format("org.apache.spark.sql.cassandra")\
  	.options(table="topics_total_count_batch", keyspace="ad_flow")\
  	.save(mode="overwrite")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="sparktestapp")
    ssc = StreamingContext(sc, 10)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
    lines.pprint()
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
