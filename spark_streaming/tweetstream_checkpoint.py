from __future__ import print_function

import sys, json, os

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


def createContext(zkQuorum, topic):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName="PythonStreamingRecoverableNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
    #lines.pprint()
    # hard one
    #pairs=lines.map(lambda x: (x['uid'],x['topic']))
    # simple one
    pairs=lines.map(lambda x: (x['uid'], 1))
    #pairs.pprint()

    windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
    windowedWordCounts.pprint()
    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: recoverable_network_wordcount.py <hostname> <port> "
              "<checkpoint-directory>", file=sys.stderr)
        exit(-1)

    host, topic, checkpoint = sys.argv[1:]
    ssc = StreamingContext.getOrCreate(checkpoint,
                                       lambda: createContext(host, topic))
    #lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
