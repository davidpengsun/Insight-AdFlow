from __future__ import print_function

import sys, json

from pyspark.sql import SQLContext, Row
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection


#   connect to cassandra
cluster = Cluster(['172.31.0.36'])
session = cluster.connect("ad_flow") 
bidPrice = session.prepare("INSERT INTO bidprice (pid, price) VALUES (?,?)")

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def process(time, lines):
    """ Sending bids to cassandra
    Input:
    lines: (pid string, price string)
    Output: to cassandra
    """
    print("========= %s =========" % str(time))
    sqlContext=getSqlContextInstance(lines.context)
#    rowRDD=lines.map(lambda x: (x['timeStamp'], x['userId'], getMeanVector(x['tweet'])))\
#                .filter(lambda (time, uid, vec): vec!=[])\
#                .map(lambda x:Row(timestamp=x[0], uid=x[1], topicVec=x[2]))
    rowRDD=lines.map(lambda x: (x['productId'],  x['bidPrice']))\
                .map(lambda x:Row(pid=x[0], bid=x[1]))
    print(rowRDD.take(10))
#   save to cassandra
    for row in rowRDD.collect():
        session.execute(bidPrice, (row.pid, row.bid, ))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <incomingTopic>", file=sys.stderr)
        exit(-1)
#   prepare stream
    conf = SparkConf().setAppName("bidProcessingStream").set("spark.cores.max", "1")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
   
    zkQuorum, incomingTopic = sys.argv[1:]

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "sparkStreamingProcessBids", {incomingTopic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
    lines.pprint()
#    ssc.checkpoint('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/checkpoint/') 
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
