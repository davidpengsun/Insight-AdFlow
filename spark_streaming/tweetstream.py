from __future__ import print_function

import sys, json

from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
from numpy.linalg import norm



def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def process(time, lines):
    print("========= %s =========" % str(time))
    sqlContext=getSqlContextInstance(lines.context)
    tmp=lines.map(lambda x: (x['uid'], np.asarray([1]+[float(i) for i in x['topic']])))\
                .reduceByKey(lambda x,y: x+y)\
  		.map(lambda (x,y): (x, y[1:]/y[0]))\
		.map(lambda (x,u): [(x, y, float(u.dot(v)/(norm(u)*norm(v)))) for (y,v) in bv.value])\
		.flatMap(lambda x:x)
#    tmp.take(1)
#    tmp=lines
    
    rowRDD=tmp.map(lambda x:Row(uid=x[0],pid=x[1],score=x[2])) 
    print(rowRDD.count())
    if(rowRDD.isEmpty==False):
        output=sqlContext.createDataFrame(rowRDD)
        output.write\
      		.format("org.apache.spark.sql.cassandra")\
      		.options(table="runningwindow10s", keyspace="ad_flow")\
      		.save(mode="overwrite")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="sparktestapp")
    ssc = StreamingContext(sc, 10)
    
    pm=sc.textFile('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/data/pidmodel.csv')\
     .map(lambda x: x.split(','))\
     .map(lambda x: (x[0], np.asarray([float(i) for i in x[2:]])))
    
    pm.count()
    bv=sc.broadcast(pm.collect())

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
    lines.pprint()
#    pairs=lines.map(lambda x: ((x['uid'], x['tick']), x['topic']))
#    uidVec=lines.map(lambda x: (x['uid'], np.asarray([1]+[float(i) for i in x['topic']])))\
#	                .reduceByKey(lambda x,y: x+y)\
#   			.map(lambda (x,y): (x, y[1:]/y[0]))\
#			.map(lambda (x,u): [(x, y, float(u.dot(v)/(norm(u)*norm(v)))) for (y,v) in bv.value])\
#			.flatMap(lambda x:x)
#    			.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 10, 1)\
#    			.reduceByKeyAndWindow(lambda x, y: x + y, 10, 1)
    lines.foreachRDD(process)
#    ssc.checkpoint('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/checkpoint/') 
    ssc.start()
    ssc.awaitTermination()
