from __future__ import print_function

import sys, json

from pyspark.sql import SQLContext, Row
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
from numpy.linalg import norm
import datetime 
from dateutil import parser

import hashlib,json
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

KAFKA_NODE="52.2.60.169:9092"
#KAFKA_TOPIC="TextEvents"
KAFKA_TOPIC="events"
client = SimpleClient(KAFKA_NODE)
producer = KeyedProducer(client)

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def roundTime(dt=None, roundTo=60):
    """Round a datetime object to any time laps in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    """
    if dt == None : dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds
    # // is a floor division, not a comment on following line:
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def getDtSeconds(dt1, dt0=None):
    """Get time different in seconds between two datetime objects
    INput:
    dt1: ending datetime.datetime object
    dt0: starting datetime.datetime object
    Output: difference in seconds
    """
    if dt0==None: dt0=datetime.datetime.utcfromtimestamp(0)
    return (dt1-dt0).total_seconds()

def isInLastSecond(dt1, dt0):
    tmp=getDtSeconds(dt1, dt0)
    """ 0<dt1-dt0<1"""
    return ((tmp<1) and (tmp>0))

def process(time, lines):
    """Calculate user-product corr table and select ad-push events
    Input:
    lines: (ts string, uid string, topic vector)
    Output:
    Json: (timeStamp, uid, {pid:score} dict)
    """
    print("========= %s =========" % str(time))
    sqlContext=getSqlContextInstance(lines.context)
#   calculate user-product correlation table  
    runningWindow=lines.map(lambda (k, v): ( (k[1], time.isoformat()), v ))\
         .reduceByKey(lambda x,y: x+y)\
	 .map(lambda (x,u): (x, [(pid, score) for (pid, score) in ( (y, float(u.dot(v)/(norm(u)*norm(v)))) for (y,v) in bv.value ) if score>.90]))\
	 .filter(lambda (k, v): v!=[])
    rowRDD=runningWindow.map(lambda x:Row(uid=x[0][0],score=x[1],ts=x[0][1])) 
#   print(rowRDD.take(10))
    print("========= %d =========" % rowRDD.count())
#   save corr table to cassandra 
    if (rowRDD.count()>0):
        for row in rowRDD.collect():
	    line = '{ "timeStamp" :"' + str(time) + '",'
            line+= '  "uid"  :"' + str(row['uid']) + '",'
            line+= ' "score":' + json.dumps(dict(row['score']))+ '}'
#	    print(line)
	    producer.send_messages(KAFKA_TOPIC, str(hash(line)), line)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setAppName("eventProcessingStream").set("spark.cores.max", "3")
    sc = SparkContext(conf=conf)
    #sc = SparkContext(appName='eventProcessingStream')
    ssc = StreamingContext(sc, 10)
   
#   load product vector 
    pm=sc.textFile('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/data/pidModel.txt')\
         .map(lambda x: x.strip().split(' '))\
         .map(lambda x: (x[0], np.asarray([float(i) for i in x[1:]])))

#    print(pm.take(100))
    bv=sc.broadcast(pm.collect())

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "sparkStreamingConsumerDetectEvents", {topic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
#    lines.pprint()
    uidVec=lines.map(lambda x: ((x['timestamp'], x['uid']), np.asarray([float(i) for i in x['topicVec']])))
#    			.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 10)
#    ssc.checkpoint('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/checkpoint/') 
    uidVec.pprint()
    window60rdd=uidVec.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
