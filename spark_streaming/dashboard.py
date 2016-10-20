from __future__ import print_function

import sys, json

from pyspark.sql import SQLContext, Row 
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import numpy as np
from numpy.linalg import norm
import datetime 
from dateutil import parser

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection


#   connect to cassandra
cluster = Cluster(['ec2-52-54-224-184.compute-1.amazonaws.com'])
session = cluster.connect("ad_flow") 
pidScore = session.prepare("INSERT INTO pidscore1h (pid, ts, score) VALUES (?,?,?) USING TTL 3600")

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
    """ Sending bids to cassandra
    Input:
        lines: (ts string, uid string, topic vector)
    Output: to cassandra
    """
    print("========= %s =========" % str(time))
    sqlContext=getSqlContextInstance(lines.context)
#   calculate user-product correlation table  
    runningWindow=lines.map(lambda v: ( time, v ))\
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda (x,u): [(pid, x, score) for (pid, score) in ( (y, float(u.dot(v)/(norm(u)*norm(v)))) for (y,v) in bv.value )])\
            .flatMap(lambda x:x)
    rowRDD=runningWindow.map(lambda x:Row(pid=x[0],ts=x[1],score=x[2]))
    #   print(rowRDD.take(10))
    print("========= %d =========" % rowRDD.count())
#   save to cassandra
    for row in rowRDD.collect():
        session.execute(pidScore, (row.pid, row.ts, row.score))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <incomingTopic>", file=sys.stderr)
        exit(-1)
#   prepare stream
    conf = SparkConf().setAppName("dashboardScore").set("spark.cores.max", "2")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)

#   load product vector
    pm=sc.textFile('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/data/pidModel.txt')\
         .map(lambda x: x.strip().split(' '))\
         .map(lambda x: (x[0], np.asarray([float(i) for i in x[1:]])))
#   print(pm.take(100))
    bv=sc.broadcast(pm.collect())

    zkQuorum, incomingTopic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "sparkStreamingDashBoard", {incomingTopic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
#    lines.pprint()
    uidVec=lines.map(lambda x: np.asarray([float(i) for i in x['topicVec']]))
#    ssc.checkpoint('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/checkpoint/') 
    uidVec.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
