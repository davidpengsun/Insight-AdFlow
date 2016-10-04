from __future__ import print_function

import sys, json

from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
from numpy.linalg import norm
import datetime 
from dateutil import parser

import hashlib
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection

KAFKA_NODE="52.2.60.169:9092"
KAFKA_TOPIC="winningBids"

#   connect to cassandra
cluster = Cluster(['172.31.0.36'])
session = cluster.connect("ad_flow") 
bidPrice = session.prepare("INSERT INTO winningbid10s (pid, ts, price) VALUES (?,?,?)")

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
    """Get time difference in seconds between two datetime objects
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

def getBidPrice(pid):
    return bidPriceBC.value[pid]

def matchBids(userProductScore):
    key0,score0,price0='',0,0
    for key, value in userProductScore.iteritems():
        tmp = value*getBidPrice(key)
        if(tmp>score0):
            key0,score0,price0=key,tmp,value
    return key0, price0


def process(time, lines):
    """match user with bidder
    Input:
    lines: (ts string, uid string, {pid:score} dict)
    """
    print("========= %s =========" % str(time))
    sqlContext=getSqlContextInstance(lines.context)
    # calculate user-product correlation table  
    rowRDD=lines.map(lambda x: ( x['uid'], matchBids(x['score']) ))\
                .map(lambda x:Row(uid=x[0],pid=x[1][0],price=x[1][1])) 
    print("========= %d =========" % rowRDD.count())
    if (rowRDD.count()>0):
#   send to kafka
	client = SimpleClient(KAFKA_NODE)
	producer = KeyedProducer(client)
        for row in rowRDD.collect():
	    line = '{ "pid" :"' + str(row['pid']) + '",'
            line+= '  "uid"  :"' + str(row['uid']) + '",'
            line+= ' "price":"' + str(row['price'])+'",'
            line+= '  "ts":' + str(time)+ '}'
#	    print(line)
	    producer.send_messages(KAFKA_TOPIC, str(hash(line)), line)
#   save to cassandra
        rowRDD.map(lambda x:Row(pid=x['pid'],ts=str(time),price=x['price']))\
              .toDF().write\
              .format("org.apache.spark.sql.cassandra")\
              .options(table='winningbid10s', keyspace='ad_flow')\
              .save(mode="append")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <EventsTopic> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="biddingStream")
    ssc = StreamingContext(sc, 10)
    sqlContext=SQLContext(sc)
    bidprice=sqlContext.read\
                       .format("org.apache.spark.sql.cassandra")\
                       .options(keyspace="ad_flow", table="bidprice")\
                       .load().rdd
    tmp={}
    for item in bidprice.collect():
        tmp[item['pid']]=item['price']
    bidPriceBC=sc.broadcast(tmp)
#    print(tmp)
    zkQuorum, topic1 = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "sparkStreamingGetNewEvents", {topic1: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
#    lines.pprint()
#    uidVec=lines.map(lambda x: ((x['uid'], x['tick']), np.asarray([float(i) for i in x['topic']])))\
#    uidVec=lines.map(lambda x: ((x['uid'], x['tick']), np.asarray([float(i) for i in x['topic']])))\
#    			.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 10)
#    ssc.checkpoint('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/checkpoint/') 
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
