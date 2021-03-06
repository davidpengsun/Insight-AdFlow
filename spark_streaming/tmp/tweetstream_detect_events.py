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

KAFKA_NODE="52.2.60.169:9092"
#KAFKA_TOPIC="TextEvents"
KAFKA_TOPIC="detected_events"

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
    """1. select user to push ads
       2. save user-product corr table to cassandra
       3. match user with bidder
       4. save bid winner to cassandra 
    Input:
    lines: (ts string, uid string, topic vector)
    """
    print("========= %s =========" % str(time))
    sqlContext=getSqlContextInstance(lines.context)
    # calculate user-product correlation table  
    #lines1s=lines.map(lambda x: ( (x['uid'], roundTime(parser.parse(x['tick']),1).isoformat()), np.asarray([1]+[float(i) for i in x['topic']])))\
#    lines1s=lines.map(lambda x: ( x[0], 1))\
    runningWindow=lines.map(lambda (k, v): ( (k[0], time.isoformat()), v ))\
         .reduceByKey(lambda x,y: x+y)\
	 .map(lambda (x,u): [(x, y, float(u.dot(v)/(norm(u)*norm(v)))) for (y,v) in bv.value])\
 	 .flatMap(lambda x:x)\
	 .filter(lambda (x,y,s):s>.97)
    rowRDD=runningWindow.map(lambda x:Row(uid=x[0][0],pid=x[1],score=x[2],ts=x[0][1])) 
#    print(rowRDD.take(10))
#    saveRDD(sqlContext, rowRDD, keyspaceName='ad_flow', tableName='records1s')
    print("========= %d =========" % rowRDD.count())
    # save corr table to cassandra 
    if (rowRDD.count()>0):
	client = SimpleClient(KAFKA_NODE)
	producer = KeyedProducer(client)
        for row in rowRDD.collect():
	    line = '{ "tick" :"' + str(time.isoformat()) + '",'
            line+= '  "uid"  :"' + str(row['uid']) + '",'
            line+= ' "score":"' + str(row['score'])+'",'
            line+= '  "pid":' + str(row['pid'])+ '}'
#	    print(line)
	    producer.send_messages(KAFKA_TOPIC, str(hash(line)), line)
	    #line=
	    #producer.send_messages(KAFKA_TOPIC, str(hash(line)), line)
#        output=sqlContext.createDataFrame(rowRDD)
#        output.write\
#        	.format("org.apache.spark.sql.cassandra")\
#        	.options(table='records1s', keyspace='ad_flow')\
#        	.save(mode="append")

    # select events to proceed
#    events=lines1s.filter(lambda (x,y,s):s<.3)

    # match biddings with users 
    # read bidder table from cassandra
    


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="biddingStream")
    ssc = StreamingContext(sc, 10)
   
#   load product vector 
    pm=sc.textFile('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/data/pidmodel.csv')\
     .map(lambda x: x.split(','))\
     .map(lambda x: (x[0], np.asarray([float(i) for i in x[2:]])))
    
    pm.count()
    bv=sc.broadcast(pm.collect())

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
#    lines.pprint()
    uidVec=lines.map(lambda x: ((x['uid'], x['tick']), np.asarray([float(i) for i in x['topic']])))\
    			.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 10)
    ssc.checkpoint('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/checkpoint/') 
    window60rdd=uidVec.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
