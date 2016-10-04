from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
import sys, datetime
import numpy as np
from numpy.linalg import norm
from dateutil import parser
from dateutil.parser import parse
from pyspark.sql.functions import *

def roundTime(dt=None, roundTo=1):
    """Round a datetime object to any time laps in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    """
    if dt == None : dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds
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

def getTimeString(s):
    return roundTime(parse(s), 600).isoformat()

def getState(s):
    return np.random.randint(0,50)

if __name__=="__main__":
    conf = SparkConf().setAppName("userTextBatch").set("spark.cores.max", "8")
    sc = SparkContext(conf=conf) 
    sqlContext = SQLContext(sc) 

#   load product vector
    pm=sc.textFile('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/data/pidmodel.csv')\
	 .map(lambda x: x.split(','))\
	 .map(lambda x: (x[0], np.asarray([float(i) for i in x[2:]])))

    pm.count()
    bv=sc.broadcast(pm.collect())
#   load user text files
#    files="hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/camus/topics/textticks/hourly/*/*/*/*/*"
    files="hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/camus/topics/textticks/hourly/2016/09/29/15/textticks.0.3.27298.721182.1475186400000.gz"
    json_format = StructType([StructField('tick', StringType(), True), 
	  StructField('topic', ArrayType(DoubleType()), True),
	  StructField('uid', StringType(), True),
	  StructField('weight', StringType(), True)])
    lines=sqlContext.read.json(files, json_format)
#   trim dataFrame
    udfParse=udf(getTimeString, StringType())
    linesDF=lines.withColumn('ts',udfParse('tick')).select('ts','uid','topic','weight')
#   1m granularity
    lines10m=linesDF.rdd\
		   .map(lambda (ts, uid, topic, state): ( (ts,getState(state)), np.asarray([1]+[float(i) for i in topic]) ))\
		   .reduceByKey(lambda x,y: x+y)\
		   .map(lambda (x,y): (x, y[1:]/y[0]))\
		   .map(lambda ((ts,state),u): [(ts, state, y, float(u.dot(v)/(norm(u)*norm(v)))) for (y,v) in bv.value])\
		   .flatMap(lambda x:x)

    lines10m.map(lambda (ts, state, pid, score): Row(pid=pid, state=state, score=score, ts=ts))\
    	   .toDF()\
    	   .write\
	   .format("org.apache.spark.sql.cassandra")\
	   .mode('overwrite')\
	   .options(table="history10m", keyspace="ad_flow")\
	   .save()
