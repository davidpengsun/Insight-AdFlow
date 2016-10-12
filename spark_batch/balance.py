from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SQLContext, Row 
from pyspark.sql.types import *
import sys, datetime
import numpy as np
from dateutil.parser import parse
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection


#   connect to cassandra
cluster = Cluster(['ec2-52-54-224-184.compute-1.amazonaws.com'])
session = cluster.connect("ad_flow") 
userprofile = session.prepare("select * from userprofile")
winningbids = session.prepare("select * from winningbids")
history1h = session.prepare("INSERT INTO history1h (pid, state, ts, cost, count) VALUES (?,?,?,?,?)") 
dashboard = session.prepare("INSERT INTO dashboardmap (pid, state, cost, count) VALUES (?,?,?,?)")
revenuemap = session.prepare("INSERT INTO revenuemap (state, cost, count) VALUES (?,?,?)")
revenueflow= session.prepare("INSERT INTO revenueflow (ts, cost, count) VALUES (?,?,?)")

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
    return roundTime(s, 3600).isoformat()

def word2vec(word):
    return np.asarray(bv.value[word])

def isInVolcabulary(word):
    return (word in bv.value)

def getMeanVector(words):
    tmp=np.mean([word2vec(item) for item in words if isInVolcabulary(item)], 0)
    return [float(i) for i in tmp]

if __name__=="__main__":
    conf = SparkConf().setAppName("userTextBatch").set("spark.cores.max", "25")
    sc = SparkContext(conf=conf) 
    sqlContext = SQLContext(sc) 

#   load user profile
    uidstate=session.execute(userprofile)
#   load bids data
    state={}
    for i in uidstate:
        state[i.uid]=i.state
    winningbids=session.execute(winningbids)
    df=sqlContext.createDataFrame(winningbids)
#   trim dataFrame
    udfParse=udf(getTimeString, StringType())
    udfState=udf(lambda x:state[x], StringType())
    df1h=df.withColumn('ts',udfParse('ts')).select('pid','uid','ts','price').withColumn('state',udfState('uid')).select('pid','uid','state','ts','price')
    historyRDD=df1h.rdd.map(lambda x:( (x.pid,x.state,x.ts), np.asarray([1, x.price])))\
                   .reduceByKey(lambda x,y:x+y)\
                   .map(lambda (key,value):Row(pid=key[0], state=key[1], ts=parse(key[2]), cnt=float(value[0]), cost=float(value[1])))
    historyRDD.cache()
    outDF=sqlContext.createDataFrame(historyRDD)
    for row in outDF.collect():
        session.execute(history1h, (row.pid, row.state, row.ts, row.cost, row.cnt, ))
    dashboardRDD=historyRDD.map(lambda x:( (x.pid, x.state), np.asarray([x.cnt, x.cost]) ))\
                           .reduceByKey(lambda x,y:x+y)\
                           .map(lambda (key, value): Row(pid=key[0], state=key[1], cnt=float(value[0]), cost=float(value[1])))
    dashboardRDD.cache()
    outDF=sqlContext.createDataFrame(dashboardRDD)
    for row in outDF.collect():
        session.execute(dashboard, (row.pid, row.state, row.cost, row.cnt, ))
    revenuemapRDD=dashboardRDD.map(lambda x:( x.state, np.asarray([x.cnt, x.cost]) ))\
                           .reduceByKey(lambda x,y:x+y)\
                           .map(lambda (key, value): Row(state=key, cnt=float(value[0]), cost=float(value[1])))
    outDF=sqlContext.createDataFrame(revenuemapRDD)
    for row in outDF.collect():
        session.execute(revenuemap, (row.state, row.cost, row.cnt, ))
    revenueflowRDD=historyRDD.map(lambda x:( x.ts, np.asarray([x.cnt, x.cost]) ))\
                             .reduceByKey(lambda x,y:x+y)\
                             .map(lambda (key, value): Row(ts=key, cnt=float(value[0]), cost=float(value[1])))
    outDF=sqlContext.createDataFrame(revenueflowRDD)
    for row in outDF.collect():
        session.execute(revenueflow, (row.ts, row.cost, row.cnt, ))
