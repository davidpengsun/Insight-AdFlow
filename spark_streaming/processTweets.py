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
from gensim.models import Word2Vec 
import hashlib,json
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def word2vec(word):
    return np.asarray(volcabularyBC.value[word])

def isInVolcabulary(word):
    return (word in volcabularyBC.value)

def getMeanVector(words):
    tmp=np.mean([word2vec(item) for item in words if isInVolcabulary(item)], 0)
    return [float(i) for i in tmp]

def process(time, lines):
    """ Processing tweets 
    Input:
    lines: (ts string, uid string, state string, tweet vector)
    Output:
    Json: (ts string, uid string, topicVec vector)
    """
    print("========= %s =========" % str(time))
    sqlContext=getSqlContextInstance(lines.context)
#    rowRDD=lines.map(lambda x: (x['timeStamp'], x['userId'], getMeanVector(x['tweet'])))\
#                .filter(lambda (time, uid, vec): vec!=[])\
#                .map(lambda x:Row(timestamp=x[0], uid=x[1], topicVec=x[2]))
    rowRDD=lines.map(lambda x: [((x['timeStamp'], x['userId']), word2vec(item)) for item in x['tweet'] if isInVolcabulary(item)] )\
                .flatMap(lambda x:x)\
                .filter(lambda (k, vec): vec!=[])\
                .reduceByKey(lambda x,y:x+y)\
                .map(lambda x:Row(timestamp=x[0][0], uid=x[0][1], topicVec=x[1]))
            
#    print(rowRDD.take(10))
    print("========= %d =========" % rowRDD.count())
#   save corr table to cassandra 
    if (rowRDD.count()>0):
	client = SimpleClient(kafkaNodeBC.value)
	producer = KeyedProducer(client)
        for row in rowRDD.collect():
	    line = '{ "timestamp" :"' + str(row['timestamp']) + '",'
            line+= '  "uid"  :"' + str(row['uid']) + '",'
            line+= ' "topicVec":' + json.dumps([float(i) for i in row['topicVec']])+ '}'
#	    print(line)
	    producer.send_messages(outgoingTopic, str(hash(line)), line)
    


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: kafka_wordcount.py <zk> <incomingTopic> <kafkaNode> <outgoingTopic>", file=sys.stderr)
        exit(-1)
    conf = SparkConf().setAppName("tweetProcessingStream").set("spark.cores.max", "4")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
   
#   load product vector 
    volcabulary=Word2Vec.load_word2vec_format('file:///home/ubuntu/data/wordVec.txt', binary=False)
    volcabularyBC=sc.broadcast(volcabulary)

    zkQuorum, incomingTopic, kafkaNode, outgoingTopic = sys.argv[1:]
    kafkaNodeBC=sc.broadcast(kafkaNode)
    outgoingTopicBC=sc.broadcast(outgoingTopic)

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "sparkStreamingProcessTweets", {incomingTopic: 1})
    lines=kvs.map(lambda x: json.loads(x[1]))
#    lines.pprint()
#    ssc.checkpoint('hdfs://ec2-52-2-60-169.compute-1.amazonaws.com:9000/checkpoint/') 
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
