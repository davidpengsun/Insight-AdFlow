from __future__ import print_function
import glob, os, sys, json
import random
import datetime,time
import hashlib
import pandas as pd
import numpy as np
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


def getTimeStamp():
    return datetime.datetime.utcnow().isoformat()

def getUid():
    return 'uid' + format(random.randint(10000,99999), '06')

def getState(stateData):
    index=int(np.random.pareto(.8)*10)%50
    return stateData[index]['code']

def getTweet(tweetData):
    return [tweetData[i] for i in np.random.choice(len(tweetData), random.randint(1,10))]

def getStateData(fileName):
    with open(fileName, 'r') as fin:
        return json.load(fin)

def getTweetData(fileName):
    with open(fileName, 'r') as fin:
        return [item.strip() for item in fin.readlines()]

def getJsonLine(stateData, tweetData):
    timestamp=getTimeStamp()
    uid=getUid()
    state=getState(stateData)
    tweet=getTweet(tweetData)

    line = '{ "timeStamp" :"' + timestamp + '",'
    line+= '  "userId"  :"' + uid + '",'
    line+= ' "state":"' + str(state)+'",'
    line+= '  "tweet":' + json.dumps(tweet)+ '}' 

    return line


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: produceTweets.py <stateData> <tweetData> <node> <topic> <rate>", file=sys.stderr)
        exit(-1)
    stateDataFile, tweetDataFile, kafkaNode, kafkaTopic, rate=sys.argv[1:]
#   load date
    stateData=getStateData(stateDataFile)
    tweetData=getTweetData(tweetDataFile)
#   send message
    client = SimpleClient(kafkaNode)
    producer = KeyedProducer(client)
#    for _ in xrange(125):
    while True:
       line = getJsonLine(stateData, tweetData)
       time.sleep(1./float(rate))
#       print(line)
       producer.send_messages(kafkaTopic, str(hash(line)%100), line)

