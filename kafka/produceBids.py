from __future__ import print_function
import glob, os, sys, json
import random
import datetime,time
import hashlib
import pandas as pd
import numpy as np
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

NumOfProduct=500

def getTimeStamp():
    return datetime.datetime.utcnow().isoformat()

def getPid(i):
    if(i<0):
        i=random.randint(0,NumOfProduct)
    return 'pid' + format(i, '05')

def getBid():
    return float('{0:.03f}'.format(np.random.pareto(.3)%64+1))

def getJsonLine(i):
    timestamp=getTimeStamp()
    pid=getPid(i)
    bidPrice=getBid()

    line = '{ "timeStamp" :"' + timestamp + '",'
    line+= '  "productId"  :"' + pid + '",'
    line+= '  "bidPrice":' + str(bidPrice)+ '}' 

    return line


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: produceBids.py <node> <topic>", file=sys.stderr)
        exit(-1)
    kafkaNode, kafkaTopic=sys.argv[1:]
#   load date
    client = SimpleClient(kafkaNode)
    producer = KeyedProducer(client)
#    for i in xrange(NumOfProduct):
    while True:
       line = getJsonLine(-1)
       time.sleep(.01)
       print(line)
       producer.send_messages(kafkaTopic, str(hash(line)%100), line)

