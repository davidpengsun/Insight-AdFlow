import glob, os, json
import random
import datetime
import hashlib
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import producer_helper_bidder as phb

DATADIR="/home/ubuntu/data/"
KAFKA_NODE="52.2.60.169:9092"
KAFKA_TOPIC="bidTicks"


def getData(FileName):
	os.chdir(DATADIR)
	topicFiles = glob.glob(FileName+'*')
	topicFile = random.choice(topicFiles)
	topicFile = DATADIR + topicFile
	t=datetime.datetime.utcnow()
	return [t.isoformat()] + ph.getMessage(t,topicFile)

def getLine(ModelName='Bidder'):
	[tick, name, price, productID]=getData(ModelName)

	line = '{ "tick" :"' + tick + '",'
	line+= '  "bidder"  :"' + name + '",'
	line+= ' "price":"' + str(price) + '",'
	line+= '  "product":' + productID + '}'

	return line


client = SimpleClient(KAFKA_NODE)
producer = KeyedProducer(client)

for _ in xrange(25):
	line = getLine()
#	print line
	producer.send_messages(KAFKA_TOPIC, str(hash(line)), line)
