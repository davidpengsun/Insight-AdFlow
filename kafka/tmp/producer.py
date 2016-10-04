import glob, os, json
import random
import datetime
import hashlib
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import producer_helper1 as ph

DATADIR="/home/ubuntu/data/"
#KAFKA_NODE="localhost:9092"
KAFKA_NODE="52.2.60.169:9092"
#KAFKA_TOPIC="TextEvents"
KAFKA_TOPIC="textticks"


def getData(FileName, NumOfItems=1):
	os.chdir(DATADIR)
	#get the relative path
	topic_files = glob.glob(FileName+'*')

	#get a random file
	topic_file = random.choice(topic_files)
	topic_file = DATADIR + topic_file

	t=datetime.datetime.utcnow()
	return [t.isoformat()] + ph.getMessage(t,topic_file)


def getLine(ModelName='User'):
	message=getData(ModelName)
	tick=message[0]
	name=message[1]
	topic=json.dumps(message[2])

	line = '{ "tick" :"' + tick + '",'
	line+= '  "uid"  :"' + name + '",'
	line+= '  "topic":' + topic+ '}'

	return line


client = SimpleClient(KAFKA_NODE)
producer = KeyedProducer(client)

for _ in xrange(50):
	line = getLine()
	print line
	#producer.send_messages(KAFKA_TOPIC, str(hash(line)), line)
