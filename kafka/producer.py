import glob, os, json
import random
import datetime
import hashlib
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import producer_helper as ph

DATADIR="/home/ubuntu/data/"
#KAFKA_NODE="localhost:9092"
KAFKA_NODE="52.45.61.78:9092"
KAFKA_TOPIC="TextEvents"
KAFKA_TOPIC="textticks"


def getData(FileName, NumOfItems=1):
	os.chdir(DATADIR)
	#get the relative path
	topic_files = glob.glob(FileName+'*')

	#get a random file
	topic_file = random.choice(topic_files)
	topic_file = DATADIR + topic_file

	#data = open(topic_file, "r")

	#line = next(data)
	#for num, nextline in enumerate(data):
	#    if random.randrange(num + 2): continue
	#    line = nextline
	#data.close()

#	if(NumOfItems<0):
#		NumOfItems=random.randint(1,5)
#	with open(topic_file, 'r') as fin:
#		#lines=[line for line in fin if random.random()>.8]
#		lines = random.sample(fin.readlines(), NumOfItems)
#		lines = [line.decode("utf8").replace(u'\xc2\xa0',u'').strip().encode() for line in lines]
	t=datetime.datetime.utcnow()
	return [t.isoformat()] + ph.getMessage(t,topic_file)

#topic = str(getData('topic',-1))
#name = getData('name')[0]
#tick = str(datetime.datetime.utcnow().isoformat())

message=getData('User')
tick=message[0]
name=message[1]
topic=json.dumps(message[2])

line = '{ "tick" :"' + tick + '",'
line+= '  "uid"  :"' + name + '",'
line+= '  "topic":' + topic+ '}'

#print line

client = SimpleClient(KAFKA_NODE)
producer = KeyedProducer(client)
producer.send_messages(KAFKA_TOPIC, str(hash(line)), line)
